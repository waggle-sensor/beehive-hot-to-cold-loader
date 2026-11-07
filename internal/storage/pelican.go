// pelican.go is the only remote object client. Every URL must stay inside the
// configured warehouse, and this client never deletes an object.

package storage

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	iceio "github.com/apache/iceberg-go/io"
)

// Pelican confines every IO operation and token to a configured isolated root.
// Data objects stream through disk. Remove is disabled; maintenance is separate.
type Pelican struct {
	Base     *url.URL // the base URL of the Pelican instance
	Prefix   string // the prefix of the warehouse
	Key      *ecdsa.PrivateKey // the private key for the Pelican instance
	Client   *http.Client // the HTTP client
	Scratch  string // the scratch directory for the Pelican instance
	ReadOnly bool // whether the Pelican instance is read-only
}

// NewPelican opens a read-write client for the destination warehouse.
func NewPelican(warehouse, keyPath, scratch string) (*Pelican, error) {
	return newPelican(warehouse, keyPath, scratch, false)
}

// NewPelicanReader opens a client that refuses writes. Bootstrap sources use it.
func NewPelicanReader(warehouse, keyPath, scratch string) (*Pelican, error) {
	return newPelican(warehouse, keyPath, scratch, true)
}

// newPelican creates a new Pelican instance for the given warehouse, key path, and scratch directory.
func newPelican(warehouse, keyPath, scratch string, readOnly bool) (*Pelican, error) {
	u, e := url.Parse(warehouse)
	if e != nil {
		return nil, e
	}
	clean := path.Clean(u.Path)
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	allowedPrefix := regexp.MustCompile(`^cold-storage-archive-[A-Za-z0-9_-]+$`)
	if readOnly {
		allowedPrefix = regexp.MustCompile(`^cold-storage-[A-Za-z0-9_-]+$`)
	}
	if u.Scheme != "https" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || u.RawPath != "" || clean != u.Path || len(parts) != 3 || parts[0] != "sage" || !allowedPrefix.MatchString(parts[1]) || parts[2] != "warehouse" || strings.Contains(parts[1], "..") {
		return nil, errors.New("warehouse must be https://host/sage/cold-storage-archive-<unique>/warehouse")
	}
	raw, e := os.ReadFile(keyPath)
	if e != nil {
		return nil, e
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("invalid signing key PEM")
	}
	var k *ecdsa.PrivateKey
	if parsed, e := x509.ParsePKCS8PrivateKey(block.Bytes); e == nil {
		k, _ = parsed.(*ecdsa.PrivateKey)
	} else {
		k, _ = x509.ParseECPrivateKey(block.Bytes)
	}
	if k == nil || k.Curve.Params().BitSize != 256 {
		return nil, errors.New("Pelican ES256 key required")
	}
	c := &http.Client{Timeout: 20 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Pelican{Base: u, Prefix: "/" + parts[1], Key: k, Client: c, Scratch: scratch, ReadOnly: readOnly}, nil
}

// Token returns a short-lived bearer token. Write tokens are minted only for PUT.
func (p *Pelican) Token(write bool) (string, error) { return p.token(write, 20*time.Minute) }

// token signs a token confined to this warehouse prefix.
func (p *Pelican) token(write bool, lifetime time.Duration) (string, error) {
	now := time.Now().Unix()
	scope := "read:" + p.Prefix
	if write {
		if p.ReadOnly {
			return "", errors.New("bootstrap source is read-only")
		}
		scope += " write:" + p.Prefix
	}
	claims := map[string]any{"sub": "sage-archiver", "aud": "ANY", "ver": "scitoken:2.0", "scope": scope, "iss": "https://sagecontinuum.org", "iat": now - 60, "nbf": now - 60, "exp": now + int64(lifetime.Seconds()), "jti": fmt.Sprint(now)}
	header, _ := json.Marshal(map[string]string{"alg": "ES256", "kid": "3cfa", "typ": "JWT"})
	body, _ := json.Marshal(claims)
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	h := sha256.Sum256([]byte(unsigned))
	r, s, e := ecdsa.Sign(rand.Reader, p.Key, h[:])
	if e != nil {
		return "", e
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// validate accepts only an HTTPS object under this warehouse's sage/obs prefix.
func (p *Pelican) validate(name string) (string, error) {
	u, e := url.Parse(name)
	if e != nil {
		return "", e
	}
	if u.Scheme != "https" || u.Host != p.Base.Host || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || path.Clean(u.Path) != u.Path || !strings.HasPrefix(u.Path, p.Base.Path+"/sage/obs/") {
		return "", errors.New("object outside managed warehouse")
	}
	return u.String(), nil
}

// request performs one scoped HTTP call. A 404 is fs.ErrNotExist.
func (p *Pelican) request(ctx context.Context, method, name string, body io.Reader, length int64) (*http.Response, error) {
	location, e := p.validate(name)
	if e != nil {
		return nil, e
	}
	tok, e := p.Token(method == "PUT")
	if e != nil {
		return nil, e
	}
	req, e := http.NewRequestWithContext(ctx, method, location, body)
	if e != nil {
		return nil, e
	}
	req.ContentLength = length
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, e := p.Client.Do(req)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		if resp.StatusCode == 404 {
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("Pelican %s HTTP %d", method, resp.StatusCode)
	}
	return resp, nil
}

// ReadFile loads a metadata object, capped at 64 MiB.
func (p *Pelican) ReadFile(name string) ([]byte, error) {
	resp, e := p.request(context.Background(), "GET", name, nil, 0)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	const max = 64 << 20
	b, e := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if len(b) > max {
		return nil, errors.New("metadata object exceeds 64 MiB")
	}
	return b, e
}

// WriteFile uploads a small metadata object. Data files use Upload.
func (p *Pelican) WriteFile(name string, b []byte) error {
	r, e := p.request(context.Background(), "PUT", name, bytes.NewReader(b), int64(len(b)))
	if e == nil {
		io.Copy(io.Discard, r.Body)
		e = r.Body.Close()
	}
	return e
}

// Remove is disabled. Remote cleanup is an explicit maintenance action.
func (p *Pelican) Remove(name string) error {
	return errors.New("automatic Pelican object deletion disabled")
}

// temporaryFile is a temporary file for the Pelican instance.
type temporaryFile struct {
	*os.File
	filename string // the filename of the temporary file
}

// Close closes the temporary file and removes it.
func (f *temporaryFile) Close() error { e := f.File.Close(); os.Remove(f.filename); return e }

// Open downloads one object to scratch for the Iceberg reader.
func (p *Pelican) Open(name string) (iceio.File, error) {
	resp, e := p.request(context.Background(), "GET", name, nil, 0)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	f, e := os.CreateTemp(p.Scratch, "iceberg-read-*")
	if e != nil {
		return nil, e
	}
	fail := func(e error) (iceio.File, error) { f.Close(); os.Remove(f.Name()); return nil, e }
	if _, e = io.Copy(f, resp.Body); e != nil {
		return fail(e)
	}
	if _, e = f.Seek(0, 0); e != nil {
		return fail(e)
	}
	return &temporaryFile{f, f.Name()}, nil
}

type objectWriter struct {
	*os.File
	p      *Pelican
	name   string
	closed bool
}

func (w *objectWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	defer os.Remove(w.File.Name())
	defer w.File.Close()
	if _, e := w.File.Seek(0, 0); e != nil {
		return e
	}
	st, e := w.File.Stat()
	if e != nil {
		return e
	}
	resp, e := w.p.request(context.Background(), "PUT", w.name, w.File, st.Size())
	if e == nil {
		io.Copy(io.Discard, resp.Body)
		e = resp.Body.Close()
	}
	return e
}

// Create starts a local temporary file that is uploaded when closed.
func (p *Pelican) Create(name string) (iceio.FileWriter, error) {
	if _, e := p.validate(name); e != nil {
		return nil, e
	}
	f, e := os.CreateTemp(p.Scratch, "iceberg-write-*")
	if e != nil {
		return nil, e
	}
	return &objectWriter{File: f, p: p, name: name}, nil
}

// Upload sends a local file and checks the remote bytes before returning.
func (p *Pelican) Upload(ctx context.Context, local, remote string) error {
	f, e := os.Open(local)
	if e != nil {
		return e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return e
	}
	resp, e := p.request(ctx, "PUT", remote, f, st.Size())
	if e != nil {
		return e
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return p.VerifyObject(ctx, local, remote)
}

// VerifyObject compares the SHA-256 of the local file with a fresh remote read.
func (p *Pelican) VerifyObject(ctx context.Context, local, remote string) error {
	f, e := os.Open(local)
	if e != nil {
		return e
	}
	defer f.Close()
	want := sha256.New()
	if _, e = io.Copy(want, f); e != nil {
		return e
	}
	resp, e := p.request(ctx, "GET", remote, nil, 0)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	got := sha256.New()
	if _, e = io.Copy(got, resp.Body); e != nil {
		return e
	}
	if !bytes.Equal(want.Sum(nil), got.Sum(nil)) {
		return errors.New("remote object SHA-256 differs")
	}
	return nil
}

// _ iceio.WriteFileIO = (*Pelican)(nil) is a compile-time check to ensure that the Pelican instance implements the iceio.WriteFileIO interface.
var _ iceio.WriteFileIO = (*Pelican)(nil)

// Head resolves the published version hint to a metadata URL.
func (p *Pelican) Head() (string, error) {
	hint, e := p.ReadFile(p.Base.String() + "/sage/obs/metadata/version-hint.text")
	if e != nil {
		return "", e
	}
	stem := string(hint)
	if !regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*$`).MatchString(stem) || strings.Contains(stem, "..") {
		return "", errors.New("invalid bootstrap metadata hint")
	}
	return p.Base.String() + "/sage/obs/metadata/" + stem + ".metadata.json", nil
}
