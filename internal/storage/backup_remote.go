// backup_remote.go publishes verified state archives to the private sibling
// Pelican prefix and checks remote metadata/data references for restore approve.
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/statebackup"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// BackupRemote talks to two disjoint Pelican scopes:
//   - reader: read-only lakehouse tokens for reference inventory/approve
//   - auth:   write tokens scoped only to the backups/hot-to-cold-loader prefix
type BackupRemote struct {
	keyPath string   // ES256 PEM used to mint both token scopes
	reader  *Pelican // lakehouse GET/HEAD/PROPFIND only
	auth    *Pelican // backup prefix PUT/DELETE/MKCOL (+ GET for readback)
	base    string   // absolute https://…/sage/cold/backups/hot-to-cold-loader
	Keep    int      // verified backups retained per identity key
}

func NewBackupRemote(lakehouse, key, scratch string, t TableIdentity, destination string, keep int) (*BackupRemote, error) {
	p, e := NewPelicanReader(lakehouse, key, scratch, t)
	if e != nil {
		return nil, e
	}
	if destination == "" {
		destination = strings.TrimSuffix(lakehouse, "/lakehouse") + "/backups/hot-to-cold-loader"
	}
	u, e := url.Parse(destination)
	if e != nil {
		return nil, e
	}
	expected := strings.TrimSuffix(p.Base.Path, "/lakehouse") + "/backups/hot-to-cold-loader"
	if u.Scheme != "https" || u.Host != p.Base.Host || u.Path != expected || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.User != nil || keep < 1 {
		return nil, errors.New("backup destination must be the isolated sibling backups/hot-to-cold-loader prefix")
	}
	auth := *p
	auth.ReadOnly = false
	auth.Prefix = strings.TrimPrefix(u.Path, "/sage")
	auth.Client = &http.Client{Timeout: 6 * time.Hour, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &BackupRemote{reader: p, auth: &auth, base: destination, Keep: keep, keyPath: key}, nil
}
func (r *BackupRemote) request(ctx context.Context, method, location string, body io.Reader, size int64) (*http.Response, error) {
	u, e := url.Parse(location)
	if e != nil {
		return nil, e
	}
	base, _ := url.Parse(r.base)
	if u.Scheme != base.Scheme || u.Host != base.Host || (u.Path != base.Path && !strings.HasPrefix(u.Path, base.Path+"/")) || u.RawPath != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || path.Clean(u.Path) != u.Path {
		return nil, errors.New("object outside backup prefix")
	}
	write := method == "PUT" || method == "DELETE" || method == "MKCOL"
	tok, e := r.auth.token(write, 6*time.Hour+time.Minute)
	if e != nil {
		return nil, e
	}
	req, e := http.NewRequestWithContext(ctx, method, location, body)
	if e != nil {
		return nil, e
	}
	req.ContentLength = size
	req.Header.Set("Authorization", "Bearer "+tok)
	if method == "PROPFIND" {
		req.Header.Set("Depth", "1")
	}
	if method == "PUT" {
		req.Header.Set("If-None-Match", "*")
	}
	resp, e := r.auth.Client.Do(req)
	if e != nil {
		return nil, e
	}
	if (resp.StatusCode >= 200 && resp.StatusCode < 300) || (method == "MKCOL" && resp.StatusCode == 405) {
		return resp, nil
	}
	resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, fs.ErrNotExist
	}
	return nil, fmt.Errorf("backup %s HTTP %d", method, resp.StatusCode)
}
func (r *BackupRemote) small(ctx context.Context, method, location string, data []byte) ([]byte, error) {
	resp, e := r.request(ctx, method, location, bytes.NewReader(data), int64(len(data)))
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024+1))
	if e != nil {
		return nil, e
	}
	if len(b) > 64*1024*1024 {
		return nil, errors.New("backup response exceeds limit")
	}
	return b, nil
}
func (r *BackupRemote) published(ctx context.Context) (string, error) {
	hint := r.reader.Table.Location(r.reader.Base.String()) + "/metadata/version-hint.text"
	resp, e := r.reader.request(ctx, "GET", hint, nil, 0)
	if errors.Is(e, fs.ErrNotExist) {
		return "", nil
	}
	if e != nil {
		return "", e
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if e != nil {
		return "", e
	}
	stem := string(b)
	if len(b) > 4096 || !regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*$`).MatchString(stem) || strings.Contains(stem, "..") {
		return "", errors.New("invalid published metadata hint")
	}
	return r.reader.Table.Location(r.reader.Base.String()) + "/metadata/" + stem + ".metadata.json", nil
}
func (r *BackupRemote) readMetadata(ctx context.Context, name string, refs map[string]string) ([]byte, error) {
	resp, e := r.reader.request(ctx, "GET", name, nil, 0)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024+1))
	if e != nil {
		return nil, e
	}
	if len(raw) > 64*1024*1024 {
		return nil, errors.New("metadata exceeds limit")
	}
	hash := sha256.Sum256(raw)
	refs[name] = hex.EncodeToString(hash[:])
	return raw, nil
}
func (r *BackupRemote) dataReference(ctx context.Context, name string, size int64, refs map[string]string) error {
	if _, ok := refs[name]; ok {
		return nil
	}
	resp, e := r.reader.request(ctx, "HEAD", name, nil, 0)
	if e != nil {
		return e
	}
	resp.Body.Close()
	if resp.ContentLength < 0 || (size >= 0 && resp.ContentLength != size) {
		return errors.New("remote data-file length differs")
	}
	refs[name] = "size:" + strconv.FormatInt(resp.ContentLength, 10)
	return nil
}
func (r *BackupRemote) graph(ctx context.Context, location string, refs map[string]string) (table.Metadata, error) {
	raw, e := r.readMetadata(ctx, location, refs)
	if e != nil {
		return nil, e
	}
	meta, e := table.ParseMetadataBytes(raw)
	if e != nil {
		return nil, e
	}
	if meta.Location() != r.reader.Table.Location(r.reader.Base.String()) || meta.Version() != 3 {
		return nil, errors.New("remote metadata identity differs")
	}
	// Every retained snapshot is checked, including manifests used by old read pins.
	var doc struct {
		Snapshots []struct {
			ManifestList string `json:"manifest-list"`
		} `json:"snapshots"`
	}
	if e = json.Unmarshal(raw, &doc); e != nil {
		return nil, e
	}
	for _, snap := range doc.Snapshots {
		if _, ok := refs[snap.ManifestList]; ok {
			continue
		}
		data, e := r.readMetadata(ctx, snap.ManifestList, refs)
		if e != nil {
			return nil, e
		}
		manifests, e := iceberg.ReadManifestList(bytes.NewReader(data))
		if e != nil {
			return nil, e
		}
		for _, mf := range manifests {
			if _, ok := refs[mf.FilePath()]; ok {
				continue
			}
			data, e = r.readMetadata(ctx, mf.FilePath(), refs)
			if e != nil {
				return nil, e
			}
			entries, e := iceberg.ReadManifest(mf, bytes.NewReader(data), true)
			if e != nil {
				return nil, e
			}
			for _, entry := range entries {
				f := entry.DataFile()
				if e = r.dataReference(ctx, f.FilePath(), f.FileSizeBytes(), refs); e != nil {
					return nil, e
				}
			}
		}
	}
	return meta, nil
}

// referenceClient honors the separately bound read-only bootstrap root.
// It never broadens either client's token scope or destination validation.
func (r *BackupRemote) referenceClient(m *statebackup.Manifest, name string) (*BackupRemote, error) {
	if m.BootstrapLakehouse != "" && strings.HasPrefix(name, r.reader.Table.Location(m.BootstrapLakehouse)+"/") {
		if m.BootstrapLakehouse == m.Lakehouse {
			return nil, errors.New("bootstrap must differ from managed lakehouse")
		}
		p, e := NewPelicanReader(m.BootstrapLakehouse, r.keyPath, r.reader.Scratch, r.reader.Table)
		if e != nil {
			return nil, e
		}
		p.Client = r.reader.Client
		clone := *r
		clone.reader = p
		return &clone, nil
	}
	return r, nil
}

// References walks catalog/journal/pin metadata reachable from the snapshot and
// records checksums (metadata) or size proofs (Parquet) for later approve.
func (r *BackupRemote) References(ctx context.Context, m *statebackup.Manifest, root string) (map[string]string, error) {
	if m.Lakehouse != r.reader.Base.String() || m.Namespace != r.reader.Table.Namespace() || m.Table != r.reader.Table.Name() {
		return nil, errors.New("backup and reader identities differ")
	}
	published, e := r.published(ctx)
	if e != nil {
		return nil, e
	}
	m.PublishedMetadata = published
	refs := map[string]string{}
	var catalogMeta table.Metadata
	if m.CatalogMetadata != "" {
		catalogMeta, e = r.graph(ctx, m.CatalogMetadata, refs)
		if e != nil {
			return nil, e
		}
	}
	if m.Metadata != "" {
		if _, e = r.graph(ctx, m.Metadata, refs); e != nil {
			return nil, e
		}
	}
	if published != "" && published != m.CatalogMetadata && published != m.Metadata {
		return nil, errors.New("published history differs from local catalog/journal")
	}
	if published == "" && m.Metadata != "" {
		return nil, errors.New("routed state has no published hint")
	}
	db, e := statebackup.OpenReadOnly(filepath.Join(root, "control.db"))
	if e != nil {
		return nil, e
	}
	defer db.Close()
	rows, e := db.QueryContext(ctx, "SELECT id,state,payload FROM jobs WHERE state!='COMPLETE'")
	if e != nil {
		return nil, e
	}
	pending := map[string]string{}
	for rows.Next() {
		var id, state, payload string
		if e = rows.Scan(&id, &state, &payload); e != nil {
			rows.Close()
			return nil, e
		}
		pending[id] = state
		var job struct {
			Metadata       string   `json:"metadata"`
			SourceMetadata string   `json:"source_metadata"`
			Files          []string `json:"files"`
		}
		if e = json.Unmarshal([]byte(payload), &job); e != nil {
			rows.Close()
			return nil, e
		}
		for _, loc := range []string{job.Metadata, job.SourceMetadata} {
			if loc != "" {
				client, err := r.referenceClient(m, loc)
				if err != nil {
					rows.Close()
					return nil, err
				}
				if _, e = client.graph(ctx, loc, refs); e != nil {
					rows.Close()
					return nil, e
				}
			}
		}
		if state == "BUILT" || state == "COMMITTED" || state == "REMOTE_VERIFIED" || state == "ROUTED" || state == "DELETING" || state == "DELETE_REQUESTED" {
			for _, loc := range job.Files {
				if e = r.dataReference(ctx, loc, -1, refs); e != nil {
					rows.Close()
					return nil, e
				}
			}
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	if m.CatalogMetadata != m.Metadata {
		if catalogMeta == nil {
			return nil, errors.New("journal/catalog relationship is incomplete")
		}
		if catalogMeta.CurrentSnapshot() != nil {
			id := catalogMeta.Properties()["sage.last.job"]
			switch pending[id] {
			case "BUILT", "COMMITTED", "REMOTE_VERIFIED", "ROUTED", "DELETING", "DELETE_REQUESTED":
			default:
				return nil, errors.New("catalog ahead without matching recoverable job")
			}
		} else if published != "" || m.Metadata != "" {
			return nil, errors.New("empty catalog differs from routed state")
		}
	}
	if m.BootstrapMetadata != "" {
		client, err := r.referenceClient(m, m.BootstrapMetadata)
		if err != nil {
			return nil, err
		}
		if _, err = client.graph(ctx, m.BootstrapMetadata, refs); err != nil {
			return nil, err
		}
	}
	// Read pins may refer to older snapshots not in the active job ledger.
	pins, e := db.QueryContext(ctx, "SELECT payload FROM readers")
	if e != nil {
		return nil, e
	}
	for pins.Next() {
		var raw string
		if e = pins.Scan(&raw); e != nil {
			pins.Close()
			return nil, e
		}
		var pin struct {
			Metadata string `json:"metadata"`
		}
		if e = json.Unmarshal([]byte(raw), &pin); e != nil {
			pins.Close()
			return nil, e
		}
		if pin.Metadata != "" {
			if _, ok := refs[pin.Metadata]; !ok {
				if _, e = r.graph(ctx, pin.Metadata, refs); e != nil {
					pins.Close()
					return nil, e
				}
			}
		}
	}
	e = pins.Err()
	pins.Close()
	if e != nil {
		return nil, e
	}
	return refs, nil
}

// CheckReferences re-reads the captured inventory against live Pelican objects.
// A changed publication hint or checksum fails closed for offline approve.
func (r *BackupRemote) CheckReferences(ctx context.Context, m *statebackup.Manifest, refs map[string]string) error {
	if m.Lakehouse != r.reader.Base.String() || m.Namespace != r.reader.Table.Namespace() || m.Table != r.reader.Table.Name() {
		return errors.New("configured restore identity differs")
	}
	pub, e := r.published(ctx)
	if e != nil {
		return e
	}
	if pub != m.PublishedMetadata {
		return errors.New("remote publication advanced or changed; manual reconciliation required")
	}
	if (m.CatalogMetadata != "" || m.Metadata != "") && len(refs) == 0 {
		return errors.New("missing remote recovery inventory")
	}
	for name, want := range refs {
		client, err := r.referenceClient(m, name)
		if err != nil {
			return err
		}
		if strings.HasPrefix(want, "size:") {
			size, e := strconv.ParseInt(strings.TrimPrefix(want, "size:"), 10, 64)
			if e == nil && size < 0 {
				e = errors.New("invalid data-file size")
			}
			if e != nil {
				return e
			}
			if e = client.dataReference(ctx, name, size, map[string]string{}); e != nil {
				return e
			}
		} else {
			got := map[string]string{}
			if _, e = client.readMetadata(ctx, name, got); e != nil {
				return e
			}
			if got[name] != want {
				return errors.New("remote metadata checksum changed")
			}
		}
	}
	return nil
}

type backupComplete struct {
	Version     int       `json:"version"`
	ID          string    `json:"id"`
	Identity    string    `json:"identity"`
	CreatedAt   time.Time `json:"created_at"`
	ArchiveSHA  string    `json:"archive_sha256"`
	ManifestSHA string    `json:"manifest_sha256"`
	Bytes       int64     `json:"archive_bytes"`
}

// Publish packs the snapshot, uploads state.tar.gz + manifest.json, then writes
// COMPLETE.json last after readback. prune failures wrap RetentionError.
func (r *BackupRemote) Publish(ctx context.Context, snapshotDir string, m *statebackup.Manifest) error {
	identityDir := r.base + "/" + statebackup.IdentityKey(m)
	backupDir := identityDir + "/" + m.ID
	if !regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(m.ID) {
		return errors.New("invalid backup id")
	}
	for _, dir := range []string{r.base, identityDir, backupDir} {
		if _, e := r.small(ctx, "MKCOL", dir, nil); e != nil {
			return e
		}
	}
	archive := filepath.Join(filepath.Dir(snapshotDir), "state.tar.gz")
	if e := statebackup.Pack(ctx, snapshotDir, archive); e != nil {
		return e
	}
	f, e := os.Open(archive)
	if e != nil {
		return e
	}
	info, e := f.Stat()
	if e != nil {
		f.Close()
		return e
	}
	resp, e := r.request(ctx, "PUT", backupDir+"/state.tar.gz", f, info.Size())
	f.Close()
	if e != nil {
		return e
	}
	resp.Body.Close()
	hash, e := statebackup.Digest(archive)
	if e != nil {
		return e
	}
	resp, e = r.request(ctx, "GET", backupDir+"/state.tar.gz", nil, 0)
	if e != nil {
		return e
	}
	h := sha256.New()
	n, e := io.Copy(h, resp.Body)
	resp.Body.Close()
	if e != nil {
		return e
	}
	if n != info.Size() || hex.EncodeToString(h.Sum(nil)) != hash {
		return errors.New("remote archive readback differs")
	}
	data, e := os.ReadFile(filepath.Join(snapshotDir, statebackup.ManifestName))
	if e != nil {
		return e
	}
	if _, e = r.small(ctx, "PUT", backupDir+"/manifest.json", data); e != nil {
		return e
	}
	readback, e := r.small(ctx, "GET", backupDir+"/manifest.json", nil)
	if e != nil {
		return e
	}
	if !bytes.Equal(data, readback) {
		return errors.New("remote manifest readback differs")
	}
	md := sha256.Sum256(data)
	complete := backupComplete{1, m.ID, statebackup.IdentityKey(m), m.CreatedAt, hash, hex.EncodeToString(md[:]), n}
	encoded, _ := json.Marshal(complete)
	if _, e = r.small(ctx, "PUT", backupDir+"/COMPLETE.json", encoded); e != nil {
		return e
	}
	got, e := r.small(ctx, "GET", backupDir+"/COMPLETE.json", nil)
	if e != nil {
		return e
	}
	if !bytes.Equal(got, encoded) {
		return errors.New("remote completion readback differs")
	}
	if e = r.prune(ctx, identityDir, complete.Identity); e != nil {
		return &statebackup.RetentionError{Err: e}
	}
	return nil
}
func (r *BackupRemote) directories(ctx context.Context, group string) ([]string, error) {
	data, e := r.small(ctx, "PROPFIND", group, nil)
	if e != nil {
		return nil, e
	}
	var listing struct {
		Responses []struct {
			Href string `xml:"href"`
		} `xml:"response"`
	}
	if e = xml.Unmarshal(data, &listing); e != nil {
		return nil, e
	}
	base, _ := url.Parse(group)
	out := []string{}
	seen := map[string]bool{}
	baseDir := *base
	baseDir.Path += "/"
	for _, entry := range listing.Responses {
		href, e := url.Parse(entry.Href)
		if e != nil {
			return nil, e
		}
		u := baseDir.ResolveReference(href)
		prefix := base.Path + "/"
		if u.Host != base.Host || u.Scheme != base.Scheme || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("invalid backup listing reference")
		}
		if strings.TrimSuffix(u.Path, "/") == base.Path {
			continue
		}
		if !strings.HasPrefix(u.Path, prefix) {
			return nil, errors.New("listing escaped backup identity")
		}
		name := strings.TrimSuffix(strings.TrimPrefix(u.Path, prefix), "/")
		if regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(name) {
			if !seen[name] {
				out = append(out, name)
				seen[name] = true
			}
		} else {
			return nil, errors.New("unexpected backup directory name")
		}
	}
	return out, nil
}
func (r *BackupRemote) prune(ctx context.Context, group, identity string) error {
	names, e := r.directories(ctx, group)
	if e != nil {
		return e
	}
	completed := []backupComplete{}
	for _, name := range names {
		raw, e := r.small(ctx, "GET", group+"/"+name+"/COMPLETE.json", nil)
		if errors.Is(e, fs.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		var c backupComplete
		if e = json.Unmarshal(raw, &c); e != nil {
			return e
		}
		if c.Version != 1 || c.ID != name || c.Identity != identity || c.CreatedAt.IsZero() {
			return errors.New("invalid completion marker; refusing retention")
		}
		data, e := r.small(ctx, "GET", group+"/"+name+"/manifest.json", nil)
		if e != nil {
			return e
		}
		hash := sha256.Sum256(data)
		if hex.EncodeToString(hash[:]) != c.ManifestSHA {
			return errors.New("invalid completed manifest; refusing retention")
		}
		var m statebackup.Manifest
		if e = json.Unmarshal(data, &m); e != nil {
			return e
		}
		if m.ID != name || statebackup.IdentityKey(&m) != identity {
			return errors.New("completed backup identity differs")
		}
		archive, e := r.request(ctx, "HEAD", group+"/"+name+"/state.tar.gz", nil, 0)
		if e != nil {
			return e
		}
		size := archive.ContentLength
		archive.Body.Close()
		if size != c.Bytes || size < 0 {
			return errors.New("completed archive missing or changed; refusing retention")
		}
		completed = append(completed, c)
	}
	sort.Slice(completed, func(i, j int) bool {
		if completed[i].CreatedAt.Equal(completed[j].CreatedAt) {
			return completed[i].ID > completed[j].ID
		}
		return completed[i].CreatedAt.After(completed[j].CreatedAt)
	})
	if len(completed) <= r.Keep {
		return nil
	}
	for _, c := range completed[r.Keep:] {
		for _, name := range []string{"COMPLETE.json", "manifest.json", "state.tar.gz"} {
			if _, e = r.small(ctx, "DELETE", group+"/"+c.ID+"/"+name, nil); e != nil {
				return e
			}
		}
	}
	return nil
}

// Download verifies both the completion marker and archive before returning a
// local bundle; it never extracts data or changes the lakehouse.
func (r *BackupRemote) Download(ctx context.Context, identity, id, dest string) error {
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(identity) || !regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(id) {
		return errors.New("invalid backup reference")
	}
	resolved, e := statebackup.ResolvePath(dest)
	if e != nil {
		return e
	}
	dest = resolved
	base := r.base + "/" + identity + "/" + id
	raw, e := r.small(ctx, "GET", base+"/COMPLETE.json", nil)
	if e != nil {
		return e
	}
	var c backupComplete
	if e = json.Unmarshal(raw, &c); e != nil {
		return e
	}
	if c.Version != 1 || c.ID != id || c.Identity != identity || c.Bytes < 0 {
		return errors.New("invalid backup completion")
	}
	f, e := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	resp, e := r.request(ctx, "GET", base+"/state.tar.gz", nil, 0)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	hash := sha256.New()
	n, e := io.Copy(io.MultiWriter(f, hash), io.LimitReader(resp.Body, c.Bytes+1))
	if e != nil {
		return e
	}
	if n != c.Bytes || hex.EncodeToString(hash.Sum(nil)) != c.ArchiveSHA {
		return errors.New("downloaded backup checksum differs")
	}
	return f.Sync()
}

// BackupSummary is enough to discover a recovery point after both local disks
// are lost. It does not expose observation payloads, credentials or file hashes.
type BackupSummary struct {
	ID        string    `json:"id"`
	Identity  string    `json:"identity"`
	CreatedAt time.Time `json:"created_at"`
	Bucket    string    `json:"bucket"`
	BucketID  string    `json:"bucket_id"`
	Org       string    `json:"org"`
	Frontier  int64     `json:"frontier_ns"`
	Bytes     int64     `json:"archive_bytes"`
}

func (r *BackupRemote) List(ctx context.Context, bucket string) ([]BackupSummary, error) {
	groups, e := r.directories(ctx, r.base)
	if e != nil {
		return nil, e
	}
	out := []BackupSummary{}
	for _, group := range groups {
		if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(group) {
			return nil, errors.New("invalid backup identity directory")
		}
		names, e := r.directories(ctx, r.base+"/"+group)
		if e != nil {
			return nil, e
		}
		for _, name := range names {
			base := r.base + "/" + group + "/" + name
			raw, e := r.small(ctx, "GET", base+"/COMPLETE.json", nil)
			if errors.Is(e, fs.ErrNotExist) {
				continue
			}
			if e != nil {
				return nil, e
			}
			var c backupComplete
			if e = json.Unmarshal(raw, &c); e != nil {
				return nil, e
			}
			if c.Version != 1 || c.ID != name || c.Identity != group || c.CreatedAt.IsZero() {
				return nil, errors.New("invalid remote completion marker")
			}
			data, e := r.small(ctx, "GET", base+"/manifest.json", nil)
			if e != nil {
				return nil, e
			}
			hash := sha256.Sum256(data)
			if hex.EncodeToString(hash[:]) != c.ManifestSHA {
				return nil, errors.New("remote manifest checksum differs")
			}
			var m statebackup.Manifest
			if e = json.Unmarshal(data, &m); e != nil {
				return nil, e
			}
			if statebackup.IdentityKey(&m) != group || m.ID != name {
				return nil, errors.New("backup identity differs")
			}
			if m.Lakehouse != r.reader.Base.String() || m.Namespace != r.reader.Table.Namespace() || m.Table != r.reader.Table.Name() || m.Bucket != bucket {
				continue
			}
			resp, e := r.request(ctx, "HEAD", base+"/state.tar.gz", nil, 0)
			if e != nil {
				return nil, e
			}
			size := resp.ContentLength
			resp.Body.Close()
			if size != c.Bytes || size < 0 {
				return nil, errors.New("backup archive missing or changed")
			}
			out = append(out, BackupSummary{m.ID, group, m.CreatedAt, m.Bucket, m.SourceBucketID, m.SourceOrg, m.Frontier, size})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

// RequireNewState prevents coordinate-only startup from creating an empty
// journal against already published cold history after loss of the state PVC.
func (r *BackupRemote) RequireNewState(ctx context.Context) error {
	published, e := r.published(ctx)
	if e != nil {
		return e
	}
	if published != "" {
		return errors.New("local control state is missing for a published lakehouse; restore and approve its backup instead of initializing empty state")
	}
	return nil
}
