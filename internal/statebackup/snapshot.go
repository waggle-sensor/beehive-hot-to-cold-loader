// Package statebackup creates and checks complete, immutable recovery bundles.
// snapshot.go owns SQLite online copies, inventory/manifest construction, and
// path safety. It does not run the scheduler or change lakehouse history.
package statebackup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const ManifestName = "manifest.json"
const PendingName = "restore-pending.json" // blocks daemon start until Approve

// Entry is one inventoried regular file inside a recovery snapshot.
type Entry struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
}

// Manifest describes one immutable recovery point and its remote references.
type Manifest struct {
	BootstrapLakehouse string            `json:"bootstrap_lakehouse,omitempty"`
	BootstrapMetadata  string            `json:"bootstrap_metadata,omitempty"`
	BootstrapBefore    string            `json:"bootstrap_before,omitempty"`
	Directories        map[string]uint32 `json:"directories"`
	UID                int               `json:"owner_uid"`
	GID                int               `json:"owner_gid"`
	Version            int               `json:"version"`
	ID                 string            `json:"id"`
	CreatedAt          time.Time         `json:"created_at"`
	StatePath          string            `json:"state_path"`
	Image              string            `json:"image"`
	Bucket             string            `json:"bucket"`
	Lakehouse          string            `json:"lakehouse"`
	Namespace          string            `json:"namespace"`
	Table              string            `json:"table"`
	SourceBucketID     string            `json:"source_bucket_id"`
	SourceOrg          string            `json:"source_org"`
	Start              int64             `json:"coverage_start_ns"`
	Frontier           int64             `json:"frontier_ns"`
	Generation         int64             `json:"generation"`
	Metadata           string            `json:"routed_metadata"`
	CatalogMetadata    string            `json:"catalog_metadata"`
	PublishedMetadata  string            `json:"published_metadata"`
	InboxMaxSeq        int64             `json:"inbox_max_seq"`
	References         map[string]string `json:"references"`
	Files              map[string]Entry  `json:"files"`
}

type Remote interface {
	// References returns the current published pointer and hashes of all objects
	// reachable from the captured catalog/routing/job snapshots.
	References(context.Context, *Manifest, string) (map[string]string, error)
	CheckReferences(context.Context, *Manifest, map[string]string) error
	Publish(context.Context, string, *Manifest) error
}

func CheckPath(p string) error {
	abs, err := filepath.Abs(p)
	if err != nil {
		return err
	}
	for _, part := range strings.Split(abs, string(filepath.Separator)) {
		if part == "node-data" {
			return errors.New("/node-data is outside backup operations")
		}
	}
	return nil
}

// ResolvePath resolves existing ancestors before any directory is created.
// This catches forbidden paths and nested scratch even through an alias.
func ResolvePath(p string) (string, error) {
	abs, e := filepath.Abs(p)
	if e != nil {
		return "", e
	}
	if e = CheckPath(abs); e != nil {
		return "", e
	}
	parent := abs
	for {
		_, e = os.Lstat(parent)
		if e == nil {
			break
		}
		if !os.IsNotExist(e) {
			return "", e
		}
		next := filepath.Dir(parent)
		if next == parent {
			return "", e
		}
		parent = next
	}
	resolved, e := filepath.EvalSymlinks(parent)
	if e != nil {
		return "", e
	}
	rel, e := filepath.Rel(parent, abs)
	if e != nil {
		return "", e
	}
	result := filepath.Join(resolved, rel)
	if e = CheckPath(result); e != nil {
		return "", e
	}
	return result, nil
}

func Disjoint(a, b string) error {
	for _, p := range []string{a, b} {
		if err := CheckPath(p); err != nil {
			return err
		}
	}
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return err
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return errors.New("backup scratch and state must be disjoint directories")
		}
	}
	return nil
}

func OpenReadOnly(path string) (*sql.DB, error) {
	if err := CheckPath(path); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("database must be a regular file")
	}
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_pragma", "busy_timeout(1000)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func SnapshotDB(ctx context.Context, source, dest string) error {
	db, err := OpenReadOnly(source)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, "VACUUM INTO '"+strings.ReplaceAll(dest, "'", "''")+"'")
	return err
}

func Integrity(ctx context.Context, path string) error {
	db, err := OpenReadOnly(path)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			return err
		}
		if v != "ok" {
			return fmt.Errorf("database integrity check failed: %s", filepath.Base(path))
		}
		n++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if n != 1 {
		return errors.New("incomplete integrity check")
	}
	return nil
}

func Identity(ctx context.Context, root string) (*Manifest, error) {
	db, err := OpenReadOnly(filepath.Join(root, "control.db"))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	m := &Manifest{Version: 1, Files: map[string]Entry{}}
	err = db.QueryRowContext(ctx, "SELECT bucket,lakehouse,start,frontier,generation,metadata FROM config WHERE id=1").Scan(&m.Bucket, &m.Lakehouse, &m.Start, &m.Frontier, &m.Generation, &m.Metadata)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT key,value FROM options")
	if err != nil {
		return nil, err
	}
	opts := map[string]string{}
	for rows.Next() {
		var k, v string
		if err = rows.Scan(&k, &v); err != nil {
			rows.Close()
			return nil, err
		}
		opts[k] = v
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	m.Namespace, m.Table = opts["table.namespace"], opts["table.name"]
	m.BootstrapLakehouse, m.BootstrapMetadata, m.BootstrapBefore = opts["bootstrap.lakehouse"], opts["bootstrap.metadata"], opts["bootstrap.before"]
	m.SourceBucketID, m.SourceOrg = opts["source.bucket_id"], opts["source.org"]
	if m.Namespace == "" || m.Table == "" {
		return nil, errors.New("state lacks table identity")
	}
	if err = db.QueryRowContext(ctx, "SELECT coalesce(max(seq),0) FROM inbox").Scan(&m.InboxMaxSeq); err != nil {
		return nil, err
	}
	if _, err = os.Stat(filepath.Join(root, "catalog.db")); err == nil {
		cat, e := OpenReadOnly(filepath.Join(root, "catalog.db"))
		if e != nil {
			return nil, e
		}
		defer cat.Close()
		var loc sql.NullString
		e = cat.QueryRowContext(ctx, "SELECT metadata_location FROM iceberg_tables WHERE catalog_name='sage' AND table_namespace=? AND table_name=?", m.Namespace, m.Table).Scan(&loc)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
		m.CatalogMetadata = loc.String
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if m.Metadata != "" && m.CatalogMetadata == "" {
		return nil, errors.New("routed state lacks catalog")
	}
	return m, nil
}

func Digest(path string) (string, error) { return DigestContext(context.Background(), path) }
func DigestContext(ctx context.Context, path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	_, e = io.Copy(h, contextReader{ctx, f})
	return hex.EncodeToString(h.Sum(nil)), e
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

func copyFile(ctx context.Context, src, dst string, mode fs.FileMode) error {
	in, e := os.Open(src)
	if e != nil {
		return e
	}
	defer in.Close()
	out, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode.Perm())
	if e != nil {
		return e
	}
	_, e = io.Copy(out, contextReader{ctx, in})
	if e == nil {
		e = out.Sync()
	}
	closeErr := out.Close()
	if e == nil {
		e = closeErr
	}
	return e
}

func AtomicJSON(path string, v any) error {
	data, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	tmp, e := os.CreateTemp(filepath.Dir(path), ".backup-json-*")
	if e != nil {
		return e
	}
	name := tmp.Name()
	defer os.Remove(name)
	if e = tmp.Chmod(0600); e == nil {
		_, e = tmp.Write(data)
	}
	if e == nil {
		e = tmp.Sync()
	}
	ce := tmp.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	return syncDir(filepath.Dir(path))
}
func syncDir(p string) error {
	f, e := os.Open(p)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

// Capture runs only under the scheduler barrier and closed write admission.
// Read leases remain live; SQLite's read snapshot supplies a coherent cutoff.
func Capture(ctx context.Context, source, dest, id, image string) (*Manifest, error) {
	original, e := filepath.Abs(source)
	if e != nil {
		return nil, e
	}
	source, e = filepath.EvalSymlinks(source)
	if e != nil {
		return nil, e
	}
	if e = CheckPath(source); e != nil {
		return nil, e
	}
	if e = CheckPath(dest); e != nil {
		return nil, e
	}
	dest, e = ResolvePath(dest)
	if e != nil {
		return nil, e
	}
	if e = Disjoint(source, dest); e != nil {
		return nil, e
	}
	if e = os.Mkdir(dest, 0700); e != nil {
		return nil, e
	}
	// Capacity estimate is conservative: include WAL bytes and reserve room for
	// the immutable copy plus archive, with a 64 MiB floor for metadata.
	var size int64
	e = filepath.WalkDir(source, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in state: %s", p)
		}
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	var stat syscall.Statfs_t
	if e = syscall.Statfs(filepath.Dir(dest), &stat); e != nil {
		return nil, e
	}
	if uint64(size)*2+64*1024*1024 > uint64(stat.Bavail)*uint64(stat.Bsize) {
		return nil, errors.New("insufficient backup scratch capacity")
	}
	for _, name := range []string{"control.db", "catalog.db"} {
		if _, e = os.Stat(filepath.Join(source, name)); errors.Is(e, fs.ErrNotExist) && name == "catalog.db" {
			continue
		} else if e != nil {
			return nil, e
		}
		if e = SnapshotDB(ctx, filepath.Join(source, name), filepath.Join(dest, name)); e != nil {
			return nil, e
		}
	}
	m, e := Identity(ctx, dest)
	if e != nil {
		return nil, e
	}
	m.ID, m.StatePath, m.Image, m.CreatedAt = id, original, image, time.Now().UTC()
	m.Directories = map[string]uint32{}
	info, e := os.Stat(source)
	if e != nil {
		return nil, e
	}
	owner := info.Sys().(*syscall.Stat_t)
	m.UID, m.GID = int(owner.Uid), int(owner.Gid)
	skip := func(rel string) bool {
		return rel == ManifestName || rel == "writer.lock" || rel == "backup-status.json" || rel == PendingName || rel == "recovery-approved.json" || rel == "control.db" || rel == "catalog.db" || strings.HasPrefix(rel, "control.db-") || strings.HasPrefix(rel, "catalog.db-")
	}
	e = filepath.WalkDir(source, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(source, p)
		if err != nil {
			return err
		}
		if rel == "." || skip(rel) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		target := filepath.Join(dest, rel)
		if d.IsDir() {
			return os.Mkdir(target, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular recovery file: %s", rel)
		}
		return copyFile(ctx, p, target, info.Mode())
	})
	if e != nil {
		return nil, e
	}
	e = filepath.WalkDir(dest, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			rel, e := filepath.Rel(dest, p)
			if e != nil {
				return e
			}
			if rel != "." {
				info, e := d.Info()
				if e != nil {
					return e
				}
				m.Directories[filepath.ToSlash(rel)] = uint32(info.Mode().Perm())
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		hash, err := DigestContext(ctx, p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dest, p)
		if err != nil {
			return err
		}
		m.Files[filepath.ToSlash(rel)] = Entry{hash, info.Size(), uint32(info.Mode().Perm())}
		return nil
	})
	if e != nil {
		return nil, e
	}
	for _, name := range []string{"control.db", "catalog.db"} {
		if _, ok := m.Files[name]; ok {
			if e = Integrity(ctx, filepath.Join(dest, name)); e != nil {
				return nil, e
			}
		}
	}
	if e = AtomicJSON(filepath.Join(dest, ManifestName), m); e != nil {
		return nil, e
	}
	if e = filepath.WalkDir(dest, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return syncDir(p)
		}
		return nil
	}); e != nil {
		return nil, e
	}
	return m, syncDir(filepath.Dir(dest))
}

func Load(root string) (*Manifest, error) {
	if e := CheckPath(root); e != nil {
		return nil, e
	}
	data, e := os.ReadFile(filepath.Join(root, ManifestName))
	if e != nil {
		return nil, e
	}
	var m Manifest
	if e = json.Unmarshal(data, &m); e != nil {
		return nil, e
	}
	if m.Version != 1 || m.ID == "" || m.Files == nil {
		return nil, errors.New("invalid backup manifest")
	}
	return &m, nil
}
func SafeName(name string) bool {
	return name != "" && name != "." && !strings.Contains(name, "\\") && !strings.HasPrefix(name, "/") && filepath.ToSlash(filepath.Clean(name)) == name && name != ".." && !strings.HasPrefix(name, "../")
}
func Verify(ctx context.Context, root string) (*Manifest, error) {
	root, e := filepath.EvalSymlinks(root)
	if e != nil {
		return nil, e
	}
	m, e := Load(root)
	if e != nil {
		return nil, e
	}
	if len(m.Files) == 0 {
		return nil, errors.New("empty backup")
	}
	if _, ok := m.Files["control.db"]; !ok {
		return nil, errors.New("missing control database")
	}
	for name, mode := range m.Directories {
		if !SafeName(name) || mode > 0777 {
			return nil, errors.New("unsafe directory inventory")
		}
		p := filepath.Join(root, filepath.FromSlash(name))
		actual, err := filepath.EvalSymlinks(p)
		if err != nil {
			return nil, err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return nil, err
		}
		if actual != p || !info.IsDir() {
			return nil, errors.New("backup directory differs")
		}
	}
	for name, entry := range m.Files {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		if !SafeName(name) {
			return nil, errors.New("unsafe manifest path")
		}
		p := filepath.Join(root, filepath.FromSlash(name))
		resolved, err := filepath.EvalSymlinks(p)
		if err != nil {
			return nil, err
		}
		if resolved != p {
			return nil, errors.New("links in backup are forbidden")
		}
		info, err := os.Lstat(p)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() != entry.Size {
			return nil, fmt.Errorf("backup file differs: %s", name)
		}
		hash, err := DigestContext(ctx, p)
		if err != nil {
			return nil, err
		}
		if hash != entry.SHA256 {
			return nil, fmt.Errorf("backup checksum differs: %s", name)
		}
	}
	for _, name := range []string{"control.db", "catalog.db"} {
		if _, ok := m.Files[name]; ok {
			if e = Integrity(ctx, filepath.Join(root, name)); e != nil {
				return nil, e
			}
		}
	}
	saved, e := Identity(ctx, root)
	if e != nil {
		return nil, e
	}
	if saved.BootstrapLakehouse != m.BootstrapLakehouse || saved.BootstrapMetadata != m.BootstrapMetadata || saved.BootstrapBefore != m.BootstrapBefore || saved.Bucket != m.Bucket || saved.Lakehouse != m.Lakehouse || saved.Namespace != m.Namespace || saved.Table != m.Table || saved.Frontier != m.Frontier || saved.Generation != m.Generation || saved.Metadata != m.Metadata || saved.CatalogMetadata != m.CatalogMetadata || saved.Start != m.Start || saved.InboxMaxSeq != m.InboxMaxSeq || saved.SourceBucketID != m.SourceBucketID || saved.SourceOrg != m.SourceOrg {
		return nil, errors.New("manifest identity differs from database snapshot")
	}
	return m, nil
}
