// backup_remote_test.go exercises private-prefix publish, readback, retention,
// and restore reference checks against an in-process WebDAV stub.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	"beehive-hot-to-cold-loader/internal/statebackup"
)

// backupObjectServer is a minimal WebDAV stand-in for lakehouse + backup URLs.
type backupObjectServer struct {
	mu       sync.Mutex
	files    map[string][]byte
	deletes  []string
	corrupt  bool // flip first byte of state.tar.gz on GET
	pubReads int  // version-hint GET count for publication-check tests
}

func (s *backupObjectServer) serve(w http.ResponseWriter, req *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := req.URL.Path
	switch req.Method {
	case "MKCOL":
		w.WriteHeader(201)
	case "PUT":
		if _, ok := s.files[p]; ok {
			w.WriteHeader(412)
			return
		}
		b, e := io.ReadAll(req.Body)
		if e != nil {
			w.WriteHeader(500)
			return
		}
		s.files[p] = b
		w.WriteHeader(201)
	case "GET", "HEAD":
		b, ok := s.files[p]
		if !ok {
			w.WriteHeader(404)
			return
		}
		if strings.HasSuffix(p, "version-hint.text") {
			s.pubReads++
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		if req.Method == "GET" {
			if s.corrupt && strings.HasSuffix(p, "state.tar.gz") {
				b = append([]byte(nil), b...)
				b[0] ^= 1
			}
			w.Write(b)
		}
	case "DELETE":
		s.deletes = append(s.deletes, p)
		delete(s.files, p)
		w.WriteHeader(204)
	case "PROPFIND":
		seen := map[string]bool{}
		prefix := p + "/"
		fmt.Fprint(w, `<multistatus xmlns="DAV:">`)
		for name := range s.files {
			if strings.HasPrefix(name, prefix) {
				dir := strings.Split(strings.TrimPrefix(name, prefix), "/")[0]
				if !seen[dir] {
					fmt.Fprintf(w, "<response><href>%s%s/</href></response>", prefix, dir)
					seen[dir] = true
				}
			}
		}
		fmt.Fprint(w, "</multistatus>")
	default:
		w.WriteHeader(405)
	}
}
func newBackupServer(t *testing.T) (*BackupRemote, *backupObjectServer, *Pelican, string) {
	t.Helper()
	p, root := mockPelican(t)
	remote, e := NewBackupRemote(p.Base.String(), filepath.Join(root, "key.pem"), root, TableIdentity{}, "", 2)
	if e != nil {
		t.Fatal(e)
	}
	s := &backupObjectServer{files: map[string][]byte{}}
	remote.auth.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		s.serve(rec, req)
		return rec.Result(), nil
	})}
	remote.reader.Client = p.Client
	return remote, s, p, root
}
func remoteSnapshot(t *testing.T, remote *BackupRemote, id string) (*statebackup.Manifest, string) {
	t.Helper()
	source := t.TempDir()
	store, e := control.Open(filepath.Join(source, "control.db"), "waggle", remote.reader.Base.String(), "2025-03-01")
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	if e = remote.reader.Table.BindState(store); e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "snapshot")
	m, e := statebackup.Capture(context.Background(), source, dest, id, "test-image")
	if e != nil {
		t.Fatal(e)
	}
	return m, dest
}
func TestBackupUploadVerificationRetentionAndDownload(t *testing.T) {
	r, s, _, _ := newBackupServer(t)
	ctx := context.Background()
	var latest *statebackup.Manifest
	for i := 1; i <= 3; i++ {
		m, root := remoteSnapshot(t, r, fmt.Sprintf("backup-%d", i))
		m.CreatedAt = time.Unix(int64(i), 0).UTC()
		if e := statebackup.AtomicJSON(filepath.Join(root, statebackup.ManifestName), m); e != nil {
			t.Fatal(e)
		}
		if e := r.Publish(ctx, root, m); e != nil {
			t.Fatal(e)
		}
		latest = m
	}
	if len(s.deletes) != 3 {
		t.Fatalf("expected exactly old backup's three objects removed: %v", s.deletes)
	}
	for _, p := range s.deletes {
		if !strings.Contains(p, "/backup-1/") || strings.Contains(p, "lakehouse") {
			t.Fatal("retention escaped old backup", p)
		}
	}
	inventory, e := r.List(ctx, "waggle")
	if e != nil {
		t.Fatal(e)
	}
	if len(inventory) != 2 {
		t.Fatalf("expected discovery of two retained backups: %v", inventory)
	}
	target := filepath.Join(t.TempDir(), "download.tar.gz")
	if e := r.Download(ctx, statebackup.IdentityKey(latest), latest.ID, target); e != nil {
		t.Fatal(e)
	}
	unpacked := filepath.Join(t.TempDir(), "unpacked")
	if e := statebackup.Unpack(ctx, target, unpacked); e != nil {
		t.Fatal(e)
	}
	if _, e := statebackup.Verify(ctx, unpacked); e != nil {
		t.Fatal(e)
	}
}
func TestBackupCorruptReadbackNeverCompletesOrPrunes(t *testing.T) {
	r, s, _, _ := newBackupServer(t)
	m, root := remoteSnapshot(t, r, "broken")
	s.corrupt = true
	if e := r.Publish(context.Background(), root, m); e == nil {
		t.Fatal("corruption accepted")
	}
	for name := range s.files {
		if strings.HasSuffix(name, "COMPLETE.json") {
			t.Fatal("failed readback completed")
		}
	}
	if len(s.deletes) != 0 {
		t.Fatal("failed upload pruned backups")
	}
}
func TestBackupScopeCannotWriteLakehouse(t *testing.T) {
	r, _, _, root := newBackupServer(t)
	for _, dest := range []string{r.reader.Base.String(), "https://other/sage/cold/backups/hot-to-cold-loader", r.base + "/../lakehouse"} {
		if _, e := NewBackupRemote(r.reader.Base.String(), filepath.Join(root, "key.pem"), root, TableIdentity{}, dest, 30); e == nil {
			t.Fatal("invalid backup root accepted", dest)
		}
	}
	if _, e := r.request(context.Background(), "DELETE", r.reader.Table.Location(r.reader.Base.String())+"/data/file", nil, 0); e == nil {
		t.Fatal("backup token could delete lakehouse")
	}
}
func TestBackupRealCatalogReferencesAndStalePublication(t *testing.T) {
	r, _, p, root := newBackupServer(t)
	ctx := context.Background()
	state := filepath.Join(root, "state")
	if e := os.Mkdir(state, 0700); e != nil {
		t.Fatal(e)
	}
	c, e := OpenCatalog(ctx, filepath.Join(state, "catalog.db"), p.Base.String(), p)
	if e != nil {
		t.Fatal(e)
	}
	defer c.DB.Close()
	if e = c.Init(ctx); e != nil {
		t.Fatal(e)
	}
	store, e := control.Open(filepath.Join(state, "control.db"), "waggle", p.Base.String(), "2025-03-01")
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	if e = p.Table.BindState(store); e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "snapshot")
	m, e := statebackup.Capture(ctx, state, dest, "initial", "test")
	if e != nil {
		t.Fatal(e)
	}
	refs, e := r.References(ctx, m, dest)
	if e != nil {
		t.Fatal(e)
	}
	m.References = refs
	if len(refs) == 0 {
		t.Fatal("catalog metadata not inventoried")
	}
	if e = r.CheckReferences(ctx, m, refs); e != nil {
		t.Fatal(e)
	}
	// A later real Iceberg publication makes the older restore incompatible.
	tbl, e := c.Catalog.LoadTable(ctx, c.Identifier())
	if e != nil {
		t.Fatal(e)
	}
	if e = c.PublishHint(ctx, tbl.MetadataLocation()); e != nil {
		t.Fatal(e)
	}
	if e = r.CheckReferences(ctx, m, refs); e == nil {
		t.Fatal("newer publication accepted")
	}
	if e = r.RequireNewState(ctx); e == nil {
		t.Fatal("empty journal would hide published history")
	}

	// Metadata corruption is independently detected, even if the pointer matches.
	m.PublishedMetadata = tbl.MetadataLocation()
	bad := []byte(`{"changed":true}`)
	if e = p.WriteFile(tbl.MetadataLocation(), bad); e != nil {
		t.Fatal(e)
	}
	if e = r.CheckReferences(ctx, m, refs); e == nil {
		t.Fatal("metadata corruption accepted")
	}
}
func TestBackupRetentionRejectsInvalidCompletion(t *testing.T) {
	r, s, _, _ := newBackupServer(t)
	group := r.base + "/" + strings.Repeat("a", 32)
	u := strings.TrimPrefix(group, "https://pelican.test")
	s.files[u+"/bad/COMPLETE.json"] = []byte(`{"version":1,"id":"bad","identity":"other"}`)
	if e := r.prune(context.Background(), group, strings.Repeat("a", 32)); e == nil {
		t.Fatal("invalid retention evidence accepted")
	}
	if len(s.deletes) > 0 {
		t.Fatal("invalid listing caused deletion")
	}
}
func TestBackupMarkerDigest(t *testing.T) {
	r, s, _, _ := newBackupServer(t)
	m, root := remoteSnapshot(t, r, "marker")
	if e := r.Publish(context.Background(), root, m); e != nil {
		t.Fatal(e)
	}
	for name, raw := range s.files {
		if strings.HasSuffix(name, "COMPLETE.json") {
			var marker backupComplete
			if e := json.Unmarshal(raw, &marker); e != nil {
				t.Fatal(e)
			}
			archive := s.files[strings.TrimSuffix(name, "COMPLETE.json")+"state.tar.gz"]
			hash := sha256.Sum256(archive)
			if marker.ArchiveSHA != hex.EncodeToString(hash[:]) {
				t.Fatal("marker not bound to archive")
			}
		}
	}
	if _, e := os.Stat(filepath.Join(root, "manifest.json")); e != nil {
		t.Fatal(e)
	}
}
