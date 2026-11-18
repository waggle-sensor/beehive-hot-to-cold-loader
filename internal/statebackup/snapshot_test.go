package statebackup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func fixture(t *testing.T) (string, func()) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "state")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	db, e := sql.Open("sqlite", filepath.Join(root, "control.db"))
	if e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE config(id INTEGER,bucket TEXT,lakehouse TEXT,start INTEGER,frontier INTEGER,generation INTEGER,metadata TEXT)`,
		`INSERT INTO config VALUES(1,'waggle','https://test/sage/cold-test-backup/lakehouse',1,2,3,'')`,
		`CREATE TABLE options(key TEXT,value TEXT)`,
		`INSERT INTO options VALUES('table.namespace','sage'),('table.name','observations'),('source.bucket_id','bucket-id'),('source.org','waggle')`,
		`CREATE TABLE inbox(seq INTEGER,payload TEXT)`,
		`INSERT INTO inbox VALUES(7,'late')`,
	} {
		if _, e = db.Exec(q); e != nil {
			t.Fatal(e)
		}
	}
	// Keep WAL live throughout capture; closing first would hide a WAL-copy bug.
	t.Cleanup(func() { db.Close() })
	if e = os.MkdirAll(filepath.Join(root, "jobs", "job1"), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "jobs", "job1", "proof.json"), []byte("proof"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Join(root, "jobs", "empty-pending-job"), 0700); e != nil {
		t.Fatal(e)
	}
	return root, func() { db.Close() }
}
func take(t *testing.T, root string) string {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "snapshot")
	_, e := Capture(context.Background(), root, dest, "fixture", "image@sha256:test")
	if e != nil {
		t.Fatal(e)
	}
	return dest
}

type fakeRemote struct{ err error }

func (f fakeRemote) References(context.Context, *Manifest, string) (map[string]string, error) {
	return nil, f.err
}
func (f fakeRemote) CheckReferences(context.Context, *Manifest, map[string]string) error {
	return f.err
}
func (f fakeRemote) Publish(context.Context, string, *Manifest) error { return f.err }

func TestWALSnapshotAndCompleteRecoveryFiles(t *testing.T) {
	root, stop := fixture(t)
	defer stop()
	dest := take(t, root)
	m, e := Verify(context.Background(), dest)
	if e != nil {
		t.Fatal(e)
	}
	if m.InboxMaxSeq != 7 || m.Generation != 3 || m.StatePath != root || m.SourceBucketID != "bucket-id" {
		t.Fatal("snapshot cutoff/identity differs")
	}
	if _, ok := m.Files["jobs/job1/proof.json"]; !ok {
		t.Fatal("missing recovery proof")
	}
	db, e := OpenReadOnly(filepath.Join(dest, "control.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var payload string
	if e = db.QueryRow("SELECT payload FROM inbox WHERE seq=7").Scan(&payload); e != nil || payload != "late" {
		t.Fatal(payload, e)
	}
	// Later live writes do not move the captured cutoff.
	live, e := sql.Open("sqlite", filepath.Join(root, "control.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer live.Close()
	if _, e = live.Exec("INSERT INTO inbox VALUES(8,'later')"); e != nil {
		t.Fatal(e)
	}
	if _, e = Verify(context.Background(), dest); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dest, "jobs/job1/proof.json"), []byte("changed"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = Verify(context.Background(), dest); e == nil {
		t.Fatal("corruption accepted")
	}
}
func TestRestoreFailClosedAndExplicitRemoteApproval(t *testing.T) {
	root, stop := fixture(t)
	defer stop()
	dest := take(t, root)
	bundle := filepath.Join(t.TempDir(), "state.tar.gz")
	if e := Pack(context.Background(), dest, bundle); e != nil {
		t.Fatal(e)
	}
	// Original is stopped and moved aside, not opened concurrently with restoration.
	stop()
	old := root + "-old"
	if e := os.Rename(root, old); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	if e := Restore(context.Background(), bundle, root); e != nil {
		t.Fatal(e)
	}
	if e := StartupCheck(root); e == nil {
		t.Fatal("unapproved restore starts")
	}
	if e := Approve(context.Background(), root, fakeRemote{}, false); e == nil {
		t.Fatal("loss acknowledgement missing")
	}
	if e := Approve(context.Background(), root, fakeRemote{errors.New("newer remote history")}, true); e == nil {
		t.Fatal("stale backup approved")
	}
	if e := StartupCheck(root); e == nil {
		t.Fatal("failed approval cleared marker")
	}
	if e := Approve(context.Background(), root, fakeRemote{}, true); e != nil {
		t.Fatal(e)
	}
	if e := StartupCheck(root); e != nil {
		t.Fatal(e)
	}
	if info, e := os.Stat(filepath.Join(root, "jobs", "empty-pending-job")); e != nil || !info.IsDir() {
		t.Fatal("empty job directory was lost", e)
	}

	if b, e := os.ReadFile(filepath.Join(root, "jobs/job1/proof.json")); e != nil || string(b) != "proof" {
		t.Fatal(e)
	}
}
func TestArchiveTraversalAndNonemptyTargetRejected(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "evil.tar.gz")
	f, e := os.Create(bundle)
	if e != nil {
		t.Fatal(e)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	if e = tw.WriteHeader(&tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Size: 1, Mode: 0600}); e != nil {
		t.Fatal(e)
	}
	tw.Write([]byte("x"))
	tw.Close()
	gz.Close()
	f.Close()
	root := filepath.Join(t.TempDir(), "target")
	os.Mkdir(root, 0700)
	if e = Restore(context.Background(), bundle, root); e == nil {
		t.Fatal("traversal accepted")
	}
	if e = StartupCheck(root); e == nil {
		t.Fatal("partial restore not blocked")
	}
	if _, e = os.Stat(filepath.Join(filepath.Dir(root), "escape")); !os.IsNotExist(e) {
		t.Fatal("escaped target")
	}
	if e = Restore(context.Background(), bundle, root); e == nil {
		t.Fatal("nonempty restore accepted")
	}
}
func TestScratchPathsAndLinks(t *testing.T) {
	if e := Disjoint("/tmp/state", "/tmp/state/scratch"); e == nil {
		t.Fatal("nested scratch accepted")
	}
	if e := Disjoint("/tmp/state", "/tmp"); e == nil {
		t.Fatal("ancestor scratch accepted")
	}
	if e := CheckPath("/node-data/backup"); e == nil {
		t.Fatal("node-data accepted")
	}
	root, stop := fixture(t)
	defer stop()
	if e := os.Symlink("/etc/passwd", filepath.Join(root, "outside")); e != nil {
		t.Fatal(e)
	}
	if _, e := Capture(context.Background(), root, filepath.Join(t.TempDir(), "snapshot"), "test", ""); e == nil {
		t.Fatal("symlink accepted")
	}
}

func TestRestoreNeverOverridesLiveWriterLock(t *testing.T) {
	root := t.TempDir()
	lock, e := lockState(root)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	if e = Restore(context.Background(), "missing-bundle", root); e == nil {
		t.Fatal("live state was accepted")
	}
	if _, e = os.Stat(filepath.Join(root, PendingName)); !os.IsNotExist(e) {
		t.Fatal("live state was modified")
	}
}
