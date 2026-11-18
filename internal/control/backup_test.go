// backup_test.go covers admission barriers, retention-after-verify, restart
// recovery of interrupted phases, scratch isolation, and online SQLite copy.
package control

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/statebackup"
)

// backupRemoteStub stands in for Pelican publish during unit tests.
// publishStarted is closed when Publish begins; allowPublishContinue blocks
// Publish until closed so tests can observe admission while upload runs.
type backupRemoteStub struct {
	refsErr             error
	publishErr          error
	publishStarted      chan struct{}
	allowPublishContinue chan struct{}
}

func (r *backupRemoteStub) References(context.Context, *statebackup.Manifest, string) (map[string]string, error) {
	return map[string]string{}, r.refsErr
}
func (r *backupRemoteStub) CheckReferences(context.Context, *statebackup.Manifest, map[string]string) error {
	return r.refsErr
}
func (r *backupRemoteStub) Publish(ctx context.Context, _ string, _ *statebackup.Manifest) error {
	if r.publishStarted != nil {
		close(r.publishStarted)
	}
	if r.allowPublishContinue != nil {
		select {
		case <-r.allowPublishContinue:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.publishErr != nil {
		return r.publishErr
	}
	return r.refsErr
}

func backupFixture(t *testing.T) (*BackupService, *backupRemoteStub) {
	t.Helper()
	store := testStore(t)
	if err := store.Bind("table.namespace", "sage"); err != nil {
		t.Fatal(err)
	}
	if err := store.Bind("table.name", "observations"); err != nil {
		t.Fatal(err)
	}
	var controlDBPath string
	if err := store.DB.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&controlDBPath); err != nil {
		t.Fatal(err)
	}
	remote := &backupRemoteStub{}
	return &BackupService{
		Store:          store,
		StateRoot:      filepath.Dir(controlDBPath),
		Scratch:        filepath.Join(t.TempDir(), "scratch"),
		Remote:         remote,
		Boundary:       make(chan func()),
		CaptureTimeout: time.Second,
	}, remote
}

func waitUntil(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestVerifiedBackupSurvivesRetentionFailure(t *testing.T) {
	svc, remote := backupFixture(t)
	remote.publishErr = &statebackup.RetentionError{Err: errors.New("older backup cleanup unavailable")}
	if _, err := svc.Start(); err != nil {
		t.Fatal(err)
	}
	runCapture := <-svc.Boundary
	runCapture()
	waitUntil(t, func() bool { return svc.Status()["phase"] == "complete" })
	status := svc.Status()
	if status["retention_failures_total"] != float64(1) || status["retention_error"] == "" {
		t.Fatalf("retention failure was not recorded: %v", status)
	}
	if svc.Store.AdmissionClosed() {
		t.Fatal("retention failure held ingestion admission")
	}
}

func TestBackupBoundaryAndReadAvailability(t *testing.T) {
	svc, remote := backupFixture(t)
	remote.publishStarted = make(chan struct{})
	remote.allowPublishContinue = make(chan struct{})
	if _, err := svc.Store.AcquireWrite("existing", "owner", []string{"2025-03-01"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Start(); err != nil {
		t.Fatal(err)
	}
	// Admission stays open until the scheduler yields Boundary.
	if svc.Store.AdmissionClosed() {
		t.Fatal("admission closed while waiting for worker")
	}
	runCapture := <-svc.Boundary
	go runCapture()
	waitUntil(t, svc.Store.AdmissionClosed)
	if _, err := svc.Store.View("reader", "query"); err != nil {
		t.Fatal("read acquire blocked", err)
	}
	if _, err := svc.Store.RenewRead("reader", "query"); err != nil {
		t.Fatal("read renew blocked", err)
	}
	if _, err := svc.Store.RenewWrite("owner", "existing"); err != nil {
		t.Fatal("existing writer renewal blocked", err)
	}
	if _, err := svc.Store.AcquireWrite("existing", "owner", []string{"2025-03-01"}); err != nil {
		t.Fatal("idempotent permit retry blocked", err)
	}
	if _, err := svc.Store.AcquireWrite("new", "owner", []string{"2025-03-01"}); !errors.Is(err, ErrAdmissionClosed) {
		t.Fatal(err)
	}
	if err := svc.Store.ReleaseWrite("owner", "existing"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-remote.publishStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot did not drain")
	}
	if svc.Store.AdmissionClosed() {
		t.Fatal("admission held during upload")
	}
	if _, err := svc.Store.AcquireWrite("after", "owner", []string{"2025-03-01"}); err != nil {
		t.Fatal(err)
	}
	close(remote.allowPublishContinue)
	waitUntil(t, func() bool { return svc.Status()["phase"] == "complete" })
	if _, err := os.Stat(filepath.Join(svc.Scratch, "status.json")); err != nil {
		t.Fatal(err)
	}
}

func TestBackupTimeoutAndFailureReleaseAdmission(t *testing.T) {
	svc, _ := backupFixture(t)
	svc.CaptureTimeout = 30 * time.Millisecond
	if _, err := svc.Store.AcquireWrite("existing", "owner", []string{"2025-03-01"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Start(); err != nil {
		t.Fatal(err)
	}
	runCapture := <-svc.Boundary
	runCapture()
	waitUntil(t, func() bool { return svc.Status()["phase"] == "failed" })
	if svc.Store.AdmissionClosed() {
		t.Fatal("timeout left writes closed")
	}
	if _, err := svc.Store.View("reader", "still-available"); err != nil {
		t.Fatal(err)
	}
}

func TestBackupBoundaryTimeoutDoesNotPauseService(t *testing.T) {
	svc, _ := backupFixture(t)
	svc.WaitTimeout = 5 * time.Millisecond
	if _, err := svc.Start(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return svc.Status()["phase"] == "failed" })
	if svc.Store.AdmissionClosed() {
		t.Fatal("waiting backup paused admission")
	}
	if _, err := svc.Store.AcquireWrite("new", "owner", []string{"2025-03-01"}); err != nil {
		t.Fatal(err)
	}
}

func TestBackupHTTPReadsContinueAndRolesStayIsolated(t *testing.T) {
	svc, _ := backupFixture(t)
	if _, err := svc.Store.AcquireWrite("held", "writer", []string{"2025-03-01"}); err != nil {
		t.Fatal(err)
	}
	handler := &Server{Store: svc.Store, LoaderToken: "loader", ReaderToken: "reader", AdminToken: "admin", Backups: svc}
	post := func(path, token, body string) int {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response.Code
	}

	if code := post("/v1/backups/create", "reader", "{}"); code != 401 {
		t.Fatal(code)
	}
	if code := post("/v1/backups/create", "admin", `{} {}`); code != 400 {
		t.Fatal(code)
	}
	if code := post("/v1/backups/create", "admin", `{}`); code != 202 {
		t.Fatal(code)
	}
	runCapture := <-svc.Boundary
	go runCapture()
	waitUntil(t, svc.Store.AdmissionClosed)
	for i := 0; i < 30; i++ {
		body := fmt.Sprintf(`{"bucket":"waggle","id":"read-%d","owner":"reader"}`, i)
		for _, path := range []string{"/v1/reads/acquire", "/v1/reads/renew", "/v1/reads/release"} {
			if code := post(path, "reader", body); code != 200 {
				t.Fatal(path, code)
			}
		}
	}
	for _, pair := range [][2]string{
		{"/v1/quarantine", `{"bucket":"waggle","id":"invalid","payload":"test"}`},
		{"/v1/late", `{"bucket":"waggle","id":"late","records":[{"time":1740787200000000194,"name":"t","tags":{},"kind":"int","value":"1"}]}`},
	} {
		if code := post(pair[0], "loader", pair[1]); code != 503 {
			t.Fatal(pair[0], code)
		}
	}
	if err := svc.Store.ReleaseWrite("writer", "held"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return svc.Status()["phase"] == "complete" })
}

func TestInterruptedBackupReopensAdmissionOnRestart(t *testing.T) {
	svc, _ := backupFixture(t)
	if err := os.MkdirAll(svc.Scratch, 0700); err != nil {
		t.Fatal(err)
	}
	if err := statebackup.AtomicJSON(filepath.Join(svc.Scratch, "status.json"), map[string]any{"phase": "copying", "last_success_timestamp_seconds": float64(123), "failures_total": float64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := svc.LoadStatus(); err != nil {
		t.Fatal(err)
	}
	if svc.Status()["phase"] != "failed" || svc.Status()["failures_total"] != float64(3) || svc.Store.AdmissionClosed() {
		t.Fatal(svc.Status())
	}
}

func TestNestedBackupScratchRejectedWithoutCreatingIt(t *testing.T) {
	svc, _ := backupFixture(t)
	svc.Scratch = filepath.Join(svc.StateRoot, "must-not-exist")
	if _, err := svc.Start(); err == nil {
		t.Fatal("nested scratch accepted")
	}
	if _, err := os.Stat(svc.Scratch); !os.IsNotExist(err) {
		t.Fatal("invalid scratch directory was created")
	}
}

func TestOnlineSQLiteCopyWhileReadLeasesChurn(t *testing.T) {
	svc, _ := backupFixture(t)
	// Make VACUUM INTO span multiple pages while the live WAL keeps changing.
	if _, err := svc.Store.DB.Exec("INSERT INTO quarantine(id,payload) VALUES('large',printf('%05000000d',1))"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snapshotPath := filepath.Join(t.TempDir(), "control.db")
	done := make(chan error, 1)
	go func() { done <- statebackup.SnapshotDB(ctx, filepath.Join(svc.StateRoot, "control.db"), snapshotPath) }()
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("concurrent-%d", i)
		if _, err := svc.Store.View("reader", id); err != nil {
			t.Fatal("read acquire during online copy", err)
		}
		if _, err := svc.Store.RenewRead("reader", id); err != nil {
			t.Fatal("read renew during online copy", err)
		}
		if err := svc.Store.ReleaseRead(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := statebackup.Integrity(ctx, snapshotPath); err != nil {
		t.Fatal(err)
	}
}
