// runner_test.go drives the archival state machine with a fake worker. Deletion
// waits for remote verification and a reader drain, and a failed step keeps the fence.

package archive

import (
	"beehive-hot-to-cold-loader/internal/control"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeWorker is a fake worker for testing.
type fakeWorker struct {
	fail  string   // the name of the method to fail
	calls []string // the names of the methods called
}

// call calls a method and returns an error.
func (w *fakeWorker) call(name string) error {
	w.calls = append(w.calls, name)
	if w.fail == name {
		return errors.New(name)
	}
	return nil
}

// Export exports a job.
func (w *fakeWorker) Export(context.Context, *control.Job) error    { return w.call("export") }
func (w *fakeWorker) Reconcile(context.Context, *control.Job) error { return w.call("reconcile") }

// Build builds a job.
func (w *fakeWorker) Build(_ context.Context, job *control.Job) error {
	job.Files = []string{"file"}
	return w.call("build")
}

// Commit commits a job.
func (w *fakeWorker) Commit(context.Context, *control.Job) (string, error) {
	return "snapshot", w.call("commit")
}

// Verify verifies a job.
func (w *fakeWorker) Verify(context.Context, *control.Job) error { return w.call("verify") }

// Delete deletes a job.
func (w *fakeWorker) Delete(context.Context, *control.Job) error { return w.call("delete") }

// ConfirmEmpty confirms that a job is empty.
func (w *fakeWorker) ConfirmEmpty(context.Context, *control.Job) error { return w.call("empty") }

// setup sets up a runner for testing.
func setup(t *testing.T) (*Runner, *control.Job, *fakeWorker) {
	s, e := control.Open(filepath.Join(t.TempDir(), "db"), "waggle", "warehouse", "2025-03-01")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	job, e := s.Next(time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC), 720*time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	worker := &fakeWorker{}
	return &Runner{Store: s, Worker: worker, Root: t.TempDir(), DeleteEnabled: true}, job, worker
}

// until waits until a job reaches a given state.
func until(t *testing.T, r *Runner, job *control.Job, state string) {
	t.Helper()
	for i := 0; i < 20 && job.State != state; i++ {
		if e := r.Step(context.Background(), job); e != nil {
			t.Fatal(job.State, e)
		}
	}
	if job.State != state {
		t.Fatal(job.State, state)
	}
}

// TestNoDeleteBeforeRemoteVerificationAndReadDrain tests that a job is not deleted before remote verification and read drain.
func TestNoDeleteBeforeRemoteVerificationAndReadDrain(t *testing.T) {
	r, job, w := setup(t)
	r.Store.View("old-api", "read")
	until(t, r, job, "ROUTED")
	if e := r.Step(context.Background(), job); !errors.Is(e, ErrWaiting) {
		t.Fatal(e)
	}
	for _, name := range w.calls {
		if name == "delete" {
			t.Fatal("deleted with active old reader")
		}
	}
	r.Store.ReleaseRead("read")
	until(t, r, job, "COMPLETE")
	if got := w.calls[len(w.calls)-3:]; got[0] != "verify" || got[1] != "delete" || got[2] != "empty" {
		t.Fatal(got)
	}
}

// TestVerificationFailureNeverActivatesOrDeletes tests that a job is not activated or deleted after verification failure.
func TestVerificationFailureNeverActivatesOrDeletes(t *testing.T) {
	r, job, w := setup(t)
	w.fail = "verify"
	until(t, r, job, "COMMITTED")
	if e := r.Step(context.Background(), job); e == nil {
		t.Fatal("verification passed")
	}
	var frontier int64
	r.Store.DB.QueryRow("SELECT frontier FROM config").Scan(&frontier)
	if control.Day(frontier) != "2025-03-01" {
		t.Fatal(frontier)
	}
	if job.State != "COMMITTED" {
		t.Fatal(job.State)
	}
}

// TestReconcileMismatchReexportsUnderSameFence tests that a job is reexported under the same fence after a reconciliation mismatch.
func TestReconcileMismatchReexportsUnderSameFence(t *testing.T) {
	r, job, w := setup(t)
	until(t, r, job, "EXPORTED")
	w.fail = "reconcile"
	if e := r.Step(context.Background(), job); e == nil || job.State != "EXPORTING" {
		t.Fatal(job.State, e)
	}
	if n, _ := r.Store.Permit("blocked", "loader", []string{job.Day}); len(n) != 0 {
		t.Fatal(n)
	}
}

// capacityWorker is a worker that returns an error indicating that the staging capacity has been exhausted.
type capacityWorker struct{ Worker }

// Reconcile reconciles a job.
func (capacityWorker) Reconcile(context.Context, *control.Job) error {
	return control.ErrStagingCapacity
}

// TestReconcileCapacityKeepsExportAndFence tests that a job is kept exported and fenced after a reconciliation capacity error.
func TestReconcileCapacityKeepsExportAndFence(t *testing.T) {
	r, job, _ := setup(t)
	until(t, r, job, "EXPORTED")
	r.Worker = capacityWorker{r.Worker}
	for n := 0; n < 2; n++ {
		if e := r.Step(context.Background(), job); !errors.Is(e, control.ErrStagingCapacity) || job.State != "EXPORTED" {
			t.Fatal(job.State, e)
		}
	}
	if days, _ := r.Store.Permit("blocked", "loader", []string{job.Day}); len(days) != 0 {
		t.Fatal("capacity failure released fence", days)
	}
}

// TestDeleteDisabledAndRetryWithoutRecommit tests that a job is not deleted and retried without a recommit after deletion is disabled.
func TestDeleteDisabledAndRetryWithoutRecommit(t *testing.T) {
	r, job, w := setup(t)
	r.DeleteEnabled = false
	until(t, r, job, "ROUTED")
	if e := r.Step(context.Background(), job); !errors.Is(e, ErrWaiting) {
		t.Fatal(e)
	}
	r.DeleteEnabled = true
	w.fail = "empty"
	until(t, r, job, "DELETE_REQUESTED")
	if e := r.Step(context.Background(), job); e == nil || job.State != "DELETING" {
		t.Fatal(job.State, e)
	}
	w.fail = ""
	until(t, r, job, "COMPLETE")
	commits := 0
	for _, c := range w.calls {
		if c == "commit" {
			commits++
		}
	}
	if commits != 1 {
		t.Fatal(commits)
	}
}

// TestStagingCleanupOnlyAfterDurableCompletion tests that the staging directory is cleaned up only after the job is completed.
func TestStagingCleanupOnlyAfterDurableCompletion(t *testing.T) {
	r, job, _ := setup(t)
	until(t, r, job, "ROUTED")
	path := filepath.Join(job.Work, "export.ndjson")
	transient := []string{path, filepath.Join(job.Work, "export-dedup.ndjson"), filepath.Join(job.Work, "export-dedup.ndjson.sort.parquet.pending")}
	for _, p := range transient {
		if e := os.WriteFile(p, []byte("evidence"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	evidence := filepath.Join(job.Work, "verification.json")
	os.WriteFile(evidence, []byte("{}"), 0600)
	if _, e := os.Stat(path); e != nil {
		t.Fatal(e)
	}
	until(t, r, job, "COMPLETE")
	for _, p := range transient {
		if _, e := os.Stat(p); !errors.Is(e, os.ErrNotExist) {
			t.Fatal(p, e)
		}
	}
	if _, e := os.Stat(evidence); e != nil {
		t.Fatal(e)
	}
}

// TestMissingNormalizedExportRewindsUnderFence tests that a job is reexported under the same fence after a reconciliation mismatch.
func TestMissingNormalizedExportRewindsUnderFence(t *testing.T) {
	r, job, _ := setup(t)
	until(t, r, job, "RECONCILED")
	r.Worker = needsReconcileWorker{r.Worker}
	if e := r.Step(context.Background(), job); !errors.Is(e, control.ErrExportNeedsReconcile) || job.State != "EXPORTED" {
		t.Fatal(job.State, e)
	}
	if days, e := r.Store.Permit("new", "loader", []string{job.Day}); e != nil || len(days) != 0 {
		t.Fatal(days, e)
	}
}

// needsReconcileWorker is a worker that returns an error indicating that the export needs reconciliation.
type needsReconcileWorker struct{ Worker }

// Build builds a job.
func (w needsReconcileWorker) Build(context.Context, *control.Job) error {
	return control.ErrExportNeedsReconcile
}
