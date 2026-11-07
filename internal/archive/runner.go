// Package archive advances durable jobs one recoverable stage at a time.
package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	"beehive-hot-to-cold-loader/internal/observability"
)

// Worker is the storage side of one job step. The runner decides when a step
// may run; the worker does not advance the durable state itself.
// Worker is the storage side of one job step. The runner decides when a step
// may run; the worker does not advance the durable state itself.
type Worker interface {
	Export(context.Context, *control.Job) error           // exports the job
	Reconcile(context.Context, *control.Job) error        // reconciles the job
	Build(context.Context, *control.Job) error            // builds the job
	Commit(context.Context, *control.Job) (string, error) // commits the job
	Verify(context.Context, *control.Job) error           // verifies the job
	Delete(context.Context, *control.Job) error           // deletes the job
	ConfirmEmpty(context.Context, *control.Job) error     // confirms the job is empty
}

// Runner walks a job through FENCING → COMPLETE. A failed step is saved and
// retried in place, except where the case below rewinds to a safe earlier state.
// Runner is the runner for the archive.
type Runner struct {
	Store         *control.Store // the store
	Worker        Worker         // the worker
	Root          string         // the root
	Retention     time.Duration  // the retention
	DeleteEnabled bool           // whether deletion is enabled
	KeepStaging   bool           // whether to keep staging
	ProfileSteps  bool           // whether to profile steps
	Metrics       *observability.Metrics
}

// ErrWaiting means the step is blocked on a writer, a reader, or deletion
// still being disabled. It is not a failed attempt and is not profiled.
var ErrWaiting = errors.New("waiting for grants or deletion enablement") // the error waiting for grants or deletion enablement

// Step performs the work for job.State and persists the next state only after
// that work succeeds. Callers may repeat Step after a crash; completed external
// effects are recognized by the worker rather than repeated blindly.
func (r *Runner) Step(ctx context.Context, job *control.Job) (result error) {
	finish := r.Metrics.Start(job.Mode, job.State)
	defer func() { finish(result, errors.Is(result, ErrWaiting)) }()
	if r.ProfileSteps {
		began, state, usage := time.Now(), job.State, readStepUsage()
		defer func() {
			// Profiling cannot alter a durable state transition. The benchmark rejects
			// missing profiles independently, rather than retrying a completed operation.
			if e := profileStep(job, state, began, usage, result); e != nil {
				log.Printf("step profile job %s: %v", job.ID, e)
			}
		}()
	}
	if job.Work == "" {
		job.Work = filepath.Join(r.Root, job.ID)
		if e := os.MkdirAll(job.Work, 0700); e != nil {
			return e
		}
		for _, directory := range []string{job.Work, r.Root, filepath.Dir(r.Root)} {
			f, e := os.Open(directory)
			if e != nil {
				return e
			}
			e = f.Sync()
			f.Close()
			if e != nil {
				return e
			}
		}
		if e := r.Store.Save(job); e != nil {
			return e
		}
	}
	var e error
	next := job.State
	switch job.State {
	case "FENCING":
		var writers int
		writers, e = r.Store.Writers(job.Day)
		if e == nil && writers > 0 {
			return ErrWaiting
		}
		next = "EXPORTING"
	case "EXPORTING":
		e = r.Worker.Export(ctx, job)
		next = "EXPORTED"
	case "EXPORTED":
		e = r.Worker.Reconcile(ctx, job)
		next = "RECONCILED"
		if e != nil && !errors.Is(e, control.ErrStagingCapacity) {
			// Keep the fence, but obtain a new engine export on retry.
			job.State = "EXPORTING"
		}
	case "RECONCILED":
		e = r.Worker.Build(ctx, job)
		if errors.Is(e, control.ErrExportNeedsReconcile) {
			job.State = "EXPORTED"
		}
		next = "BUILT"
	case "BUILT":
		job.Metadata, e = r.Worker.Commit(ctx, job)
		next = "COMMITTED"
	case "COMMITTED":
		e = r.Worker.Verify(ctx, job)
		next = "REMOTE_VERIFIED"
	case "REMOTE_VERIFIED":
		return r.Store.Activate(job)
	case "ROUTED":
		if job.Mode == "late" {
			return r.finish(job)
		}
		if !r.DeleteEnabled {
			return ErrWaiting
		}
		var readers int
		readers, e = r.Store.OldReaders(job.Day)
		if e == nil && readers > 0 {
			return ErrWaiting
		}
		next = "DELETING"
	case "DELETING":
		if !r.DeleteEnabled {
			return ErrWaiting
		}
		// Reverify on every resumed destructive attempt, including the first.
		e = r.Worker.Verify(ctx, job)
		if e == nil {
			e = r.Worker.Delete(ctx, job)
		}
		next = "DELETE_REQUESTED"
	case "DELETE_REQUESTED":
		e = r.Worker.ConfirmEmpty(ctx, job)
		if e == nil {
			return r.finish(job)
		}
		// Retry a partial/ambiguous delete without rebuilding or recommitting.
		job.State = "DELETING"
		job.Error = e.Error()
		if save := r.Store.Save(job); save != nil {
			return save
		}
		return e
	case "COMPLETE":
		return nil
	default:
		return fmt.Errorf("unknown archival state %q", job.State)
	}
	if e != nil {
		job.Error = e.Error()
		if save := r.Store.Save(job); save != nil {
			return save
		}
		return e
	}
	job.Error = ""
	job.State = next
	return r.Store.Save(job)
}

// finish marks the job COMPLETE, then removes transient staging.
func (r *Runner) finish(job *control.Job) error {
	if e := r.Store.Finish(job); e != nil {
		return e
	}
	return r.cleanup(job)
}

// cleanup deletes known scratch names inside the job directory. It refuses a
// work path that is not that job's private directory.
func (r *Runner) cleanup(job *control.Job) error {
	if r.KeepStaging || job.Work == "" {
		return nil
	}
	expected := filepath.Join(r.Root, job.ID)
	if filepath.Clean(job.Work) != filepath.Clean(expected) {
		return errors.New("job staging is outside its private job directory")
	}
	names := []string{"export.parquet", "export-dedup.parquet", "export-dedup.parquet.sort.parquet.pending", "influx.parquet", "after-delete.parquet", "export.ndjson", "export-dedup.ndjson", "export-dedup.ndjson.sort.parquet.pending", "influx.ndjson", "expected.parquet", "batch.parquet", "late.ndjson", "after-delete.ndjson"}
	for _, remote := range job.Files {
		base := filepath.Base(remote)
		if strings.HasPrefix(base, job.ID+"-") && strings.HasSuffix(base, ".parquet") {
			names = append(names, base)
		}
	}
	for _, name := range names {
		for _, suffix := range []string{"", ".pending"} {
			if e := os.Remove(filepath.Join(job.Work, name+suffix)); e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
	}
	return nil
}

// CleanupCompleted also recovers a crash after the durable completion marker.
func (r *Runner) CleanupCompleted() error {
	rows, e := r.Store.DB.Query("SELECT payload FROM jobs WHERE state='COMPLETE'")
	if e != nil {
		return e
	}
	var jobs []control.Job
	for rows.Next() {
		var raw string
		if e = rows.Scan(&raw); e != nil {
			rows.Close()
			return e
		}
		var job control.Job
		if e = json.Unmarshal([]byte(raw), &job); e != nil {
			rows.Close()
			return e
		}
		jobs = append(jobs, job)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, job := range jobs {
		if e = r.cleanup(&job); e != nil {
			return e
		}
	}
	return nil
}
