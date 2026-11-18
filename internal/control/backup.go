// backup.go runs online state backups without stopping coordination HTTP.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"beehive-hot-to-cold-loader/internal/observability"
	"beehive-hot-to-cold-loader/internal/statebackup"
	"github.com/google/uuid"
)

// ErrBackupUnavailable means scratch, state root, remote destination, or the
// scheduler handoff channel is not configured.
var ErrBackupUnavailable = errors.New("backup destination and disjoint scratch storage must be configured")

// BackupService captures SQLite/recovery files while the scheduler is idle.
// Capture runs on the scheduler goroutine via Boundary so no archival worker
// step can overlap the admission barrier. HTTP coordination stays on its own
// server goroutines; reads and existing writer renewals continue.
type BackupService struct {
	Store  *Store
	Remote statebackup.Remote // private Pelican backup prefix; nil disables creation

	// Scratch is HOT_TO_COLD_BACKUP_SCRATCH: attempt dirs and status.json.
	// StateRoot is the durable journal/catalog mount (HOT_TO_COLD_STATE).
	// Image is recorded into the manifest (HOT_TO_COLD_BACKUP_IMAGE).
	Scratch, StateRoot, Image string

	// Parent cancels in-flight backup work on daemon shutdown.
	Parent context.Context
	// Boundary hands the drain+copy closure to the scheduler loop.
	Boundary chan func()
	// WaitTimeout bounds waiting for that handoff (default 30m).
	// CaptureTimeout bounds drain+copy after handoff (default 10m).
	WaitTimeout, CaptureTimeout time.Duration

	mu     sync.Mutex
	status map[string]any // latest operation; mirrored to Scratch/status.json
	busy   bool           // true while Start's goroutine is still running
}

// Status returns a copy of the latest backup operation fields.
func (b *BackupService) Status() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]any{}
	for k, v := range b.status {
		out[k] = v
	}
	return out
}

// update writes status.json to scratch only. It is not part of recovery snapshots
func (b *BackupService) update(values map[string]any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.status == nil {
		b.status = map[string]any{}
	}
	for k, v := range values {
		b.status[k] = v
	}
	// status.json lives on scratch only. It is not part of recovery snapshots
	// and is never written under the journal/catalog connection locks.
	if b.Scratch != "" {
		if err := statebackup.AtomicJSON(filepath.Join(b.Scratch, "status.json"), b.status); err != nil {
			b.status["status_persistence_error"] = err.Error()
		}
	}
}

// LoadStatus reloads scratch status.json after restart. An unfinished phase is
// marked failed so an abandoned admission barrier cannot survive the process.
func (b *BackupService) LoadStatus() error {
	if b.Scratch == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(b.Scratch, "status.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var status map[string]any
	if err = json.Unmarshal(data, &status); err != nil {
		return err
	}
	b.mu.Lock()
	b.status = status
	b.mu.Unlock()
	phase, _ := status["phase"].(string)
	if phase != "" && phase != "complete" && phase != "failed" {
		n, _ := status["failures_total"].(float64)
		b.update(map[string]any{"failures_total": n + 1, "barrier_started_at": "", "phase": "failed", "error": "backup interrupted by daemon restart", "completed_at": time.Now().UTC().Format(time.RFC3339Nano)})
	}
	return nil
}

// Start accepts one backup. Capture begins only after the scheduler receives
// the Boundary task. Returns the operation id and HTTP 202 semantics to callers.
func (b *BackupService) Start() (string, error) {
	if b.Scratch == "" || b.StateRoot == "" || b.Remote == nil || b.Boundary == nil {
		return "", ErrBackupUnavailable
	}
	state, err := filepath.EvalSymlinks(b.StateRoot)
	if err != nil {
		return "", err
	}
	scratch, err := statebackup.ResolvePath(b.Scratch)
	if err != nil {
		return "", err
	}
	if err = statebackup.Disjoint(state, scratch); err != nil {
		return "", err
	}
	if err = os.MkdirAll(scratch, 0700); err != nil {
		return "", err
	}
	b.mu.Lock()
	if b.busy {
		b.mu.Unlock()
		return "", errors.New("backup already running")
	}
	b.busy = true
	b.mu.Unlock()
	id := time.Now().UTC().Format("20060102T150405Z") + "-" + uuid.NewString()
	b.update(map[string]any{"id": id, "phase": "waiting_boundary", "started_at": time.Now().UTC().Format(time.RFC3339Nano), "error": ""})
	go b.run(id, scratch)
	return id, nil
}

// run executes one backup: wait for scheduler → drain writers → copy → upload.
func (b *BackupService) run(id, scratchRoot string) {
	defer func() { b.mu.Lock(); b.busy = false; b.mu.Unlock() }()
	parent := b.Parent
	if parent == nil {
		parent = context.Background()
	}
	boundaryWait := b.WaitTimeout
	if boundaryWait == 0 {
		boundaryWait = 30 * time.Minute
	}
	captureLimit := b.CaptureTimeout
	if captureLimit == 0 {
		captureLimit = 10 * time.Minute
	}
	startedAt := time.Now()
	fail := func(err error) {
		status := b.Status()
		n, _ := status["failures_total"].(float64)
		b.update(map[string]any{"phase": "failed", "error": err.Error(), "failures_total": n + 1, "completed_at": time.Now().UTC().Format(time.RFC3339Nano)})
	}
	defer func() {
		if recover() != nil {
			fail(errors.New("backup operation panicked"))
		}
	}()
	attemptDir := filepath.Join(scratchRoot, id)
	if err := os.Mkdir(attemptDir, 0700); err != nil {
		fail(err)
		return
	}
	snapshotDir := filepath.Join(attemptDir, "snapshot")
	result := make(chan error, 1)
	var manifest *statebackup.Manifest
	// captureOnBoundary runs exclusively on the scheduler goroutine.
	captureOnBoundary := func() {
		defer func() {
			if recover() != nil {
				b.Store.OpenAdmission()
				result <- errors.New("backup capture panicked; admission reopened")
			}
		}()
		ctx, cancel := context.WithTimeout(parent, captureLimit)
		defer cancel()
		var err error
		func() {
			b.Store.CloseAdmission()
			defer b.Store.OpenAdmission()
			b.update(map[string]any{"phase": "draining_writers", "barrier_started_at": time.Now().UTC().Format(time.RFC3339Nano)})
			barrierStart := time.Now()
			defer func() {
				b.update(map[string]any{"barrier_seconds": time.Since(barrierStart).Seconds(), "barrier_started_at": ""})
			}()
			drainStarted := time.Now()
			err = b.Store.DrainWrites(ctx)
			b.update(map[string]any{"drain_seconds": time.Since(drainStarted).Seconds()})
			if err != nil {
				return
			}
			b.update(map[string]any{"phase": "copying"})
			copyStarted := time.Now()
			defer func() { b.update(map[string]any{"capture_seconds": time.Since(copyStarted).Seconds()}) }()
			manifest, err = statebackup.Capture(ctx, b.StateRoot, snapshotDir, id, b.Image)
			if err != nil {
				return
			}
			manifest.References, err = b.Remote.References(ctx, manifest, snapshotDir)
			if err != nil {
				return
			}
			err = statebackup.AtomicJSON(filepath.Join(snapshotDir, statebackup.ManifestName), manifest)
		}()
		result <- err
	}
	waitCtx, cancelWait := context.WithTimeout(parent, boundaryWait)
	select {
	case b.Boundary <- captureOnBoundary:
		b.update(map[string]any{"wait_seconds": time.Since(startedAt).Seconds()})
		cancelWait()
	case <-waitCtx.Done():
		fail(waitCtx.Err())
		cancelWait()
		return
	}
	if err := <-result; err != nil {
		fail(err)
		return
	}
	b.update(map[string]any{"phase": "uploading", "snapshot_cutoff": manifest.CreatedAt.Format(time.RFC3339Nano), "identity": statebackup.IdentityKey(manifest)})
	uploadCtx, cancelUpload := context.WithTimeout(parent, 6*time.Hour)
	defer cancelUpload()
	uploadStarted := time.Now()
	defer func() { b.update(map[string]any{"upload_seconds": time.Since(uploadStarted).Seconds()}) }()
	if err := b.Remote.Publish(uploadCtx, snapshotDir, manifest); err != nil {
		var retention *statebackup.RetentionError
		if !errors.As(err, &retention) {
			fail(err)
			return
		}
		// Verified remote backup still counts as complete; only cleanup failed.
		status := b.Status()
		n, _ := status["retention_failures_total"].(float64)
		b.update(map[string]any{"retention_error": err.Error(), "retention_failures_total": n + 1})
	} else {
		b.update(map[string]any{"retention_error": ""})
	}
	var archiveBytes int64
	for _, f := range manifest.Files {
		archiveBytes += f.Size
	}
	b.update(map[string]any{"phase": "complete", "last_success_timestamp_seconds": float64(time.Now().Unix()), "bytes": archiveBytes, "duration_seconds": time.Since(startedAt).Seconds(), "completed_at": time.Now().UTC().Format(time.RFC3339Nano)})
	// Only a verified attempt directory is removed. Failed work stays for diagnosis.
	if err := os.RemoveAll(attemptDir); err != nil {
		b.update(map[string]any{"cleanup_error": err.Error()})
	}
}

// Samples publishes hot_to_cold_backup_* gauges/counters for /metrics.
func (b *BackupService) Samples() observability.Snapshot {
	status := b.Status()
	number := func(key string) float64 {
		switch v := status[key].(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		}
		return 0
	}
	admissionClosed := float64(0)
	if b.Store.AdmissionClosed() {
		admissionClosed = 1
	}
	lastSuccessUnix := number("last_success_timestamp_seconds")
	ageSeconds := float64(0)
	if lastSuccessUnix > 0 {
		ageSeconds = time.Since(time.Unix(int64(lastSuccessUnix), 0)).Seconds()
		if ageSeconds < 0 {
			ageSeconds = 0
		}
	}
	samples := observability.Snapshot{
		observability.Gauge("backup_admission_closed", admissionClosed),
		observability.Gauge("backup_last_success_timestamp_seconds", lastSuccessUnix),
		observability.Gauge("backup_age_seconds", ageSeconds),
		observability.Gauge("backup_last_duration_seconds", number("duration_seconds")),
		observability.Gauge("backup_last_barrier_seconds", number("barrier_seconds")),
		observability.Gauge("backup_last_bytes", number("bytes")),
		observability.Gauge("backup_failures_total", number("failures_total")),
		observability.Gauge("backup_blocked_writes_total", float64(b.Store.admissionRejected.Load())),
	}
	retentionMetric := observability.Gauge("backup_retention_failures_total", number("retention_failures_total"))
	retentionMetric.Counter = true
	samples[len(samples)-1].Counter = true // backup_blocked_writes_total
	samples[len(samples)-2].Counter = true // backup_failures_total
	samples = append(samples, retentionMetric)
	configured := float64(0)
	if b.Remote != nil {
		configured = 1
	}
	samples = append(samples, observability.Gauge("backup_configured", configured))
	if started, ok := status["barrier_started_at"].(string); ok && started != "" {
		if at, e := time.Parse(time.RFC3339Nano, started); e == nil {
			samples = append(samples, observability.Gauge("backup_barrier_age_seconds", time.Since(at).Seconds()))
		}
	} else {
		samples = append(samples, observability.Gauge("backup_barrier_age_seconds", 0))
	}
	// Metric phases map status wait_seconds/drain_seconds/capture_seconds/upload_seconds.
	for _, phase := range []string{"wait", "drain", "capture", "upload"} {
		samples = append(samples, observability.Gauge("backup_last_phase_seconds", number(phase+"_seconds"), "phase", phase))
	}
	running := float64(0)
	b.mu.Lock()
	if b.busy {
		running = 1
	}
	b.mu.Unlock()
	return append(samples, observability.Gauge("backup_running", running))
}
