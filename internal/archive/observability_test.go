// observability_test.go checks attempt metrics, a scrape during a running
// attempt, and which profile files retention is allowed to delete.

package archive

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	"beehive-hot-to-cold-loader/internal/observability"
	"github.com/prometheus/common/expfmt"
)

func attemptCount(t *testing.T, m *observability.Metrics, stage, outcome string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "hot_to_cold_step_attempts_total" {
			continue
		}
		for _, metric := range f.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["stage"] == stage && labels["outcome"] == outcome && labels["mode"] == "archive" {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatal("missing attempt sample")
	return 0
}

func TestAttemptMetricsWithoutProfiling(t *testing.T) {
	r, j, w := setup(t)
	r.Metrics = observability.New(720*time.Hour, true, false)
	if err := r.Step(context.Background(), j); err != nil {
		t.Fatal(err)
	} // fencing -> exporting
	w.fail = "export"
	if err := r.Step(context.Background(), j); err == nil {
		t.Fatal("expected export failure")
	}
	w.fail = ""
	if err := r.Step(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if attemptCount(t, r.Metrics, "EXPORTING", "error") != 1 || attemptCount(t, r.Metrics, "EXPORTING", "success") != 1 {
		t.Fatal("retry observations lost")
	}
	if paths, _ := filepath.Glob(filepath.Join(j.Work, "step-*.json")); len(paths) != 0 {
		t.Fatal("metrics enabled JSON profiling")
	}
	until(t, r, j, "ROUTED")
	r.DeleteEnabled = false
	if err := r.Step(context.Background(), j); !errors.Is(err, ErrWaiting) {
		t.Fatal(err)
	}
	if attemptCount(t, r.Metrics, "ROUTED", "success") != 0 || attemptCount(t, r.Metrics, "ROUTED", "error") != 0 {
		t.Fatal("wait counted as execution")
	}
	r.DeleteEnabled = true
	until(t, r, j, "COMPLETE")
}

type blockingExport struct {
	Worker
	entered, release chan struct{}
}

func (w blockingExport) Export(ctx context.Context, j *control.Job) error {
	close(w.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.release:
		return nil
	}
}

func TestScrapeDuringRunningAndCanceledAttempt(t *testing.T) {
	r, j, _ := setup(t)
	r.Metrics = observability.New(720*time.Hour, true, false)
	if err := r.Step(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	w := blockingExport{Worker: r.Worker, entered: make(chan struct{}), release: make(chan struct{})}
	r.Worker = w
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Step(ctx, j) }()
	<-w.entered
	h := r.Metrics.Handler(func(ctx context.Context) (observability.Snapshot, error) {
		return r.Store.MetricsSnapshot(ctx, control.SnapshotOptions{Retention: 720 * time.Hour, DeletionEnabled: true})
	})
	req := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(strings.NewReader(response.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, metric := range families["hot_to_cold_step_in_progress"].Metric {
		if metric.GetGauge().GetValue() == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("running step not exposed")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if attemptCount(t, r.Metrics, "EXPORTING", "canceled") != 1 {
		t.Fatal("cancellation not classified")
	}
}

func TestProfileRetentionScopeAndBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		age                          time.Duration
		staging, unfinished, unknown bool
		retention                    time.Duration
		remove                       bool
	}{
		{name: "exact boundary", age: 720 * time.Hour, retention: 720 * time.Hour, remove: true},
		{name: "before boundary", age: 720*time.Hour - time.Nanosecond, retention: 720 * time.Hour},
		{name: "keep staging", age: 800 * time.Hour, retention: 720 * time.Hour, staging: true},
		{name: "retain profiles indefinitely", age: 800 * time.Hour, retention: 0},
		{name: "routed", age: 800 * time.Hour, retention: 720 * time.Hour, unfinished: true},
		{name: "legacy time unknown", age: 800 * time.Hour, retention: 720 * time.Hour, unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, j, _ := setup(t)
			j.Work = filepath.Join(r.Root, j.ID)
			if err := os.Mkdir(j.Work, 0700); err != nil {
				t.Fatal(err)
			}
			if err := r.Store.Finish(j); err != nil {
				t.Fatal(err)
			}
			completed := time.Now().Add(-time.Hour).UTC()
			ns := completed.UnixNano()
			if tc.unknown {
				ns = 0
			}
			if _, err := r.Store.DB.Exec("UPDATE job_milestones SET at_ns=? WHERE job_id=? AND milestone='complete'", ns, j.ID); err != nil {
				t.Fatal(err)
			}
			if tc.unfinished {
				j.State = "ROUTED"
				j.Error = "unresolved failure"
				if err := r.Store.Save(j); err != nil {
					t.Fatal(err)
				}
			}
			r.KeepStaging = tc.staging
			profiles := []string{"step-EXPORTING-attempt1.json", "operation-verify-attempt1.json", "expected-profile.json", "packing-profile.json"}
			evidence := []string{"files.json", "verification-COMMITTED.json", "export-dedup.json", "unknown-profile.json", "step-EXPORTING-attempt2.json.pending"}
			for _, name := range append(profiles, evidence...) {
				if err := os.WriteFile(filepath.Join(j.Work, name), []byte("preserved evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			p := &ProfileRetention{Runner: r, Duration: tc.retention}
			n, err := p.Prune(context.Background(), completed.Add(tc.age))
			if err != nil {
				t.Fatal(err)
			}
			if tc.remove && n != len(profiles) || !tc.remove && n != 0 {
				t.Fatal(n)
			}
			for _, name := range evidence {
				b, err := os.ReadFile(filepath.Join(j.Work, name))
				if err != nil || string(b) != "preserved evidence" {
					t.Fatal("evidence changed", name, err)
				}
			}
			for _, name := range profiles {
				_, err := os.Stat(filepath.Join(j.Work, name))
				if os.IsNotExist(err) != tc.remove {
					t.Fatal(name, err)
				}
			}
		})
	}
}

func TestProfileRetentionRejectsEscapesAndSymlinks(t *testing.T) {
	r, j, _ := setup(t)
	j.Work = filepath.Join(r.Root, j.ID)
	os.Mkdir(j.Work, 0700)
	if err := r.Store.Finish(j); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "evidence.json")
	os.WriteFile(external, []byte("do not remove"), 0600)
	if err := os.Symlink(external, filepath.Join(j.Work, "expected-profile.json")); err != nil {
		t.Fatal(err)
	}
	p := &ProfileRetention{Runner: r, Duration: time.Hour}
	if n, err := p.Prune(context.Background(), time.Now().Add(2*time.Hour)); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if b, err := os.ReadFile(external); err != nil || string(b) != "do not remove" {
		t.Fatal(err)
	}
	// A fresh pass must refuse an out-of-root work path even for COMPLETE jobs.
	j.Work = filepath.Dir(external)
	if err := r.Store.Save(j); err != nil {
		t.Fatal(err)
	}
	p.cursor = ""
	r.Store.DB.Exec("UPDATE options SET value='' WHERE key='observability.profile_cleanup_cursor'")
	if _, err := p.Prune(context.Background(), time.Now().Add(2*time.Hour)); err == nil {
		t.Fatal("work path escape accepted")
	}
}

func TestLifecycleMetricsSurviveRestart(t *testing.T) {
	r, j, w := setup(t)
	r.Metrics = observability.New(720*time.Hour, true, false)
	view, err := r.Store.View("api", "old-view")
	if err != nil {
		t.Fatal(err)
	}
	if view.ID == "" {
		t.Fatal("missing read grant")
	}
	h := r.Metrics.Handler(func(ctx context.Context) (observability.Snapshot, error) {
		return r.Store.MetricsSnapshot(ctx, control.SnapshotOptions{Retention: 720 * time.Hour, DeletionEnabled: true})
	})
	scrape := func() {
		t.Helper()
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
	}
	until(t, r, j, "COMMITTED")
	w.fail = "verify"
	if err = r.Step(context.Background(), j); err == nil {
		t.Fatal("expected failed verification")
	}
	scrape()
	w.fail = ""
	until(t, r, j, "ROUTED")
	scrape()
	if err = r.Step(context.Background(), j); !errors.Is(err, ErrWaiting) {
		t.Fatal("old reader did not block deletion", err)
	}
	if err = r.Store.ReleaseRead(view.ID); err != nil {
		t.Fatal(err)
	}
	until(t, r, j, "COMPLETE")
	scrape()
	var seq int
	var name, path string
	if err = r.Store.DB.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	r.Store.Close()
	reopened, err := control.Open(path, "waggle", "warehouse", "2025-03-01")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var total int
	if err = reopened.DB.QueryRow("SELECT total FROM milestone_totals WHERE mode='archive' AND milestone='complete'").Scan(&total); err != nil || total != 1 {
		t.Fatal(total, err)
	}
	var complete int
	if err = reopened.DB.QueryRow("SELECT count(*) FROM jobs WHERE state='COMPLETE'").Scan(&complete); err != nil || complete != 1 {
		t.Fatal(complete, err)
	}
	restarted := observability.New(720*time.Hour, true, false)
	if attemptCount(t, restarted, "COMMITTED", "error") != 0 {
		t.Fatal("attempt counter did not reset")
	}
}

func TestProfileRetentionRejectsDirectorySymlink(t *testing.T) {
	r, j, _ := setup(t)
	j.Work = filepath.Join(r.Root, j.ID)
	external := t.TempDir()
	evidence := filepath.Join(external, "packing-profile.json")
	if err := os.WriteFile(evidence, []byte("external evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, j.Work); err != nil {
		t.Fatal(err)
	}
	if err := r.Store.Finish(j); err != nil {
		t.Fatal(err)
	}
	p := &ProfileRetention{Runner: r, Duration: time.Hour}
	if _, err := p.Prune(context.Background(), time.Now().Add(2*time.Hour)); err == nil {
		t.Fatal("directory symlink followed")
	}
	if b, err := os.ReadFile(evidence); err != nil || string(b) != "external evidence" {
		t.Fatal("external evidence changed", err)
	}
}
