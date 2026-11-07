// metrics_test.go checks durable milestones, restart backfill, and the
// public metrics exposition.

package control

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/observability"
	"github.com/prometheus/common/expfmt"
)

func metricValue(t *testing.T, s *Store, name string, labels ...string) float64 {
	t.Helper()
	values, err := s.MetricsSnapshot(context.Background(), SnapshotOptions{Retention: 720 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range values {
		if sample.Name != name {
			continue
		}
		match := true
		for i := 0; i < len(labels); i += 2 {
			found := false
			for k, label := range sample.Labels {
				if label == labels[i] && sample.Values[k] == labels[i+1] {
					found = true
				}
			}
			if !found {
				match = false
			}
		}
		if match {
			return sample.Value
		}
	}
	t.Fatal("missing sample", name, labels)
	return 0
}

func TestDurableMilestonesAndAtomicRollback(t *testing.T) {
	s := testStore(t)
	j, err := s.Next(now(), 720*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	j.Metadata = "verified"
	if _, err = s.DB.Exec(`CREATE TRIGGER fail_milestone BEFORE INSERT ON job_milestones BEGIN SELECT RAISE(FAIL,'injected marker failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err = s.Activate(j); err == nil {
		t.Fatal("expected transaction rollback")
	}
	if v := metricValue(t, s, "job_milestones_total", "mode", "archive", "milestone", "routed"); v != 0 {
		t.Fatal(v)
	}
	if metadata, err := s.Metadata(); err != nil || metadata != "" {
		t.Fatal("activation was not rolled back", metadata, err)
	}
	s.DB.Exec("DROP TRIGGER fail_milestone")
	if err = s.Activate(j); err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(j); err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(j); err != nil {
		t.Fatal(err)
	}
	for _, milestone := range []string{"routed", "complete"} {
		if v := metricValue(t, s, "job_milestones_total", "mode", "archive", "milestone", milestone); v != 1 {
			t.Fatal(v)
		}
	}
	if v := metricValue(t, s, "last_job_milestone_timestamp_seconds", "mode", "archive", "milestone", "complete"); v <= 0 {
		t.Fatal(v)
	}
	// Historical counters survive completed payload pruning.
	s.DB.Exec("DELETE FROM jobs WHERE id=?", j.ID)
	if v := metricValue(t, s, "job_milestones_total", "mode", "archive", "milestone", "complete"); v != 1 {
		t.Fatal(v)
	}
}

func TestMilestoneBackfillAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	open := func() *Store {
		s, err := Open(path, "waggle", "warehouse", "2025-03-01")
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	j, err := s.Next(now(), 720*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	j.State = "COMPLETE"
	if err = s.Save(j); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"job_milestones", "milestone_totals", "schema_migrations"} {
		if _, err = s.DB.Exec("DROP TABLE " + table); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	s = open()
	for _, milestone := range []string{"routed", "complete"} {
		if v := metricValue(t, s, "job_milestone_baseline_total", "mode", "archive", "milestone", milestone); v != 1 {
			t.Fatal(v)
		}
		if v := metricValue(t, s, "last_job_milestone_timestamp_seconds", "mode", "archive", "milestone", milestone); v != 0 {
			t.Fatal("fabricated historical timestamp", v)
		}
	}
	s.Close()
	s = open()
	defer s.Close()
	if v := metricValue(t, s, "job_milestones_total", "mode", "archive", "milestone", "complete"); v != 1 {
		t.Fatal("backfill repeated", v)
	}
}

func TestPublicMetricsExpositionAndProtectedCommands(t *testing.T) {
	s := testStore(t)
	m := observability.New(720*time.Hour, false, false)
	h := m.Handler(func(ctx context.Context) (observability.Snapshot, error) {
		return s.MetricsSnapshot(ctx, SnapshotOptions{Retention: 720 * time.Hour})
	})
	server := &Server{Store: s, LoaderToken: "loader", ReaderToken: "reader", AdminToken: "admin", MetricsHandler: h}
	call := func(method, path, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(`{"bucket":"waggle"}`))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		return w
	}
	for _, token := range []string{"", "wrong", "loader", "reader", "admin"} {
		if w := call("GET", "/metrics", token); w.Code != 200 {
			t.Fatal(token, w.Code)
		}
	}
	for _, path := range []string{"/v1/status", "/v1/reads/acquire", "/v1/writes/acquire"} {
		for _, token := range []string{"", "wrong"} {
			if w := call("POST", path, token); w.Code != 401 {
				t.Fatal("unauthorized command accepted", path, w.Code)
			}
		}
	}
	if w := call("POST", "/v1/status", "admin"); w.Code != 200 {
		t.Fatal("admin status access lost", w.Code)
	}
	if w := call("POST", "/metrics", ""); w.Code != 405 {
		t.Fatal(w.Code)
	}
	w := call("GET", "/metrics", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(strings.NewReader(w.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"hot_to_cold_jobs", "hot_to_cold_job_milestones_total", "hot_to_cold_step_attempts_total", "go_goroutines"}
	if runtime.GOOS == "linux" {
		names = append(names, "process_cpu_seconds_total")
	}
	for _, name := range names {
		if families[name] == nil {
			t.Fatal("missing family", name)
		}
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				for _, bad := range []string{"id", "day", "job_id", "owner", "error", "url"} {
					if label.GetName() == bad {
						t.Fatal("unbounded label", bad)
					}
				}
			}
		}
	}
	s.Close()
	if w := call("GET", "/metrics", ""); w.Code != 503 {
		t.Fatal("journal failure looked healthy", w.Code)
	}
}

func TestMetricsSnapshotCancellationAndWaits(t *testing.T) {
	s := testStore(t)
	j, _ := s.Next(now(), 720*time.Hour)
	// Durable journal waits can be reconstructed independently of process counters.
	_, err := s.DB.Exec(`INSERT INTO permits(id,owner,days,state,expires_at) VALUES('writer','pod',?,'active',9223372036854775807)`, fmtJSON([]string{j.Day}))
	if err != nil {
		t.Fatal(err)
	}
	if v := metricValue(t, s, "jobs_waiting", "mode", "archive", "reason", "writers"); v != 1 {
		t.Fatal(v)
	}
	tx, err := s.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err = s.MetricsSnapshot(ctx, SnapshotOptions{}); err == nil {
		t.Fatal("blocked collection ignored context")
	}
	if time.Since(start) > time.Second {
		t.Fatal("collection did not respect cancellation")
	}
}

func fmtJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
