// profile_test.go checks that a step profile records the transition, including a
// failure, without changing the retry, and that cleanup leaves the profile behind.

package archive

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestStepProfilesPreserveTransitionsAndSurviveCleanup tests that a step profile preserves transitions and survives cleanup.
func TestStepProfilesPreserveTransitionsAndSurviveCleanup(t *testing.T) {
	r, job, _ := setup(t)
	r.ProfileSteps = true
	until(t, r, job, "COMPLETE")
	paths, e := filepath.Glob(filepath.Join(job.Work, "step-*.json"))
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	for _, p := range paths {
		data, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		var record struct {
			Before    string  `json:"state_before"`
			Failed    bool    `json:"operation_failed"`
			Available bool    `json:"usage_available"`
			Seconds   float64 `json:"elapsed_seconds"`
		}
		if e = json.Unmarshal(data, &record); e != nil {
			t.Fatal(e)
		}
		if record.Failed || !record.Available || record.Seconds < 0 {
			t.Fatal(record)
		}
		seen[record.Before] = true
	}
	for _, state := range []string{"EXPORTING", "EXPORTED", "RECONCILED", "BUILT", "COMMITTED", "DELETING", "DELETE_REQUESTED"} {
		if !seen[state] {
			t.Fatal("missing state", state)
		}
	}
}

// TestStepProfilesRecordFailureWithoutChangingRetry tests that a step profile records a failure without changing the retry.
func TestStepProfilesRecordFailureWithoutChangingRetry(t *testing.T) {
	r, job, _ := setup(t)
	until(t, r, job, "RECONCILED")
	r.ProfileSteps = true
	r.Worker = needsReconcileWorker{r.Worker}
	if e := r.Step(context.Background(), job); e == nil || job.State != "EXPORTED" {
		t.Fatal(job.State, e)
	}
	paths, _ := filepath.Glob(filepath.Join(job.Work, "step-RECONCILED-*.json"))
	if len(paths) != 1 {
		t.Fatal(paths)
	}
	data, e := os.ReadFile(paths[0])
	if e != nil {
		t.Fatal(e)
	}
	var record struct {
		Failed bool   `json:"operation_failed"`
		After  string `json:"state_after"`
	}
	if e = json.Unmarshal(data, &record); e != nil || !record.Failed || record.After != "EXPORTED" {
		t.Fatal(record, e)
	}
}
