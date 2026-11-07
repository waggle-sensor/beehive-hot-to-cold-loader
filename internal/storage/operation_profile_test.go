// operation_profile_test.go checks that a profile keeps every attempt, including
// the failing step, and that a profile write error does not change the operation.

package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"beehive-hot-to-cold-loader/internal/control"
)

// TestOperationProfilesKeepAttemptsAndFailingStep tests that a operation profile keeps attempts and failing step.
func TestOperationProfilesKeepAttemptsAndFailingStep(t *testing.T) {
	root := t.TempDir()
	job := &control.Job{ID: "job", Day: "2025-03-01", State: "DELETING"}
	for _, failure := range []bool{true, false} {
		p := newOperationProfile(job, "delete", DuckDB{})
		err := p.measure("source_validation", func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		err = p.measure("delete_request", func() error {
			if failure {
				return errors.New("Bearer secret-token and SQL")
			}
			return nil
		})
		if e := p.save(root, err); e != nil {
			t.Fatal(e)
		}
	}
	paths, e := filepath.Glob(filepath.Join(root, "operation-delete-*.json"))
	if e != nil || len(paths) != 2 {
		t.Fatal(paths, e)
	}
	var failed, passed int
	for _, path := range paths {
		data, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(string(data), "secret-token") || strings.Contains(string(data), "Bearer") {
			t.Fatal("profile leaked error text")
		}
		var p operationProfile
		if e = json.Unmarshal(data, &p); e != nil {
			t.Fatal(e)
		}
		if p.Passed {
			passed++
		} else {
			failed++
		}
		if p.State != "DELETING" || p.Threads != 16 || p.Memory != "3GB" || len(p.Steps) != 2 || p.Steps[1].Failed == p.Passed || p.TotalSeconds < p.Steps[0].Seconds+p.Steps[1].Seconds {
			t.Fatal(p)
		}
	}
	if failed != 1 || passed != 1 {
		t.Fatal(failed, passed)
	}
	pending, _ := filepath.Glob(filepath.Join(root, "*.pending"))
	if len(pending) != 0 {
		t.Fatal(pending)
	}
}

// TestOperationProfileWriteFailureDoesNotAlterOperation tests that a operation profile write failure does not alter the operation.
func TestOperationProfileWriteFailureDoesNotAlterOperation(t *testing.T) {
	p := newOperationProfile(&control.Job{ID: "job"}, "delete", DuckDB{})
	completed := false
	err := p.measure("delete_request", func() error { completed = true; return nil })
	p.finish(filepath.Join(t.TempDir(), "missing"), err)
	if err != nil || !completed {
		t.Fatal("report failure changed operation")
	}
}
