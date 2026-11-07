// comparison_benchmark_test.go is the opt-in retained comparison. Each arm calls
// fresh EqualReconciliation. It skips unless SAGE_COMPARE_EXPORT and SAGE_COMPARE_INFLUX are set.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

// reconciliationBenchmarkSQL returns the SQL for a reconciliation benchmark.
func reconciliationBenchmarkSQL(path string) string {
	return "SELECT * EXCLUDE(seq) FROM (" + Typed(path) + ")"
}

// Each arm is a separate process/container. This invokes the same fresh EqualReconciliation
// path as Worker.Reconcile, not build-local retained canonical staging.
func TestNativeRetainedComparison(t *testing.T) {
	left, right := os.Getenv("SAGE_COMPARE_EXPORT"), os.Getenv("SAGE_COMPARE_INFLUX")
	if left == "" {
		t.Skip("set SAGE_COMPARE_* for retained fresh comparison")
	}
	root := os.Getenv("SAGE_COMPARE_DIR")
	if root == "" {
		t.Fatal("fresh benchmark directory required")
	}
	for _, path := range []string{left, right, root} {
		if _, e := localPath(path); e != nil {
			t.Fatal(e)
		}
	}
	for _, path := range []string{left, right} {
		rel, e := filepath.Rel(filepath.Dir(path), root)
		if e != nil || !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Fatal("scratch must be outside source directory")
		}
	}
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	threads, e := optionalThreadCount("SAGE_COMPARE_THREADS")
	if e != nil || threads == 0 {
		t.Fatal("positive threads required", e)
	}
	rows, e := strconv.ParseInt(os.Getenv("SAGE_COMPARE_ROWS"), 10, 64)
	if e != nil || rows < 1 {
		t.Fatal("positive audited row count required")
	}
	memory := os.Getenv("SAGE_COMPARE_MEMORY")
	if memory == "" {
		memory = "3GB"
	}
	var profile ComparisonProfile
	d := DuckDB{Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: root, Memory: memory, Threads: threads, comparisonProfile: &profile}
	report := map[string]any{"logical_cpus": runtime.NumCPU(), "go_max_procs": runtime.GOMAXPROCS(0), "passed": false, "threads": threads, "memory": memory, "rows": rows, "fresh_comparison": true}
	report["canonicalization"] = "raw_tag_dictionary"
	before := map[string]string{}
	t.Cleanup(func() {
		unchanged := true
		after := map[string]string{}
		for path, hash := range before {
			actual, e := digest(path)
			after[path] = actual
			if e != nil || actual != hash {
				unchanged = false
				t.Error("retained input changed", e)
			}
		}
		report["source_unchanged"] = unchanged
		report["input_sha256"] = before
		report["input_sha256_after"] = after
		report["passed"] = !t.Failed()
		if e := writeJSON(filepath.Join(root, "benchmark.json"), report); e != nil {
			t.Error(e)
		}
	})
	ctx := context.Background()
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{left, right} {
		info, e := os.Lstat(path)
		if e != nil || !info.Mode().IsRegular() || filepath.Ext(path) != ".parquet" {
			t.Fatal("regular retained Parquet input required", e)
		}
		count, e := partitionRows([]string{path})
		if e != nil || count != rows {
			t.Fatal("input cardinality differs from audited day", count, e)
		}
		hash, e := digest(path)
		if e != nil {
			t.Fatal(e)
		}
		before[path] = hash
	}
	peak, stop := sampleStageBytes(root)
	defer stop()
	self, child, available := operationCPU()
	began := time.Now()
	e = d.EqualReconciliation(ctx, left, right)
	report["comparison_seconds"] = time.Since(began).Seconds()
	afterSelf, afterChild, afterAvailable := operationCPU()
	report["usage_available"] = available && afterAvailable
	report["self_cpu_seconds"] = afterSelf - self
	report["child_cpu_seconds"] = afterChild - child
	report["comparison"] = profile
	report["go_peak_rss_bytes"] = peakRSS(syscall.RUSAGE_SELF)
	report["largest_child_peak_rss_bytes"] = peakRSS(syscall.RUSAGE_CHILDREN)
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		report["cgroup_memory_peak_bytes"] = string(b)
	}
	stop()
	report["sampled_scratch_peak_bytes"] = <-peak
	if e != nil {
		t.Fatal(e)
	}
	if !profile.Passed || profile.ReusedInitial || profile.Mode != "fixed" || profile.Canonicalization != "raw_tag_dictionary" || profile.InitialFanout != 16 || profile.LeafPartitions < 1 || !available || !afterAvailable {
		t.Fatal("incomplete fresh exact comparison evidence", profile)
	}
	report["full_multiset_equal"] = true
	report["metric_scope"] = "wall/CPU cover promoted fresh primitive reconciliation including dictionary/count/staging/refinement/exact proof; excludes admission/hashing; no prior proof reuse; process/cgroup lifetime peaks include admission; OS caches uncontrolled; no live Influx, remote storage or deletion"
}

// Typed reconciliation gate at the arm's actual thread count, including values
// that must retain type/precision, metadata and nanosecond identity.
func TestNativeTypedComparison(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("native DuckDB 2 required")
	}
	threads, e := optionalThreadCount("SAGE_COMPARE_THREADS")
	if e != nil {
		t.Fatal(e)
	}
	if threads == 0 {
		threads = 2
	}
	root := t.TempDir()
	d := DuckDB{Binary: binary, Work: root, Memory: "512MB", Threads: threads}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	values := []struct{ kind, value string }{{"int", "9007199254740993"}, {"uint", "18446744073709551615"}, {"float", "11.7"}, {"bool", "true"}, {"string", "a\nquote \" slash \\ literal\\n"}}
	write := func(path string, mutation int) {
		t.Helper()
		_, e := writeStaging(path, path, func(enc recordEncoder) (int64, error) {
			for i := 0; i < 100; i++ {
				v := values[i%len(values)]
				r := control.Record{Time: 1740787200000000194 + int64(i), Name: "fixture", Kind: v.kind, Value: v.value, Tags: map[string]string{"plugin": "p", "vsn": "n1", "sensor": "a,b=c"}}
				if mutation == 1 && i == 0 {
					r.Time++
				}
				if mutation == 2 && i == 0 {
					r.Kind = "uint"
				}
				if e := enc.Encode(exportRecord{Record: r, Seq: int64(i + 1)}); e != nil {
					return int64(i), e
				}
			}
			return 100, nil
		})
		if e != nil {
			t.Fatal(e)
		}
	}
	left, right, bad := filepath.Join(root, "export.parquet"), filepath.Join(root, "influx.parquet"), filepath.Join(root, "changed.parquet")
	write(left, 0)
	write(right, 0)
	write(bad, 1)
	var profile ComparisonProfile
	d.comparisonProfile = &profile
	if e := d.EqualReconciliation(ctx, left, right); e != nil {
		t.Fatal(e)
	}
	if !profile.Passed || profile.ReusedInitial || profile.InitialFanout != 16 || profile.Canonicalization != "raw_tag_dictionary" {
		t.Fatal(profile)
	}
	if e := d.EqualReconciliation(ctx, left, bad); e == nil {
		t.Fatal("nanosecond mutation accepted")
	}
	typed := filepath.Join(root, "changed-type.parquet")
	write(typed, 2)
	if e := d.EqualReconciliation(ctx, left, typed); e == nil {
		t.Fatal("signed/unsigned type change accepted")
	}
}
