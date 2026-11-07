// normalize_existing_test.go is the opt-in proof that an audited all-unique export
// normalizes to itself. Overwrite cases stay in the small native oracle.

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
)

// This gate deliberately accepts only an audited all-unique retained export.
// Overwrite correctness is covered independently by the small native oracle.
// The full benchmark therefore proves exact equality of every input/output
// record, including sequence, without an unbounded oracle on the full day.
func TestNativeExistingExportNormalization(t *testing.T) {
	source := os.Getenv("SAGE_ARCHIVE_NORMALIZE_SOURCE")
	if source == "" {
		t.Skip("set SAGE_ARCHIVE_NORMALIZE_{SOURCE,DIR,EXPECTED_ROWS} and SAGE_ARCHIVE_DUCKDB2")
	}
	var e error
	source, e = localPath(source)
	if e != nil {
		t.Fatal(e)
	}
	info, e := os.Lstat(source)
	if e != nil || !info.Mode().IsRegular() || filepath.Ext(source) != ".parquet" {
		t.Fatal("regular retained Parquet source required", e)
	}
	root, e := localPath(os.Getenv("SAGE_ARCHIVE_NORMALIZE_DIR"))
	if e != nil || os.Getenv("SAGE_ARCHIVE_NORMALIZE_DIR") == "" {
		t.Fatal("fresh isolated benchmark directory required", e)
	}
	rel, e := filepath.Rel(filepath.Dir(source), root)
	if e != nil || rel == "." || !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatal("benchmark directory must be outside retained source directory")
	}
	if e = os.Mkdir(root, 0700); e != nil {
		t.Fatal("benchmark directory must not already exist", e)
	}
	expected, e := strconv.ParseInt(os.Getenv("SAGE_ARCHIVE_NORMALIZE_EXPECTED_ROWS"), 10, 64)
	if e != nil || expected < 1 {
		t.Fatal("positive audited expected row count required", e)
	}
	if rows, e := partitionRows([]string{source}); e != nil || rows != expected {
		t.Fatal("retained export is not the audited all-unique input", rows, expected, e)
	}
	memory := os.Getenv("SAGE_ARCHIVE_NORMALIZE_MEMORY")
	if memory == "" {
		memory = "3GB"
	}
	const threads = 16
	const fanIn = normalizationMergeFanIn
	d := DuckDB{Threads: threads, Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: root, Memory: memory}
	if d.Binary == "" {
		t.Fatal("DuckDB 2.0 CLI required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()
	report := map[string]any{"merge_fan_in": fanIn, "merge_concurrency": 1, "threads": threads, "logical_cpus": runtime.NumCPU(), "go_max_procs": runtime.GOMAXPROCS(0), "passed": false, "source": source, "expected_rows": expected, "source_bytes": info.Size(), "duckdb_memory": memory, "phase": "version-check"}
	before := ""
	t.Cleanup(func() {
		if before != "" {
			after, e := digest(source)
			report["source_sha256_after"] = after
			report["source_unchanged"] = e == nil && after == before
			if e != nil || after != before {
				t.Error("retained source changed", e)
			}
		}
		report["passed"] = !t.Failed()
		if e := writeJSON(filepath.Join(root, "normalization-benchmark.json"), report); e != nil {
			t.Error(e)
		}
	})
	if e = d.CheckVersion(ctx); e != nil {
		report["error"] = e.Error()
		t.Fatal(e)
	}
	report["phase"] = "source-hash"
	before, e = digest(source)
	if e != nil {
		t.Fatal(e)
	}
	report["source_sha256_before"] = before
	peakDone, stop := sampleStageBytes(root)
	defer stop()
	self, child, usage := operationCPU()
	began := time.Now()
	report["phase"] = "normalization"
	if e = writeJSON(filepath.Join(root, "normalization-benchmark.json"), report); e != nil {
		t.Fatal(e)
	}
	output := filepath.Join(root, "normalized.parquet")
	stats, e := d.DeduplicateExport(ctx, source, output)
	report["normalization_seconds"] = time.Since(began).Seconds()
	afterSelf, afterChild, afterUsage := operationCPU()
	report["usage_available"] = usage && afterUsage
	report["normalization_self_cpu_seconds"] = afterSelf - self
	report["normalization_child_cpu_seconds"] = afterChild - child
	if !usage || !afterUsage {
		t.Fatal("CPU evidence unavailable")
	}
	report["stats"] = stats
	report["normalization_go_peak_rss_bytes"] = peakRSS(syscall.RUSAGE_SELF)
	report["normalization_largest_child_peak_rss_bytes"] = peakRSS(syscall.RUSAGE_CHILDREN)
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		if n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil {
			report["normalization_cgroup_peak_bytes"] = n
		}
	}
	stop()
	report["normalization_sampled_scratch_peak_bytes"] = <-peakDone
	if e != nil {
		report["error"] = e.Error()
		t.Fatal(e)
	}
	if stats.InputRows != expected || stats.UniqueRows != expected || stats.SupersededRows != 0 {
		t.Fatal("unexpected retained input/output cardinalities", stats)
	}
	report["normalization_completed"] = true
	report["phase"] = "full-source-verification"
	if e = writeJSON(filepath.Join(root, "normalization-benchmark.json"), report); e != nil {
		t.Fatal(e)
	}
	// Compare every scalar byte and replay sequence, plus complete identity.
	canonical := func(path string) string {
		return "SELECT time,name,tags,kind,to_json(struct_pack(value:=value,seq:=seq))::VARCHAR AS value FROM (" + exportCanonicalSQL(path) + ")"
	}
	began = time.Now()
	// Verification is fixed across arms and outside normalization timing.
	proof := d
	proof.Threads = 16
	var profile ComparisonProfile
	proof.comparisonProfile = &profile
	e = proof.EqualCanonical(ctx, canonical(source), canonical(output), "")
	report["verification"] = profile
	report["verification_seconds"] = time.Since(began).Seconds()
	if e != nil {
		report["error"] = e.Error()
		t.Fatal(e)
	}
	if !profile.Passed || profile.ReusedInitial || profile.Threads != 16 || profile.Mode != "fixed" {
		t.Fatal("incomplete fresh exact proof", profile)
	}
	report["full_source_multiset_equal"] = true
	report["phase"] = "complete"
	report["metric_scope"] = "normalization wall excludes source hashing and subsequent exact full-source verification; RSS is process lifetime through normalization, largest-child RSS is not a combined peak; cgroup includes page cache; scratch sampling excludes retained source and can miss transient/unlinked files; OS caches uncontrolled and source hashing warms input"
	t.Logf("normalized and fully verified %d records; reports retained at %s", expected, root)
}
