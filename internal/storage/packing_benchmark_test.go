// packing_benchmark_test.go is the opt-in retained packing measurement. One process
// per arm keeps that arm's memory peak separate from the others.

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// A separate process/container per arm keeps lifetime RSS and cgroup peaks scoped.
func TestNativeRetainedPacking(t *testing.T) {
	input := os.Getenv("SAGE_PACK_NORMALIZED")
	if input == "" {
		t.Skip("set SAGE_PACK_* for retained packing benchmark")
	}
	reference := os.Getenv("SAGE_PACK_EXPECTED")
	root := os.Getenv("SAGE_PACK_DIR")
	threads, e := strconv.Atoi(os.Getenv("SAGE_PACK_THREADS"))
	if e != nil || threads < 1 {
		t.Fatal("positive threads required")
	}
	rows, e := strconv.ParseInt(os.Getenv("SAGE_PACK_ROWS"), 10, 64)
	if e != nil || rows < 1 {
		t.Fatal("positive rows required")
	}
	for _, p := range []string{input, reference, root} {
		if _, e := localPath(p); e != nil {
			t.Fatal(e)
		}
	}
	if root == "" {
		t.Fatal("fresh output directory required")
	}
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	sortThreads, e := optionalThreadCount("SAGE_PACK_SORT_THREADS")
	if e != nil {
		t.Fatal(e)
	}
	d := DuckDB{Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: root, Memory: os.Getenv("SAGE_PACK_MEMORY"), Threads: threads, SortThreads: sortThreads}

	ctx := context.Background()
	report := map[string]any{"passed": false, "threads": threads, "sort_threads": d.sortThreadCount(), "memory": d.Memory, "rows": rows, "comparison_partitioning": "fixed", "reuse_canonical_staging": true}
	report["build_canonicalization"] = "tag_dictionary"
	before := map[string]string{}
	t.Cleanup(func() {
		unchanged := true
		afterHashes := map[string]string{}
		for path, hash := range before {
			after, e := digest(path)
			afterHashes[path] = after
			if e != nil || after != hash {
				unchanged = false
				t.Error("retained input changed", e)
			}
		}
		report["source_unchanged"] = unchanged
		report["input_sha256"] = before
		report["input_sha256_after"] = afterHashes
		report["passed"] = !t.Failed()
		if e := writeJSON(filepath.Join(root, "benchmark.json"), report); e != nil {
			t.Error(e)
		}
	})
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{input, reference} {
		hash, e := digest(path)
		if e != nil {
			t.Fatal(e)
		}
		count, err := partitionRows([]string{path})
		if err != nil || count != rows {
			t.Fatal("retained row count differs", count, err)
		}
		before[path] = hash
	}
	settings, e := d.Run(ctx, "SELECT current_setting('threads') AS threads,current_setting('memory_limit') AS memory_limit;")
	if e != nil {
		t.Fatal(e)
	}
	var nativeSettings any
	if e = json.Unmarshal(settings, &nativeSettings); e != nil {
		t.Fatal(e)
	}
	report["native_settings"] = nativeSettings
	peak, stop := sampleStageBytes(root)
	defer stop()
	self, child, usage := operationCPU()
	began := time.Now()
	expected := filepath.Join(root, "expected.parquet")
	materialization, e := d.MaterializeExpected(ctx, "SELECT * EXCLUDE(seq) FROM ("+Typed(input)+")", expected, "", true)
	report["materialization"] = materialization
	if e != nil {
		t.Fatal(e)
	}
	paths, profile, e := d.PackExpected(ctx, expected, root, "benchmark")
	report["packing"] = profile
	report["build_seconds"] = time.Since(began).Seconds()
	afterSelf, afterChild, afterUsage := operationCPU()
	report["usage_available"] = usage && afterUsage
	report["self_cpu_seconds"] = afterSelf - self
	report["child_cpu_seconds"] = afterChild - child
	if !usage || !afterUsage {
		t.Fatal("build CPU metrics unavailable")
	}
	report["files"] = len(paths)
	report["go_peak_rss_bytes"] = peakRSS(syscall.RUSAGE_SELF)
	report["largest_child_peak_rss_bytes"] = peakRSS(syscall.RUSAGE_CHILDREN)
	if b, e := os.ReadFile("/sys/fs/cgroup/memory.peak"); e == nil {
		report["cgroup_memory_peak_bytes"] = string(b)
	}
	stop()
	report["sampled_scratch_peak_bytes"] = <-peak
	if e != nil {
		t.Fatal(e)
	}
	if profile.Rows != rows {
		t.Fatalf("rows %d != %d", profile.Rows, rows)
	}
	// Extra independent retained-reference proof is outside production build timing.
	began = time.Now()
	referenceDuck := d
	report["reference_canonicalization"] = "baseline"
	var referenceProfile ComparisonProfile
	referenceDuck.comparisonProfile = &referenceProfile
	report["reference_memory"] = referenceDuck.Memory
	report["reference_threads"] = referenceDuck.threadCount()
	report["reference_comparison_partitioning"] = "fixed"
	if e := referenceDuck.Equal(ctx, Parquet([]string{reference}), Parquet(paths), ""); e != nil {
		t.Fatal(e)
	}
	report["retained_reference_verification_seconds"] = time.Since(began).Seconds()
	report["full_reference_equal"] = true
	report["reference_comparison"] = referenceProfile
	report["metric_scope"] = "build includes typed materialization and all production packing checks; excludes input hashing and extra retained-reference comparison; RSS lifetime through build, child RSS not additive; cgroup includes cache; scratch sampling may miss transient files; OS cache uncontrolled"
}

func TestPackingThreadSettings(t *testing.T) {
	for _, threads := range []int{2, 4} {
		d := DuckDB{Threads: threads}
		want := "SET partitioned_write_max_open_files=" + strconv.Itoa(16*threads) + ";SET partitioned_write_flush_threshold=65536;SET preserve_insertion_order=false;"
		if d.partitionSettings() != want {
			t.Fatal(d.partitionSettings())
		}
	}
	if (DuckDB{}).threadCount() != 16 || (DuckDB{}).sortThreadCount() != 16 {
		t.Fatal("default changed")
	}
	if (DuckDB{Threads: 16, SortThreads: 2}).sortThreadCount() != 2 || (DuckDB{Threads: 16}).sortThreadCount() != 16 {
		t.Fatal("sort threads must follow the session count unless explicitly split")
	}
}

// optionalThreadCount returns the optional thread count.
func optionalThreadCount(name string) (int, error) {
	s := os.Getenv(name)
	if s == "" {
		return 0, nil
	}
	n, e := strconv.Atoi(s)
	if e != nil || n < 1 || n > 256 {
		return 0, fmt.Errorf("%s must be 1..256", name)
	}
	return n, nil
}

func TestNativePackingProfile(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("native DuckDB 2 required")
	}
	threads := 2
	if s := os.Getenv("SAGE_PACK_THREADS"); s != "" {
		var e error
		threads, e = strconv.Atoi(s)
		if e != nil {
			t.Fatal(e)
		}
	}
	sortThreads, e := optionalThreadCount("SAGE_PACK_SORT_THREADS")
	if e != nil {
		t.Fatal(e)
	}
	root := t.TempDir()
	memory := os.Getenv("SAGE_PACK_MEMORY")
	if memory == "" {
		memory = "512MB"
	}
	d := DuckDB{Binary: binary, Work: root, Memory: memory, Threads: threads, SortThreads: sortThreads}

	ctx := context.Background()
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	source := `SELECT make_timestamp_ns(1740787200000000194+n)::TIMESTAMPTZ_NS AS time,'temperature'::VARCHAR AS name,
 CASE WHEN n%2=0 THEN 'a' ELSE 'b' END AS plugin,'N1'::VARCHAR AS vsn,
 NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,
 MAP{'unit':'C'}::MAP(VARCHAR,VARCHAR) AS meta,
 CASE WHEN n%3=0 THEN 9007199254740993::BIGINT::VARIANT WHEN n%3=1 THEN 11.7::DOUBLE::VARIANT ELSE 'abc'::VARCHAR::VARIANT END AS value FROM range(1000) r(n)`
	expected := filepath.Join(root, "expected.parquet")
	if _, e := d.MaterializeExpected(ctx, source, expected, "", false); e != nil {
		t.Fatal(e)
	}
	paths, p, e := d.PackExpected(ctx, expected, root, "gate")
	if e != nil || !p.Passed || p.Rows != 1000 || len(paths) != 1 || len(p.Steps) != 7 {
		t.Fatal(paths, p, e)
	}
	if !p.ReuseCanonicalStaging || p.Canonicalization != "tag_dictionary" {
		t.Fatal("reuse setting missing")
	}
	// Equal always uses the original canonical SQL as an independent oracle.
	if e := d.Equal(ctx, Parquet([]string{expected}), Parquet(paths), ""); e != nil {
		t.Fatal("independent packing gate reference failed", e)
	}
	for _, step := range p.Steps {
		if step.Name == "whole_build_exact_comparison" && !step.Comparison.ReusedInitial {
			t.Fatal("whole proof did not use requested preparation", step)
		}
	}
	if scratch, _ := filepath.Glob(filepath.Join(root, "packing-canonical-*")); len(scratch) != 0 {
		t.Fatal("build-local scratch retained", scratch)
	}
	for _, step := range p.Steps {
		if step.Name == "file_exact_comparison" || step.Name == "whole_build_exact_comparison" {
			if step.Comparison == nil || !step.Comparison.Passed || step.Comparison.LeafPartitions < 1 {
				t.Fatal("missing exact comparison profile", step)
			}
		}
	}
}

func TestNativeEmptyPackingReuse(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("native DuckDB 2 required")
	}
	root := t.TempDir()
	d := DuckDB{Binary: binary, Work: root, Memory: "512MB"}
	ctx := context.Background()
	expected := filepath.Join(root, "expected.parquet")
	if _, e := d.MaterializeExpected(ctx, Parquet(nil), expected, "", true); e != nil {
		t.Fatal(e)
	}
	paths, p, e := d.PackExpected(ctx, expected, root, "empty")
	if e != nil || !p.Passed || !p.ReuseCanonicalStaging || p.Canonicalization != "tag_dictionary" || p.Rows != 0 || len(paths) != 0 {
		t.Fatal(p, e)
	}
	last := p.Steps[len(p.Steps)-1]
	if last.Name != "whole_build_exact_comparison" || !last.Comparison.Passed || !last.Comparison.ReusedInitial {
		t.Fatal(last)
	}
	if scratch, _ := filepath.Glob(filepath.Join(root, "packing-canonical-*")); len(scratch) != 0 {
		t.Fatal(scratch)
	}
}

func TestPackingProfileFailure(t *testing.T) {
	root := t.TempDir()
	_, p, e := (DuckDB{Work: root}).PackExpected(context.Background(), "unused", root, "../invalid")
	if e == nil || p.Passed {
		t.Fatal("invalid packing id accepted", p, e)
	}
	if _, e := os.Stat(filepath.Join(root, "packing-profile.json")); e != nil {
		t.Fatal("failure profile missing", e)
	}
}
