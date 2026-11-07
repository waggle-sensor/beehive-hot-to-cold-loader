// remote_comparison_benchmark_test.go is the opt-in pinned remote comparison.
// It reads one published snapshot and has no path that writes or deletes a remote object.

package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"syscall"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// remoteBenchmarkTarget is the target for a remote comparison benchmark.
type remoteBenchmarkTarget struct {
	Warehouse      string            `json:"warehouse"` // the warehouse of the target
	Metadata       string            `json:"metadata"` // the metadata of the target
	Day            string            `json:"day"` // the day of the target
	ExpectedSHA256 string            `json:"expected_parquet_sha256"` // the expected SHA-256 of the target
	FileSHA256     map[string]string `json:"file_sha256"` // the SHA-256 of the files in the target
}

// remoteBenchmarkContext is the context for a remote comparison benchmark.
type remoteBenchmarkContext struct {
	SnapshotID     int64             `json:"snapshot_id"` // the snapshot ID of the target
	MetadataSHA256 map[string]string `json:"metadata_sha256"` // the SHA-256 of the metadata of the target
	Rows           int64             `json:"rows"` // the rows of the target
	Objects        int               `json:"objects"` // the objects of the target
}

// The manifest reader has no write interface. Every URL is scope-checked before
// Open, and all current data-file references must match the verification journal.
// remoteBenchmarkIO is the IO for a remote comparison benchmark.
type remoteBenchmarkIO struct {
	reader *Pelican // the reader for the target
	hashes map[string]string // the SHA-256 of the files in the target
}

// Remove removes a file.
func (r *remoteBenchmarkIO) Remove(string) error { return fmt.Errorf("benchmark is read-only") }

// Open opens a file.
func (r *remoteBenchmarkIO) Open(name string) (iceio.File, error) {
	f, e := r.reader.Open(name)
	if e != nil {
		return nil, e
	}
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		f.Close()
		return nil, e
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		f.Close()
		return nil, e
	}
	r.hashes[name] = hex.EncodeToString(h.Sum(nil))
	return f, nil
}

// inspectRemoteComparison inspects a remote comparison.
func inspectRemoteComparison(p *Pelican, target remoteBenchmarkTarget) (remoteBenchmarkContext, error) {
	var out remoteBenchmarkContext
	if !p.ReadOnly {
		return out, fmt.Errorf("benchmark requires a read-only reader")
	}
	raw, e := p.ReadFile(target.Metadata)
	if e != nil {
		return out, e
	}
	digest := sha256.Sum256(raw)
	reader := &remoteBenchmarkIO{reader: p, hashes: map[string]string{target.Metadata: hex.EncodeToString(digest[:])}}
	meta, e := table.ParseMetadataBytes(raw)
	if e != nil {
		return out, e
	}
	snapshot := meta.CurrentSnapshot()
	if meta.Version() != 3 || snapshot == nil || meta.Location() != target.Warehouse+"/sage/obs" {
		return out, fmt.Errorf("expected a pinned Iceberg v3 managed snapshot")
	}
	manifests, e := snapshot.Manifests(reader)
	if e != nil {
		return out, e
	}
	seen := map[string]bool{}
	for _, manifest := range manifests {
		if manifest.ManifestContent() != iceberg.ManifestContentData {
			return out, fmt.Errorf("benchmark accepts data-only snapshots")
		}
		entries, e := manifest.FetchEntries(reader, true)
		if e != nil {
			return out, e
		}
		for _, entry := range entries {
			file := entry.DataFile()
			name := file.FilePath()
			if _, e := p.validate(name); e != nil {
				return out, e
			}
			if seen[name] || target.FileSHA256[name] == "" || file.ContentType() != iceberg.EntryContentData || file.FileFormat() != iceberg.ParquetFile || file.Count() <= 0 {
				return out, fmt.Errorf("snapshot files differ from the pinned journal")
			}
			seen[name] = true
			out.Rows += file.Count()
		}
	}
	if len(seen) != len(target.FileSHA256) {
		return out, fmt.Errorf("pinned object inventory is incomplete")
	}
	out.SnapshotID = snapshot.SnapshotID
	out.MetadataSHA256 = reader.hashes
	out.Objects = len(seen)
	return out, nil
}

// verifyRemoteObjectHashes verifies the hashes of the objects in the target.
func verifyRemoteObjectHashes(ctx context.Context, p *Pelican, target remoteBenchmarkTarget) error {
	if !p.ReadOnly {
		return fmt.Errorf("benchmark requires a read-only reader")
	}
	names := make([]string, 0, len(target.FileSHA256))
	for name := range target.FileSHA256 {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		response, e := p.request(ctx, "GET", name, nil, 0)
		if e != nil {
			return e
		}
		hash := sha256.New()
		_, e = io.Copy(hash, response.Body)
		closeError := response.Body.Close()
		if e != nil {
			return e
		}
		if closeError != nil {
			return closeError
		}
		if hex.EncodeToString(hash.Sum(nil)) != target.FileSHA256[name] {
			return fmt.Errorf("published object hash differs from verification journal")
		}
	}
	return nil
}

// remoteBenchmarkInputs returns the inputs for a remote comparison benchmark.
func remoteBenchmarkInputs(t *testing.T) (remoteBenchmarkTarget, *Pelican, string, string, int64) {
	t.Helper()
	expected := os.Getenv("SAGE_REMOTE_EXPECTED")
	if expected == "" {
		t.Skip("remote read-only benchmark environment required")
	}
	root := os.Getenv("SAGE_REMOTE_DIR")
	if root == "" {
		t.Fatal("new private benchmark output required")
	}
	for _, path := range []string{expected, root, os.Getenv("SAGE_REMOTE_KEY"), os.Getenv("SAGE_REMOTE_TARGET")} {
		if _, e := localPath(path); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(os.Getenv("SAGE_REMOTE_TARGET"))
	if e != nil {
		t.Fatal(e)
	}
	var target remoteBenchmarkTarget
	if e = json.Unmarshal(raw, &target); e != nil {
		t.Fatal(e)
	}
	if _, _, e := control.Bounds(target.Day); e != nil {
		t.Fatal(e)
	}
	rows, e := strconv.ParseInt(os.Getenv("SAGE_REMOTE_ROWS"), 10, 64)
	if e != nil || rows < 1 {
		t.Fatal("positive audited count required")
	}
	reader, e := NewPelicanReader(target.Warehouse, os.Getenv("SAGE_REMOTE_KEY"), root)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := reader.validate(target.Metadata); e != nil {
		t.Fatal(e)
	}
	hash, e := digest(expected)
	if e != nil || hash != target.ExpectedSHA256 {
		t.Fatal("expected artifact does not match journal", e)
	}
	count, e := partitionRows([]string{expected})
	if e != nil || count != rows {
		t.Fatal("expected row count mismatch", count, e)
	}
	return target, reader, expected, root, rows
}

func TestNativeRemoteComparisonPin(t *testing.T) {
	target, reader, _, root, rows := remoteBenchmarkInputs(t)
	snapshot, e := inspectRemoteComparison(reader, target)
	if e != nil {
		t.Fatal(e)
	}
	if snapshot.Rows != rows {
		t.Fatal("snapshot manifest rows do not match audited day")
	}
	if e := verifyRemoteObjectHashes(context.Background(), reader, target); e != nil {
		t.Fatal(e)
	}
	if e := writeJSON(filepath.Join(root, "pin.json"), snapshot); e != nil {
		t.Fatal(e)
	}
}

func TestNativeRemoteComparison(t *testing.T) {
	target, reader, expected, root, rows := remoteBenchmarkInputs(t)
	threads, e := optionalThreadCount("SAGE_COMPARE_THREADS")
	if e != nil || threads == 0 {
		t.Fatal("positive comparison threads required", e)
	}
	memory := os.Getenv("SAGE_COMPARE_MEMORY")
	if memory == "" {
		memory = "3GB"
	}
	var pinned remoteBenchmarkContext
	raw, e := os.ReadFile(os.Getenv("SAGE_REMOTE_PIN"))
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &pinned); e != nil {
		t.Fatal(e)
	}
	report := map[string]any{"passed": false, "rows": rows, "threads": threads, "memory": memory, "fresh_comparison": true, "read_only": reader.ReadOnly,
		"metadata": target.Metadata, "warehouse": target.Warehouse, "day": target.Day, "pin": pinned, "logical_cpus": runtime.NumCPU()}
	report["projection_arm"] = "local_dictionary"
	t.Cleanup(func() {
		hash, e := digest(expected)
		report["expected_sha256_after"] = hash
		report["expected_sha256_before"] = target.ExpectedSHA256
		report["source_unchanged"] = e == nil && hash == target.ExpectedSHA256
		if e != nil || hash != target.ExpectedSHA256 {
			t.Error("expected artifact changed", e)
		}
		report["passed"] = !t.Failed()
		if e := writeJSON(filepath.Join(root, "benchmark.json"), report); e != nil {
			t.Error(e)
		}
	})
	ctx := context.Background()
	var detail ComparisonProfile
	d := DuckDB{Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: root, Memory: memory, Threads: threads, comparisonProfile: &detail}
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	before, e := inspectRemoteComparison(reader, target)
	if e != nil || !reflect.DeepEqual(before, pinned) {
		t.Fatal("pinned metadata changed", e)
	}
	began := time.Now()
	if e := verifyRemoteObjectHashes(ctx, reader, target); e != nil {
		t.Fatal(e)
	}
	report["object_readback_before_seconds"] = time.Since(began).Seconds()
	// Use the production six-hour read scope. Tokens never enter logs or reports.
	token, e := reader.token(false, 6*time.Hour+time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	setup := "LOAD httpfs; LOAD iceberg; CREATE SECRET archival_read (TYPE http,SCOPE " + Q(target.Warehouse) + ",BEARER_TOKEN " + Q(token) + ");"
	settings, e := d.Run(ctx, setup+"SELECT name,value FROM duckdb_settings() WHERE name IN ('threads','memory_limit','async_threads','read_ahead_depth','http_client_pool_capacity') ORDER BY name;")
	if e != nil {
		t.Fatal(e)
	}
	report["native_settings"] = json.RawMessage(settings)
	scan := (&Worker{}).Scan(target.Metadata, target.Day)
	peak, stop := sampleStageBytes(root)
	defer stop()
	self, child, available := operationCPU()
	began = time.Now()
	e = d.EqualRemote(ctx, expected, scan, setup)
	report["comparison_seconds"] = time.Since(began).Seconds()
	afterSelf, afterChild, afterAvailable := operationCPU()
	report["self_cpu_seconds"] = afterSelf - self
	report["child_cpu_seconds"] = afterChild - child
	report["usage_available"] = available && afterAvailable
	report["comparison"] = detail
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
	if !detail.Passed || detail.Canonicalization != "local_dictionary" || detail.ReusedInitial || detail.Mode != "fixed" || detail.InitialFanout != 16 || !available || !afterAvailable {
		t.Fatal("incomplete fresh proof", detail)
	}
	// Diagnostic-only full local-input oracle is outside timing and resource
	// sampling, but within the before/after remote-integrity checks.
	checker := d
	var reference ComparisonProfile
	checker.comparisonProfile = &reference
	began = time.Now()
	if err := checker.EqualCanonical(ctx, Canonical(Parquet([]string{expected})), typedDictionaryCanonical(Parquet([]string{expected})), ""); err != nil {
		t.Fatal("independent original-SQL local-input proof failed", err)
	}
	report["independent_input_oracles"] = []map[string]any{{"input": "local_expected", "seconds": time.Since(began).Seconds(), "reference_canonicalization": "baseline", "comparison": reference}}
	began = time.Now()
	after, e := inspectRemoteComparison(reader, target)
	if e != nil || !reflect.DeepEqual(after, pinned) {
		t.Fatal("pinned metadata changed after comparison", e)
	}
	if e := verifyRemoteObjectHashes(ctx, reader, target); e != nil {
		t.Fatal(e)
	}
	report["post_validation_seconds"] = time.Since(began).Seconds()
	report["remote_unchanged"] = true
	report["full_multiset_equal"] = true
	report["metric_scope"] = "fresh production EqualRemote includes setup/counts/all local dictionary work/actual remote scan/staging/refinement/exact proof; excludes hashes/pin validation and diagnostic original-SQL local-input oracle; Iceberg side retains original canonical SQL; resource peaks through measured comparison include admission; OS/server caches uncontrolled; no publication or deletion"
}
