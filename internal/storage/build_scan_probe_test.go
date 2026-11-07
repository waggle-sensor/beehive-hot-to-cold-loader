// build_scan_probe_test.go checks window bounds against every overlapping row group.
// The native scan is opt-in and is not the production build path.

package storage

import (
	"beehive-hot-to-cold-loader/internal/control"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// windowTimeBounds is the time bounds for a window.
type windowTimeBounds struct {
	Offset            int64 `json:"offset"` // the offset of the window
	Rows              int64 `json:"rows"` // the rows of the window
	Min               int64 `json:"min_ns"` // the minimum nanosecond of the window
	Max               int64 `json:"max_ns"` // the maximum nanosecond of the window
	OverlappingGroups int   `json:"overlapping_groups"` // the overlapping groups of the window
	TotalGroups       int   `json:"total_groups"` // the total groups of the window
}

// Test-only candidate: use conservative min/max across ALL physical row groups
// overlapping the unchanged ordinal window. No assumption of global time order.
// expectedWindowBounds returns the expected window bounds.
func expectedWindowBounds(path string, offset, rows int64) (windowTimeBounds, error) {
	b := windowTimeBounds{Offset: offset, Rows: rows}
	r, e := file.OpenParquetFile(path, false)
	if e != nil {
		return b, e
	}
	defer r.Close()
	if offset < 0 || rows < 1 || offset >= r.NumRows() || rows > r.NumRows()-offset {
		return b, errors.New("invalid source window")
	}
	md := r.MetaData()
	idx := md.Schema.ColumnIndexByName("time")
	if idx < 0 {
		return b, errors.New("missing time column")
	}
	logical, ok := md.Schema.Column(idx).LogicalType().(schema.TimestampLogicalType)
	if !ok || logical.TimeUnit() != schema.TimeUnitNanos || !logical.IsAdjustedToUTC() {
		return b, errors.New("source time must be UTC nanoseconds")
	}
	b.TotalGroups = r.NumRowGroups()
	next := int64(0)
	for n := 0; n < r.NumRowGroups(); n++ {
		group := r.RowGroup(n).MetaData()
		end := next + group.NumRows()
		if end > offset && next < offset+rows {
			col, e := group.ColumnChunk(idx)
			if e != nil {
				return b, e
			}
			stats, e := col.Statistics()
			if e != nil {
				return b, e
			}
			s, ok := stats.(*metadata.Int64Statistics)
			if !ok || !s.HasMinMax() || s.Min() > s.Max() || !s.HasNullCount() || s.NullCount() != 0 {
				return b, errors.New("missing/non-null time bounds")
			}
			if b.OverlappingGroups == 0 {
				b.Min = s.Min()
				b.Max = s.Max()
			} else {
				b.Min = min(b.Min, s.Min())
				b.Max = max(b.Max, s.Max())
			}
			b.OverlappingGroups++
		}
		next = end
	}
	if next != r.NumRows() || b.OverlappingGroups == 0 {
		return b, errors.New("invalid row group coverage")
	}
	return b, nil
}

// ordinalWindowSQL returns the ordinal window SQL.
func ordinalWindowSQL(path string, offset, rows int64) string {
	return fmt.Sprintf("SELECT * EXCLUDE(file_row_number) FROM read_parquet(%s,file_row_number=true,hive_partitioning=false) WHERE file_row_number >= %d AND file_row_number < %d", Q(path), offset, offset+rows)
}

// prunedWindowSQL returns the pruned window SQL.
func prunedWindowSQL(base string, b windowTimeBounds) string {
	stamp := func(ns int64) string { return Q(time.Unix(0, ns).UTC().Format(time.RFC3339Nano)) }
	return base + " AND time >= TIMESTAMPTZ_NS " + stamp(b.Min) + " AND time <= TIMESTAMPTZ_NS " + stamp(b.Max)
}

func TestWindowTimeBoundsOverlap(t *testing.T) {
	// Staging fixtures have integer time, so reject rather than treating the unit
	// or timezone as implicit. Unit mismatch must never yield a pruning predicate.
	path := sortedExportFixture(t, []exportRecord{{Record: control.Record{Time: 100, Name: "x", Kind: "int", Value: "1", Tags: map[string]string{"node": "n"}}, Seq: 1}})
	if _, e := expectedWindowBounds(path, 0, 1); e == nil {
		t.Fatal("accepted invalid/empty window")
	}
	if _, e := expectedWindowBounds(path, -1, 1); e == nil {
		t.Fatal("accepted negative window")
	}
	b := windowTimeBounds{Min: 1740787200000000194, Max: 1740787200000000195}
	sql := prunedWindowSQL(ordinalWindowSQL("x.parquet", 10, 20), b)
	if !strings.Contains(sql, "file_row_number >= 10") || !strings.Contains(sql, "file_row_number < 30") || !strings.Contains(sql, ".000000194Z") || !strings.Contains(sql, ".000000195Z") {
		t.Fatal(sql)
	}
}

func TestWindowBoundsUseAllOverlappingGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timestamp.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	fields := arrow.NewSchema([]arrow.Field{{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}}}, nil)
	writer, e := pqarrow.NewFileWriter(fields, f, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(8)), pqarrow.DefaultWriterProps())
	if e != nil {
		t.Fatal(e)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, fields)
	defer builder.Release()
	for _, base := range []int64{1000, 500, 2000} {
		for n := int64(0); n < 8; n++ {
			builder.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(base + n))
		}
		record := builder.NewRecordBatch()
		e = writer.Write(record)
		record.Release()
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = writer.Close(); e != nil {
		t.Fatal(e)
	}
	f.Close()
	for _, tc := range []struct {
		offset, rows, min, max int64
		groups                 int
	}{{7, 2, 500, 1007, 2}, {8, 8, 500, 507, 1}, {23, 1, 2000, 2007, 1}} {
		b, e := expectedWindowBounds(path, tc.offset, tc.rows)
		if e != nil {
			t.Fatal(e)
		}
		if b.Min != tc.min || b.Max != tc.max || b.OverlappingGroups != tc.groups || b.TotalGroups != 3 {
			t.Fatal(tc, b)
		}
	}
}

func TestNativeBuildScanGate(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("native DuckDB 2 required")
	}
	d := DuckDB{Binary: binary, Work: t.TempDir(), Memory: "512MB", Threads: 16}
	ctx := context.Background()
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(d.Work, "fixture.parquet")
	// Equal timestamps cross boundaries; time is deliberately not globally sorted.
	_, e := d.Run(ctx, "COPY (SELECT CAST(make_timestamp_ns(1740787200000000194+(CASE WHEN i<2048 THEN 2000 WHEN i<4096 THEN 0 WHEN i<6144 THEN 4000 WHEN i<8192 THEN 1000 ELSE 3000 END)+(i%17)::BIGINT) AS TIMESTAMPTZ_NS) AS time,i::BIGINT AS id FROM range(10000) r(i)) TO "+Q(path)+" (FORMAT parquet,ROW_GROUP_SIZE 2048);")
	if e != nil {
		t.Fatal(e)
	}
	for _, offset := range []int64{0, 2047, 2048, 4095, 9999} {
		rows := min(int64(2050), 10000-offset)
		b, e := expectedWindowBounds(path, offset, rows)
		if e != nil {
			t.Fatal(e)
		}
		base := ordinalWindowSQL(path, offset, rows)
		candidate := prunedWindowSQL(base, b)
		if _, e = d.Run(ctx, exactPartitionSQL(base, candidate)); e != nil {
			t.Fatal("pruned window differs", offset, e)
		}
	}
}

func TestNativeRetainedBuildScan(t *testing.T) {
	source := os.Getenv("SAGE_BUILD_SCAN_SOURCE")
	if source == "" {
		t.Skip("retained build scan probe only")
	}
	root := os.Getenv("SAGE_BUILD_SCAN_DIR")
	if root == "" {
		t.Fatal("fresh result directory required")
	}
	var e error
	source, e = localPath(source)
	if e != nil {
		t.Fatal(e)
	}
	root, e = localPath(root)
	if e != nil {
		t.Fatal(e)
	}
	rel, e := filepath.Rel(filepath.Dir(source), root)
	if e != nil || !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatal("scratch must be outside source directory")
	}
	if e = os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	rows, e := strconv.ParseInt(os.Getenv("SAGE_BUILD_SCAN_ROWS"), 10, 64)
	if e != nil || rows < 1 {
		t.Fatal("positive audited row count required")
	}
	actual, e := partitionRows([]string{source})
	if e != nil || actual != rows {
		t.Fatal("source rows differ", actual, e)
	}
	d := DuckDB{Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: root, Memory: os.Getenv("SAGE_BUILD_SCAN_MEMORY"), Threads: 16, SortThreads: 2}
	ctx := context.Background()
	report := map[string]any{"passed": false, "rows": rows, "threads": 16, "sort_threads": 2, "memory": d.Memory, "scope": "test-only conservative time pruning; original ordinal predicate retained; no materialization, packing, publish, Influx, Pelican or deletion; profiles and caches affect timing"}
	before, e := digest(source)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		after, e := digest(source)
		same := e == nil && after == before
		if !same {
			t.Error("source changed", e)
		}
		report["source_sha256"] = before
		report["source_sha256_after"] = after
		report["source_unchanged"] = same
		report["passed"] = !t.Failed()
		if e = writeJSON(filepath.Join(root, "probe.json"), report); e != nil {
			t.Error(e)
		}
	})
	if e = d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	last := (rows - 1) / fileRows
	offsets := []int64{0}
	if last > 1 {
		offsets = append(offsets, (last/2)*fileRows)
	}
	if last > 0 {
		offsets = append(offsets, last*fileRows)
	}
	entries := []map[string]any{}
	for _, offset := range offsets {
		n := min(fileRows, rows-offset)
		bounds, e := expectedWindowBounds(source, offset, n)
		if e != nil {
			t.Fatal(e)
		}
		base := ordinalWindowSQL(source, offset, n)
		for pass, arm := range []string{"ordinal", "time_pruned", "time_pruned", "ordinal"} {
			label := fmt.Sprintf("window-%d-%d-%s", offset, pass+1, arm)
			work := filepath.Join(root, label)
			if e = os.Mkdir(work, 0700); e != nil {
				t.Fatal(e)
			}
			sql := base
			if arm == "time_pruned" {
				sql = prunedWindowSQL(base, bounds)
			}
			entry := map[string]any{"arm": arm, "label": label, "bounds": bounds, "passed": false}
			entries = append(entries, entry)
			report["windows"] = entries
			for _, phase := range []string{"sort_copy", "canonical_source_copy"} {
				duck := d
				duck.Work = work
				if phase == "sort_copy" {
					duck.Threads = 2
				}
				profile := filepath.Join(work, phase+".duckdb.json")
				duck.testSetup = "SET enable_profiling='json';SET profiling_output=" + Q(profile) + ";"
				output := filepath.Join(work, "sorted.parquet")
				statement := CopySQL(sql+" ORDER BY plugin NULLS LAST,vsn NULLS LAST,time NULLS LAST", output)
				if phase == "canonical_source_copy" {
					statement = canonicalCopy(Canonical(sql), filepath.Join(work, "canonical"), 0)
					settings, _ := duck.comparisonWriterSettings()
					statement = settings + statement
				}
				self, child, usage := operationCPU()
				began := time.Now()
				_, e = duck.Run(ctx, statement)
				wall := time.Since(began).Seconds()
				afterSelf, afterChild, afterUsage := operationCPU()
				entry[phase] = map[string]any{"seconds": wall, "self_cpu_seconds": afterSelf - self, "child_cpu_seconds": afterChild - child, "usage_available": usage && afterUsage, "threads": duck.Threads, "profile": profile}
				if e != nil {
					t.Fatal(e)
				}
				if !usage || !afterUsage {
					t.Fatal("CPU evidence unavailable")
				}
				raw, e := os.ReadFile(profile)
				if e != nil {
					t.Fatal(e)
				}
				var parsed map[string]any
				if e = json.Unmarshal(raw, &parsed); e != nil {
					t.Fatal(e)
				}
				if phase == "sort_copy" {
					count, e := partitionRows([]string{output})
					if e != nil || count != n {
						t.Fatal("sorted window count differs", count, e)
					}
				} else {
					_, count, e := partitionInventory(filepath.Join(work, "canonical"), 16)
					if e != nil || count != n {
						t.Fatal("canonical window count differs", count, e)
					}
				}
			}
			checker := d
			checker.Work = work
			var proof ComparisonProfile
			checker.comparisonProfile = &proof
			began := time.Now()
			e = checker.Equal(ctx, base, Parquet([]string{filepath.Join(work, "sorted.parquet")}), "")
			entry["verification_seconds"] = time.Since(began).Seconds()
			entry["verification"] = proof
			if e != nil {
				t.Fatal(e)
			}
			entry["full_window_equal"] = true
			entry["passed"] = true
		}
	}
}

func TestRetainedBuildWindowFooters(t *testing.T) {
	source := os.Getenv("SAGE_BUILD_SCAN_SOURCE")
	if source == "" {
		t.Skip("retained source footer inspection only")
	}
	source, e := localPath(source)
	if e != nil {
		t.Fatal(e)
	}
	rows, e := partitionRows([]string{source})
	if e != nil || rows < 1 {
		t.Fatal(rows, e)
	}
	last := (rows - 1) / fileRows
	offsets := []int64{0}
	if last > 1 {
		offsets = append(offsets, last/2*fileRows)
	}
	if last > 0 {
		offsets = append(offsets, last*fileRows)
	}
	for _, offset := range offsets {
		b, e := expectedWindowBounds(source, offset, min(fileRows, rows-offset))
		if e != nil {
			t.Fatal(e)
		}
		t.Logf("offset=%d rows=%d overlapping_groups=%d/%d min_ns=%d max_ns=%d", b.Offset, b.Rows, b.OverlappingGroups, b.TotalGroups, b.Min, b.Max)
	}
}
