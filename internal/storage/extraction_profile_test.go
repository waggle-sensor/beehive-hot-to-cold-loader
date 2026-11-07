// extraction_profile_test.go measures CSV extraction CPU and allocation. The retained
// profile is opt-in; the small round trip runs without that source.

package storage

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

// extractionSink is a test sink for the extraction profile.
type extractionSink struct{ rows int64 }

// Encode encodes a value into the sink.
func (s *extractionSink) Encode(v any) error {
	if _, ok := v.(control.Record); !ok {
		return errors.New("unexpected decoded record")
	}
	s.rows++
	return nil
}

// Count canonical records through the same optimized decoder branch as Parquet.
func (s *extractionSink) encodeCSV(v stagingView) error { s.rows++; return nil }

// extractionCollector is a extraction collector.
type extractionCollector struct{ rows []control.Record }

// Encode encodes a value.
func (s *extractionCollector) Encode(v any) error {
	r, ok := v.(control.Record)
	if !ok {
		return errors.New("unexpected decoded record")
	}
	s.rows = append(s.rows, r)
	return nil
}

// Reconstruct a bounded, typed annotated CSV fixture, emitting a table whenever
// series/type changes. It is not a captured HTTP response or an Influx oracle.
// extractionCSV returns the extraction CSV.
func extractionCSV(ctx context.Context, source, target string, limit int) (records []control.Record, err error) {
	c, e := openStagingCursor(ctx, source, 8192)
	if e != nil {
		return nil, e
	}
	defer c.close()
	f, e := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	w := csv.NewWriter(f)
	last := ""
	table := 0
	for len(records) < limit && c.next() {
		v := c.head
		r := control.Record{Time: v.Time, Name: strings.Clone(v.Name), Kind: strings.Clone(v.Kind), Value: strings.Clone(v.Value)}
		if e = json.Unmarshal([]byte(v.Tags), &r.Tags); e != nil {
			return nil, e
		}
		if e = r.Validate(); e != nil {
			return nil, e
		}
		typ, ok := map[string]string{"int": "long", "uint": "unsignedLong", "float": "double", "bool": "boolean", "string": "string"}[r.Kind]
		if !ok {
			return nil, errors.New("unsupported primitive scalar kind")
		}
		keys := make([]string, 0, len(r.Tags))
		for k := range r.Tags {
			if k == "" || k == "result" || k == "table" || strings.HasPrefix(k, "_") {
				return nil, errors.New("fixture contains reserved Flux tag key")
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		identity := r.Name + "\x00" + r.Kind + "\x00" + v.Tags
		if len(records) == 0 || identity != last {
			table++
			types := []string{"#datatype", "string", "long", "dateTime:RFC3339", typ, "string", "string"}
			defaults := []string{"#default", "_result", "", "", "", "", ""}
			names := []string{"", "result", "table", "_time", "_value", "_field", "_measurement"}
			for _, k := range keys {
				types = append(types, "string")
				defaults = append(defaults, "")
				names = append(names, k)
			}
			for _, line := range [][]string{types, defaults, names} {
				if e = w.Write(line); e != nil {
					return nil, e
				}
			}
			last = identity
		}
		line := []string{"", "", strconv.Itoa(table), time.Unix(0, r.Time).UTC().Format(time.RFC3339Nano), r.Value, "value", r.Name}
		for _, k := range keys {
			line = append(line, r.Tags[k])
		}
		if e = w.Write(line); e != nil {
			return nil, e
		}
		records = append(records, r)
	}
	if c.err != nil {
		return nil, c.err
	}
	if len(records) != limit {
		return nil, errors.New("source has fewer rows than requested sample")
	}
	w.Flush()
	if e = w.Error(); e != nil {
		return nil, e
	}
	if e = f.Sync(); e != nil {
		return nil, e
	}
	return records, nil
}

// checkExtractionOutput checks the extraction output.
func checkExtractionOutput(t *testing.T, path string, want []control.Record) {
	t.Helper()
	n := 0
	if e := visitStagingFixture(path, func(r exportRecord) error {
		if n >= len(want) || r.Seq != 0 || !reflect.DeepEqual(r.Record, want[n]) {
			return fmt.Errorf("extraction changed record %d", n)
		}
		n++
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if n != len(want) {
		t.Fatal("extraction output row count differs", n, len(want))
	}
}

// extractionPass passes the extraction.
func extractionPass(mode, csvPath, output string, records []control.Record, rows int) error {
	var enc recordEncoder
	var f *os.File
	var finish func() error
	if mode == "decode" {
		enc = &extractionSink{}
	} else {
		var e error
		f, e = os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		defer f.Close()
		var release func()
		enc, finish, release, e = newRecordEncoder(f, output)
		if e != nil {
			return e
		}
		defer release()
	}
	if mode == "encode" {
		for _, r := range records {
			if e := enc.Encode(r); e != nil {
				return e
			}
		}
	} else {
		in, e := os.Open(csvPath)
		if e != nil {
			return e
		}
		e = DecodeCSV(in, enc, 0, 1<<63-1)
		in.Close()
		if e != nil {
			return e
		}
	}
	if sink, ok := enc.(*extractionSink); ok && sink.rows != int64(rows) {
		return errors.New("decode count differs")
	}
	if finish != nil {
		if e := finish(); e != nil {
			return e
		}
		if e := f.Sync(); e != nil {
			return e
		}
		return f.Close()
	}
	return nil
}

// extractionProfile runs the extraction profile.
func extractionProfile(t *testing.T, root, mode, csvPath string, records []control.Record, rows, reps int, top bool) map[string]any {
	t.Helper()
	dir := filepath.Join(root, mode)
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	runtime.GC()
	writeAlloc := func(name string) {
		f, e := os.Create(filepath.Join(dir, name))
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		if e = pprof.Lookup("allocs").WriteTo(f, 0); e != nil {
			t.Fatal(e)
		}
	}
	writeAlloc("alloc-before.pprof")
	cpu, e := os.Create(filepath.Join(dir, "cpu.pprof"))
	if e != nil {
		t.Fatal(e)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if e = pprof.StartCPUProfile(cpu); e != nil {
		t.Fatal(e)
	}
	self, child, available := operationCPU()
	began := time.Now()
	var outputs []string
	for n := 0; n < reps; n++ {
		output := filepath.Join(dir, fmt.Sprintf("output-%d.parquet", n))
		if mode != "decode" {
			outputs = append(outputs, output)
		}
		if e = extractionPass(mode, csvPath, output, records, rows); e != nil {
			break
		}
	}
	wall := time.Since(began).Seconds()
	afterSelf, afterChild, afterAvailable := operationCPU()
	pprof.StopCPUProfile()
	cpu.Close()
	runtime.ReadMemStats(&after)
	if e != nil {
		t.Fatal(e)
	}
	if !available || !afterAvailable {
		t.Fatal("CPU metrics unavailable")
	}
	runtime.GC()
	writeAlloc("alloc-after.pprof")
	// Verification and pprof report generation are outside timed/profiled work.
	if len(outputs) > 0 && records == nil {
		in, e := os.Open(csvPath)
		if e != nil {
			t.Fatal(e)
		}
		collector := &extractionCollector{}
		e = DecodeCSV(in, collector, 0, 1<<63-1)
		in.Close()
		if e != nil {
			t.Fatal(e)
		}
		records = collector.rows
	}
	for _, path := range outputs {
		checkExtractionOutput(t, path, records)
	}
	if top {
		exe, e := os.Executable()
		if e != nil {
			t.Fatal(e)
		}
		for _, item := range []struct {
			name string
			args []string
		}{
			{"cpu-top.txt", []string{"tool", "pprof", "-top", "-nodecount=40", exe, filepath.Join(dir, "cpu.pprof")}},
			{"alloc-top.txt", []string{"tool", "pprof", "-top", "-nodecount=40", "-sample_index=alloc_space", "-base", filepath.Join(dir, "alloc-before.pprof"), exe, filepath.Join(dir, "alloc-after.pprof")}},
		} {
			f, e := os.Create(filepath.Join(dir, item.name))
			if e != nil {
				t.Fatal(e)
			}
			cmd := exec.Command("go", item.args...)
			cmd.Stdout = f
			cmd.Stderr = f
			e = cmd.Run()
			f.Close()
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	return map[string]any{"mode": mode, "rows_per_rep": rows, "reps": reps, "seconds": wall, "rows_per_second": float64(rows*reps) / wall, "self_cpu_seconds": afterSelf - self, "child_cpu_seconds": afterChild - child, "usage_available": true, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "mallocs": after.Mallocs - before.Mallocs, "gc_cycles": after.NumGC - before.NumGC, "output_equal": mode != "decode", "count_equal": true, "outputs": outputs}
}

func TestRetainedExtractionProfile(t *testing.T) {
	source := os.Getenv("SAGE_EXTRACTION_SOURCE")
	if source == "" {
		t.Skip("retained extraction profile only")
	}
	root := os.Getenv("SAGE_EXTRACTION_DIR")
	if root == "" {
		t.Fatal("fresh profile directory required")
	}
	source, e := localPath(source)
	if e != nil {
		t.Fatal(e)
	}
	root, e = localPath(root)
	if e != nil {
		t.Fatal(e)
	}
	rel, e := filepath.Rel(filepath.Dir(source), root)
	if e != nil || !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatal("profile scratch must be outside source directory")
	}
	if e = os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	rows, e := strconv.Atoi(os.Getenv("SAGE_EXTRACTION_ROWS"))
	if e != nil || rows < 1 || rows > 1_000_000 {
		t.Fatal("sample rows must be 1..1000000")
	}
	reps, e := strconv.Atoi(os.Getenv("SAGE_EXTRACTION_REPS"))
	if e != nil || reps < 1 || reps > 20 {
		t.Fatal("reps must be 1..20")
	}
	before, e := digest(source)
	if e != nil {
		t.Fatal(e)
	}
	report := map[string]any{"implementation": "cached-column-plan-tags", "passed": false, "source_sha256": before, "rows": rows, "reps": reps, "logical_cpus": runtime.NumCPU(), "go_max_procs": runtime.GOMAXPROCS(0), "fixture_kind": "derived annotated CSV prefix, not captured HTTP"}
	t.Cleanup(func() {
		after, e := digest(source)
		unchanged := e == nil && after == before
		if !unchanged {
			t.Error("retained source changed", e)
		}
		report["source_sha256_after"] = after
		report["source_unchanged"] = unchanged
		report["passed"] = !t.Failed()
		if e = writeJSON(filepath.Join(root, "profile.json"), report); e != nil {
			t.Error(e)
		}
	})
	csvPath := filepath.Join(root, "sample.csv")
	records, e := extractionCSV(context.Background(), source, csvPath, rows)
	if e != nil {
		t.Fatal(e)
	}
	// Independently compare derived CSV decoding with the original primitive rows.
	f, e := os.Open(csvPath)
	if e != nil {
		t.Fatal(e)
	}
	collector := &extractionCollector{}
	e = DecodeCSV(f, collector, 0, 1<<63-1)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(records, collector.rows) {
		t.Fatal("derived CSV changed source records")
	}
	collector = nil
	info, e := os.Stat(csvPath)
	if e != nil {
		t.Fatal(e)
	}
	report["csv_bytes"] = info.Size()
	meta, e := os.Open(csvPath)
	if e != nil {
		t.Fatal(e)
	}
	reader := csv.NewReader(meta)
	reader.FieldsPerRecord = -1
	tables := 0
	for {
		row, e := reader.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			meta.Close()
			t.Fatal(e)
		}
		if len(row) > 0 && row[0] == "#datatype" {
			tables++
		}
	}
	meta.Close()
	report["derived_table_count"] = tables

	report["csv_source_equal"] = true
	kinds := map[string]int{}
	names := map[string]bool{}
	tagKeys := map[string]bool{}
	for _, r := range records {
		kinds[r.Kind]++
		names[r.Name] = true
		for k := range r.Tags {
			tagKeys[k] = true
		}
	}
	report["scalar_kinds"] = kinds
	report["measurement_count"] = len(names)
	report["tag_key_count"] = len(tagKeys)

	// Keep the streaming phases free of a retained expected-record array.
	records = nil
	phases := []map[string]any{}
	for _, mode := range []string{"decode", "encode", "full"} {
		var input []control.Record
		if mode == "encode" {
			f, e := os.Open(csvPath)
			if e != nil {
				t.Fatal(e)
			}
			collector := &extractionCollector{}
			e = DecodeCSV(f, collector, 0, 1<<63-1)
			f.Close()
			if e != nil {
				t.Fatal(e)
			}
			input = collector.rows
		}
		phases = append(phases, extractionProfile(t, root, mode, csvPath, input, rows, reps, true))
	}
	report["phases"] = phases
	report["scope"] = "CPU and allocation profiles cover local warm CSV replay/encoding, including file open/finalization/fsync; fixture construction, hashes, independent record checks and pprof reports excluded; derived source prefix; encode-only preloads records, streaming modes do not; no Influx/network/production changes; phase costs are separate experiments and not additive"
}

func TestExtractionProfileRoundTrip(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.parquet")
	values := []struct{ kind, value string }{{"int", "9007199254740993"}, {"uint", "18446744073709551615"}, {"float", "11.7"}, {"bool", "true"}, {"string", "line\nliteral\\n\""}}
	_, e := writeStaging(source, source, func(enc recordEncoder) (int64, error) {
		for n, v := range values {
			r := control.Record{Time: 1740787200000000194 + int64(n), Name: "fixture", Tags: map[string]string{"node": "n1", "sensor": "a,b=c\n\"\\<&", "unicode": "☃"}, Kind: v.kind, Value: v.value}
			if e := enc.Encode(exportRecord{Record: r, Seq: int64(n + 1)}); e != nil {
				return int64(n), e
			}
		}
		return int64(len(values)), nil
	})
	if e != nil {
		t.Fatal(e)
	}
	csvPath := filepath.Join(root, "fixture.csv")
	records, e := extractionCSV(context.Background(), source, csvPath, len(values))
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"decode", "encode", "full"} {
		path := filepath.Join(root, mode+".parquet")
		if e = extractionPass(mode, csvPath, path, records, len(records)); e != nil {
			t.Fatal(e)
		}
		if mode != "decode" {
			checkExtractionOutput(t, path, records)
		}
	}
	f, e := os.Open(csvPath)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	collector := &extractionCollector{}
	if e = DecodeCSV(f, collector, 0, 1<<63-1); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(records, collector.rows) {
		t.Fatal("CSV fixture changed typed records")
	}
	if _, e = extractionCSV(context.Background(), source, filepath.Join(root, "short.csv"), len(values)+1); e == nil {
		t.Fatal("accepted short input")
	}
}
