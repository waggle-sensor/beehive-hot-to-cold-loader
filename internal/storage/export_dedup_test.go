// export_dedup_test.go checks half-open day bounds and replay order. Collapse keeps
// the first row of an identity and rejects a later row that is not a lower sequence.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"beehive-hot-to-cold-loader/internal/control"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// TestStreamExportRetainsReplayOrderAndHalfOpenBounds tests that a stream export retains replay order and half-open bounds.
func TestStreamExportRetainsReplayOrderAndHalfOpenBounds(t *testing.T) {
	const start int64 = 1740787200000000000
	const end = start + 86400000000000
	input := `t,plugin=p,node=n1 value=11.7 1740787200000000194
count,node=n1 value=9007199254740993i 1740787200000000195
t,node=n1,plugin=p value=11.9 1740787200000000194
status value="123" 1740873599999999999
boundary value=1.0 1740873600000000000
`
	var out bytes.Buffer
	if e := streamExport(strings.NewReader(input), json.NewEncoder(&out), start, end); e != nil {
		t.Fatal(e)
	}
	var rows []exportRecord
	dec := json.NewDecoder(&out)
	for {
		var r exportRecord
		e := dec.Decode(&r)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		rows = append(rows, r)
	}
	if len(rows) != 4 {
		t.Fatal(rows)
	}
	for i, r := range rows {
		if r.Seq != int64(i+1) {
			t.Fatal(r)
		}
	}
	if rows[0].Value != "11.7" || rows[2].Value != "11.9" || rows[2].Time != start+194 {
		t.Fatal(rows)
	}
	if rows[1].Kind != "int" || rows[1].Value != "9007199254740993" {
		t.Fatal(rows[1])
	}
	if rows[3].Time != end-1 || rows[3].Kind != "string" || len(rows[3].Tags) != 0 {
		t.Fatal(rows[3])
	}
}

// emissionReader is a reader that emits a single record.
type emissionReader struct {
	first   bool // whether the first record has been emitted
	emitted *bool // whether the record has been emitted
}

// Read reads a record.
func (r *emissionReader) Read(p []byte) (int, error) {
	if !r.first {
		r.first = true
		return copy(p, "t,node=n1 value=1i 1740787200000000194\n"), nil
	}
	if !*r.emitted {
		return 0, errors.New("source consumed to EOF before emitting its first record")
	}
	return 0, io.EOF
}

// emissionWriter is a writer that emits a single record.
type emissionWriter struct{ emitted *bool }

// Write writes a record.
func (w emissionWriter) Write(p []byte) (int, error) { *w.emitted = true; return len(p), nil }

// TestStreamExportEmitsBeforeSourceEOF tests that a stream export emits before the source EOF.
func TestStreamExportEmitsBeforeSourceEOF(t *testing.T) {
	emitted := false
	e := streamExport(&emissionReader{emitted: &emitted}, json.NewEncoder(emissionWriter{&emitted}), 1740787200000000000, 1740873600000000000)
	if e != nil || !emitted {
		t.Fatal(e, emitted)
	}
}

// TestDeduplicateExportFailurePreservesPublishedOutput tests that a deduplicate export failure preserves the published output.
func TestDeduplicateExportFailurePreservesPublishedOutput(t *testing.T) {
	root := t.TempDir()
	raw := filepath.Join(root, "raw.ndjson")
	output := filepath.Join(root, "normalized.ndjson")
	binary := filepath.Join(root, "failing-duckdb")
	for path, contents := range map[string]string{
		raw: "raw input", output: "previous complete output",
		output + ".pending":              "interrupted output",
		output + ".sort.parquet.pending": "interrupted sort",
		binary:                           "#!/bin/sh\nexit 1\n",
	} {
		if e := os.WriteFile(path, []byte(contents), 0700); e != nil {
			t.Fatal(e)
		}
	}
	duck := DuckDB{Binary: binary, Work: root}
	if _, e := duck.DeduplicateExport(context.Background(), raw, output); e == nil {
		t.Fatal("failed sort must stop normalization")
	}
	contents, e := os.ReadFile(output)
	if e != nil || string(contents) != "previous complete output" {
		t.Fatal("failed normalization replaced published output", string(contents), e)
	}
	for _, path := range []string{output + ".pending", output + ".sort.parquet.pending"} {
		if _, e := os.Stat(path); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("scratch artifact survived failed attempt", path, e)
		}
	}
}

// TestStreamExportRejectsOutsideDayAndUnknownFields tests that a stream export rejects outside day and unknown fields.
func TestStreamExportRejectsOutsideDayAndUnknownFields(t *testing.T) {
	for _, line := range []string{
		"t value=1i 1740787199999999999\n",
		"t value=1i 1740873600000000001\n",
		"t other=1i 1740787200000000194\n",
	} {
		if e := streamExport(strings.NewReader(line), json.NewEncoder(io.Discard), 1740787200000000000, 1740873600000000000); e == nil {
			t.Fatal(line)
		}
	}
}

// sortedExportFixture creates a sorted export fixture.
func sortedExportFixture(t *testing.T, rows []exportRecord) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sorted.parquet")
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String}, {Name: "tags", Type: arrow.BinaryTypes.String},
		{Name: "kind", Type: arrow.BinaryTypes.String}, {Name: "value", Type: arrow.BinaryTypes.String},
		{Name: "seq", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, r := range rows {
		tags, e := json.Marshal(r.Tags)
		if e != nil {
			t.Fatal(e)
		}
		builder.Field(0).(*array.Int64Builder).Append(r.Time)
		for i, value := range []string{r.Name, string(tags), r.Kind, r.Value} {
			builder.Field(i + 1).(*array.StringBuilder).Append(value)
		}
		builder.Field(5).(*array.Int64Builder).Append(r.Seq)
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	writer, e := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(3)), pqarrow.DefaultWriterProps())
	if e != nil {
		t.Fatal(e)
	}
	if e = writer.Write(batch); e != nil {
		t.Fatal(e)
	}
	if e = writer.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}

func TestCollapseSortedExportAcrossBatchesAndValueKinds(t *testing.T) {
	record := func(stamp int64, tags map[string]string, kind, value string, seq int64) exportRecord {
		return exportRecord{Record: control.Record{Time: stamp, Name: "t", Tags: tags, Kind: kind, Value: value}, Seq: seq}
	}
	const stamp int64 = 1740787200000000194
	rows := []exportRecord{
		record(stamp, map[string]string{"node": "n1"}, "string", "123", 9),
		record(stamp, map[string]string{"node": "n1"}, "int", "9007199254740993", 7),
		record(stamp, map[string]string{"node": "n1"}, "float", "11.7", 5),
		// Delimiter-bearing metadata must not collide with another tag map.
		record(stamp, map[string]string{"a": "b\x00c\x00d"}, "bool", "true", 8),
		record(stamp, map[string]string{"a": "b", "c": "d"}, "uint", "18446744073709551615", 6),
		record(stamp+1, map[string]string{}, "int", "9007199254740993", 1),
	}
	sort.Slice(rows, func(i, j int) bool {
		a, _ := json.Marshal(rows[i].Tags)
		b, _ := json.Marshal(rows[j].Tags)
		order := compareExportIdentity(exportIdentity{rows[i].Time, rows[i].Name, string(a)}, exportIdentity{rows[j].Time, rows[j].Name, string(b)})
		if order == 0 {
			return rows[i].Seq > rows[j].Seq
		}
		return order < 0
	})
	path := sortedExportFixture(t, rows)
	var out bytes.Buffer
	input, unique, e := collapseExport(context.Background(), path, json.NewEncoder(&out), 2)
	if e != nil || input != 6 || unique != 4 {
		t.Fatal(input, unique, e)
	}
	seen := map[int64]exportRecord{}
	dec := json.NewDecoder(&out)
	for {
		var r exportRecord
		e := dec.Decode(&r)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		seen[r.Seq] = r
	}
	if seen[9].Kind != "string" || seen[9].Value != "123" || seen[9].Time != stamp {
		t.Fatal(seen)
	}
	if seen[1].Value != "9007199254740993" || seen[8].Kind != "bool" || seen[6].Kind != "uint" {
		t.Fatal(seen)
	}
	if _, ok := seen[7]; ok {
		t.Fatal("superseded integer retained")
	}
}

// TestCollapseRejectsBadOrderAndMissingSequence tests that a collapse rejects bad order and missing sequence.
func TestCollapseRejectsBadOrderAndMissingSequence(t *testing.T) {
	base := control.Record{Time: 1740787200000000194, Name: "t", Tags: map[string]string{}, Kind: "int", Value: "1"}
	for _, sequences := range [][]int64{{1, 2}, {2, 2}, {0}} {
		var rows []exportRecord
		for _, seq := range sequences {
			rows = append(rows, exportRecord{Record: base, Seq: seq})
		}
		if _, _, e := collapseExport(context.Background(), sortedExportFixture(t, rows), json.NewEncoder(io.Discard), 1); e == nil {
			t.Fatal(sequences)
		}
	}
}

func TestCollapseEmptyExport(t *testing.T) {
	var out bytes.Buffer
	total, unique, e := collapseExport(context.Background(), sortedExportFixture(t, nil), json.NewEncoder(&out), 2)
	if e != nil || total != 0 || unique != 0 || out.Len() != 0 {
		t.Fatal(total, unique, e, out.String())
	}
}
