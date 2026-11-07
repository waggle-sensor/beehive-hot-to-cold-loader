// staging_test.go checks that compressed staging round-trips every record across
// batch seams, and that collapse publishes the compressed winner.

package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"beehive-hot-to-cold-loader/internal/control"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// visitStagingFixture visits a staging fixture.
func visitStagingFixture(path string, visit func(exportRecord) error) error {
	r, e := file.OpenParquetFile(path, false)
	if e != nil {
		return e
	}
	defer r.Close()
	ar, e := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{BatchSize: 8192}, memory.DefaultAllocator)
	if e != nil {
		return e
	}
	rr, e := ar.GetRecordReader(context.Background(), nil, nil)
	if e != nil {
		return e
	}
	defer rr.Release()
	for rr.Next() {
		b := rr.RecordBatch()
		for n := 0; n < int(b.NumRows()); n++ {
			row := exportRecord{Record: control.Record{Time: b.Column(0).(*array.Int64).Value(n), Name: strings.Clone(b.Column(1).(*array.String).Value(n)), Kind: strings.Clone(b.Column(3).(*array.String).Value(n)), Value: strings.Clone(b.Column(4).(*array.String).Value(n))}, Seq: b.Column(5).(*array.Int64).Value(n)}
			if e = json.Unmarshal([]byte(b.Column(2).(*array.String).Value(n)), &row.Tags); e != nil {
				return e
			}
			if e = visit(row); e != nil {
				return e
			}
		}
	}
	return rr.Err()
}

// TestCompressedStagingPreservesExactRecordsAndSeams tests that a compressed staging preserves exact records and seams.
func TestCompressedStagingPreservesExactRecordsAndSeams(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	enc, finish, release, e := newRecordEncoder(f, path)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	values := []struct{ kind, value string }{{"int", "9007199254740993"}, {"uint", "18446744073709551615"}, {"float", "-0"}, {"bool", "true"}, {"string", "a\n\nquote \" slash \\ literal\\n"}}
	var logical int64
	for n := 0; n < 65539; n++ {
		v := values[n%len(values)]
		r := exportRecord{Record: control.Record{Time: 1740787200000000194 + int64(n), Name: "fixture", Tags: map[string]string{"sensor": "comma, equals= newline-free", "plugin": "p"}, Kind: v.kind, Value: v.value}, Seq: int64(n + 1)}
		if e = enc.Encode(r); e != nil {
			t.Fatal(e)
		}
		b, _ := json.Marshal(r)
		logical += int64(len(b) + 1)
	}
	if e = finish(); e != nil {
		t.Fatal(e)
	}
	if e = f.Sync(); e != nil {
		t.Fatal("Parquet finish closed the file before sync", e)
	}
	var n int64
	e = visitStagingFixture(path, func(r exportRecord) error {
		v := values[n%int64(len(values))]
		if r.Time != 1740787200000000194+n || r.Seq != n+1 || r.Value != v.value || r.Kind != v.kind || r.Tags["sensor"] != "comma, equals= newline-free" {
			t.Fatalf("staging changed record %d: %+v", n, r)
		}
		n++
		return nil
	})
	if e != nil || n != 65539 {
		t.Fatal(n, e)
	}
	info, e := os.Stat(path)
	if e != nil {
		t.Fatal(e)
	}
	if info.Size() >= logical/5 {
		t.Fatalf("compressed staging unexpectedly large: %d / %d", info.Size(), logical)
	}
}

// TestCollapseExportPublishesCompressedWinner tests that a collapse export publishes compressed winner.
func TestCollapseExportPublishesCompressedWinner(t *testing.T) {
	r := exportRecord{Record: control.Record{Time: 1740787200000000194, Name: "x", Tags: map[string]string{"node": "n"}, Kind: "string", Value: "first\nsecond"}, Seq: 9}
	old := r
	old.Seq = 1
	old.Value = "old"
	input := sortedExportFixture(t, []exportRecord{r, old})
	path := filepath.Join(t.TempDir(), "normalized.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	enc, finish, release, e := newRecordEncoder(f, path)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if n, unique, e := collapseExport(context.Background(), input, enc, 1); e != nil || n != 2 || unique != 1 {
		t.Fatal(n, unique, e)
	}
	if e = finish(); e != nil {
		t.Fatal(e)
	}
	n := 0
	if e = visitStagingFixture(path, func(got exportRecord) error {
		if got.Seq != 9 || got.Value != r.Value || got.Time != r.Time || got.Tags["node"] != "n" {
			t.Fatal(got)
		}
		n++
		return nil
	}); e != nil || n != 1 {
		t.Fatal(n, e)
	}
}
