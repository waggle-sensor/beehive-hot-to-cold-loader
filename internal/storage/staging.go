// staging.go writes primitive Parquet for exports, queries, and normalized
// merges. NDJSON remains available for small fixtures.

package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"beehive-hot-to-cold-loader/internal/control"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type recordEncoder interface{ Encode(any) error }

// stagingRecord keeps canonical tag JSON without constructing a tag map again.
type stagingRecord struct {
	Time  int64           `json:"time"`
	Name  string          `json:"name"`
	Tags  json.RawMessage `json:"tags"`
	Kind  string          `json:"kind"`
	Value string          `json:"value"`
	Seq   int64           `json:"seq"`
}

// Views refer to a live reader batch; Append copies them into the output
// builder. Keeping tag JSON as a string avoids a map/JSON round trip in merges.
type stagingView struct {
	Time                    int64
	Name, Tags, Kind, Value string
	Seq                     int64
}

// emitStaging writes a view directly into Parquet. Other encoders receive JSON.
func emitStaging(enc recordEncoder, r stagingView) error {
	if _, ok := enc.(*parquetRecords); ok {
		return enc.Encode(r)
	}
	return enc.Encode(stagingRecord{r.Time, r.Name, json.RawMessage(r.Tags), r.Kind, r.Value, r.Seq})
}

var stagingSchema = arrow.NewSchema([]arrow.Field{
	{Name: "time", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "tags", Type: arrow.BinaryTypes.String},
	{Name: "kind", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.BinaryTypes.String},
	{Name: "seq", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type parquetRecords struct {
	b     *array.RecordBuilder
	w     *pqarrow.FileWriter
	path  string
	bytes int
}

// The NDJSON option remains useful for small independent fixtures. Production
// exports/query copies and normalized output use primitive compressed Parquet.
func newRecordEncoder(f *os.File, path string) (recordEncoder, func() error, func(), error) {
	if !strings.HasSuffix(path, ".parquet") {
		buffer := bufio.NewWriterSize(f, 1<<20)
		return json.NewEncoder(buffer), buffer.Flush, func() {}, nil
	}
	if _, e := stagingSpace(path); e != nil {
		return nil, nil, nil, e
	}
	// Hide Close: the caller must sync the completed footer before closing f.
	w, e := pqarrow.NewFileWriter(stagingSchema, struct{ io.Writer }{f}, parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(exportBatchRows), parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(3)), pqarrow.DefaultWriterProps())
	if e != nil {
		return nil, nil, nil, e
	}
	p := &parquetRecords{b: array.NewRecordBuilder(memory.DefaultAllocator, stagingSchema), w: w, path: path}
	return p, func() error {
		if e := p.flush(); e != nil {
			return e
		}
		return p.w.Close()
	}, func() { p.w.Close(); p.b.Release() }, nil
}

// Encode accepts a view, a staging record, an export record, or a control record.
func (p *parquetRecords) Encode(input any) error {
	if v, ok := input.(stagingView); ok {
		return p.appendView(v)
	}
	var r stagingRecord
	switch v := input.(type) {
	case stagingRecord:
		r = v
	case exportRecord:
		tags, e := json.Marshal(v.Tags)
		if e != nil {
			return e
		}
		r = stagingRecord{v.Time, v.Name, tags, v.Kind, v.Value, v.Seq}
	case control.Record:
		tags, e := json.Marshal(v.Tags)
		if e != nil {
			return e
		}
		r = stagingRecord{v.Time, v.Name, tags, v.Kind, v.Value, 0}
	default:
		return fmt.Errorf("unsupported staging record %T", input)
	}
	return p.appendView(stagingView{r.Time, r.Name, string(r.Tags), r.Kind, r.Value, r.Seq})
}

// appendView buffers one row and flushes at the export batch size or 8 MiB.
func (p *parquetRecords) appendView(r stagingView) error {
	p.b.Field(0).(*array.Int64Builder).Append(r.Time)
	for n, v := range []string{r.Name, r.Tags, r.Kind, r.Value} {
		p.b.Field(n + 1).(*array.StringBuilder).Append(v)
	}
	p.b.Field(5).(*array.Int64Builder).Append(r.Seq)
	p.bytes += len(r.Name) + len(r.Tags) + len(r.Kind) + len(r.Value) + 16
	if p.b.Field(0).Len() >= int(exportBatchRows) || p.bytes >= 8<<20 {
		return p.flush()
	}
	return nil
}

// flush writes the buffered rows. It rechecks free space before the write.
func (p *parquetRecords) flush() error {
	if p.b.Field(0).Len() == 0 {
		return nil
	}
	if _, e := stagingSpace(p.path); e != nil {
		return e
	}
	r := p.b.NewRecordBatch()
	defer r.Release()
	if e := p.w.Write(r); e != nil {
		return e
	}
	p.bytes = 0
	return nil
}
