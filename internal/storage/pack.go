// pack.go rewrites one sorted window into a Parquet file whose row groups each
// hold a single plugin. Input must already be ordered by plugin, vsn, then time.

package storage

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const fileRows int64 = 10_000_000 // fileRows is the maximum number of rows in a parquet file

// localPath accepts a local file and rejects anything under /node-data.
func localPath(location string) (string, error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "" && u.Scheme != "file") || (u.Host != "" && u.Host != "localhost") {
		return "", fmt.Errorf("expected isolated local file")
	}
	p, err := filepath.Abs(u.Path)
	if err != nil {
		return "", err
	}
	for _, part := range strings.Split(p, string(filepath.Separator)) {
		if part == "node-data" {
			return "", fmt.Errorf("/node-data is not a layout staging location")
		}
	}
	return p, nil
}

// key is the packed order: plugin, vsn, then timestamp. Nulls sort last.
type key struct {
	plugin, vsn         string // the plugin and vsn of the key
	pluginNull, vsnNull bool // whether the plugin and vsn are null
	stamp               int64 // the timestamp of the key
}

// cmpString compares two strings and returns an integer.
func cmpString(a string, an bool, b string, bn bool) int {
	if an && bn {
		return 0
	}
	if an != bn {
		if an {
			return 1
		}
		return -1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// samePlugin checks if two keys have the same plugin.
func samePlugin(a, b key) bool { return cmpString(a.plugin, a.pluginNull, b.plugin, b.pluginNull) == 0 }

// less checks if one key is less than another.
func less(a, b key) bool {
	if c := cmpString(a.plugin, a.pluginNull, b.plugin, b.pluginNull); c != 0 {
		return c < 0
	}
	if c := cmpString(a.vsn, a.vsnNull, b.vsn, b.vsnNull); c != 0 {
		return c < 0
	}
	return a.stamp < b.stamp
}

// pack rewrites a sorted window into row groups that each contain one plugin.
func pack(ctx context.Context, input, output string, rowGroupRows int64) (err error) {
	if rowGroupRows < 100_000 || rowGroupRows > 1_000_000 {
		return fmt.Errorf("row-group-rows must be between 100000 and 1000000")
	}
	input, err = localPath(input)
	if err != nil {
		return err
	}
	output, err = localPath(output)
	if err != nil {
		return err
	}
	reader, err := file.OpenParquetFile(input, false)
	if err != nil {
		return err
	}
	defer reader.Close()
	if reader.NumRows() < 1 || reader.NumRows() > fileRows {
		return fmt.Errorf("input must contain 1..%d rows", fileRows)
	}
	ar, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 65536}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	rr, err := ar.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}
	defer rr.Release()
	schema := rr.Schema()
	values := schema.FieldIndices("value")
	if len(values) != 1 {
		return fmt.Errorf("missing value VARIANT")
	}
	value := schema.Field(values[0])
	id, ok := value.Metadata.GetValue("PARQUET:field_id")
	ext, isExt := value.Type.(arrow.ExtensionType)
	if !ok || id != "11" || !isExt || ext.ExtensionName() != "arrow.parquet.variant" {
		return fmt.Errorf("expected value VARIANT with field ID 11")
	}
	p, v, t := schema.FieldIndices("plugin"), schema.FieldIndices("vsn"), schema.FieldIndices("time")
	if len(p) != 1 || len(v) != 1 || len(t) != 1 {
		return fmt.Errorf("missing layout keys")
	}
	if err = os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer func() {
		out.Close()
		if err != nil {
			os.Remove(output)
		}
	}()
	sorting := make([]parquet.SortingColumn, 0, 3)
	for _, name := range []string{"plugin", "vsn", "time"} {
		index := reader.MetaData().Schema.ColumnIndexByName(name)
		if index < 0 {
			return fmt.Errorf("missing Parquet sort column %s", name)
		}
		sorting = append(sorting, parquet.SortingColumn{ColumnIdx: int32(index), Descending: false, NullsFirst: false})
	}
	fw, err := pqarrow.NewFileWriter(schema, out, parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(rowGroupRows), parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(3), parquet.WithSortingColumns(sorting)), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer func() {
		if fw != nil {
			fw.Close()
		}
	}()
	var previous key
	seen := false
	for rr.Next() {
		rec := rr.RecordBatch()
		plugins, pok := rec.Column(p[0]).(*array.String)
		vsns, vok := rec.Column(v[0]).(*array.String)
		stamps, tok := rec.Column(t[0]).(*array.Timestamp)
		if !pok || !vok || !tok {
			return fmt.Errorf("unexpected layout key types")
		}
		ts := stamps.DataType().(*arrow.TimestampType)
		if ts.Unit != arrow.Nanosecond || ts.TimeZone != "UTC" {
			return fmt.Errorf("time must retain UTC nanoseconds")
		}
		start := int64(0)
		for i := 0; i < int(rec.NumRows()); i++ {
			if stamps.IsNull(i) {
				return fmt.Errorf("null time")
			}
			current := key{plugin: plugins.Value(i), pluginNull: plugins.IsNull(i), vsn: vsns.Value(i), vsnNull: vsns.IsNull(i), stamp: int64(stamps.Value(i))}
			if seen && less(current, previous) {
				return fmt.Errorf("input is not sorted by plugin/vsn/time")
			}
			if !seen || !samePlugin(current, previous) {
				if int64(i) > start {
					slice := rec.NewSlice(start, int64(i))
					err = fw.WriteBuffered(slice)
					slice.Release()
					if err != nil {
						return err
					}
				}
				if err = fw.NewBufferedRowGroupChecked(); err != nil {
					return err
				}
				start = int64(i)
			}
			previous = current
			seen = true
		}
		if start < rec.NumRows() {
			slice := rec.NewSlice(start, rec.NumRows())
			err = fw.WriteBuffered(slice)
			slice.Release()
			if err != nil {
				return err
			}
		}
	}
	if err = rr.Err(); err != nil {
		return err
	}
	err = fw.Close()
	fw = nil
	return err
}
