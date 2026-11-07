// audit.go checks a packed data file: row cap, row-group cap, and one plugin
// value inside each row group.

package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// digest is the lowercase SHA-256 of a local file.
func digest(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	hash := sha256.New()
	if _, e = io.Copy(hash, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// auditPacked returns the row-group count after the layout checks pass.
func auditPacked(ctx context.Context, path string) (int, error) {
	reader, e := file.OpenParquetFile(path, false)
	if e != nil {
		return 0, e
	}
	defer reader.Close()
	if reader.NumRows() < 1 || reader.NumRows() > fileRows {
		return 0, errors.New("packed file row limit violated")
	}
	ar, e := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 65536}, memory.DefaultAllocator)
	if e != nil {
		return 0, e
	}
	column := reader.MetaData().Schema.ColumnIndexByName("plugin")
	if column < 0 {
		return 0, errors.New("packed plugin column missing")
	}
	for group := 0; group < reader.NumRowGroups(); group++ {
		if reader.MetaData().RowGroup(group).NumRows() > 262144 {
			return 0, errors.New("packed row-group cap violated")
		}
		rr, e := ar.GetRecordReader(ctx, []int{column}, []int{group})
		if e != nil {
			return 0, e
		}
		seen := false
		var previous string
		var previousNull bool
		for rr.Next() {
			values, ok := rr.RecordBatch().Column(0).(*array.String)
			if !ok {
				rr.Release()
				return 0, errors.New("packed plugin type invalid")
			}
			for i := 0; i < values.Len(); i++ {
				value, null := values.Value(i), values.IsNull(i)
				if seen && (null != previousNull || (!null && value != previous)) {
					rr.Release()
					return 0, errors.New("packed group contains multiple plugins")
				}
				seen = true
				previous = value
				previousNull = null
			}
		}
		e = rr.Err()
		rr.Release()
		if e != nil {
			return 0, e
		}
	}
	return reader.NumRowGroups(), nil
}
