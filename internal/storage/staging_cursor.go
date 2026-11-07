// staging_cursor.go walks a primitive staging file one row at a time. Values
// point at the current Arrow batch until the next batch replaces it.

package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type stagingCursor struct {
	f       *file.Reader
	rr      pqarrow.RecordReader
	ctx     context.Context
	batch   arrow.RecordBatch
	indices []int
	pos     int
	read    int64
	err     error
	head    stagingView
}

// openStagingCursor requires the six primitive columns and a positive batch size.
func openStagingCursor(ctx context.Context, path string, batchRows int64) (*stagingCursor, error) {
	if batchRows < 1 {
		return nil, errors.New("positive staging reader batch size required")
	}
	f, e := file.OpenParquetFile(path, false)
	if e != nil {
		return nil, e
	}
	ar, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{BatchSize: batchRows}, memory.DefaultAllocator)
	if e != nil {
		f.Close()
		return nil, e
	}
	rr, e := ar.GetRecordReader(ctx, nil, nil)
	if e != nil {
		f.Close()
		return nil, e
	}
	c := &stagingCursor{f: f, rr: rr, ctx: ctx, pos: -1}
	for _, name := range []string{"time", "name", "tags", "kind", "value", "seq"} {
		i := rr.Schema().FieldIndices(name)
		if len(i) != 1 {
			c.close()
			return nil, fmt.Errorf("staging column %s missing", name)
		}
		c.indices = append(c.indices, i[0])
	}
	return c, nil
}

// close releases the Arrow reader and the Parquet file.
func (c *stagingCursor) close() { c.rr.Release(); c.f.Close() }

// lastInBatch reports the last row of the current batch, where string bytes
// must be cloned before the next batch reuses them.
func (c *stagingCursor) lastInBatch() bool {
	return c.batch != nil && c.pos+1 == int(c.batch.NumRows())
}

// next advances one row. It rejects null fields, a nonpositive sequence, and a
// footer count that does not match the rows actually read.
func (c *stagingCursor) next() bool {
	if c.err != nil {
		return false
	}
	if e := c.ctx.Err(); e != nil {
		c.err = e
		return false
	}
	c.pos++
	for c.batch == nil || c.pos >= int(c.batch.NumRows()) {
		if !c.rr.Next() {
			c.err = c.rr.Err()
			if c.err == nil && c.read != c.f.NumRows() {
				c.err = errors.New("staging row count differs from footer")
			}
			return false
		}
		c.batch = c.rr.RecordBatch()
		c.pos = 0
	}
	cols := make([]arrow.Array, 6)
	for n, i := range c.indices {
		cols[n] = c.batch.Column(i)
		if cols[n].IsNull(c.pos) {
			c.err = errors.New("staging contains a null field")
			return false
		}
	}
	times, ok := cols[0].(*array.Int64)
	if !ok {
		c.err = errors.New("staging time must be int64")
		return false
	}
	seqs, ok := cols[5].(*array.Int64)
	if !ok {
		c.err = errors.New("staging sequence must be int64")
		return false
	}
	text := make([]string, 4)
	for n := range text {
		a, ok := cols[n+1].(*array.String)
		if !ok {
			c.err = errors.New("staging text column must be VARCHAR")
			return false
		}
		text[n] = a.Value(c.pos)
	}
	c.head = stagingView{times.Value(c.pos), text[0], text[1], text[2], text[3], seqs.Value(c.pos)}
	if c.head.Seq < 1 {
		c.err = errors.New("staging sequence must be positive")
		return false
	}
	c.read++
	return true
}

// identity is the sort and collapse key: timestamp, measurement, and tag JSON.
func (v stagingView) identity() exportIdentity { return exportIdentity{v.Time, v.Name, v.Tags} }
