// normalize.go turns a raw engine export into one identity-sorted Parquet file.
// Routing ignores values, so two replays of the same point meet in one shard.

package storage

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

const normalizationPartitionRows int64 = 1_000_000
const normalizationMergeFanIn = 16
const mergeBatchRows int64 = 2048
const stagingColumns = "time,name,tags,kind,value,seq"

// exportCanonicalSQL sorts tag keys and rejects a missing sequence.
func exportCanonicalSQL(raw string) string {
	return "SELECT time,name,to_json(map_from_entries(list_sort(map_entries(tags))))::VARCHAR AS tags,kind,value," +
		"CASE WHEN seq IS NULL OR seq<1 THEN error('export sequence missing or invalid; re-export the fenced day') ELSE seq END AS seq FROM " + Raw(raw)
}

// stagingFiles reads the primitive columns from one or more local Parquet files.
func stagingFiles(paths []string) string {
	q := make([]string, len(paths))
	for n, p := range paths {
		q[n] = Q(p)
	}
	return "SELECT " + stagingColumns + " FROM read_parquet([" + strings.Join(q, ",") + "],hive_partitioning=false)"
}

func normalizationCopy(source, path string, bits int) string {
	// Values and types must NOT influence routing: changed values for the same
	// identity must meet before selecting the greatest physical replay sequence.
	return "COPY (SELECT *, (hash(time,name,tags) >> " + strconv.Itoa(bits) + ")%16 AS shard FROM (" + source + ")) TO " + Q(path) +
		" (FORMAT parquet,PARTITION_BY(shard),COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 65536);"
}

// writeStaging creates path exclusively and returns the number of encoded rows.
func writeStaging(path, format string, produce func(recordEncoder) (int64, error)) (n int64, err error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	enc, finish, release, e := newRecordEncoder(f, format)
	if e != nil {
		return 0, e
	}
	defer release()
	if n, e = produce(enc); e != nil {
		return n, e
	}
	if e = finish(); e != nil {
		return n, e
	}
	if e = f.Sync(); e != nil {
		return n, e
	}
	return n, f.Close()
}

// DeduplicateExport sorts only bounded identity partitions, collapses replay
// versions locally, then merges winners chronologically with bounded fan-in.
// Only this attempt's fresh scratch is reclaimed; raw and published output are
// untouched on failure. The caller's inherited singleton lock excludes orphans.
// DeduplicateExport partitions by identity, keeps one row per identity, and
// merges the parts with fan-in 16. Threads are fixed at 16 for this step.
func (d DuckDB) DeduplicateExport(ctx context.Context, raw, output string) (stats ExportStats, err error) {
	// Normalization always uses the accepted native and merge policy.
	d.Threads = 16
	const fanIn = normalizationMergeFanIn
	stats.Threads = d.threadCount()
	stats.MergeFanIn = fanIn
	stats.MergeConcurrency = 1
	if filepath.Clean(raw) == filepath.Clean(output) {
		return stats, errors.New("raw and normalized export paths must differ")
	}
	if _, e := stagingSpace(filepath.Dir(output)); e != nil {
		return stats, e
	}
	pending := output + ".pending"
	for _, p := range []string{pending, output + ".sort.parquet.pending"} {
		if e := os.Remove(p); e != nil && !os.IsNotExist(e) {
			return stats, e
		}
	}
	defer os.Remove(pending)
	root, e := os.MkdirTemp(filepath.Dir(output), "normalize-")
	if e != nil {
		return stats, e
	}
	defer os.RemoveAll(root)
	info, e := os.Stat(raw)
	if e != nil {
		return stats, e
	}
	stats.RawBytes = info.Size()
	stats.Strategy = "identity-partitions-merge-v1"
	count, e := d.Count(ctx, "SELECT * FROM "+Raw(raw))
	if e != nil {
		return stats, e
	}
	limit := d.normalizationRows
	if limit <= 0 {
		limit = normalizationPartitionRows
	}
	serial := 0
	var winners []string
	var split func(string, int64, int) error
	split = func(source string, rows int64, bits int) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		serial++
		if rows <= limit {
			began := time.Now()
			sorted := filepath.Join(root, fmt.Sprintf("sort-%d.parquet", serial))
			winner := filepath.Join(root, fmt.Sprintf("winner-%d.parquet", serial))
			_, e := d.Run(ctx, "SET default_collation='';COPY (SELECT "+stagingColumns+" FROM ("+source+") ORDER BY time,name,tags,seq DESC) TO "+Q(sorted)+" (FORMAT parquet,COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 65536);")
			if e != nil {
				return e
			}
			info, e := os.Stat(sorted)
			if e != nil {
				return e
			}
			stats.SortedBytes += info.Size()
			var input int64
			unique, e := writeStaging(winner, winner, func(enc recordEncoder) (int64, error) {
				var n int64
				var err error
				input, n, err = collapseExport(ctx, sorted, enc, exportBatchRows)
				return n, err
			})
			if e != nil {
				return e
			}
			if input != rows {
				return errors.New("normalization partition count changed")
			}
			stats.Partitions++
			stats.MaxPartitionRows = max(stats.MaxPartitionRows, rows)
			stats.InputRows += input
			stats.UniqueRows += unique
			stats.SortCollapseSeconds += time.Since(began).Seconds()
			winners = append(winners, winner)
			return os.Remove(sorted)
		}
		if bits >= 64 {
			return fmt.Errorf("%w: normalization identity partition exceeds %d rows after 64 hash bits", control.ErrStagingCapacity, limit)
		}
		part := filepath.Join(root, fmt.Sprintf("partition-%d", serial))
		began := time.Now()
		_, e := d.Run(ctx, d.partitionSettings()+normalizationCopy(source, part, bits))
		if e != nil {
			return e
		}
		parts, total, e := partitionInventory(part, 16)
		if e != nil {
			return e
		}
		if total != rows {
			return errors.New("normalization routing row count changed")
		}
		for _, files := range parts {
			for _, path := range files {
				info, e := os.Stat(path)
				if e != nil {
					return e
				}
				stats.PartitionBytes += info.Size()
			}
		}
		stats.PartitionSeconds += time.Since(began).Seconds()
		for shard := 0; shard < 16; shard++ {
			files := parts[shard]
			if len(files) == 0 {
				continue
			}
			n, e := partitionRows(files)
			if e != nil {
				return e
			}
			if e = split(stagingFiles(files), n, bits+4); e != nil {
				return e
			}
		}
		return os.RemoveAll(part)
	}
	if e = split(exportCanonicalSQL(raw), count, 0); e != nil {
		return stats, e
	}
	if stats.InputRows != count {
		return stats, errors.New("normalization input coverage mismatch")
	}
	began := time.Now()
	mergeSelf, mergeChild, mergeUsage := operationCPU()
	for len(winners) > fanIn {
		stats.MergePasses++
		var next []string
		for n := 0; n < len(winners); n += fanIn {
			end := min(n+fanIn, len(winners))
			paths := winners[n:end]
			serial++
			path := filepath.Join(root, fmt.Sprintf("merge-%d.parquet", serial))
			if _, e = writeStaging(path, path, func(enc recordEncoder) (int64, error) {
				return mergeStaging(ctx, paths, enc, mergeBatchRows)
			}); e != nil {
				return stats, e
			}
			info, err := os.Stat(path)
			if err != nil {
				return stats, err
			}
			stats.MergeWrittenBytes += info.Size()
			stats.MaxMergeFanIn = max(stats.MaxMergeFanIn, len(paths))
			next = append(next, path)
			for _, p := range paths {
				if e = os.Remove(p); e != nil {
					return stats, e
				}
			}
		}
		winners = next
	}
	stats.MergePasses++
	stats.MaxMergeFanIn = max(stats.MaxMergeFanIn, len(winners))
	n, e := writeStaging(pending, output, func(enc recordEncoder) (int64, error) {
		return mergeStaging(ctx, winners, enc, mergeBatchRows)
	})
	if e != nil {
		return stats, e
	}
	if n != stats.UniqueRows {
		return stats, errors.New("normalization merged row count mismatch")
	}
	stats.MergeSeconds = time.Since(began).Seconds()
	afterSelf, afterChild, afterUsage := operationCPU()
	stats.MergeUsageAvailable = mergeUsage && afterUsage
	if stats.MergeUsageAvailable {
		stats.MergeSelfCPUSeconds = afterSelf - mergeSelf
		stats.MergeChildCPUSeconds = afterChild - mergeChild
	}
	stats.SupersededRows = stats.InputRows - stats.UniqueRows
	info, e = os.Stat(pending)
	if e != nil {
		return stats, e
	}
	stats.NormalizedBytes = info.Size()
	stats.MergeWrittenBytes += info.Size()
	if e = os.Rename(pending, output); e != nil {
		return stats, e
	}
	dir, e := os.Open(filepath.Dir(output))
	if e != nil {
		return stats, e
	}
	defer dir.Close()
	return stats, dir.Sync()
}

type cursorHeap []*stagingCursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(a, b int) bool {
	return compareExportIdentity(h[a].head.identity(), h[b].head.identity()) < 0
}
func (h cursorHeap) Swap(a, b int) { h[a], h[b] = h[b], h[a] }
func (h *cursorHeap) Push(v any)   { *h = append(*h, v.(*stagingCursor)) }
func (h *cursorHeap) Pop() any     { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }

// mergeStaging k-way merges identity-sorted inputs and checks that the result stays ordered.
func mergeStaging(ctx context.Context, paths []string, enc recordEncoder, batchRows int64) (rows int64, err error) {
	if len(paths) > normalizationMergeFanIn {
		return 0, errors.New("normalization merge fan-in exceeded")
	}
	var cursors []*stagingCursor
	defer func() {
		for _, c := range cursors {
			c.close()
		}
	}()
	var h cursorHeap
	for _, path := range paths {
		c, e := openStagingCursor(ctx, path, batchRows)
		if e != nil {
			return rows, e
		}
		cursors = append(cursors, c)
		if c.next() {
			heap.Push(&h, c)
		} else if c.err != nil {
			return rows, c.err
		}
	}
	var previous exportIdentity
	seen := false
	for h.Len() > 0 {
		if e := ctx.Err(); e != nil {
			return rows, e
		}
		c := h[0]
		r := c.head
		key := r.identity()
		if seen && compareExportIdentity(key, previous) <= 0 {
			return rows, errors.New("normalization merge contains duplicate or out-of-order identities")
		}
		if e := emitStaging(enc, r); e != nil {
			return rows, e
		}
		rows++
		previous = key
		seen = true
		if c.lastInBatch() {
			previous.name = strings.Clone(previous.name)
			previous.tags = strings.Clone(previous.tags)
		}
		if c.next() {
			heap.Fix(&h, 0)
		} else {
			if c.err != nil {
				return rows, c.err
			}
			heap.Pop(&h)
		}
	}
	return rows, nil
}
