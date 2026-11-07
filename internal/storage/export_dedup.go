// export_dedup.go collapses an identity-sorted export. The first row of each
// identity is kept; a later row of that identity must have a lower sequence.

package storage

import (
	"cmp"
	"context"
	"errors"
	"strings"
)

const exportBatchRows int64 = 65536
const normalizedExportName = "export-dedup.parquet"

// ExportStats reports retained/superseded rows and observable stage sizes.
// Stage bytes are artifact sizes, not a claim about total peak staging space:
// DuckDB's external sort can also use the configured spill directory.
type ExportStats struct {
	Threads              int     `json:"threads"`
	MergeFanIn           int     `json:"merge_fan_in"`
	MergeConcurrency     int     `json:"merge_concurrency"`
	MergeWrittenBytes    int64   `json:"merge_written_bytes"`
	MergeUsageAvailable  bool    `json:"merge_usage_available"`
	MergeSelfCPUSeconds  float64 `json:"merge_self_cpu_seconds"`
	MergeChildCPUSeconds float64 `json:"merge_child_cpu_seconds"`
	InputRows            int64   `json:"input_rows"`
	UniqueRows           int64   `json:"unique_rows"`
	SupersededRows       int64   `json:"superseded_rows"`
	RawBytes             int64   `json:"raw_bytes"`
	SortedBytes          int64   `json:"sorted_bytes"`
	NormalizedBytes      int64   `json:"normalized_bytes"`
	Strategy             string  `json:"strategy"`
	Partitions           int     `json:"partitions"`
	MaxPartitionRows     int64   `json:"max_partition_rows"`
	PartitionBytes       int64   `json:"partition_bytes"`
	MergePasses          int     `json:"merge_passes"`
	MaxMergeFanIn        int     `json:"max_merge_fan_in"`
	PartitionSeconds     float64 `json:"partition_seconds"`
	SortCollapseSeconds  float64 `json:"sort_collapse_seconds"`
	MergeSeconds         float64 `json:"merge_seconds"`
}

type exportIdentity struct {
	stamp      int64
	name, tags string
}

// compareExportIdentity orders by time, then measurement, then tag JSON.
func compareExportIdentity(a, b exportIdentity) int {
	if n := cmp.Compare(a.stamp, b.stamp); n != 0 {
		return n
	}
	if n := cmp.Compare(a.name, b.name); n != 0 {
		return n
	}
	return cmp.Compare(a.tags, b.tags)
}

// collapseExport emits one row per identity. The input must already be ordered
// by identity and, within an identity, by descending sequence.
func collapseExport(ctx context.Context, path string, enc recordEncoder, batchRows int64) (rows, unique int64, err error) {
	c, e := openStagingCursor(ctx, path, batchRows)
	if e != nil {
		return 0, 0, e
	}
	defer c.close()
	var previous exportIdentity
	var previousSeq int64
	seen := false
	for c.next() {
		row := c.head
		key := row.identity()
		order := compareExportIdentity(key, previous)
		if seen && (order < 0 || (order == 0 && row.Seq >= previousSeq)) {
			return c.read, unique, errors.New("export identity/sequence ordering violated")
		}
		if !seen || order != 0 {
			if e = emitStaging(enc, row); e != nil {
				return c.read, unique, e
			}
			unique++
		}
		previous = key
		previousSeq = row.Seq
		seen = true
		if c.lastInBatch() {
			previous.name = strings.Clone(previous.name)
			previous.tags = strings.Clone(previous.tags)
		}
	}
	return c.read, unique, c.err
}
