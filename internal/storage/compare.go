// compare.go proves two inputs are the same multiset. Rows are hash-routed
// into fixed shards, then each shard is compared with EXCEPT ALL.

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	"github.com/apache/arrow-go/v18/parquet/file"
)

const comparisonRows int64 = 1_000_000               // comparisonRows is the default row limit for exact partition comparisons
const canonicalColumns = "time,name,tags,kind,value" //canonicalColumns is the set of columns to compare

const comparisonBits = 4                     // comparisonBits is the number of bits to use for the hash shard
const comparisonFanout = 1 << comparisonBits // comparisonFanout is the number of shards to use for the hash shard

// canonicalCopy writes hash shards of the canonical columns. The hash only
// chooses a shard; it is not the equality proof.
func canonicalCopy(source, path string, bits int) string {
	return "COPY (SELECT *, (hash(time,name,tags,kind,value) >> " + strconv.Itoa(bits) + ")%" + strconv.Itoa(comparisonFanout) + " AS shard FROM (" + source + ")) TO " + Q(path) +
		" (FORMAT parquet,PARTITION_BY(shard),COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 65536);"
}

// Bound concurrent partition writers independently of input size.
func (d DuckDB) comparisonWriterSettings() (string, int) {
	count := min(16*d.threadCount(), 256)
	return fmt.Sprintf("SET partitioned_write_max_open_files=%d;SET partitioned_write_flush_threshold=65536;SET preserve_insertion_order=false;", count), count
}

// canonicalEmpty is a typed empty relation used when a shard has no files.
func canonicalEmpty() string {
	return "SELECT NULL::BIGINT AS time,NULL::VARCHAR AS name,NULL::VARCHAR AS tags,NULL::VARCHAR AS kind,NULL::VARCHAR AS value WHERE false"
}

// canonicalFiles reads the primitive columns already staged for one shard.
func canonicalFiles(paths []string) string {
	if len(paths) == 0 {
		return canonicalEmpty()
	}
	q := make([]string, len(paths))
	for n, p := range paths {
		q[n] = Q(p)
	}
	return "SELECT " + canonicalColumns + " FROM read_parquet([" + strings.Join(q, ",") + "],hive_partitioning=false)"
}

// partitionInventory lists shard files and sums their footer row counts.
func partitionInventory(root string, buckets int) (map[int][]string, int64, error) {
	parts := map[int][]string{}
	var total int64
	entries, e := os.ReadDir(root)
	if os.IsNotExist(e) {
		return parts, 0, nil
	}
	if e != nil {
		return nil, 0, e
	}
	for _, entry := range entries {
		id, e := strconv.Atoi(strings.TrimPrefix(entry.Name(), "shard="))
		if e != nil || !strings.HasPrefix(entry.Name(), "shard=") || id < 0 || id >= buckets || !entry.IsDir() {
			return nil, 0, fmt.Errorf("unexpected comparison partition")
		}
		files, e := filepath.Glob(filepath.Join(root, entry.Name(), "*.parquet"))
		if e != nil {
			return nil, 0, e
		}
		var rows int64
		for _, path := range files {
			r, e := file.OpenParquetFile(path, false)
			if e != nil {
				return nil, 0, e
			}
			rows += r.NumRows()
			e = r.Close()
			if e != nil {
				return nil, 0, e
			}
		}

		parts[id] = files
		total += rows
	}
	return parts, total, nil
}

// exactPartitionSQL fails unless the two relations have the same rows and duplicates.
func exactPartitionSQL(left, right string) string {
	return "WITH a AS (" + left + "),b AS (" + right + ") SELECT CASE WHEN count(*)=0 THEN true ELSE error('full record reconciliation failed') END AS equal FROM ((SELECT * FROM a EXCEPT ALL SELECT * FROM b) UNION ALL (SELECT * FROM b EXCEPT ALL SELECT * FROM a));"
}

// Equal stages canonical primitive columns once per side, then compares bounded
// local shards. Hashes route rows only; both exact multisets are still compared.
func (d DuckDB) Equal(ctx context.Context, left, right, setup string) error {
	err := d.EqualCanonical(ctx, Canonical(left), Canonical(right), setup)
	if d.comparisonProfile != nil {
		d.comparisonProfile.Canonicalization = "baseline"
	}
	return err
}

// ComparisonProfile is the timing record for one fresh exact proof.
type ComparisonProfile struct {
	Canonicalization      string  `json:"canonicalization,omitempty"`
	Threads               int     `json:"threads"`
	BindingSeconds        float64 `json:"binding_seconds"`
	ReusedInitial         bool    `json:"reused_initial"`
	Mode                  string  `json:"mode"`
	InitialFanout         int     `json:"initial_fanout"`
	WriterOpenFiles       int     `json:"writer_open_files"`
	InitialRoutingSeconds float64 `json:"initial_routing_seconds"`
	RefinementSeconds     float64 `json:"refinement_seconds"`
	RefinementPasses      int     `json:"refinement_passes"`
	CountSeconds          float64 `json:"count_seconds"`
	RoutingSeconds        float64 `json:"routing_seconds"`
	ExactSeconds          float64 `json:"exact_seconds"`
	TotalSeconds          float64 `json:"total_seconds"`
	LeafPartitions        int     `json:"leaf_partitions"`
	Passed                bool    `json:"passed"`
}

// EqualCanonical compares two caller-supplied canonical projections. It does
// not choose a dictionary; Equal, EqualRemote, and EqualReconciliation do.
func (d DuckDB) EqualCanonical(ctx context.Context, left, right, setup string) error {
	return d.equalCanonical(ctx, left, right, setup, nil)
}

// equalCanonical counts both sides, copies them into fixed shards, and proves
// each shard. When retained is set, the initial shards stay available for the
// build's whole-file proof and are checked again before that proof runs.
func (d DuckDB) equalCanonical(ctx context.Context, left, right, setup string, retained **canonicalStage) (err error) {
	began := time.Now()
	profile := ComparisonProfile{Mode: "fixed", Threads: d.threadCount()}
	limit := d.comparisonLimit
	if limit == 0 {
		limit = comparisonRows
	}
	defer func() {
		profile.TotalSeconds = time.Since(began).Seconds()
		profile.RoutingSeconds = profile.InitialRoutingSeconds + profile.RefinementSeconds
		profile.Passed = err == nil
		if d.comparisonProfile != nil {
			*d.comparisonProfile = profile
		}
	}()
	if limit < 1 {
		return fmt.Errorf("positive comparison row limit required")
	}
	writerSettings, writerFiles := d.comparisonWriterSettings()
	profile.WriterOpenFiles = writerFiles
	run := func(sql string, seconds *float64) ([]byte, error) {
		began := time.Now()
		b, e := d.Run(ctx, sql)
		*seconds += time.Since(began).Seconds()
		return b, e
	}
	b, e := run(setup+";SELECT (SELECT count(*) FROM ("+left+")) AS a,(SELECT count(*) FROM ("+right+")) AS b;", &profile.CountSeconds)
	if e != nil {
		return e
	}
	var counts []struct {
		A *int64 `json:"a"`
		B *int64 `json:"b"`
	}
	if e = json.Unmarshal(b, &counts); e != nil {
		return e
	}
	if len(counts) != 1 || counts[0].A == nil || counts[0].B == nil || *counts[0].A < 0 || *counts[0].B < 0 {
		return fmt.Errorf("missing comparison counts")
	}
	countA, countB := *counts[0].A, *counts[0].B
	if countA != countB {
		return fmt.Errorf("full record reconciliation failed: row counts %d/%d", countA, countB)
	}

	root, e := os.MkdirTemp(d.Work, "compare-")
	if e != nil {
		return e
	}
	keep := false
	defer func() {
		if !keep {
			os.RemoveAll(root)
		}
	}()
	// Always stage the original sources once, including small remote inputs. All
	// comparisons/refinement below operate on local compressed primitives only.
	profile.InitialFanout = comparisonFanout
	leftDir, rightDir := filepath.Join(root, "a"), filepath.Join(root, "b")
	_, e = run(setup+";"+writerSettings+canonicalCopy(left, leftDir, 0)+canonicalCopy(right, rightDir, 0), &profile.InitialRoutingSeconds)
	if e != nil {
		return e
	}
	leftParts, leftRows, e := partitionInventory(leftDir, comparisonFanout)
	if e != nil {
		return e
	}
	rightParts, rightRows, e := partitionInventory(rightDir, comparisonFanout)
	if e != nil {
		return e
	}
	if leftRows != countA || rightRows != countB {
		return fmt.Errorf("comparison staging row counts changed")
	}
	stage := &canonicalStage{root: root, left: leftParts, right: rightParts, rows: countA}
	if retained != nil {
		started := time.Now()
		stage.bindings, e = bindCanonical(leftParts, rightParts)
		profile.BindingSeconds += time.Since(started).Seconds()
		if e != nil {
			return e
		}
	}
	if e = d.compareCanonicalPartitions(ctx, leftParts, rightParts, countA, root, &profile); e != nil {
		return e
	}
	if retained != nil {
		started := time.Now()
		e = validateBindings(stage.bindings)
		profile.BindingSeconds += time.Since(started).Seconds()
		if e != nil {
			return e
		}
		*retained = stage
		keep = true
	}
	return nil
}

// compareCanonicalPartitions splits any shard above the row limit and proves
// every leaf in both directions.
func (d DuckDB) compareCanonicalPartitions(ctx context.Context, leftParts, rightParts map[int][]string, rows int64, root string, profile *ComparisonProfile) error {
	scratch, e := os.MkdirTemp(root, "refine-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(scratch)
	root = scratch

	limit := d.comparisonLimit
	if limit == 0 {
		limit = comparisonRows
	}
	writerSettings, _ := d.comparisonWriterSettings()
	run := func(sql string, seconds *float64) ([]byte, error) {
		began := time.Now()
		b, e := d.Run(ctx, sql)
		*seconds += time.Since(began).Seconds()
		return b, e
	}
	var sql strings.Builder
	serial := 0
	var partition func(string, string, string, int, int64, int64) error
	partition = func(aSource, bSource, session string, bits int, aCount, bCount int64) error {
		if aCount != bCount {
			return fmt.Errorf("full record reconciliation failed: partition row counts differ")
		}
		if aCount == 0 {
			return nil
		}
		if aCount <= limit {
			profile.LeafPartitions++
			sql.WriteString(exactPartitionSQL(aSource, bSource))
			return nil
		}
		if bits >= 64 {
			return fmt.Errorf("%w: identical-hash comparison partition exceeds %d rows; equality not accepted", control.ErrStagingCapacity, limit)
		}
		serial++
		profile.RefinementPasses++
		part := filepath.Join(root, strconv.Itoa(serial))
		if e := os.Mkdir(part, 0700); e != nil {
			return e
		}
		leftDir, rightDir := filepath.Join(part, "a"), filepath.Join(part, "b")
		_, e := run(session+";"+writerSettings+canonicalCopy(aSource, leftDir, bits)+canonicalCopy(bSource, rightDir, bits), &profile.RefinementSeconds)
		if e != nil {
			return e
		}
		leftParts, leftRows, e := partitionInventory(leftDir, comparisonFanout)
		if e != nil {
			return e
		}
		rightParts, rightRows, e := partitionInventory(rightDir, comparisonFanout)
		if e != nil {
			return e
		}
		if leftRows != aCount || rightRows != bCount {
			return fmt.Errorf("comparison staging row counts changed")
		}
		for shard := 0; shard < comparisonFanout; shard++ {
			leftRows, e := partitionRows(leftParts[shard])
			if e != nil {
				return e
			}
			rightRows, e := partitionRows(rightParts[shard])
			if e != nil {
				return e
			}
			if e = partition(canonicalFiles(leftParts[shard]), canonicalFiles(rightParts[shard]), "", bits+comparisonBits, leftRows, rightRows); e != nil {
				return e
			}
		}
		return nil
	}
	var totalA, totalB int64
	for shard := 0; shard < comparisonFanout; shard++ {
		leftRows, e := partitionRows(leftParts[shard])
		if e != nil {
			return e
		}
		rightRows, e := partitionRows(rightParts[shard])
		if e != nil {
			return e
		}
		totalA += leftRows
		totalB += rightRows
		if e = partition(canonicalFiles(leftParts[shard]), canonicalFiles(rightParts[shard]), "", comparisonBits, leftRows, rightRows); e != nil {
			return e
		}
	}
	if totalA != rows || totalB != rows {
		return fmt.Errorf("comparison staging coverage changed")
	}
	sql.WriteString("SELECT true AS all_partitions_equal;")
	result, e := run(sql.String(), &profile.ExactSeconds)
	if e != nil {
		return e
	}
	var done []struct {
		Equal bool `json:"all_partitions_equal"`
	}
	if e = json.Unmarshal(result, &done); e != nil {
		return e
	}
	if len(done) != 1 || !done[0].Equal {
		return fmt.Errorf("missing full comparison completion")
	}
	return nil
}

// partitionRows sums Parquet footer counts. It does not scan row values.
func partitionRows(paths []string) (int64, error) {
	var rows int64
	for _, p := range paths {
		r, e := file.OpenParquetFile(p, false)
		if e != nil {
			return 0, e
		}
		rows += r.NumRows()
		if e = r.Close(); e != nil {
			return 0, e
		}
	}
	return rows, nil
}
