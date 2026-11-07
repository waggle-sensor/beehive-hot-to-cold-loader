// normalize_test.go checks that a merge stays in chronological order across batch
// seams and fan-in passes, including the native partitioned normalization path.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

// TestMergeStagingInterleavesChronologicallyAcrossBatchSeams tests that a merge staging interleaves chronologically across batch seams.
func TestMergeStagingInterleavesChronologicallyAcrossBatchSeams(t *testing.T) {
	ctx := context.Background()
	var paths []string
	for shard := 0; shard < 8; shard++ {
		var rows []exportRecord
		for n := shard; n < 40; n += 8 {
			rows = append(rows, exportRecord{Record: control.Record{Time: 1740787200000000194 + int64(n), Name: "m", Tags: map[string]string{"node": "n"}, Kind: "string", Value: "a\nb"}, Seq: int64(n + 1)})
		}
		path := sortedExportFixture(t, rows)
		paths = append(paths, path)
	}
	var out bytes.Buffer
	n, e := mergeStaging(ctx, paths, json.NewEncoder(&out), 2)
	if e != nil || n != 40 {
		t.Fatal(n, e)
	}
	decoder := json.NewDecoder(&out)
	for n := 0; n < 40; n++ {
		var r exportRecord
		if e = decoder.Decode(&r); e != nil {
			t.Fatal(e)
		}
		if r.Time != 1740787200000000194+int64(n) || r.Seq != int64(n+1) || r.Value != "a\nb" {
			t.Fatal(n, r)
		}
	}
	if _, e = mergeStaging(ctx, []string{paths[0], paths[0]}, json.NewEncoder(&bytes.Buffer{}), 2); e == nil {
		t.Fatal("accepted duplicate identity across partitions")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = mergeStaging(cancelled, paths, json.NewEncoder(&bytes.Buffer{}), 2); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestNativePartitionedNormalization(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("set SAGE_ARCHIVE_DUCKDB2 for bounded normalization correctness")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root := t.TempDir()
	memory := os.Getenv("SAGE_ARCHIVE_NORMALIZE_MEMORY")
	if memory == "" {
		memory = "256MB"
	}
	// A caller's generic session setting cannot lower normalization threads.
	d := DuckDB{Binary: binary, Work: root, Memory: memory, Threads: 2, normalizationRows: 300}
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	raw := filepath.Join(root, "raw.parquet")
	output := filepath.Join(root, "normalized.parquet")
	// The later replay changes both value and kind and reorders tag keys.
	source := `SELECT 1740787200000000194+50000-i::BIGINT AS time,'m' AS name,'{"node":"n","sensor":"a,b=c"}' AS tags,
 CASE i%6 WHEN 0 THEN 'int' WHEN 1 THEN 'uint' WHEN 2 THEN 'float' WHEN 3 THEN 'bool' ELSE 'string' END AS kind,
 CASE i%6 WHEN 0 THEN '9007199254740993' WHEN 1 THEN '18446744073709551615' WHEN 2 THEN '11.7' WHEN 3 THEN 'true' ELSE 'line' || chr(10) || 'literal' || chr(92) || 'n' END AS value,
 i+1 AS seq FROM range(50000) r(i)
 UNION ALL SELECT 1740787200000000194+50000-i::BIGINT,'m','{"sensor":"a,b=c","node":"n"}','string','later' || chr(10) || i::VARCHAR,50001+i FROM range(50000) r(i) WHERE i%5=0`
	if _, e := d.Run(ctx, "COPY (SELECT * FROM ("+source+") ORDER BY seq) TO "+Q(raw)+" (FORMAT parquet,COMPRESSION zstd,ROW_GROUP_SIZE 2048);"); e != nil {
		t.Fatal(e)
	}
	stats, e := d.DeduplicateExport(ctx, raw, output)
	if e != nil {
		t.Fatal(e)
	}
	if stats.InputRows != 60000 || stats.UniqueRows != 50000 || stats.SupersededRows != 10000 || stats.MaxPartitionRows > 300 || stats.MergePasses < 2 || stats.MaxMergeFanIn > normalizationMergeFanIn || stats.Threads != 16 || stats.MergeFanIn != 16 || stats.MergeConcurrency != 1 || d.Threads != 2 {
		t.Fatal(stats)
	}
	// An independent, small global window oracle checks complete records and
	// replay sequence, not only counts or agreement between two implementations.
	oracle := "SELECT " + stagingColumns + " FROM (SELECT *,row_number() OVER(PARTITION BY time,name,tags ORDER BY seq DESC) AS rn FROM (" + exportCanonicalSQL(raw) + ")) WHERE rn=1 ORDER BY time,name,tags"
	want := filepath.Join(root, "oracle.parquet")
	if _, e = d.Run(ctx, "COPY ("+oracle+") TO "+Q(want)+" (FORMAT parquet);"); e != nil {
		t.Fatal(e)
	}
	var gotRows, wantRows []exportRecord
	if e = visitStagingFixture(output, func(r exportRecord) error { gotRows = append(gotRows, r); return nil }); e != nil {
		t.Fatal(e)
	}
	if e = visitStagingFixture(want, func(r exportRecord) error { wantRows = append(wantRows, r); return nil }); e != nil {
		t.Fatal(e)
	}
	a, _ := json.Marshal(gotRows)
	b, _ := json.Marshal(wantRows)
	if !bytes.Equal(a, b) {
		t.Fatal("normalization differs from independent latest-replay oracle")
	}
	// Include empty input and atomic refusal of invalid sequences.
	empty := filepath.Join(root, "empty.parquet")
	if _, e = d.Run(ctx, "COPY (SELECT * FROM ("+source+") WHERE false) TO "+Q(empty)+" (FORMAT parquet);"); e != nil {
		t.Fatal(e)
	}
	if s, e := d.DeduplicateExport(ctx, empty, filepath.Join(root, "empty-out.parquet")); e != nil || s.UniqueRows != 0 {
		t.Fatal(s, e)
	}
	before, e := os.ReadFile(output)
	if e != nil {
		t.Fatal(e)
	}
	bad := filepath.Join(root, "bad.parquet")
	if _, e = d.Run(ctx, "COPY (SELECT * REPLACE(0::BIGINT AS seq) FROM ("+source+") LIMIT 1) TO "+Q(bad)+" (FORMAT parquet);"); e != nil {
		t.Fatal(e)
	}
	if _, e = d.DeduplicateExport(ctx, bad, output); e == nil {
		t.Fatal("accepted invalid replay sequence")
	}
	after, _ := os.ReadFile(output)
	if !bytes.Equal(before, after) {
		t.Fatal("failure replaced published normalized output")
	}
	left, _ := filepath.Glob(filepath.Join(root, "normalize-*"))
	sort.Strings(left)
	if len(left) != 0 {
		t.Fatal("normalization scratch survived", left)
	}
}

// TestMergeFanInPreservesGlobalOrder tests that a merge fan in preserves global order.
func TestMergeFanInPreservesGlobalOrder(t *testing.T) {
	fanIn := normalizationMergeFanIn
	var paths []string
	for shard := 0; shard < fanIn; shard++ {
		var rows []exportRecord
		for n := shard; n < fanIn*3; n += fanIn {
			rows = append(rows, exportRecord{Record: control.Record{Time: 1740787200000000194 + int64(n), Name: "m", Tags: map[string]string{"node": "n"}, Kind: "string", Value: "line\nliteral\\n"}, Seq: int64(n + 1)})
		}
		paths = append(paths, sortedExportFixture(t, rows))
	}
	var out bytes.Buffer
	count, err := mergeStaging(context.Background(), paths, json.NewEncoder(&out), 2)
	if err != nil || count != int64(fanIn*3) {
		t.Fatal(fanIn, count, err)
	}
	decoder := json.NewDecoder(&out)
	for n := 0; n < fanIn*3; n++ {
		var r exportRecord
		if err := decoder.Decode(&r); err != nil {
			t.Fatal(err)
		}
		if r.Time != 1740787200000000194+int64(n) || r.Seq != int64(n+1) || r.Value != "line\nliteral\\n" {
			t.Fatal(fanIn, n, r)
		}
	}
	if _, err = mergeStaging(context.Background(), append(paths, paths[0]), json.NewEncoder(&out), 2); err == nil {
		t.Fatal("accepted excess fan-in", fanIn)
	}
	if _, err = mergeStaging(context.Background(), []string{paths[0], paths[0]}, json.NewEncoder(&out), 2); err == nil {
		t.Fatal("accepted duplicate identities", fanIn)
	}
}
