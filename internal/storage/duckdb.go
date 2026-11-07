// duckdb.go runs SQL in a pinned DuckDB 2.0 process. Spill and the memory
// limit stay outside this service so a native query cannot grow the Go heap.

package storage

import (
	"beehive-hot-to-cold-loader/internal/control"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// DuckDB uses the native CLI so the coordinator does not embed a second CGO
// runtime. The executable is pinned and checked at startup, never downloaded.
type DuckDB struct {
	Binary, Work, Memory string
	Threads              int // zero defaults to sixteen base threads; fresh worker proofs use sixteen
	// SortThreads overrides only chronological batch sort/copy. Zero follows Threads.
	// Worker.Build sets its measured sort override on a private session copy.
	SortThreads       int   // zero defaults to the session thread count; fresh worker proofs use sixteen
	comparisonLimit   int64 // private native-test override; production uses comparisonRows
	comparisonProfile *ComparisonProfile
	testSetup         string // private native-test override; production uses testSetup
	normalizationRows int64  // private native-test override; production uses normalizationPartitionRows
	// testHTTP is appended to read secrets. The native lifecycle fixture uses it
	// to accept its own local certificate; production sessions leave it empty.
	testHTTP string   // private native-test override; production uses testHTTP
	Lock     *os.File // singleton lock; the coordinator uses it to serialize a live worker
}

// Q quotes s as a single-quoted SQL literal.
func Q(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// threadCount is the session size. Zero means the production default of 16.
func (d DuckDB) threadCount() int {
	if d.Threads == 0 {
		return 16
	}
	return d.Threads
}

// sortThreadCount applies only to chronological batch sort/copy.
func (d DuckDB) sortThreadCount() int {
	if d.SortThreads == 0 {
		return d.threadCount()
	}
	return d.SortThreads
}

// partitionSettings caps how many shard files one COPY may hold open.
func (d DuckDB) partitionSettings() string {
	return fmt.Sprintf("SET partitioned_write_max_open_files=%d;SET partitioned_write_flush_threshold=65536;SET preserve_insertion_order=false;", 16*d.threadCount())
}

// Run executes one script and returns its last JSON result. The script inherits
// UTC, the session thread count, the memory limit, and the 16 GiB spill cap.
func (d DuckDB) Run(ctx context.Context, sql string) ([]byte, error) {
	if d.threadCount() < 1 || d.threadCount() > 256 {
		return nil, fmt.Errorf("DuckDB threads must be 1..256")
	}
	free, e := stagingSpace(d.Work)
	if e != nil {
		return nil, e
	}
	spill, e := spillBudget(free)
	if e != nil {
		return nil, e
	}
	cmd := exec.CommandContext(ctx, d.Binary, "-batch", "-bail", "-json")
	// Keep the singleton lock alive if the coordinator dies mid-command.
	// A restarted daemon must not race an orphaned native staging writer.
	if d.Lock != nil {
		cmd.ExtraFiles = []*os.File{d.Lock}
	}
	memory := d.Memory
	if memory == "" {
		memory = "3GB"
	}
	cmd.Stdin = strings.NewReader(fmt.Sprintf("SET TimeZone='UTC'; SET threads=%d; SET memory_limit=", d.threadCount()) + Q(memory) + "; SET temp_directory=" + Q(filepath.Join(d.Work, "spill")) + "; SET max_temp_directory_size=" + Q(fmt.Sprintf("%dB", spill)) + ";\n" + d.testSetup + sql)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if e := cmd.Run(); e != nil {
		safe := fmt.Errorf("DuckDB command failed: %w (%s)", e, redact(stderr.String()))
		// Classify before redaction, but never return credential-bearing stderr.
		if errors.Is(capacityError(errors.New(stderr.String())), control.ErrStagingCapacity) {
			return nil, fmt.Errorf("%w: %v", control.ErrStagingCapacity, safe)
		}
		return nil, safe
	}
	// DuckDB 2.0 emits one JSON document per result. Setup statements such as
	// CREATE SECRET precede the query, so callers receive the last document.
	return lastJSON(out.Bytes())
}

// lastJSON keeps the final JSON document. Earlier documents are setup results.
func lastJSON(b []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	var last []byte
	for {
		var raw json.RawMessage
		if e := dec.Decode(&raw); e != nil {
			if e == io.EOF {
				break
			}
			return nil, fmt.Errorf("DuckDB JSON output: %w", e)
		}
		last = append([]byte(nil), raw...)
	}
	if last == nil {
		return b, nil
	}
	return last, nil
}

// redact drops stderr that may contain a signed token, then caps the remainder.
func redact(s string) string {
	// SQL errors may echo signed credentials; never include SQL-bearing stderr.
	if strings.Contains(s, "SECRET") || strings.Contains(s, "TOKEN") || strings.Contains(s, "Bearer") || strings.Contains(s, "authz=") {
		return "credential-bearing SQL error redacted"
	}
	if len(s) > 2048 {
		s = s[:2048]
	}
	return s
}

// CheckVersion accepts only a DuckDB 2.0 release.
func (d DuckDB) CheckVersion(ctx context.Context) error {
	b, e := d.Run(ctx, "SELECT version() AS version;")
	if e != nil {
		return e
	}
	var rows []struct {
		Version string `json:"version"`
	}
	if e = json.Unmarshal(b, &rows); e != nil {
		return e
	}
	if len(rows) != 1 || !strings.HasPrefix(rows[0].Version, "v2.0.") {
		return fmt.Errorf("DuckDB 2.0.x required; found %s", strings.TrimSpace(string(b)))
	}
	return nil
}

// Check verifies the CLI version and installs the httpfs and iceberg extensions.
func (d DuckDB) Check(ctx context.Context) error {
	if e := d.CheckVersion(ctx); e != nil {
		return e
	}
	_, e := d.Run(ctx, "INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
	return e
}

// Tags are the promoted identity columns. Every other tag stays in meta.
var Tags = []string{"plugin", "vsn", "node", "host", "task", "job", "zone"}

// FieldIDs is the published Iceberg field map. Value is VARIANT field 11.
const FieldIDs = "{'time':1,'name':2,'plugin':3,'vsn':4,'node':5,'host':6,'task':7,'job':8,'zone':9,'meta':{__duckdb_field_id:10,key:12,value:13},'value':11}"

// Raw reads a primitive staging file. Parquet tags are JSON text; NDJSON tags are maps.
func Raw(path string) string {
	if strings.HasSuffix(path, ".parquet") {
		return "(SELECT time,name,tags::JSON::MAP(VARCHAR,VARCHAR) AS tags,kind,value,seq FROM read_parquet(" + Q(path) + ",hive_partitioning=false))"
	}
	return "read_ndjson(" + Q(path) + ", columns={time:'BIGINT',name:'VARCHAR',tags:'MAP(VARCHAR,VARCHAR)',kind:'VARCHAR',value:'VARCHAR',seq:'BIGINT'}, ignore_errors=false)"
}

// Typed projects promoted tags, leftover metadata, and a typed VARIANT value.
func Typed(path string) string {
	var cols []string
	for _, tag := range Tags {
		cols = append(cols, "tags["+Q(tag)+"] AS "+tag)
	}
	return "SELECT make_timestamp_ns(time)::TIMESTAMPTZ_NS AS time,name," + strings.Join(cols, ",") + `,map_from_entries(list_filter(map_entries(tags),lambda x: x.key NOT IN ('plugin','vsn','node','host','task','job','zone'))) AS meta,
 ` + PrimitiveValue() + ` AS value,coalesce(seq,0) AS seq FROM ` + Raw(path)
}

// fullTags rebuilds one map from meta plus the non-null promoted columns.
func fullTags() string {
	keys := []string{}
	for _, tag := range Tags {
		keys = append(keys, Q(tag))
	}
	return "map_concat(meta,map_from_entries(list_filter(list_zip([" + strings.Join(keys, ",") + "],[" + strings.Join(Tags, ",") + "]),lambda x: x[2] IS NOT NULL)))"
}

// IdentityTags is the sorted JSON form of fullTags. Key order does not change identity.
func IdentityTags() string {
	return "to_json(map_from_entries(list_sort(map_entries(" + fullTags() + "))))"
}

// Canonical is the independent row identity: epoch, name, sorted tags, kind, and JSON value.
func Canonical(source string) string {
	return "SELECT epoch_ns(time) AS time,name," + IdentityTags() + "::VARCHAR AS tags," + logicalValueKind("value") + " AS kind,to_json(value)::VARCHAR AS value FROM (" + source + ")"
}

// CopySQL writes zstd Parquet with the published field IDs.
func CopySQL(source, path string) string {
	return "COPY (" + source + ") TO " + Q(path) + " (FORMAT parquet,COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 122880,FIELD_IDS " + FieldIDs + ");"
}

// Count returns the number of rows in source.
func (d DuckDB) Count(ctx context.Context, source string) (int64, error) {
	b, e := d.Run(ctx, "SELECT count(*) AS n FROM ("+source+");")
	if e != nil {
		return 0, e
	}
	var r []struct {
		N int64 `json:"n"`
	}
	e = json.Unmarshal(b, &r)
	if e != nil {
		return 0, e
	}
	if len(r) != 1 {
		return 0, fmt.Errorf("missing row count")
	}
	return r[0].N, nil
}

// Parquet reads local typed files without Hive partition columns. An empty
// list is an empty relation with the warehouse schema.
func Parquet(paths []string) string {
	if len(paths) == 0 {
		return "SELECT NULL::TIMESTAMPTZ_NS AS time,NULL::VARCHAR AS name," + "NULL::VARCHAR AS plugin,NULL::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,MAP{}::MAP(VARCHAR,VARCHAR) AS meta,NULL::VARIANT AS value WHERE false"
	}
	q := []string{}
	for _, p := range paths {
		q = append(q, Q(p))
	}
	return "SELECT * FROM read_parquet([" + strings.Join(q, ",") + "],hive_partitioning=false)"
}

// writeJSON publishes v by renaming a synced temporary file into place.
func writeJSON(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	tmp := path + ".pending"
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}

// syncFile flushes a completed artifact before its directory entry is trusted.
func syncFile(path string) error {
	f, e := os.OpenFile(path, os.O_RDWR, 0)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
