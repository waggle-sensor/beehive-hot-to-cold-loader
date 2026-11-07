// typed_dictionary_test.go is the native gate for the build-local tag dictionary.
// It skips unless SAGE_ARCHIVE_DUCKDB2 points at a DuckDB 2.0 CLI.

package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestNativeCanonicalTagDictionaryGate(t *testing.T) {
	if os.Getenv("SAGE_ARCHIVE_DUCKDB2") == "" {
		t.Skip("native DuckDB 2 required")
	}
	d := DuckDB{Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: t.TempDir(), Memory: "3GB", Threads: 16}
	ctx := context.Background()
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	// Duplicate records, differently ordered maps, nullable keys/values, promoted
	// overrides, empty/null metadata, escaped strings, every scalar VARIANT kind,
	// and nanosecond instants. A second case has unique tag tuples on every row.
	base := `SELECT make_timestamp_ns(1740787200000000194+n%7)::TIMESTAMPTZ_NS AS time,'temperature'::VARCHAR AS name,
 CASE WHEN n%3=0 THEN NULL ELSE 'p' END::VARCHAR AS plugin,'N1'::VARCHAR AS vsn,
 NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,
 CASE n%5 WHEN 0 THEN MAP{'z':'last','a':'first','plugin':'old'} WHEN 1 THEN MAP{'plugin':'old','a':'first','z':'last'}
 WHEN 2 THEN MAP{'nullable':NULL,'escaped':E'line\n"slash\\'} WHEN 3 THEN MAP{} ELSE NULL END::MAP(VARCHAR,VARCHAR) AS meta,
 CASE n%6 WHEN 0 THEN 9007199254740993::BIGINT::VARIANT WHEN 1 THEN 18446744073709551615::UBIGINT::VARIANT
 WHEN 2 THEN 11.7::DOUBLE::VARIANT WHEN 3 THEN true::VARIANT WHEN 4 THEN E'line\n"slash\\'::VARCHAR::VARIANT ELSE NULL::VARIANT END AS value
 FROM range(210) r(n)`
	changes := []string{}
	for i, key := range Tags {
		changes = append(changes, fmt.Sprintf("CASE WHEN id%%%d=0 THEN NULL ELSE (id%%%d)::VARCHAR END AS %s", i+2, i+3, key))
	}
	for name, source := range map[string]string{
		"duplicates": "SELECT * FROM (" + base + ") UNION ALL SELECT * FROM (" + base + ")",
		"unique":     "SELECT * REPLACE (MAP{'unique':row_number() OVER ()::VARCHAR} AS meta) FROM (" + base + ")",
		"empty":      "SELECT * FROM (" + base + ") WHERE false",
		"promoted":   "WITH numbered AS (SELECT *,row_number() OVER () AS id FROM (" + base + ")) SELECT * EXCLUDE(id) REPLACE (" + strings.Join(changes, ",") + ") FROM numbered",
	} {
		t.Run(name, func(t *testing.T) {
			// Compare the SQL fixture directly. This DuckDB cannot store the
			// maximum uint64 inside a VARIANT Parquet column, while canonical
			// to_json text can. Packing/lifecycle tests cover real Parquet.
			baseline, candidate := Canonical(source), typedDictionaryCanonical(source)
			if _, e := d.Run(ctx, exactPartitionSQL(baseline, candidate)); e != nil {
				t.Fatal("dictionary changed complete canonical multiset", e)
			}
			if name != "empty" {
				for _, mutation := range []string{"tags || 'corrupt' AS tags", "time+1 AS time", "'corrupt' AS kind"} {
					if _, e := d.Run(ctx, exactPartitionSQL(baseline, "SELECT * REPLACE ("+mutation+") FROM ("+candidate+")")); e == nil {
						t.Fatal("independent proof accepted mutation", mutation)
					}
				}
				if name == "duplicates" {
					if _, e := d.Run(ctx, exactPartitionSQL(baseline, "SELECT DISTINCT * FROM ("+candidate+")")); e == nil {
						t.Fatal("independent proof accepted lost duplicate multiplicity")
					}
				}
			}
		})
	}
}
