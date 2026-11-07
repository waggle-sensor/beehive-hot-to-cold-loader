// reconciliation_dictionary_test.go checks that the primitive reconciliation
// dictionary matches canonical SQL, and that the two inputs are independent local Parquet files.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Adapt the production Typed query to an in-memory primitive fixture. Real
// retained trials call Canonical(reconciliationBenchmarkSQL(path)) directly.
// reconciliationFixtureBaseline returns the baseline for a reconciliation dictionary.
func reconciliationFixtureBaseline(source string) string {
	path := "fixture.parquet"
	raw := "(SELECT time,name,tags::JSON::MAP(VARCHAR,VARCHAR) AS tags,kind,value,seq FROM (" + source + "))"
	return Canonical(strings.Replace(Typed(path), Raw(path), raw, 1))
}

func TestNativeReconciliationDictionary(t *testing.T) {
	if os.Getenv("SAGE_ARCHIVE_DUCKDB2") == "" {
		t.Skip("native DuckDB 2 required")
	}
	d := DuckDB{Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: t.TempDir(), Memory: "3GB", Threads: 16}
	ctx := context.Background()
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	// Noncanonical JSON spellings, null promoted/metadata keys, all promoted
	// keys, empty/null maps, escaped strings, max uint, >2^53 integers, ns, and
	// duplicate rows. Do not assume upstream JSON is already normalized.
	source := `SELECT 1740787200000000194+n%3 AS time,'x' AS name,
 CASE n%7 WHEN 0 THEN '{"plugin":"p","vsn":"n","sensor":"a"}'
 WHEN 1 THEN '{ "sensor": "a", "vsn":"n", "plugin":"p" }'
 WHEN 2 THEN '{"plugin":null,"vsn":null,"node":null,"host":null,"task":null,"job":null,"zone":null,"nullable":null}'
 WHEN 3 THEN '{"plugin":"p","vsn":"n","node":"a","host":"b","task":"c","job":"d","zone":"e","escaped":"line\nquote\"slash\\"}'
 WHEN 4 THEN '{}' WHEN 5 THEN NULL ELSE '{"unique":"'||n::VARCHAR||'"}' END::VARCHAR AS tags,
 CASE n%5 WHEN 0 THEN 'int' WHEN 1 THEN 'uint' WHEN 2 THEN 'float' WHEN 3 THEN 'bool' ELSE 'string' END AS kind,
 CASE n%5 WHEN 0 THEN '9007199254740993' WHEN 1 THEN '18446744073709551615' WHEN 2 THEN '11.7' WHEN 3 THEN 'true' ELSE E'line\nquote"slash\\' END::VARCHAR AS value,
 n::BIGINT AS seq FROM range(210) r(n)`
	for name, input := range map[string]string{"mixed": source, "duplicates": "(" + source + ") UNION ALL (" + source + ")", "empty": "SELECT * FROM (" + source + ") WHERE false", "null_values": "SELECT * REPLACE (NULL::VARCHAR AS value) FROM (" + source + ")"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(d.Work, name+".parquet")
			if _, e := d.Run(ctx, "COPY ("+input+") TO "+Q(path)+" (FORMAT parquet);"); e != nil {
				t.Fatal(e)
			}
			baseline, candidate := Canonical(reconciliationBenchmarkSQL(path)), reconciliationDictionarySQL(reconciliationPrimitiveSource(path))
			if _, e := d.Run(ctx, exactPartitionSQL(baseline, candidate)); e != nil {
				t.Fatal("primitive dictionary changed canonical rows", e)
			}
			query := filepath.Join(d.Work, name+"-query.parquet")
			data, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(query, data, 0600); e != nil {
				t.Fatal(e)
			}
			checker := d
			var proof ComparisonProfile
			checker.comparisonProfile = &proof
			if e = checker.EqualReconciliation(ctx, path, query); e != nil || !proof.Passed || proof.ReusedInitial || proof.Canonicalization != "raw_tag_dictionary" {
				t.Fatal("promoted fresh reconciliation path failed", proof, e)
			}
			if name != "empty" {
				for _, change := range []string{"time+1 AS time", "tags||'bad' AS tags", "'bad' AS name", "'bad' AS kind", "'bad' AS value"} {
					if _, e := d.Run(ctx, exactPartitionSQL(baseline, "SELECT * REPLACE ("+change+") FROM ("+candidate+")")); e == nil {
						t.Fatal("accepted mutation", change)
					}
				}
			}
			if name == "duplicates" {
				if _, e := d.Run(ctx, exactPartitionSQL(baseline, "SELECT DISTINCT * FROM ("+candidate+")")); e == nil {
					t.Fatal("accepted lost duplicates")
				}
			}
		})
	}
	for _, change := range []string{"'invalid' AS kind", "'not-an-int' AS value", "'{invalid' AS tags"} {
		bad := "SELECT * REPLACE (" + change + ") FROM (SELECT * FROM (" + source + ") WHERE kind='int')"
		for _, query := range []string{reconciliationFixtureBaseline(bad), reconciliationDictionarySQL(bad)} {
			if _, e := d.Run(ctx, "SELECT * FROM ("+query+");"); e == nil {
				t.Fatal("accepted malformed input", change)
			}
		}
	}
}

// TestReconciliationRejectsNonIndependentLocalInputs tests that a reconciliation rejects non-independent local inputs.
func TestReconciliationRejectsNonIndependentLocalInputs(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "export.parquet")
	if e := os.WriteFile(path, []byte("admission fixture"), 0600); e != nil {
		t.Fatal(e)
	}
	alias := filepath.Join(root, "alias.parquet")
	if e := os.Link(path, alias); e != nil {
		t.Fatal(e)
	}
	symlink := filepath.Join(root, "link.parquet")
	if e := os.Symlink(path, symlink); e != nil {
		t.Fatal(e)
	}
	for _, other := range []string{path, alias, symlink, "https://example.invalid/influx.parquet", root} {
		if e := (DuckDB{}).EqualReconciliation(context.Background(), path, other); e == nil {
			t.Fatal("accepted non-independent/non-local input", other)
		}
	}
}
