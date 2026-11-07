// remote_verification_test.go checks local-dictionary verification against the
// original canonical scan, and rejects an expected input that is not local Parquet.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeLocalDictionaryVerification(t *testing.T) {
	if os.Getenv("SAGE_ARCHIVE_DUCKDB2") == "" {
		t.Skip("native DuckDB 2 required")
	}
	d := DuckDB{Binary: os.Getenv("SAGE_ARCHIVE_DUCKDB2"), Work: t.TempDir(), Memory: "3GB", Threads: 16}
	ctx := context.Background()
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	base := `SELECT make_timestamp_ns(1740787200000000194+n)::TIMESTAMPTZ_NS AS time,'x' AS name,
 'p'::VARCHAR AS plugin,'v'::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
 NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,
 MAP{'sensor':(n%7)::VARCHAR}::MAP(VARCHAR,VARCHAR) AS meta,
 CASE n%4 WHEN 0 THEN 9007199254740993::BIGINT::VARIANT WHEN 1 THEN 11.7::DOUBLE::VARIANT
 WHEN 2 THEN true::VARIANT ELSE E'line\nquote"slash\\'::VARCHAR::VARIANT END AS value FROM range(100) r(n)`
	for name, source := range map[string]string{"equal": base, "duplicates": "(" + base + ") UNION ALL (" + base + ")", "empty": "SELECT * FROM (" + base + ") WHERE false"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(d.Work, name+".parquet")
			if _, e := d.Run(ctx, CopySQL(source, path)); e != nil {
				t.Fatal(e)
			}
			checker := d
			var proof ComparisonProfile
			checker.comparisonProfile = &proof
			if e := checker.EqualRemote(ctx, path, source, ""); e != nil || !proof.Passed || proof.ReusedInitial || proof.Canonicalization != "local_dictionary" {
				t.Fatal("fresh promoted verification failed", proof, e)
			}
			// The independent oracle must stay original on both sides.
			if e := checker.Equal(ctx, Parquet([]string{path}), source, ""); e != nil || !proof.Passed || proof.Canonicalization != "baseline" {
				t.Fatal("independent original proof changed", proof, e)
			}
			if name != "empty" {
				for _, mutation := range []string{"time+1 AS time", "MAP{'bad':'meta'} AS meta", "false::VARIANT AS value"} {
					if e := checker.EqualRemote(ctx, path, "SELECT * REPLACE ("+mutation+") FROM ("+source+")", ""); e == nil {
						t.Fatal("accepted changed remote row", mutation)
					}
				}
			}
			if name == "duplicates" {
				if e := checker.EqualRemote(ctx, path, base, ""); e == nil {
					t.Fatal("accepted missing remote duplicates")
				}
			}
		})
	}
}

// TestRemoteVerificationRequiresLocalExpectedParquet tests that a remote verification requires local expected Parquet.
func TestRemoteVerificationRequiresLocalExpectedParquet(t *testing.T) {
	root := t.TempDir()
	bad := filepath.Join(root, "expected.bin")
	if e := os.WriteFile(bad, []byte("admission fixture"), 0600); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(root, "link.parquet")
	if e := os.Symlink(bad, link); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{bad, link, root, "https://example.invalid/expected.parquet"} {
		if e := (DuckDB{}).EqualRemote(context.Background(), path, "SELECT 1", ""); e == nil {
			t.Fatal("accepted invalid local expected", path)
		}
	}
}
