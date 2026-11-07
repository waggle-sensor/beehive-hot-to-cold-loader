// value_test.go proves the uint64 decimal convention through native Parquet.
// It skips unless SAGE_ARCHIVE_DUCKDB2 is set, and it uses no production warehouse.

package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeUnsignedVariantProof(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("set SAGE_ARCHIVE_DUCKDB2")
	}
	ctx := context.Background()
	work := t.TempDir()
	duck := DuckDB{Binary: binary, Work: work, Memory: "512MB"}
	if err := duck.CheckVersion(ctx); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(work, "input.ndjson")
	rows := []string{
		`{"time":1740787200000000194,"name":"large-u","tags":{"vsn":"a"},"kind":"uint","value":"18446744073709551615","seq":1}`,
		`{"time":1740787200000000195,"name":"small-u","tags":{"vsn":"a"},"kind":"uint","value":"6","seq":2}`,
		`{"time":1740787200000000196,"name":"small-i","tags":{"vsn":"a"},"kind":"int","value":"6","seq":3}`,
	}
	if err := os.WriteFile(input, []byte(strings.Join(rows, "\n")), 0600); err != nil {
		t.Fatal(err)
	}
	source := "SELECT * EXCLUDE(seq) FROM (" + Typed(input) + ")"
	output := filepath.Join(work, "stored.parquet")
	if _, err := duck.Run(ctx, CopySQL(source+" ORDER BY plugin,vsn,time", output)); err != nil {
		t.Fatal(err)
	}
	if err := duck.Equal(ctx, source, Parquet([]string{output}), ""); err != nil {
		t.Fatal("unsigned Parquet proof", err)
	}
	packed := filepath.Join(work, "packed.parquet")
	if err := pack(ctx, output, packed, 262144); err != nil {
		t.Fatal(err)
	}
	if err := duck.Equal(ctx, source, Parquet([]string{packed}), ""); err != nil {
		t.Fatal("unsigned plugin-packing proof", err)
	}
	data, err := duck.Run(ctx, Canonical(Parquet([]string{output}))+" ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	var result []struct{ Name, Kind, Value string }
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 || result[0].Kind != "UINT64" || result[0].Value != "18446744073709551615" {
		t.Fatalf("%s", data)
	}
	unsigned := "SELECT * FROM (" + Parquet([]string{output}) + ") WHERE name='small-u'"
	changed := "SELECT * REPLACE(6::BIGINT::VARIANT AS value) FROM (" + unsigned + ")"
	if err = duck.Equal(ctx, unsigned, changed, ""); err == nil {
		t.Fatal("proof accepted uint to int kind loss")
	}
	for _, value := range []string{"-1", "18446744073709551616"} {
		_, err = duck.Run(ctx, "SELECT "+logicalValueKind("value")+" FROM (SELECT "+Q(value)+"::DECIMAL(20,0)::VARIANT AS value)")
		if err == nil {
			t.Fatal("accepted invalid unsigned encoding", value)
		}
	}
}
