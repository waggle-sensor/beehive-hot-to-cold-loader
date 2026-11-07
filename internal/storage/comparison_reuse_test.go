// comparison_reuse_test.go checks that a canonical artifact stays bound to the
// file that produced it. A replaced or rewritten file is not reused.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestComparisonArtifactBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact")
	if e := os.WriteFile(path, []byte("original"), 0600); e != nil {
		t.Fatal(e)
	}
	binding, e := bindFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if e := validateBindings([]fileBinding{binding}); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte("modified"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := validateBindings([]fileBinding{binding}); e == nil {
		t.Fatal("same-size modification accepted")
	}
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	}
	if e := validateBindings([]fileBinding{binding}); e == nil {
		t.Fatal("missing artifact accepted")
	}
	if e := os.Symlink("missing", path); e != nil {
		t.Fatal(e)
	}
	if _, e := bindFile(path); e == nil {
		t.Fatal("symlink accepted")
	}
}

func TestNativeCanonicalReuse(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("native DuckDB 2 required")
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	d := DuckDB{Binary: binary, Work: root, Memory: "512MB", Threads: 2, comparisonLimit: 16}
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	input := filepath.Join(root, "input.parquet")
	source := `SELECT 1740787200000000194+i::BIGINT AS time,'x' AS name,'{"node":"n"}' AS tags,'string' AS kind,i::VARCHAR AS value FROM range(512) r(i)`
	copy := func(source, path string) {
		t.Helper()
		if _, e := d.Run(ctx, "COPY ("+source+") TO "+Q(path)+" (FORMAT parquet);"); e != nil {
			t.Fatal(e)
		}
	}
	copy(source, input)
	reuse, e := newPackingReuse(input, root)
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(reuse.root)
	var paths []string
	for i := 0; i < 2; i++ {
		window := "SELECT * FROM (" + source + ") WHERE time >= 1740787200000000194+" + strconv.Itoa(i*256) + " AND time < 1740787200000000194+" + strconv.Itoa((i+1)*256)
		output := filepath.Join(root, "out-"+strconv.Itoa(i)+".parquet")
		copy(window, output)
		bound, e := bindFile(output)
		if e != nil {
			t.Fatal(e)
		}
		checker := d
		checker.Work = reuse.root
		var stage *canonicalStage
		if e := checker.equalCanonical(ctx, window, Parquet([]string{output}), "", &stage); e != nil {
			t.Fatal(e)
		}
		reuse.entries = append(reuse.entries, packingReuseEntry{offset: int64(i * 256), rows: 256, output: bound, stage: stage})
		paths = append(paths, output)
	}
	profile, e := d.compareReused(ctx, reuse, 512, paths)
	if e != nil || !profile.Passed || !profile.ReusedInitial || profile.InitialRoutingSeconds != 0 || profile.RefinementPasses == 0 || profile.ExactSeconds <= 0 {
		t.Fatal(profile, e)
	}
	if _, e := d.compareReused(ctx, reuse, 513, paths); e == nil {
		t.Fatal("total coverage mismatch accepted")
	}
	reuse.entries[1].offset = 0
	if _, e := d.compareReused(ctx, reuse, 512, paths); e == nil {
		t.Fatal("overlapping windows accepted")
	}
	reuse.entries[1].offset = 256
	if _, e := d.compareReused(ctx, reuse, 512, paths[:1]); e == nil {
		t.Fatal("missing output accepted")
	}
	// Same-size mutations must fail before a reused proof can pass. Restore only
	// this test's new files so each independent case starts with valid bindings.
	corrupt := func(path string) {
		t.Helper()
		before, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		after := append([]byte(nil), before...)
		after[len(after)/2] ^= 1
		if e := os.WriteFile(path, after, 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := d.compareReused(ctx, reuse, 512, paths); e == nil {
			t.Fatal("changed reuse artifact accepted", path)
		}
		if e := os.WriteFile(path, before, 0600); e != nil {
			t.Fatal(e)
		}
	}
	corrupt(input)
	corrupt(paths[0])
	corrupt(reuse.entries[0].stage.bindings[0].Path)
	if _, e := d.compareReused(ctx, reuse, 512, paths); e != nil {
		t.Fatal("restored proof failed", e)
	}

	// Rebind deliberately altered canonical content: bindings alone cannot accept
	// semantic equality. With large leaf limits this must reach exact leaf SQL.
	altered := reuse.entries[0].stage.bindings[0].Path
	pending := filepath.Join(root, "altered.parquet")
	copy("SELECT * REPLACE (value||'-changed' AS value) FROM read_parquet("+Q(altered)+",hive_partitioning=false)", pending)
	if e := os.Rename(pending, altered); e != nil {
		t.Fatal(e)
	}
	stage := reuse.entries[0].stage
	stage.bindings, e = bindCanonical(stage.left, stage.right)
	if e != nil {
		t.Fatal(e)
	}
	d.comparisonLimit = comparisonRows
	profile, e = d.compareReused(ctx, reuse, 512, paths)
	if e == nil || profile.Passed || profile.ExactSeconds <= 0 {
		t.Fatal("exact proof did not reject altered records", profile, e)
	}

}
