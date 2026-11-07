// compare_test.go checks the spill budget. Free space below the reserve is a capacity
// error, and a larger disk does not raise the 16 GiB cap.

package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

func TestCapacityErrorsRetainSafeClassification(t *testing.T) {
	for _, free := range []uint64{0, stagingReserve - 1, stagingReserve} {
		if _, e := spillBudget(free); !errors.Is(e, control.ErrStagingCapacity) {
			t.Fatal(free, e)
		}
	}
	for _, test := range []struct{ free, want uint64 }{{10 << 30, 2 << 30}, {100 << 30, 16 << 30}} {
		if got, e := spillBudget(test.free); e != nil || got != test.want {
			t.Fatal(got, e)
		}
	}
	for _, message := range []string{"failed to offload data block: max_temp_directory_size", "write: no space left on device"} {
		if !errors.Is(capacityError(errors.New(message)), control.ErrStagingCapacity) {
			t.Fatal(message)
		}
	}
	if errors.Is(capacityError(errors.New("record mismatch")), control.ErrStagingCapacity) {
		t.Fatal("classified data mismatch as capacity")
	}
	_, _, e := partitionInventory(filepath.Join(t.TempDir(), "missing"), 16)
	if e != nil {
		t.Fatal(e)
	}
}

// Exact native gate: all small records collide in routing partitions, including
// duplicate observations. Counts/hashes alone cannot pass the mismatch cases.
func TestNativeCanonicalPartitionComparison(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("set SAGE_ARCHIVE_DUCKDB2 for DuckDB 2.0 comparison gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	d := DuckDB{Binary: binary, Work: t.TempDir(), Memory: "128MB"}
	if e := d.Check(ctx); e != nil {
		t.Fatal(e)
	}
	base := `SELECT 1740787200000000194::BIGINT AS time,'x' AS name,'{"node":"n","sensor":"a,b=c"}' AS tags,'string' AS kind,'"a\nb"' AS value`
	duplicate := "(" + base + ") UNION ALL (" + base + ")"
	if e := d.EqualCanonical(ctx, duplicate, duplicate, ""); e != nil {
		t.Fatal(e)
	}
	for _, changed := range []string{
		"SELECT * REPLACE (time+1 AS time) FROM (" + base + ")",
		"SELECT * REPLACE ('y' AS name) FROM (" + base + ")",
		"SELECT * REPLACE ('{}' AS tags) FROM (" + base + ")",
		"SELECT * REPLACE ('float' AS kind) FROM (" + base + ")",
		"SELECT * REPLACE ('0' AS value) FROM (" + base + ")",
	} {
		if e := d.EqualCanonical(ctx, duplicate, "("+base+") UNION ALL ("+changed+")", ""); e == nil {
			t.Fatal("accepted altered record", changed)
		}
	}
	if e := d.EqualCanonical(ctx, duplicate, base, ""); e == nil {
		t.Fatal("lost duplicate multiplicity")
	}
	// Deliberately choose a different full record with the SAME routing hash
	// bucket. This must fail the exact comparison despite equal bucket counts.
	collision := "SELECT * REPLACE (i::VARCHAR AS value) FROM (" + base + "),range(10000) r(i) WHERE hash(time,name,tags,kind,i::VARCHAR)%16=(SELECT hash(time,name,tags,kind,value)%16 FROM (" + base + ")) LIMIT 1"
	if n, e := d.Count(ctx, collision); e != nil || n != 1 {
		t.Fatal("failed to construct routing collision", n, e)
	}
	if e := d.EqualCanonical(ctx, duplicate, "("+base+") UNION ALL (SELECT "+canonicalColumns+" FROM ("+collision+"))", ""); e == nil {
		t.Fatal("routing hash collision hid a different value")
	}
	// Force recursive local refinement with >1M rows, under a small buffer budget.
	large := `SELECT 1740787200000000194+i::BIGINT AS time,'x' AS name,'{}' AS tags,'int' AS kind,i::VARCHAR AS value FROM range(17000000) r(i)`
	if e := d.EqualCanonical(ctx, large, large, ""); e != nil {
		t.Fatal(e)
	}
	paths, _ := filepath.Glob(filepath.Join(d.Work, "compare-*"))
	if len(paths) != 0 {
		t.Fatal("comparison scratch not reclaimed", paths)
	}
}

func TestComparisonWriterBudget(t *testing.T) {
	for _, threads := range []int{2, 16, 256} {
		_, open := (DuckDB{Threads: threads}).comparisonWriterSettings()
		if open != min(16*threads, 256) {
			t.Fatal("writer budget", threads, open)
		}
	}
}

// Offline exact gate exercises fixed initial routing and recursive refinement
// at a private lower row cap, without a huge fixture.
func TestNativeBoundedComparison(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("native DuckDB 2 required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	threads, e := optionalThreadCount("SAGE_COMPARE_THREADS")
	if e != nil {
		t.Fatal(e)
	}
	if threads == 0 {
		threads = 2
	}
	d := DuckDB{Binary: binary, Work: t.TempDir(), Memory: "512MB", Threads: threads, comparisonLimit: 128}
	if e := d.CheckVersion(ctx); e != nil {
		t.Fatal(e)
	}
	base := `SELECT 1740787200000000194+i::BIGINT AS time,'x' AS name,'{"node":"n","sensor":"a,b=c"}' AS tags,'string' AS kind,i::VARCHAR AS value FROM range(4096) r(i)`

	var profile ComparisonProfile
	d.comparisonProfile = &profile
	if e := d.EqualCanonical(ctx, base, base, ""); e != nil {
		t.Fatal(e)
	}
	want := 16
	if !profile.Passed || profile.InitialFanout != want || profile.RefinementPasses == 0 || profile.InitialRoutingSeconds <= 0 || profile.RefinementSeconds <= 0 {
		t.Fatal(profile)
	}
	// Same row counts but a changed value must fail, never pass on count/routing.
	changed := "SELECT * REPLACE (CASE WHEN value='0' THEN 'changed' ELSE value END AS value) FROM (" + base + ")"
	if e := d.EqualCanonical(ctx, base, changed, ""); e == nil || profile.Passed {
		t.Fatal("changed value accepted")
	}
	// Equal counts and equal routing bucket: only full leaf comparison can reject.
	one := `SELECT 194::BIGINT AS time,'x' AS name,'{}' AS tags,'string' AS kind,'original' AS value`
	collision := "SELECT time,name,tags,kind,i::VARCHAR AS value FROM (" + one + "),range(10000) r(i) WHERE hash(time,name,tags,kind,i::VARCHAR)%16=(SELECT hash(time,name,tags,kind,value)%16 FROM (" + one + ")) LIMIT 1"
	if e := d.EqualCanonical(ctx, one, collision, ""); e == nil {
		t.Fatal("routing collision accepted")
	}
	// Equal totals, bucket counts and distinct sets, different multiplicities.
	aab := "(" + one + ") UNION ALL (" + one + ") UNION ALL (" + collision + ")"
	abb := "(" + one + ") UNION ALL (" + collision + ") UNION ALL (" + collision + ")"
	if e := d.EqualCanonical(ctx, aab, abb, ""); e == nil {
		t.Fatal("equal-count multiplicity mismatch accepted")
	}
	empty := "SELECT * FROM (" + one + ") WHERE false"
	if e := d.EqualCanonical(ctx, empty, empty, ""); e != nil {
		t.Fatal("empty equality", e)
	}
	duplicate := "(" + one + ") UNION ALL (" + one + ")"
	if e := d.EqualCanonical(ctx, duplicate, duplicate, ""); e != nil {
		t.Fatal(e)
	}
	if e := d.EqualCanonical(ctx, duplicate, one, ""); e == nil {
		t.Fatal("lost multiplicity")
	}
	// All rows share one full hash: exhausting hash bits must remain fail-closed.
	d.comparisonLimit = 1
	identical := `SELECT 194::BIGINT AS time,'x' AS name,'{}' AS tags,'string' AS kind,'same' AS value FROM range(17)`
	if e := d.EqualCanonical(ctx, identical, identical, ""); !errors.Is(e, control.ErrStagingCapacity) {
		t.Fatal("unbounded collision accepted", e)
	}
	paths, _ := filepath.Glob(filepath.Join(d.Work, "compare-*"))
	if len(paths) != 0 {
		t.Fatal("comparison scratch remains", paths)
	}
}
