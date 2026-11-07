// influx_fixture_helpers_test.go checks the isolated fixture's metrics, TSM copy,
// expected nanosecond bounds, and the split between tombstone cleanup and full compaction.

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"beehive-hot-to-cold-loader/internal/control"
)

// TestFixtureFullMetricsScopesCompletedCompactions tests that a fixture full metrics scopes completed compactions.
func TestFixtureFullMetricsScopesCompletedCompactions(t *testing.T) {
	text := `# HELP storage_compactions_duration_seconds Histogram of compactions by level since startup
storage_compactions_duration_seconds_count{bucket="target",id="7",level="cache"} 9
storage_compactions_duration_seconds_count{bucket="target",id="7",level="optimize"} 8
storage_compactions_duration_seconds_count{bucket="other",id="7",level="full"} 7
storage_compactions_duration_seconds_count{bucket="target",id="8",level="full"} 6
storage_compactions_duration_seconds_count{path="/fixture/a b",bucket="target",id="7",level="full"} 2
storage_compactions_failed{bucket="target",id="7",level="full"} 1
storage_compactions_active{bucket="target",id="7",level="full"} 0
`
	got, e := fixtureFullMetrics(text, "target", "7")
	if e != nil || got.Completed != 2 || got.Failed != 1 || got.Active != 0 {
		t.Fatal(got, e)
	}
	if _, e = fixtureFullMetrics("unrelated metrics only", "target", "7"); e == nil {
		t.Fatal("missing metrics must not look like no compaction")
	}
	for _, value := range []string{"NaN", "+Inf", "-1", "garbage"} {
		sample := fmt.Sprintf("# HELP storage_compactions_duration_seconds test\nstorage_compactions_duration_seconds_count{bucket=\"target\",id=\"7\",level=\"full\"} %s\n", value)
		if _, e = fixtureFullMetrics(sample, "target", "7"); e == nil {
			t.Fatal("accepted invalid completion count", value)
		}
	}
}

// TestFixtureTSMViewCopiesWithoutWALOrSharedFiles tests that a fixture TSM view copies without WAL or shared files.
func TestFixtureTSMViewCopiesWithoutWALOrSharedFiles(t *testing.T) {
	root := t.TempDir()
	bucket := "isolated"
	tsm := filepath.Join(root, "data", bucket, "autogen", "7", "000000001-000000001.tsm")
	tombstone := tsm[:len(tsm)-4] + ".tombstone"
	wal := filepath.Join(root, "wal", bucket, "autogen", "7", "_00001.wal")
	other := filepath.Join(root, "data", "other", "autogen", "7", "other.tsm")
	for _, path := range []string{tsm, tombstone, wal, other} {
		if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(path, []byte("source"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	destination := t.TempDir()
	inventory, e := fixtureTSMView(root, bucket, destination)
	if e != nil || len(inventory.TSM) != 1 || len(inventory.Tombstones) != 1 || len(inventory.WAL) != 1 {
		t.Fatal(inventory, e)
	}
	copied, e := fixtureInventory(destination, bucket)
	if e != nil || len(copied.TSM) != 1 || len(copied.Tombstones) != 1 || len(copied.WAL) != 0 {
		t.Fatal(copied, e)
	}
	if _, e = os.Stat(filepath.Join(destination, "data", "other")); !os.IsNotExist(e) {
		t.Fatal("copied another bucket", e)
	}
	if e = os.WriteFile(copied.TSM[0], []byte("changed private copy"), 0600); e != nil {
		t.Fatal(e)
	}
	contents, e := os.ReadFile(tsm)
	if e != nil || string(contents) != "source" {
		t.Fatal("view shared mutable storage with engine", e)
	}
	link := filepath.Join(root, "data", bucket, "autogen", "7", "link.tsm")
	if e = os.Symlink(tsm, link); e != nil {
		t.Fatal(e)
	}
	if _, e = fixtureInventory(root, bucket); e == nil {
		t.Fatal("fixture followed a symlink")
	}
}

func TestFixtureExpectedNanosecondBoundariesAndDeletedDay(t *testing.T) {
	cases := []struct {
		day     string
		fillers int
		want    map[int64]string
	}{
		{"2025-02-28", 0, map[int64]string{fixtureStart - 1: "0"}},
		{"2025-03-01", 0, map[int64]string{fixtureStart: "1", fixtureEnd - 1: "2"}},
		{"2025-03-02", 0, map[int64]string{fixtureEnd: "3", fixtureEnd + 1: "4"}},
		{"2025-03-01", -1, map[int64]string{}},
	}
	for _, test := range cases {
		path := filepath.Join(t.TempDir(), "expected.ndjson")
		if e := fixtureExpected(path, test.day, "11.9", false, test.fillers); e != nil {
			t.Fatal(e)
		}
		got := map[int64]string{}
		count := 0
		if e := fixtureRecords(path, func(r control.Record) error {
			count++
			if r.Name == "boundary" {
				got[r.Time] = r.Value
			}
			if r.Name == "victim" {
				t.Fatal("deleted victim in expected state")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got, test.want) || test.fillers < 0 && count != 0 {
			t.Fatal(test.day, got, count)
		}
	}
}

// Replay the VM failure's file/metric observations: level-1 cleanup is valid
// tombstone evidence, but must not satisfy the later full-compaction phase.
// TestFixtureSeparatesTombstoneCleanupFromFullCompaction tests that a fixture separates tombstone cleanup from full compaction.
func TestFixtureSeparatesTombstoneCleanupFromFullCompaction(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "data", "target", "autogen", "1")
	if e := os.MkdirAll(base, 0700); e != nil {
		t.Fatal(e)
	}
	old := filepath.Join(base, "000000001-000000001.tsm")
	tombstone := filepath.Join(base, "000000001-000000001.tombstone")
	write := func(path string) {
		t.Helper()
		if e := os.WriteFile(path, []byte("fixture"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	remove := func(path string) {
		t.Helper()
		if e := os.Remove(path); e != nil {
			t.Fatal(e)
		}
	}
	write(old)
	write(tombstone)
	original := fixtureFiles{TSM: []string{old}, Tombstones: []string{tombstone}}
	if _, ready, e := fixtureReplacement(root, "target", original); e != nil || ready {
		t.Fatal("unreplaced inputs", ready, e)
	}
	level1 := filepath.Join(base, "000000002-000000002.tsm")
	write(level1)
	remove(old)
	remove(tombstone)
	cleaned, ready, e := fixtureReplacement(root, "target", original)
	if e != nil || !ready {
		t.Fatal("level-1 tombstone cleanup must complete its own phase", ready, e)
	}
	text := "# HELP storage_compactions_duration_seconds completed compactions\n" +
		`storage_compactions_duration_seconds_count{bucket="target",id="1",level="1"} 1` + "\n"
	full, e := fixtureFullMetrics(text, "target", "1")
	if e != nil || full.Completed != 0 {
		t.Fatal("level-1 cleanup counted as full compaction", full, e)
	}
	fresh := filepath.Join(base, "000000003-000000001.tsm")
	write(fresh)
	inputs := fixtureFiles{TSM: append(cleaned.TSM, fresh)}
	if _, ready, e = fixtureReplacement(root, "target", inputs); e != nil || ready {
		t.Fatal("fresh full inputs were not replaced", ready, e)
	}
	final := filepath.Join(base, "000000003-000000002.tsm")
	write(final)
	remove(level1)
	remove(fresh)
	leftover := filepath.Join(base, "000000003-000000002.tombstone")
	write(leftover)
	if _, ready, e = fixtureReplacement(root, "target", inputs); e != nil || ready {
		t.Fatal("remaining tombstone accepted", ready, e)
	}
	remove(leftover)
	if _, ready, e = fixtureReplacement(root, "target", inputs); e != nil || !ready {
		t.Fatal("full inputs should be replaced", ready, e)
	}
	text += `storage_compactions_duration_seconds_count{bucket="target",id="1",level="full"} 1` + "\n"
	full, e = fixtureFullMetrics(text, "target", "1")
	if e != nil || full.Completed != 1 {
		t.Fatal("missing actual full-compaction evidence", full, e)
	}
}
