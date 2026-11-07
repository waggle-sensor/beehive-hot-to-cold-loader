// influx_csv_test.go checks that the cached CSV plan preserves records when the tag
// set changes, and that an invalid cache transition is rejected.

package storage

import (
	"beehive-hot-to-cold-loader/internal/control"
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// changingCSVFixture returns the changing CSV fixture.
func changingCSVFixture(t *testing.T) string {
	t.Helper()
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	lines := [][]string{
		{"#datatype", "string", "long", "dateTime:RFC3339", "double", "string", "string", "string", "string"},
		{"#default", "_result", "", "", "", "value", "m", "n1", "default"},
		{"", "result", "table", "_time", "_value", "_field", "_measurement", "node", "sensor"},
		{"", "", "0", "2025-03-01T00:00:00.000000194Z", "11.7", "", "", "", "s1"},
		{"", "", "0", "2025-03-01T00:00:00.000000195Z", "12.7", "", "", "", "s1"},
		{"", "", "0", "2025-03-01T00:00:00.000000196Z", "13.7", "", "", "n2", "s1"},
		{"", "", "0", "2025-03-01T00:00:00.000000197Z", "14.7", "", "", "", ""},
		{"#default", "_result", "", "", "", "value", "m", "n3", ""},
		{"", "", "0", "2025-03-01T00:00:00.000000198Z", "15.7", "", "", "", ""},
		{"#datatype", "string", "long", "string", "string", "dateTime:RFC3339", "unsignedLong", "string"},
		{"#default", "_result", "", "n4", "count", "", "", "value"},
		{"", "result", "table", "node", "_measurement", "_time", "_value", "_field"},
		{"", "", "1", "", "", "2025-03-01T00:00:00.000000199Z", "18446744073709551615", ""},
	}
	for _, line := range lines {
		if e := w.Write(line); e != nil {
			t.Fatal(e)
		}
	}
	w.Flush()
	if e := w.Error(); e != nil {
		t.Fatal(e)
	}
	return b.String()
}

func TestCSVPlanAndTagCachePreserveRecords(t *testing.T) {
	text := changingCSVFixture(t)
	start := int64(1740787200000000194)
	want := []control.Record{
		{Time: start, Name: "m", Kind: "float", Value: "11.7", Tags: map[string]string{"node": "n1", "sensor": "s1"}},
		{Time: start + 1, Name: "m", Kind: "float", Value: "12.7", Tags: map[string]string{"node": "n1", "sensor": "s1"}},
		{Time: start + 2, Name: "m", Kind: "float", Value: "13.7", Tags: map[string]string{"node": "n2", "sensor": "s1"}},
		{Time: start + 3, Name: "m", Kind: "float", Value: "14.7", Tags: map[string]string{"node": "n1", "sensor": "default"}},
		{Time: start + 4, Name: "m", Kind: "float", Value: "15.7", Tags: map[string]string{"node": "n3"}},
		{Time: start + 5, Name: "count", Kind: "uint", Value: "18446744073709551615", Tags: map[string]string{"node": "n4"}},
	}
	got := &extractionCollector{}
	if e := DecodeCSV(strings.NewReader(text), got, 0, 1<<63-1); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(want, got.rows) {
		t.Fatal("decoded records differ from explicit oracle", got.rows)
	}
	path := filepath.Join(t.TempDir(), "decoded.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	enc, finish, release, e := newRecordEncoder(f, path)
	if e != nil {
		f.Close()
		t.Fatal(e)
	}
	e = DecodeCSV(strings.NewReader(text), enc, 0, 1<<63-1)
	if e == nil {
		e = finish()
	}
	release()
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	checkExtractionOutput(t, path, want)
	got.rows[0].Tags["node"] = "mutated"
	if got.rows[1].Tags["node"] != "n1" {
		t.Fatal("generic records alias a cached tag map")
	}
}

func TestCSVCacheRejectsInvalidTransitions(t *testing.T) {
	text := changingCSVFixture(t)
	cases := []string{
		strings.Replace(text, "unsignedLong", "unrecognized", 1),
		strings.Replace(text, "11.7", "NaN", 1),
		strings.Replace(text, "_field,_measurement,node,sensor", "_field,_measurement,node,node", 1),
		strings.Replace(text, "double,string,string,string,string", "double,string,string,string,long", 1),
		text + ",error,reference\n,query failed,1\n",
	}
	for n, input := range cases {
		for _, fast := range []bool{false, true} {
			var enc recordEncoder = &extractionCollector{}
			var closeSink func()
			if fast {
				path := filepath.Join(t.TempDir(), "invalid.parquet")
				f, e := os.Create(path)
				if e != nil {
					t.Fatal(e)
				}
				p, _, release, e := newRecordEncoder(f, path)
				if e != nil {
					t.Fatal(e)
				}
				enc = p
				closeSink = func() { release(); f.Close() }
			}
			e := DecodeCSV(strings.NewReader(input), enc, 0, 1<<63-1)
			if closeSink != nil {
				closeSink()
			}
			if e == nil {
				t.Fatal("accepted invalid transition", n, fast)
			}
		}
	}
}
