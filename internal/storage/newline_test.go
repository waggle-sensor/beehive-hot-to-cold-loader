// newline_test.go checks that string values containing newlines survive annotated
// CSV export and match the rows returned by the query.

package storage

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewlineStringsExportAndQueryAgree(t *testing.T) {
	const start int64 = 1740787200000000000
	values := []string{"first\nsecond", "\nleading", "trailing\n", "a\n\nb", `literal\ntext`, "quote \" slash \\\nnext", "a\nforged,node=n1 value=99i 1740787200000000000"}
	var lp, annotated bytes.Buffer
	annotated.WriteString("#datatype,string,long,dateTime:RFC3339,string,string,string,string\n")
	w := csv.NewWriter(&annotated)
	w.Write([]string{"", "result", "table", "_time", "_value", "_field", "_measurement", "node"})
	for n, value := range values {
		stamp := start + 194 + int64(n)
		escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value)
		fmt.Fprintf(&lp, "status,node=n1 value=\"%s\" %d\n", escaped, stamp)
		w.Write([]string{"", "", "0", time.Unix(0, stamp).UTC().Format(time.RFC3339Nano), value, "value", "status", "n1"})
	}
	w.Flush()
	if e := w.Error(); e != nil {
		t.Fatal(e)
	}
	// Influx 2.7.7 emits CRLF even for embedded stored LF. Go's CSV reader
	// normalizes those transport newlines; keep that dialect in this regression.
	annotatedCRLF := strings.ReplaceAll(annotated.String(), "\n", "\r\n")
	for label, text := range map[string]string{"export": lp.String(), "query": annotatedCRLF} {
		path := filepath.Join(t.TempDir(), label+".parquet")
		f, e := os.Create(path)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		enc, finish, release, e := newRecordEncoder(f, path)
		if e != nil {
			t.Fatal(e)
		}
		defer release()
		decode := streamExport
		if label == "query" {
			decode = DecodeCSV
		}
		if e = decode(strings.NewReader(text), enc, start, start+86400000000000); e != nil {
			t.Fatal(e)
		}
		if e = finish(); e != nil {
			t.Fatal(e)
		}
		if e = f.Sync(); e != nil {
			t.Fatal(e)
		}
		n := 0
		if e = visitStagingFixture(path, func(row exportRecord) error {
			if n >= len(values) || row.Value != values[n] || row.Time != start+194+int64(n) || row.Tags["node"] != "n1" {
				t.Fatalf("%s Parquet changed row %d: %+v", label, n, row)
			}
			n++
			return nil
		}); e != nil || n != len(values) {
			t.Fatal(label, n, e)
		}
	}
	var exported, queried, queriedCRLF bytes.Buffer
	if e := streamExport(&lp, json.NewEncoder(&exported), start, start+86400000000000); e != nil {
		t.Fatal(e)
	}
	if e := DecodeCSV(&annotated, json.NewEncoder(&queried), start, start+86400000000000); e != nil {
		t.Fatal(e)
	}
	if e := DecodeCSV(strings.NewReader(annotatedCRLF), json.NewEncoder(&queriedCRLF), start, start+86400000000000); e != nil {
		t.Fatal(e)
	}
	for label, output := range map[string]*bytes.Buffer{"export": &exported, "query": &queried, "query-CRLF": &queriedCRLF} {
		if bytes.Count(output.Bytes(), []byte{'\n'}) != len(values) {
			t.Fatalf("%s: strings must remain escaped inside NDJSON records", label)
		}
		decoder := json.NewDecoder(output)
		for n, value := range values {
			var row exportRecord
			if e := decoder.Decode(&row); e != nil {
				t.Fatal(label, e)
			}
			if row.Value != value || row.Kind != "string" || row.Name != "status" || row.Tags["node"] != "n1" || row.Time != start+194+int64(n) {
				t.Fatalf("%s changed string/identity: %+v", label, row)
			}
		}
		var extra exportRecord
		if e := decoder.Decode(&extra); e != io.EOF {
			t.Fatalf("%s: unexpected extra record: %+v (%v)", label, extra, e)
		}
	}
}
