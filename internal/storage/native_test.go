// native_test.go runs the local DuckDB 2 lifecycle and bounded plugin packing.
// It skips unless SAGE_ARCHIVE_DUCKDB2 is set, and it uses no real Pelican or Influx credentials.

package storage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/archive"
	"beehive-hot-to-cold-loader/internal/control"
)

// TestNativeLifecycle exercises real DuckDB 2.0, native VARIANT packing,
// atomic Iceberg append/replacement, remote scans and verified hot deletion.
// All IO is local to this test; no real Pelican/Influx credentials are used.
func TestNativeLifecycle(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("set SAGE_ARCHIVE_DUCKDB2 to DuckDB 2.0.x CLI; test needs local listener permission")
	}
	ctx := context.Background()
	p, root := pelicanFixture(t, true)
	duck := DuckDB{Binary: binary, Work: root, Memory: "512MB"}
	if e := duck.Check(ctx); e != nil {
		t.Fatal(e)
	}
	duck.testSetup = "LOAD httpfs; SET enable_server_cert_verification=false;\n"
	duck.testHTTP = ", VERIFY_SSL false"
	cat, e := OpenCatalog(ctx, filepath.Join(root, "catalog.db"), p.Base.String(), p)
	if e != nil {
		t.Fatal(e)
	}
	defer cat.DB.Close()
	if e = cat.Init(ctx); e != nil {
		t.Fatal(e)
	}
	store, e := control.Open(filepath.Join(root, "control.db"), "waggle", p.Base.String(), "2025-02-28")
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	script := filepath.Join(root, "influxd")
	lp := `t,plugin=p,node=n1 value=11.7 1740787200000000194
counter,node=n1 value=9007199254740993i 1740787200000000195
status,plugin=q,node=n1 value="first
second" 1740787200000000196
unsigned,plugin=p,node=n1 value=18446744073709551615u 1740787200000000197
`
	if e = os.WriteFile(script, []byte("#!/bin/sh\ncase \"$*\" in *2025-02-28*) exit 0;; esac\ncat <<'ARCHIVE_LP'\n"+lp+"ARCHIVE_LP\n"), 0700); e != nil {
		t.Fatal(e)
	}
	deleted := map[string]bool{}
	influx := Influx{URL: "https://influx.test", Org: "waggle", Bucket: "waggle", BucketID: "fixture", Engine: root, Binary: script, Token: "fixture"}
	influx.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var body []byte
		if req.Body != nil {
			body, _ = io.ReadAll(req.Body)
		}
		data := ""
		if req.Method == "GET" {
			data = `{"id":"fixture","name":"waggle","orgID":"org","retentionRules":[]}`
			if req.URL.Path == "/api/v2/orgs" {
				data = `{"orgs":[{"id":"org","name":"waggle"}]}`
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(data)), Header: make(http.Header)}, nil
		}
		var payload struct {
			Query string `json:"query"`
			Start string `json:"start"`
		}
		if e := json.Unmarshal(body, &payload); e != nil {
			t.Fatal(e)
		}
		if strings.HasPrefix(req.URL.Path, "/api/v2/delete") {
			deleted[payload.Start[:10]] = true
		} else if !deleted["2025-03-01"] && strings.Contains(payload.Query, `range(start: time(v: "2025-03-01T00:00:00Z")`) {
			data = `#datatype,string,long,dateTime:RFC3339,double,string,string,string,string
,result,table,_time,_value,_field,_measurement,node,plugin
,,0,2025-03-01T00:00:00.000000194Z,11.7,value,t,n1,p
#datatype,string,long,dateTime:RFC3339,long,string,string,string,string
,result,table,_time,_value,_field,_measurement,node,plugin
,,1,2025-03-01T00:00:00.000000195Z,9007199254740993,value,counter,n1,
#datatype,string,long,dateTime:RFC3339,string,string,string,string,string
,result,table,_time,_value,_field,_measurement,node,plugin
,,2,2025-03-01T00:00:00.000000196Z,"first
second",value,status,n1,q
#datatype,string,long,dateTime:RFC3339,unsignedLong,string,string,string,string
,result,table,_time,_value,_field,_measurement,node,plugin
,,3,2025-03-01T00:00:00.000000197Z,18446744073709551615,value,unsigned,n1,p
`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(data)), Header: make(http.Header)}, nil
	})}
	w := &Worker{Store: store, Influx: influx, Duck: duck, Catalog: cat}
	runner := &archive.Runner{Store: store, Worker: w, Root: filepath.Join(root, "jobs"), DeleteEnabled: true, KeepStaging: true}
	job, e := store.Next(time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC), 720*time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	for steps := 0; job.State != "COMPLETE" && steps < 20; steps++ {
		if e = runner.Step(ctx, job); e != nil {
			t.Fatalf("%s: %v", job.State, e)
		}
	}
	if job.State != "COMPLETE" || job.Day != "2025-02-28" || !deleted["2025-02-28"] {
		t.Fatal("empty initial day failed", job.State, deleted)
	}
	job, e = store.Next(time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC), 720*time.Hour)
	if e != nil || job == nil {
		t.Fatal(job, e)
	}
	for steps := 0; job.State != "COMPLETE" && steps < 20; steps++ {
		if e = runner.Step(ctx, job); e != nil {
			t.Fatalf("nonempty %s: %v", job.State, e)
		}
	}
	if job.State != "COMPLETE" || !deleted["2025-03-01"] {
		t.Fatal(job.State, deleted)
	}
	// Inspect actual Worker.Build outputs, not only the configuration helper.
	var expectedProfile, packingProfile PackingProfile
	for name, target := range map[string]*PackingProfile{"expected-profile.json": &expectedProfile, "packing-profile.json": &packingProfile} {
		data, e := os.ReadFile(filepath.Join(job.Work, name))
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(data, target); e != nil {
			t.Fatal(e)
		}
	}
	if expectedProfile.Threads != 16 || packingProfile.Threads != 16 || packingProfile.SortThreads != 2 || packingProfile.Canonicalization != "tag_dictionary" || !packingProfile.ReuseCanonicalStaging || w.Duck.threadCount() != 16 {
		t.Fatal("actual build did not preserve execution scope", expectedProfile, packingProfile)
	}
	var normalization ExportStats
	data, err := os.ReadFile(filepath.Join(job.Work, "export-dedup.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &normalization); err != nil {
		t.Fatal(err)
	}
	if normalization.Threads != 16 || normalization.MergeFanIn != 16 || normalization.MergeConcurrency != 1 || normalization.MaxMergeFanIn > 16 {
		t.Fatal("unexpected actual normalization policy", normalization)
	}
	requiredOperations := map[string]bool{"reconcile/EXPORTED": false, "verify/COMMITTED": false, "verify/DELETING": false, "delete/DELETING": false}
	operationFiles, e := filepath.Glob(filepath.Join(job.Work, "operation-*.json"))
	if e != nil {
		t.Fatal(e)
	}
	for _, path := range operationFiles {
		data, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		var report operationProfile
		if e = json.Unmarshal(data, &report); e != nil {
			t.Fatal(e)
		}
		if !report.Passed || report.Threads != 16 {
			t.Fatal("operation profile failed or base threads changed", report)
		}
		key := report.Operation + "/" + report.State
		if _, ok := requiredOperations[key]; ok {
			requiredOperations[key] = true
		}
		for _, step := range report.Steps {
			if step.Failed || !step.UsageAvailable {
				t.Fatal(step)
			}
			if step.Comparison != nil && (!step.Comparison.Passed || step.Comparison.ReusedInitial || step.Comparison.Threads != 16) {
				t.Fatal("remote/reconciliation proof was not fresh", step)
			}
			if step.Comparison != nil {
				canonicalization := "local_dictionary"
				if report.Operation == "reconcile" {
					canonicalization = "raw_tag_dictionary"
				}
				if step.Comparison.Canonicalization != canonicalization {
					t.Fatal("unexpected actual proof projection", report.Operation, step)
				}
			}
		}
	}
	for key, found := range requiredOperations {
		if !found {
			t.Fatal("missing operation profile", key)
		}
	}
	// Validate the read-only benchmark's pinned-manifest reader against the
	// fixture's actual v3 manifests and published Parquet inventory.
	journalBytes, e := os.ReadFile(filepath.Join(job.Work, "verification-COMMITTED.json"))
	if e != nil {
		t.Fatal(e)
	}
	var benchmarkTarget remoteBenchmarkTarget
	if e = json.Unmarshal(journalBytes, &benchmarkTarget); e != nil {
		t.Fatal(e)
	}
	benchmarkTarget.Warehouse = p.Base.String()
	reader := *p
	reader.ReadOnly = true
	snapshot, e := inspectRemoteComparison(&reader, benchmarkTarget)
	if e != nil || snapshot.Rows != 4 || snapshot.Objects != len(job.Files) || len(snapshot.MetadataSHA256) < 3 {
		t.Fatal(snapshot, e)
	}
	if _, e := reader.Token(true); e == nil {
		t.Fatal("benchmark reader obtained write authority")
	}
	if e := verifyRemoteObjectHashes(ctx, &reader, benchmarkTarget); e != nil {
		t.Fatal(e)
	}
	missing := benchmarkTarget
	missing.FileSHA256 = map[string]string{}
	if _, e := inspectRemoteComparison(&reader, missing); e == nil {
		t.Fatal("unlisted snapshot object accepted")
	}
	original, e := p.ReadFile(job.Files[0])
	if e != nil {
		t.Fatal(e)
	}
	changed := append([]byte(nil), original...)
	changed[0] ^= 1
	if e := p.WriteFile(job.Files[0], changed); e != nil {
		t.Fatal(e)
	}
	if e := verifyRemoteObjectHashes(ctx, &reader, benchmarkTarget); e == nil {
		t.Fatal("changed published bytes accepted")
	}
	if e := p.WriteFile(job.Files[0], original); e != nil {
		t.Fatal(e)
	}
	if e = store.Enqueue("late", []control.Record{{Time: 1740787200000000194, Name: "t", Tags: map[string]string{"node": "n1", "plugin": "p"}, Kind: "float", Value: "11.9"}}); e != nil {
		t.Fatal(e)
	}
	late, e := store.NextLate()
	if e != nil || late == nil {
		t.Fatal(late, e)
	}
	for steps := 0; late.State != "COMPLETE" && steps < 20; steps++ {
		if e = runner.Step(ctx, late); e != nil {
			t.Fatalf("late %s: %v", late.State, e)
		}
	}
	if late.State != "COMPLETE" || late.Metadata == job.Metadata {
		t.Fatal(late)
	}
	setup, e := w.setup()
	if e != nil {
		t.Fatal(e)
	}
	b, e := duck.Run(ctx, setup+"SELECT to_json(value)::VARCHAR AS value,epoch_ns(time) AS ns FROM ("+w.Scan(late.Metadata, late.Day)+") WHERE name='t';")
	if e != nil {
		t.Fatal(e)
	}
	var rows []struct {
		Value string `json:"value"`
		NS    int64  `json:"ns"`
	}
	if e = json.Unmarshal(b, &rows); e != nil {
		t.Fatal(e)
	}
	if len(rows) != 1 || rows[0].Value != "11.9" || rows[0].NS != 1740787200000000194 {
		t.Fatal(string(b))
	}
	// The original pinned snapshot still exists after partition replacement.
	if e = duck.Equal(ctx, Parquet([]string{filepath.Join(job.Work, "expected.parquet")}), w.Scan(job.Metadata, job.Day), setup); e != nil {
		t.Fatal("old snapshot invalidated", e)
	}
}

func TestNativeBoundedPluginPacking(t *testing.T) {
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if binary == "" {
		t.Skip("set SAGE_ARCHIVE_DUCKDB2 to validate native bounded VARIANT packing")
	}
	ctx := context.Background()
	root := t.TempDir()
	duck := DuckDB{Binary: binary, Work: root, Memory: "512MB"}
	if e := duck.Check(ctx); e != nil {
		t.Fatal(e)
	}
	source := `SELECT make_timestamp_ns(1740787200000000194+n)::TIMESTAMPTZ_NS AS time,'t'::VARCHAR AS name,
 CASE WHEN n<500000 THEN 'a' WHEN n<500100 THEN 'b' WHEN n<600000 THEN 'c' ELSE NULL END AS plugin,
 CASE WHEN n%2=0 THEN 'N1' ELSE 'N2' END AS vsn,
 NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,
 MAP{}::MAP(VARCHAR,VARCHAR) AS meta,
 CASE WHEN n%4=0 THEN (9007199254740993+n)::BIGINT::VARIANT WHEN n%4=1 THEN 11.7::DOUBLE::VARIANT
 WHEN n%4=2 THEN '123'::VARCHAR::VARIANT ELSE true::VARIANT END AS value
 FROM range(600005) r(n)`
	input := filepath.Join(root, "input.parquet")
	output := filepath.Join(root, "output.parquet")
	if _, e := duck.Run(ctx, CopySQL(source+" ORDER BY plugin NULLS LAST,vsn NULLS LAST,time", input)); e != nil {
		t.Fatal(e)
	}
	if e := pack(ctx, input, output, 262144); e != nil {
		t.Fatal(e)
	}
	groups, e := auditPacked(ctx, output)
	if e != nil || groups != 5 {
		t.Fatal("expected two large-plugin groups, two small-plugin groups, one null-plugin group", groups, e)
	}
	if e = duck.Equal(ctx, Parquet([]string{input}), Parquet([]string{output}), ""); e != nil {
		t.Fatal(e)
	}
}
