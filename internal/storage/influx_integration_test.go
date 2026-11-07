// influx_integration_test.go is the source-version rollout gate. It creates its own
// unlimited-retention bucket and skips unless the isolated fixture variables are set.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestInfluxEngineConsistency is an explicit source-version rollout gate.
// It creates its own unlimited-retention bucket and never touches an existing
// bucket. Use the isolated Compose fixture, not a production token.
func TestInfluxEngineConsistency(t *testing.T) {
	root := os.Getenv("SAGE_ARCHIVE_FIXTURE_ENGINE")
	base := os.Getenv("SAGE_ARCHIVE_FIXTURE_URL")
	token := os.Getenv("SAGE_ARCHIVE_FIXTURE_TOKEN")
	binary := os.Getenv("SAGE_ARCHIVE_DUCKDB2")
	if root == "" || base == "" || token == "" || binary == "" {
		t.Skip("set isolated SAGE_ARCHIVE_FIXTURE_{ENGINE,URL,TOKEN} and SAGE_ARCHIVE_DUCKDB2")
	}
	root, e := localPath(root)
	if e != nil {
		t.Fatal(e)
	}
	resolved, e := filepath.EvalSymlinks(root)
	if e != nil {
		t.Fatal(e)
	}
	root, e = localPath(resolved)
	if e != nil {
		t.Fatal(e)
	}
	// Check the tree roots before invoking the native exporter as well.
	if _, e = fixtureInventory(root, "fixture-preflight"); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: time.Minute}
	call := func(method, path, kind string, body []byte, out any) {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, bytes.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Authorization", "Token "+token)
		req.Header.Set("Content-Type", kind)
		resp, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			t.Fatalf("fixture %s HTTP %d", method, resp.StatusCode)
		}
		if out != nil {
			if e = json.NewDecoder(resp.Body).Decode(out); e != nil {
				t.Fatal(e)
			}
		}
	}
	org := os.Getenv("SAGE_ARCHIVE_FIXTURE_ORG")
	if org == "" {
		org = "waggle"
	}
	var orgs struct{ Orgs []struct{ ID string } }
	call("GET", "/api/v2/orgs?org="+url.QueryEscape(org), "application/json", nil, &orgs)
	if len(orgs.Orgs) != 1 {
		t.Fatal("fixture org missing")
	}
	name := "sage-archive-fixture-" + uuid.NewString()
	body, _ := json.Marshal(map[string]any{"name": name, "orgID": orgs.Orgs[0].ID, "retentionRules": []any{}})
	var bucket struct{ ID string }
	call("POST", "/api/v2/buckets", "application/json", body, &bucket)
	// Leave a failed fixture for inspection; remove only this test's bucket on success.
	t.Cleanup(func() {
		if !t.Failed() {
			req, _ := http.NewRequest("DELETE", strings.TrimRight(base, "/")+"/api/v2/buckets/"+bucket.ID, nil)
			req.Header.Set("Authorization", "Token "+token)
			if resp, e := client.Do(req); e == nil {
				resp.Body.Close()
			}
		}
	})
	write := func(lp string) {
		call("POST", "/api/v2/write?org="+url.QueryEscape(org)+"&bucket="+url.QueryEscape(name)+"&precision=ns", "text/plain", []byte(lp), nil)
	}
	const day = "2025-03-01"
	directory := os.Getenv("SAGE_ARCHIVE_FIXTURE_REPORT_DIR")
	if directory == "" {
		directory = t.TempDir()
	} else {
		var e error
		directory, e = localPath(directory)
		if e != nil {
			t.Fatal(e)
		}
		parent, e := filepath.EvalSymlinks(filepath.Dir(directory))
		if e != nil {
			t.Fatal(e)
		}
		if _, e = localPath(parent); e != nil {
			t.Fatal(e)
		}
		if e = os.Mkdir(directory, 0700); e != nil {
			t.Fatal("report directory must be fresh", e)
		}
	}
	duck := DuckDB{Binary: binary, Work: directory, Memory: "512MB"}
	if e := duck.Check(ctx); e != nil {
		t.Fatal(e)
	}
	influxd := os.Getenv("SAGE_ARCHIVE_INFLUXD")
	if influxd == "" {
		influxd = "influxd"
	}
	source := Influx{URL: base, Org: org, Bucket: name, Token: token, BucketID: bucket.ID, Engine: root, Binary: influxd, Client: client}
	if e := source.ValidateSource(ctx); e != nil {
		t.Fatal(e)
	}
	t.Logf("isolated fixture bucket=%s id=%s reports=%s", name, bucket.ID, directory)
	report := map[string]any{"bucket": name, "bucket_id": bucket.ID, "engine": root}
	t.Cleanup(func() {
		report["passed"] = !t.Failed()
		if e := writeJSON(filepath.Join(directory, "engine-consistency.json"), report); e != nil {
			t.Error(e)
		}
	})
	inventory := func() fixtureFiles {
		files, e := fixtureInventory(root, bucket.ID)
		if e != nil {
			t.Fatal(e)
		}
		return files
	}
	expected := func(label, queryDay, temperature string, victim bool, fillers int) string {
		path := filepath.Join(directory, label+"-expected.ndjson")
		if e := fixtureExpected(path, queryDay, temperature, victim, fillers); e != nil {
			t.Fatal(e)
		}
		return path
	}
	typed := func(path string) string { return "SELECT * EXCLUDE(seq) FROM (" + Typed(path) + ")" }
	reconcile := func(label, queryDay, temperature string, victim bool, fillers int) (string, ExportStats) {
		t.Helper()
		report["stage"] = label
		raw := filepath.Join(directory, label+"-raw.parquet")
		logical := filepath.Join(directory, label+"-logical.parquet")
		query := filepath.Join(directory, label+"-query.parquet")
		if e := source.Export(ctx, queryDay, raw); e != nil {
			t.Fatal(label, e)
		}
		stats, e := duck.DeduplicateExport(ctx, raw, logical)
		if e != nil {
			t.Fatal(label, e)
		}
		if e = source.Query(ctx, queryDay, query); e != nil {
			t.Fatal(label, e)
		}
		if e = duck.Equal(ctx, typed(logical), typed(query), ""); e != nil {
			t.Fatal(label, e)
		}
		if e = duck.Equal(ctx, typed(query), typed(expected(label, queryDay, temperature, victim, fillers)), ""); e != nil {
			t.Fatal(label, "independent expected state", e)
		}
		report[label] = map[string]any{"stats": stats, "files": inventory()}
		t.Logf("%s: reconciled %d logical points (%d superseded)", label, stats.UniqueRows, stats.SupersededRows)
		return raw, stats
	}
	// Only TSM/tombstone files are copied into this private view, never WAL.
	// Inspecting that view establishes where the value is stored, independently
	// of the query API and live export's combined TSM/WAL replay.
	tsmView := func(probeContext context.Context, probe func(string) (bool, error)) (bool, error) {
		staging, e := os.MkdirTemp(directory, "tsm-view-")
		if e != nil {
			return false, e
		}
		defer os.RemoveAll(staging)
		files, e := fixtureTSMView(root, bucket.ID, staging)
		if e != nil {
			return false, e
		}
		if len(files.TSM) == 0 {
			return false, nil
		}
		copySource := source
		copySource.Engine = staging
		raw := filepath.Join(staging, "raw.parquet")
		if e = copySource.Export(probeContext, day, raw); e != nil {
			return false, e
		}
		logical := filepath.Join(staging, "logical.parquet")
		if _, e = duck.DeduplicateExport(probeContext, raw, logical); e != nil {
			return false, e
		}
		return probe(logical)
	}
	requireHas := func(path, name, value string) {
		t.Helper()
		found, e := fixtureHas(path, name, value)
		if e != nil || !found {
			t.Fatal("missing expected replay record", name, value, e)
		}
	}
	// All five explicit boundary instants are distinct observations. Adjacent-day
	// checks below prove their survival, rather than merely finding one timestamp.
	var seed strings.Builder
	for n, value := range fixtureStrings {
		escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value)
		fmt.Fprintf(&seed, "multiline,node=n1 value=\"%s\" %d\n", escaped, fixtureStart+500+int64(n))
	}
	seed.WriteString("temp,plugin=p,node=n1 value=11.7 1740787200000000194\ncount,node=n1 value=9007199254740993i 1740787200000000195\nstatus,node=n1 value=\"123\" 1740787200000000196\nflag,node=n1 value=true 1740787200000000197\nwaltemp,node=n1 value=11.7 1740787200000000198\nvictim,node=n1 value=23i 1740787200000000300\n")
	for n, stamp := range []int64{fixtureStart - 1, fixtureStart, fixtureEnd - 1, fixtureEnd, fixtureEnd + 1} {
		fmt.Fprintf(&seed, "boundary,node=n1 value=%di %d\n", n, stamp)
	}
	write(seed.String())
	write("waltemp,node=n1 value=11.9 1740787200000000198\n")
	writeFiller := func(low, high int) {
		var filler strings.Builder
		for n := low; n < high; n++ {
			fmt.Fprintf(&filler, "filler,node=n1 value=%di %d\n", n, fixtureStart+10000+int64(n))
		}
		write(filler.String())
	}
	writeFiller(0, 30000)
	raw, stats := reconcile("wal-overwrite", day, "11.7", true, 30000)
	requireHas(raw, "waltemp", "11.7")
	requireHas(raw, "waltemp", "11.9")
	if stats.SupersededRows < 1 || len(inventory().TSM) != 0 {
		t.Fatal("initial overwrite was not proven WAL-only; increase fixture snapshot cold duration")
	}
	t.Log("waiting for the older temperature and victim to reach immutable TSM")
	report["stage"] = "waiting-for-older-value-in-tsm"
	if e := fixtureWait(ctx, "older value and victim in TSM", func(waitContext context.Context) (bool, error) {
		return tsmView(waitContext, func(path string) (bool, error) {
			old, e := fixtureHas(path, "temp", "11.7")
			if e != nil || !old {
				return false, e
			}
			return fixtureHas(path, "victim", "23")
		})
	}); e != nil {
		t.Fatal(e)
	}
	before := inventory()
	// This test deliberately keeps all observations in one target shard. Fail
	// rather than counting another shard's compaction as proof for these points.
	shard := ""
	for _, path := range before.TSM {
		id := filepath.Base(filepath.Dir(path))
		if shard != "" && id != shard {
			t.Fatal("fixture timestamps span multiple shards; expected one target shard")
		}
		shard = id
	}
	if shard == "" {
		t.Fatal("no target TSM shard")
	}
	report["older-value-in-tsm"] = before
	write("temp,plugin=p,node=n1 value=11.9 1740787200000000194\n")
	raw, stats = reconcile("tsm-to-wal-overwrite", day, "11.9", true, 30000)
	requireHas(raw, "temp", "11.7")
	requireHas(raw, "temp", "11.9")
	if stats.SupersededRows < 1 {
		t.Fatal("TSM/WAL overwrite did not supersede a replay row")
	}
	ready, e := tsmView(ctx, func(path string) (bool, error) {
		newer, e := fixtureHas(path, "temp", "11.9")
		if e != nil {
			return false, e
		}
		older, e := fixtureHas(path, "temp", "11.7")
		return older && !newer, e
	})
	if e != nil || !ready {
		t.Fatal("overwrite reached TSM before WAL-only evidence was captured; increase fixture snapshot cold duration", e)
	}
	// A second snapshot generation ensures the full compaction has work to do.
	writeFiller(30000, 60000)
	fullExpected := expected("snapshot", day, "11.9", true, 60000)
	t.Log("waiting for the overwritten value and second batch to reach TSM")
	report["stage"] = "waiting-for-second-snapshot"
	if e = fixtureWait(ctx, "overwritten value and second batch in TSM", func(waitContext context.Context) (bool, error) {
		return tsmView(waitContext, func(path string) (bool, error) {
			e := duck.Equal(waitContext, typed(path), typed(fullExpected), "")
			return e == nil, e
		})
	}); e != nil {
		t.Fatal(e)
	}
	reconcile("after-second-snapshot", day, "11.9", true, 60000)
	metrics := func(metricContext context.Context) fixtureCompactions {
		request, e := http.NewRequestWithContext(metricContext, "GET", strings.TrimRight(base, "/")+"/metrics", nil)
		if e != nil {
			t.Fatal(e)
		}
		request.Header.Set("Authorization", "Token "+token)
		response, e := client.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatal("fixture /metrics", response.StatusCode)
		}
		data, e := io.ReadAll(io.LimitReader(response.Body, 16<<20))
		if e != nil {
			t.Fatal(e)
		}
		result, e := fixtureFullMetrics(string(data), bucket.ID, shard)
		if e != nil {
			t.Fatal(e)
		}
		return result
	}
	deletion, _ := json.Marshal(map[string]string{"start": "2025-03-01T00:00:00Z", "stop": "2025-03-01T23:59:59.999999999Z", "predicate": `_measurement="victim"`})
	call("POST", "/api/v2/delete?org="+url.QueryEscape(org)+"&bucket="+url.QueryEscape(name), "application/json", deletion, nil)
	tombstones := inventory()
	if len(tombstones.Tombstones) == 0 {
		t.Fatal("fixture delete did not leave observable TSM tombstones")
	}
	report["tombstone-observed"] = tombstones
	// Capture the deleted view before background compaction removes its files.
	// Its WAL-free export must honor the actual tombstone, even if the running
	// engine finishes compaction during the subsequent live reconciliation.
	report["stage"] = "capturing-tombstone"
	tombstoneView := filepath.Join(directory, "tombstone-engine")
	captured, e := fixtureTSMView(root, bucket.ID, tombstoneView)
	if e != nil || len(captured.Tombstones) == 0 {
		t.Fatal("could not capture the tombstoned TSM view before compaction", e)
	}
	report["tombstone-captured"] = captured
	capturedSource := source
	capturedSource.Engine = tombstoneView
	capturedRaw := filepath.Join(directory, "tombstone-captured-raw.parquet")
	capturedLogical := filepath.Join(directory, "tombstone-captured-logical.parquet")
	if e = capturedSource.Export(ctx, day, capturedRaw); e != nil {
		t.Fatal(e)
	}
	if _, e = duck.DeduplicateExport(ctx, capturedRaw, capturedLogical); e != nil {
		t.Fatal(e)
	}
	if e = duck.Equal(ctx, typed(capturedLogical), typed(expected("tombstone-captured", day, "11.9", false, 60000)), ""); e != nil {
		t.Fatal("captured TSM tombstone was not respected", e)
	}
	reconcile("with-tombstone", day, "11.9", false, 60000)
	// Tombstones allow level-1 compaction below its usual generation threshold.
	// Require physical cleanup without mislabelling that operation as full.
	report["stage"] = "waiting-for-tombstone-compaction"
	t.Log("waiting for tombstone cleanup (any compaction level)")
	if e = fixtureWait(ctx, "replacement of tombstoned TSM", func(waitContext context.Context) (bool, error) {
		files, replaced, e := fixtureReplacement(root, bucket.ID, tombstones)
		report["tombstone-compaction"] = map[string]any{"files": files, "replaced": replaced}
		return replaced, e
	}); e != nil {
		t.Fatal(e)
	}
	raw, _ = reconcile("after-tombstone-compaction", day, "11.9", false, 60000)
	if found, e := fixtureHas(raw, "victim", "23"); e != nil || found {
		t.Fatal("tombstone cleanup resurrected victim", e)
	}

	// Create another generation after tombstones are gone, leaving work for
	// full compaction after the cold window. Preserve the expected dataset.
	// Sample the full baseline before the write, not after the cold wait.
	baseline := metrics(ctx)
	beforeFull := inventory()
	write("filler,node=n1 value=0i 1740787200000010000\n")
	report["stage"] = "waiting-for-full-compaction-inputs"
	t.Log("waiting for a fresh snapshot generation without tombstones")
	fullExpected = expected("full-compaction-inputs", day, "11.9", false, 60000)
	var fullInputs fixtureFiles
	if e = fixtureWait(ctx, "new tombstone-free generation", func(waitContext context.Context) (bool, error) {
		files := inventory()
		if len(files.TSM) <= len(beforeFull.TSM) || len(files.Tombstones) != 0 {
			return false, nil
		}
		ready, e := tsmView(waitContext, func(path string) (bool, error) {
			e := duck.Equal(waitContext, typed(path), typed(fullExpected), "")
			return e == nil, e
		})
		if ready && e == nil {
			fullInputs = files
		}
		return ready, e
	}); e != nil {
		t.Fatal(e)
	}
	report["full-compaction-inputs"] = fullInputs
	t.Logf("waiting for full compaction of bucket=%s shard=%s; baseline=%+v", bucket.ID, shard, baseline)
	report["stage"] = "waiting-for-full-compaction"
	if e = fixtureWait(ctx, "completed successful full compaction of fresh generations", func(waitContext context.Context) (bool, error) {
		current := metrics(waitContext)
		report["full-compaction"] = map[string]any{"before": baseline, "latest": current, "files": inventory(), "shard": shard}
		if current.Failed > baseline.Failed {
			return false, fmt.Errorf("full compaction failed: %+v", current)
		}
		if current.Completed <= baseline.Completed || current.Active != 0 {
			return false, nil
		}
		// Completion alone includes failed/aborted attempts. Require replacement
		// of this phase's inputs as well as a clean failure count.
		_, replaced, e := fixtureReplacement(root, bucket.ID, fullInputs)
		return replaced, e
	}); e != nil {
		t.Fatal(e)
	}
	report["full-compaction"] = map[string]any{"before": baseline, "after": metrics(ctx), "files": inventory(), "shard": shard}
	raw, _ = reconcile("after-full-compaction", day, "11.9", false, 60000)
	if found, e := fixtureHas(raw, "victim", "23"); e != nil || found {
		t.Fatal("compacted raw export resurrected victim", e)
	}
	for _, adjacent := range []struct{ label, day string }{{"previous", "2025-02-28"}, {"next", "2025-03-02"}} {
		reconcile("before-delete-"+adjacent.label, adjacent.day, "", false, 0)
	}
	// Only this test-created bucket is modified. The production deletion flag is
	// neither enabled nor consulted by this explicit isolated-source test.
	if e := source.Delete(ctx, day); e != nil {
		t.Fatal(e)
	}
	reconcile("after-day-delete", day, "", false, -1)
	for _, adjacent := range []struct{ label, day string }{{"previous", "2025-02-28"}, {"next", "2025-03-02"}} {
		reconcile("after-delete-"+adjacent.label, adjacent.day, "", false, 0)
	}
	report["stage"] = "complete"
}
