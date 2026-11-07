// influx.go reads and deletes one bucket. Engine export and the Flux query are
// separate copies of the same day; reconciliation proves they agree.

package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	lp "github.com/influxdata/line-protocol"
)

// Influx is one source bucket. Engine is a read-only mount used by inspect export-lp.
type Influx struct {
	URL, Org, Bucket, Token, Engine, BucketID, Binary string       // the URL, organization, bucket, token, engine, bucket ID, and binary of the InfluxDB instance
	Client                                            *http.Client // the HTTP client
	Lock                                              *os.File     // the lock file
}

// Value renders a scalar as the canonical text and one of the five supported kinds.
func Value(v any) (string, string, error) {
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", "", errors.New("nonfinite value")
		}
		return "float", strconv.FormatFloat(x, 'g', -1, 64), nil
	case int64:
		return "int", strconv.FormatInt(x, 10), nil
	case uint64:
		return "uint", strconv.FormatUint(x, 10), nil
	case string:
		return "string", x, nil
	case bool:
		return "bool", strconv.FormatBool(x), nil
	}
	return "", "", fmt.Errorf("unsupported observation value %T", v)
}

// Export streams the engine's line protocol for the half-open UTC day into Parquet.
func (i Influx) Export(ctx context.Context, day, path string) error {
	if _, e := stagingSpace(path); e != nil {
		return e
	}
	start, stop, e := control.Bounds(day)
	if e != nil {
		return e
	}
	cmd := exec.CommandContext(ctx, i.Binary, "inspect", "export-lp", "--bucket-id", i.BucketID, "--engine-path", i.Engine, "--start", start.Format(time.RFC3339Nano), "--end", stop.Format(time.RFC3339Nano), "--output-path", "-")
	if i.Lock != nil {
		cmd.ExtraFiles = []*os.File{i.Lock}
	}
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	cmd.Stderr = os.Stderr
	f, e := os.Create(path + ".pending")
	if e != nil {
		return e
	}
	defer f.Close()
	if e = cmd.Start(); e != nil {
		return e
	}
	success := false
	defer func() {
		if !success {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	enc, finish, release, e := newRecordEncoder(f, path)
	if e != nil {
		return e
	}
	defer release()
	if e = streamExport(stdout, enc, start.UnixNano(), stop.UnixNano()); e != nil {
		return e
	}

	if e = cmd.Wait(); e != nil {
		return e
	}
	success = true
	if e = finish(); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(path+".pending", path)
}

// exportRecord preserves physical replay order without retaining a day's points.
type exportRecord struct {
	control.Record
	Seq int64 `json:"seq"`
}

// streamExport parses line protocol and rejects points outside [low, high).
func streamExport(reader io.Reader, enc recordEncoder, low, high int64) error {
	parser := lp.NewStreamParser(bufio.NewReaderSize(reader, 1<<20))
	var sequence int64
	for {
		point, e := parser.Next()
		if errors.Is(e, io.EOF) || errors.Is(e, lp.EOF) {
			return nil
		}
		if e != nil {
			return fmt.Errorf("line protocol parse: %w", e)
		}
		stamp := point.Time().UnixNano()
		if stamp < low || stamp > high {
			return errors.New("exporter returned an out-of-window observation")
		}
		// Keep the VM-proven inclusive whole-second export boundary handling.
		if stamp == high {
			continue
		}
		fields := point.FieldList()
		if len(fields) != 1 || fields[0].Key != "value" {
			return errors.New("bucket violates the single value-field contract")
		}
		r := control.Record{Time: stamp, Name: point.Name(), Tags: map[string]string{}}
		for _, tag := range point.TagList() {
			r.Tags[tag.Key] = tag.Value
		}
		r.Kind, r.Value, e = Value(fields[0].Value)
		if e != nil {
			return e
		}
		if e = r.Validate(); e != nil {
			return e
		}
		if sequence == math.MaxInt64 {
			return errors.New("export sequence overflow")
		}
		sequence++
		if e = enc.Encode(exportRecord{Record: r, Seq: sequence}); e != nil {
			return e
		}
	}
}

// request calls the Influx API with the source token. The token is not logged.
func (i Influx) request(ctx context.Context, endpoint string, body any, accept string) (*http.Response, error) {
	raw, e := json.Marshal(body)
	if e != nil {
		return nil, e
	}
	u := strings.TrimRight(i.URL, "/") + endpoint
	req, e := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(string(raw)))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Authorization", "Token "+i.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)
	c := i.Client
	if c == nil {
		c = &http.Client{Timeout: time.Hour}
	}
	res, e := c.Do(req)
	if e != nil {
		return nil, e
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		res.Body.Close()
		return nil, fmt.Errorf("Influx HTTP %d", res.StatusCode)
	}
	return res, nil
}

// Query streams annotated CSV rather than materializing all series in memory.
// Every table and value type is retained, including measurements without plugin.
func (i Influx) Query(ctx context.Context, day, path string) error {
	if _, e := stagingSpace(path); e != nil {
		return e
	}
	dayStart, dayEnd, e := control.Bounds(day)
	if e != nil {
		return e
	}
	f, e := os.Create(path + ".pending")
	if e != nil {
		return e
	}
	defer f.Close()
	enc, finish, release, e := newRecordEncoder(f, path)
	if e != nil {
		return e
	}
	defer release()
	for start := dayStart; start.Before(dayEnd); start = start.Add(time.Hour) {
		end := start.Add(time.Hour)
		flux := fmt.Sprintf("from(bucket: %s) |> range(start: time(v: %s), stop: time(v: %s))", fluxString(i.Bucket), fluxString(start.Format(time.RFC3339Nano)), fluxString(end.Format(time.RFC3339Nano)))
		res, e := i.request(ctx, "/api/v2/query?org="+url.QueryEscape(i.Org), map[string]any{"query": flux, "dialect": map[string]any{"annotations": []string{"datatype", "group", "default"}, "dateTimeFormat": "RFC3339Nano"}}, "application/csv")
		if e != nil {
			return e
		}
		e = DecodeCSV(res.Body, enc, start.UnixNano(), end.UnixNano())
		res.Body.Close()
		if e != nil {
			return e
		}
	}
	if e = finish(); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(path+".pending", path)
}

// fluxString quotes s as a Flux string literal.
func fluxString(s string) string { b, _ := json.Marshal(s); return string(b) }

// Delete removes the half-open UTC day through the Influx delete API.
func (i Influx) Delete(ctx context.Context, day string) error {
	start, nextDay, e := control.Bounds(day)
	if e != nil {
		return e
	}
	// InfluxDB 2.7 delete keeps the stop instant. Send the previous nanosecond
	// so a day delete stays [start, next midnight) and retains that midnight.
	stop := nextDay.Add(-time.Nanosecond)
	res, e := i.request(ctx, "/api/v2/delete?org="+url.QueryEscape(i.Org)+"&bucket="+url.QueryEscape(i.Bucket), map[string]any{"start": start.Format(time.RFC3339Nano), "stop": stop.Format(time.RFC3339Nano), "predicate": ""}, "application/json")
	if e == nil {
		res.Body.Close()
	}
	return e
}

// ValidateSource prevents a bucket-ID mixup and refuses retention that could
// remove points before archival succeeds.
func (i Influx) ValidateSource(ctx context.Context) error {
	read := func(endpoint string, out any) error {
		req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(i.URL, "/")+endpoint, nil)
		if e != nil {
			return e
		}
		req.Header.Set("Authorization", "Token "+i.Token)
		c := i.Client
		if c == nil {
			c = &http.Client{Timeout: 30 * time.Second}
		}
		res, e := c.Do(req)
		if e != nil {
			return e
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("Influx source preflight HTTP %d", res.StatusCode)
		}
		return json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(out)
	}
	var bucket struct {
		ID, Name, OrgID string
		RetentionRules  []struct{ EverySeconds int64 }
	}
	if e := read("/api/v2/buckets/"+url.PathEscape(i.BucketID), &bucket); e != nil {
		return e
	}
	if bucket.ID != i.BucketID || bucket.Name != i.Bucket {
		return errors.New("source bucket ID/name mismatch")
	}
	for _, rule := range bucket.RetentionRules {
		if rule.EverySeconds != 0 {
			return errors.New("automatic Influx retention must be disabled")
		}
	}
	var orgs struct{ Orgs []struct{ ID, Name string } }
	if e := read("/api/v2/orgs?org="+url.QueryEscape(i.Org), &orgs); e != nil {
		return e
	}
	if len(orgs.Orgs) != 1 || orgs.Orgs[0].Name != i.Org || orgs.Orgs[0].ID != bucket.OrgID {
		return errors.New("source bucket organization mismatch")
	}
	return nil
}
