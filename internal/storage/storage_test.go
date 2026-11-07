// storage_test.go covers CSV decoding, empty confirmation by row count, the icebergv3
// catalog, Pelican path scope, read-only bootstrap IO, and scoped DuckDB settings.

package storage

import (
	"beehive-hot-to-cold-loader/internal/control"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestCSVPreservesTypesTagsAndNanoseconds tests that a CSV preserves types, tags, and nanoseconds.
func TestCSVPreservesTypesTagsAndNanoseconds(t *testing.T) {
	csv := `#datatype,string,long,dateTime:RFC3339,double,string,string,string
,result,table,_time,_value,_field,_measurement,node
,,0,2025-03-01T00:00:00.000000194Z,11.7,value,temp,n1
#datatype,string,long,dateTime:RFC3339,long,string,string,string
,result,table,_time,_value,_field,_measurement,node
,,1,2025-03-01T00:00:00.000000195Z,9007199254740993,value,count,n1
`
	var b bytes.Buffer
	if e := DecodeCSV(strings.NewReader(csv), json.NewEncoder(&b), 1740787200000000000, 1740873600000000000); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(b.String(), `"time":1740787200000000194`) || !strings.Contains(b.String(), `"value":"9007199254740993"`) {
		t.Fatal(b.String())
	}
	if !strings.Contains(b.String(), `"node":"n1"`) {
		t.Fatal(b.String())
	}
}

// TestCSVRejectsUnknownFieldAndErrorTables tests that a CSV rejects unknown fields and error tables.
func TestCSVRejectsUnknownFieldAndErrorTables(t *testing.T) {
	for _, csv := range []string{
		`,error,reference
,query failed,1
`,
		`#datatype,string,long,dateTime:RFC3339,double,string,string
,result,table,_time,_value,_field,_measurement
,,0,2025-03-01T00:00:00Z,2,other,temp
`,
	} {
		if e := DecodeCSV(strings.NewReader(csv), json.NewEncoder(io.Discard), 0, 1<<63-1); e == nil {
			t.Fatal("accepted invalid query", csv)
		}
	}
}

// TestConfirmEmptyUsesParquetRowsNotFooterSize tests that a confirm empty uses Parquet rows not footer size.
func TestConfirmEmptyUsesParquetRowsNotFooterSize(t *testing.T) {
	for _, nonempty := range []bool{false, true} {
		t.Run(fmt.Sprint(nonempty), func(t *testing.T) {
			calls := 0
			i := Influx{URL: "https://influx.test", Org: "waggle", Bucket: "fixture", Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body := ""
				if nonempty && calls == 0 {
					body = "#datatype,string,long,dateTime:RFC3339,double,string,string\n,result,table,_time,_value,_field,_measurement\n,,0,2025-03-01T00:00:00.000000194Z,11.7,value,temp\n"
				}
				calls++
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			work := t.TempDir()
			w := Worker{Influx: i}
			e := w.ConfirmEmpty(context.Background(), &control.Job{Day: "2025-03-01", Work: work})
			if (e != nil) != nonempty {
				t.Fatalf("nonempty=%v: %v", nonempty, e)
			}
			info, err := os.Stat(filepath.Join(work, "after-delete.parquet"))
			if err != nil || info.Size() == 0 {
				t.Fatal("expected Parquet footer even for zero rows", err)
			}
		})
	}
}

// mockPelican creates a mock Pelican.
func mockPelican(t *testing.T) (*Pelican, string) { return pelicanFixture(t, false) }

// pelicanFixture creates a Pelican fixture.
func pelicanFixture(t *testing.T, live bool) (*Pelican, string) {
	t.Helper()
	root := t.TempDir()
	objects := map[string][]byte{}
	var mu sync.Mutex
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			w.WriteHeader(401)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case "PUT":
			b, e := io.ReadAll(r.Body)
			if e != nil {
				t.Error(e)
			}
			objects[r.URL.Path] = b
			w.WriteHeader(201)
		case "GET", "HEAD":
			b, ok := objects[r.URL.Path]
			if !ok {
				w.WriteHeader(404)
				return
			}
			if r.Method == "GET" {
				w.Write(b)
			}
		case "PROPFIND":
			for p := range objects {
				if strings.HasPrefix(p, r.URL.Path) {
					w.WriteHeader(207)
					return
				}
			}
			w.WriteHeader(404)
		default:
			w.WriteHeader(405)
		}
	})
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	der, e := x509.MarshalECPrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	keyPath := filepath.Join(root, "key.pem")
	os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0600)
	base := "https://pelican.test"
	var server *httptest.Server
	if live {
		server = httptest.NewTLSServer(handler)
		t.Cleanup(server.Close)
		base = server.URL
	}
	p, e := NewPelican(base+"/sage/cold-storage-archive-test/warehouse", keyPath, root)
	if e != nil {
		t.Fatal(e)
	}
	p.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Result(), nil
	})}
	if live {
		p.Client = server.Client()
	}
	return p, root
}

func TestCatalogV3SchemaAndRemoteMetadata(t *testing.T) {
	p, root := mockPelican(t)
	ctx := context.Background()
	c, e := OpenCatalog(ctx, filepath.Join(root, "catalog.db"), p.Base.String(), p)
	if e != nil {
		t.Fatal(e)
	}
	defer c.DB.Close()
	if e = c.Init(ctx); e != nil {
		t.Fatal(e)
	}
	tbl, e := c.Catalog.LoadTable(ctx, ident)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := p.ReadFile(tbl.MetadataLocation())
	if e != nil {
		t.Fatal(e)
	}
	var metadata map[string]any
	if e = json.Unmarshal(raw, &metadata); e != nil {
		t.Fatal(e)
	}
	if metadata["format-version"] != float64(3) {
		t.Fatal(metadata)
	}
	value, ok := tbl.Schema().FindFieldByName("value")
	if !ok || value.ID != 11 || value.Type.String() != "variant" {
		t.Fatal(value)
	}
	if e = c.Init(ctx); e != nil {
		t.Fatal("restart schema validation", e)
	}
	if e = c.PublishHint(ctx, tbl.MetadataLocation()); e != nil {
		t.Fatal(e)
	}
}

func TestPelicanScopeAndStreamingDigest(t *testing.T) {
	p, root := mockPelican(t)
	for _, name := range []string{"https://other.test/sage/cold-storage-archive-test/warehouse/sage/obs/x", p.Base.String() + "/sage/obs/../../node-data/x", p.Base.String() + "/sage/obs/%2e%2e/x"} {
		if _, e := p.validate(name); e == nil {
			t.Fatal(name)
		}
	}
	local := filepath.Join(root, "file")
	os.WriteFile(local, bytes.Repeat([]byte("test"), 1024), 0600)
	remote := p.Base.String() + "/sage/obs/data/test.parquet"
	if e := p.Upload(context.Background(), local, remote); e != nil {
		t.Fatal(e)
	}
	f, e := p.Open(remote)
	if e != nil {
		t.Fatal(e)
	}
	b, e := io.ReadAll(f)
	f.Close()
	if e != nil || len(b) != 4096 {
		t.Fatal(len(b), e)
	}
	os.WriteFile(local, []byte("different"), 0600)
	if e = p.VerifyObject(context.Background(), local, remote); e == nil {
		t.Fatal("digest mismatch accepted")
	}
	if e = p.Remove(remote); e == nil {
		t.Fatal("automatic delete enabled")
	}
}

// roundTripFunc is a round trip function.
type roundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip rounds a request.
func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestBootstrapIOCannotWrite tests that a bootstrap IO cannot write.
func TestBootstrapIOCannotWrite(t *testing.T) {
	p, root := mockPelican(t)
	reader, e := NewPelicanReader(p.Base.String(), filepath.Join(root, "key.pem"), root)
	if e != nil {
		t.Fatal(e)
	}
	reader.Client = p.Client
	if e = reader.WriteFile(p.Base.String()+"/sage/obs/source", []byte("no")); e == nil {
		t.Fatal("bootstrap source writable")
	}
}

// TestSourcePreflightRejectsAutomaticRetention tests that a source preflight rejects automatic retention.
func TestSourcePreflightRejectsAutomaticRetention(t *testing.T) {
	source := Influx{URL: "https://influx.test", Bucket: "waggle", Org: "waggle", BucketID: "bucket", Token: "test"}
	source.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"bucket","name":"waggle","orgID":"org","retentionRules":[{"everySeconds":3600}]}`)), Header: make(http.Header)}, nil
	})}
	if e := source.ValidateSource(context.Background()); e == nil {
		t.Fatal("independent retention accepted")
	}
}

// TestUncertainEmptyCommitIsIdempotentAndLostCatalogRefusesAdoption tests that a uncertain empty commit is idempotent and lost catalog refuses adoption.
func TestUncertainEmptyCommitIsIdempotentAndLostCatalogRefusesAdoption(t *testing.T) {
	p, root := mockPelican(t)
	ctx := context.Background()
	c, e := OpenCatalog(ctx, filepath.Join(root, "catalog.db"), p.Base.String(), p)
	if e != nil {
		t.Fatal(e)
	}
	defer c.DB.Close()
	if e = c.Init(ctx); e != nil {
		t.Fatal(e)
	}
	job := &control.Job{ID: "empty-day-job", Day: "2025-03-01", Mode: "archive"}
	head, e := c.Commit(ctx, job)
	if e != nil {
		t.Fatal(e)
	}
	again, e := c.Commit(ctx, job)
	if e != nil || head != again {
		t.Fatal(head, again, e)
	}
	lost, e := OpenCatalog(ctx, filepath.Join(root, "lost.db"), p.Base.String(), p)
	if e != nil {
		t.Fatal(e)
	}
	defer lost.DB.Close()
	if e = lost.Init(ctx); e == nil {
		t.Fatal("occupied remote root was silently adopted")
	}
}

// TestBuildDuckSettingsAreScoped tests that a build Duck settings are scoped.
func TestBuildDuckSettingsAreScoped(t *testing.T) {
	base := DuckDB{Binary: "native-duckdb", Work: "private-work", Memory: "3GB", Threads: 2, testSetup: "fixture", testHTTP: "test-tls"}
	worker := Worker{Duck: base}
	build, e := worker.buildDuck()
	if e != nil {
		t.Fatal(e)
	}
	if build.Threads != 16 || build.SortThreads != 2 {
		t.Fatal("measured build policy missing", build)
	}
	if worker.Duck != base || build.Binary != base.Binary || build.Work != base.Work || build.Memory != base.Memory || build.testSetup != base.testSetup || build.testHTTP != base.testHTTP {
		t.Fatal("base session changed or runtime settings lost")
	}
	worker.BuildThreads = 8
	worker.BuildSortThreads = 1
	build, e = worker.buildDuck()
	if e != nil || build.Threads != 8 || build.SortThreads != 1 || worker.Duck.Threads != 2 {
		t.Fatal(build, e)
	}
	for _, counts := range [][2]int{{-1, 2}, {257, 2}, {16, -1}, {16, 257}} {
		worker.BuildThreads, worker.BuildSortThreads = counts[0], counts[1]
		if _, e := worker.buildDuck(); e == nil {
			t.Fatal("invalid build counts accepted", counts)
		}
	}
}

// TestComparisonDuckSettingsAreScoped tests that a comparison Duck settings are scoped.
func TestComparisonDuckSettingsAreScoped(t *testing.T) {
	for _, threads := range []int{0, 2, 8, 32} {
		base := DuckDB{Binary: "native", Work: "private", Memory: "3GB", Threads: threads, testSetup: "fixture", testHTTP: "tls"}
		w := Worker{Duck: base}
		proof := w.comparisonDuck()
		want := base
		want.Threads = 16
		if proof != want || w.Duck != base {
			t.Fatal("fixed proof policy changed base/runtime state", proof)
		}
		build, err := w.buildDuck()
		if err != nil || build.Threads != 16 || build.SortThreads != 2 {
			t.Fatal("proof policy changed build", build, err)
		}
	}
}
