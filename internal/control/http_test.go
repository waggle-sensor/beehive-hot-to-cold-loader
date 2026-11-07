// http_test.go checks that each token can call only its own routes, and that
// repeating a read acquire returns the snapshot from the first acquire.

package control

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRoleIsolationAndDurableReceipt tests that each token can call only its own routes, and that repeating a read acquire returns the snapshot from the first acquire.
func TestRoleIsolationAndDurableReceipt(t *testing.T) {
	s := testStore(t)
	server := &Server{Store: s, LoaderToken: "loader", ReaderToken: "reader", AdminToken: "admin"}
	call := func(path, token string, body any) *httptest.ResponseRecorder {
		b, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", path, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		server.ServeHTTP(res, req)
		return res
	}
	body := map[string]any{"bucket": "waggle", "id": "read", "owner": "pod"}
	if res := call("/v1/reads/acquire", "loader", body); res.Code != http.StatusUnauthorized {
		t.Fatal(res.Code)
	}
	if res := call("/v1/reads/acquire", "reader", body); res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	if res := call("/v1/reads/renew", "loader", body); res.Code != http.StatusUnauthorized {
		t.Fatal("loader renewed read lease", res.Code)
	}
	if res := call("/v1/reads/renew", "reader", body); res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	s.Next(now(), 720*60*60*1e9)
	late := map[string]any{"bucket": "waggle", "id": "event", "records": []Record{{Time: 1740787200000000194, Name: "t", Tags: map[string]string{}, Kind: "int", Value: "42"}}}
	if res := call("/v1/late", "loader", late); res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	if res := call("/v1/status", "reader", map[string]string{"bucket": "waggle"}); res.Code != 401 {
		t.Fatal(res.Code)
	}
	if res := call("/v1/status", "admin", map[string]string{"bucket": "waggle"}); res.Code != 200 {
		t.Fatal(res.Code)
	}
}

// TestReadAcquireRetryRetainsView tests that a read acquire retry retains the view.
func TestReadAcquireRetryRetainsView(t *testing.T) {
	s := testStore(t)
	a, e := s.View("api", "id")
	if e != nil {
		t.Fatal(e)
	}
	job, _ := s.Next(now(), 720*60*60*1e9)
	job.Metadata = "new"
	if e = s.Activate(job); e != nil {
		t.Fatal(e)
	}
	b, e := s.View("api", "id")
	if e != nil {
		t.Fatal(e)
	}
	if a != b {
		t.Fatal(a, b)
	}
}
