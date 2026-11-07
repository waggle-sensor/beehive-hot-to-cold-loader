// http.go is the coordination API. Loader, reader, and admin tokens select
// different routes; a token for one role cannot call another role's route.

package control

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// Server admits writers and readers and reports aggregate status.
type Server struct {
	MetricsHandler                       http.Handler
	Store                                *Store        // the store
	LoaderToken, ReaderToken, AdminToken string        // the loader, reader, and admin tokens
	Retention                            time.Duration // the retention period
}

// ServeHTTP serves public metrics and authenticates coordination/status commands.
// Metrics are the only GET. Bodies are limited to 16 MiB and one JSON value.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" {
		if r.Method != http.MethodGet {
			http.Error(w, "GET required", 405)
			return
		}
		if s.MetricsHandler == nil {
			http.Error(w, "metrics unavailable", 503)
			return
		}
		s.MetricsHandler.ServeHTTP(w, r)
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	expected := s.LoaderToken
	if strings.HasPrefix(r.URL.Path, "/v1/reads/") {
		expected = s.ReaderToken
	}
	if r.URL.Path == "/v1/status" {
		expected = s.AdminToken
	}

	if expected == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	var request struct {
		ID      string   `json:"id"`
		Owner   string   `json:"owner"`
		Bucket  string   `json:"bucket"`
		Days    []string `json:"days"`
		Records []Record `json:"records"`
		Payload string   `json:"payload"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&request)
	if err == nil {
		var rest any
		if decoder.Decode(&rest) != io.EOF {
			err = errors.New("one JSON body required")
		}
	}
	if err != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	if request.Bucket != s.Store.Bucket {
		http.Error(w, "bucket mismatch", 400)
		return
	}
	var response any = map[string]bool{"ok": true}
	switch r.URL.Path {
	case "/v1/writes/acquire":
		response, err = s.Store.AcquireWrite(request.ID, request.Owner, request.Days)
	case "/v1/writes/renew":
		response, err = s.Store.RenewWrite(request.Owner, request.ID)
	case "/v1/writes/release":
		err = s.Store.ReleaseWrite(request.Owner, request.ID)
	case "/v1/late":
		err = s.Store.Enqueue(request.ID, request.Records)
	case "/v1/quarantine":
		err = s.Store.Quarantine(request.ID, request.Payload)
	case "/v1/reads/acquire":
		response, err = s.Store.View(request.Owner, request.ID)
	case "/v1/reads/renew":
		response, err = s.Store.RenewRead(request.Owner, request.ID)
	case "/v1/reads/release":
		err = s.Store.ReleaseRead(request.ID)
	case "/v1/status":
		response, err = s.Store.Status()
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
