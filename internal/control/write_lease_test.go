// write_lease_test.go checks renewal, expiry, the one-time migration window,
// and the writer HTTP routes.

package control

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestWriterLeaseRenewalExpiryAndFence(t *testing.T) {
	s := testStore(t)
	clock := time.Now().UTC()
	s.now = func() time.Time { return clock }
	grant, err := s.AcquireWrite("batch", "writer", []string{"2025-03-01"})
	if err != nil {
		t.Fatal(err)
	}
	expiry, _ := time.Parse(time.RFC3339Nano, grant.ExpiresAt)
	if !expiry.Equal(clock.Add(DefaultWriteLeaseDuration)) {
		t.Fatal(grant)
	}
	clock = clock.Add(time.Minute)
	retry, err := s.AcquireWrite("batch", "writer", []string{"2025-03-01"})
	if err != nil || retry.ExpiresAt != grant.ExpiresAt {
		t.Fatal("retry renewed lease", retry, err)
	}
	if _, err = s.RenewWrite("other", "batch"); err == nil {
		t.Fatal("wrong owner renewed")
	}
	if err = s.ReleaseWrite("other", "batch"); err == nil {
		t.Fatal("wrong owner released")
	}
	job, err := s.Next(now(), 720*time.Hour)
	if err != nil || job == nil {
		t.Fatal(job, err)
	}
	renewed, err := s.RenewWrite("writer", "batch")
	if err != nil || len(renewed.HotDays) != 1 || renewed.HotDays[0] != job.Day {
		t.Fatal("renewal changed admission after fence", renewed, err)
	}
	expiry, _ = time.Parse(time.RFC3339Nano, renewed.ExpiresAt)
	if !expiry.Equal(clock.Add(DefaultWriteLeaseDuration)) {
		t.Fatal(renewed)
	}
	// Shorter runtime settings cannot shorten an already confirmed grant.
	s.writeLeaseDuration = time.Second
	shorter, err := s.RenewWrite("writer", "batch")
	if err != nil || shorter.ExpiresAt != renewed.ExpiresAt {
		t.Fatal(shorter, err)
	}
	clock = expiry
	if _, err = s.RenewWrite("writer", "batch"); err == nil {
		t.Fatal("expired lease renewed")
	}
	if _, err = s.AcquireWrite("batch", "writer", []string{job.Day}); err == nil {
		t.Fatal("expired lease reacquired")
	}
	if err = s.ReleaseWrite("writer", "batch"); err == nil {
		t.Fatal("expired batch confirmed")
	}
	if n, err := s.Writers(job.Day); err != nil || n != 1 {
		t.Fatal("drain grace skipped", n, err)
	}
	clock = expiry.Add(WriteExpiryGrace)
	if n, err := s.Writers(job.Day); err != nil || n != 0 {
		t.Fatal("expired writer blocks fence", n, err)
	}
	status, err := s.Status()
	if err != nil || status["writers"] != int64(0) || status["writers_expired"] != int64(1) {
		t.Fatal(status, err)
	}
	if v := metricValue(t, s, "writers"); v != 0 {
		t.Fatal(v)
	}
	if v := metricValue(t, s, "writers_expired"); v != 1 {
		t.Fatal(v)
	}
	if v := metricValue(t, s, "jobs_waiting", "mode", "archive", "reason", "writers"); v != 0 {
		t.Fatal("expired permit still counted as blocking", v)
	}
	next, err := s.AcquireWrite("replayed", "new-writer", []string{job.Day})
	if err != nil || len(next.HotDays) != 0 {
		t.Fatal("replay bypassed fence", next, err)
	}
	if err = s.ReleaseWrite("new-writer", "replayed"); err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseWrite("new-writer", "replayed"); err != nil {
		t.Fatal("release retry failed", err)
	}
	if _, err = s.RenewWrite("new-writer", "replayed"); err == nil {
		t.Fatal("completed lease renewed")
	}
}

func TestWriterLeaseMigrationRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	open := func(ttl time.Duration) *Store {
		s, err := Open(path, "waggle", "warehouse", "2025-03-01", time.Minute, ttl)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open(time.Minute)
	if _, err := s.Permit("legacy", "writer", []string{"2025-03-01"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("DROP INDEX permits_by_expiry"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("ALTER TABLE permits DROP COLUMN expires_at"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(2 * time.Minute)
	grant, err := s.AcquireWrite("legacy", "writer", []string{"2025-03-01"})
	if err != nil || grant.ExpiresAt == "" {
		t.Fatal(grant, err)
	}
	s.Close()
	s = open(time.Hour)
	defer s.Close()
	retry, err := s.AcquireWrite("legacy", "writer", []string{"2025-03-01"})
	if err != nil || retry.ExpiresAt != grant.ExpiresAt {
		t.Fatal("restart extended lease", retry, err)
	}
	for _, ttl := range []time.Duration{0, -time.Second, 25 * time.Hour} {
		if store, err := Open(filepath.Join(t.TempDir(), "bad.db"), "waggle", "warehouse", "2025-03-01", time.Minute, ttl); err == nil {
			store.Close()
			t.Fatal("invalid writer duration", ttl)
		}
	}
}

func TestWriterLeaseHTTP(t *testing.T) {
	s := testStore(t)
	server := &Server{Store: s, LoaderToken: "loader", ReaderToken: "reader"}
	call := func(path, token, owner string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{"id": "batch", "owner": owner, "bucket": "waggle", "days": []string{"2025-03-01"}})
		req := httptest.NewRequest("POST", path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, req)
		return response
	}
	response := call("/v1/writes/acquire", "loader", "writer")
	var grant WriteGrant
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &grant) != nil || grant.ExpiresAt == "" {
		t.Fatal(response.Code, response.Body.String())
	}
	if response = call("/v1/writes/renew", "reader", "writer"); response.Code != 401 {
		t.Fatal(response.Code)
	}
	if response = call("/v1/writes/renew", "loader", "other"); response.Code != 409 {
		t.Fatal(response.Code)
	}
	if response = call("/v1/writes/renew", "loader", "writer"); response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	if response = call("/v1/writes/release", "loader", "writer"); response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	if response = call("/v1/writes/renew", "loader", "writer"); response.Code != 409 {
		t.Fatal(response.Code)
	}
}
