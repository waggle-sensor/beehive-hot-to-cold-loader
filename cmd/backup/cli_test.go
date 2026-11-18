package backup

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func withTransport(t *testing.T, handler http.Handler) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response.Result(), nil
	})
}

func TestBackupRequestCLIUsesAdminAndWaitsForVerifiedCompletion(t *testing.T) {
	t.Setenv("HOT_TO_COLD_ADMIN_TOKEN", "test-admin")
	calls := []string{}
	withTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer test-admin" {
			t.Error("wrong method or credentials")
		}
		calls = append(calls, r.URL.Path)
		if r.URL.Path == "/v1/backups/create" {
			w.WriteHeader(202)
			json.NewEncoder(w).Encode(map[string]string{"id": "fixture", "state": "running"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"id": "fixture", "phase": "complete"})
	}))
	if err := Run([]string{"request", "--endpoint", "https://coordinator.test"}); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 {
		t.Fatal(calls)
	}
}

func TestBackupRequestCLIDoesNotFollowCredentialRedirects(t *testing.T) {
	t.Setenv("HOT_TO_COLD_ADMIN_TOKEN", "test-admin")
	withTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://elsewhere.invalid", 302)
	}))
	if err := Run([]string{"request", "--endpoint", "https://coordinator.test"}); err == nil {
		t.Fatal("redirect accepted")
	}
}
