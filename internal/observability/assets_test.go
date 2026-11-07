// assets_test.go checks that the dashboard and alert rules name only metrics
// this process can publish, plus the read-renewal series scraped from the API.
// Every panel query must also stay scoped to its selected job and instance.

package observability_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
	"beehive-hot-to-cold-loader/internal/observability"
	"github.com/prometheus/common/expfmt"
	"go.yaml.in/yaml/v3"
)

func TestDashboardAndRuleMetricsExist(t *testing.T) {
	store, err := control.Open(filepath.Join(t.TempDir(), "journal.db"), "waggle", "warehouse", "2025-03-01")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	metrics := observability.New(720*time.Hour, true, false)
	handler := metrics.Handler(func(ctx context.Context) (observability.Snapshot, error) {
		return store.MetricsSnapshot(ctx, control.SnapshotOptions{Retention: 720 * time.Hour, DeletionEnabled: true})
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(strings.NewReader(response.Body.String()))
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	// These external families belong to the separately scraped API, not this registry.
	for _, name := range []string{"hot_to_cold_read_renewals_total", "hot_to_cold_read_renewal_duration_seconds_sum", "hot_to_cold_read_renewal_duration_seconds_count"} {
		known[name] = true
	}
	for name, family := range families {
		known[name] = true
		if family.GetType().String() == "HISTOGRAM" {
			known[name+"_bucket"] = true
			known[name+"_sum"] = true
			known[name+"_count"] = true
		}
	}
	metricNames := regexp.MustCompile(`hot_to_cold_[a-z_]+`)
	base := filepath.Join("..", "..", "observability")
	read := func(name string) []byte {
		contents, err := os.ReadFile(filepath.Join(base, name))
		if err != nil {
			t.Fatal(err)
		}
		return contents
	}
	check := func(expr string) {
		for _, name := range metricNames.FindAllString(expr, -1) {
			if !known[name] {
				t.Error("artifact refers to missing metric", name)
			}
		}
	}
	var dashboard struct {
		UID    string `json:"uid"`
		Schema int    `json:"schemaVersion"`
		Panels []struct {
			ID      int    `json:"id"`
			Type    string `json:"type"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err = json.Unmarshal(read("grafana/beehive-hot-to-cold-loader.json"), &dashboard); err != nil {
		t.Fatal(err)
	}
	if dashboard.UID != "beehive-hot-to-cold-loader" || dashboard.Schema != 38 {
		t.Fatal("unstable dashboard identity/schema")
	}
	ids := map[int]bool{}
	for _, panel := range dashboard.Panels {
		if ids[panel.ID] {
			t.Fatal("duplicate panel ID")
		}
		ids[panel.ID] = true
		if panel.Type == "row" {
			continue
		}
		if len(panel.Targets) == 0 {
			t.Fatal("panel missing query")
		}
		for _, target := range panel.Targets {
			check(target.Expr)
			job, instance := `job=~"$job"`, `instance=~"$instance"`
			if strings.Contains(target.Expr, "$api_job") {
				job, instance = `job=~"$api_job"`, `instance=~"$api_instance"`
			}
			if !strings.Contains(target.Expr, job) || !strings.Contains(target.Expr, instance) {
				t.Error("query not scoped to selected targets", target.Expr)
			}
		}
	}
	var rules struct {
		Groups []struct {
			Rules []struct{ Alert, Expr, For string } `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err = yaml.Unmarshal(read("prometheus/alerts.yaml"), &rules); err != nil {
		t.Fatal(err)
	}
	for _, group := range rules.Groups {
		for _, rule := range group.Rules {
			if rule.Alert == "" || rule.Expr == "" {
				t.Fatal("empty alert")
			}
			check(rule.Expr)
			if _, err = time.ParseDuration(rule.For); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range []string{"prometheus/scrape.yaml", "prometheus/api-scrape.yaml", "grafana/provisioning/dashboards.yaml", "prometheus/alerts.test.yaml"} {
		var doc map[string]any
		if err = yaml.Unmarshal(read(name), &doc); err != nil {
			t.Fatal(name, err)
		}
	}
}
