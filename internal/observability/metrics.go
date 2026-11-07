// Package observability publishes process and journal series.
// It records attempts and durations. It does not choose the next archival step.
package observability

import (
	"context"
	"errors"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Modes, States, and Reasons are the only labels this process will publish.
// Start ignores a mode or stage outside these lists. The journal snapshot
// refuses a job whose mode or state is outside them. COMPLETE is a durable
// outcome, not an in-progress stage.
var Modes = []string{"archive", "late"}
var States = []string{"FENCING", "EXPORTING", "EXPORTED", "RECONCILED", "BUILT", "COMMITTED", "REMOTE_VERIFIED", "ROUTED", "DELETING", "DELETE_REQUESTED", "COMPLETE"}
var Reasons = []string{"writers", "readers", "deletion_disabled", "retry_backoff", "coordinate_only", "unknown"}

// Sample is one journal observation. Name is the suffix after hot_to_cold_.
// Labels and Values stay in pairs. Counter publishes a counter; otherwise the
// sample is a gauge of the current journal population.
type Sample struct {
	Name    string
	Value   float64
	Labels  []string
	Values  []string
	Counter bool
}

// Gauge pairs label names and values from the alternating labels list.
func Gauge(name string, value float64, labels ...string) Sample {
	sample := Sample{Name: name, Value: value}
	for i := 0; i < len(labels); i += 2 {
		sample.Labels = append(sample.Labels, labels[i])
		sample.Values = append(sample.Values, labels[i+1])
	}
	return sample
}

// Snapshot is every journal sample for one scrape. The handler publishes it
// whole or returns an error; it does not keep a partial result for the next scrape.
type Snapshot []Sample

// snapshotCollector publishes one scrape. The same description is used for
// every sample of a metric, so the first sample supplies the label names.
type snapshotCollector struct{ samples Snapshot }

// Describe emits one description per metric name in this scrape.
func (collector snapshotCollector) Describe(output chan<- *prometheus.Desc) {
	seen := map[string]bool{}
	for _, sample := range collector.samples {
		if !seen[sample.Name] {
			seen[sample.Name] = true
			output <- description(sample)
		}
	}
}

// description uses the first sample's label names for that metric.
func description(sample Sample) *prometheus.Desc {
	return prometheus.NewDesc("hot_to_cold_"+sample.Name, metricHelp(sample.Name), sample.Labels, nil)
}

// Collect emits each sample as a gauge, or as a counter when the journal total is cumulative.
func (collector snapshotCollector) Collect(output chan<- prometheus.Metric) {
	for _, sample := range collector.samples {
		kind := prometheus.GaugeValue
		if sample.Counter {
			kind = prometheus.CounterValue
		}
		output <- prometheus.MustNewConstMetric(description(sample), kind, sample.Value, sample.Values...)
	}
}

// metricHelp is fixed for the journal series the dashboard expects. Any other
// name is described as the current population of that series.
func metricHelp(name string) string {
	help := map[string]string{
		"writers":                              "Writer permits blocking archival, including expiry drain grace.",
		"writers_expired":                      "Unreleased writer permits past expiry and drain grace; not cumulative events.",
		"jobs":                                 "Journal jobs by durable next-step state.",
		"jobs_with_errors":                     "Unfinished journal jobs carrying an error; not cumulative failures.",
		"jobs_failed":                          "Unfinished journal jobs carrying an error; not cumulative failures.",
		"job_milestones_total":                 "First job milestones recorded durably in this journal, including backfill baseline.",
		"last_job_milestone_timestamp_seconds": "Last known milestone Unix timestamp; zero if unknown or never observed.",
		"job_milestone_baseline_total":         "Legacy milestones counted at observability migration, without invented event times.",
		"jobs_waiting":                         "Distinct currently blocked journal jobs by reason.",
		"readers":                              "Reader grants blocking deletion, including expiry drain grace.",
		"readers_expired":                      "Unreleased reader rows past expiry and drain grace; not cumulative events.",
		"lag_seconds":                          "Retention-adjusted cold routing frontier lag; not hot deletion lag.",
		"routing_frontier_timestamp_seconds":   "Exclusive hot/cold routing boundary as Unix seconds.",
		"late_oldest_age_seconds":              "Age of oldest pending late record, zero when none.",
		"oldest_pending_job_created_timestamp_seconds": "Oldest unfinished job creation Unix timestamp, zero when none or unknown.",
	}
	if text, ok := help[name]; ok {
		return text
	}
	return "Current journal population: " + name + "."
}

// Metrics holds the process-local series. They reset when the process restarts.
// Journal population is not stored here; Handler attaches a fresh snapshot per scrape.
type Metrics struct {
	Registry                      *prometheus.Registry
	attempts                      *prometheus.CounterVec
	duration                      *prometheus.HistogramVec
	inProgress, started           *prometheus.GaugeVec
	SchedulerErrors               *prometheus.CounterVec
	SchedulerIteration, NextRetry prometheus.Gauge
	CleanupFiles, CleanupErrors   prometheus.Counter
	CleanupSuccess                prometheus.Gauge
	mu                            sync.Mutex
	retryID                       string
	retryAt                       time.Time
}

// New registers the process series and creates every allowed label combination
// up front, so an idle stage is a real zero. COMPLETE is omitted. deletion,
// coordinateOnly, and retention are published once from the process configuration.
func New(retention time.Duration, deletion, coordinateOnly bool) *Metrics {
	registry := prometheus.NewRegistry()
	factory := promauto.With(registry)
	metrics := &Metrics{Registry: registry}
	metrics.attempts = factory.NewCounterVec(prometheus.CounterOpts{Name: "hot_to_cold_step_attempts_total", Help: "Finished execution attempts since process start; excludes normal waits."}, []string{"mode", "stage", "outcome"})
	metrics.duration = factory.NewHistogramVec(prometheus.HistogramOpts{Name: "hot_to_cold_step_duration_seconds", Help: "Execution attempt wall time since process start; excludes normal waits.", Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200, 14400, 21600}}, []string{"mode", "stage", "outcome"})
	metrics.inProgress = factory.NewGaugeVec(prometheus.GaugeOpts{Name: "hot_to_cold_step_in_progress", Help: "Currently executing attempt; resets on restart."}, []string{"mode", "stage"})
	metrics.started = factory.NewGaugeVec(prometheus.GaugeOpts{Name: "hot_to_cold_step_started_timestamp_seconds", Help: "Current attempt start Unix timestamp; zero while idle."}, []string{"mode", "stage"})
	metrics.SchedulerErrors = factory.NewCounterVec(prometheus.CounterOpts{Name: "hot_to_cold_scheduler_errors_total", Help: "Scheduler selection errors since process start."}, []string{"operation"})
	metrics.SchedulerIteration = factory.NewGauge(prometheus.GaugeOpts{Name: "hot_to_cold_scheduler_last_iteration_timestamp_seconds", Help: "Last scheduler iteration; synchronous worker steps can delay subsequent iterations."})
	metrics.NextRetry = factory.NewGauge(prometheus.GaugeOpts{Name: "hot_to_cold_next_retry_timestamp_seconds", Help: "Scheduled retry Unix timestamp; zero when none."})
	metrics.CleanupFiles = factory.NewCounter(prometheus.CounterOpts{Name: "hot_to_cold_profile_cleanup_files_total", Help: "Performance profiles removed since process start."})
	metrics.CleanupErrors = factory.NewCounter(prometheus.CounterOpts{Name: "hot_to_cold_profile_cleanup_errors_total", Help: "Failed profile maintenance passes since process start."})
	metrics.CleanupSuccess = factory.NewGauge(prometheus.GaugeOpts{Name: "hot_to_cold_profile_cleanup_last_success_timestamp_seconds", Help: "Last successful profile maintenance pass Unix timestamp; zero before first success."})
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	for _, mode := range Modes {
		for _, stage := range States {
			if stage == "COMPLETE" {
				continue
			}
			metrics.inProgress.WithLabelValues(mode, stage).Set(0)
			metrics.started.WithLabelValues(mode, stage).Set(0)
			for _, outcome := range []string{"success", "error", "canceled"} {
				metrics.attempts.WithLabelValues(mode, stage, outcome)
				metrics.duration.WithLabelValues(mode, stage, outcome)
			}
		}
	}
	for _, operation := range []string{"pending", "late", "deletions", "readers", "archive"} {
		metrics.SchedulerErrors.WithLabelValues(operation)
	}
	for name, value := range map[string]float64{"deletion_enabled": boolValue(deletion), "coordinate_only": boolValue(coordinateOnly), "hot_retention_seconds": retention.Seconds()} {
		gauge := factory.NewGauge(prometheus.GaugeOpts{Name: "hot_to_cold_" + name, Help: "Effective configuration: " + name + "."})
		gauge.Set(value)
	}
	version, revision := "unknown", "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" {
			version = info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				revision = setting.Value
			}
		}
	}
	buildInfo := factory.NewGaugeVec(prometheus.GaugeOpts{Name: "hot_to_cold_build_info", Help: "Service build metadata."}, []string{"version", "revision"})
	buildInfo.WithLabelValues(version, revision).Set(1)
	return metrics
}
func boolValue(enabled bool) float64 {
	if enabled {
		return 1
	}
	return 0
}

// Start marks one stage in progress and returns the function that finishes it.
// A nil recorder, COMPLETE, or a mode or stage outside the closed sets returns
// a function that records nothing. waiting drops the attempt, which is how an
// ordinary block stays out of the success and error counts. Any other error is
// "canceled" when the context ended and "error" otherwise.
func (metrics *Metrics) Start(mode, stage string) func(error, bool) {
	if metrics == nil || stage == "COMPLETE" {
		return func(error, bool) {}
	}
	if !contains(Modes, mode) || !contains(States, stage) {
		return func(error, bool) {}
	}
	began := time.Now()
	metrics.inProgress.WithLabelValues(mode, stage).Set(1)
	metrics.started.WithLabelValues(mode, stage).Set(float64(began.UnixNano()) / 1e9)
	return func(err error, waiting bool) {
		metrics.inProgress.WithLabelValues(mode, stage).Set(0)
		metrics.started.WithLabelValues(mode, stage).Set(0)
		if waiting {
			return
		}
		outcome := "success"
		if err != nil {
			outcome = "error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				outcome = "canceled"
			}
		}
		metrics.attempts.WithLabelValues(mode, stage, outcome).Inc()
		metrics.duration.WithLabelValues(mode, stage, outcome).Observe(time.Since(began).Seconds())
	}
}
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// Retry publishes the next time a job may run. A zero time publishes zero.
// The id is kept so a scrape can tell that job's backoff apart from other waits.
func (metrics *Metrics) Retry(id string, at time.Time) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.retryID, metrics.retryAt = id, at
	unixSeconds := float64(0)
	if !at.IsZero() {
		unixSeconds = float64(at.UnixNano()) / 1e9
	}
	metrics.NextRetry.Set(unixSeconds)
}

// Retrying returns the job id and time last passed to Retry.
func (metrics *Metrics) Retrying() (string, time.Time) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return metrics.retryID, metrics.retryAt
}

// Handler serves GET /metrics from the process registry plus one journal snapshot.
// The snapshot has five seconds and its own registry, so concurrent scrapes cannot
// combine samples. A snapshot error is 503. Any other method is 405.
func (metrics *Metrics) Handler(snapshot func(context.Context) (Snapshot, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
		defer cancel()
		samples, err := snapshot(ctx)
		if err != nil {
			http.Error(w, "journal metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		snapshotRegistry := prometheus.NewRegistry()
		if err = snapshotRegistry.Register(snapshotCollector{samples}); err != nil {
			http.Error(w, "invalid journal metrics", 500)
			return
		}
		promhttp.HandlerFor(prometheus.Gatherers{metrics.Registry, snapshotRegistry}, promhttp.HandlerOpts{}).ServeHTTP(w, request)
	})
}
