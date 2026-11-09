# Metrics, dashboards and profile retention

[Documentation](README.md) · [Operations](operations.md) · [Implementation plan](../plans/1_prometheus_observability.md)

The service exposes public `GET /metrics` on its existing internal coordination listener, default port 10001. Scraping never contacts Influx or Pelican, executes DuckDB, reads profile files or changes a job's state. The Go scheduler and SQLite journal remain authoritative. A successful scrape does not prove Influx/Pelican availability or replace a small actual-query health check.

## Connect Prometheus

Use the [scrape job](../observability/prometheus/scrape.yaml) with your actual internal service address. Merge it into the collector configuration; its default 30-second interval and 10-second timeout allow for a bounded five-second journal snapshot. Treat a failed scrape as unknown, not zero backlog. Snapshot queries honor request cancellation, including while waiting for the journal connection.

`GET /metrics` requires no token or authorization header. Other methods on this path return 405. Coordination and `/v1/status` routes retain their role-based authentication.

```sh
curl --fail --silent --show-error http://127.0.0.1:10001/metrics
```

No Prometheus scrape credentials or Pushgateway are required.

## What the metrics mean

Application names below use prefix `hot_to_cold_`:

| Suffix | Type / meaning |
| --- | --- |
| `writers` | Gauge: writer permits protecting archival, including expiry drain grace |
| `writers_expired` | Gauge: unreleased writer permits past expiry and drain grace; not cumulative events |
| `readers`, `readers_expired` | Gauges: readers still protecting deletion including grace; expired unreleased rows no longer protecting deletion |
| `late_pending`, `quarantined` | Gauges: pending late records and retained quarantined records |
| `jobs_pending`, `deletions_pending`, `jobs_failed` | Gauges: all unfinished jobs, archive jobs waiting for/performing deletion, unfinished jobs carrying an error |
| `late_oldest_age_seconds`, `lag_seconds` | Gauges: pending late age and routing-frontier lag beyond configured hot retention; zero when no age/lag |
| `jobs{mode,state}`, `jobs_with_errors{mode,state}` | Gauges: job population and current errors by durable next-step state |
| `job_milestones_total{mode,milestone}` | Journal-persistent counter: first ROUTED and COMPLETE milestones per job |
| `job_milestone_baseline_total{mode,milestone}` | Journal-persistent counter: legacy milestones backfilled at migration; not new runtime events |
| `last_job_milestone_timestamp_seconds{mode,milestone}` | Gauge: last known milestone timestamp; zero when unknown/not observed |
| `step_attempts_total{mode,stage,outcome}` | Process-local counter: completed execution attempts; excludes ordinary waits |
| `step_duration_seconds{mode,stage,outcome}` | Process-local histogram: attempt wall time, including failed/canceled attempts; excludes ordinary waits |
| `step_in_progress{mode,stage}`, `step_started_timestamp_seconds{mode,stage}` | Gauges: executing attempt and start; zero while idle/after restart |
| `jobs_waiting{mode,reason}` | Gauge: distinct jobs blocked by writers, readers, disabled deletion, retry backoff or coordinate-only mode |
| `next_retry_timestamp_seconds` | Gauge: scheduled retry time, zero when none |
| `scheduler_errors_total{operation}` | Process-local counter: scheduler/journal selection errors |
| `scheduler_last_iteration_timestamp_seconds` | Gauge: last scheduler iteration; can stop advancing during a synchronous worker operation |
| `routing_frontier_timestamp_seconds` | Gauge: exclusive hot/cold boundary; not an Influx deletion boundary |
| `oldest_pending_job_created_timestamp_seconds{mode}` | Gauge: oldest unfinished job creation time, zero when none or unknown |
| `deletion_enabled`, `coordinate_only`, `hot_retention_seconds` | Gauges: effective runtime settings |
| `build_info{version,revision}` | Constant gauge 1 with available build metadata |
| `profile_cleanup_files_total`, `profile_cleanup_errors_total` | Process-local counters: performance profiles removed and failed maintenance passes |
| `profile_cleanup_last_success_timestamp_seconds` | Gauge: last successful enabled maintenance pass, zero before first success |

`mode` is `archive` or `late`. `milestone` is `routed` or `complete`. `outcome` is `success`, `error` or `canceled`. Stage/state names are the fixed archival state machine. Scheduler operation and wait reason labels are bounded; an `unknown` wait series is initialized for future explicitly unknown reasons. No job IDs, days, plugins, owners, paths, tokens or error strings are labels. Configure environment/deployment labels at the Prometheus target instead.

ROUTED means verified cold data is active for reads. It does not mean hot data was deleted. COMPLETE is the separate durable job outcome. Waits do not inflate failure/success counts. Retries produce separate attempt observations and one durable first milestone. Finished-attempt metrics can miss an attempt interrupted by process death; current job state and durable milestones survive that restart.

The additive milestone migration counts unambiguous legacy routing/completion states once, but supplies no invented event times or duration samples. Those timestamps remain zero. Counters survive completed-payload pruning through their separate aggregate/marker tables. Replacing/restoring the journal changes that persistence boundary; Prometheus counter-reset handling still applies. SQLite transactions commit milestone markers/totals together with routing/completion.

Standard `go_*` collectors expose runtime information. Standard `process_*` collectors expose process resources on supported platforms, including the Linux deployment; they may be absent on macOS. They exclude DuckDB child resources and container totals. Use native JSON profiles and host/container exporters for those scopes. Prometheus provides target `up`, not the service.

Metrics work when `--profile-steps` is off. JSON profiles retain detailed per-job CPU/substep/comparison evidence; they are not read during a scrape.

## Grafana dashboard

Import [the dashboard JSON](../observability/grafana/beehive-hot-to-cold-loader.json) or provision it with [the provider example](../observability/grafana/provisioning/dashboards.yaml). It uses schema version 38 and standard Grafana panels (target Grafana 10 or later).

Import asks for the Prometheus datasource once and stores that choice in the dashboard. Instance and mode remain dashboard filters. API renewal panels have separate API job and instance filters. The dashboard defaults to 24 hours and refreshes every 30 seconds. Mode filters only apply to metrics carrying that label. Operational series retain instance separation. Panels cover service health, outcomes/errors, executing/waiting work, backlog/frontier, stage duration/sample counts, safe deletion/readers, profile cleanup and Go process resources.

Missing samples display unknown/no data. Zero/unset timestamps are hidden. Empty duration denominators produce no average, not zero latency. Histogram p95 is a bucket estimate and can be unreliable with sparse samples; check counts. Time-range milestone increases count events observed in scrape history, not events before the first scrape. Grafana is read-only: no retry, grant-release or delete controls, no embedded credentials, and no duplicate Grafana alert policy.

## Alerts and diagnosis

Use [the example rules](../observability/prometheus/alerts.yaml) as an initial policy, adjusting job selection and budgets to the server workload. They cover target loss, persistent job errors, late backlog, routing lag, long stages, an idle stalled scheduler, new quarantine records, growing enabled deletion backlog and profile-maintenance errors. Disabled deletion/coordinate-only operation is visible without automatically being an incident. A synchronous step can pause the scheduler heartbeat; its own in-progress/age metrics provide the right scope.

```sh
promtool check config observability/prometheus/scrape.yaml
promtool check rules observability/prometheus/alerts.yaml
cd observability/prometheus
promtool test rules alerts.test.yaml
```

Examples have alert fixtures for target loss, persistent errors and intentional pauses. Structural/metric-reference tests are part of Go tests; `promtool` and live Grafana checks still require those tools.

Use authenticated `/v1/status` and service logs to identify individual failed jobs/owners. Pending read grants have renewable leases; expired rows can remain for diagnosis without blocking deletion. Expiry is not proof a remote read has stopped. Writer permits also expire after their confirmed deadline and drain grace; explicit stopped-owner recovery remains available. A failed scrape means unknown status; do not replace it with zeros in dashboards/rules.

## Profile retention

`--profile-retention` defaults from `HOT_TO_COLD_PROFILE_RETENTION`, or `720h` (30 days). Zero disables expiry; negative/invalid durations fail startup. The clock starts at the job's persisted COMPLETE milestone, not file modification time, ROUTED activation or the age of the measurements. Use `--profile-retention=0` to preserve diagnostic profiles indefinitely without retaining large staging artifacts. `--keep-staging` also suppresses profile expiry. These controls do not affect Prometheus retention or hot-data retention.

After startup and then daily, enabled non-coordinate-only service instances run a bounded pass under singleton ownership: up to 100 eligible jobs, 4096 directory entries and a five-second context. A journal cursor resumes after restart and wraps after an exhausted batch. Budget/error cases retain remaining files and report an error for operator investigation; the next daily pass retries. This is best-effort disk maintenance, not a guarantee of removal at the exact instant retention elapses.

Only these regular performance files inside the verified job directory qualify:

- `step-<known-state>-<unique>.json`;
- `operation-reconcile-*.json`, `operation-verify-*.json`, `operation-delete-*.json`;
- `expected-profile.json` and `packing-profile.json`.

Unfinished/failing jobs retain all profiles. Failed-attempt reports of an eventually completed job follow its completion retention window. Legacy jobs with unknown completion time retain profiles. Unknown files, pending writes, symlinks, manifests, verification evidence, receipts, journals, catalogs and job directories are preserved. No recursive deletion, remote storage cleanup or historical research/fixture scan occurs. Maintenance errors never alter job outcomes or authorize/retry hot-data deletion.

Saved benchmark fixtures should keep their original evidence. Do not start the service against those saved state directories merely to exercise retention; use fresh isolated temporary state for tests.

## API reader-renewal metrics

The matching API emits `hot_to_cold_read_renewals_total{outcome}` and `hot_to_cold_read_renewal_duration_seconds{outcome}` on its own `/metrics` endpoint. Outcomes are `success`, `error`, `canceled`; transport and invalid-view failures are observed at the API, even when the coordinator receives nothing. These counters/histograms reset on API restart. Metrics callbacks do not authorize or extend a lease; the tiered package remains independent of Prometheus. Scrape the API separately. The dashboard's API renewal row uses separate API job/instance filters, so loader health cannot mask API renewal failures. Reuse an existing API scrape or merge the [optional API scrape example](../observability/prometheus/api-scrape.yaml).
