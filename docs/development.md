# Developer guide

[Documentation](README.md) · [Architecture](architecture.md) · [CI/CD](ci-cd.md) · [Validation](validation.md)

## Source map

| Location | Responsibility |
| --- | --- |
| `main.go` | Configuration, startup checks, exclusive lock, dependencies, HTTP listener, synchronous scheduler, and shutdown |
| `internal/control/store.go` | SQLite journal, permits, inbox/quarantine, pinned read views, scheduling selection, activation, and recovery |
| `internal/control/http.go` | Role-authenticated coordination/status commands and public metrics route |
| `internal/control/metrics.go` | Transactional outcome totals and bounded journal snapshots |
| `internal/archive/runner.go` | Recoverable state transitions, deletion guards, and completed-job scratch cleanup |
| `internal/archive/profile.go`, `retention.go` | Step reports and bounded completed-profile maintenance |
| `internal/storage/influx*.go` | Source validation, engine export, independent CSV query decoding, and deletion |
| `internal/storage/normalize.go`, `export_dedup.go`, `staging*.go` | Primitive Parquet, overwrite normalization, and bounded merge |
| `internal/storage/value.go` | Marked uint64 VARIANT encoding and logical-kind proof normalization |
| `internal/storage/duckdb.go`, `pack.go` | Native SQL execution, build materialization, ordering, and bounded plugin groups |
| `internal/storage/compare*.go`, `typed_dictionary.go` | Canonical exact proofs, private build reuse, and the build and local-expected tag dictionary |
| `internal/storage/catalog.go`, `pelican.go` | Local SQL Iceberg catalog and scoped HTTPS object IO |
| `internal/storage/worker.go`, `reconciliation.go`, `remote_verification.go` | Storage-side lifecycle operations, the reconciliation raw-tag dictionary, and fresh remote proof policy |
| `internal/observability/metrics.go` | Private Prometheus registry, process-local instrumentation, and snapshot exposition |
| `observability/` | Prometheus scrape/alerts and Grafana dashboard/provisioning |
| `testing/` | Isolated Python preparation, diagnostic, and lifecycle harnesses |

`archive.Runner` depends on the `archive.Worker` interface. Unit tests can supply a fake worker to exercise ordering, failure, retries, and cleanup without native storage. `storage Worker` implements actual side effects; it does not independently advance the runner's durable state.

The coordinator invokes the DuckDB CLI rather than embedding DuckDB's Go driver. Do not add the API's Arrow build tag to this module. Native child processes inherit the writer lock so an orphaned child prevents a conflicting restart.

## Local checks

Run from the service root:

```sh
CGO_ENABLED=0 go build -trimpath -o /tmp/beehive-hot-to-cold-loader .
CGO_ENABLED=1 go test -race -count=1 ./...
go vet ./...
```

The default suite includes fake-worker, journal, HTTP, storage, and observability checks. Native/engine/integration gates need explicit runtime inputs and may skip without them. A passing default suite alone does not validate real Pelican IO, source engine consistency, or full-day capacity.

Reproduce the CI race and native smoke selections locally with the commands in [CI/CD](ci-cd.md). Some gates bind local listeners. Use a suitable local/VM environment and dedicated fixtures. See [validation](validation.md) for the matching-engine gate and the [testing index](../testing/README.md) for Python checks and full lifecycle/capacity runs.

## Changing the pipeline

Preserve recoverability before optimizing work. Each successful state transition must follow the corresponding completed effect; interrupted work must be
recognizable on retry. Test failures around publication, routing, deletion, and post-delete confirmation rather than only a successful sequence.

Keep these invariants explicit in code reviews:

- New fences reject hot admission for their day and wait for existing permits to release or reach expiry plus drain grace.
- Late receipts are durable before acknowledgement and idempotent on retry.
- Identity includes exact nanosecond time, measurement name, and complete tags.
- Verification compares actual canonical rows and duplicate counts, not hashes alone.
- Routing activates only remotely verified data; pinned older views remain valid.
- Deletion waits for readers and repeats a fresh proof on every attempt.
- Source scope, destination scope, layout properties, and field IDs are validated.
- Cleanup is confined to known private artifacts and respects preservation controls.

Benchmark candidates against retained immutable inputs in new scratch directories. Use the independent reference proof, then run the native/lifecycle gates before promoting a policy. Record results in the plan with metric scope and resource limits; distinguish isolated-stage gains from complete lifecycle gains.

## Coordination API

All commands use POST with a JSON body containing the configured `bucket`, and `Authorization: Bearer <role-token>`. Bodies are limited to 16 MiB, require one JSON value, and reject unknown fields. Invalid input returns 400; store-level conflicts return 409. Successful command responses are JSON.

| Route | Role | Main request fields / result |
| --- | --- | --- |
| `/v1/writes/acquire` | Loader | `id`, `owner`, `days`; returns `id`, admitted `hot_days`, and `expires_at` |
| `/v1/writes/renew` | Loader | `id`, `owner`; extends a live permit without changing admitted days |
| `/v1/writes/release` | Loader | `id`, `owner`; confirms successful completion of a live permit |
| `/v1/late` | Loader | `id`, `records`; durably accepts late data |
| `/v1/quarantine` | Loader | `id`, `payload`; retains rejected input |
| `/v1/reads/acquire` | Reader | `id`, `owner`; returns pinned view and `expires_at` |
| `/v1/reads/renew` | Reader | `id`, `owner`; renews the same view |
| `/v1/reads/release` | Reader | `id`; releases the view |
| `/v1/status` | Admin | Returns aggregate control status |
| `/metrics` (GET) | Public | Prometheus exposition; no JSON body or token |

A late record has `time` (signed nanosecond epoch integer), `name`, `tags` (string map), `kind` (`float`, `int`, `uint`, `bool`, `string`), and `value` (text matching the kind). Nonfinite numeric values and empty tag keys/values are rejected. Use the maintained loader/API coordination clients rather than creating a second ingestion path that bypasses writer fencing.
