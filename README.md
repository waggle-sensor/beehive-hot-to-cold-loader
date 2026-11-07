# Beehive hot-to-cold loader

Beehive's archival service moves completed UTC days from InfluxDB into an Iceberg v3 warehouse on [Pelican](https://docs.pelicanplatform.org/about-pelican). It verifies the published data before routing queries to cold storage, then deletes the verified hot interval when deletion is enabled and older readers have drained.

The service coordinates with [beehive-influxdb-loader](https://github.com/waggle-sensor/beehive-influxdb-loader) for writer fencing and late measurements, and with
[beehive-data-api](https://github.com/waggle-sensor/beehive-data-api/blob/main/docs/README.md) for pinned read views. A Go scheduler and SQLite journal manage recoverable jobs; no external queue is required. Hot retention defaults to 30 days and is configurable.

## Documentation 

| Guide | Read this for |
| --- | --- |
| [Service overview and architecture](docs/architecture.md) | Lifecycle, late data, read routing, verification, and warehouse layout |
| [Configuration](docs/configuration.md) | Flags, environment variables, defaults, and integration settings |
| [Deployment](docs/deployment.md) | Coordinated rollout, bootstrap, and enabling deletion |
| [Operations and recovery](docs/operations.md) | Status, reader leases, stopped writers, cleanup, and troubleshooting |
| [Metrics and Grafana](docs/metrics.md) | Public Prometheus endpoint, dashboard, alerts, and JSON profile retention |
| [Developer guide](docs/development.md) | Package responsibilities, local checks, and change invariants |
| [Validation and performance](docs/validation.md) | Native gates, accepted policies, and benchmark methodology |
| [Testing tools](testing/README.md) | Isolated live and retained-artifact harnesses |

## Build

Use Go **1.26.5** as declared by `go.mod`:

```sh
go build -o bin/beehive-hot-to-cold-loader .
go test -race ./...
go vet ./...
./bin/beehive-hot-to-cold-loader --help
```

Archival execution also needs the source-compatible `influxd` executable (current image: **2.7.7**) and the **DuckDB 2.0.x CLI** with `httpfs` and `iceberg`
extensions available to the runtime user. The service invokes native processes; it does not require the API's `duckdb_arrow` build tag.

The container image copies `influxd` **2.7.7** from the official [InfluxDB image](https://hub.docker.com/_/influxdb/) and the DuckDB **2.0.0** CLI from the official [DuckDB image](https://hub.docker.com/r/duckdb/duckdb). Both land on `PATH`, so the `--duckdb` and `--influxd` defaults need no binaries from the build machine:

```sh
docker build -t beehive-hot-to-cold-loader:local .
```

Mount persistent state, the Influx engine read-only, and the Pelican signing key at runtime. The image does not include deployment credentials or service state.

## Run an isolated, bounded archive

Supply distinct secrets of at least 32 bytes for `HOT_TO_COLD_LOADER_TOKEN`, `HOT_TO_COLD_READER_TOKEN`, and `HOT_TO_COLD_ADMIN_TOKEN`, plus `INFLUX_TOKEN` and `PELICAN_KEY_PATH`. Use deployment secret injection for real installations.

After every writer for the selected bucket participates in coordination:

```sh
./bin/beehive-hot-to-cold-loader \
  --state /var/lib/beehive-hot-to-cold-loader \
  --warehouse 'https://<host>/sage/cold-storage-archive-<unique>/warehouse' \
  --archive-start 2025-03-01 --archive-before 2025-03-02 \
  --influx-url http://influxdb:8086 \
  --org waggle --bucket waggle --bucket-id '<source bucket ID>' \
  --engine-path /influx-engine \
  --writers-coordinated
```

This bounds new archive scheduling to March 1; it does not disable recovery or late corrections. Choose coverage dates matching the source history. Use `--coordinate-only` during writer migration. Follow the [deployment guide](docs/deployment.md) before enabling archival or deletion against a real bucket.

**Deletion is off by default.** Enabling it requires `--delete-enabled` and `--readers-coordinated` alongside coordinated writers. Keep Influx bucket retention unlimited so unverified age-based expiration cannot bypass the service. Use one active instance and one persistent state/catalog per warehouse. Never use `/node-data` or an existing demo warehouse as the writable destination.

## Monitor

The internal listener defaults to port **10001**. Prometheus can scrape `GET /metrics` without authentication. Coordination and status endpoints require the appropriate role token:

```sh
curl --fail http://127.0.0.1:10001/metrics
curl --fail -H "Authorization: Bearer $HOT_TO_COLD_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"waggle"}' http://127.0.0.1:10001/v1/status
```

The [Grafana dashboard](observability/grafana/beehive-hot-to-cold-loader.json) covers job outcomes, stage timings, backlog, coordination, deletion, and process health. JSON profiles provide detailed per-job evidence alongside metrics.

## Research history

This service was promoted from `sage-data-rework/archive-exporter`. Additional Design decisions and measured results remain in [plan 12](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md). Historical saved runs retain their original locations; new service runs use this repository's state directory. See [validation tooling](testing/README.md) for workspace dependencies and fixture preparation.
