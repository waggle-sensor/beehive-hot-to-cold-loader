# Validation tooling

These are the maintained tests and isolated diagnostics for the official
`beehive-hot-to-cold-loader` service. Run commands from the workspace root unless
a guide says otherwise. Go unit/native tests live under `internal` and run from
the service directory:

```sh
cd beehive-hot-to-cold-loader
go test -race ./...
go vet ./...
```

Native DuckDB/Influx gates are opt-in through the variables described in the
[validation guide](../docs/validation.md). Docker diagnostics need architecture-matching pinned native
tools; `bin/` is gitignored and runtime inputs must be supplied before image
builds. Service state, snapshots, secrets and local build artifacts are not source.

| Guide | Purpose |
| --- | --- |
| [Live gate](live/README.md) | Loader/Influx/Pelican/API/client coordination, late data, recovery and guarded fixture deletion |
| [Full-day capacity](capacity/README.md) | Representative complete archival lifecycle and resource/proof evidence |

Live/capacity preparations freeze adjacent `beehive-data-api`,
`beehive-influxdb-loader` and `sage-data-client` code. Signing keys are configured
as external runtime inputs; they are not part of the service source or images.
Preparations do not deploy production. Execution uses fresh private fixtures and
scoped storage; deletion remains guarded and limited to the fixture.

Saved results, frozen source, old binaries and legacy utilities remain under
`sage-data-rework/archive-exporter`. Historical `state/...` measurement references
retain that meaning; use explicit old input paths for retained diagnostics.
New preparations default to `beehive-hot-to-cold-loader/state`, and should be
created from this code. Never reuse a completed/failed preparation as current
service validation, move saved datasets, or delete older runs implicitly.

Offline Python tests can be run per harness, for example:

```sh
python3 -m unittest discover -s beehive-hot-to-cold-loader/testing/capacity -v
python3 -m unittest discover -s beehive-hot-to-cold-loader/testing/live -v
```
