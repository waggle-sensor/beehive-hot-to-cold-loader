# CI/CD (GitHub Actions)

[Documentation](README.md) · [Developer guide](development.md) · [Validation](validation.md)

The workflow lives at [`.github/workflows/ci-cd-pipeline.yaml`](../.github/workflows/ci-cd-pipeline.yaml). It validates the module on every change that reaches `main`, and publishes a container image only for pull requests and published releases.

## Triggers

| Event | Validation | Image publish |
| --- | --- | --- |
| Pull request targeting `main` | Yes | Yes (PR tags via the reusable build workflow) |
| Push to `main` | Yes | No |
| Published GitHub release | Yes | Yes (release tags) |
| Manual `workflow_dispatch` | Yes | No |

The workflow requests `contents: read` only. Image publishing uses the shared Waggle reusable workflow with inherited secrets.

## Jobs

All validation jobs run on `ubuntu-24.04` with a 30-minute timeout. Go version comes from `go.mod`. Image build waits on both validation jobs.

```text
go_checks ────────┐
                  ├──► push_to_registry (PR or release only)
native_smoke ─────┘
```

### Go checks (`go_checks`)

| Step | What it proves |
| --- | --- |
| Build production binary | `CGO_ENABLED=0 go build -trimpath` succeeds for the service entrypoint |
| Vet | `go vet ./...` finds no static issues across the module |
| Unit / integration / race tests | `CGO_ENABLED=1 go test -race -count=1 ./...` passes with the race detector |

Race tests write JSON events to `go-test.json`. That file is uploaded as the `go-test-log` artifact for seven days, including when the step fails.

Native DuckDB gates that need `SAGE_ARCHIVE_DUCKDB2` skip in this job when the binary is absent. A green race suite alone does not prove those native paths.

### Native smoke (`native_smoke`)

This job installs the same DuckDB **2.0.0** CLI used by the Dockerfile (`duckdb/duckdb:2.0.0`), loads `httpfs` and `iceberg`, then runs five selected tests in `./internal/storage`:

| Test | Coverage |
| --- | --- |
| `TestNativeLifecycle` | Empty day, append, remote verification, deletion, late correction, and pinned snapshot against a local TLS object fixture |
| `TestNativeBoundedPluginPacking` | Native VARIANT encoding, nanosecond instants, row-group caps, and plugin purity |
| `TestNativePartitionedNormalization` | Partitioned export normalization correctness |
| `TestNativeUnsignedVariantProof` | Unsigned VARIANT handling |
| `TestNativeBoundedComparison` | Bounded exact comparison path |

Command shape (also useful locally):

```sh
SAGE_ARCHIVE_DUCKDB2=/absolute/path/to/duckdb \
  go test -json -count=1 -timeout=20m ./internal/storage \
  -run '^(TestNativeLifecycle|TestNativeBoundedPluginPacking|TestNativePartitionedNormalization|TestNativeUnsignedVariantProof|TestNativeBoundedComparison)$' \
  | tee native-test.json
```

A follow-up step always parses `native-test.json` and fails unless each of those five tests reports `pass`. A skip or missing event fails the job, so an unset DuckDB path cannot look green. The JSON log uploads as `native-test-log` for seven days.

This selection is a smoke gate for DuckDB-backed normalization, packing, comparison, and proof behavior. It does not replace the matching-engine gate, live Pelican lifecycle, or full-day capacity runs documented in [validation](validation.md).

### Image build (`push_to_registry`)

Runs only when both validation jobs succeed and the event is a pull request or a published release. It calls [`waggle-sensor/.github` `build-and-push-docker-image.yml`](https://github.com/waggle-sensor/.github/blob/main/.github/workflows/build-and-push-docker-image.yml). Pushes to `main` and manual dispatch validate without publishing.

## Artifacts

| Artifact | Source job | Retention |
| --- | --- | --- |
| `go-test-log` | `go_checks` (`go-test.json`) | 7 days |
| `native-test-log` | `native_smoke` (`native-test.json`) | 7 days |

## Out of scope for this workflow

CI does not run the Influx engine consistency gate, the small live end-to-end harness, or the representative full-day capacity guide. Those remain operator/VM gates under [validation](validation.md) and [testing](../testing/README.md).
