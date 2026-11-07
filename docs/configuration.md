# Configuration reference

[Documentation](README.md) · [Deployment](deployment.md) · [Operations](operations.md)

Run `beehive-hot-to-cold-loader --help` for the executable's flag reference. Flags override environment-backed defaults. Empty environment values fall back to the built-in default. Durations use Go syntax such as `30m`, `24h`, or `720h`; use `720h` for 30 days. Dates use UTC `YYYY-MM-DD` and exclusive upper bounds.

## Listener and persistent identity

| Flag | Environment | Default / requirement |
| --- | --- | --- |
| `--addr` | `HOT_TO_COLD_ADDR` | `:10001` |
| `--state` | `HOT_TO_COLD_STATE` | `./state`; private, persistent, outside `/node-data` |
| `--warehouse` | `HOT_TO_COLD_WAREHOUSE` | Required; `https://host/sage/cold-storage-archive-<unique>/warehouse` |
| `--archive-start` | `HOT_TO_COLD_START` | Required; first UTC day whose coverage is managed |
| `--archive-before` | — | Unset; optional exclusive day bounding new archive scheduling |
| `--bootstrap-warehouse` | — | Unset; audited read-only source, paired with `--bootstrap-before` |
| `--bootstrap-before` | — | Unset; exclusive end of audited source coverage, after archive start |

Bucket, warehouse, and coverage start are bound to the control journal. Source bucket ID, organization, and bootstrap settings are also durably bound during archival startup. Do not repoint existing state at another source or warehouse. A new catalog refuses to silently adopt an occupied remote table root.

## Source and native tools

| Flag | Environment | Default / requirement |
| --- | --- | --- |
| `--influx-url` | `INFLUX_URL` | `http://localhost:8086` |
| `--org` | `INFLUX_ORG` | `waggle` |
| `--bucket` | `INFLUX_BUCKET` | `waggle` |
| `--bucket-id` | `INFLUX_BUCKET_ID` | Required for archival; source engine bucket ID |
| `--engine-path` | `INFLUX_ENGINE_PATH` | Required for archival; read-only Influx engine mount |
| `--influxd` | `INFLUXD_BINARY` | `influxd`; matching source version |
| `--duckdb` | `DUCKDB_BINARY` | `duckdb`; version 2.0.x with required extensions |
| `--duckdb-memory` | `DUCKDB_MEMORY` | `3GB`; native buffer limit, not total RSS |
| `--duckdb-build-threads` | — | `16`; valid range 1–256 |
| `--duckdb-build-sort-threads` | — | `2`; valid range 1–256 |
| `--pelican-key` | `PELICAN_KEY_PATH` | ES256 PEM signing key required for archival |

Normalization/base work and fresh exact comparisons use sixteen threads. The build overrides apply to private build sessions. Native commands enforce free-space/spill guards; leave capacity for staging, output, Go memory, native allocations, and other processes. See [validation](validation.md).

## Scheduling and deletion gates

| Flag | Default | Meaning |
| --- | --- | --- |
| `--hot-retention` | `720h` | Positive minimum hot age; complete UTC days only |
| `--interval` | `1h` | Positive new archival scheduling interval |
| `--late-interval` | `5m` | Positive late correction scheduling interval |
| `--coordinate-only` | `false` | Serve coordination without starting archival work or deletion |
| `--writers-coordinated` | `false` | Required to archive; attests all selected-bucket writers participate |
| `--delete-enabled` | `false` | Permit guarded deletion after routing and proofs |
| `--readers-coordinated` | `false` | Required with deletion; attests all readers use managed views |
| `--write-lease-duration` | `30m` | Initial/renewed writer lease; positive and at most `24h`; fixed one-minute grace after expiry |
| `--read-lease-duration` | `30m` | Initial/renewed read lease; positive and at most `24h` |

These switches are operator attestations, not discovery of bypass credentials. `--coordinate-only` cannot be combined with deletion; it does not require the engine/bucket ID or `INFLUX_TOKEN`, and preserves existing durable fences. Retention changes govern new jobs; they do not move archived data back to hot. `--archive-before` does not suppress existing recovery, late work, or deletions.

## Credentials

| Environment | Purpose |
| --- | --- |
| `HOT_TO_COLD_LOADER_TOKEN` | Loader-role coordination requests |
| `HOT_TO_COLD_READER_TOKEN` | API read-view requests |
| `HOT_TO_COLD_ADMIN_TOKEN` | Administrative status requests |
| `INFLUX_TOKEN` | Source query/read/delete credential; required outside coordinate-only mode |

The three coordination secrets must each be at least 32 bytes and differ. Inject secrets using deployment configuration. Pelican access uses the signing key configured above; per-object requests mint scoped short-lived tokens, while native scan tokens cover the six-hour worker deadline plus a margin. `GET /metrics` has no authentication and no metrics-token setting.

## Profiles and recovery

| Flag | Environment | Default / meaning |
| --- | --- | --- |
| `--profile-steps` | — | `false`; detailed per-attempt step reports |
| `--profile-retention` | `HOT_TO_COLD_PROFILE_RETENTION` | `720h` after completion; `0` disables expiry |
| `--keep-staging` | — | `false`; preserve transient data and profiles |
| `--resolve-stopped-owner` | — | Unset; offline grant recovery for a confirmed stopped owner |
| `--confirm-owner-stopped` | — | `false`; required for owner recovery |

Metrics do not require `--profile-steps`. Other build, normalization, and operation reports provide separate evidence. Retention only removes allowlisted performance profiles from completed jobs, preserving recovery/verification records and unknown legacy completion times. See [metrics](metrics.md).

## Configure sibling services

These are settings in the sibling applications, not environment variables read by this coordinator:

| Application | Settings |
| --- | --- |
| `beehive-influxdb-loader` | `HOT_TO_COLD_LOADER_URL`, `HOT_TO_COLD_LOADER_LOADER_TOKEN`, `HOT_TO_COLD_LOADER_LOADER_OWNER` |
| `beehive-data-api` | `COLD_FRONTIER=dynamic`, `HOT_TO_COLD_LOADER_URL`, `HOT_TO_COLD_LOADER_READER_TOKEN`, `HOT_TO_COLD_LOADER_READER_OWNER`, `COLD_WAREHOUSE` |

The Influx loader's `HOT_TO_COLD_LOADER_LOADER_TOKEN` must equal this service's `HOT_TO_COLD_LOADER_TOKEN`. The data API's `HOT_TO_COLD_LOADER_READER_TOKEN` must equal this service's `HOT_TO_COLD_READER_TOKEN`. Use distinct process owners. The API's warehouse must match the managed warehouse; set `COLD_BORDER` in dynamic mode. An uncoordinated loader can still write Influx when its optional coordination settings are unset, so migrate all writers and restrict bypass access before asserting `--writers-coordinated`.
