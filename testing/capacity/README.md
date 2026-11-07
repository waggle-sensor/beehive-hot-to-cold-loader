# Representative full-day archival capacity

This clones one real historical day, read-only, into a fresh private Influx fixture, then runs the archiver through publication, remote verification, routing, reverification, and confirmed deletion. Each run gets a new UUID directory and a new managed Pelican prefix. The original warehouse and demo stack stay unchanged.

From the workspace root, on a host with Docker:

```sh
python3 beehive-hot-to-cold-loader/testing/capacity/run.py
```

The default source is March 1, 2025 in the adopted warehouse:

```text
https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse
```

That day was audited at 140,578,674 rows. The harness measures the selected snapshot and requires that total for the default source and day. `--expected-rows` supplies another audited baseline. `--source-warehouse` and `--day YYYY-MM-DD` select another complete historical day. The source snapshot URL is pinned before extraction, and the source credentials are read-only. This path does not generate a synthetic 140-million-row day, and it does not start RabbitMQ or the loader. Those integrations are covered by the live gate.

The issuer key, DuckDB 2.0 Linux CLI, and origin default to the same paths as the live gate. Override them with `--pelican-key`, `--duckdb`, and `--origin`. Docker needs the base images and their dependencies.

## Isolated directory and capacity admission

Pass `--run-dir` as a new directory on a filesystem with enough free space:

```sh
python3 beehive-hot-to-cold-loader/testing/capacity/run.py \
  --run-dir /path/to/large-volume/sage-full-day-20250301-unique
```

The default is a fresh UUID directory under `beehive-hot-to-cold-loader/state`. That directory is separate from older state and still uses the same filesystem. An earlier NDJSON run retained about 145 GiB and reached about 214 GiB of staging before it failed. This implementation writes compressed primitive Parquet. That older footprint is not a peak estimate for the current path.

Before seeding, admission requires the maximum of 64 GiB, `--min-free-gib` (default 32), and 32 copies of the extracted reference Parquet plus 32 GiB. This is a heuristic. `reports/source-pinned.json` records the row count, primitive Parquet size, estimate, and free space. A failure stops import, archive, and deletion. Extraction may already have written its private reference and destination marker. Native probe commands keep an 8 GiB free reserve and cap DuckDB spill at 16 GiB, or at free space minus that reserve when that amount is smaller.

The pinned-source comparison follows the worker: compressed canonical staging, local refinement by further hash bits until each exact comparison has at most one million rows per side, and `EXCEPT ALL` in both directions on complete records, including duplicate multiplicity. Hashes route records. A partition that is still over the cap after 64 hash bits fails closed. The result records the leaf partition count and row cap. The published warehouse layout is unchanged.

Run the native comparison, lifecycle, and matching-engine gates in the [validation guide](../../docs/validation.md#bounded-exact-verification-and-disk-guards) before another full-day import. If deleted files are still held open, recover that space before starting. This runner does not restart unrelated services or remove old runs.

The VM preflight requires 10 GiB `MemAvailable` by default (`--min-available-gib`). Container and buffer limits are:

| Component | Default limit |
| --- | ---: |
| Influx fixture | 10 GiB |
| Archiver container | 8 GiB |
| Native DuckDB buffer budget | 3 GB |
| Extraction and verification probe | 4 GiB |
| API container, started after routing | 4 GiB |

These are separate limits. Source preparation runs while the archiver is coordination-only. The independent verification and API probes run after archival work routes. Leave room for the VM and other applications. The harness does not stop the demo or any unrelated project. `--influx-memory`, `--archiver-memory`, `--probe-memory`, `--duckdb-memory`, `--min-free-gib`, and `--min-available-gib` document another budget. The DuckDB buffer setting is not the process or container limit.

The Influx default is 10 GiB. Smaller fixture containers were killed during seeding on the VM, and a later 10 GiB trial failed with a write HTTP 500 and no OOM, so 10 GiB is not a proven full-lifecycle budget. `reports/seed-progress.json` separates writing, writes-complete, sentinel writing, source counting, and sentinel counting. A write failure records the acknowledged row count, and counting records progress per window. Full-range Influx counts are consecutive half-open five-minute queries. Their sum must equal the pinned source count.

On a fixture write or count HTTP failure, `reports/influx-http-error.json` keeps up to 8 KiB of the response body with credentials redacted. `reports/influx-logs.json` keeps that project's Influx tail, up to 200 lines or 64 KiB, also redacted, and is saved before the runner stops its containers. A diagnostic failure does not replace the primary error or skip teardown. The runner does not retry the write.

After the API starts, the runner waits up to 240 seconds for `/metrics`, polling every two seconds. That endpoint is registered after cold backend initialization. It does not run a data query or acquire a read grant. `reports/api-ready.json` records that startup. The later Arrow, NDJSON, and client checks are not retried. On failure, the same bounded redacted capture saves `reports/api-logs.json` before teardown. An API that never starts fails the run with deletion still disabled.

## What the run checks

1. Create the private fixture services and check the destination origin, using the same marker placement and image binary path as the live gate.
2. Read the source hint once and pin the immutable metadata URL. Native DuckDB 2.0 scans the chosen day into primitive staging Parquet, preserving integer nanoseconds, name, the complete canonical tags, scalar kind, the original native VARIANT kind, and the JSON value. An unsupported scalar kind fails.
3. Stream Arrow and Parquet batches into lossless line protocol while a seeding permit is held and archival is disabled. Batches are at most 10,000 rows and 4 MiB. Writes go to the new fixture's Influx API at nanosecond precision. This is source preparation. No scalar is coerced, no tag is dropped, no timestamp is rounded, and no record is omitted. Unrepresentable values fail. Literal LF is preserved inside quoted string fields, matching the [InfluxDB 2.7.7 parser](https://github.com/influxdata/influxdb/blob/v2.7.7/models/points.go). A literal backslash followed by `n` stays distinct. Newlines in identifiers, and CR or NUL in string fields, are rejected. Empty tags, invalid names, oversized points, and non-finite values also fail. A failed import can leave partial data in its private fixture. The source warehouse is unchanged, and archival stays disabled.

   Before any historical scan, a seven-row `strings` gate writes gzip-compressed batches to a separate private fixture bucket and reads annotated CSV back. It checks leading, trailing, and repeated LF, quotes and backslashes, a literal backslash-n, line-protocol-looking text after a newline, the row count, and exact nanoseconds. That bucket stays with the fixture for inspection and is outside the archival source. Run only this check before another full-day attempt:

   ```sh
   python3 beehive-hot-to-cold-loader/testing/capacity/run.py \
     --strings-only --run-dir /path/to/new-isolated-string-check
   ```

   Its manifest is `kind: string-roundtrip`. A pass here is a string round-trip, not a full-day capacity result. It uses the usual bootstrap, memory floor, and destination marker, starts only Influx and the probe, and then stops its own containers.
4. Require the seeded row count to match the pinned source, then add one next-day hot sentinel. Release the write permit and start the archiver with `--profile-steps`. The default 30-day retention remains. `--archive-before` limits scheduling to this one eligible day.
5. Wait for export, normalization and query reconciliation, packing, upload, verification, and routing. Retries stay in the timings. A vanished coordinator fails promptly.
6. Before fixture deletion, compare both complete multisets with `EXCEPT ALL`: every timestamp, name, complete tag set, native value kind, and JSON value, between the pinned source reference and the published destination. Counts and hashes do not authorize this phase.
7. Start the managed API and check one bounded series and time window against the source through direct Arrow, NDJSON, and the client's Arrow request. The checker shrinks the window until the result fits the probe row cap. A truncated response is not treated as complete. No full-day DataFrame is created. JSON numbers such as `1` and `1.0` compare by exact decimal value. Strings and bools stay distinct. The storage check separately requires the native VARIANT kinds to match.
8. Enable deletion only for this fixture, with its writers and readers coordinated. The archiver reverifies the original remote object hashes and the full multiset immediately before deleting. Confirm zero rows on the target day and the one retained next-day sentinel. Repeat the bounded API and client checks.
9. Require the stage profiles, kernel resource peaks, and full verification evidence. Ordinary completed-job cleanup then removes the large raw, query, normalized, and packed staging files. The source reference, JSON evidence, and step profiles remain.

Source extraction, seeding, the extra pinned-source comparison, and the bounded API probes have their own timings and resource records. They are outside worker-stage throughput. `archive_to_routed_seconds` includes startup and schema initialization for this warehouse's first day. Exact step timings exclude that startup. The result is this fresh lifecycle, not a warm backlog rate or a broker ingest rate.

## Results

The runner prints its private directory.

| File | Contents |
| --- | --- |
| `run.json` | Result, phase, source and destination, budgets, source hashes, startup-inclusive routing wall time, and whether the fixture services stopped |
| `reports/source-pinned.json`, `source.json`, `seed.json` | Pinned source, measured count, extent, type distribution, admission figures, and preparation cost |
| `reports/routed.json`, `verify.json`, `complete.json` | Normalized engine count, packed data bytes, source and destination equality, and confirmed fixture deletion with the pre-delete object hashes |
| `reports/summary.json` | Stage-attempt totals, retry counts, Go and largest-native-child lifetime RSS, exact attempt profiles, and effective publish and verify rates |
| `reports/resources.jsonl`, `resources.json` | Five-second samples, per-container cgroup lifetime peaks, OOM and exit state, and logical and allocated disk peaks |
| `reports/api-before-delete.json`, `api-after-delete.json` | Bounded wire and client checks |

Publish and verify rates divide compressed data-file bytes by the whole stage wall time. Metadata, CPU, and hash readback are inside that time. They are not physical network bandwidth. Disk peaks cover `/state`, its spill directory, `/influx`, `/bench`, and probe spill. A child path is already inside its parent.

The timed worker stages are:

| State before the attempt | Work timed |
| --- | --- |
| `EXPORTING` | Source preflight, native engine export, and streamed NDJSON |
| `EXPORTED` | External normalization, independent Influx CSV query, and full reconciliation |
| `RECONCILED` | Chronological batches, bounded plugin packing, and the packed-output audit |
| `BUILT` | Data upload, SHA readback, metadata and manifests, and the catalog commit |
| `COMMITTED` | Object-hash readback, full remote scan and comparison, and hint publication |
| `REMOTE_VERIFIED` | Managed route and frontier activation |
| `DELETING` | Full reverification, then the Influx delete request |
| `DELETE_REQUESTED` | Confirm the interval is empty, then finish and clean up |

This harness enables `--profile-steps`. The flag does not change durable transitions, retries, or deletion guards. A missing profile fails the run. CPU deltas cover the Go process and the native children it reaped during the attempt. RSS is a process-lifetime high-water mark. Parent and child peaks stay separate. Kernel cgroup peaks include page cache. Each archiver restart has a new cgroup, and the sampler keeps the earlier observations. Probe reports record their own kernel peaks before exit. Disk sampling can miss a file that existed only briefly or was already unlinked. Peaks from different roles are not one combined VM peak.

A failure is a capacity result. Inspect `job-latest.json`, the failed phase JSON, redacted `native-error.json`, the resource samples, and the retained container OOM or exit state. The per-stage deadline is six hours (`--timeout`). The runner does not resume against a prefix that was already published.

Accepted full-day measurements, including the successive dictionary, reconciliation, and local-verification lifecycles, are in [plan 12](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md). A later policy still needs its own fresh run. Production deletion settings stay unchanged.

## Preparation and stop

```sh
python3 beehive-hot-to-cold-loader/testing/capacity/run.py --prepare-only
python3 beehive-hot-to-cold-loader/testing/capacity/run.py --prepared /absolute/path/to/prepared-directory
python3 beehive-hot-to-cold-loader/testing/capacity/run.py --stop /absolute/path/to/run-directory
```

`--prepared` executes a preparation that has not started. After a failure, use a new directory and prefix. Services and one-shot probes stop on success and on failure. Keep their state, catalog, and remote prefix together. This harness does not touch `/node-data`, an existing source bucket, a demo catalog, production deletion defaults, or remote object cleanup.

Offline checks:

```sh
python3 -m unittest discover -s beehive-hot-to-cold-loader/testing/capacity -v
```

## Archiver execution policy

Fresh runs fix the native thread policy. Normalization uses sixteen threads, merge fan-in sixteen, and one merge worker. Build materialization and per-file comparison use `--duckdb-build-threads=16`. Build batch sort and copy use `--duckdb-build-sort-threads=2`. Fresh reconciliation, commit, and pre-delete comparisons use sixteen threads on a private session copy. `--duckdb-threads` is not a service flag. Canonical shard reuse is required, and the first comparison partition count is fixed. The same native memory setting applies throughout.

`run.json` records `execution_policy`: those thread counts, canonical reuse, fixed partitioning, and `tag_dictionary`, `raw_tag_dictionary`, and `local_dictionary`. `export-dedup.json` records the normalization policy the job used. `expected-profile.json` and `packing-profile.json` record the threads the build used. An existing prepared directory keeps its frozen commands. Prepare a fresh run to validate this policy. Preparation does not modify a deployed service.

Before fixture deletion, `routed` requires the recorded policies: build `tag_dictionary` with sixteen build threads and two sort threads, the configured memory, canonical staging reuse, all fifteen fresh per-file proofs, and the new whole-build proof that reuses those shards. Reconciliation profiles must report `raw_tag_dictionary`. `COMMITTED` and `DELETING` profiles must report `local_dictionary`. Every one of those proofs is a fresh sixteen-thread exact comparison. A remote scan is read again. Older build shards and earlier proof output do not replace it. A missing or legacy profile fails the run. `routed.json` and `summary.json` keep the materialization, packing, and operation profiles after the confirmed-deletion staging cleanup.

## Bounded API probe filters

The API sample sends plain literal `name`, `vsn`, and `node` filters. A pattern without `|` or `*` is equality, so the probe adds no regex anchors or backslash escapes. When an observed identity contains a character outside the probe's literal subset, that filter is omitted from both the HTTP request and the source SQL oracle. The time window shrinks until the complete result fits the probe row cap. A truncated response is not accepted as equality. Production API syntax is unchanged.

## Operation substep profiles

Fresh runs require `operation-*.json` reports. `routed.json` includes the pre-deletion operation evidence. `summary.json` includes `operations.attempts` and `operations.totals`:

| Report | Steps |
| --- | --- |
| `reconcile/EXPORTED` | Normalization, Influx extraction, and exact comparison |
| `verify/COMMITTED` | Object SHA readback, setup, fresh exact comparison, hint publication, and the verification journal |
| `verify/DELETING` | Fresh pre-delete verification, timed on its own |
| `delete/DELETING` | Source validation and the HTTP delete request |

Every attempt is kept. Totals include failures, wall time, self and child CPU, and the nested comparison-phase times. Normalization profiles still record partition, sort, and merge. A successful record has its ordered steps, CPU usage, sixteen base threads, and a nested comparison `threads` count of sixteen. Missing evidence before routing fails the run before deletion is enabled. Missing evidence after deletion fails the final measurement and does not replay the delete.

Return `summary.json` and every job `operation-*.json` with the stage and build reports. They survive ordinary staging cleanup.
