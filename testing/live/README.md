# Isolated live archival gate

This runs the workspace loader, archiver, Beehive API, and Sage client against RabbitMQ, InfluxDB, and Pelican. It is the small functional gate. Full-day capacity and production rollout are separate validation work. The [2026-10-27 VM run](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md#small-live-gate-vm-run-2026-10-27) passed with real Pelican readback, late corrections, and guarded fixture deletion.

Run from the workspace root on a host with Docker Compose:

```sh
python3 beehive-hot-to-cold-loader/testing/live/run.py
```

The issuer key defaults to `honeyhouse-config/applications/waggle-access/pelican/issuer-key.pem`. The probe image uses the architecture-matching DuckDB 2.0.x Linux CLI at `beehive-hot-to-cold-loader/bin/duckdb`. Override either with `--pelican-key` or `--duckdb`. Pass `--archiver-image`, `--api-image`, and `--loader-image` together, each as `name@sha256:<64 hex>`, to run those published linux/amd64 images unchanged. The probe is still built for the harness. The default command builds the archiver, API, and loader from the workspace. The archiver image copies DuckDB 2.0.0 and InfluxDB 2.7.7 `influxd` from their official images. Docker builds need the base images, Go and Python dependencies, and DuckDB extensions. The host needs Python 3.9 or newer. The fixture loader and probe run in Python 3.12 containers.

The script prints its private run directory. Read `run.json` and `reports/release-and-finish.json` for the result. Success is `passed: true`, phase `complete`, and a successful final probe. Fixture services stop afterward, including after a failure. A failure keeps state, reports, and containers for inspection. `services_stopped` records teardown. A teardown error fails the run.

## Isolation and storage

Each invocation allocates a UUID, a new Compose project, a new local state directory, and a new managed warehouse:

```text
https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-archive-live-<uuid>/warehouse
```

`--origin https://host[:port]/sage` changes the origin and keeps the generated prefix. The script creates a new warehouse and Compose project on every invocation. Host ports stay unpublished. The stack attaches no existing volumes or brokers. The `waggle` bucket exists only inside this run's Influx container. The observations are synthetic March 1 and March 2 points. The default 30-day hot retention stays in force, and `--archive-before=2025-03-02` limits the archive to March 1.

The signing key is mounted read-only. Tokens are scoped to this run's prefix. SQL reads use read-only tokens, and TLS certificate verification stays on. Source and build snapshots exclude the key, `.git`, state, and caches. Generated service tokens live in `private-config.json`. Share `run.json` and the functional reports. Source-tree hashes in `run.json` identify the workspace code frozen into each build.

Archival deletion is enabled only inside this stack, and only after the remote and client checks pass. An old hot read grant blocks deletion. Releasing that grant lets the archiver reverify and delete the fixture's March 1 rows. Production code, deployments, and deletion defaults stay unchanged. The script leaves remote objects in place and does not touch `/node-data` or an existing demo warehouse. Keep the local state and catalog with their prefix.

Container memory caps total about 10.5 GiB, including the one-shot probe. Leave room for image builds and other workloads. The dataset ends at five logical observations, so this run does not set a full-day memory or disk bound.

## What the run checks

1. Confirm DuckDB 2.0 and a missing version hint at the new prefix, then write and read a small marker on the origin. Initialize a private RabbitMQ exchange and queue, and discover the fresh Influx bucket ID.
2. Start the coordinator in coordination-only mode. Publish three persistent, confirmed broker messages through the loader: two March 1 points and one March 2 point. Check Influx, Arrow HTTP, NDJSON HTTP, and the client's default Arrow request. The values include a float and the string `"123"`.
3. Hold a synthetic write permit and an old hot read grant, then enable archival. After the job is fenced, publish an offline-buffered March 1 observation. Require a durable inbox receipt, a higher broker acknowledgement count, and no ready or unacknowledged messages. The point must be absent from Influx. Restart the coordinator and confirm the receipt, fence, and grants survived, then release the write permit.
4. Wait for the verified archive to route. Record its initial cold metadata from the durable job, acquire a cold read grant, and confirm the fenced late observation is published. Scan that initial metadata independently with native DuckDB against Pelican.
5. Stop the loader and publish two late observations: an overwrite of the original temperature and a new reconnect measurement. Receive one delivery on a real broker channel without acknowledging it, close the channel so it is requeued, observe the redelivery flag and the same body, and requeue it again. Restart the loader and require its acknowledgements and the completed late publication.
6. Compare the complete expected multiset across Arrow HTTP, NDJSON HTTP, and the client's default Arrow response, for the full range and for each day. Check the public four-column wire schema, JSON value and metadata types, metadata with and without `plugin`, zstd IPC, and nanosecond timestamps. The initial cold snapshot must still show its original two points after replacement, and the acquired cold grant must keep the rows it captured. That grant may already include the fenced receipt when the job routed before the probe started.
7. Audit the published schema, layout, and committed remote-verification reports before deletion. Restart the archiver with deletion enabled for this fixture. The held hot read must block deletion across scheduler ticks. Release that grant, wait for the job to complete, and confirm March 1 is empty in Influx while the March 2 point remains. The API and client must still return all five current logical points, and the old cold snapshot must remain readable.
8. Release the old cold grant. Require writers, readers, inbox work, and jobs to be drained. Audit the published Iceberg v3 metadata: `timestamptz_ns`, field 11 VARIANT, field 10 MAP, day partitions, and the bounded-plugin layout properties. Check the persisted full-multiset and SHA-256 remote verification reports for every job, including the reverification immediately before fixture deletion.

The held grants stand in for old readers and writers. The run does not hold a streaming API response open. The redelivery probe uses a real unacknowledged broker delivery. It does not inject an ambiguous Influx write or crash the loader after a completed write.

## Preparation, stopping, and reruns

Prepare a frozen run without Docker or network operations:

```sh
python3 beehive-hot-to-cold-loader/testing/live/run.py --prepare-only
```

Execute that never-started preparation with the printed path:

```sh
python3 beehive-hot-to-cold-loader/testing/live/run.py --prepared /absolute/path/to/state/live-<uuid>
```

Stop only the stack recorded by an interrupted run:

```sh
python3 beehive-hot-to-cold-loader/testing/live/run.py --stop /absolute/path/to/state/live-<uuid>
```

After a failed live run, inspect the stage report and coordinator journal, then start a fresh invocation. Resume is refused once execution has begun, so a new local catalog cannot adopt the same remote warehouse. A prepared run whose Docker preflight failed before any action can still be executed with `--prepared` on a host that has Docker.

## Offline verification

```sh
python3 -m unittest discover -s beehive-hot-to-cold-loader/testing/live -v
```

The two decoder tests need Arrow and Pandas. The other tests use the standard library. They check isolation, the verifiers, and failure handling, including refusal of deletion after a readback failure. They do not close the live gate. The full-day run measures the archival lifecycle against realistic source data and resource budgets.

## Archiver execution policy

Fresh runs fix the native thread policy. Normalization uses sixteen threads, merge fan-in sixteen, and one merge worker. Build materialization and per-file comparison use `--duckdb-build-threads=16`. Build batch sort and copy use `--duckdb-build-sort-threads=2`. Fresh reconciliation, commit, and pre-delete comparisons use sixteen threads on a private session copy. `--duckdb-threads` is not a service flag. Canonical shard reuse is required, and the first comparison partition count is fixed. The same native memory setting applies throughout.

`run.json` records `execution_policy`: those thread counts, canonical reuse, fixed partitioning, and the `tag_dictionary`, `raw_tag_dictionary`, and `local_dictionary` canonicalization policies. `export-dedup.json` records the normalization policy the job actually used, and the live probe checks it before fixture deletion. `expected-profile.json` and `packing-profile.json` record the threads the build used. An existing prepared directory keeps its frozen commands. Prepare a fresh run to validate this policy. Preparation does not modify a deployed service.
