# Validation and performance evidence

[Documentation](README.md) · [Developer guide](development.md) · [CI/CD](ci-cd.md) · [Harness index](../testing/README.md)

Commands below run from the service root unless a linked harness says otherwise. Use dedicated fixtures and fresh isolated storage; native gates do not all run in the default unit suite. Detailed historical measurements remain in [the archival plan](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md).

### Representative full-day capacity

The [full-day capacity guide](../testing/capacity/README.md) clones a pinned real historical day read-only into fresh isolated storage and measures the entire archival lifecycle. It includes full source/destination multiset comparison before fixture deletion, exact worker timings, resource/disk sampling and bounded API/client checks. The accepted implementation passed representative full-day VM lifecycles before migration; validate the relocated build with a fresh run before rollout. Choose a fresh `--run-dir` on a filesystem with sufficient capacity; a new folder on the same volume does not increase its free space.

`--profile-steps` enables atomic per-attempt wall/CPU and lifetime RSS reports under each private job directory. It is disabled by default and does not alter durable transitions or deletion guards. The guide describes metric scopes and the distinction between preparation, extra reference validation and worker cost.

### Small live end-to-end gate

The [live gate operator guide](../testing/live/README.md) provides a single command to build isolated fixture services from this workspace and exercise the actual loader/broker, Influx, archiver, real Pelican and managed API/client together. It creates a new managed prefix and uses synthetic data. It checks fenced late receipt durability across restart, broker redelivery, snapshot replacement, both HTTP formats, client Arrow decoding, and deletion blocked by an old read grant before releasing that grant. Deletion is enabled only for its own fixture bucket after the preceding checks pass; production defaults remain unchanged. The [small live gate passed on the VM](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md#small-live-gate-vm-run-2026-10-27), including real Pelican readback, late corrections and guarded fixture deletion. Representative full-day lifecycles also passed; production reader/writer coordination and deletion remain deployment-specific rollout requirements.

### Bounded export normalization

Engine export streams directly to `export.parquet`, including monotonic replay sequence, without retaining a day-sized Go map. Normalization canonicalizes the complete tag map and routes records by `(time, name, tags)` into 16 private partitions. Value, kind and sequence are excluded from routing so all versions of an identity meet. Oversized partitions are refined with further hash bits; only partitions of at most 1,000,000 input rows are sorted by identity and sequence descending. A streaming collapse keeps the greatest replay sequence.

Survivors are merged into chronological identity order with at most sixteen open readers and 2,048-row Arrow batches per reader. Intermediate passes bound fan-in when there are many partitions. Input/partition/merge row counts and ordering are validated; duplicates across supposedly disjoint winners fail closed. A partition still oversized after all hash bits is a capacity failure. The final `export-dedup.parquet` preserves the latest value, kind, full metadata, replay sequence and exact nanoseconds; it still must match an independent Influx query before building or publishing.

DuckDB uses the configured memory/spill guards for routing and bounded sorts. Go holds bounded Arrow buffers, merge cursors, a small heap and Parquet metadata. Row width and footer size still affect memory; neither the native buffer limit nor row-count caps constitute a process RSS guarantee. Budget for compressed raw/query/normalized staging, partition/sort/merge scratch and spill in addition to subsequent build artifacts. `export-dedup.json` records counts, strategy, observed partition cap, merge fan-in/passes and phase timings. Artifact byte counters are physical/cumulative sizes, not simultaneous peak staging space.

Normalization publishes atomically after a complete read, flush and fsync. Failures retain the fence and prevent progression. Scratch files are reclaimed on retry; completed job cleanup removes normalized data and retains its JSON report. An older `RECONCILED` job without normalized data rewinds to reconciliation; an unsequenced raw export requires re-export. Completed build manifests remain reusable.

Run the resource harness alone in a precompiled test binary, in a fresh local directory outside `/node-data`. It contacts neither Influx nor Pelican:

```sh
go test -c -o /tmp/archive-storage.test ./internal/storage
mkdir -p state
SAGE_ARCHIVE_DUCKDB2=/absolute/path/to/duckdb \
SAGE_ARCHIVE_SCALE_ROWS=2000000 \
SAGE_ARCHIVE_SCALE_MEMORY=512MB \
SAGE_ARCHIVE_SCALE_DIR="$PWD/state/export-scale-2m-$(date +%s)" \
  /tmp/archive-storage.test -test.run '^TestNativeExportDedupScale$' -test.v
```

Repeat with 20 million and approximately 140 million unique points, each in a fresh directory and test process. Ten percent of identities have an overwrite; reversed timestamps exercise partitioned sorting and chronological merging. Every retained record is checked. `export-scale.json` records elapsed times, separate Go/largest-child peak RSS, the last native sort's profile buffers/spill and sampled staging file bytes. The profile is not a maximum across all partitions; process RSS and staging sampling cover the complete normalization. RSS values are not a simultaneous combined peak; staging sampling can miss short-lived or unlinked files. A reported cgroup peak covers the container's lifetime; use a fresh container to interpret it as this run's peak. These measurements cover export parsing/normalization, not live `influxd`, full reconciliation, packing or remote verification. Run the matching-engine and native lifecycle gates too.

### Bounded exact verification and disk guards

All production `DuckDB.Equal` callers (source reconciliation, batch/build checks, remote readback and pre-deletion re-verification) now stage canonical primitive Parquet records locally. Each source is counted and canonicalized, with complete nanosecond time, name, sorted full tags, native VARIANT kind and JSON value. A hash of the full record routes rows into 16 private scratch partitions. Large partitions are refined using the next four hash bits until each comparison has at most 1,000,000 rows per side. Both directions of `EXCEPT ALL` still compare actual records and duplicate multiplicities. Matching hashes/counts alone never pass verification. If a partition still exceeds the cap after all 64 hash bits, verification fails closed with a capacity error. These scratch partitions do not change the published daily warehouse layout.

Fresh exact comparisons use sixteen threads, a 65,536-row partition flush threshold and at most 256 open files, accommodating 16 partitions per thread. Only the original canonicalization reads the source data; recursive refinement/exact comparisons read local files. Count prechecks may additionally read source metadata/data. Comparison scratch belongs to a fresh private directory and is removed when that comparison returns. A process crash can leave it for operator inspection; no existing run directory is automatically adopted or removed.

Ordinary archive builds preserve the chronological order already established by normalization, with explicit insertion-order preservation through the single- file projection/COPY. This avoids globally sorting expanded MAP/VARIANT records again. Bootstrap merges and late replacements still explicitly sort by time. The published 10-million-row chronological batches, plugin/vsn/time order and 262,144-row plugin-pure groups are unchanged.

Native commands require an 8 GiB free staging reserve and cap DuckDB spill at the smaller of 16 GiB and current free space minus that reserve. Streaming Parquet writers recheck space when flushing bounded batches (65,536 rows or about 8 MiB of record payload, whichever occurs first). These are safeguards, not a quota or proof of a total disk/RSS bound: output files, wide records, concurrent users and metadata consume additional resources. Capacity failures keep a reconciled export at `EXPORTED` under its fence instead of forcing another export. Scheduler capacity retries wait 15 minutes after the failed attempt; other failures wait one minute after completion. Actual data mismatches retain the existing re-export behavior.

Before another full-day benchmark, run the native gates on the VM:

```sh
SAGE_ARCHIVE_DUCKDB2=/absolute/path/to/duckdb \
  go test -count=1 -v ./internal/storage \
  -run 'TestNativeCanonicalPartitionComparison|TestNativeLifecycle|TestNativeBoundedPluginPacking'
```

The comparison gate checks duplicate multiplicities, changes to every canonical field, a deliberate routing-hash collision and recursive refinement over 17 million generated rows at a 128 MB DuckDB buffer budget. It uses fresh local scratch only. Also rerun `TestInfluxEngineConsistency` below: it now uses the production compressed export/query/normalized path. These are separate from a real full-day Pelican lifecycle capacity result.

Resources: [DuckDB partitioned writes](https://duckdb.org/docs/2.0/data/partitioning/partitioned_writes), [configuration](https://duckdb.org/docs/2.0/configuration/overview), and [order preservation](https://duckdb.org/docs/2.0/sql/dialect/order_preservation).

### Native lifecycle

```sh
SAGE_ARCHIVE_DUCKDB2=/absolute/path/to/duckdb go test -count=1 -v ./...
```

`TestNativeLifecycle` uses real DuckDB 2.0 and Arrow/Iceberg libraries with a local TLS object fixture, simulated Influx responses, and a temporary engine-export fixture. It covers an initially empty day, append, remote verification, deletion, a late correction, and an old pinned snapshot. It never contacts real Pelican. Its self-signed TLS exception exists only inside that test. Local listener permission is required.

Separately prove the running Influx engine exporter against query-visible state under WAL writes, overwrites and compaction, verify both HTTP response formats, and run a real isolated Pelican publication. These are rollout gates, not claims made by the offline unit suite.

### Matching Influx engine gate

Use a fresh local directory for the dedicated fixture, outside `/node-data`:

```sh
export HOT_TO_COLD_FIXTURE_ROOT="$PWD/state/source-fixture-$(date +%s)"
mkdir -p "$HOT_TO_COLD_FIXTURE_ROOT"
docker compose -f testing/docker-compose.yml up -d --force-recreate --wait
SAGE_ARCHIVE_FIXTURE_URL=http://localhost:18087 \
SAGE_ARCHIVE_FIXTURE_TOKEN=archive-fixture-only-token \
SAGE_ARCHIVE_FIXTURE_ENGINE="$HOT_TO_COLD_FIXTURE_ROOT/engine" \
SAGE_ARCHIVE_INFLUXD=/absolute/path/to/influxd \
SAGE_ARCHIVE_DUCKDB2=/absolute/path/to/duckdb \
SAGE_ARCHIVE_FIXTURE_REPORT_DIR="$PWD/state/engine-gate-$(date +%s)" \
  go test -count=1 -v ./internal/storage -run TestInfluxEngineConsistency
```

This recreates only the named dedicated fixture service. Use a host/container layout exposing its engine at the provided path, with the matching InfluxDB 2.7.7 executable. Stop it afterward with the same Compose file. Keep the fresh report directory outside the engine mount; it must not already exist.

The test creates one unique `sage-archive-fixture-*` bucket and checks:

- A WAL-only overwrite before any TSM exists, then an older value proven present in a private TSM-only copy before a new WAL overwrite. Both old and new replay rows must be observed, and normalization must retain the new value.
- A second snapshot containing all 60,000 filler points and the overwritten value. A subsequent measurement delete must create a real TSM tombstone. Export of a captured WAL-free tombstoned view must omit the deleted point. Physical replacement of the old TSM/tombstone files then proves cleanup, regardless of the compaction level chosen by Influx.
- A separate full-compaction phase: rewrite an existing filler point with the same value, wait for another tombstone-free snapshot generation, then require completed full compaction for the exact test bucket/shard. Check the duration histogram count, failure count, active gauge and replacement of that phase's input files. A level-1 cleanup, cache snapshot or another shard's compaction cannot pass. The rewrite preserves the expected logical dataset.
- Full export/query/independently generated expected-multiset equality at every data phase. The post-compaction target day has 60,007 points.
- Five explicit boundary instants: `start - 1 ns`, `start`, `end - 1 ns`, `end`, `end + 1 ns`. After day deletion the target day must be empty, the previous day must retain its one boundary point, and the next day must retain both.

The fixture uses a 10-second snapshot cold duration and 30-second full-compaction cold duration. Each readiness wait has a two-minute deadline; it observes state rather than assuming a fixed sleep completes a snapshot. Missing evidence fails the gate. If a heavily throttled VM advances past the WAL-only phase before it can be captured, increase the fixture snapshot cold duration and rerun; that failure does not establish coverage of the phase.

`engine-consistency.json` records pass/fail, last stage, per-phase row counts, file inventories and scoped full-compaction metrics. Persistent report directories also retain compressed staging Parquet, small expected NDJSON fixtures, and the captured tombstoned engine for inspection. Without `SAGE_ARCHIVE_FIXTURE_REPORT_DIR`, artifacts use Go's temporary test directory. Failed buckets remain for inspection; only this test's bucket is removed on success. The test performs explicit deletes in that bucket, without enabling production archival deletion. Real Pelican publication, a representative full-day lifecycle and public HTTP Arrow/NDJSON checks remain separate VM rollout gates.

The evidence follows Influx's documented [compaction metrics](https://docs.influxdata.com/influxdb/v2/reference/internals/metrics/) and [engine export interface](https://docs.influxdata.com/influxdb/v2/reference/cli/influxd/inspect/export-lp/).

The same DuckDB environment enables `TestNativeBoundedPluginPacking`, which checks native VARIANT encoding, nanosecond instants, the 262,144-row cap and plugin purity on a mixed-plugin file with a large plugin and several small groups.

### Packing performance

The worker uses a separate DuckDB session copy for builds:

| Setting | Default | Scope |
| --- | ---: | --- |
| Fixed normalization/base policy | 16 | Normalization and base native work |
| Fixed fresh comparison policy | 16 | Reconciliation and commit/pre-delete exact comparisons |
| `--duckdb-build-threads` | 16 | Build materialization and per-file/whole-build exact comparison |
| `--duckdb-build-sort-threads` | 2 | Build batch sort/copy |

Normalization uses fan-in sixteen and one merge worker. `export-dedup.json` records actual native threads, configured/maximum fan-in, concurrency and passes. Build thread overrides accept 1–256. Normalization has no thread/fan-in override. Fresh exact comparisons always use sixteen threads on a private session copy, independently of the base setting. Builds retain the same native memory/spill settings, executable, credentials and process lock as the base session. The Go packer remains sequential. `expected-profile.json` and `packing-profile.json` record actual build threads and substep timings.

Build verification always uses canonical staging reuse with fixed sixteen-way initial partitioning. Oversized comparison buckets are recursively refined; adaptive initial partitioning has been removed. Successful per-file proofs retain canonical shards for a new whole-build exact comparison. SHA-256 bindings, shard inventories and contiguous source-window coverage are checked. Reuse is private to one build; remote and pre-delete verification still prepare fresh comparison data from published objects. The old opt-in and fresh-build benchmark alternatives have been removed.

### Build-window scan profiles

`TestNativeBuildScanGate` and `TestNativeRetainedBuildScan` check window bounds against every overlapping row group on the first, middle, and final packing windows. A test-only timestamp bound derived from physical row-group statistics is compared with the unchanged ordinal filter, and each result is verified against the original window. Running the diagnostic leaves build policy and exact proofs unchanged.

[Canonical dictionaries](architecture.md#canonical-dictionaries) are the build, reconciliation, and remote-verification helpers. `packing-profile.json` records `canonicalization: tag_dictionary`. Reconciliation profiles record `canonicalization: raw_tag_dictionary`. `COMMITTED` and `DELETING` profiles record `local_dictionary` and keep original `Canonical` SQL on the Iceberg scan. Fresh manifests record `build_canonicalization: tag_dictionary`, `reconciliation_canonicalization: raw_tag_dictionary`, and `verification_canonicalization: local_dictionary`. Accepted measurements for these paths remain in [plan 12](https://github.com/seanshahkarami/sage-data-rework/blob/main/plans/12_dynamic_archival_service.md).

### Reconciliation and deletion substeps

Each reconciliation, verification and delete attempt retains an atomic `operation-<operation>-*.json` report in its job directory. Reports identify the job/day/state, base DuckDB threads/memory, elapsed time, CPU deltas and failed substep. Reconciliation separates normalization, independent Influx extraction and fresh exact comparison. Verification separates remote object readback, read setup, fresh exact comparison, optional hint publication and journal work. Deletion separates source validation and the HTTP delete request. The `DELETING` state has separate verification and deletion reports.

Exact comparison details record the proof's actual `threads` separately from the operation's base `threads` (both normally sixteen), alongside count prechecks, initial canonical staging, recursive refinement and exact SQL. These are nested in step wall times and must not be added again. CPU metrics are self/reaped-child deltas; self includes other concurrent goroutines. Report serialization is outside the operation timer but inside the enclosing stage. Reports retain failed/retried attempts, contain no SQL/error text or credentials, and survive ordinary data cleanup. A report write failure is logged without retrying a completed external operation. The capacity harness independently rejects missing/incomplete evidence.
