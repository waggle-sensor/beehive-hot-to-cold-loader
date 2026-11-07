# Deployment

[Documentation](README.md) · [Configuration](configuration.md) · [Operations](operations.md)

Build the service and supply its native dependencies as described in the [repository README](../README.md). Use one active coordinator, persistent private state, a read-only Influx engine mount, and a fresh managed warehouse prefix. The coordinator must query the same Influx bucket whose engine it exports.

## Rollout sequence

1. Start with an isolated test bucket and a fresh managed prefix. Start the archiver in `--coordinate-only` mode while migrating loaders; this serves grants and durable receipts but creates no new fences or archival jobs. Existing durable fences are preserved on maintenance restarts. Mount Influx's engine read-only; use matching Influx binaries. On Kubernetes, a sidecar in the Influx pod can share its engine PVC without a second RWO attachment.
2. Expose port 10001 only internally, with TLS termination and network policy. Separate loader, reader and admin tokens. There is no public control API.
3. Set loader `HOT_TO_COLD_LOADER_URL`, `HOT_TO_COLD_LOADER_LOADER_TOKEN`, and unique `HOT_TO_COLD_LOADER_LOADER_OWNER` (pod UID prefix; a process UUID is appended automatically). Update every writer of the selected bucket, including imports and other writers targeting `waggle`. The current recent-loader targets the separate `waggle-recent` bucket and must not use this coordinator. Remove bypass access before asserting `--writers-coordinated`. Do not run old and new loaders against the same archival bucket during fencing.
4. Set API `COLD_FRONTIER=dynamic`, `HOT_TO_COLD_LOADER_URL`, `HOT_TO_COLD_LOADER_READER_TOKEN`, unique `HOT_TO_COLD_LOADER_READER_OWNER` (pod UID), and `COLD_WAREHOUSE` pointing to the managed warehouse. Its reader token must match this service's `HOT_TO_COLD_READER_TOKEN`. Dynamic mode obtains its frontier per request; leave `COLD_BORDER` unset. The Influx loader retains `HOT_TO_COLD_LOADER_URL` with separate loader-role token/owner settings.
5. After all selected-bucket writers participate, restart without `--coordinate-only` and with `--writers-coordinated`. Complete source-consistency and native lifecycle tests, then run the intended workload with deletion disabled. Compare Arrow and NDJSON against the source.
6. Drain/stop static API readers and verify all writers/readers participate. Only then restart with `--delete-enabled --readers-coordinated`.

Use `--archive-before 2025-03-02` to bound an initial March 1 validation run. This limits creation of new archival jobs; existing jobs, late corrections and verified pending deletions remain recoverable. Retaining staging while deletion is disabled can consume substantial disk, so start with a bounded run.

The two booleans are operator attestations, not discovery of Kubernetes network policy. The service cannot prove that an independent administrator holds no bypass credential. Deployment enforcement is required.

## Bootstrap an audited warehouse

An optional bootstrap source is read-only and must already have audited coverage. Provide both `--bootstrap-warehouse` and `--bootstrap-before`, with the latter an exclusive UTC date after `--archive-start`. The source must differ from the managed destination. Its metadata snapshot is pinned in durable configuration.

For each covered day, the service merges that snapshot with verified hot rows; hot values win for matching observation identities. It rewrites the adopted layout into the managed prefix and verifies the result. It does not point the new catalog at an unverified existing prefix or modify the source warehouse.

Changing bootstrap configuration on an existing state directory is refused. See [configuration](configuration.md) before initializing persistent state.

## Writer lease rollout

Drain existing Influx loader processes and deploy the renewal-aware loader with the coordinator before resuming archival. Do not leave old clients writing under an expiring permit they cannot renew or check. Existing permit rows receive one persisted `--write-lease-duration` migration window; reopening does not renew them. Use durable broker delivery/redelivery and finite Influx transport timeouts. See [writer lease recovery and limitations](operations.md#writer-leases-and-stopped-writers).

## Value-encoding rollout

A new managed table is created with `sage.value.encoding=variant-uint64-decimal-v1`. An existing destination without that marker refuses to start, so the two encodings stay in separate tables. Prepare a fresh isolated managed prefix and a fresh state directory. When the history has already been audited, bring it across with the bootstrap source. The old warehouse stays as it is: the service does not rewrite it, and labeling its files unsigned by hand does not mark them. State recorded for another warehouse stays with that warehouse.

A bootstrap source is accepted when it declares this convention, or when it is unmarked and holds only native float, int, bool, and string values. An unknown marker is rejected, and so is a decimal in an unmarked source. Once the new files exist, deploy a reader that understands the marker before sending aggregation traffic to them. The new prefix does not rewrite a warehouse that is already published, and it does not restore an unsigned kind that native Parquet has already dropped.
