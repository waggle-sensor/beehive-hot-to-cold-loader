# Online state backup and disaster recovery

[Documentation](README.md) · [Operations](operations.md) · [Configuration](configuration.md)

Backups do not scale or stop any service. The daemon waits for an archival step to finish, pauses its scheduler and file cleanup, blocks new write grants/late/quarantine submissions, and drains admitted writers. Existing writer renewals/releases and all read-grant operations remain available. It uses SQLite online snapshots on dedicated connections while reads continue, and copies the matching recovery files. Admission and archival work resume before compression, remote upload and verification.

## Configuration and storage

| Environment | Meaning |
| --- | --- |
| `HOT_TO_COLD_BACKUP_SCRATCH` | Separate writable directory/mount, outside the primary state tree; unset disables backup creation |
| `HOT_TO_COLD_BACKUP_URL` | Private sibling `https://host/sage/cold/backups/hot-to-cold-loader`; isolated tests use `/sage/cold-test-<id>/backups/hot-to-cold-loader` |
| `HOT_TO_COLD_BACKUP_KEEP` | Positive integer; default 30 completed backups per source/table identity |
| `HOT_TO_COLD_BACKUP_IMAGE` | Qualified deployed image reference/digest to record in the manifest; keep synchronized with the workload image |

Only the existing admin token can call backup routes. Backup object tokens are scoped to the backup prefix; the existing lakehouse reader stays read-only and its path validation is unchanged. The backup prefix must already be private and its parent directory must exist. These archives are **not encrypted**. Keep signing keys and coordination credentials in separate Secret storage outside `HOT_TO_COLD_STATE`; they are not a backup of those Secrets. Do not place credential files inside the state tree.

The prepared StatefulSet mounts a separate `backup-scratch` claim at `/var/lib/beehive-state-backups`. Local-path requests do not reserve space. Capacity is preflighted for the snapshot plus compressed archive, with metadata headroom. Compression may save little on existing compressed Parquet. Failed snapshots/uploads remain in scratch for diagnosis; monitor capacity and remove only confirmed abandoned generated attempts manually. Successful remote backups remove their generated local attempt directories. Scratch `status.json` remains outside the primary state volume and retains the latest operation and success time across daemon restarts. It is operational information, not a recovery proof.

## Request and observe a backup

From an existing authenticated operational shell, forward the internal Service:

```sh
kubectl --context kubernetes-admin@sage-cluster.local -n shared \
    port-forward service/beehive-hot-to-cold-loader 18001:10001
```

In a separate terminal with `HOT_TO_COLD_ADMIN_TOKEN` already set (do not enable shell tracing):

```sh
curl --fail-with-body --silent --show-error \
    -H "Authorization: Bearer $HOT_TO_COLD_ADMIN_TOKEN" \
    -H 'Content-Type: application/json' -d '{}' \
    http://127.0.0.1:18001/v1/backups/create
curl --fail-with-body --silent --show-error \
    -H "Authorization: Bearer $HOT_TO_COLD_ADMIN_TOKEN" \
    -H 'Content-Type: application/json' -d '{}' \
    http://127.0.0.1:18001/v1/backups/status
```

Create returns HTTP 202 and an operation ID; status reports the latest operation. Only one operation runs at a time. No complete destination/scratch configuration returns 503; a concurrent request returns 409. Request bodies use the same bounded single-object JSON validation as other coordination routes.

The same qualified executable can request and poll a backup for automation:

```sh
beehive-hot-to-cold-loader state-backup request --endpoint http://127.0.0.1:18001
```
A verified backup remains `complete` if only retention cleanup fails; status records `retention_error` and a separate failure metric alerts on it. Do not mistake an older-backup cleanup error for a failed new backup.

| Phase | Meaning |
| --- | --- |
| `waiting_boundary` | Backup accepted; waiting for the scheduler to yield its next archival boundary before capture starts |
| `draining_writers` | New write/late/quarantine admission is closed; admitted writers drain (renewals/releases still allowed) |
| `copying` | SQLite online snapshots and recovery files are captured into scratch under the drain/capture budget |
| `uploading` | Admission is open again; archive is compressed, published to the private backup prefix, and read back |
| `complete` | Off-node verified backup (only this phase counts as success) |
| `failed` | Operation ended without a verified backup; admission is open |

The 10-minute drain/capture budget begins after the scheduler yields its boundary; waiting for a long worker step is separately bounded to 30 minutes. There is a six-hour remote publication deadline. A restart fails any interrupted operation and opens admission; no abandoned pause is carried into the new process.

New write grants and late/quarantine submissions are rejected during capture and receive 503 with `Retry-After: 30`. The ingestion loader leaves deliveries unacknowledged, closes its connection and restarts/retries after the delay. For a mixed hot/late batch rejected **before any hot write was attempted**, it releases the unused permit before replay. A failed/uncertain Influx write never releases a permit as successful just to unblock backup. Healthy existing writers continue renewing.
Expired permits follow the existing expiry/grace policy; expiry is not server-side cancellation. A drain timeout fails the backup and reopens admission.

Each immutable snapshot contains both databases where initialized, jobs/staging, receipts/proofs and an inventory with checksums/modes, original UID/GID and state path, source/table identity, frontier/generation, late inbox cutoff and metadata references. Source symlinks/special files are rejected. Database WAL contents are included through SQLite's snapshot mechanism rather than copied as live files. The manifest is derived from the saved databases, not a later live-state query. A coordinate-only state without an initialized catalog can be backed up only if it agrees with the empty remote publication state.

The remote layout is `<backup-prefix>/<identity-key>/<UTC-id-uuid>/` containing `state.tar.gz`, `manifest.json`, and `COMPLETE.json`. The completion marker is written last after archive and manifest readback verification. Retention accepts only valid markers/manifests from that identity and removes only those known backup objects, never lakehouse files or partial uploads. Metadata references are checksummed; referenced Parquet objects are checked for existence and length, not rescanned in full. This does not replace the pipeline's exact data proofs or an independent backup of the cold data itself.

## Daily schedule and monitoring

`honeyhouse-config/.../beehive-hot-to-cold-loader/backup/` contains a separately deployed CronJob scheduled for **03:00 UTC**, with `concurrencyPolicy: Forbid` and **`suspend: true`**. It uses the existing admin Secret, the operational-client NetworkPolicy label and no Kubernetes API token or scaling RBAC.

The existing metrics endpoint includes `hot_to_cold_backup_*`: configured/running state, admission state/age, last verified success/age, failure count, blocked writes, phase durations and snapshot bytes. Alert rules cover failures, no verified backup within 36 hours (including never-successful configurations), and an overlong admission barrier. A failed scrape is unknown, not successful backup. The Grafana dashboard includes backup success, barrier and phase duration panels.

## Restore after state loss

**This is an offline disaster operation, not a routine redeployment**. Stop the coordinator and all its writer/reader clients, let in-flight requests finish, and preserve damaged state. Do not redirect dynamic clients to stale static routing, start fresh state against an occupied lakehouse, or remove a lock to bypass a surviving process. Restore with the qualified image/version and original UID/GID (or root, which restores ownership to that state owner) at the **original configured state path**, because job records contain absolute local paths. The target must be an empty existing directory. If the volume itself was lost, prepare its replacement separately with the operator. Fence any surviving old coordinator and its native child processes, including on another node or volume. The local writer lock cannot fence a different PVC.

Set `COLD_LAKEHOUSE`, `COLD_NAMESPACE`, `COLD_TABLE_NAME`, `PELICAN_KEY_PATH`, `INFLUX_BUCKET`, `INFLUX_BUCKET_ID`, `INFLUX_ORG` and `HOT_TO_COLD_BACKUP_URL` to the recorded identity/private destination. Obtain `BACKUP_ID` and `BACKUP_IDENTITY` from retained status or the read-only remote `list` command below. Listing works without either original local volume and includes source bucket identity to help select the correct recovery point. Never expose the signing key or bearer tokens in command output. Example commands in a recovery container running as the original owner with the replacement PVC mounted at its original path:

For a bootstrapped lakehouse, the snapshot also records the pinned bootstrap metadata and its remote dependencies. Preserve the original `--bootstrap-lakehouse` and `--bootstrap-before` startup settings; recovery does not copy or replace that read-only source lakehouse.

```sh
# Only on a fresh replacement volume, if the original child does not exist yet:
mkdir -m 700 /var/lib/beehive-hot-to-cold-loader/state
beehive-hot-to-cold-loader state-backup list --bucket waggle
beehive-hot-to-cold-loader state-backup download \
    --identity "$BACKUP_IDENTITY" --id "$BACKUP_ID" \
    --bundle /var/lib/beehive-state-backups/recovery.tar.gz
beehive-hot-to-cold-loader state-backup restore \
    --bundle /var/lib/beehive-state-backups/recovery.tar.gz \
    --state /var/lib/beehive-hot-to-cold-loader/state --confirm-clients-stopped
beehive-hot-to-cold-loader state-backup inspect \
    --snapshot /var/lib/beehive-hot-to-cold-loader/state
beehive-hot-to-cold-loader state-backup approve \
    --state /var/lib/beehive-hot-to-cold-loader/state \
    --confirm-clients-stopped --accept-post-backup-data-loss
```

`restore` validates the archive, checksums and database integrity. It installs a `restore-pending.json` marker before extraction so a failed/partial restoration cannot start the daemon. It refuses nonempty targets, path traversal, links, wrong mount paths and a recovery user that is neither the original owner nor root. A privileged restore normalizes recovered payload/lock ownership to the original state-directory UID/GID so the non-root coordinator can reopen it. Failed targets remain for investigation; there is no automatic overwrite/cleanup of recovery state.

`approve` acquires the same exclusive writer lock, revalidates the snapshot, and reads the current remote publication/reference inventory. It refuses different lakehouse/table identities, publication changes, missing objects and changed metadata/lengths. It does not register a different catalog, write a remote hint, rollback Influx, or discard newer snapshots. Only after successful verification and explicit acceptance of post-backup loss does it durably record approval and clear the marker. Normal and coordinate-only startup refuse a pending marker. With backup configuration enabled, a missing control database also triggers a remote publication check before creating a new journal; already-published history requires restoration instead of silently resetting the frontier.

An older backup is not automatically safe just because both SQLite files open. If publication advanced after backup, leave the coordinator blocked and perform manual reconciliation. Missing acknowledged late measurements accepted after the backup may require an independent replay source; **up to the interval since the last successful backup can be lost**. Failed/missed daily backups enlarge that window. This version does not implement zero-loss replication or automatic stale catalog/journal reconstruction. Existing old reader/writer clients must remain stopped throughout recovery; restored permits retain their recorded deadlines.

After approval, start with deletion disabled and verify status, source identity, hot/cold/cross-border HTTP Arrow and NDJSON queries and lease behavior. Then resume ingestion and separately review deletion enablement. API availability during normal backups does not imply high availability after actual state loss.

## Validation before enabling automation

The offline suite covers WAL snapshots, recovery files, checksums, barrier/read availability, existing writer renewal/release, failure cleanup, archive safety, remote completion/readback, retention scope and stale publication rejection. Run the opt-in real-service fixture on a Docker host with the existing signing key; it creates its own `cold-test-<uuid>` root and never accesses production Influx or `/node-data`:

```sh
python3 beehive-hot-to-cold-loader/testing/live/run.py --backup
```

The fixture backs up before and after late corrections while exercising Arrow, NDJSON, Python client queries and read renewals. After its guarded synthetic Influx deletion, it stops only its own services, preserves their original state, and proves the older restore is blocked specifically by newer publication. It then restores the current backup into empty fixture state at the original mount, approves it, restarts with deletion disabled and compares API results again. Read `run.json`, `reports/online-backup*.json`, `reports/stale-restore.json` and `reports/backup-restored.json`. These reports must pass before unsuspending the CronJob. Measure API p95 latency, WAL/disk growth and capture time on the intended server workload as well; the small fixture does not establish full-day capacity.