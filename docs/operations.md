# Operations and recovery

[Documentation](README.md) · [Metrics](metrics.md) · [Validation](validation.md)

## Persistent state and ownership

A managed prefix must have exactly one catalog and state directory; never run independent state directories against the same warehouse. Native child processes inherit the writer lock so a restart cannot race an orphaned staging writer. If the lock remains held after a crash, inspect and stop the prior child process before restarting; do not remove the lock file to bypass it.

State includes the control journal, SQL catalog, late inbox, quarantine and job staging. Back it up consistently while the daemon is stopped (or using SQLite backup for both databases plus their corresponding files). Do not delete a journal to retry a job. No snapshot expiry or object GC runs in this initial version. Transient local data is removed only after durable job completion; JSON journals and verification reports remain. Use `--keep-staging` to retain data for debugging; cleanup resumes on the next startup when the flag is removed. Completed-job performance profiles have a separate `--profile-retention` setting (default 30 days). Use `--profile-retention=0` for indefinite profile retention without retaining staging data; `--keep-staging` also preserves profiles. Recovery/verification evidence and legacy profiles with unknown completion times remain preserved. Monitor local and remote capacity.

## Status and monitoring

The public `/metrics` endpoint reports durable outcomes, current job states, attempt timings/errors, backlog, coordination and Go process health. See [metrics, Grafana and profile retention](metrics.md) for the scrape configuration, dashboard/provisioning and alert rules. Scraping requires no authentication; coordination and status routes remain authenticated.

```sh
curl --fail -H "Authorization: Bearer $HOT_TO_COLD_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"waggle"}' http://127.0.0.1:10001/v1/status
```

Alert on archival lag, pending/old late receipts, jobs with errors, permits or read grants that do not drain, queued deletions, and local disk capacity. The SQLite `jobs.payload` retains the last stage/error; `permits` and `readers` identify blocked owners. Restrict access to this private state.

## Reader leases

Read grants use renewable leases with `--read-lease-duration` (default `30m`, positive, at most `24h`). This is the time allowed without successful renewal, not a maximum query duration. The API renews in the background every minute or one-third of the remaining lease, whichever is shorter, including while a query is being prepared or streamed. Healthy queries can continue beyond the original lease. Each renewal preserves the same pinned frontier and Iceberg metadata. Its confirmed expiry is the coordinator's renewal time plus the lease duration (never shortened if an existing deadline is later); no extra lease duration is added after expiry.

If the API crashes, closes/cancels its request, or cannot renew before the last confirmed `expires_at`, renewal stops and the query is canceled. Temporary renewal errors retry within that confirmed window; an expired or released grant cannot be revived. Expired readers stop blocking hot deletion after a further one-minute drain grace. Grant deadlines persist across archiver restarts. New leases and renewals use the configured timeout, but renewal never shortens an already confirmed deadline. Legacy grants receive one timeout window at migration. Expired rows remain in the journal for diagnosis/idempotency until released or explicitly resolved. Status/metrics report active (including grace) readers and `readers_expired` separately.

Renewal uses a small authenticated `/v1/reads/renew` request and journal update; it does not scan data or run on the row/batch path. It adds background coordinator load, so monitor renewal errors and query latency under concurrent workloads. Successful result closure still releases the grant immediately. If closure is uncertain, renewal stops and automatic expiry bounds the deletion delay.

Clients must retry queries canceled by lease loss using a new request, obtaining current routing. Streaming failures abort the HTTP response rather than treating partial output as a successful extract. Keep API/coordinator clocks synchronized; the grace is a drain allowance, not proof that a paused process or remote query has terminated. This availability policy lets deletion progress after expiry despite uncertain closure, without assuming Influx provides query-wide snapshot isolation. A still-active but stuck request can keep renewing; this is crash recovery, not progress-based stall detection. Existing Influx/client timeouts and caller cancellation still apply.

Deploy the renewal-aware API and coordinator together before enabling deletion. Drain/stop old API instances first. Deploy the renewal-aware Influx loader with writer leases as described below.

## Writer leases and stopped writers

Writer permits use `--write-lease-duration` (default `30m`, positive, at most `24h`). Acquire returns `expires_at`. The Influx loader renews in the background every minute or one-third of remaining time, whichever is shorter. Renewal keeps the original hot-day admission, including after a fence appears. New/renewed expiry is coordinator time plus the duration, never shortening an existing deadline. Acquire retries and restarts do not extend it; expired/released permits cannot renew.

On failed writes or lease loss the loader stops renewing, does not release an uncertain batch as successful, and does not acknowledge its RabbitMQ messages. The failure exits consumption so broker redelivery can acquire fresh admission and route fenced days through the late inbox. A crashed writer stops blocking archival at its last confirmed expiry plus a fixed one-minute drain grace. Expired rows remain for diagnosis and reject ID reuse; status/metrics distinguish `writers` (including grace) and `writers_expired`.

Expiry is an availability policy, not server-side cancellation. An Influx request already in flight can still complete after lease loss. The loader checks the lease before hot IO and before release, uses a finite client transport timeout, and keeps uncertain deliveries unacknowledged for replay. Keep broker delivery durability, automatic consumer restart/redelivery, and synchronized clocks operational. A paused or hung Influx request can outlive the grace; strict exclusion of all stale server-side writes requires enforced Influx-side deadlines or fenced write ingress. Do not interpret the lease as that guarantee.

Deploy the renewal-aware writer and coordinator together, draining old writer processes before archival resumes. Legacy permits receive one persisted migration window and are not extended on restart. Use finite Influx transport timeouts comfortably below the lease and grace budgets. Healthy long batches can renew beyond the initial deadline; renewal is not progress-based stall detection.

Offline owner resolution remains available to release a confirmed stopped writer sooner. Confirm its pod/process is stopped and outstanding Influx requests have finished or been cancelled. Stop the archiver, then rerun its normal configuration with:

```sh
--resolve-stopped-owner '<exact owner from the grant ledger/log>' --confirm-owner-stopped
```

This resolves that owner's grants offline under the same exclusive process lock; it performs no Pelican IO or deletion. Never use it while the owner can still write or read. Restart normally; a fenced day is reconciled again before it can be deleted. Otherwise, the configured expiry and drain grace bound the wait automatically.

## Late data and historical coverage

Late arrivals are durably accepted before broker acknowledgement. Their original nanosecond timestamps are retained. Corrections replace the affected day's files atomically; old snapshots remain readable. Late input before the configured coverage start remains queued pending an explicit coverage/backfill operation; it is not silently dropped or exposed through an unverified historical range. History before the configured start requires a separate explicit backfill; changing the durable start in place is refused.

## Diagnose stalled work

| Symptom | Check / action |
| --- | --- |
| Job stays in `FENCING` | Inspect writer deadlines and renewal; wait for release or expiry plus grace, or resolve only confirmed stopped owners. |
| Job stays in `ROUTED` | Check deletion is enabled and old read grants have drained. Routing alone does not delete hot data. |
| Capacity error | Free or add staging capacity without removing journal/proof evidence; capacity retry waits 15 minutes. |
| Other failed attempt | Inspect the persisted job error and stage reports; normal retry waits one minute. |
| Repeated reconciliation mismatch | Investigate engine/query consistency and bypass writers; the runner re-exports under the fence. |
| Failed `/metrics` scrape | Treat state as unknown; check the process and journal access rather than assuming zero backlog. |
| Late receipt remains pending | Check its day is in declared coverage and that earlier work/retries are not blocking scheduling. |

Every worker step has a six-hour deadline. Work is resumed from durable state; restart with the same state, warehouse, source identity, and coverage settings. Do not force a state transition or remove a database to skip a failing proof.

The engine export and independent query must refer to the same source. A successful publication is insufficient for hot deletion: fresh remote proof, routing, reader drain, another pre-delete proof, and post-delete emptiness checks remain required. Disk reclamation by Influx compaction can lag logical deletion.
