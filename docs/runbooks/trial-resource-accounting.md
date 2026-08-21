# Trial resource accounting

Loom persists resource usage per trial attempt and execution container in
`trial_resource_usage`. This evidence is separate from configured limits,
Prometheus process health, provider billing, and Slurm requested resources.

## Semantics

- Identity is `(trial_id, attempt_count, execution_key)`. Worker replay is an
  idempotent upsert; cumulative counters and sampled peaks never decrease.
- `complete` means a final observation was collected before container removal.
  `partial` means some observations exist but final collection or delivery was
  interrupted. `unavailable` means the backend exposed no supported resource
  telemetry. Missing or unsupported fields are `null`, never fabricated zero.
- CPU usage/throttling and block-I/O counters are cumulative per container.
  `memory_peak_bytes` and `pids_peak` are maxima observed for that container.
- The multi-container projection exposes
  `memory_peak_upper_bound_bytes`/`pids_peak_upper_bound`. These sum individual
  peaks and are conservative upper bounds, not synchronized whole-trial peaks.
- The worker stores only a SHA-256 hash of the runtime/container identity. It
  never persists environment variables, commands, host paths, credentials, or
  unbounded backend payloads.

Docker workers sample Docker's cgroup-backed stats. Daytona and Modal currently
persist typed `unavailable` records because their Loom adapters do not expose a
stable CPU/RSS/PID/I/O contract. Adding provider telemetry requires a new typed
adapter implementation and contract tests; it must not map billing duration to
resource use.

## Durability and recovery

Each active execution checkpoints its latest report under the worker's private
`trajectory_cache_dir/resource-usage-outbox`. Files are mode `0600` in a mode
`0700` directory, atomically replaced, capped at 64 KiB each and 10,000 entries.
Normal teardown stages the final report before removing the container, sends an
idempotent Control Plane `PUT`, and deletes the file only after acknowledgement.
At worker startup, unfinished reports become final `partial` records with
`worker_restart_before_finalize`; acknowledged reports are then removed.

Collection and delivery failures do not change trial correctness. Operators can
alert on:

- `loom_worker_resource_accounting_events_total{result,reason}`
- `loom_worker_resource_accounting_outbox_backlog`

## Read and export surfaces

- `GET /api/v1/trials/{trial_id}/resource-usage`
- `GET /api/v1/batches/{batch_id}/resource-usage`
- delivery archives: `ledger/resource_usage.jsonl`
- raw Harbor delivery archives:
  `agent_runs/<task>/<trial>/resource_usage.json`

These surfaces use the parent trial/batch team authorization. A legacy trial
returns `items: []` plus aggregate `telemetry_status: unavailable`.

## Capacity calibration

Do not change slot limits from a small smoke. Collect at least 1,000
representative trials over at least two weeks and include one 100-200 concurrent
batch. Exclude or separately report trials whose telemetry is not complete.
Group by workload class, architecture, backend, image/candidate and resource
profile. Report P50/P95/P99/P99.5 for container memory peak, conservative trial
memory upper bound, CPU usage per wall second, CPU throttled ratio, PID peak and
I/O. Also report OOM/OOM-kill counts and telemetry completeness.

For memory, a starting sizing rule is `ceil(P99.5 * 1.25)` within each workload
class, followed by a fixed-candidate concurrency validation. CPU changes must
also show that throttling and end-to-end latency remain within the accepted
service objective. Live deployment and calibration runs require separate
authorization; a merged migration does not prove collection is active.

## Rollback

Disable writers/read projections first and drain or explicitly preserve every
outbox file. Migration 0107 refuses downgrade while accounting rows exist so
evidence cannot be silently dropped. Repository rollback is not authority to
delete live telemetry or modify a deployed database.
