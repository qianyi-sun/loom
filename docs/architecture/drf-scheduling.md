# Scheduling (DRF + claim query)

How service-mode workers pick the next trial to run. CLI mode has no
scheduler — `loom run` runs whatever you asked for in submission
order.

This page documents the current reusable-worker claim path. The accepted
terminal path uses frozen workload requirements, execution classes, durable
leases, environment bindings on a shared cluster, and Kubernetes execution units as specified in
[Nebius service execution](nebius-service-execution.md). During migration,
legacy `requires_caps`, backend, and pool fields are compatibility evidence;
they cannot silently satisfy or weaken the versioned admission contract.

## The model: claim, don't push

Workers **poll** the Control Plane for trials; the Control Plane
does not push. The claim is a **single SQL query** with
`FOR UPDATE SKIP LOCKED` that atomically transitions one trial from
`queued` to `claimed` and returns it. No worker assignment table,
no leader election, no per-team queues. Every concurrency-safe
property follows from Postgres's row locks.

Why polling + skip-locked:

- **Workers are stateless** — they can crash and restart at any
  time; the scheduler doesn't track which trials they're working
  on (the trial row's `worker_id` is the only durable record).
- **One query = one decision** — no two-phase commit, no "I picked
  this trial, please confirm" round-trip. The trial either claimed
  or didn't.
- **Skip-locked makes concurrent claims race-free** — N workers
  polling at once each get a different row or `None`. Postgres
  handles the contention.

## The ordering

Every poll runs this ordered query:

```sql
SELECT t.id
  FROM trials t
  JOIN team_quotas q ON q.team_id = t.team_id
 WHERE t.state = 'queued'
   AND t.attempt_count < q.max_attempts_ceiling
   AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
   AND t.requires_caps->>'os'         = ANY(:worker_os)
   AND (
     COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
     OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches)
   )
   AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
   AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
 ORDER BY
     (q.in_flight_count * 1.0) / NULLIF(q.fair_share_weight, 0) ASC,
     t.submit_priority DESC,
     t.submitted_at ASC
 LIMIT 1
 FOR UPDATE OF t SKIP LOCKED
```

Three ordering keys, most → least important:

1. **DRF tie-break** — `in_flight_count / fair_share_weight`
   ascending. A team using less than its share goes first; a team
   already saturating its share goes last. The `weight` is the
   policy knob; the `in_flight_count` is reality.
2. **`submit_priority`** descending — explicit per-trial bump
   (default 0; admins can raise).
3. **`submitted_at`** ascending — break remaining ties FIFO so the
   queue never starves an old trial.

## Why DRF (not strict FIFO, not strict priority)

| Scheme              | Failure mode under load                                |
|---------------------|--------------------------------------------------------|
| Strict FIFO         | One team's bulk submission starves everyone else       |
| Strict priority     | High-priority team starves low-priority indefinitely   |
| Round-robin / team  | Two trials from the same team must wait on rotation, even if no other team is queued |
| **DRF**             | Each team's share scales with what they configured; spare capacity flows to the team that needs it; no starvation |

DRF makes one decision: *"of all teams with eligible trials, which
team is furthest under their share?"* — and picks the oldest
high-priority trial from that team. Spare capacity (no other
team queued) flows back to whoever is queued, regardless of share.

Quota knobs (`team_quotas` table):
- `fair_share_weight` — relative weight. Two teams at weight 1 +
  weight 3 saturate the cluster at 1:3 ratio when both are queued.
- `in_flight_count` — maintained by the Control Plane on claim +
  release. The denominator in the DRF expression.
- `max_attempts_ceiling` — admin's per-team ceiling that gates
  claim (`attempt_count < max_attempts_ceiling`). Seeded from the
  `team_quota_max_attempts_ceiling_default` schema knob at team
  creation. Semantically distinct from the trial's requested
  `TrialConfig.retry.max_attempts` (submitter's ask), which is
  clamped to this ceiling at submit time.

## Eligibility predicates (caps)

`requires_caps` is **derived** from the task config at submission
time (`derive_requires_caps` in
`src/loom_control_plane/scheduler/requires_caps.py`); submitters
don't write it. Four predicates today:

| Predicate          | Comes from task            | Matches against worker      |
|--------------------|----------------------------|------------------------------|
| `os`               | container base image       | `worker_os: list[str]`      |
| `cpu_arch`         | task image architecture    | `worker_cpu_arches: list[str]` |
| `gpu_vendor`       | `requires.gpu`             | `worker_gpu_vendors`        |
| `network_policies` | task's egress allow/deny   | `worker_network_policies` (superset) |

`cpu_arch` is intentionally conservative for mixed x86_64/ARM64 fleets.
Missing legacy `requires_caps.cpu_arch` is treated as `x86_64`, so new ARM64
workers do not claim pre-existing tasks such as SWE-Bench images whose task
rows were created before architecture gating existed. Task authors can set
`environment.cpu_arch = "arm64"` for ARM-only images or
`environment.cpu_arch = "any"` only after proving the task image and verifier
are credible on both x86_64 and ARM64.

`gpu_types` (specific SKUs like `A100`, `H100`) is reported by workers but is
not an eligibility predicate. Tasks can currently constrain GPU vendor, not a
specific GPU SKU.

`mounted_fs` is not an eligibility predicate.

## Crash recovery

Workers heartbeat. The **crash detector**
(`src/loom_control_plane/scheduler/crash_detector.py`) wakes
periodically and re-queues any `claimed` trial whose
`worker_id` hasn't heartbeated in the timeout window:

```sql
UPDATE trials
   SET state = 'queued',
       worker_id = NULL,
       failure_reason = CASE
         WHEN state = 'claimed' AND started_at IS NULL
         THEN 'worker_lost_claim'
         ELSE failure_reason
       END,
       next_attempt_at = NOW() + INTERVAL '30 seconds'
 WHERE state = 'claimed'
   AND <heartbeat is stale>
```

`next_attempt_at` sets a 30-second cool-down before re-claim so the
trial doesn't immediately re-flap if the worker is recovering. If a
reclaimed trial was still `claimed` with `started_at IS NULL`, the
crash detector also writes a `failure_message` diagnostic containing
the trial id, previous worker id, `claimed_at`, expiry window, and
`started_at=NULL`. That diagnostic is preserved if the row immediately
exhausts retries; a later successful re-claim clears stale failure
fields before the next attempt starts. The
`attempt_count < max_attempts_ceiling` predicate in the claim query then
caps total retries; over-quota trials end up in `failed_terminal`.

## Why one query (not "scheduler picks, then updates")

Two-step ("SELECT then UPDATE") needs an explicit transaction +
optimistic-lock retry on conflict. With `FOR UPDATE SKIP LOCKED`,
the SELECT *is* the lock — concurrent claimers either get
different rows or `None`, with no retry loop. The whole transition
is one atomic SQL statement, which:

- Eliminates the lost-update race entirely.
- Makes the claim path testable as a pure SQL fixture.
- Lets workers retry on `None` (queue empty) without any
scheduler-side state.

GB10 capacity intent and prod-pressure control use the same atomic claim
boundary. Publishing `draining`/`stopped` desired intent, or applying a prod
pressure signal while a busy host temporarily keeps active intent, reconciles
every matching worker registration by hostname and pool before node-agent
shutdown; the `EXISTS` clause above requires `w.drain_state = 'active'`. This
makes a still-running or duplicate container unclaimable while graceful work drains.
Recovery requires a subsequent node-agent report confirming
`current_intent=active` and `apply_state=applied`; changing a file-only
capacity manifest never bypasses this registry fence.

## Bounded prod-pressure preemption

- A normal drain is non-preemptive: registry fencing stops new claims while
  the busy Compose worker remains active.
- A preemptible staging lease may advance to `stopped` only after its configured
  grace period. The Control Plane records `prod_capacity_pressure` on affected
  claimed/running trials before stopping the host. The existing crash detector
  then returns those trials to `queued` with retry backoff while preserving the
  explicit pressure diagnostic.
- Non-preemptible busy hosts never advance to stopped because of prod pressure;
  they stop only after their assigned claimed/running count reaches zero.

## What this is NOT

- **Not fair-aware across providers.** DRF is per-team, not
  per-provider. A team submitting 100 Anthropic trials gets the
  same share as one submitting 100 OpenAI trials; provider RPM
  throttling lives at the Gateway, not here.
- **Not GPU-aware beyond vendor.** `gpu_vendor` matches; `gpu_types`
  is metadata and is not part of the predicate (see above).
- **Not CLI-mode-relevant.** `loom run` runs locally without a
  Control Plane; concurrency is `asyncio.Semaphore(N)`, ordering is
  whatever your task list passes in.

## See also

- [`service-mode.md`](service-mode.md) — where the scheduler sits
  in the service stack
- [`driver-protocol.md`](driver-protocol.md) — what `requires_caps`
  is matched against on the worker side
