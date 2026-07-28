# Shared Sandbox Capacity Broker

The shared capacity broker is the single slot authority for the disposable
`sandbox-qianyi`, `sandbox-hongjian`, and `sandbox-devansh` Control Planes. It
does not run inside any sandbox and does not read a sandbox database. One
submit-host service owns its SQLite state and publishes candidate-bound,
secret-free grant handoffs for sandbox-specific adapters.

This repository slice is a broker and handoff contract. It does not authorize
Slurm policy changes, non-exclusive worker activation, shared-host mutation, or
production/staging reclaim. Those remain gated by their owning issues.

## Safety contract

- The sandbox allowlist is fixed to `qianyi`, `hongjian`, and `devansh`.
- Every request binds a full lowercase 40-character candidate SHA, one pool,
  `min_slots`, `target_slots`, bounded TTL, purpose, and preemptibility.
- All request, cancel, observation, fair-share, grant, and audit changes occur
  under one SQLite `BEGIN IMMEDIATE` transaction.
- The broker reserves a new grant as `pending_slots` before emitting it.
  Capacity is therefore counted before a sandbox adapter can submit work.
- A lower grant does not immediately free capacity. The previous
  `pending + active + draining` observation remains committed until the
  sandbox reports the new lease epoch drained.
- Global, per-pool, global-pending, and per-pool-pending budgets are checked
  before every one-slot grant. A final partial grant cannot overshoot.
- Grant handoffs contain no worker token, admin token, object-store credential,
  provider secret, private endpoint, or environment-file body.
- An observation with an old lease epoch, regressing terminal count, or more
  nonterminal slots than the broker committed is rejected atomically.

The state database should be installed at:

```text
/var/lib/loom-shared-capacity/broker.sqlite3
```

The directory and database must be writable only by the dedicated broker
service identity. Sandboxes receive handoff JSON through a separately
authenticated transport; they must never receive write access to the database.
Back up the database and its WAL/SHM files as one SQLite unit.

Before that service identity exists, the safe bootstrap is a `root:root`
mode-`0700` directory with no database file. That state permits only explicit
root-invoked validation; do not relax the mode or transfer ownership to any
sandbox account as a shortcut.

## Request capacity

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  request \
  --sandbox qianyi \
  --candidate-sha 0123456789abcdef0123456789abcdef01234567 \
  --pool gb10 \
  --min-slots 20 \
  --target-slots 140 \
  --ttl-minutes 120 \
  --purpose large-batch-runtime-validation \
  --idempotency-key qianyi-gb10-runtime-validation-001 \
  --preemptible
```

The idempotency key may be replayed only with an identical request. It cannot
be rebound to another candidate or capacity shape.

## Reconcile grants

The submit-host supervisor supplies reviewed physical budgets on every pass:

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  reconcile \
  --global-budget 160 \
  --pool-budget gb10=140 \
  --pool-budget oldlab=20 \
  --global-pending-budget 40 \
  --pool-pending-budget gb10=30 \
  --pool-pending-budget oldlab=10
```

Allocation has two fair rounds:

1. rotate one slot at a time across eligible sandboxes until their requested
   minima or the available budget is reached;
2. continue the same aging-aware rotation toward target slots.

An idle pool can therefore burst to one sandbox's target. When several
sandboxes remain active, persisted `last_granted_seq` aging moves a scarce
slot to the least recently granted sandbox. The old holder first receives a
lower handoff and drains. The next holder is granted only after release is
observed, so fairness never creates temporary overcommit.

## Sandbox autoscaler handoff

Each `handoffs` item is an `AutoscalerGrantHandoff`:

```json
{
  "schema_version": 1,
  "request_id": "uuid",
  "lease_epoch": 3,
  "sandbox": "qianyi",
  "environment": "sandbox-qianyi",
  "candidate_sha": "0123456789abcdef0123456789abcdef01234567",
  "pool_name": "gb10",
  "enabled": true,
  "min_slots": 0,
  "max_slots": 47,
  "expires_at": "2026-07-27T22:00:00Z",
  "preemptible": true
}
```

The sandbox adapter must:

1. reject a handoff unless its sandbox, pool, and exact deployed candidate SHA
   all match;
2. reject a lease epoch older than its last applied epoch;
3. set the local autoscaler ceiling to `max_slots`; the broker remains the only
   authority that may raise that ceiling;
4. treat `enabled=false` or `max_slots=0` as drain-to-zero, not immediate proof
   that capacity was released;
5. return an observation for the same request and epoch.

This interface intentionally does not call the existing Control Plane
autoscaler API. The environment-specific adapter below owns authentication and
delivery, while the broker remains transport- and token-agnostic.

## Persistent sandbox handoff adapter

The environment-specific adapter is now
`scripts/ops/shared_capacity_adapter.py`. It is deliberately a separate
process from the broker:

- the broker remains the only process allowed to increase a grant;
- the adapter consumes one exact broker-produced `handoffs` item and cannot
  construct or expand a grant;
- the adapter validates sandbox, environment, pool, full candidate SHA,
  request ID, and monotonically increasing lease epoch before calling the
  sandbox Control Plane;
- the adapter reads the sandbox's `sandbox-state.json` and requires the local
  autoscaler policy's `actuator_config.candidate_sha` to match that same SHA;
- the admin credential is read from the existing mode-`0600` admin secret
  TOML. Neither the config, argv, result JSON, observation, nor durable adapter
  state contains the token;
- an expired or disabled handoff can only lower the local ceiling to zero;
- after at least one accepted handoff, a missing handoff reuses only its
  durable request/epoch binding and drains the local ceiling to zero;
- a malformed, rewritten-same-epoch, regressing-epoch, or
  candidate-mismatched handoff fails without an autoscaler mutation;
- restart state is durable. Unknown Control Plane counters retain the last
  committed observation instead of releasing uncertain capacity.

Checked-in adapter configs cover both pools for all three sandboxes:

```text
deploy/developer-sandboxes/shared-capacity-adapters/
  qianyi-gb10.toml       qianyi-oldlab.toml
  hongjian-gb10.toml     hongjian-oldlab.toml
  devansh-gb10.toml      devansh-oldlab.toml
```

Each config contains paths and a secret-file reference, never a secret. The
installed contract is:

```text
/etc/loom/shared-capacity-adapters/<sandbox>-<pool>.toml
/etc/loom/developer-sandboxes/<sandbox>-admin.toml
/srv/loom/developer-sandboxes/<sandbox>/sandbox-state.json
/var/lib/loom-shared-capacity/handoffs/<sandbox>-<pool>.json
/var/lib/loom-shared-capacity/observations/<sandbox>-<pool>.json
/var/lib/loom-shared-capacity/adapters/<sandbox>-<pool>.json
```

The handoff transport must atomically copy the **unaltered** matching item from
the broker's latest accepted `reconcile` result into the handoff path. It must
not synthesize `max_slots`, change an epoch, or make the file writable by a
sandbox account. Mode `0600`, owner `root:root`, inside the broker's mode-`0700`
state root is the current bootstrap contract. A future dedicated broker
identity may replace root only as one reviewed ownership migration.

The adapter writes a one-element JSON observation array compatible with
`--observations-json`. A supervisor combines the six arrays without modifying
their objects, supplies the same reviewed budgets on every broker reconcile,
then atomically republishes the resulting handoffs. Observation files are
inputs, never grants. A sandbox-written observation cannot increase capacity:
the broker rejects old epochs and nonterminal counts above its commitment.

Run one adapter manually for validation:

```bash
python scripts/ops/shared_capacity_adapter.py \
  --config /etc/loom/shared-capacity-adapters/qianyi-gb10.toml \
  run
```

The secret-free result reports the request, epoch, applied ceiling, expiry
state, and observation. Exit status `1` emits only:

```json
{"error":"shared-capacity-adapter-failed-safely"}
```

### Exact-candidate systemd contract

The checked-in template and timer are:

```text
deploy/developer-sandboxes/loom-shared-capacity-adapter@.service
deploy/developer-sandboxes/loom-shared-capacity-adapter@.timer
```

Render the service against the full candidate SHA:

```bash
python scripts/ops/render_shared_capacity_adapter_service.py \
  --git-sha <full-lowercase-40-character-SHA> \
  > /tmp/loom-shared-capacity-adapter@.service
```

The renderer rejects a mutable path, an unresolved placeholder, or a partial
SHA. The installed unit runs from:

```text
/opt/loom-shared-capacity/candidates/<SHA>/repo
/opt/loom-shared-capacity/candidates/<SHA>/venv
```

The timer is a persistent 15-second oneshot schedule. Install and enablement
mutate shared-host systemd state and therefore require explicit live authority;
repository merge alone does not authorize them. Before activation, verify the
rendered service and timer with `systemd-analyze verify`, copy the reviewed
instance configs to `/etc/loom/shared-capacity-adapters`, and prove the admin
secret and sandbox-state files have their required ownership/modes.

The bootstrap unit runs as root because the three sandbox roots are private
mode `0700` under different owners and the broker root is root-only. Do not
weaken those modes. Replacing root requires a dedicated service identity plus
reviewed ACLs that grant only the exact state, handoff, observation, and secret
files needed by each instance.

## Report observed capacity

Prepare a secret-free JSON array:

```json
[
  {
    "request_id": "uuid",
    "lease_epoch": 3,
    "pending_slots": 7,
    "active_slots": 40,
    "draining_slots": 0,
    "terminal_slots": 0
  }
]
```

Then reconcile it with the same reviewed budgets:

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  reconcile \
  --global-budget 160 \
  --pool-budget gb10=140 \
  --pool-budget oldlab=20 \
  --global-pending-budget 40 \
  --pool-pending-budget gb10=30 \
  --pool-pending-budget oldlab=10 \
  --observations-json /var/lib/loom-shared-capacity/observations.json
```

The status output follows
[`shared-sandbox-capacity-evidence.schema.json`](../evidence/shared-sandbox-capacity-evidence.schema.json).
Its aggregate and each lease expose:

- requested slots;
- broker-granted slots;
- observed/reserved pending slots;
- active slots;
- draining slots;
- cumulative terminal slots;
- committed slots, defined as
  `max(granted, pending + active + draining)`.

## Cancel, TTL, and recovery

Cancel is drain-first:

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  cancel \
  --request-id REQUEST_UUID \
  --reason operator_cancelled
```

The handoff becomes `enabled=false`, `max_slots=0`, and its epoch increments.
The request stays `draining` while any pending, active, or draining slots are
observed. It becomes `terminal` only after a same-epoch zero-nonterminal
observation. TTL expiry uses the same path with terminal reason `ttl_expired`.

After a broker restart, run `status`, query the three sandbox adapters and
Slurm for the current epoch observations, then run `reconcile`. Do not edit the
SQLite tables or reduce observed slot counts by hand. A missing or uncertain
observation keeps capacity committed and fails toward lower utilization.

After an adapter restart, its durable state fences replay. Reapplying an
identical handoff is idempotent. Do not delete adapter state to bypass an epoch
error: cancel the broker request, publish the zero-ceiling handoff, and wait
for a same-epoch zero-nonterminal observation first.

## Rollback

Rollback is drain-first and preserves authority records:

1. cancel each nonterminal broker request;
2. reconcile and atomically publish the resulting `enabled=false`,
   `max_slots=0` handoffs;
3. leave the adapter timers running until every same-epoch observation reports
   zero pending, active, and draining slots and broker status is terminal;
4. stop and disable the six adapter timers;
5. retain `broker.sqlite3`, WAL/SHM files, adapter states, observations, and
   audit/evidence JSON as one incident/recovery record.

Never roll back by deleting the SQLite database, editing a lease epoch,
lowering an observation by hand, removing the handoff before drain completes,
or restoring a larger local autoscaler `max_slots`. Named sandbox volumes are
outside this adapter rollback and remain governed by the developer-sandbox
destroy contract.

## Production-pressure drain and retry acceptance

The broker grant ceiling and the Control Plane production-pressure intent are
separate, composable authorities. A production-pressure signal must first
fence new claims through
`POST /admin/worker-pools/<sandbox-environment>/<pool>/prod-pressure`. The
installed submit-host external autoscaler is then the only process allowed to
run `scancel`.

For Slurm pools the durable drain contract is:

- pending jobs and running jobs with zero in-flight trials are cancelled on the
  next external reconcile, without waiting for the preemption grace period;
- busy non-preemptible jobs remain `draining` until their trials finish, then
  the next reconcile cancels the now-idle allocation;
- busy preemptible jobs are not cancelled until the recorded grace period has
  elapsed;
- a successful `scancel` does not release broker or Control Plane capacity.
  The Slurm job must read back terminal first;
- a successful cancel awaiting terminal read-back is persisted on the
  `SlurmWorkerJob`. A submit-host restart observes it instead of issuing a
  duplicate `scancel`;
- a failed `scancel` leaves the job and worker draining. It is not marked
  released and its active trials are not marked interrupted;
- after terminal read-back, an authorized preemption records
  `failure_reason=prod_capacity_pressure`; the crash detector preserves that
  attribution when it returns the interrupted trial to `queued` with retry
  backoff;
- pressure recovery reactivates only controller-owned draining workers that
  still have live Slurm jobs. A cancellation already in flight is settled
  before normal scaling resumes.

`LIVE SANDBOX + SLURM AUTHORITY REQUIRED`

Exercise this only through the exact installed bridge and external autoscaler
services. Do not run an ambient checkout or call `scancel` manually. Capture
the secret-free outputs of:

```bash
systemctl status loom-prod-pressure-worker-control.timer
systemctl --user status 'loom-autoscaler-*.timer'
squeue -h -o '%i|%T|%j|%a|%q|%R'
```

For each `sandbox-{qianyi,hongjian,devansh}` and each reviewed pool, the
acceptance sequence is:

1. record the current autoscaler status, active Slurm jobs, worker drain states,
   and in-flight trial counts;
2. apply a sanitized nonzero pressure snapshot through the installed bridge;
3. prove claims are fenced before the next actuator tick;
4. prove pending/idle jobs disappear only after terminal Slurm read-back;
5. keep one busy non-preemptible job and prove it is not cancelled, finish its
   trial, then prove the next cycle releases it;
6. keep one busy preemptible job through the grace boundary, prove terminal
   read-back and the persistent retry attribution, then prove crash reclaim
   returns the trial to `queued`;
7. interrupt/restart the external service between `scancel` and terminal
   read-back and prove no duplicate cancellation or lost retry attribution;
8. clear pressure and prove only still-live held workers return to `active`.

The autoscaler decision reason is the operator status surface. It reports
secret-free counts for `cancelled`, `awaiting_terminal`, `cancel_failed`,
`held_busy`, and `retryable_trials`. Any nonzero `cancel_failed` count is a
stop condition; repair the installed Slurm authority and let the timer retry.
## Evidence retention

Persist the complete JSON output from every accepted request, reconcile,
cancel, and final terminal status. The append-only audit stream records only
bounded identifiers, counts, candidate SHA, and reasons. It intentionally
contains no token-shaped field or raw sandbox configuration.
