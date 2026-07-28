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
- each adapter config has a reviewed `max_slots_bound`; the checked-in GB10
  bound is `140` and the OLDLAB bound is `20`, and a larger handoff is rejected
  before any Control Plane call;
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
/srv/loom/developer-sandboxes/<sandbox>/secrets/admin.toml
/srv/loom/developer-sandboxes/<sandbox>/sandbox-state.json
/var/lib/loom-shared-capacity/handoffs/current/<sandbox>-<pool>.json
/var/lib/loom-shared-capacity/observations/<sandbox>-<pool>.json
/var/lib/loom-shared-capacity/adapters/<sandbox>-<pool>.json
```

The supervisor publishes an immutable generation directory containing all six
instance outcomes. A published outcome is either the **unaltered** matching
broker handoff or an explicit `status=absent` manifest entry. It fsyncs every
file and the generation directory, then commits the complete set with one
atomic `current` symlink replacement. Adapters only read through `current`; an
interrupted cycle therefore exposes the old complete generation or the new
complete generation, never a mixed six-file set. The current and immediately
previous generations are retained. Files are mode `0600`, directories are mode
`0700`, and a sandbox account has no write access.

The adapter writes a one-element JSON observation array compatible with
`--observations-json`. Observation files are inputs, never grants. A
sandbox-written observation cannot increase capacity: the broker rejects old
epochs and nonterminal counts above its commitment.

### Persistent broker supervisor

`scripts/ops/shared_capacity_supervisor.py` owns the persistent broker cycle.
Its checked-in closed configuration is:

```text
deploy/developer-sandboxes/shared-capacity-supervisor/config.toml
```

The installed paths are:

```text
/etc/loom/shared-capacity-supervisor.toml
/var/lib/loom-shared-capacity/broker.sqlite3
/var/lib/loom-shared-capacity/supervisor-state.json
/var/lib/loom-shared-capacity/supervisor-audit.jsonl
/var/lib/loom-shared-capacity/evidence/supervisor-latest.json
```

One invocation performs this ordered cycle:

1. read broker status and reject more than one nonterminal request for any
   sandbox/pool instance;
2. read at most one observation from each of the six configured files;
3. accept an observation only when its request and epoch exactly match the
   current broker binding; ignore a proven terminal old request or lower stale
   epoch, and reject an unknown request or epoch ahead of the broker;
4. call `SharedCapacityBroker.reconcile` exactly once, so all accepted
   observations, TTL/cancel changes, fair-share decisions, budgets, leases,
   and broker audit events commit in one `BEGIN IMMEDIATE` transaction;
5. independently recompute the report aggregate and per-pool committed and
   pending slots. Publication is rejected unless the persisted budgets exactly
   equal the configured budgets and all of these bounds hold:

   ```text
   global slots <= 160       global pending <= 40
   gb10 slots <= 140         gb10 pending <= 30
   oldlab slots <= 20        oldlab pending <= 10
   ```

6. validate every handoff against its request ID, epoch, sandbox, environment,
   pool, candidate, grant, preemptibility, and enabled state, materialize all
   six published-or-absent outcomes in an immutable generation, fsync it, then
   publish the complete set with one atomic `current` symlink replacement;
7. append one secret-free, monotonically sequenced audit event and atomically
   update supervisor state and latest evidence.

A crash after the broker transaction but before the `current` replacement
cannot overshoot: capacity is already reserved by the broker and adapters still
see the prior complete generation. A crash after the replacement exposes the
new complete generation. The next idempotent cycle validates and reuses an
unchanged generation. Missing observations preserve the broker's prior
commitment and reduce utilization rather than freeing uncertain capacity.

### Initial sandbox autoscaler policies

A fresh sandbox Control Plane has no GB10 or OLDLAB autoscaler row. The adapter
unit therefore runs the idempotent `bootstrap` command before every handoff
cycle. The checked-in policy templates are:

```text
deploy/developer-sandboxes/shared-capacity-policies/gb10.toml
deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml
```

Bootstrap creates only a disabled, scale-to-zero row (`enabled=false`,
`min_slots=max_slots=0`). It binds the exact sandbox candidate, non-exclusive
Slurm mode, the reviewed node allowlist, `loom-dev-<sandbox>` account,
`loom-dev` QoS, external-runner authority, and positive container CPU, memory,
and PID containment. `container_pids` is the per-container cap, while the exact
actuator `job_pids_max` is the Slurm-job aggregate; bootstrap rejects a policy
unless `job_pids_max >= container_pids * requested_concurrency`. Existing rows
are never silently overwritten:
any candidate, account/QoS, node, containment, or authority drift fails closed.
The admin token is read from the mode-`0600` TOML and never appears in argv,
policy files, output, state, or audit.

Manual read-only validation of the same idempotent path is:

```bash
python scripts/ops/shared_capacity_adapter.py \
  --config /etc/loom/shared-capacity-adapters/qianyi-gb10.toml \
  bootstrap
```

This creates the disabled Control Plane row if absent; it does not activate
Slurm. The candidate checkout remains immutable under the candidate namespace.
The secret-bearing worker env uses the separate canonical runtime path:

```text
/shared_work/loom/runtime/sandboxes/<sandbox>/<candidate-SHA>/worker-<pool>.env
```

OLDLAB and GB10 are independent NFS domains: the identical logical path does
not mean they share one backing file. A later persistent publisher must
materialize the exact candidate-bound mode-`0600` env independently in each
Slurm domain before authorized activation (`worker-oldlab.env` in OLDLAB and
`worker-gb10.env` in GB10). Missing, wrong-SHA, wrong-domain, or unsafe-mode
runtime env materialization is a fail-closed activation blocker.

Run one supervisor cycle manually:

```bash
python scripts/ops/shared_capacity_supervisor.py \
  --config /etc/loom/shared-capacity-supervisor.toml \
  run
```

The command emits only counts, digests, binding identifiers, and publication
status. Failure emits only:

```json
{"error":"shared-capacity-supervisor-failed-safely"}
```

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

The checked-in adapter template and timer are:

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

The broker supervisor uses its own network-isolated exact-candidate unit:

```text
deploy/developer-sandboxes/loom-shared-capacity-supervisor.service
deploy/developer-sandboxes/loom-shared-capacity-supervisor.timer
```

Render it with:

```bash
python scripts/ops/render_shared_capacity_supervisor_service.py \
  --git-sha <full-lowercase-40-character-SHA> \
  > /tmp/loom-shared-capacity-supervisor.service
```

The supervisor timer also runs every 15 seconds and needs no network. Install
the supervisor timer first; install the six adapter timers only after one
manual supervisor cycle has published and read back the expected handoffs.
Both services must render from the same immutable candidate SHA. Use
`systemd-analyze verify` on both rendered services and both checked-in timers
before installation.

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

After a supervisor restart, the last sequence is recovered from both its
state and append-only audit tail. A config digest change is rejected while any
capacity remains committed. Drain all leases before changing budgets, paths,
or instance membership; never delete supervisor state or audit to bypass that
gate.

## Rollback

Rollback is drain-first and preserves authority records:

1. cancel each nonterminal broker request;
2. reconcile and atomically publish the resulting `enabled=false`,
   `max_slots=0` handoffs;
3. leave the adapter timers running until every same-epoch observation reports
   zero pending, active, and draining slots and broker status is terminal;
4. stop and disable the six adapter timers, then stop and disable the
   supervisor timer;
5. retain `broker.sqlite3`, WAL/SHM files, adapter states, observations, and
   audit/evidence JSON as one incident/recovery record.

Never roll back by deleting the SQLite database, editing a lease epoch,
lowering an observation by hand, removing the handoff before drain completes,
or restoring a larger local autoscaler `max_slots`. Named sandbox volumes are
outside this adapter rollback and remain governed by the developer-sandbox
destroy contract.

## Evidence retention

Persist the complete JSON output from every accepted request, reconcile,
cancel, and final terminal status. The append-only audit stream records only
bounded identifiers, counts, candidate SHA, and reasons. It intentionally
contains no token-shaped field or raw sandbox configuration.
