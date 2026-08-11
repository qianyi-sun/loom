# Shared sandbox capacity ledger compatibility interface

> This is the internal ledger of the single global development fleet
> autoscaler, not a separately deployed service. An explicitly installed live
> supervisor uses
> `scripts/ops/global_dev_fleet_autoscaler_external_once.py`; the lower-level
> `global_dev_fleet_autoscaler_once.py` and commands below are deterministic
> offline/compatibility tools. The checked-in development profile keeps this
> capacity path disabled. See
> `docs/architecture/global-dev-fleet-autoscaler.md`.

The shared capacity broker is the single slot authority for the disposable
`sandbox-qianyi`, `sandbox-hongjian`, and `sandbox-devansh` Control Planes. It
does not run inside any sandbox and does not read a sandbox database. One
submit-host service owns its SQLite state and publishes candidate-bound,
secret-free grant handoffs for sandbox-specific adapters.

This broker and handoff contract has no authority to change Slurm policy,
activate non-exclusive workers, mutate shared hosts, or reclaim production or
staging capacity.

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
  "expires_at": "2030-01-01T00:00:00Z",
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
autoscaler API. Authentication and delivery are outside this interface; the
broker remains transport- and token-agnostic.

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

## Evidence retention

Persist the complete JSON output from every accepted request, reconcile,
cancel, and final terminal status. The append-only audit stream records only
bounded identifiers, counts, candidate SHA, and reasons. It intentionally
contains no token-shaped field or raw sandbox configuration.
