# Global development-fleet autoscaler

Status: implemented (issue #1192)

## Decision

Development environments do not each own a capacity budget and do not deploy
separate arbitration services. One submit-host process runs the global dev
fleet autoscaler. Its internal durable lease ledger performs the transaction
that turns the complete, dynamic registry cohort into grants.

```text
dev demand + registry + lease observations
                    │
                    v
       global dev fleet autoscaler
       (one transactional ledger)
                    │
        candidate/generation grants
                    │
                    v
     environment-bound Slurm reconcilers
```

The ledger is an implementation detail, not a separately deployed “broker.”
This preserves one place that sees all demand while retaining crash-safe lease
epochs, pending-slot accounting, audit history, and drain-first reallocation.

## Contracts

Each demand snapshot identifies `(environment, deployment_generation,
candidate_sha, pool_name)`, its local minimum and requested slot ceiling, and a
fresh timestamp. The complete snapshot set is validated before lease mutation.
Membership is data-driven; another developer requires no allow-list change.

The authority enforces global and per-pool committed-slot budgets plus separate
pending-slot budgets. Allocation shares requested minima first and then burst
demand, choosing the least-allocated environment. Policy `max_slots` is demand,
not a reservation: every environment may request the whole dev budget, letting
one active developer use otherwise-idle capacity.

Every handoff contains the exact environment, pool, deployment generation,
candidate SHA, lease epoch, expiry, and maximum slots. The local worker
autoscaler treats it as a hard ceiling. Missing, expired, or mismatched grants
fail closed at zero.

## Reconciliation and safety

One global tick validates the full input, applies lease observations, cancels
removed/idle/superseded requests, renews unchanged requests, and atomically
allocates a new grant report. Cancellation never pretends workers disappeared:
active, pending, and draining slots stay committed until matching-epoch
termination is observed. A new deployment therefore cannot double-allocate
while its predecessor drains. TTL renewal makes health explicit; abandoned
grants expire.

## Operations

`scripts/ops/global_dev_fleet_autoscaler_once.py` consumes versioned `demands`
and `observations`, updates the owner-only SQLite authority, and atomically
writes an owner-only report. Environment runners pass that report plus their
deployment generation to `worker_pool_autoscaler_external_once.py`; the
existing Slurm actuator remains the worker lifecycle owner.

Production pressure remains higher priority. Dev grants are preemptible and
the global budget may be reduced to zero, but capacity is reused only after its
drain is observed.

## Activation gate

Code and deterministic tests are complete. A live timer rollout still requires
operator-provided budgets, a dynamic demand/observation feed, and an approved
submit-host change. Developer commands never mutate the global ledger directly.
