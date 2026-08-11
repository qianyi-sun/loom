# Global Development-Fleet Autoscaler

One submit-host supervisor can arbitrate capacity for the complete registry of
development environments. It uses a durable SQLite lease ledger and emits
candidate- and generation-bound grants to environment-local Slurm
reconcilers. Individual development deployments do not own independent global
budgets.

## Demand and grants

Each demand snapshot identifies its environment, deployment generation,
candidate SHA, pool, minimum slots, requested slots, and observation time. A
reconciliation validates the entire current cohort before changing leases.

The ledger enforces global and per-pool committed and pending budgets. It
shares requested minima first, then burst demand, choosing the least allocated
eligible environment. A requested maximum is demand, not a reservation, so one
active environment can use otherwise idle development capacity.

Every grant includes the exact environment, pool, deployment generation,
candidate SHA, lease epoch, expiry, and maximum slots. Local autoscalers treat
that value as a hard ceiling. A missing, expired, wrong-generation,
wrong-candidate, or wrong-epoch grant yields zero authorized slots.

## Reconciliation safety

A tick applies matching lease observations, cancels removed, idle, or
superseded requests, renews unchanged requests, and atomically writes a new
report. Cancellation does not immediately free active, pending, or draining
slots; they remain committed until the matching lease epoch reports
termination. Grants expire when the supervisor stops renewing them.

Development grants are preemptible. Reducing the development budget to zero
drains capacity, and the ledger reuses it only after observing the drain.

## Runtime

`scripts/ops/global_dev_fleet_autoscaler_external_once.py` is the
registry-driven reconciliation entry point. It reads the dynamic registry and
pool observations, updates the owner-only ledger, writes an owner-only report,
refreshes exact-bound worker environment files for ready instances, and calls
the existing external Slurm reconciler with each grant.

`deploy/dev-fleet/` contains the hardened systemd service and timer. The lower
level `scripts/ops/global_dev_fleet_autoscaler_once.py` accepts versioned files
for deterministic offline checks. The autoscaler changes live capacity only
where operators have installed and enabled the external supervisor with valid
budgets, credentials, registry input, and local reconcilers.
