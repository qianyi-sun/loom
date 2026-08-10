# Global Fleet Capacity Manager

The global fleet capacity manager is the single allocation authority for every
Loom application environment. It receives demand from production, staging,
and every per-person development deployment, then calculates one fleet-wide
result across the physical `gb10` and `oldlab` pools. The shared `loom-dev`
namespace hosts trusted infrastructure and is not itself an application or
capacity subject.

The detailed approved design is
[Global Fleet Capacity Manager Design](global-fleet-capacity-manager-design.md).
Package 1 was implemented in PR #1268; its execution plan remains available in
Git history rather than in the repository documentation tree.

## Authority and data flow

```text
fleet generation ───────────┐
environment configurations ├──> independent management database
demand reports (all envs) ──┤                  │
pool reports (gb10/oldlab) ─┘                  │ one fenced writer
                                               v
                                   deterministic shadow allocator
                                               │
                            ┌──────────────────┼──────────────────┐
                            v                  v                  v
                     bounded status       audit history       metrics
                     and evidence         and epochs           (no env IDs)
```

The management database uses its own `capacity_0001` migration tree and must
not be an environment's application database. One authority incarnation and a
monotonic writer epoch fence calculations and commits. A calculation runs
outside its serializable commit transaction; the commit succeeds only if the
exact input digest and writer fence still match. A concurrent input change is
retried from a fresh snapshot, and no partial allocation rows are committed.

One global calculation is plausible even though `gb10` and `oldlab` have
different Slurm controllers. The manager owns policy and computes a complete
cross-pool allocation. It does not need one process to open both controllers.
Later packages can deliver fenced, pool-specific intents to one adapter per
controller. Each adapter may mutate only its bound physical pool, and the
global manager remains the sole source of allocation authority.

## Multiple environments and people

Every deployment is a stable subject identity and incarnation, not a physical
pool. Consequently, several people can deploy and report demand concurrently
without creating separate capacity silos or synthetic `dev-<name>` pools. All
development subjects share the development tier and its account/fleet limits.

Allocation does not use user-configurable pool weights. It applies, in order:

1. strict tier priority (`production`, `staging`, `development`);
2. fleet, tier, account, subject, pending, and rollout ceilings;
3. progressive round-robin fairness between accounts and then subjects;
4. local task priority and stable current assignments;
5. constrained-before-flexible placement with exact per-node packing.

`min_slots` is configurable per subject and defaults to `0`. It is a bounded
minimum request, not a guarantee when compatible capacity is unavailable or a
higher-priority limit applies.

An architecture-specific task lists only compatible pools/domains, so it waits
or runs there. An architecture-neutral task may use either `gb10` or `oldlab`;
the allocator preserves existing placement where possible and otherwise uses
deterministic constrained-first topology placement. Users do not steer neutral
work by assigning pool weights. An explicit task pool requirement remains a
hard eligibility constraint.

Missing, stale, invalid, or equivocal reports never free capacity. Existing
claims and physical commitments remain charged, uncertain scope is frozen, and
old/new rollout overlap is limited by the subject and account surge ceilings.

## Package 1 service boundary

Package 1 provides strict versioned contracts, fleet drift diagnostics, the
independent database, reporter fencing, deterministic topology and allocation,
one fenced shadow reconciler, an authenticated mTLS API, bounded status and
metrics, and an offline evidence driver.

The HTTP service exposes only configuration proposal/activation, report
ingestion, shadow reconciliation, status, audit, health, and metrics routes.
There is no grant, launch-permit, execution, Slurm, claim-admission, or release
route. Mutual TLS authenticates the transport, hashed bearer principals bind
the exact operator or reporter authority, and metric labels never contain
subject IDs or dynamic environment names.

Package 1 is not capacity activation. It cannot authorize a task claim,
worker launch, Slurm mutation, or physical release. The database rejects a
non-zero executable ceiling.

The reproducible offline check uses only synthetic checked-in inputs:

```bash
output="$(pwd)/shadow-evidence.json"
uv run --frozen python scripts/ops/global_fleet_capacity_shadow_once.py \
  --fleet tests/fixtures/capacity/fleet-v1.toml \
  --subjects tests/fixtures/capacity/subjects-v1.toml \
  --snapshot tests/fixtures/capacity/snapshot-v1.json \
  --output "$output"
```

The output is canonical JSON, atomically replaced with mode `0600`, and always
states `mode: shadow`, `executable: false`, and
`executable_new_capacity_ceiling: 0`. It contains hypothetical diagnostic
allocations, never grants or launch permits. It accepts no database, Slurm, or
live-environment argument.

## Current activation blockers

There is intentionally no live global fleet manifest. The checked-in
[fleet-state example](../../deploy/fleet-state/README.md) is synthetic. The
diagnostic inventory of the current development, staging, and production
environment copies reports these conflicts:

- `gb10`: allowed nodes, slot/job/concurrency ceilings, per-slot CPU and
  memory, requested/reserved resources, and resource-aware settings;
- `oldlab`: controller and cluster identity, partition, allowed nodes,
  architecture/exclusivity/container settings, slot/job/concurrency ceilings,
  per-slot CPU and memory, requested/reserved resources, and resource-aware
  settings.

Those facts must be measured and reconciled into one reviewed immutable fleet
generation. The manager must not choose an environment copy or merge node
lists implicitly.

Live activation remains blocked on Packages 2–5 in the approved design,
including protected task/claim and execution-generation bindings, fenced
pool-local actuation, legacy-writer containment and drain evidence tracked by
issue #896, and the re-scoped activation evidence tracked by issue #906. Until
all activation-boundary evidence is approved, the executable ceiling remains
zero and existing legacy autoscaling behavior is unchanged.

## Verification

The Package 1 gate runs the capacity contract, state, topology, allocator,
store, API, mTLS, property, and offline-driver tests; Ruff; mypy; whitespace
checks; and a source audit for grant, launch-permit, worker-claim, or Slurm
mutation vocabulary. Integration tests additionally prove the authority
ceiling and every stored allocation executable flag remain zero/false.
