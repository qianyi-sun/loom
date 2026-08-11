# Global Fleet Capacity Manager

The global capacity-manager service computes deterministic, fleet-wide shadow
allocations from versioned fleet configuration, subject configuration, demand
reports, pool observations, and current commitments. It is evidence and audit
infrastructure only: it cannot grant a worker claim, launch capacity, mutate a
Slurm controller, or release physical capacity.

## Authority boundary

The service uses an independent management database and its own migration
tree. It must not share an application environment's database. One authority
incarnation and monotonically fenced writer epoch protect shadow
reconciliation. A commit succeeds only while the exact input digest and writer
fence still match; changed inputs cause a fresh calculation instead of a
partial result.

The HTTP server requires mutual TLS and separately verifies hashed bearer
principals with bounded scopes. Owner-only `0600` files hold its database URL,
principal data, and TLS keys. Request bodies and list limits are bounded, and
metrics avoid dynamic environment or subject labels.

The service accepts configuration proposals and activation records, reporter
input, dynamic personal-subject projections, shadow-reconciliation requests,
and read-only status/audit queries. Its database enforces:

- `executable_new_capacity_ceiling = 0`;
- all allocation epochs and results are non-executable;
- reporter sequence and writer fences are monotonic; and
- conflicting idempotency or report replay fails closed.

## Shadow allocation

The allocator applies tier priority, configured global/tier/account/subject
ceilings, stable current assignments, constrained-before-flexible placement,
and deterministic fairness across accounts and subjects. Missing, stale,
invalid, or equivocal reports do not free physical commitments.

The output is a hypothetical allocation plus diagnostics and audit evidence.
Existing environment-local scheduling and autoscaler paths remain the only
executable authorities.

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

## Dynamic personal subject projection

After stable-route activation, the lifecycle service registers the personal
deployment through `PUT /v1/development-projections/{subject_id}` before the
environment can be marked ready. The manager derives the
`dev-<name>` subject, its immutable owner account, and both physical-pool
profiles from the active operator-owned fleet template in one serializable
configuration epoch. The request binds the candidate publication, local
activation acknowledgement, deployment/configuration generations, reporter
incarnation, protected-admission evidence, trusted capacity-agent installation,
supported architectures, and required protocols. The lifecycle
cannot supply a priority tier, pool weight, worker shape, account ceiling, or
an executable override.

Projection is unavailable until an active fleet generation explicitly
contains a development-subject template and an owner-account template. Exact
operation and idempotency replays converge; identity reuse, stale epochs,
quota violations, or incomplete pool/architecture bindings fail closed.
Derived reporter credentials are hash-only and bound to the exact subject,
incarnation, configuration generation, deployment generation, and reporter
incarnation. Retrying a deployment rotates the reporter incarnation and
fences the predecessor. The projection response and audit log never expose
the token or its hash. All resulting allocations remain shadow-only while the
global executable ceiling is zero.

The independently installed capacity agent then captures protected lifecycle
demand and publishes an exact, sequence-fenced report. Its readiness probe
remains unavailable until a report succeeds. Personal lifecycle readiness is
therefore gated on both the subject projection and the agent's initial demand
publication, but neither event grants or launches physical capacity.

## Service surface

The HTTP service exposes configuration proposal/activation, dynamic personal
subject projection, report ingestion, shadow reconciliation, status, audit,
health, and metrics routes.
There is no grant, launch-permit, execution, Slurm, claim-admission, or release
route. Mutual TLS authenticates the transport, hashed bearer principals bind
the exact operator or reporter authority, and metric labels never contain
subject IDs or dynamic environment names.

The service cannot authorize a task claim, worker launch, Slurm mutation, or
physical release. The database rejects a non-zero executable ceiling.

Run the checked-in offline proof without a live database or controller:

```bash
uv run --frozen python scripts/ops/global_fleet_capacity_shadow_once.py \
  --fleet tests/fixtures/capacity/fleet-v1.toml \
  --subjects tests/fixtures/capacity/subjects-v1.toml \
  --snapshot tests/fixtures/capacity/snapshot-v1.json \
  --output shadow-evidence.json
```

The output is canonical JSON, written atomically with mode `0600`, and records
`mode: shadow`, `executable: false`, and a zero executable ceiling.

## Environment-side guard data

Application databases can contain the separately owned
`loom_capacity_guard` schema. It stores sealed trial requirements, protected
attempt identities, prepared bindings, lifecycle observations, legacy-writer
inventory, and audit records under append-only and serializable constraints.

This guard remains disabled at allocation epoch zero. Its admission checks
deny execution, its stored bindings are non-executable, and normal submission
and claim routes do not use it to authorize work. It is useful only for
validating the protected data model and comparing current demand with shadow
allocations.

Implementation lives under `src/loom_capacity_manager/` and
`src/loom_capacity_agent/`. The service and protected-store integration suites
prove that no stored or returned allocation becomes executable.
