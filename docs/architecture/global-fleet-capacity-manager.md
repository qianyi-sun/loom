# Global Fleet Capacity Manager

The global capacity-manager service computes deterministic, fleet-wide shadow
allocations from versioned fleet configuration, subject configuration, demand
reports, pool observations, and current commitments. It also records a fenced
dry-run protocol for reservations, launch ordering, executor inventory, and
release evidence. Both surfaces are evidence and audit infrastructure only:
they cannot authorize a worker claim, launch capacity, mutate a Slurm
controller, or release physical capacity.

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
input, dynamic personal-subject projections, dry-run grant and executor
records, shadow-reconciliation requests, and read-only status/audit queries.
Its database enforces:

- `executable_new_capacity_ceiling = 0`;
- all allocation, reservation, permit, executor, inventory, and release
  records are non-executable;
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

## Fenced dry-run executor protocol

The same mTLS service exposes an implemented, non-executable grant protocol. A
manager principal can propose an exact reservation and issue a launch-ordering
permit. A pool-bound executor principal can register, renew its lease, fetch
its checkpoint, publish complete inventory, accept a reservation, register
bootstrap evidence, account for permit consumption, fence an unused intent,
and submit partial-release evidence. A subject-bound demand reporter publishes
the corresponding protected-environment release acknowledgement.

Protocol state and transition validation bind the authority and writer epochs,
pool and pool generation, executor identity and incarnation, and the relevant
reservation, intent, permit, subject, candidate, and deployment identities.
Every top-level contract and receipt has `executable: false`, while health and
status continue to report an executable-new-capacity ceiling of zero. The
executor library has no scheduler client, process-execution entry point, or
Slurm mutation surface, so a reservation acceptance or consumed permit is an
ordering/accounting record, not permission to run `sbatch`, cancel a job,
signal a worker, or release live capacity.

Registration binds one controller-local Ed25519 ownership key to an exact pool
executor. Only the public verification key reaches the manager. Complete
inventories carry signed immutable ownership metadata; missing, conflicting,
foreign, or unverifiable observations remain charged or quarantined rather
than being treated as free capacity. Fresh pool evidence is required at each
capacity-increase transition.

Executor commands are journal-first. The controller-local append-only journal
is fsynced before a transition, reconciled with the manager checkpoint, and
confirmed only after the bounded response validates. An ambiguous transport
failure is replayed with the same contract; a changed or regressed journal
fences the incarnation. Release evidence is accepted only after intent-close
state and the exact append-only environment-agent release fence agree, which
prevents delayed bootstrap or worker registration from reclaiming the shape.
See the
[pool-executor dry-run runbook](../runbooks/global-fleet-pool-executor-dry-run.md)
for controller binding, recovery, and rehearsal checks.

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

Personal teardown uses the same projection route with `operation_kind` set to
`destroy`. The manager first records the subject as `disabled` with zero
minimum and maximum demand. Only after that acknowledgement does the lifecycle
seal the personal database authorities and begin namespace, database, bucket,
tenant, and credential cleanup. Epoch contention is retried against a newer
configuration; incomplete retirement evidence fails before local deletion.

## Service surface

The HTTP service exposes configuration proposal/activation, dynamic personal
subject projection, report ingestion, dry-run executor registration,
checkpoint, heartbeat, and inventory, reservation and launch-permit records,
protected-release acknowledgements, partial-release evidence, shadow
reconciliation, status, audit, health, and metrics routes. Mutual TLS
authenticates the transport, hashed bearer principals bind the exact operator,
reporter, manager, or pool-executor authority, and metric labels never contain
subject IDs or dynamic environment names.

The grant, permit, executor, inventory, and release routes accept only dry-run
contracts. The service has no task-claim admission, scheduler execution, Slurm
mutation, or physical-release authority, and the database rejects a non-zero
executable ceiling.

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
attempt identities, prepared bindings, lifecycle observations, protected
release fences, legacy-writer inventory, and audit records under append-only
and serializable constraints.

This guard remains disabled at allocation epoch zero. Its admission checks
deny execution, its stored bindings are non-executable, and normal submission
and claim routes do not use it to authorize work. It is useful only for
validating the protected data model and comparing current demand with shadow
allocations.

Implementation lives under `src/loom_capacity_manager/`,
`src/loom_capacity_executor/`, and `src/loom_capacity_agent/`. The service,
executor, and protected-store integration suites prove that no stored or
returned allocation, reservation, permit, or release record becomes
executable.
