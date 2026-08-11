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

## Package 2A protected admission foundation

Package 2A adds an independently migrated `loom_capacity_guard` schema inside
each environment database. Its migration login must explicitly assume a
pre-provisioned non-login owner role before creating the protected revision
table or any protected object. The entrypoint rejects superuser and other
broad cluster-level migration credentials. Candidate roles and `PUBLIC` receive no schema,
table, sequence, function, or future-object default privileges.

The protected foundation stores only an exact disabled authority fence,
canonical sealed trial requirements, immutable queued attempt identities, and
bounded append-only audit records. Composite foreign keys bind an attempt to
the exact trial and requirements digest and bind attempt audit records back to
that same trial. Database checks permit only `authority_mode=disabled`,
`allocation_epoch=0`, and unassigned `claim_state=queued` rows; database
triggers reject update, delete, and truncate operations.

The Python store requires a SERIALIZABLE session already operating as the
exact non-login owner. Exact replays converge without duplicate rows or audit
events, identity conflicts roll back without fragments, and persisted JSON and
digests are revalidated on read. A separate startup gate checks only the
protected schema's qualified revision table and never falls back to an
application or management database URL.

Package 2A changes no running route or environment behavior. It cannot assign
or claim an attempt, mint a worker credential, start or cancel a worker, call a
Slurm controller, or release physical capacity. Its migrations are not
deployed by this implementation slice. Package 2B adds the trusted environment
agent and protected admission transitions; Package 2C closes every legacy
capacity mutation path and supplies rollback compatibility before enforcement
can be considered.

## Package 2B disconnected admission preparation

Package 2B registers one candidate-independent environment agent, captures a
bounded protected demand observation, and persists manager-selected placement,
bootstrap, and worker bindings only as prepared, non-executable records.  Its
attempt lifecycle can model inert assignment, withdrawal, and cancellation;
its SERIALIZABLE claim inspection always denies and cannot create a protected
claim lease.  The immutable activation row remains disabled at epoch zero with
an executable ceiling of zero and live-claim entry disabled.

## Package 2C1 inert legacy-authority fence

The first Package 2C slice binds the exact machine-readable mutation inventory
to a bounded set of writer-domain cursors.  Every one of the twenty mutation
paths must be represented, while paths backed by more than one authority—such
as the distinct OLDLAB and GB10 physical writers—retain separate incarnation,
epoch, high-water, and policy-digest cursors.  A prepared compatibility record
is expiring, append-only, and non-executable.  It cannot coexist with a
prepared global admission in either serialization order.

A monotonic freeze record requires a one-to-one acknowledgement for every
prepared writer domain at the identical epoch and high-water mark.  The agent
can store only fixed-shape, canonical records through two protected functions;
candidate roles and `PUBLIC` receive no access.  Preparing or freezing these
records does not invoke a legacy writer, change a candidate route, mutate a
public trial, create a claim, mint a credential, or touch Slurm.  Inventory
entries remain activation-blocking and `open` until later Package 2C slices
instrument and fence the corresponding live writers.

## Package 2C2 lifecycle-aware demand projection

The second Package 2C slice closes the ambiguity between the immutable
Package 2A queued-attempt row and the current append-only Package 2B lifecycle.
The legacy v1 capture remains available for rollback only while every protected
attempt is currently pending-unassigned; it rejects the complete capture after
an assignment or terminal transition instead of misreporting that attempt as
new demand.

The lifecycle-aware v2 capture reads one stable protected projection under the
same writer mutex used by plan and lifecycle mutations. A trigger-maintained,
monotonic lifecycle head avoids rescanning append-only event history. A partial
current-demand index and a `max_attempts + 1` source limit bound each capture;
deferred pending attempts count toward that source bound even though they are
the only nonterminal rows omitted from the emitted view.

Pending attempts carry no assignment fields. Assigned attempts carry their
exact allowance, plan, admission incarnation, allocation epoch, pool and
profile generations, profile digest, semantic shape, shape instance, and
submission intent, and become manager `CurrentAssignmentV1` records rather
than pending slots. A missing or incompatible prepared binding, a legacy pool
projection, or disagreement with the public trial state rejects the whole
observation before reporter high-water advances.

A terminal transition that observes a still-runnable public trial appends a
blocker; migration backfill creates the same evidence for pre-existing terminal
heads. Only a later append-only public non-runnable observation can advance the
blocker's protected `resolved_at` projection. A partial unresolved-blocker
index keeps steady-state capture independent of resolved terminal history.
Public-state regression and retry-generation registration remain activation
blockers for the later submission-boundary package; this slice records exact
evidence at the terminal transition but does not claim that later writer fence.

This remains a disconnected and non-executable projection. It adds no route,
runtime reporter loop, public mutation, credential, live claim, worker launch,
Slurm operation, or activation authority. The protected ceiling remains zero.

## Package 2C3 inert trial-submission registration

The third Package 2C slice corrects the initial protected attempt key before
submission cutover. `execution_generation` is the deployment generation bound
at submission, not a retry counter. Protected attempts now also carry a
monotonic `attempt_sequence`; the initial attempt is sequence zero, and a later
retry can retain the same deployment generation while receiving a fresh
protected identity and sequence. The old uniqueness rule that allowed only one
attempt per trial and deployment generation is removed. Existing Package 2A
attempts backfill to sequence zero without updating their append-only rows.

One candidate-independent agent procedure can now register an already-created
initial public trial. It accepts only canonical, hash-verified contracts bound
to the exact registered disabled fence, stamps the current deployment
generation, normalizes the public pool pin into the sealed requirement, rejects
nonqueued, cancelled, assigned, retried, drifted, or malformed public rows, and
creates the sealed requirement, sequence-zero attempt, pending-unassigned
lifecycle event, and bounded audit atomically. Exact replay converges; a
conflicting identity or requirement fails closed. The agent receives only
`EXECUTE` on this procedure and no direct protected-object privilege. The guard
owner remains read-only on the public trial projection.

This slice deliberately does not call the procedure from a candidate route and
does not claim that the live `trial-submission` inventory entry is closed. The
candidate public insert and protected registration are not yet one transaction,
and the legacy direct public writer is not yet fenced. Those are mandatory
cutover conditions for the later live submission boundary. Until then the
procedure is disconnected, every inventory entry remains `open`, and the guard
stays disabled with zero executable capacity and no live-claim entry.

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
