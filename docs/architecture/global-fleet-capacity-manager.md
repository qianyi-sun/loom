# Global Fleet Capacity Manager

The global capacity-manager service computes deterministic, fleet-wide
allocations from versioned fleet configuration, subject configuration, demand
reports, pool observations, and current commitments. Shadow allocation and the
v1 executor protocol remain non-executable evidence surfaces. A separate v2
ledger can persist sealed executable allocation epochs and issue exact,
short-lived launch permits after a fenced execution epoch is activated.

The manager still has no worker-claim mutation or scheduler client. A v2 permit
can be consumed only by the exact registered pool executor; the executor and
protected admission paths that would turn it into physical capacity are
separate activation blockers. The checked-in Package 5A deployment remains
inert at an executable ceiling of zero.

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
records, fenced execution preparation/activation evidence, executable-v2 pool
work, reconciliation requests, and read-only status/audit queries. Its database
enforces:

- executable work has one exact prepared execution epoch, manifest, fleet
  release, pool generation, and executor incarnation;
- executable intents descend only from sealed executable allocation epochs;
- executor and intent high-waters and states advance monotonically;
- command and protected-release receipts are append-only; and
- reporter sequence, writer fences, idempotency, and replay fail closed.

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

## Fenced executable-v2 work queue

Executable-v2 state is physically separate from the dry-run-v1 ledger. The
manager serves a pool only while the authority, execution epoch, manifest,
executor registration, executor incarnation, pool generation, and latest
sealed allocation epoch all match. Proposal, acceptance, bootstrap, permit,
consumption, close, and partial release commands share one authority-first lock
order and append exact command receipts.

Permit issue and consumption recheck global, tier, account, subject, pool,
pending-job, pending-slot, rate, topology, selected-node, executor-lease, and
inventory-freshness bounds. The final database-time fence includes the
earliest deadline of every pool observation used for global accounting. A
newer allocation epoch supersedes unused older work, and expired allocation
inputs cannot create or consume a permit.

Consumption moves an intent to `submitting-unknown`, which remains charged
until signed inventory observes physical work or the executor publishes an
exact post-consumption recovery command. Recovery requires a fresh complete
inventory plus authenticated controller evidence that both the submit process
and scheduler submission are absent; it moves the intent only into the normal
protected close path. It never frees capacity from an empty observation alone.

Protected release acknowledgements are authority-validated before replay and
stored as append-only, strictly increasing receipts. An old exact replay stays
valid after a successor is recorded, while physical release uses only the
highest retained protected registration epoch and matching terminal evidence.
Database triggers use `search_path=pg_catalog`, qualify queue roots through
`public`, reject illegal direct-SQL state/high-water changes, prohibit executor
unfencing, and make command and protected-release receipts immutable.

Accepted multi-shape reservations use one batched manager admission plan for
the complete subject/pool tranche. After the first covered intent reaches
`bootstrap-acknowledged`, the manager delivers the plan's exact shapes and
allowances to the subject reporter. The capacity agent enriches those manager
identities only with guard-local execution generations, sealed requirements
digests, and lifecycle sequences. It then commits the prepared plan, worker
shapes, placement allowances, and every protected assignment transition in one
serializable transaction. Any changed or incomplete local fact rolls back the
whole convergence. Only that committed transaction yields the exact
acknowledgement returned to the manager; the manager rejects a partial,
rebound, or replay-equivocated assignment set.

Admission acknowledgement does not bypass bootstrap protection. An intent
becomes `launch-ready` only when its exact protected bootstrap and the complete
batched admission plan are both acknowledged; `bootstrap-acknowledged` remains
the launch barrier until the plan acknowledgement is stored. PR #1425 completed
the protected manager-plan, capacity-agent admission, exact-assignment, claim
lifecycle, and release-acknowledgement mutation path. Public task-claim routes
remain disconnected, and the shipped executable ceiling remains zero until an
operator performs the protected activation sequence.

This queue is executable authority, not scheduler actuation. The manager has
no scheduler client and never calls Slurm from its HTTP routes. A separate
controller-local active executor can consume those exact commands only under a
matching activation artifact and manager execution context. The control-plane
CLI still exposes no apply, start, or ceiling-changing command; activation,
drain, and retirement are least-scope protected HTTP transitions.

## Controller-local Slurm inventory and active execution

The separate `loom_capacity_pool_executor` namespace in the Loom wheel can
capture one controller-local Slurm 23.11 snapshot with only `scontrol show
nodes --json` and `squeue --json`. It brackets the node read with two queue
reads. The fixed queue argv runs with protected `SQUEUE_ALL=1`; the runner
requires its effective UID to equal the dedicated non-root query UID. The
protected policy binds that UID, its query-principal identity, and a nonzero
evidence digest proving that the principal has complete job visibility under
the controller's `PrivateData` policy. All of those query semantics enter the
controller evidence digest. The adapter accepts only allocation-identical
queue documents whose finite positive `last_update`, exact cluster, Slurm
patch release, and data-parser identity match the protected policy and node
document. Every protected node must retain its exact CPU, memory, GPU, and
partition envelope. The adapter maps OLDLAB's uppercase controller names back
to canonical fleet IDs. The protected policy, rather than a compiled range,
selects GB10 nodes; the current safe set is 1 through 9 and 11 through 15.
Node 10 remains outside that set until it has the accepted partition envelope,
while physical node 16 remains outside Loom authority.

One accepted snapshot produces both `PoolObservationV1` and
`ExecutableExecutorInventoryV2`. Healthy busy nodes remain visible; current
jobs, node-less nonterminal jobs, pending arrays, GPU/TRES use, and unavailable
canonical nodes stay charged. A node-less job's canonical comma-separated
partition set is charged whenever any eligible partition reaches the protected
nodes; malformed, empty, or duplicate partition entries fail closed. Per-node
allocation counters are reconciled against visible jobs, and any hidden
residual or ambiguity becomes a quarantined node or full-pool charge. The
subprocess runner owns fixed `/usr/bin` binaries, a digest-bound root-owned
`/etc/loom/capacity/slurm.conf`, a minimal environment, bounded output and
timeout, and cancellation-safe child reaping. Every foreign or ambiguous
physical record is quarantined and therefore cannot authorize a capacity
increase.

The checked-in prepared systemd package uses that inventory path only at an
effective ceiling of zero and cannot construct the scheduler backend. The
separate active oneshot/timer requires an owner-only exact
`ActivationRuntimeArtifactV2`, a positive approved profile-set digest, and the
manager's exact active or drain-only context before constructing its fixed
Slurm submit/cancel backend. Drain atomically zeros ceiling and rate while the
active timers continue release cleanup and final inventory publication;
retirement requires fresh retirement-safe checkpoints from both pools and
every executable intent released.

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
subject projection, report ingestion, dry-run-v1 executor records,
executable-v2 checkpoint/work/inventory/command routes, protected-release
acknowledgements, reconciliation, status, audit, health, and metrics. Mutual
TLS authenticates the transport, hashed bearer principals bind the exact
operator, reporter, manager, or pool-executor authority, and metric labels
never contain subject IDs or dynamic environment names.

The manager service has no scheduler client, Slurm mutation, or direct
physical-release client. Protected claim admission and lifecycle convergence
are mediated through exact manager plans, the capacity agent, and the personal
guard mutation surface merged in PR #1425; they do not expose an ordinary
public claim route. The packaged deployment's mTLS startup/readiness probe
observes any exact ready nonnegative ceiling so the Service remains routable
during activation and drain. The separate operator `status` command continues
to require the exact zero-ceiling boundary.

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

The base guard remains disabled at allocation epoch zero. Its ordinary
prepared bindings are non-executable, and normal submission and claim routes
do not use it to authorize work.

A separate least-privileged executor role can call serializable protected
procedures that prepare an executable intent, bind its exact Slurm job,
register or drain its worker incarnation, admit an exact protected attempt,
project an admitted claim's terminal evidence, and acknowledge terminal worker
release. Claim admission requires the attempt's current protected assignment to
name the same intent, allocation epoch, pool, and shape as the exact registered
worker; an unassigned or cross-intent attempt fails before a lease can persist.
Guard 0020 activation is fail-closed: it requires zero pre-exact-assignment
`executable_claim_leases` rows because guard 0013 stored no immutable
claim-to-assignment transition reference, so a later lifecycle head cannot
prove the claim's exact assigned intent at admission time.
Preparation requires the exact append-only protected bootstrap
registration produced by the trusted demand agent, including its subject,
intent, full execution binding, command sequence, epochs, bootstrap hash, and
receipt digest. These transitions are append-only, bind the subject, candidate
publication, execution fence, worker, requirements digest, and monotonic
high-water marks, and serialize claim admission against terminal lifecycle
projection. A candidate or deployment reconfiguration immediately denies new
claims to workers registered under the prior binding while preserving their
credential-authenticated drain and release cleanup path. The candidate role,
the trusted demand-agent role, and `PUBLIC` have no access to this surface.
Personal-development provisioning creates the executor role sealed as
`NOLOGIN`; no checked-in candidate route, worker route, or deployed daemon
invokes the procedures. Consequently this protected surface cannot consume
manager work or change physical capacity while the global executable ceiling
and deployment entry points remain zero.

Implementation lives under `src/loom_capacity_manager/`,
`src/loom_capacity_executor/`, `src/loom_capacity_pool_executor/`, and
`src/loom_capacity_agent/`. The service, executor, and protected-store
integration suites prove v1/v2 ledger isolation,
exact executable bindings, fail-closed ambiguity, and protected release before
capacity is uncharged.

## Package 5A render-only control-plane foundation

Package 5A packages the single management authority without activating it. A
strict profile at
[`deploy/dev-fleet/capacity-control-plane.toml`](../../deploy/dev-fleet/capacity-control-plane.toml)
renders one independent capacity PostgreSQL instance, one migration/authority
bootstrap Job, one manager Deployment and ClusterIP Service, and
component-scoped least-access NetworkPolicies in `loom-dev`. The manager
release image is published as `loom-capacity-manager` for native AMD64 and
ARM64 and runs as UID/GID 65532.

Release publication preserves source-accurate, immutable evidence. Each native
archive is rebuilt from the protected release commit inside the hosted
`publish` job and is checked by Trivy v0.70.0 using image scanning, OS and
library vulnerabilities, a `10m0s` timeout, `CRITICAL` severity, exit code 1,
unfixed findings included, the vulnerability scanner only, and no cache. A
repository helper writes the fixed config and reviewed ignore file outside the
checkout before each scan. Fixable findings must be removed by updating the
image or dependency. The expiring exceptions cover the four unfixed Perl CVEs
(CVE-2026-13221, CVE-2026-42496, CVE-2026-57433, and CVE-2026-8376) only on
the Debian Perl packages required by Debian base runtimes, the agent toolchain,
and the staging-compatible PostgreSQL 17.4 rehearsal image, CVE-2026-43185
only on the agent compiler's
`linux-libc-dev`, and CVE-2025-7458, CVE-2026-6653, and CVE-2023-45853 only on
required PostgreSQL 17.4 rehearsal dependencies. Every entry includes exact
Debian PURL scopes, its review statement, and 2026-09-12 UTC expiration in the
signed predicate. Policy
generation fails closed at that boundary. A repository-owned installer accepts
only the architecture-specific v0.70.0 release archive whose repository-pinned
SHA-256 matches; no policy-forbidden third-party action is required. The signed
predicate binds the scanner identity, release URL and architecture archive
digest, complete policy-file identities, explicit exception metadata, and scan
report. Its only publication mode is `trusted-rebuild`, bound to the protected
release head, tree, ref, and current
run. PR candidate archives remain untrusted CI evidence only: the publisher
never downloads, loads, scans as release, attests, or publishes those bytes.
Each architecture push contributes its emitted digest directly to a canonical
post-verification record, rather than allowing a later mutable-tag lookup.

The manifest job accepts exactly the current image's AMD64 and ARM64 records,
validates their release and mode identities, verifies the registry
attestations at the recorded immutable subjects, and joins only those digests.
It records the temporary manifest's creation digest once and performs registry
validation, attestation, and attestation verification through that immutable
digest. The official release SHA and branch tags move only after final
verification succeeds. These publication controls produce the inert Package
5A image; by themselves they do not apply infrastructure, activate authority,
or execute capacity.

The Kubernetes namespace `loom-dev` is the shared infrastructure home, not the
logical shared-development demand subject. The one authority accounts for all
four demand classes: production; staging; shared development (the logical
`development` subject under the `shared-development` account); and personal
development (each `dev-<name>` subject backed by a `loom-dev-<name>` application
namespace). All four share the operator-defined physical OLDLAB/GB10 capacity
according to their tiers and limits.

The renderer requires a digest-pinned manager image and reviewed non-nil
authority UUID. It references, but never creates or prints, the existing
`loom-capacity-manager` Secret. That Secret supplies PostgreSQL identity,
`database-url`, bearer-principal and executor-public-key registries, manager
server/client trust, and the dedicated health client certificate and key. The
exact key contract and evidence commands are documented in the
[`deploy/dev-fleet` operator notes](../../deploy/dev-fleet/README.md).
Only credential-preparation init containers mount that projected Secret. They
copy the bounded, exact key set to mode-0600 UID-owned files on a memory-backed
volume; the migration and manager application containers mount only that
prepared runtime directory, read-only. A held projected-generation descriptor
and pre-install rebinding check prevent a Kubernetes `..data` rotation from
mixing credential generations.

The schema migration writes a canonical seed event beside its generated
bootstrap authority UUID. A reviewed replacement requires that one pristine
seed and writes an append-only binding event in the same locked transaction.
Legacy markerless state allows only same-UUID backfill. Duplicate, malformed,
contradictory, or different later reserved evidence fails closed even before a
writer registers. Percent-encoded database
URLs retain their SQLAlchemy meaning: percent escaping occurs only at the
Alembic ConfigParser boundary.
Migration connections have fixed connect, lock, and statement timeouts, the Job
has an active deadline, and PostgreSQL startup is protected by a bounded startup
probe before liveness begins.
The DNS-label-safe, length-bounded migration Job name incorporates the
migration head and manager image digest plus a digest of the canonical complete
Job spec and exact head. Any immutable spec change therefore renders a new Job
instead of colliding with an old template.

The control-plane CLI commands are deterministic `render` and read-only
`status`; executor rendering now includes separately artifact-bound active
config and environment outputs. The status path performs a real in-Pod mTLS
probe. The probe first verifies that the mounted server certificate contains
both the `127.0.0.1` IP SAN and the
`loom-capacity-manager.loom-dev.svc.cluster.local` DNS SAN, then succeeds only
for the exact canonical response
`{"executable_new_capacity_ceiling":0,"status":"ready"}`. The CLI has no
apply, install, start, external-exposure, HTTP-transition, or ceiling-changing
operation. Protected activation/drain/retire live on the manager API, and the
separate controller-local active executor package owns Slurm actuation. Merging
repository support does not authorize a live deployment; apply and activation
remain reserved for #906's explicit operator change window.

## Current activation blockers

There is intentionally no live global fleet manifest. Repository support can
render the manager and prepared/active executor artifacts and exposes protected
activation, drain, and retirement transitions, but it does not apply or start
them. The checked-in
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

The sealed allocation/work queue, protected claim/admission/lifecycle routing,
atomic activation/drain/retire transitions, and active executor package are
implemented. Live use remains blocked on reviewed real-fleet evidence, exact
fenced OLDLAB and GB10 executor installation, live personal-lifecycle
convergence, mixed-workload containment tracked by issue #896, GB10
health/capacity convergence, and the evidence and explicit operator window
tracked by issue #906. Until those activation-boundary gates pass, the global
manager remains undeployed and inert at zero; existing environment-local
OLDLAB and GB10 autoscalers remain the live writers.

## Verification

The capacity gate runs contract, state, topology, allocator, store, API, mTLS,
property, migration, and offline-driver tests; Ruff; strict Mypy; compilation;
whitespace checks; and a scheduler/process/path source audit. Integration tests
prove v1 isolation, exact current-allocation fencing, crash recovery,
authority-first release replay, hostile-search-path safety, direct-SQL guards,
and upgrade/downgrade/re-upgrade parity. Deployment tests separately prove the
checked-in Package 5A remains at a zero executable ceiling.

## Protected executable bridge package

The executable-v2 package has one global manager spanning production, staging,
shared development, and personal-development subjects, and exactly one
controller-local executor for OLDLAB and GB10. Users do not configure pool
weights, QoS, shapes, profiles, or priorities: `min_slots` defaults to zero,
architecture-specific demand constrains eligibility, and neutral placement is
manager-owned. `loom-dev` is shared infrastructure; personal namespaces are
`loom-dev-<owner>`, never `loom-dev-shared`.

The checked-in executor profile has immutable images and an exact zero
executable ceiling. A positive runtime is a separate owner-reviewed artifact
bound to the exact active execution context and approved launch profiles.
Rendering and systemd validation are non-installing; no merge authorizes
activation or live infrastructure mutation. The manager's v2 status may report
exact active physical Slurm-job intent, but only a matching fresh protected
personal guard registration, with no later release/drain, can make a worker
available. Scheduler evidence and pod readiness alone are insufficient.
