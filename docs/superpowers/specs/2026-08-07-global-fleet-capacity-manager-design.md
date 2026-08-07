# Global Fleet Capacity Manager Design

Status: approved in conversation; written specification pending user review

Date: 2026-08-07

## Summary

Loom will have one logical global capacity manager for every environment that
consumes Loom-managed OLDLAB or GB10 worker capacity:

- `production`;
- `staging`;
- the shared `development` environment; and
- every dynamic `dev-<name>` environment.

The manager is the only allocation writer. It makes one resource-aware,
priority-aware decision across both physical pools and publishes fenced,
versioned grants. One controller-local fleet executor per physical Slurm pool
applies those grants. Environment capacity agents publish runnable demand,
apply manager-issued task-placement allowances, and enforce local admission.

The design has no user-configured pool weights. Users configure an aggregate
`min_slots` and `max_slots`; `min_slots` defaults to `0`. The manager places
architecture-specific and architecture-neutral work according to complete
capability matching, pool health, resource feasibility, priority, fairness,
and stable existing assignments.

Capacity reclamation is always drain-first. Loom cancels excess pending worker
jobs, prevents excess new task claims, drains idle workers, and allows running
trials to finish. It does not hard-preempt running trials and never cancels
foreign Slurm jobs.

## Motivation

The repository now contains most of the mechanics needed to run Slurm-backed
workers on both OLDLAB and GB10. In particular, merged PR #1205 established
pool-local external Slurm execution, dual-pool demand routing, scale-to-zero,
and controller-specific supervision.

Those mechanics do not yet constitute a correct global allocation system:

- production, staging, shared development, and dynamic development still have
  independent allocation paths;
- the current global development authority covers only development identities
  and uses a submit-host SQLite ledger;
- dynamic development currently registers one logical `dev-<name>` pool
  instead of physical `oldlab` and `gb10` policies;
- current neutral routing happens independently inside each environment and
  can make choices without a fleet-wide view;
- the current scheduler can claim unassigned compatible tasks;
- per-environment Slurm timers and production-pressure reclamation can compete
  with a future global writer;
- scalar slot budgets do not describe the differing CPU and memory costs of
  all environment/pool worker profiles;
- development candidate selection is service-wide rather than per instance;
  and
- the lifecycle API cannot update a ready instance's candidate or capacity
  without destroy/recreate.

A single process also cannot safely operate both Slurm controllers. OLDLAB
commands must run on the OLDLAB controller and GB10 commands must run on the
GB10 controller. Therefore the correct topology is one logical allocation
brain with controller-local execution, not one process that invokes both
Slurm installations.

## Goals

1. Make one component authoritative for all Loom capacity allocations on
   OLDLAB and GB10.
2. Let development workloads use either physical pool when compatible.
3. Support simultaneous independent development deployments without mutable
   shared candidate, path, credential, or capacity state.
4. Enforce physical-pool, priority-tier, owner, environment, and deployment
   limits without double allocation.
5. Make architecture-neutral placement automatic and stable without exposing
   pool weights.
6. Preserve running trials during scale-down, priority reclamation, rollout,
   manager failure, and environment deletion.
7. Fail safely across manager, database, reporter, executor, network, and
   controller failures.
8. Provide an auditable cutover from all existing allocation writers with no
   dual-writer interval.
9. Make activation measurable, reversible, and safe at zero initial capacity.

## Non-goals

- Reimplementing the GB10 or OLDLAB Slurm actuator mechanics delivered by
  #1205. This design wraps and refactors those mechanisms behind exact grants.
- Killing running trials to satisfy priority immediately.
- Managing, preempting, or cancelling non-Loom Slurm jobs.
- Allowing users to configure priority classes, physical resource profiles,
  pool weights, or Slurm QoS.
- Treating a grant lease timeout as proof that physical capacity disappeared.
- Providing automatic failover to a second Slurm execution host. Each physical
  controller remains the sole execution authority for its cluster.

## Terminology

- **Environment**: a control-plane identity such as `production`, `staging`,
  `development`, or `dev-alice`.
- **Capacity account**: the fairness and quota identity. A dynamic development
  account is its immutable owner user; static environments have
  operator-defined service accounts.
- **Priority tier**: an operator-owned class. The default order is production,
  staging, then development.
- **Physical pool**: a Slurm execution domain, initially `oldlab` or `gb10`.
- **Slot**: capacity for one concurrent Loom trial. It is the user-facing unit.
- **Resource vector**: the operator-owned CPU, memory, GPU, and other physical
  costs of one slot for one environment/pool worker profile.
- **Demand bucket**: runnable task demand grouped by compatible-pool set,
  execution generation, and local task-priority band.
- **Commitment**: capacity that is proposed, accepted, pending, active,
  draining, unobservable, or otherwise not proven released.
- **Grant**: a fenced manager decision for one environment, physical pool,
  candidate, and deployment generation.
- **Placement allowance**: the number of unclaimed trials in a demand bucket
  that an environment agent may assign to one physical pool.

## Authoritative Topology

```text
static environment state       dynamic dev-instance lifecycle
           |                                |
           +---------- management API ------+
                              |
                    management PostgreSQL
                              |
          environment demand  |  pool observations
                    reporters | executors
                         \     |     /
                          \    |    /
                   global capacity manager
                     (one fenced writer)
                              |
             versioned grants and allowances
                       /             \
                      /               \
         OLDLAB fleet executor     GB10 fleet executor
          OLDLAB controller         GB10 controller
                      \               /
                       environment agents
                              |
                 scheduler admission and task DB
```

### Global capacity manager

The manager runs on an always-on management plane that does not consume the
worker capacity it controls. Multiple replicas may be installed, but only the
replica holding the PostgreSQL authority lock and current writer epoch may
publish allocations.

The manager:

- reads authoritative configuration, fresh demand snapshots, pool readiness,
  accepted commitments, executor observations, and release acknowledgements;
- computes one allocation epoch across the complete registered cohort;
- reserves capacity before any external side effect;
- publishes exact desired capacity and placement allowances;
- records every allocation decision and reason; and
- never invokes Slurm or handles worker bootstrap secrets.

### Environment capacity agent

Each environment has one logical capacity agent and worker-claim guard.
Multiple process replicas are safe because the environment database fences
application by allocation epoch. This component is installed from a trusted
fleet release independently of the environment candidate. A dynamic
development candidate cannot replace or modify it.

The agent:

- publishes bounded, versioned demand buckets derived from the scheduler's
  runnable predicate;
- reports the current candidate, deployment generation, configuration
  generation, task assignments, claims, and agent health;
- applies accepted placement allowances using database compare-and-set;
- applies an accepted admission epoch, retained-worker claim leases, excess
  worker drain states, and placement changes in one environment-database
  transaction;
- clears only unclaimed assignments when an allowance is withdrawn;
- owns worker-token minting and the queued-to-claimed transition used by real
  fleet workers; and
- cannot raise its aggregate minimum, maximum, tier, owner quota, or physical
  resource profile.

The agent uses a protected database role and schema. Candidate runtime and
migration roles cannot mutate capacity grants, worker claim leases, allocation
epochs, or token authority. Candidate services may create and manage their own
workload records only through the bounded interfaces granted to them.

### Pool-local fleet executor

There is one fleet executor for OLDLAB and one for GB10. Each runs on the exact
controller validated by the current external-autoscaler authority checks. A
controller-local lock and one installed service prevent competing local
processes.

An executor:

- reads only grants for its physical pool;
- must accept an upward grant atomically before submitting a Slurm job;
- applies the existing autoscaler and Slurm job reconciliation mechanisms;
- reconciles every registered environment for that pool with per-environment
  failure isolation;
- cancels only Loom-owned pending jobs;
- executes the Slurm cancellation and release mechanics for the worker drain
  plan committed by the environment-local grant application;
- verifies terminal release against both Loom control-plane state and Slurm
  state;
- reports pool-scoped observations and acknowledgements; and
- cannot grant, transfer, or prioritize capacity.

The executor uses environment-scoped protected bindings. It does not receive a
fixture-wide database administrator credential through a capacity grant.

## Configuration Authorities

### Environment capacity policy

The management database is authoritative for each environment's:

- capacity account;
- priority tier;
- aggregate `min_slots`;
- aggregate `max_slots`;
- optional `rollout_surge_slots`, defaulting to `0`;
- lifecycle state;
- desired candidate and deployment generation;
- enabled physical-pool profiles; and
- monotonically increasing configuration generation.

`min_slots` defaults to `0` and must not exceed `max_slots`. Dynamic users may
change minimum and maximum values only within their operator-configured owner
quota. The aggregate minimum across one owner's live development environments
must not exceed that owner's reservation quota.

Priority membership is derived server-side. A dynamic development user cannot
promote an environment to staging or production.

Static policies are applied through environment-state tooling to the
management API. Dynamic policies and the dev-instance registry change in the
same management-database transaction so there is no second capacity source of
truth.

### Priority-tier policy

The default strict order is:

```text
production > staging > development
```

Each tier has an aggregate slot ceiling and, where needed, aggregate resource
ceilings. Shared `development` and all dynamic `dev-<name>` environments are in
the same development tier and consume the same development ceiling.

Tier policy is operator-owned. Lower tiers may borrow unused compatible
capacity, but a new higher-tier demand starts drain-first reclamation.

### Physical-pool policy

Each physical pool publishes an operator-approved capacity envelope:

- supported capabilities and architecture;
- configured Loom slot ceiling;
- CPU, memory, GPU, and other allocatable resource totals;
- reserved infrastructure headroom;
- pending-launch ceiling;
- allowed nodes and Slurm authority identity;
- health and scale-up eligibility; and
- a monotonically increasing pool-configuration generation.

The configured Loom envelope, not momentary Slurm free-node count, is the hard
global limit. Transient foreign workload can delay a Loom job but cannot cause
the manager to duplicate grants or flap the configured envelope.

### Environment/pool worker profile

An operator-owned worker profile binds one environment generation to one
physical pool. It includes the per-slot resource vector, worker shape,
candidate artifact, protocol version, trusted launcher configuration, local
safety ceiling, and protected binding references.

User-authored task or candidate data cannot change this profile.

## Management Data Model

The implementation will add versioned management-plane records equivalent to:

- `capacity_authority_state`: authority incarnation, writer epoch, schema
  version, recovery state, and global pending-launch ceiling;
- `capacity_tiers`: priority order and aggregate ceilings;
- `capacity_accounts`: immutable owner/service identity, aggregate min/max
  quota, live-environment quota, and fairness state;
- `capacity_pools`: capabilities, resource envelope, pending ceiling, health,
  and configuration generation;
- `capacity_subjects`: environment, account, tier, min/max/surge, lifecycle,
  candidate, deployment generation, and configuration generation;
- `capacity_candidates`: immutable source, artifact, architecture, launcher,
  attestation, and protocol bindings;
- `capacity_worker_profiles`: environment/pool/generation resource cost,
  artifact and protocol binding, and readiness;
- `capacity_demand_snapshots`: versioned compatibility buckets, fixed claims,
  freshness, and reporter acknowledgement;
- `capacity_allocations`: desired slots, accepted reserved slots, unaccepted
  proposed slots, releasing slots, placement allowances, grant fencing fields,
  expiry, and state;
- `capacity_executors`: pool-scoped identity, authority binding, accepted
  high-water mark, heartbeat, inventory state, and local-authority evidence;
- `capacity_executor_observations`: pool inventory, pending/active/draining
  counts, terminal evidence, adoption results, and executor fencing fields;
- `dev_lifecycle_limits`: global provisioning/build concurrency and global
  live-instance limits; and
- `capacity_audit_events`: bounded, secret-free configuration, allocation,
  acceptance, drain, release, recovery, and operator events.

Environment databases will add records equivalent to:

- a mirrored capacity-admission grant keyed by physical pool, candidate,
  deployment generation, and allocation epoch;
- placement-allowance consumption keyed by the same epoch; and
- worker claim-authorization epoch plus trial execution-generation and
  physical-pool assignment fields.

Admission records and transition functions live in a protected schema owned by
the trusted capacity role. Candidate runtime and migration roles have neither
DDL authority over that schema nor direct permission to perform the protected
queued-to-claimed transition. Column privileges and protected trigger/function
checks prevent those roles from changing worker claim epoch, worker drain
state, trial pool assignment, execution generation, or a queued trial into a
claimed trial outside the trusted transition. Candidate migrations cannot
disable or replace those guards.

Every contract has an explicit schema version and rejects unknown fields or
versions fail-closed.

## Capacity Accounting and Grant State

### Reservation-tranche states

```text
proposed -> accepted -> releasing -> released
```

The state machine applies to each reserved increase, not to the aggregate
allocation row as an indivisible object. One allocation may therefore contain
an accepted base and a proposed increase at the same time. An allocation may
also retain accepted reservations while its desired target decreases. The
manager tracks three separate aggregate quantities:

- `desired_slots`: the current target;
- `proposed_slots`: additional reserved capacity not yet accepted by the
  executor; and
- `reserved_slots`: accepted capacity not yet proven released.

The charged capacity for an allocation is conservatively equivalent to:

```text
max(reserved_slots, observed_nonterminal_slots) + proposed_slots
```

Before normal allocation, an executor imports any discovered Loom-owned
orphan into a quarantined reservation. A missing ledger row never makes an
observed Loom job free.

### Upward change

1. The manager reserves the increase as `proposed_slots`.
2. The pool executor atomically accepts the current, unexpired proposal.
3. Accepted proposal slots move into `reserved_slots`.
4. The environment agent may activate the corresponding admission and
   placement allowance only after acceptance.
5. The executor may submit Slurm work only after acceptance.

If the proposal expires before acceptance, the manager closes it atomically
and may reuse it. A late executor cannot accept a closed proposal.

### Downward change

1. The manager publishes a lower `desired_slots` and lower placement/admission
   allowance.
2. One environment-database transaction advances the admission epoch, clears
   excess unclaimed assignments, authorizes only a retained set of whole
   workers whose capacity does not exceed the new target, and marks every
   excess worker draining. The scheduler cannot observe the new epoch without
   also observing the worker fences.
3. The executor cancels excess pending jobs and applies the committed worker
   drain plan to Slurm.
4. `reserved_slots` remains at its old value while capacity is pending, active,
   draining, unknown, or not conclusively terminal.
5. The executor reports a fenced full inventory and terminal release.
6. Only then does the manager lower `reserved_slots` and reuse capacity.

Lease expiry is a scale-up and claim fence. It is never release evidence.

### Capacity reductions below existing commitments

An operator may reduce a pool, tier, owner, or environment limit below current
commitments. This creates an explicit over-limit draining state:

- all scale-up stops in the affected scope;
- deterministic priority and fairness rules choose capacity to drain;
- running trials continue; and
- the system remains visibly over limit until terminal acknowledgements arrive.

The system does not falsify accounting to make a reduced limit appear met.

## Demand Classification

The environment reporter and scheduler share one runnable predicate. A queued
trial contributes demand only if it is eligible apart from unavailable worker
capacity, including retry timing, attempt ceilings, family-run sequencing, and
other scheduler gates.

Compatibility uses the complete registered pool capability set. Initial
architecture behavior is:

- explicit `worker_pool=oldlab` or `worker_pool=gb10` is a hard pin and must be
  compatible with the remaining requirements;
- `cpu_arch=x86_64` requires an x86-compatible pool, initially OLDLAB;
- `cpu_arch=arm64` requires an ARM-compatible pool, initially GB10;
- `cpu_arch=any` may use any otherwise compatible pool; and
- missing `cpu_arch` remains `x86_64` for backward compatibility.

Unsupported or contradictory requirements are reported as non-capacity
blockers and do not cause worker scale-up.

Demand snapshots are bounded aggregates rather than raw task payloads. They
group runnable work by:

- compatible physical-pool set;
- execution candidate and generation;
- task-priority band;
- count; and
- oldest eligible submission time.

Claimed and running trials are fixed commitments to their current pool and
generation.

## Global Allocation Algorithm

The manager computes a deterministic allocation epoch from one consistent
management-database snapshot. Reporter observations are asynchronous, but
they cannot directly free capacity; only the manager processes fenced release
evidence.

The allocator applies these lexicographic objectives:

1. Preserve and charge all fixed, proposed, accepted, pending, active,
   draining, stale, unknown, and quarantined commitments.
2. Respect physical slot and resource-vector limits, tier ceilings, owner
   limits, environment aggregate limits, and rollout surge.
3. Serve priority tiers in strict order for each compatible resource domain.
4. Within one tier, satisfy aggregate environment minimums using hierarchical
   constrained progressive fairness by capacity account and then environment.
5. Within one tier, allocate task-backed demand using the same hierarchy.
6. Place single-pool demand before flexible demand within each fairness round
   so flexible work does not strand constrained work.
7. Preserve healthy accepted placements where they remain feasible.
8. Minimize churn and resource fragmentation, then use oldest waiting demand
   and stable identifiers as deterministic tie-breakers.

Priority is resource-local. Unmet production x86 demand prevents lower-tier
borrowing on OLDLAB, but it does not block lower-tier ARM demand on GB10.
Lower-tier work may use only capacity that is not currently needed by a
higher-tier compatible demand. The allocator does not backfill a contested
resource fragment with a long-running lower-tier trial that would delay the
higher tier.

Within development, fairness is hierarchical:

```text
development tier -> immutable owner account -> owner's environments
```

Creating more environment names therefore cannot create more top-level fair
shares. Static shared development has one operator-defined service account.

Progressive filling compares delivered concurrent-trial slots, first across
accounts and then across their environments. During the minimum phase it fills
one requested floor slot per eligible fairness round until a minimum is met;
during the demand phase it fills one task-backed slot per round until demand or
a ceiling is met. Resource vectors are hard feasibility constraints on those
rounds, not fairness weights.

There are no configurable fairness or pool weights. Resource costs and
compatibility are facts supplied by operator-owned profiles.

### Aggregate target

For one environment, before global constraints are applied:

```text
requested_slots = min(
    max_slots,
    max(min_slots, fixed_running_slots + runnable_queued_slots),
)
```

The allocator's grant may be lower than `requested_slots`. Observed
commitments may be higher than both during drain or after a limit reduction.

### Warm minimum placement

`min_slots` is aggregate and defaults to zero. When the minimum exceeds actual
task demand, the residual is warm capacity. The manager:

1. retains healthy existing warm capacity to avoid churn;
2. excludes pools without a ready candidate/profile or healthy executor;
3. preserves capacity needed by constrained task demand;
4. chooses the pool with the most feasible normalized headroom; and
5. uses deterministic architecture diversity and tie-breaking where headroom
   is otherwise equal.

A minimum is a desired reservation under finite capacity, not permission to
oversubscribe a pool or bypass higher priority.

### Pending-launch control

The manager issues an exact launch allowance in addition to desired capacity.
Proposed, accepted-but-not-active, and observed Slurm-pending capacity consume
the global, optional tier, and physical-pool pending ceilings. Executors cannot
choose a different environment to launch first merely because they iterate
grants in a particular order.

## Task Placement and Scheduler Admission

Every queued trial managed by the global authority must receive a physical
pool assignment before claim. This includes single-pool and multi-pool demand.

The environment agent applies an accepted placement allowance in one local
database transaction:

1. lock the current allowance epoch;
2. select the oldest locally eligible unclaimed trials in the matching demand
   bucket;
3. compare-and-set their physical pool and assignment epoch;
4. clear only excess unclaimed assignments from superseded epochs; and
5. publish the matching local admission grant.

The trial row is the assignment authority. The design does not claim a
distributed exactly-once transaction across the management and environment
databases. It achieves one current assignment through local compare-and-set,
idempotent epochs, and leases.

The trusted worker-claim guard permits a claim only when:

- the worker is active and not draining;
- the trial is assigned to the worker's physical pool;
- worker and trial execution generations are compatible;
- candidate and worker protocol bindings match;
- the worker's claim-authorization epoch matches the current admission epoch;
- the local admission grant is current and accepted; and
- all existing scheduler, team-quota, capability, family, and retry gates pass.

An unassigned globally managed trial is not claimable. This removes the
current neutral-routing bypass. Fleet worker credentials point at the trusted
claim path; candidate code cannot mint an alternative worker credential or
authorize a protected claim transition.

When an allocation shrinks, the environment-local transaction retains a
deterministic set of whole workers whose combined concurrency does not exceed
the new target and marks the rest draining immediately, including occupied
workers. Their current trials finish, but drain state and claim epoch both
prevent replacement claims. If existing worker sizes cannot represent the
exact target, the system temporarily undershoots after drain and the executor
launches a partially sized replacement worker to converge exactly.

## Priority Reclamation and Drain Semantics

The normal sequence is:

```text
publish lower desired grant
-> atomically advance admission, retain worker leases, and fence excess workers
-> revoke excess placement allowance
-> cancel excess Loom-owned pending jobs
-> let claimed/running trials finish
-> verify terminal control-plane and Slurm state
-> acknowledge release
-> reallocate released capacity
```

Normal drain timeouts create a visible blocker and alert. They do not force
termination. An owner may explicitly cancel their own trial through the normal
trial API; that is separate from capacity preemption.

Production may therefore wait behind a running staging or development trial.
The manager must display that commitment as the reason rather than granting
the same capacity twice.

Foreign Slurm jobs are never cancelled. Their presence may make a configured
Loom grant pending or temporarily unlaunchable. Flexible unclaimed work may be
moved to another compatible pool after a bounded anti-flap interval; pinned
work waits.

## Development Environment and Candidate Lifecycle

### Physical-pool identity

A dynamic environment's logical identity remains `dev-<name>`. Its worker
policies use physical pool names `oldlab` and `gb10`; `dev-<name>` is no longer
used as a synthetic physical pool.

The current singular dev-instance Slurm configuration becomes an exact
operator-owned map of physical-pool templates. Each enabled dynamic instance
gets both policies when both candidate/profile bindings are ready.

Protected worker files and artifact paths include environment, physical pool,
candidate, and deployment generation. A new generation never overwrites a
path that an old pending or running job can still read.

### CLI contract

`loom service up` requires an explicit environment and never infers a remote
target from the current checkout:

```text
loom service up --environment local
loom service up --environment dev-alice
loom service up --environment development
loom service up --environment staging
loom service up --environment production
```

The selector routes to the correct authority:

- `local` runs the local Compose workflow;
- `dev-<name>` submits the guarded create-or-update development lifecycle;
- shared development uses its authenticated environment deployment path; and
- staging and production use their existing protected rollout authorities and
  cannot bypass gates through a Compose or direct-Kubernetes shortcut.

Existing `loom dev create`, `status`, `list`, and `destroy` commands remain
lower-level lifecycle interfaces. A capacity/update operation is added so a
ready instance can change aggregate min/max without destroy/recreate.

### Per-instance candidate

Candidate selection is per environment rather than a single loom-service
setting. A create or update binds an immutable candidate descriptor containing:

- source commit and source-tree/content digest;
- immutable image and source-artifact digests;
- supported physical pools and architectures;
- worker and control-plane protocol versions;
- trusted launcher/profile digest; and
- publisher identity and bounded attestation.

Mutable tags and ambient controller checkouts are not candidate authorities.
Candidate publication is content-addressed and idempotent, so multiple users
may publish or deploy concurrently without sharing mutable paths.

### Untrusted candidate boundary

Development candidates are untrusted. Pool executors run an installed trusted
launcher and trusted containment profile. Candidate source cannot replace the
Slurm submission script, Compose security definition, host mounts, cgroup
limits, resource requests, token scope, or controller command path.

The capacity agent, worker-token authority, and worker-claim guard are also
installed from the trusted fleet release rather than the candidate. Candidate
runtime and migration credentials cannot alter their protected database schema
or impersonate their management/API identity. Protected column privileges and
database guards prevent candidate roles from restoring a drained worker or
manufacturing an authorized task claim.

Candidate builds that execute user source run in a bounded isolated builder,
not through an unrestricted host Docker socket. Activation remains gated on
the non-exclusive containment evidence required by #896.

### Deployment generations

Every trial records the execution generation active when it is submitted. A
trusted submission boundary stamps that value from the current lifecycle
state; candidate code cannot select an arbitrary generation. Every worker
registers its candidate and deployment generation. This prevents queued work
from silently crossing a candidate rollout.

During update:

- new submissions bind the new generation after the lifecycle cutover point;
- claimed and running old-generation trials finish on old-generation workers;
- unclaimed old-generation trials stay old unless the owner explicitly
  migrates or cancels them;
- old and new commitments both count against aggregate `max_slots`;
- optional `rollout_surge_slots` permits bounded temporary excess and defaults
  to `0`; and
- old worker credentials and artifacts are removed only after terminal release.

The control plane and database schema must declare compatibility with every
overlapping worker protocol. If compatibility is absent, the rollout drains
the old generation completely before activating the new one.

### Concurrent lifecycle limits

Capacity fairness alone does not protect the shared Kubernetes, PostgreSQL,
MinIO, DNS, or candidate-builder control plane. The development lifecycle also
enforces:

- a required operator-configured global live-instance limit;
- a configurable per-owner live-instance limit;
- per-owner aggregate min/max capacity quotas;
- a bounded fair provisioning/build queue; and
- per-environment operation-epoch serialization.

Independent environments may provision in parallel. Shared fixture mutations
use narrowly scoped locks and idempotent checkpoints. Two operations on the
same environment cannot overlap.

## Credentials and Security

- Capacity grants, demand snapshots, observations, audit rows, and status
  responses contain no secrets.
- Reporter credentials are environment-scoped and may publish demand only for
  their environment.
- Executor credentials are pool-scoped and may accept grants or publish
  observations only for their physical pool.
- Only the manager role may mutate allocations.
- Tier, quota, pool, and worker-profile configuration requires an operator
  role and is audited.
- Worker tokens are scoped to environment, physical pool, candidate, and
  deployment generation.
- Only the trusted environment capacity agent may mint those worker tokens or
  execute the protected queued-to-claimed transition.
- The trusted submission boundary stamps trial execution generation, and only
  an explicit authorized migration operation may change it while unclaimed.
- Revocation is generation-scoped; deleting an old generation cannot revoke a
  current generation's workers.
- Controller-local binding files are owner-only, immutable per generation, and
  referenced rather than embedded in grants.
- A pool executor rejects a grant whose environment/pool/profile binding is
  absent from its local allowlist.
- Network policy prevents candidate workloads from reaching management or
  controller authority endpoints.

## Failure and Recovery Semantics

### Stale or missing demand

A missing or stale demand snapshot means `unknown`, not zero and not deleted.
The manager freezes increases during a configurable grace period. If the
reporter remains stale, it lowers desired capacity and begins drain, but holds
the reservation until executor release evidence arrives.

Only an explicit lifecycle tombstone means permanent environment removal.

### Manager or management-database outage

Already accepted grants remain usable only for their bounded grace lease. No
component may exceed the last accepted capacity. When local admission expires,
new claims stop; executors cancel pending work and drain when able.

The manager does not reuse any capacity while its release state is unknown.
The management service and PostgreSQL authority must therefore run outside the
managed worker pools with production-grade availability.

### Environment outage

If an environment database or agent is unreachable, its demand and release
state are unknown. Existing pool commitments remain charged. Other
environments continue reconciling. No administrator database credential is
used to guess the unavailable environment empty.

### Pool-executor or controller outage

The manager issues no new grants to an unhealthy executor. All of that pool's
unreleased capacity remains charged. On restart, the executor enters inventory
and adoption mode before accepting new work.

### Crash around Slurm submission

The executor accepts capacity before `sbatch`. Submitted jobs include stable
Loom ownership, environment, pool, candidate, generation, allocation, and
executor-epoch metadata. If the process crashes between `sbatch` and the
control-plane record, restart inventory adopts the job rather than submitting
a duplicate.

### Database restore and authority incarnation

Grants include an authority-incarnation identifier and monotonically
increasing epochs. Executors persist their accepted high-water marks.

If a database restore rolls the authority behind an executor's high-water
mark, the executor rejects new grants and enters recovery. Recovery either:

1. advances the restored ledger beyond every observed executor high-water
   mark and imports all commitments; or
2. creates a new authority incarnation only after every old executor and job
   is fenced, inventoried, and adopted or drained.

Changing incarnation is never an automatic way to forget old capacity.

### Pool-capacity reduction or pool failure

Specific incompatible demand waits. Flexible unclaimed demand may move to a
healthy compatible pool. Running work remains fixed and charged. A configured
capacity reduction below commitments enters over-limit drain mode.

Pool health is based on executor heartbeat, validated Slurm authority,
candidate/profile readiness, and independent infrastructure checks. A missing
or zero capacity grant is not a pool-health failure, avoiding a circular
grant-missing deadlock.

## Observability

The management API and CLI expose, without secrets:

- manager incarnation, writer epoch, and last successful allocation;
- registered pools, resource envelopes, health, and executor heartbeat;
- tier, owner, and environment requested, desired, proposed, reserved,
  pending, active, draining, unknown, and released slots;
- candidate, deployment generation, and configuration-generation bindings;
- demand buckets and placement allowances;
- explicit allocation, block, drain, and release reasons;
- time since demand, grant, admission, executor, and terminal observations;
  and
- orphan/quarantine and over-limit states.

Required alerts include:

- multiple manager or pool-executor authorities;
- capacity-envelope or generation-binding violations;
- stale demand, admission, or executor heartbeat;
- accepted but unapplied grants;
- draining beyond timeout;
- unreleased or quarantined capacity;
- rejected stale epochs or authority incarnations;
- candidate/profile readiness failures; and
- any legacy allocation writer active after cutover.

Every manager decision includes a deterministic explanation suitable for
testing and operator diagnosis.

## Migration and Cutover

The migration must not create a dual-writer interval.

### Phase 1: schema and shadow reporting

1. Add the central management schema and scoped APIs.
2. Add environment demand reporting and local admission/placement tables with
   enforcement disabled.
3. Add pool inventory and observation reporting without Slurm mutation.
4. Register static and dynamic environments with global pool capacities set to
   zero.
5. Run the global manager in shadow mode and compare its decisions with live
   capacity without publishing executable grants.

### Phase 2: task and lifecycle readiness

1. Add physical `oldlab` and `gb10` profiles to dynamic environments.
2. Replace the synthetic `dev-<name>` physical pool identity while preserving
   it as a drain-only alias for any legacy worker.
3. Add mandatory task pool assignment and execution-generation binding behind
   a migration feature gate.
4. Add per-instance candidate and capacity update lifecycle operations.
5. Publish immutable per-pool candidate artifacts and protected bindings.

### Phase 3: pool-by-pool authority cutover

For each physical pool:

1. freeze new legacy scale-up;
2. stop and verify every per-environment executor timer for that pool;
3. disable the existing production-pressure writer for that pool at the
   planned fencing boundary;
4. acquire the controller-local fleet-executor lock;
5. inventory and adopt every Loom-owned Slurm job and worker, including legacy
   pool aliases and neutral assignments;
6. import current commitments into the global ledger;
7. publish matching accepted grants at the imported capacity;
8. enable local assignment/admission enforcement;
9. start the one pool fleet executor; and
10. verify no legacy unit or code path can submit or cancel a job.

The pool not yet cut over is treated as a fixed legacy commitment domain. The
global allocator does not move flexible demand into or out of it until its own
cutover completes.

### Phase 4: remove obsolete authorities

After both pools pass acceptance:

- disable and remove the global-development SQLite supervisor and broker
  handoff path;
- remove local policy-pressure neutral routing as an allocation authority;
- remove production-pressure capacity mutation;
- replace per-environment external timers with the two pool fleet executors;
- update environment-state profiles and dynamic provisioner templates; and
- retain compatibility readers only for the documented migration window.

### Rollback

Rollback first sets new grant proposals and scale-up to zero. Accepted and
running capacity remains charged and drains or is explicitly adopted.

A legacy writer may be re-enabled only after:

- the global executor for that pool is stopped and fenced;
- the global ledger has a terminal or transferred record for every commitment;
- current Slurm inventory is captured; and
- an explicit rollback authority epoch is recorded.

Emergency rollback prefers a safe no-scale state over running two allocation
writers.

## Testing Strategy

### Allocator and property tests

Generated scenario tests must establish:

- no increase when any physical, tier, owner, environment, generation, or
  resource-vector constraint would be exceeded;
- no proposal or launch when a global, tier, or pool pending ceiling would be
  exceeded;
- accepted or observed capacity is never reused before release;
- proposed capacity can be reused only when atomic acceptance is impossible;
- creating more environments does not increase an owner's top-level fair
  share;
- aggregate min/max spans both pools and all generations;
- strict priority has no cross-pool head-of-line blocking;
- single-pool demand is not stranded by flexible demand;
- no user pool weights influence placement;
- deterministic results for identical state; and
- convergence after demand, health, quota, and capacity changes.

### Scheduler and placement concurrency tests

Tests must race multiple environment-agent replicas and workers to prove:

- a globally managed trial cannot be claimed while unassigned;
- one trial row has one current assignment;
- a claimed trial cannot be reassigned;
- stale placement/admission epochs cannot authorize claims;
- draining workers cannot claim replacements;
- retry/requeue safely clears or renews assignment state; and
- candidate/generation mismatch is rejected.

### Grant and executor tests

Tests must cover:

- proposal acceptance and expiry races;
- manager leader loss and stale-writer rejection;
- executor local-lock exclusion;
- crash before and after `sbatch`;
- orphan adoption;
- pending cancellation failure;
- partial worker concurrency;
- stale, missing, or regressing observations;
- terminal proof; and
- authority-incarnation recovery after database restore.

### Lifecycle tests

Tests must cover:

- simultaneous creates and updates by different users;
- multiple environments owned by one user without extra fair share;
- global, owner, and provisioning-queue limits;
- per-instance candidate selection;
- immutable per-pool bindings;
- aggregate capacity updates with default `min_slots=0`;
- old/new generation overlap with and without surge;
- queued task generation binding and explicit migration;
- drain-first destroy and generation-scoped credential revocation;
- malicious candidate attempts to alter trusted launch containment; and
- malicious candidate attempts to alter admission state, mint worker tokens,
  or bypass the trusted claim transition.

### End-to-end acceptance

A bounded test fleet must demonstrate:

1. one dynamic development environment runs x86-specific work on OLDLAB;
2. one runs ARM-specific work on GB10;
3. neutral work is assigned once and uses healthy capacity without weights;
4. multiple owners share the development tier fairly;
5. one owner with multiple environments does not gain capacity;
6. aggregate environment and development-tier ceilings hold across both pools;
7. production demand drains borrowed lower-tier capacity without terminating a
   running trial;
8. manager, reporter, and executor outages do not double-allocate;
9. a candidate rollout does not mix trial or worker generations;
10. environment deletion waits for terminal release; and
11. foreign Slurm jobs remain untouched.

## Activation Boundary

Repository implementation is not live activation. Activation requires:

- management PostgreSQL and API availability independent of worker capacity;
- scoped reporter and executor RBAC;
- validated OLDLAB and GB10 controller-local executor installation;
- exact candidate artifact and trusted launcher publication for both pools;
- wildcard DNS/TLS and shared fixture readiness;
- PostgreSQL, object-store, namespace, and provisioning concurrency limits;
- measured physical resource envelopes and OLDLAB infrastructure headroom;
- #896 non-exclusive containment evidence;
- zero-capacity shadow and inventory evidence;
- proof that every legacy writer is disabled;
- a one-slot architecture-specific and neutral acceptance sequence;
- alerting and operator status visibility; and
- an exercised no-dual-writer rollback procedure.

Issue #906 remains the appropriate carrier for live activation evidence. The
repository implementation and the activation package should remain separate
deliverables so code review cannot be confused with permission to consume live
capacity.

## Acceptance Criteria

The design is implemented when all of the following are true:

1. One fenced manager is the only allocation writer for production, staging,
   shared development, and dynamic development.
2. Exactly one executor per physical pool applies grants locally.
3. Dynamic development has physical OLDLAB and GB10 policies and no synthetic
   pool allocation authority.
4. `min_slots` is configurable, defaults to zero, and is aggregate across pools
   and generations.
5. No pool weights are present in user or operator allocation configuration.
6. Complete capability matching and mandatory pool assignment cover pinned,
   architecture-specific, neutral, and incompatible tasks.
7. Development fairness is owner-safe and cannot be gamed by creating more
   environments.
8. Slot and physical resource limits remain conservative across all
   environments.
9. Accepted capacity is never reused before terminal release.
10. Reclamation and rollout are drain-first with no automatic hard preemption.
11. Per-instance immutable candidates and generation-bound trials support
    simultaneous independent deployments.
12. Untrusted candidate code cannot change the trusted Slurm/container launch
    boundary.
13. Failure and restore paths preserve commitments and reject stale authority.
14. Migration proves that no legacy and global writer overlap.
15. Live capacity remains zero until the separate activation gate approves it.
