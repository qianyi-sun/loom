# Global Fleet Capacity Manager Design

Status: approved after iterative self-review; implementation planning authorized
Live activation: not authorized; gated by the Activation Boundary
Last reviewed: 2026-08-10

Date: 2026-08-07

## Summary

Loom will have one logical global capacity manager for every environment that
consumes Loom-managed OLDLAB or GB10 worker capacity:

- `production`;
- `staging`;
- every personal `dev-<name>` environment.

The Kubernetes namespace `loom-dev` is the trusted shared development
infrastructure plane, not another application environment. It contains the
development lifecycle service, global manager, candidate-builder coordinator,
management PostgreSQL, shared application PostgreSQL, and shared MinIO. A
personal application deployment runs in `loom-dev-<name>`. There is no
`loom-dev-shared` namespace and no static shared `development` application
subject.

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

- production, staging, and personal development still have independent
  allocation paths;
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
9. Make activation measurable, reversible, and safe with new global-manager
   scale-up initially held at zero.

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
  or `dev-alice`. The shared `loom-dev` infrastructure namespace is not an
  environment subject.
- **Subject identity**: the immutable UUID and lifecycle incarnation behind an
  environment display name. Names are never fencing keys by themselves.
- **Capacity account**: the fairness and quota identity. A dynamic development
  account is its immutable authenticated-principal UUID, not a mutable user
  name; static environments have operator-defined service accounts.
- **Priority tier**: an operator-owned class. The default order is production,
  staging, then development.
- **Physical pool**: a Slurm execution domain, initially `oldlab` or `gb10`.
- **Resource domain**: an operator-declared class of interchangeable nodes or
  other indivisible pool capacity with the same partition, features,
  architecture, topology, and allocatable resource envelope.
- **Slot**: capacity for one concurrent Loom trial. It is the user-facing unit.
- **Resource vector**: the operator-owned CPU, memory, GPU, and other physical
  costs of one exact worker shape.
- **Worker shape**: an approved indivisible launch shape with a concurrency
  slot count and total resource vector.
- **Demand bucket**: runnable task demand grouped by normalized requirement
  fingerprint, compatible pool/resource-domain/worker-shape set, execution
  generation, and local task-priority band.
- **Commitment**: capacity that is proposed, accepted, pending, active,
  draining, unobservable, or otherwise not proven released.
- **Grant**: a fenced manager decision for one environment, physical pool,
  candidate, and deployment generation, including an exact desired multiset
  of worker shapes and its slot and resource totals.
- **Placement allowance**: the number of unclaimed trials in a demand bucket
  that an environment agent may assign to one physical pool.
- **Protected attempt identity**: an immutable UUID/incarnation for one trial
  execution attempt and its capacity claim. A retry receives a new identity;
  an identity is never reset or reused.

Three terminality domains are deliberately distinct:

- **Workload terminality** is the user-visible trial state: `succeeded`,
  `failed`, or `cancelled`. It controls batch lifecycle and delivery behavior.
- **Protected-claim terminality** means the trusted claim guard has closed the
  exact worker concurrency lease and fenced retry or replacement as required.
- **Physical terminality** means the submission, Slurm job, worker, bootstrap,
  and reservation evidence required by the release protocol is complete.

A workload may become terminal before its protected claim or physical
commitment. In particular, the existing single-trial and batch cancellation
APIs may report `cancelled` before the worker observes cancellation and stops.
Demand and capacity accounting therefore use protected claim and physical
state, never workload state alone. Workload terminality is not manager release
evidence, and `finished_at` is not a capacity-release timestamp.

Every authoritative record, credential, API compare-and-set, filesystem path,
and job binding uses the subject UUID and lifecycle incarnation. In the rest of
this document, “environment” is shorthand for that exact subject; the display
name is lookup and diagnostic metadata only.

Environment display names are normalized once under a bounded canonical
DNS-label grammar and uniqueness rule and are immutable for the subject
lifecycle. A rename requires terminal deletion and creation of a new subject;
it cannot free an alias while old jobs or paths exist. Logs and metadata encode
names as data; filesystem/object paths use the UUID/incarnation as the authority
component and only a length-bounded escaped display-name suffix for diagnosis.
Raw names are never concatenated into a path, shell command, Slurm selector,
SQL identifier, credential scope, or authorization decision.

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
  fleet workers, plus protected worker heartbeat and capacity-claim completion
  transitions; and
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
processes. The process must also hold the one management-registered executor
incarnation and bounded authority lease for that pool. A process with a copied
credential but no registered incarnation cannot act. Lease loss forbids
acceptance, submission, or any increase; the still locally locked incumbent may
perform only the monotonic fail-safe pending-cancel/drain actions defined for a
management outage and journals them for later reporting. An explicitly fenced
or replaced incarnation cannot mutate at all. Conflicting use of one
incarnation is detected by its monotonic sequence/digest and fences it.

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

The executor may optimize command batching, but it cannot substitute a
different worker shape or resource request for a manager-approved launch
intent. Any unavailable exact shape remains pending and is reported back for a
future manager decision.

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

`min_slots` defaults to `0`; min, max, and surge are nonnegative integers, and
minimum must not exceed maximum. `max_slots` is always finite: create must
supply it or resolve an operator-owned finite default, while update preserves
an omitted value. There is no unlimited sentinel. Dynamic users may change
minimum and maximum values only within their operator-configured owner quota.
The aggregate minimum across one owner's live development environments must
not exceed that owner's reservation quota. A new subject remains at effective
zero capacity until every required policy, profile, and authority binding is
ready.

`rollout_surge_slots` is not ordinary demand capacity. It is an
operator/account-bounded allowance for temporary old/new worker-generation
overlap during one fenced rollout. It never raises the environment's task-claim
ceiling, minimum, steady-state target, or fair share, and it is unusable when no
old-generation commitment has been selected for replacement.

Priority membership is derived server-side. A dynamic development user cannot
promote an environment to staging or production.

Static subject policy and worker-profile references are applied through
environment-state tooling to the management API. One separately reviewed
fleet-state manifest owns global pool/resource-domain, tier, account, and
protocol generations and publishes them once to that API. For dynamic users it
owns account-policy templates and limits; authenticated-principal account
instances are created transactionally from those templates rather than listed
as mutable fleet-state names. After migration, an
environment-state manifest may reference those immutable fleet generations and
narrow a profile, but cannot carry an authoritative copy of global controller,
partition, node, or envelope fields. Dynamic policies and the dev-instance
registry change in the same management-database transaction so there is no
second capacity source of truth.

Every personal development policy requires both `oldlab` and `gb10` profiles.
A user or candidate update cannot disable either pool; task requirements
choose or pin where work is eligible. An operator may mark a physical pool
globally ineligible for maintenance through a new audited pool-configuration
generation, which makes compatible work wait or use the other pool without
rewriting each environment's required profile set.

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

- supported capabilities and architecture by resource domain;
- configured Loom slot ceiling;
- CPU, memory, GPU, and other allocatable resource totals;
- resource-domain node counts, per-node vectors, partitions, features,
  topology constraints, and permitted overcommit assumptions;
- required non-exclusive scheduling/packing mode and measured co-tenant
  headroom;
- reserved infrastructure headroom;
- pending slot/shape-job ceilings and submission-rate limit;
- allowed nodes and Slurm authority identity;
- controller-enforced submit-job, running-job, partition, and TRES/QoS bounds
  for the dedicated Loom association at or below the configured envelope;
- health and scale-up eligibility; and
- a monotonically increasing pool-configuration generation.

Controller identity, partition, global node/resource-domain inventory,
association bounds, and allocatable envelope are declared once per immutable
physical-pool generation. Environment-state manifests and worker profiles
reference that generation by identity and digest; they cannot duplicate or
override its `allowed_nodes`, controller, partition, or capacity totals. An
environment profile may narrow eligibility to a declared subset of resource
domains for candidate visibility, capability, or containment reasons, but it
cannot add a node, reinterpret a domain, or make globally unavailable capacity
eligible. Foreign reservation or infrastructure headroom is represented in the
global domain envelope, not as contradictory per-environment topology.

The configured Loom envelope, not momentary Slurm free-node count, is the hard
global limit. Transient foreign workload can delay a Loom job but cannot cause
the manager to duplicate grants or flap the configured envelope.

Pool-envelope and resource-domain generations are immutable. A capacity or
topology change creates a new generation; nonterminal commitments retain and
are charged by the envelope/profile vectors under which they were accepted. A
lower new envelope enters over-limit drain rather than reinterpreting old jobs
as cheaper or absent.

Slots, node counts, CPU quanta, bytes of memory/storage, GPU counts, and every
other allocatable dimension use schema-versioned canonical nonnegative integer
units with explicit upper bounds. Configuration validation and allocator
summation use checked arithmetic; overflow, lossy unit conversion, NaN/float
input, or a worker-shape-to-Slurm render that cannot round-trip to the exact
controller-normalized vector rejects the configuration generation fail-closed.

### Environment/pool worker profile

An operator-owned worker profile binds one environment generation to one
physical pool. It includes a finite catalog of permitted worker shapes, each
with exact concurrency, total resource vector, node count, compatible resource
domains, advertised task capabilities, and placement constraints, plus
candidate artifact, protocol version, trusted launcher configuration, local
safety ceiling, and protected binding references. The catalog must include a
one-slot shape for each supported task-capability class so every integer target
is representable, although the manager may prefer larger shapes to reduce
overhead. It also marks the operator-approved warm-capacity shapes used when a
minimum has no matching task demand.

User-authored task or candidate data cannot change this profile.

An environment/profile placement constraint can only narrow the referenced
physical-pool generation. Validation rejects an unknown domain, a node outside
that generation, or any repeated controller/partition/resource field that does
not exactly match the referenced global record.

Worker-profile generations are also immutable. Replacement profiles govern new
proposals only. An unaccepted proposal bound to a superseded profile is closed
and cannot be accepted. Executors retain old bindings strictly for already
accepted launch, adoption, and drain until every referenced job is terminal;
they cannot substitute a new shape/vector for an old commitment.

## Management Data Model

The implementation will add versioned management-plane records equivalent to:

- `capacity_authority_state`: authority incarnation, writer epoch, schema
  version, recovery/activation state, executable new-capacity ceiling, and
  global pending slot/job/rate ceilings;
- `capacity_tiers`: priority order, aggregate slot/resource, and pending
  slot/job ceilings;
- `capacity_accounts`: immutable owner/service identity, aggregate minimum
  reservation, max/surge, pending slot/job/rate, live-environment, build, and
  artifact quotas plus fairness state;
- `capacity_pools`: capabilities, resource-domain and topology envelope,
  pending slot/job/rate limits, health, and configuration generation;
- `capacity_subjects`: immutable subject UUID/incarnation, environment display
  name, account, tier, min/max/surge and operator-owned pending slot/job/rate
  limits, lifecycle, candidate, deployment generation, and configuration
  generation;
- `capacity_candidates`: immutable source, artifact, architecture, launcher,
  attestation, and protocol bindings;
- `capacity_deployment_generations`: per-environment candidate generation,
  readiness state, required pool-profile set, cutover epoch, and lifecycle
  state;
- `capacity_worker_profiles`: environment/pool/generation resource cost,
  approved worker-shape catalog, artifact and protocol binding, and readiness;
- `capacity_demand_snapshots`: versioned pending-unassigned compatibility
  buckets, current unclaimed assignments with allowance epochs, fixed claims,
  freshness, reporter incarnation, monotonic sequence, and acknowledgement;
- `capacity_allocations`: desired slots, accepted reserved slots, unaccepted
  proposed slots, exact desired worker-shape multiset, slot/resource totals,
  releasing slots, placement allowances, grant fencing fields, expiry, and
  state;
- `capacity_reservation_tranches`: stable tranche identifier, authority and
  allocation epochs, subject/pool/generation binding, proposed and accepted
  stable worker-shape identities and slot/resource totals, per-shape release
  state, cumulative released identities, optional rollout-surge slot charge
  and distinct old-shape replacement backing, and release evidence;
- `capacity_submission_intents`: stable executor operation identifier,
  reservation-tranche binding, requested resource vector, worker shape,
  signed ownership metadata digest, and submission/adoption state;
- `capacity_launch_permits`: supersedable allocation-epoch binding from one
  immutable ready intent to its current eligibility and deterministic launch
  rank, plus one-way consumption state;
- `capacity_launch_rate_buckets`: durable global, account, subject, and pool
  token state advanced only from management-database time;
- `capacity_executors`: pool-scoped identity, authority binding, accepted
  high-water marks, signing-key identity, heartbeat, inventory state, and
  local-authority evidence;
- `capacity_executor_observations`: pool inventory, pending/active/draining
  counts, cumulative terminal evidence, adoption results, executor
  incarnation, monotonic sequence, and fencing fields;
- `dev_lifecycle_limits`: global and per-owner provisioning/build concurrency,
  build admission/rate, retained-artifact count/bytes, and live-instance
  limits;
- `dev_lifecycle_operations`: idempotency and subject/owner binding, expected
  epoch, immutable current attempt identity/high-water mark, requested
  candidate/policy, state, deadline, and attempt-bound checkpoint manifest;
- `capacity_artifact_refs`: immutable digest, size, attestation, owner quota
  charge, generation/job references, retention state, and GC manifest; and
- `capacity_audit_events`: bounded, secret-free configuration, allocation,
  acceptance, drain, release, recovery, and operator events.

Environment databases will add protected companion records equivalent to:

- a mirrored capacity-admission grant keyed by physical pool, candidate,
  deployment generation, authority mode, and allocation/legacy-writer epoch;
- placement-allowance consumption keyed by the same epoch; and
- worker claim-authorization epoch plus authoritative trial
  execution-generation, sealed normalized capacity-requirement fingerprint,
  physical-pool assignment, capacity-claim state, submission-bound
  bootstrap-registration/revocation epoch, immutable protected attempt/claim
  identity, worker identity/shape, and live concurrency leases.

Admission records and transition functions live in a protected schema owned by
an operator-created non-login trusted capacity owner. Candidate runtime and
migration roles are not table owners or members of that role; they have neither
DDL authority over the protected schema nor direct permission to perform the
protected queued-to-claimed transition. Capacity-critical state is kept in
these companion records rather than depending on a trigger attached to a table
owned by candidate code. Protected functions prevent candidate roles from
changing worker claim epoch, worker drain state, trial pool assignment,
execution generation, or authoritative capacity-claim state.

Candidate migrations run through a trusted migration runner using a role
limited to the candidate-owned application schema. A candidate may propose and
test application migrations, but it cannot change protected objects, role
membership, ownership, grants, or guard functions. A protected capacity-schema
change is a trusted fleet-release change and follows its own reviewed migration
path. Candidate-visible trial state is a workload projection: changing it
directly neither grants capacity nor produces a valid worker claim.

Protected database functions have a fixed safe `search_path`, schema-qualify
every referenced object, revoke default/public execution, and expose only the
minimum checked procedures to named trusted roles. Migration and runtime roles
are `NOSUPERUSER`, `NOCREATEROLE`, `NOBYPASSRLS`, cannot install extensions or
create in protected/shared schemas, and cannot transfer ownership. Temporary
or candidate-schema objects with trusted-looking names therefore cannot shadow
objects used by a security-definer transition.

The protected transition surface must be closed over every operation that can
change capacity eligibility or ownership. Before enforcement can be enabled,
the implementation inventories and replaces or fences every direct SQL, ORM,
background-sweeper, and service path for trial submission, requirement change,
pool assignment, claim, heartbeat, start, completion, failure, cancellation,
crash recovery, retry/requeue, worker reassignment, batch/family cancellation,
and environment deletion. Each such path calls a named trusted procedure that
updates the protected companion record and any candidate-visible projection in
one environment-database transaction. A compatibility writer that cannot do
so is disabled before global activation; a database privilege or trigger guard
rejects an overlooked direct mutation. This includes the existing
single-trial cancellation route: it may make the workload projection terminal
immediately, but it must atomically mark a live protected claim
`cancel-pending` rather than closing its concurrency lease.

Every contract has an explicit schema version and rejects unknown fields or
versions fail-closed.

### Monotonic reporting contracts

Every complete demand snapshot is keyed by environment, configuration
generation, reporter incarnation, and a monotonically increasing sequence.
Every complete executor inventory is similarly keyed by pool, executor
incarnation, and sequence. The management database records a high-water mark
and applies a report transactionally only if its binding is current and its
sequence is newer. Exact replay is an idempotent success; a lower sequence,
retired incarnation, partial page, or mismatched generation is rejected and
cannot overwrite newer state.

“Exact replay” requires the stored canonical payload/manifest digest to match.
Reusing the same incarnation and sequence with a different digest is
equivocation: the authority fences that reporter/executor, retains the prior
state as unknown, and alerts. The same stable release or terminal identity with
conflicting bindings/evidence is quarantined rather than merged.

A reporter or executor cannot choose a new incarnation by itself. The trusted
lifecycle or authority-recovery path registers it and fences its predecessor.
Large reports use staged pages plus a final manifest digest; no page affects
allocation until the complete manifest is committed. Release evidence is
cumulative and follows the stricter tranche rules below.

Freshness and acceptance deadlines use management-database receipt time, not a
reporter-supplied wall clock. Source timestamps and measured clock offset are
retained only for diagnosis and skew alerts.

## Capacity Accounting and Grant State

### Reservation-tranche states

```text
proposal tranche: proposed -> accepted
proposal tranche: proposed -> closed
each accepted shape: accepted -> releasing -> released
```

`closed` is terminal for a proposal that expired or was superseded before
acceptance. It has no accepted capacity or external side effect and cannot be
reopened.

Proposal acceptance is atomic for the tranche, but release is monotonic per
stable accepted shape identity. A multi-shape tranche may therefore contain
retained `accepted`, `releasing`, and `released` shapes simultaneously; its
aggregate state is derived and it is wholly released only when every accepted
shape is released. A releasing shape can never become accepted again, while a
retained shape remains launchable/claimable only if the current grant and
admission plan still select it.

The state machines apply to each reserved increase and its shapes, not to the
aggregate allocation row as an indivisible object. One allocation may therefore
contain an accepted base and a proposed increase at the same time. An
allocation may also retain accepted reservations while its desired target
decreases. The manager tracks three separate aggregate quantities:

- `desired_slots`: the current target;
- `proposed_slots`: additional reserved capacity not yet accepted by the
  executor; and
- `reserved_slots`: accepted capacity not yet proven released.

Each reservation tranche has a stable, globally unique identifier and an
immutable authority-incarnation, executor, pool, subject UUID/lifecycle
incarnation, environment display name, candidate, deployment-generation,
allocation-epoch, exact worker-shape multiset, and slot/resource-total binding.
Proposal acceptance is a compare-and-set on that identifier. Aggregate proposed
and reserved values are derived from tranche rows rather than adjusted by
unidentified deltas.

An accepted tranche records a monotonically increasing set of released shape
identities and derives cumulative released slots and resource vectors, bounded
by its accepted shapes. A release acknowledgement names the tranche and the
stable submission, Slurm-job, worker, or explicitly unused reservation
identities that became terminal. The manager applies each identity once;
duplicate acknowledgement is an idempotent success and an out-of-order or
regressing acknowledgement cannot free capacity twice. Partial release
therefore leaves the remainder of the same tranche charged.

Accepted slots that never reached `sbatch` are released only by a fenced,
replayable close protocol. An atomic central compare-and-set from `prepared` or
`launch-ready` to `closing` first prevents further submission; the executor
persists that fence in its journal and asks the agent to atomically advance the
intent's protected bootstrap-registration epoch, reject every older in-flight
registration, and revoke every capability/worker under the preceding epoch.
The agent's acknowledgement names the new epoch and proves there is no
protected worker. The executor then binds that acknowledgement and full
inventory evidence to the journal high-water mark. The manager performs the
final central release compare-and-set only afterward. If the environment agent
is unavailable, the unused reservation cannot close. This is terminal evidence
for an unused reservation, not a timeout or an unkeyed count.

Charged capacity is the sum over this identity union:

```text
unaccepted open proposal shapes
UNION unreleased accepted tranche shapes
UNION observed nonterminal job/worker shapes
UNION unattributed quarantine shapes
```

Stable tranche, submission, Slurm-job, and worker bindings deduplicate multiple
observations only when every immutable binding and exact resource vector
agrees. Disjoint observed shapes are added, never collapsed with `max`; this
charges both a known reservation and an unrelated orphan. A conflicting or
resource-mismatched observation is not deduplicated with its accepted intent:
the accepted vector remains charged and the authoritative observed vector
receives an additional quarantine charge. Only an operator-audited recovery
manifest proving that both records describe one physical job may replace those
two conservative charges with the exact observed commitment. Slots and every
physical resource component are summed from the exact vectors after valid
deduplication.

Before normal allocation, an executor imports any conclusively Loom-owned
orphan into a quarantined reservation. A job inside the dedicated Loom
submitter/account or frozen legacy-authority scope whose ownership proof is
incomplete is also quarantined for capacity safety but treated as foreign for
mutation. A job conclusively submitted by an unrelated Slurm identity remains
foreign and outside the configured Loom ledger; it can delay launches but does
not dynamically rewrite the Loom envelope. A missing ledger row never makes an
in-scope observed job free.

An observed commitment that cannot yet be attributed to an allocation is
charged directly against its physical pool's slot and resource envelopes as an
unassigned quarantine. It cannot consume an innocent environment's quota, but
it reduces allocatable pool headroom until an operator-backed adoption binds it
or authoritative terminal evidence removes it.

Authoritative Slurm requests supply the quarantine's physical resources. If
they do not map uniquely to an approved worker shape, the manager uses the
largest compatible slot cost; if no finite mapping can be proven, it reserves
the pool's entire remaining slot envelope. Ambiguity therefore loses
availability rather than creating unaccounted capacity.

### Upward change

1. The manager reserves the increase as `proposed_slots`.
2. The pool executor atomically accepts the current, unexpired proposal and
   creates exactly one stable central `prepared` intent per accepted shape.
3. Accepted proposal slots move into `reserved_slots`.
4. The environment agent may activate the corresponding admission and
   placement allowance only after acceptance.
5. The executor may submit Slurm work only after acceptance.

If the proposal expires before acceptance, the manager closes it atomically
and may reuse it. A late executor cannot accept a closed proposal.

### Submission-intent states

Each accepted shape has exactly one stable submission intent:

```text
prepared -> launch-ready -> submitting-unknown -> bound -> observed -> terminal
prepared or launch-ready -> closing -> closed
submitting-unknown -> quarantined -> bound
bound or observed -> quarantined
quarantined -> terminal
```

Acceptance creates `prepared` centrally with no external side effect. The
executor fsyncs the local journal and registers the bootstrap hash, then
compare-and-sets the matching central intent to `launch-ready`. Immediately
before invoking `sbatch`, it calls one central procedure that revalidates the
active authority/configuration/allocation epochs, executor lease, subject
lifecycle, exact intent, manager-issued launch order, and any required
authenticated protected drain acknowledgement for a rollout-surge pair. The
procedure permits only the earliest eligible `launch-ready` intent under the
current versioned launch permit and global priority/fairness order, atomically
consumes that permit and the applicable durable global, account, subject, and
pool rate tokens using management-database time, and changes the immutable
intent to `submitting-unknown`. The executor then fsyncs the same state in the journal;
the Slurm call is allowed only after both steps. A consumed permit or token is
not refunded after an ambiguous transition. Any crash, timeout, or ambiguous
client error from the central transition onward is treated as though it may
have created a job. The executor therefore inventories by signed operation
identity and never invokes `sbatch` again for that intent. A returned or
recovery-adopted exact job moves to `bound`; authoritative Slurm observation
moves through `observed`.

`prepared` or `launch-ready` may enter closing only before the call and reaches
closed only under the bootstrap-revocation/inventory rules. An ambiguous
`submitting-unknown` intent moves to `quarantined`; if no job is found, it may
become terminal only after authoritative Slurm controller/accounting high-water
evidence spans the submission attempt and proves absence. Otherwise it remains
quarantined, or moves to `bound` when exactly one authenticated job is later
found. Intent terminality still requires the protected worker/bootstrap
evidence defined for release. Intent and operation identifiers are never
reused.

Only an unresolved pre-binding quarantine may return to `bound`, and only when
one exact authenticated job is proven. A resource mismatch, conflicting
binding, duplicate job, or post-binding mutation moves `bound`/`observed` to a
nonrecoverable quarantine for automatic execution: bootstrap and new claims are
fenced, the conservative charges remain, and the intent can advance only to
terminal unless the audited manual-recovery protocol replaces its accounting.

### Downward change

1. The manager publishes a lower `desired_slots` and lower placement/admission
   allowance.
2. One environment-database transaction advances the admission epoch, clears
   excess unclaimed assignments, authorizes only the deterministic whole worker
   identities whose approved shapes, concurrency, and resource totals fit the
   new exact plan, and marks every excess worker draining. The scheduler cannot
   observe the new epoch without also observing the worker fences.
3. The executor cancels excess pending jobs and applies the committed worker
   drain plan to Slurm.
4. `reserved_slots` remains at its old value while capacity is pending, active,
   draining, unknown, or not conclusively terminal.
5. The executor reports a fenced full inventory and cumulative terminal
   release evidence for stable tranche and submission identities. A Slurm job
   is terminal only when authoritative controller/accounting state confirms
   termination; disappearance from `squeue` is insufficient. A worker release
   additionally requires the protected environment record to be non-claimable
   and terminal.
6. Only then does the manager derive a lower `reserved_slots` and reuse
   capacity.

Lease expiry is a scale-up and claim fence. It is never release evidence.

Drain is monotonic for a concrete worker/shape identity. Once a protected local
transaction marks it draining or its tranche shape releasing, no later demand
or stale epoch can make that identity active/claimable again. Returning demand
uses still-retained active shapes or a new proposal after compatible headroom is
available. This avoids reversing a release while delayed cancellation,
completion, or acknowledgement is in flight.

Subject to reaching the manager's exact feasible shape plan, release selection
continues already-releasing identities first, then closes unsubmitted intents,
cancels eligible pending jobs, drains idle workers, and only then marks occupied
workers draining. Among otherwise equivalent choices it minimizes the number of
protected live claims fenced from replacement, then minimizes fragmentation and
uses stable identities as the tie-breaker. Marking an occupied worker draining
never moves or terminates its existing claims; it only prevents replacements.

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

Compatibility uses the complete registered worker-shape capability set. Initial
architecture behavior is:

- explicit `worker_pool=oldlab` or `worker_pool=gb10` is a hard pin and must be
  compatible with the remaining requirements;
- `cpu_arch=x86_64` requires an x86-compatible pool, initially OLDLAB;
- `cpu_arch=arm64` requires an ARM-compatible pool, initially GB10;
- `cpu_arch=any` may use any otherwise compatible pool; and
- missing `cpu_arch` remains `x86_64` for backward compatibility.

Unsupported or contradictory requirements are reported as non-capacity
blockers and do not cause worker scale-up.

Compatibility is actually evaluated against complete approved worker-shape
capabilities and resource-domain constraints; the pool-level architecture rules
above are only the initial user-facing cases. Two trials that can both use
OLDLAB but require different GPU, feature, node-class, or other capabilities
do not share a demand bucket unless their complete compatible shape sets are
identical.

Demand snapshots are bounded aggregates rather than raw task payloads. They
group pending-unassigned runnable work by:

- normalized requirement fingerprint and compatible physical-pool,
  resource-domain, and worker-shape set;
- execution candidate and generation;
- task-priority band;
- count; and
- oldest eligible submission time.

Current unclaimed `assigned` attempts are reported separately with their exact
bucket, pool, assignment/allowance epoch, and stable attempt identity. They
remain part of total runnable-unclaimed demand but are excluded from the count
that can receive a new allowance. The manager validates and preserves or clears
them in the joint matching witness before issuing residual allowance units, so
one queued workload is counted once even when reports and allocation epochs
cross in flight.

Every nonterminal protected claim is a fixed commitment to its current pool,
generation, worker, and protected attempt identity, regardless of whether the
workload projection says `claimed`, `running`, `failed`, or `cancelled`. This
includes cancel-pending, retry-pending, completion-pending, and unknown claims.

An allocation class is keyed by the complete compatible approved-shape set,
generation, and bounded task-priority band—not by unbounded candidate-authored
labels. Distinct normalized requirements with the same complete compatible set
may share a class because the claim guard still checks each sealed requirement.
Worker-profile validation caps the finite shape catalog, the derived allocation
classes, and priority bands. A snapshot must contain every nonempty class plus
its manifest; class overflow, an unmappable fingerprint, or truncation rejects
the whole snapshot as stale/invalid rather than silently omitting demand.

The trusted trial-submission boundary validates and normalizes every
capacity-relevant requirement—including pool pin, architecture, resources,
features, topology, and protocol—and seals its canonical fingerprint in the
protected companion row with the execution generation. Demand reporting and
claim admission use that sealed value, not a mutable candidate-owned
projection. An authorized requirement change is allowed only while unclaimed;
one protected compare-and-set replaces the fingerprint, clears any pool
assignment and allowance consumption, and makes the trial wait for a new
allocation epoch. Requirements are immutable after claim.

## Global Allocation Algorithm

The manager computes a deterministic allocation epoch from one consistent
management-database snapshot. Reporter observations are asynchronous, but
they cannot directly free capacity; only the manager processes fenced release
evidence.

The manager commits the complete epoch—desired shape plans, reservations,
drains, and placement allowances for every affected subject—in one serializable
management-database transaction guarded by its writer epoch. No executor or
agent can observe an executable half-plan. If topology packing, validation, or
the transaction fails or exceeds its bounded computation deadline, the manager
publishes no part of that epoch and retains all existing commitments. When the
database is reachable, it records a separate writer-fenced increase-freeze and
revokes every unconsumed launch permit before reporting the failure. If even
that small transaction cannot commit, every permit and final launch CAS is
still bounded by management-database expiry and freshness of the last complete
successful allocation epoch. Thus already consumed/ambiguous intents remain
charged, but no new physical increase can continue beyond the bounded failure
window. Only a later complete, validated allocation transaction under the
current writer epoch may clear the freeze and publish replacement permits; a
manual status acknowledgement cannot bypass it. The deterministic failure
reason is alerted.

The allocator applies these lexicographic objectives:

1. Preserve and charge all fixed, proposed, accepted, pending, active,
   draining, stale, unknown, and quarantined commitments.
2. Respect physical slot and resource-vector limits, tier ceilings, owner
   limits, environment aggregate limits, and rollout surge.
3. Serve priority tiers in strict order for each compatible resource domain.
4. Within one tier, satisfy aggregate environment minimums using hierarchical
   constrained progressive fairness by capacity account and then environment.
5. Within one tier, allocate task-backed demand using the same hierarchy.
6. In each account/environment fairness turn, serve its highest local
   task-priority band first, then place more constrained compatible-shape demand
   before flexible demand within that band so flexible work does not strand
   constrained work. A subject's local priority never changes its tier or
   top-level account share.
7. Preserve healthy accepted placements where they remain feasible.
8. After a higher tier's service and existing placements are fixed, choose
   among its remaining equivalent placements to maximize the lexicographic
   feasible service of lower tiers, preferring preservation of their
   constrained demand.
9. Place rollout-surge replacement shapes only from headroom left after all
   ordinary minimum and task-backed service; surge never initiates reclamation
   from another subject and is the first optional capacity withdrawn when that
   headroom becomes contested.
10. Minimize churn and resource fragmentation, then use oldest waiting demand
   and stable identifiers as deterministic tie-breakers.

Priority is resource-local. Unmet production x86 demand prevents lower-tier
borrowing on OLDLAB, but it does not block lower-tier ARM demand on GB10.
Lower-tier work may use only capacity that is not currently needed by a
higher-tier compatible demand. The allocator does not backfill a contested
resource fragment with a long-running lower-tier trial that would delay the
higher tier.

Fairness is also contention-local: accounts whose complete compatible shape
sets are disjoint do not consume one another's fair turn. The lower-tier
preserving placement rule never reduces service, violates fairness, or adds
churn for a higher tier; it resolves only otherwise equivalent choices.

Within development, fairness is hierarchical:

```text
development tier -> immutable owner account -> owner's environments
```

Creating more environment names therefore cannot create more top-level fair
shares.

Progressive filling compares delivered concurrent-trial slots, first across
accounts and then across their environments. During the minimum phase it fills
one requested floor slot per eligible fairness round until a minimum is met;
during the demand phase it fills one task-backed slot per round until demand or
a ceiling is met. Resource vectors are hard feasibility constraints on those
rounds, not fairness weights.

There are no configurable global-capacity account weights or pool-placement
weights. Resource costs and compatibility are facts supplied by operator-owned
profiles. An environment-local scheduler may retain its existing operator-owned
team fair-share weights solely to order which team task consumes capacity
already granted to that environment. Such a local weight cannot change the
environment's global tier/account turn, requested or granted capacity, physical
pool assignment, placement allowance, or manager launch order.

Feasibility is topology-aware, not merely an aggregate-vector comparison. The
manager conservatively packs every fixed and desired worker shape into the
configured resource domains, including node counts, per-node resources,
features, and placement constraints. A plan that fits aggregate CPU and memory
but cannot fit the declared node topology is infeasible. The executor may
report transient Slurm placement delay, but it cannot use Slurm's queue as a
way to approve a globally infeasible shape plan.

### Aggregate target

For one environment, before global constraints are applied:

```text
requested_slots = min(
    max_slots,
    max(min_slots, fixed_live_protected_claim_slots + runnable_unclaimed_slots),
)
```

The allocator's grant may be lower than `requested_slots`. Observed
commitments may be higher than both during drain or after a limit reduction.
`requested_slots` is a total concurrent-trial target, not an increment: every
live protected claim consumes the target first, even when its workload row is
already terminal, and only the residual is available to runnable unclaimed
demand. `runnable_unclaimed_slots` includes each valid current assignment and
each pending-unassigned attempt exactly once; already assigned attempts do not
receive another placement allowance. The manager does not add fixed or
assigned work a second time when producing a grant.

The normal worker-capacity ceiling is also `max_slots`. During one old/new
generation rollout, the manager may temporarily hold at most
`max_slots + rollout_surge_slots` in environment worker-capacity commitments,
but every slot of capacity above `max_slots` must be backed by slot capacity
from distinct old-generation shapes already selected for monotonic replacement
drain. Across the current set of nonterminal surge pairings, an old shape's
cumulative backing cannot exceed its approved concurrency. The tranche records
the exact old identities and backed slot counts; different old/new shape sizes
are allowed only when the aggregate matching proves the surge charge is no
greater than that distinct draining-old capacity. Before a surge intent can
consume a launch permit, the protected environment transaction must
acknowledge every old worker identity backing that intent under the paired
allocation epoch as draining and nonclaimable for replacements. The new worker
binds that newer admission epoch. Backing may move to a replacement intent
under a newer allocation epoch only after the prior intent completes the
fenced unused-reservation close protocol and proves it created no physical
commitment. `submitting-unknown`, bound, observed, or quarantined intent
backing remains occupied until physical terminality; ambiguity can never back
two replacement attempts.
Surge-only slots authorize no extra placement allowance or protected task
claim: live claims remain bounded by `max_slots` (apart from already-existing
over-limit claims draining after a limit reduction). When the paired old shape
releases, its temporary surge allowance disappears; the completed rollout must
converge back to at most `max_slots`. Proposed, accepted, observed, and
quarantined capacity all count against the applicable normal-plus-surge
commitment ceiling.

### Warm minimum placement

`min_slots` is aggregate and defaults to zero. When the minimum exceeds actual
task demand, the residual is warm capacity. The manager:

1. retains healthy existing warm capacity to avoid churn;
2. excludes pools without a ready candidate/profile or healthy executor;
3. preserves capacity needed by constrained task demand;
4. considers only the profile's operator-approved warm-capacity shapes;
5. chooses the pool and warm shape with the most feasible normalized headroom;
   and
6. uses deterministic architecture diversity and tie-breaking where headroom
   is otherwise equal.

A minimum is a desired reservation under finite capacity, not permission to
oversubscribe a pool or bypass higher priority.

“Normalized headroom” means the deterministic post-placement free fraction of
each relevant slot/resource-domain constraint, compared lexicographically by
the most constrained dimension. Zero or inapplicable dimensions are ignored.
There are no configurable coefficients or proportions hidden in this rule.

### Pending-launch control

The manager issues an exact launch allowance in addition to desired capacity.
Proposed, accepted-but-not-active, and observed Slurm-pending capacity consume
finite operator-owned global, tier, capacity-account, subject, and physical-pool
pending slot and shape/job-count ceilings. Global, account, subject, and pool
submission-rate limits additionally bound controller/API load. These are
safety/failure-isolation limits, not user weights. Executors cannot choose a
different environment to launch first merely because they iterate grants in a
particular order or local rate bucket.

Launch permits are part of the complete allocation epoch and point to immutable
submission intents rather than mutating their shape or ownership binding. An
unconsumed older permit may be superseded by a newer complete epoch. The final central
`launch-ready -> submitting-unknown` compare-and-set serializes rate-token
consumption across both pool executors and rejects an out-of-order intent.
Prepared, lifecycle-fenced, unhealthy, or otherwise ineligible intents do not
block the eligible order; the manager recomputes order after state changes.
This makes a local polling race affect latency only, not priority, fairness, or
the global submission rate.

## Task Placement and Scheduler Admission

Every queued trial managed by the global authority must receive a physical
pool assignment before claim. This includes single-pool and multi-pool demand.

For each pool/generation/capability domain, every fixed protected claim remains
bound to its exact worker slot even when that worker is draining. Current
unclaimed assignments and newly offered placement allowances may match only
distinct free slots in the admission-eligible accepted shape plan: accepted
shapes that are retained or pending launch, not draining, closing,
quarantined, terminal, or surge-only. Proposed-but-unaccepted shapes do not
authorize placement. Each successful local assignment consumes one allowance
unit under its epoch; replay is idempotent, and an unconsumed superseded unit
does not remain usable.

That check is joint across overlapping capability sets, not one independent
scalar check per bucket. The manager first reserves the exact slots occupied by
fixed claims, including their nonclaimable draining workers, then computes a
deterministic bipartite matching witness from every current assignment/new
allowance unit to distinct compatible admission-eligible slots. Equivalently,
every subset of those demand buckets must fit the union of its compatible free
eligible shape slots. The epoch carries the bounded matching/slice witness
needed for the environment transaction to preserve that invariant as it clears
old assignments and consumes new allowances. A slot matched to one outstanding
assignment cannot simultaneously justify an allowance for another bucket;
claim order may delay work but cannot create a capability-overcommitted plan.

The environment agent applies an accepted placement allowance in one local
database transaction:

1. lock the current allowance epoch;
2. select the oldest locally eligible unclaimed trials in the matching demand
   bucket;
3. compare-and-set their physical pool and assignment epoch;
4. clear only excess unclaimed assignments from superseded epochs; and
5. publish the matching local admission grant.

The protected trial-admission companion row is the assignment authority;
candidate-owned trial state is only a projection. The design does not claim a
distributed exactly-once transaction across the management and environment
databases. It achieves one current assignment through local compare-and-set,
idempotent epochs, and leases.

The trusted worker-claim guard permits a claim only when:

- the worker is active and not draining;
- the worker is the one live registration for its accepted submission intent,
  approved shape, and authenticated Slurm job;
- its number of protected live claim leases is below that shape's concurrency;
- the trial is assigned to the worker's physical pool;
- the worker's exact approved shape satisfies the trial's sealed normalized
  capacity requirements;
- worker and trial execution generations are compatible;
- candidate and worker protocol bindings match;
- the worker's claim-authorization epoch matches the current admission epoch;
- the local admission grant is current and accepted; and
- all existing scheduler, team-quota, capability, family, and retry gates pass.

The guard locks the worker registration and allocates one concurrency lease in
the same transaction that changes the protected trial claim state, so racing
worker processes cannot oversubscribe a shape.

The protected attempt/claim lifecycle is:

```text
pending-unassigned <-> assigned -> live
pending-unassigned or assigned -> cancelled-terminal
live -> completion-pending -> terminal
live -> cancel-pending -> terminal
live -> retry-pending -> terminal
```

Only an unclaimed `assigned` attempt may return to `pending-unassigned`, through
the fenced allowance-withdrawal or authorized requirement-change transaction.
`live` begins in the atomic worker-claim transition and owns one exact worker
concurrency lease. The three pending-terminal states are nonclaimable and keep
that lease charged until the exact cessation/completion rule succeeds. A
verified infrastructure-lost transaction may advance the old live/pending
claim directly to `terminal` while recording its reason. `terminal` and
`cancelled-terminal` are monotonic for that attempt identity. Retry creates a
new `pending-unassigned` attempt with a fresh identity only after the preceding
attempt is terminal; it never moves the old identity backward.
`pending-unassigned` consumes demand but no placement or claim capacity;
`assigned` consumes one matching placement-allowance slot but no worker
concurrency lease; `live` and every pending-terminal/unknown claimed state
consume one fixed protected claim slot; terminal states consume neither.

Before global cutover only, the accepted local admission may use a fenced
`legacy-compatibility` mode. This mode is not an allocator and creates no
global grant or extra capacity: the trusted adapter mirrors only the exact
current legacy policy, pool assignment, worker/job identity, shape/concurrency,
candidate, generation, and legacy-writer epoch already authorized by the
single active legacy path. The protected guard applies all normal sealed
requirement, generation, concurrency, retry, and attempt-identity checks. A
unique authority-mode constraint permits at most one pre-cutover legacy,
global, or explicitly prepared legacy-rollback admission incarnation for a
subject/pool. The fleet-wide freeze expires legacy new-claim authority;
imported commitments are installed as global admissions while claims remain
frozen, and final activation changes the mode only after the matching global
epoch is acknowledged. Legacy compatibility cannot consume a global allowance,
move neutral work, accept a new worker after its writer is fenced, or survive
global activation.

An unassigned globally managed trial is not claimable. This removes the
current neutral-routing bypass. Fleet worker credentials point at the trusted
claim path; candidate code cannot mint an alternative worker credential or
authorize a protected claim transition.

A fleet worker releases its protected capacity claim through the same trusted
guard using its scoped token and claim identity. Candidate-owned trial outcome
or status updates may be reflected for workload behavior, but cannot end a
capacity claim, make a draining worker claimable, or provide manager release
evidence by themselves.

If authoritative, ownership-verified Slurm state proves a bound job terminal
before its protected claims close, the trusted agent fences that exact worker
incarnation and transactionally closes its concurrency leases as
infrastructure-lost. It then applies the environment's protected failure,
retry, or destroy policy to the affected trials. Only that agent
acknowledgement can complete worker release; if the environment is unavailable,
the physical reservation remains charged despite the terminal Slurm job.

New-claim authority expires with the admission lease. A worker holding an
already protected live claim receives a completion-only capability bound to
that claim; the trusted guard continues to accept its bounded heartbeat,
result/failure, and completion transitions through drain or management outage,
without permitting another claim. Completion capability and the minimum result
path remain available until that claim is terminal. An owner cancellation first
marks the protected claim cancel-pending and prevents retry. It does not make
that concurrency slot claimable or reduce physical reserved capacity. The
exact worker must acknowledge cessation through the guard, or authoritative
terminal-job evidence must let the trusted agent close the claim. After claim
terminality, the concurrency slot may be reused only if that worker remains
active under the current admission plan; manager capacity stays charged until
the separate worker/reservation release protocol proves physical terminality.
The trusted wrapper, rather than candidate code alone, observes an
authenticated cancel-pending record bound to the exact attempt, worker, and
sandbox. It may terminate only that claim's contained trial process, perform
its scoped cleanup, and submit the cessation acknowledgement through the guard.
It cannot cancel a sibling claim, terminate the outer multi-concurrency worker
job, or synthesize owner cancellation for capacity reclamation.

Retry, requeue, and worker reassignment use the same rule. A stale heartbeat,
watchdog deadline, workload `failed`/`cancelled` state, or missing worker
process is not enough to return a protected attempt to the unclaimed queue.
The old claim first enters a nonclaimable retry-pending state and remains fixed
and charged. One protected transaction may create or expose the replacement
unclaimed attempt only after exact worker cessation acknowledgement or
ownership-verified terminal-job evidence has closed the old concurrency lease.
The replacement receives a fresh protected attempt/claim identity. Every
heartbeat, result, cancellation acknowledgement, terminal transition, and
retry compare-and-set names that identity, so a late operation from the fenced
old worker cannot mutate or close the replacement; it is rejected and retained
as audit evidence. If cessation cannot be proven, the attempt remains visibly
blocked instead of executing twice.

Before `sbatch`, the executor registers a hash of a one-time, submission-bound
bootstrap capability with the trusted environment agent under the current
protected bootstrap-registration epoch and places the secret only in the
protected worker binding consumed by the trusted bootstrap wrapper. Registration
is a compare-and-set on the stable intent and epoch; exact replay is idempotent,
while an older or differently bound request is rejected.
After `sbatch`, the executor records the returned Slurm identity centrally and
with the agent; crash recovery performs the same record after signed adoption.
The wrapper waits until that binding exists, then exchanges the capability for
a worker credential. Possession of the capability without the exact recorded
Slurm job cannot register. The wrapper removes the bootstrap secret before
starting candidate code and binds the registration to the accepted intent,
Slurm job, exact shape, candidate/generation, admission epoch, and one live
worker identity. Restart or requeue uses a fenced re-registration that revokes
the preceding worker credential; the same intent can never authorize two
concurrent worker identities. Bootstrap material is absent from Slurm metadata,
management grants, candidate artifacts, and logs.

Closing an unsubmitted intent or observing a terminal job revokes its bootstrap
and worker credentials under a newer protected epoch before release evidence is
complete. A delayed executor registration request or wrapper exchange therefore
cannot register after the corresponding shape reservation has been freed.

When an allocation shrinks, the manager selects an exact desired multiset of
approved shapes. The environment-local transaction retains the corresponding
deterministic set of whole worker identities whose combined concurrency and
resources fit that plan, and marks the rest draining immediately, including
occupied workers. Their current trials finish, but drain state and claim epoch
both prevent replacement claims. If the retained workers do not represent the
exact target, the system temporarily undershoots after drain and the manager
may issue an approved smaller replacement shape to converge exactly; the
executor cannot invent a partial shape.

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

Pending-job cancellation occurs only after the local admission/drain
transaction has fenced replacement claims. The Slurm mutation must carry a
controller-evaluated pending-state predicate in addition to the complete
ownership proof. If the backend cannot atomically make cancellation conditional
on the job still being pending, Loom does not issue the cancellation; a job
that starts instead registers as draining, cannot claim work under the new
epoch, and exits normally. An active job with any protected claim is never sent
through the pending-cancel path.

The executor never sends a terminating Slurm signal to an active worker job as
part of scale-down, priority reclamation, rollout, lease expiry, deletion, or
normal timeout. Active drain is enforced through the protected claim guard.
The candidate worker receives a graceful drain request, but liveness does not
depend on its cooperation: after the trusted agent proves every protected claim
for that exact worker terminal, the trusted host wrapper fences worker
credentials, stops the contained candidate runtime, performs exact sandbox
cleanup, and lets the outer Slurm job exit. It cannot take that path while a
claim is live or the environment proof is unavailable. This zero-claim wrapper
shutdown is not trial preemption. Infrastructure or explicit owner-initiated
trial cancellation is reported separately from capacity reclamation.

Normal drain timeouts create a visible blocker and alert. They do not force
termination. An owner may explicitly cancel their own trial through the normal
trial API; that is separate from capacity preemption.

The environment's pre-existing task/agent execution deadline remains a
workload policy: it may stop the exact timed-out trial through the trusted
attempt-scoped cleanup path and record the protected cessation result. The
capacity manager cannot add, shorten, or trigger that deadline, and a drain or
priority timeout cannot masquerade as a workload execution timeout.

Production may therefore wait behind a running staging or development trial.
The manager must display that commitment as the reason rather than granting
the same capacity twice.

Foreign Slurm jobs are never cancelled. Their presence may make a configured
Loom grant pending or temporarily unlaunchable. Flexible unclaimed work may be
moved to another compatible pool after a bounded anti-flap interval; pinned
work waits.

### Slurm ownership proof

Job names, comments, environment names, and visible Loom metadata are not
ownership proof because another Slurm user can copy them. The fleet executor
may adopt, signal, or cancel a job only when all of these facts agree:

- the job was submitted under the dedicated pool-executor Slurm identity and
  operator-approved account/association;
- its stable submission-operation identifier and resource request match an
  accepted central reservation tranche and submission intent;
- its metadata authentication code verifies under the identified
  controller-only ownership key; and
- its pool, subject UUID/lifecycle incarnation, environment display name,
  candidate, deployment generation, allocation epoch, worker shape, resource
  vector, Slurm cluster identity, submitter identity, and submit timestamp match
  the central intent and durable local journal.

The ownership key is readable only by the trusted pool executor. It is absent
from candidates, worker bindings, job payloads, logs, and management grants;
Slurm metadata contains only the signed fields and authentication code.
Verification keys remain available for existing jobs through drain and are
retired only after terminal inventory.

For each launch, the executor first accepts capacity, then commits a central
submission intent and fsyncs a controller-local journal under one stable
operation identifier. Only then may it call `sbatch` with authenticated
metadata, after which it records the returned job identifier. If it crashes
after `sbatch` but before recording the identifier, recovery scans the
dedicated Slurm identity and adopts the uniquely matching signed operation.
While the outcome is unknown, it does not resubmit that operation. Missing,
conflicting, or unverifiable facts cause a visible quarantine: the reservation
stays charged and the job is never mutated automatically. Thus a forged
Loom-looking foreign job cannot become cancellable.

The trusted launcher renders every resource, node, partition, feature, account,
and containment argument explicitly from the approved shape. Before recording
the job ready for worker registration, the executor re-reads authoritative
Slurm state and verifies the controller-normalized request equals the intent.
A larger, incompatible, or otherwise changed request is charged from observed
resources, quarantined, and denied bootstrap/claim authorization; controller
defaults or rounding are never silently accepted as the planned shape.

Every later inventory revalidates the same immutable facts. If a registered job
is changed out of band, the agent fences that worker against new claims, the
manager charges the observed resources and marks an over-limit quarantine, and
existing protected claims still follow drain-first completion.

Immediately before a mutating Slurm command, the executor re-reads the current
job and revalidates the complete proof, then constrains the command by the
strongest supported job, cluster, submitter, and account selectors. A terminal,
recycled, or changed job identity aborts the mutation and returns to inventory;
a stale numeric job identifier alone is never sufficient.

Cryptographic ownership proof is necessary but does not create Slurm
permission. New jobs use the pool's dedicated executor identity. A manifested
legacy job under a different submitter is mutable only through an
operator-installed, manifest-bound authority that can act on that exact
cluster/job/submit-time/submitter tuple and repeats the full proof; the global
executor is not granted blanket Slurm operator power for migration. If no such
narrow authority exists, the job is accounting-adopted but drain-only: its
protected worker stops replacement claims and it remains charged until it exits
and reaches authoritative terminal state. Foreign jobs remain untouched in all
cases.

## Development Environment and Candidate Lifecycle

### Namespace and shared-service topology

`loom-dev` is the only persistent shared development namespace. It contains
trusted infrastructure installed from the fleet release:

- the authenticated development lifecycle API;
- the global capacity manager and management PostgreSQL authority;
- the personal-candidate builder coordinator;
- one shared application PostgreSQL service that provisions a distinct login
  role and database for each personal environment; and
- one shared MinIO service that provisions distinct bucket-scoped credentials
  and task, trajectory, and artifact buckets for each personal environment.

The management PostgreSQL database is a separate authority from every
candidate application database even when both services occupy the same
namespace. Management and shared-root storage credentials are never mounted
into a personal namespace or an untrusted build.

Each personal environment runs in exactly `loom-dev-<name>`. That namespace
contains the candidate frontend, Loom Service, Control Plane, LLM Gateway,
candidate-scoped migration job, Services, Ingress, and a trusted capacity
agent/claim guard installed independently from the candidate. It contains no
PostgreSQL or MinIO server. Its namespace-local credentials can reach only its
derived database and buckets. Deleting a personal environment cannot delete,
replace, or mutate an object in `loom-dev`.

Untrusted build execution does not run in `loom-dev`. The coordinator creates
an attempt-scoped restricted builder sandbox with no service-account token,
host namespace, host path, Docker socket, management network, Slurm network,
shared-root credential, or sibling-environment route. The sandbox receives
only the sealed source input and an attempt-scoped artifact-upload capability,
and it is deleted after terminal publication or bounded cleanup. Build
execution may use a transient system-generated namespace, but no second
persistent shared namespace exists.

There is no static shared `development` application. The historical
`development` environment is a legacy allocation and rollout input that must
be frozen, drained, and retired during migration. Feature testing happens in
personal environments; `loom-dev` remains trusted infrastructure.

### Physical-pool identity

A dynamic environment's logical identity remains `dev-<name>`. Its worker
policies use physical pool names `oldlab` and `gb10`; `dev-<name>` is no longer
used as a synthetic physical pool.

The current singular dev-instance Slurm configuration becomes an exact
operator-owned map of physical-pool templates. Each enabled dynamic instance
gets both policies when both candidate/profile bindings are ready.

Protected worker files and artifact paths include subject UUID/lifecycle
incarnation, environment display name, physical pool, candidate, and deployment
generation. A new subject or generation never overwrites a path that an old
pending or running job can still read.

### CLI contract

`loom service up` requires an explicit environment and never infers a remote
target from the current checkout:

```text
loom service up --environment local
loom service up --environment dev-alice
loom service up --environment staging --candidate <immutable-id>
loom service up --environment production --candidate <immutable-id>
```

These illustrate target selection, not a way to omit target-policy arguments.
The command fails before mutation when the selected authority requires an exact
candidate, approval, expected rollout epoch, or other protected input that was
not supplied.

The selector routes to the correct authority:

- `local` runs the existing local Compose workflow and retains its source-fresh
  `--build` behavior for mutable local `:dev` images;
- `dev-<name>` seals the current allowed build contexts, submits the immutable
  source digest to the trusted personal-dev candidate pipeline, and deploys
  only the exact safety-attested output through the guarded create-or-update
  lifecycle; and
- staging and production use their existing protected rollout authorities and
  cannot bypass gates through a Compose or direct-Kubernetes shortcut.

For a personal development environment, `--candidate <digest>` reuses an
already published immutable personal-dev candidate and skips a build only when
the caller owns or is explicitly authorized to use it and its attestation,
protocols, artifacts, and retention state are current. Without that option,
the CLI stages an immutable source manifest and verifies it before and after
copying so a concurrently changing checkout fails rather than producing a
mislabelled artifact. The manifest includes allowed tracked and untracked build
inputs, records Git commit and dirty state for provenance, and excludes Git
metadata, ignored outputs, credentials, and paths outside the declared build
contexts. Committed, uncommitted, and permitted untracked source is allowed for
personal development; the content digest, never a mutable tag, branch, Git
label, or base-commit check result, is authoritative.

The trusted personal-dev pipeline attests infrastructure safety, not feature
correctness. Mandatory gates validate the sealed input, build policy, builder
isolation, base-image and dependency constraints, secret exclusion, artifact
and image scanning, protocol compatibility, immutable publication, and exact
source/image/profile digests. Ordinary feature tests may fail because testing
unfinished behavior is the purpose of the environment. The resulting
attestation is explicitly `personal-dev-only`; no staging or production
authority accepts it. A base commit's CI status never approves additional
working-tree content.

Snapshotting never follows a symlink outside an allowed context, rejects path
traversal and unsupported special files, enforces per-owner file-count and byte
limits before upload, and hashes deterministic relative paths, file types,
modes, and contents. The isolated builder consumes only the sealed snapshot,
not the mutable invoking checkout.

The snapshotter traverses from already-open context directory descriptors using
no-follow, beneath-root resolution; it opens and verifies each input by
descriptor and compares identity/metadata around the read. It never reopens a
previously checked candidate-controlled pathname for copying. A symlink or file
replacement race therefore fails or snapshots the verified opened object, but
cannot substitute an out-of-context file between manifest checks.

Remote preflight authenticates the caller and resolves the immutable owner and
environment operation epoch before it starts a build. A user cannot create or
update another owner's personal environment merely by spelling its name, and a
lost ownership/operation-epoch race does not mutate the environment. Any
unreferenced content-addressed result enters normal bounded garbage collection.

Staging and production accept only the exact candidate forms allowed by their
existing authenticated rollout policy. In particular, this command does not
turn a personal-dev-only candidate or dirty local checkout into a staging or
production candidate. Candidate, `min_slots`, `max_slots`, and the expected
environment operation epoch are submitted in one lifecycle request, so
concurrent users cannot apply the image from one invocation and the capacity
from another. Repeating the same content and policy is idempotent and does not
create a new deployment generation.

Existing `loom dev create`, `status`, `list`, and `destroy` commands remain
lower-level lifecycle interfaces. A capacity/update operation is added so a
ready instance can change aggregate min/max without destroy/recreate or an
unnecessary candidate rollout.

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

Although Slurm accounts the outer job to the executor-approved submitter, the
trusted host wrapper runs candidate worker code only inside the contained
runtime. That runtime receives no Munge socket/key, Slurm client configuration,
controller DNS/network route, host PID/user namespace, executor journal,
ownership key, controller-local binding path, or unrestricted host Docker
socket. It cannot invoke `sbatch`, `srun`, `scontrol`, or `scancel` with the
outer job's authority. The minimal host wrapper is immutable trusted-fleet code
and accepts no candidate-supplied command line or script fragment.

Every contained worker/trial uses project, container, network, cgroup, and
sandbox identities derived from subject incarnation, submission intent, and
worker identity; concurrent environments cannot collide. The trusted wrapper,
not candidate code, owns idempotent terminal cleanup and orphan reconciliation.
It also enforces drain completion only after an authenticated protected
zero-live-claim acknowledgement for that worker identity; a candidate that
ignores its graceful drain request cannot retain an idle reservation forever or
cause the wrapper to terminate another worker's live claim.
For explicit owner cancellation, it separately enforces only the authenticated
attempt-scoped cancellation and cleanup described by the claim protocol; that
authority cannot be broadened into worker-job or sibling-trial termination.
Both OLDLAB and GB10 use the measured non-exclusive packing model; whole-node
exclusivity is not a fallback for missing containment evidence.

The capacity agent, worker-token authority, and worker-claim guard are also
installed from the trusted fleet release rather than the candidate. Candidate
runtime and migration credentials cannot alter their protected database schema
or impersonate their management/API identity. Protected column privileges and
database guards prevent candidate roles from restoring a drained worker or
manufacturing an authorized task claim.

A personal candidate may change scheduler proposal logic behind the bounded
guard interface, but changes to the manager, executor, trusted launcher,
capacity agent, bootstrap wrapper, or claim guard do not become trusted merely
because they are present in that candidate. Such changes are exercised in
local/CI isolation and require the reviewed trusted-fleet release path before
shared capacity can run them. Status reports both candidate and trusted-fleet
versions.

Candidate builds that execute user source run in a bounded isolated builder,
not through an unrestricted host Docker socket. Each build gets an ephemeral
owner/operation-scoped sandbox with CPU, memory, PID, disk, output-size,
network, and wall-time limits. Build steps receive no management, Kubernetes,
registry-push, object-store, executor, or other environment credentials. A
trusted exporter outside the sandbox verifies the sealed input digest, scans
and publishes the immutable output, and writes the attestation. Cross-owner
cache input is content-addressed and read-only; untrusted mutable cache state is
never shared. Activation remains gated on the non-exclusive containment
evidence required by #896.

### Deployment generations

A capacity-only update that retains the current candidate, worker-profile
bindings, and protocol compatibility does not create a deployment generation.
It compare-and-sets the expected environment operation/configuration epoch and
updates aggregate min/max/surge policy in one management transaction; the next
allocation epoch performs any ordinary scale or drain. A combined candidate
and capacity update uses the generation cutover below and makes the new policy
effective only in its final central transaction, so candidate and policy cannot
mix across concurrent invocations.

A deployment generation has the lifecycle:

```text
preparing -> ready -> activating -> current -> draining -> terminal
preparing or ready -> failed
preparing, ready, or failed -> cancelled -> terminal
```

The existing current generation remains current while a replacement is
`preparing`, `ready`, or `failed`. A replacement becomes `ready` only after the
trusted lifecycle has verified its immutable candidate descriptor and
attestation, control-plane and database compatibility, trusted capacity-agent
and claim-guard protocol compatibility, scoped token/binding material, and
every physical-pool worker profile required by the environment policy. An
operator environment-policy migration that changes a non-development static
environment's enabled pool set does so in the same reviewed operation.
Personal development always requires OLDLAB and GB10; missing artifacts for
either cannot be hidden by a rollout or user candidate silently dropping the
pool.

`cancelled` is allowed only before the central activation intent and only under
the owning lifecycle operation epoch. It fences late readiness publication,
closes any unaccepted proposal, and reaches `terminal` after proving that the
generation has no trial, accepted shape, submission, job, worker, or protected
credential commitment. A failed or superseded update and a pre-activation
destroy use this path; cancellation cannot undo `activating` or later states.

Generation cutover is an epoch-bound two-phase protocol because management and
environment state are in different PostgreSQL databases:

1. The trusted lifecycle revalidates readiness freshness and exact profile,
   artifact, protocol, policy, and configuration generations, then records a
   central `activating` intent against the expected environment operation epoch
   and previous current generation.
2. The environment agent verifies that intent and, in one protected local
   transaction, changes the local current generation, marks the previous local
   generation draining, and records an acknowledgement/high-water mark. Trial
   submission locks this row and stamps its current value in the same local
   transaction.
3. The agent idempotently publishes the acknowledgement. The lifecycle then
   changes central current/draining generation state and the request's
   candidate/capacity policy together in one management-database transaction.

Unique constraints permit at most one local and one central current generation.
Before step 2, submissions remain old. After step 2, they bind new and may
queue, but the manager issues no new-generation capacity until step 3 completes.
A central `activating` intent also freezes every new capacity increase and
placement change for that subject, including old-generation scale-up, until
step 3 commits; existing assignments, accepted commitments, claims, completion,
and drain continue. This prevents the old capacity policy from making a new
side effect after the atomic candidate/policy update has become irreversible.
A crash between steps resumes the same operation epoch. Once step 1 publishes
the central activating intent, delayed acknowledgement cannot prove step 2 did
not race, so the operation cannot be declared failed, reversed in place, or
interleaved with another update. If safety requires, submissions are frozen
while that operation finalizes; rollback is then a subsequent fenced generation
operation. A failed or stale operation rejected before step 1 leaves the old
generation and policy current. No trial can bind to a merely ready or unrelated
generation.

If only worker-pool capacity or health is lost later, new submissions still
bind to the current generation and queue. If candidate integrity, control-plane
protocol, or trusted claim-guard readiness is invalid, submission fails with a
retryable unavailable result and creates no trial. Neither case silently
selects a different generation.

Every trial records the execution generation active when it is submitted. A
trusted submission boundary stamps that value from the current lifecycle
state; candidate code cannot select an arbitrary generation. Every worker
registers its candidate and deployment generation. This prevents queued work
from silently crossing a candidate rollout.

During update:

- submissions before the protected local cutover remain on the old current
  generation and submissions after it bind the new local current generation;
- live old-generation protected claims finish or acknowledge cessation on
  old-generation workers, regardless of workload projection state;
- unclaimed old-generation trials stay old unless the owner explicitly
  migrates or cancels them;
- old and new commitments both count against the aggregate environment
  commitment ceiling;
- optional `rollout_surge_slots`, defaulting to `0`, permits only exact
  replacement capacity above `max_slots`, paired with old-generation shapes
  already selected for monotonic drain; and
- old worker credentials and artifacts are removed only after terminal release.

Generation `draining` means closed to new submissions; it is not the protected
worker-drain flag. Existing unclaimed old-generation trials remain eligible for
old-generation placement and claims, and the manager may retain or allocate
old-generation shapes within the aggregate policy so they can finish. A
specific old worker stops replacement claims only when a separate allocation
decision marks that worker draining. A generation becomes terminal only after
its queued workload, every protected claim, and every physical commitment are
terminal, migrated, or explicitly cancelled as applicable; workload
terminality alone is insufficient.

Residual warm minimum belongs to the current generation. Warm old-generation
workers with no old demand are selected for monotonic drain instead of being
retained forever; with zero surge, their terminal release precedes replacement
current-generation warm capacity. This rollout-specific rule overrides the
general preference to retain healthy warm placement but never terminates a
running old-generation trial.

The control plane and database schema must declare compatibility with every
overlapping worker protocol. If compatibility is absent, the rollout drains
the old generation completely before activating the new one.

Surge raises only the environment's temporary worker-generation-overlap
ceiling. It does not raise task-claim concurrency or fair-share service and
does not bypass a physical-pool envelope, resource limit, pending-launch limit,
tier ceiling, or capacity-account quota. If those scopes have no free
compatible capacity, rollout waits even when the environment has configured
surge.

Demand remains separated by generation. Fixed work consumes the aggregate
target first; among otherwise equal task-priority demand for one environment,
older eligible submission time retains its normal order across generations.
With the default zero surge, a new generation may therefore wait for old
commitments to release. Status reports the exact old-generation blocker, and
the owner may explicitly cancel or migrate eligible unclaimed work rather than
the allocator silently crossing generations.

Explicit migration is a protected compare-and-set operation, not a candidate
field update. It requires the trial still be unclaimed, the target be the
same generation marked current at one finalized central and protected-local
cutover epoch with valid readiness, and its payload/protocol be declared
compatible. It is unavailable while an activation is between those databases.
The operation clears the old pool assignment and admission epoch, preserves the
original submission time and task priority, records the audit reason, and lets
the global manager place it anew. A racing claim, incompatible payload, stale
operation epoch, or non-current target rejects the migration without changing
the old binding.

### Concurrent lifecycle limits

Capacity fairness alone does not protect the shared Kubernetes, PostgreSQL,
MinIO, DNS, or candidate-builder control plane. The development lifecycle also
enforces:

- a required operator-configured global live-instance limit;
- a configurable per-owner live-instance limit;
- per-owner aggregate min/max capacity quotas;
- global and per-owner build admission/rate and retained-artifact count/byte
  quotas;
- a bounded fair provisioning/build queue; and
- per-environment operation-epoch serialization.

Independent environments may provision in parallel. Shared fixture mutations
use narrowly scoped locks and idempotent checkpoints. Two operations on the
same environment cannot overlap.

Published source bundles, images, profiles, and bindings are immutable and
reference-counted by deployment generation and nonterminal submission/job.
Garbage collection deletes only unreferenced artifacts after a configured
grace period and a repeatable mark manifest; it cannot remove bytes or keys
needed by a queued, pending, running, draining, rollback-retained, or
quarantined generation. Identical content is deduplicated without transferring
environment ownership or authorization.

### Environment operation lifecycle

The environment subject lifecycle and the durable operation lifecycle are
separate. The subject projection is:

```text
requested -> provisioning -> ready
requested or provisioning -> failed
failed -> provisioning with a fresh attempt, or deleting
ready -> updating -> ready (success or pre-activation update failure)
updating -> activating -> ready
requested, provisioning, failed, ready, or pre-activation updating -> deleting
activating -> ready -> deleting when a successor destroy is queued
deleting -> draining -> terminal tombstone
```

Each create, update, capacity change, or destroy has its own durable logical
operation row:

```text
requested -> running -> succeeded
requested or running -> failed
failed -> retrying -> running with a fresh operation-attempt identity
requested, pre-commit running, failed, or retrying -> cancelling -> cancelled
running -> activating -> succeeded
running -> deleting -> succeeded
```

The operation enters `activating` when a candidate/policy activation intent is
committed and enters `deleting` when the central deletion fence is committed.
Both states are irreversible in place: they can only converge to `succeeded`.
Failure before an irreversible intent records the operation `failed`; for an
update, the subject returns to `ready` on its unchanged current generation and
policy. Subject state therefore never has to pretend that a failed update
removed the still-serving generation.

Every operation has a stable idempotency key, subject UUID/lifecycle
incarnation, immutable owner, expected environment epoch, requested candidate
and capacity policy, deadline, and checkpoint manifest. A retry with the same
key resumes the same logical request. A different create, update, or capacity
operation compare-and-set fails while one is active. The sole exception is an
idempotent destroy, which may occupy one fenced successor slot but cannot start
until the active operation reaches its permitted convergence point. Attempts
inside a logical operation have immutable, monotonically increasing identities.
A retry may resume an incomplete checkpoint only while its attempt is still
`running`. Once an attempt or replacement deployment generation is `failed`,
retry first fences its late callbacks, cancels and terminalizes that generation
after proving it has no commitment, and creates a fresh attempt and
replacement-generation identity. Neither a failed attempt nor a failed
generation ever returns to `running`/`preparing` or publishes readiness later.

Capacity remains effectively zero until the subject, current candidate,
required OLDLAB and GB10 profiles, trusted guard, and all mandatory fixtures are
ready in one management transition.

Every created database, namespace, DNS record, object prefix, credential,
binding, and builder artifact is tagged by subject, logical operation, and
operation-attempt identity. Compensation deletes only objects proven to belong
to that failed attempt and never a shared, retried, or newer-generation
fixture. Failed and timed-out operations remain visible and charged to
applicable provisioning, live-instance, and artifact quotas until their failed
attempt cleanup completes or subject deletion reaches a terminal tombstone.

Before a fresh attempt touches a deterministic subject-scoped singleton, it
reconciles the predecessor attempt's checkpoint and the external object's
ownership metadata. It may reuse an exact compatible object only through an
audited compare-and-set that transfers the object to the new attempt epoch in
both management state and the external authority. The predecessor's cleanup
then loses deletion authority. Otherwise the retry waits for proven predecessor
cleanup and creates a new object; it never overwrites or informally adopts an
ambiguous fixture. An external system without conditional ownership mutation
uses the cleanup-and-recreate path.

Destroy racing provisioning records cancellation under a newer environment
epoch; the old operation can finish only cleanup checkpoints, not publish
readiness.

An update failure before the central activating intent records the operation
failed and returns the subject to `ready` on its unchanged current generation
and policy. After that intent, the operation remains `activating` and must
resume the same local application and management finalization; any rollback is
a subsequent fenced generation operation. It cannot publish a mixed
candidate/capacity update or simply declare success.

A destroy request racing an `activating` operation is recorded as the next
fenced lifecycle operation. It blocks later updates, but cannot skip or reverse
the already-published activation intent: the trusted lifecycle first converges
that exact activation to `ready`, then immediately applies the successor
deletion epoch. A destroy may transition directly from `updating` to `deleting`
only while no central activation intent exists.

### Drain-preserving deletion

Destroy is an asynchronous, idempotent lifecycle rather than immediate fixture
deletion:

```text
requested, ready, updating, provisioning, or failed
    -> deleting -> draining -> terminal tombstone
    -> fixtures/artifacts garbage-collected while tombstone remains
```

Destroy begins with a management transaction that records the deletion
operation/lifecycle epoch and makes effective min/max zero. In that same
central transaction it closes unaccepted proposals, revokes unconsumed launch
permits, moves every `prepared` or `launch-ready` intent to `closing`, and makes
the lifecycle/configuration epoch fail the final launch CAS. Accepted,
`submitting-unknown`, bound, observed, and running commitments remain charged.
Thus deletion stops new physical increases immediately even before its next
allocator cycle, while a launch CAS that serialized just before deletion is
treated as possibly submitted and reconciled normally.

The environment agent then applies that exact deletion epoch in one protected
local transaction: it rejects new submissions, advances admission, clears
only unclaimed assignments, marks protected unclaimed trials cancelled by
environment destroy, suppresses retry/requeue, and marks workers draining.
Every live protected claim retains its exact pool, generation, worker,
protected attempt identity, and completion binding until that claim terminates,
regardless of candidate-visible workload state. Candidate-visible state is
updated as a projection but cannot veto the protected cancellation. The agent
acknowledges application centrally; replay of either step is idempotent. If the
environment is unreachable, deletion stays pending and charged rather than
pretending the local fence succeeded.

The environment database, trusted capacity agent, minimum control-plane
completion path, scoped bindings, DNS, and old-generation artifacts remain
available while protected claims finish or acknowledge cessation and executor
reservations remain nonterminal. A live claim whose workload projection later
fails completes under the destroy policy rather than requeueing. Normal timeout
only alerts; it does not kill a running trial. An owner may separately cancel
their own running trial through the normal trial API.

Only after protected environment state and authenticated Slurm inventory prove
every generation terminal may the lifecycle revoke generation credentials,
delete mutable fixtures, and leave a durable terminal tombstone. Environment
name and immutable owner binding cannot be reused before that point. Later
display-name reuse creates a new subject UUID/lifecycle incarnation and cannot
inherit old grants, tokens, artifacts, paths, or audit authority. Deleting and
draining environments continue to count against live-instance,
provisioning, owner, tier, and capacity limits until their corresponding
resources are terminal, preventing delete/recreate quota bypass.

## Credentials and Security

- Capacity grants, demand snapshots, observations, audit rows, and status
  responses contain no secrets.
- Reporter credentials are subject-UUID/lifecycle-incarnation scoped and may
  publish demand only for that exact environment identity.
- Executor credentials are pool-, host-, and incarnation-scoped workload
  identities and may accept grants or publish observations only for their
  physical pool from the registered controller authority.
- Only the manager role may choose or mutate desired allocations, shape plans,
  and placement allowances. Scoped executor procedures may compare-and-set an
  offered tranche to accepted, create the corresponding immutable `prepared`
  submission intents bound to its accepted shapes, and append monotonic
  observation or release evidence; they cannot change the offered environment,
  pool, shape, resource, slot count, priority, or epoch.
- Tier, quota, pool, and worker-profile configuration requires an operator
  role and is audited.
- Worker tokens are scoped to subject UUID/lifecycle incarnation, environment
  display name, physical pool, candidate, deployment generation, submission
  intent, worker identity, approved shape, admission epoch, and bounded expiry.
- Only the trusted environment capacity agent may mint those worker tokens or
  execute the protected queued-to-claimed transition.
- Candidate scheduler logic may propose a trial selection to the trusted claim
  guard, so candidate scheduler changes remain testable. The guard alone locks
  and validates the protected admission, assignment, generation, worker, and
  claim records before granting work. Candidate-owned trial state is never
  sufficient authorization.
- The trusted submission boundary stamps trial execution generation, and only
  an explicit authorized migration operation may change it while unclaimed.
- Revocation is generation-scoped; deleting an old generation cannot revoke a
  current generation's workers.
- Controller-local binding files are mode-restricted to the trusted executor
  service identity, immutable per generation/submission, and referenced rather
  than embedded in grants.
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
new claims stop; executors cancel pending work and drain when able. Existing
protected claims retain only their completion/heartbeat path and may finish.

An accepted reservation is not an offline launch permit. While the management
database cannot execute the final central CAS, `prepared` and `launch-ready`
intents cannot call `sbatch`. Only an intent that durably reached central
`submitting-unknown` before the outage may complete its one already-authorized
call, after the matching journal fsync; it then follows normal ambiguous-result
recovery.

Proposal acceptance and authority expiry use management-database time. Local
consumers also bound a lease by monotonic elapsed time since receipt and reject
timestamps outside the configured clock-skew envelope. Clock disagreement may
stop work early or delay a local fence within that envelope, but it can never
release central capacity; terminal executor evidence is still required.

The elapsed-time source must include host suspend. A process or host restart
does not restart the grace interval: the consumer treats the in-memory lease as
invalid and must revalidate the still-current grant against management-database
time before accepting proposals, submitting jobs, or authorizing new claims.
If the management plane is unavailable during that restart, it fails closed;
accepted physical commitments remain charged.

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

A missing, corrupt, regressing, or checksum-invalid controller-local journal or
ownership-key set fences that executor incarnation and stops acceptance,
submission, and mutation. Central reservations and every job in the dedicated
authority scope remain charged. Recovery reconstructs a journal only from the
frozen central intent high-water mark, complete authenticated Slurm/accounting
inventory, protected environment bindings, and any securely retained key
version; ambiguous jobs remain non-mutable quarantine. A fresh empty journal or
rotated key is never treated as proof that old operations do not exist.

### Crash around Slurm submission

The executor accepts capacity and durably records the central intent and local
journal before `sbatch`. Submitted jobs include the stable operation identity
and authenticated ownership, environment, pool, candidate, generation,
allocation, resource, and executor-epoch metadata. If the process crashes
between `sbatch` and recording its result, restart inventory either adopts the
unique verifiable job or quarantines the unresolved operation. It never assumes
absence and submits a duplicate while the result is unknown.

### Database restore and authority incarnation

Grants include an authority-incarnation identifier and monotonically
increasing epochs. Executors and protected environment agents persist their
accepted allocation/admission high-water marks independently of the management
database.

If a database restore rolls the authority behind an executor or environment
agent high-water mark, that component rejects new authority and enters
recovery. Recovery either:

1. advances the restored ledger beyond every observed executor and environment
   high-water mark and imports all protected claims, workers, submission
   intents, jobs, and other commitments; or
2. creates a new authority incarnation only after every old agent, executor,
   worker, and job is fenced, inventoried, and adopted or drained.

Changing incarnation is never an automatic way to forget old capacity.
An unreachable environment or pool remains charged as unknown quarantine and
prevents capacity reuse during recovery.

### Environment-database restore

The protected environment schema stores its own database incarnation and
admission high-water mark. If a restore or replacement falls behind a worker,
agent, or manager high-water mark, the claim guard rejects new claims and the
environment enters recovery. The trusted agent inventories scoped workers,
claims, trial bindings, and executor-observed Slurm jobs, then either
reconstructs protected companion records under a newer admission epoch or
drains the unmatched commitments. Candidate-owned trial state is not accepted
as proof that capacity is free. Worker tokens from the lost incarnation remain
fenced, while all observed physical commitments remain charged until the
reconstructed record or terminal evidence is complete.

### Pool-capacity reduction or pool failure

Specific incompatible demand waits. Flexible unclaimed demand may move to a
healthy compatible pool. Running work remains fixed and charged. A configured
capacity reduction below commitments enters over-limit drain mode.

Physical pool health is based only on executor heartbeat, validated Slurm
authority/configuration, and independent infrastructure checks. Candidate,
generation, protected binding, and worker-profile readiness determine one
subject's eligibility for that pool and cannot mark the physical pool unhealthy
for other subjects. A missing or zero capacity grant is not a pool-health
failure, avoiding a circular grant-missing deadlock.

### Quarantine and manual recovery

There is no generic operator endpoint that subtracts reserved slots or marks a
job released. To resolve missing ownership or accounting evidence, an operator
starts a recovery epoch that:

1. stops pool scale-up and new claims, fences the affected executor incarnation
   and ownership key, and captures the management high-water marks;
2. obtains complete authoritative Slurm controller/accounting inventory and
   complete protected inventories from every affected environment;
3. binds conclusively owned commitments to stable tranches and adopts or drains
   them, while leaving foreign jobs untouched; and
4. records terminal identities only where the frozen inventories prove no
   corresponding nonterminal job, worker, or claim can exist.

The resulting reconciliation manifest, operator identity, authority epochs,
and evidence digests are audited before normal allocation resumes. If required
evidence is unavailable, the quarantine remains charged. A physically rebuilt
controller can establish absence only through the database-restore/new-
incarnation fencing procedure; rebuilding or rotating credentials alone does
not free old capacity.

## Observability

The management API and CLI expose, without secrets:

- manager incarnation, writer epoch, recovery/activation mode, executable
  new-capacity ceiling, increase-freeze state, and last successful allocation;
- registered pools, resource envelopes, health, and executor heartbeat;
- tier, owner, and environment requested, desired, proposed, reserved,
  pending, active, draining, unknown, and released slots;
- corresponding exact worker-shape plans and CPU, memory, GPU, node/domain, and
  other resource-vector commitments and headroom;
- normal versus rollout-surge shape identities, their exact replacement pairs,
  and protected old-worker drain acknowledgements;
- candidate, deployment generation, and configuration-generation bindings;
- candidate/profile readiness, current/old generation state, reporter/executor
  incarnations, sequence high-water marks, lease state, protected admission
  authority mode/incarnation, and distinct subject, logical-operation,
  operation-attempt, and checkpoint/phase state;
- sealed demand/allocation classes, invalid-snapshot reasons, matching
  witnesses, and placement allowances;
- protected attempt counts by unassigned, assigned, live, completion-pending,
  cancel-pending, retry-pending, unknown, and terminal state, including
  workload-terminal rows that remain capacity-live;
- launch-ready intents, current permit order/expiry, durable rate-bucket
  availability, and rejection reasons;
- explicit allocation, block, drain, and release reasons;
- time since demand, grant, admission, executor, and terminal observations; and
- submission-intent/journal recovery, legacy-adoption, orphan/quarantine, and
  over-limit states.

Required alerts include:

- multiple manager or pool-executor authorities;
- allocator computation deadline, serializable-commit failure, or prolonged
  absence of a successful epoch;
- capacity-envelope or generation-binding violations;
- stale demand, admission, or executor heartbeat;
- invalid/overflowed demand classes or incomplete snapshot manifests;
- accepted but unapplied grants;
- stuck launch permits, exhausted submission rates, or bootstrap-registration
  epoch conflicts;
- conflicting protected admission modes, or legacy compatibility surviving its
  freeze/activation epoch;
- stuck completion-pending, cancel-pending, retry-pending, or operation-attempt
  ownership-transfer state;
- draining beyond timeout;
- unreleased or quarantined capacity;
- rejected stale epochs or authority incarnations;
- report/observation sequence equivocation or conflicting terminal evidence;
- ownership-signature failure, duplicate signed operation, or signing-key
  compromise;
- candidate/profile readiness failures;
- stuck lifecycle/build/cleanup operations or sustained pending/quota
  exhaustion; and
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
4. Reconcile every repeated legacy environment/pool topology into one measured
   immutable physical-pool generation. Environment-specific node lists become
   validated narrowing constraints only where a real profile restriction
   exists; conflicting controller, partition, replacement-node, or allocatable
   envelope facts block shadow operation rather than selecting one copy
   implicitly.
5. Register static and dynamic environments with the measured physical pool
   envelopes, but set the independent executable new-global-capacity ceiling
   and launch gate to zero.
6. Run the global manager in shadow mode against those real envelopes and
   compare its hypothetical decisions with live capacity. Shadow rows are
   explicitly nonexecutable and cannot be accepted, converted into intents, or
   used for admission.

### Phase 2: task and lifecycle readiness

1. Add or verify physical `oldlab` and `gb10` profiles for shared and dynamic
   development environments.
2. Replace the synthetic `dev-<name>` physical pool identity while preserving
   it as a drain-only alias for any legacy worker.
3. Add mandatory task pool assignment and execution-generation binding behind
   a migration feature gate. Canonicalize capacity-relevant requirements into
   protected companion rows for every workload-nonterminal trial and every
   workload-terminal row with live or ambiguous legacy worker, claim, or Slurm
   evidence. A workload `cancelled`/`failed` label never excludes a possible
   physical commitment. An ambiguous or unsupported legacy requirement remains
   unclaimable, charged when commitment evidence exists, and produces no new
   demand.
4. Add per-instance candidate and capacity update lifecycle operations.
5. Publish immutable per-pool candidate artifacts and protected bindings.
6. Install dedicated executor identities and ownership keys, and teach every
   still-authoritative legacy writer to emit a bounded migration inventory
   record before it is frozen. This does not grant the legacy writer new
   capacity authority.
7. Route every legacy fleet worker claim through the trusted guard and
   submission-bound worker identity.
8. Install the fenced legacy-compatibility admission adapter described above.
   Prove it mirrors only the one current legacy writer, creates no global
   capacity, cannot coexist with global admission, and loses new-claim
   authority at the fleet-wide freeze.
9. Complete the protected-transition inventory for submission, assignment,
   claim, heartbeat, terminal state, cancellation, crash reclaim, retry,
   batch/family cancellation, and deletion. Prove with database privileges and
   tests that no direct writer can change capacity eligibility or close a live
   claim outside the guard. Mark protocol-incompatible workers draining; they
   must become terminal before global activation because an old direct claim
   or terminal-state path is not grandfathered.

### Phase 3: pool-by-pool authority cutover

Before the first pool cutover, enter one fleet-wide migration epoch and freeze
new worker claims, new legacy worker-job submissions/scale-up, and all legacy
neutral/pool placement mutations across both pools. Existing live protected
claims continue their completion or cancellation path, while new submissions
and unclaimed work wait in a visible bounded migration queue. Capture each
environment's
placement/admission high-water mark and each legacy writer's submission
high-water mark, then verify that the trusted claim guard and both old job
writers enforce the freeze. This global freeze remains in force until both
pools have been inventoried and adopted; it prevents the uncut legacy router or
autoscaler from changing the physical/placement commitment set across a
mixed-authority boundary.

The migration epoch closes the active-environment cohort and captures the exact
pool, resource-domain, tier, account, subject, candidate, worker-profile, and
protected-protocol configuration generations transactionally. Changes to
those generations, including capacity-only updates, are durably queued but do
not mutate the frozen authority snapshot. Lifecycle create/update operations
may build and queue, but cannot make a new environment or generation claimable
until global activation. A destroy requested after cohort closure is recorded
but does not advance the subject's deletion epoch until activation or rollback;
normal protected completion and owner-cancellation acknowledgement paths remain
available and are reconciled as terminal evidence. Any subject registered after
the cohort snapshot is born at zero capacity under the frozen epoch. An urgent
physical-envelope reduction or security fence aborts prepared cutover to the
safe no-scale state and starts a new migration epoch; it is never applied as an
unversioned mutation to the frozen snapshot. Cutover does not start until every
pre-existing subject acknowledges the freeze; an unreachable subject remains a
fixed charged commitment and blocks activation rather than being omitted.

Then, for each physical pool:

1. verify the fleet-wide legacy scale-up/submission freeze at the captured
   high-water mark;
2. stop and verify every per-environment executor timer for that pool;
3. disable the existing production-pressure writer for that pool at the
   planned fencing boundary;
4. acquire the controller-local fleet-executor lock;
5. inventory every Slurm job and worker, including legacy pool aliases and
   neutral assignments, and reconcile the frozen legacy ledger until its
   high-water mark and authoritative Slurm/accounting inventory are stable;
6. create an operator-audited, key-signed legacy-adoption manifest binding each
   conclusively matched legacy ledger row, Slurm identity/job/submit time,
   resource request, registered subject UUID/incarnation, environment worker,
   candidate, pool, and any exact retained legacy mutation authority to the
   migration epoch; ambiguous rows become non-mutable quarantine, and a proved
   row without narrow mutation authority is marked drain-only;
7. import the manifested commitments as exact accepted legacy tranches and
   bound/observed legacy submission intents in the global ledger; jobs that
   become terminal during reconciliation are imported with their terminal
   evidence rather than omitted;
8. publish matching accepted grants at the imported capacity;
9. install matching local assignment/admission state while the global claim
   freeze remains active;
10. start the one pool fleet executor in adoption/drain-only mode, with proposal
   acceptance and new `sbatch` disabled; and
11. verify no legacy unit or code path can submit or cancel a job.

The signed migration manifest is the only exception to the normal
controller-local-journal proof for jobs created before cutover. It requires all
authoritative legacy, Slurm, and protected worker facts to agree, cannot be
extended after that pool's frozen high-water mark, and expires one job at a
time as imported commitments become terminal. A name, comment, or environment
label alone never qualifies a legacy job for adoption or cancellation.

The pool not yet cut over is treated as a fixed legacy commitment domain. No
legacy or global component may move flexible demand into or out of either pool
during the freeze. After both pool executors, protected local admission gates,
inventories, and imported commitments agree at the migration epoch, the manager
publishes a prepared activation epoch. Both executors and every protected
environment gate acknowledge that exact preparation while remaining frozen.
The manager then commits the epoch active and changes the executable
new-capacity ceiling from zero to its approved bounded value in one
management-database transaction. Executors may accept only proposals carrying
that active epoch, and local claim guards release their freeze only after
observing it. Consumers may observe the commit at different times, which can
delay work but cannot create a mixed authority or exceed a reservation. Queued
work is then admitted under the global epoch.

If either pool fails cutover, both pools remain placement-frozen while already
running work completes; recovery either finishes adoption or executes the
fenced rollback procedure. It never restores cross-pool routing to only one
side of a mixed-authority fleet.

### Phase 4: remove obsolete authorities

After both pools pass acceptance and a defined global-authority soak/legacy
rollback window:

- disable and remove the global-development SQLite supervisor and broker
  handoff path;
- remove local policy-pressure neutral routing as an allocation authority;
- remove production-pressure capacity mutation;
- replace per-environment external timers with the two pool fleet executors;
- update environment-state profiles and dynamic provisioner templates; and
- retain compatibility readers only for the documented migration window.

Central and protected-schema migrations remain backward compatible through
that rollback window, and the exact legacy binaries/configuration are retained
immutably until its decommission checkpoint. After that checkpoint, rollback
means a compatible previous global-manager/fleet release or the safe no-scale
state; removed per-environment writers and unprotected direct claim paths are
never reconstructed ad hoc.

### Rollback

Rollback first commits a fleet-wide rollback epoch that freezes new placement
and claims across both pools and sets new grant proposals and scale-up to zero.
Accepted and running capacity remains charged and drains or is explicitly
transferred. The trusted admission, worker identity, and claim guard remain in
force; rollback never restores an unprotected direct scheduler claim path.

A legacy writer may be re-enabled only after:

- the global executor for that pool is stopped and fenced;
- the global ledger has a terminal or transferred record for every commitment;
- current Slurm inventory is captured; and
- an explicit rollback authority epoch is recorded.

Every affected protected environment gate installs and acknowledges a fresh
`legacy-rollback` admission incarnation bound to the rollback epoch, exact
transferred commitments, and the re-enabled writer's identity. It has the same
non-allocating mirror and claim-guard restrictions as pre-cutover compatibility
mode. It never reuses a pre-cutover legacy epoch, and the unique authority-mode
constraint rejects it until global admission for that subject/pool is frozen
and transferred. An unreachable environment blocks that pool's rollback rather
than regaining an unprotected legacy claim path.

A one-pool transfer may proceed before the other only while the transferred
pool is a fixed commitment domain and all flexible cross-pool placement remains
frozen. In that mixed mode the legacy writer and prepared legacy-rollback
admissions are inventory/adoption/drain-only: they cannot submit a worker job,
authorize a new task claim, or increase any commitment. The global manager
cannot reserve or place into that legacy-controlled pool. This prevents pinned
as well as flexible work from escaping aggregate environment, account, tier,
and fleet constraints while the other pool remains global.

New claims, scale-up, and cross-pool placement resume only after both pools and
every protected environment gate share one declared authority mode and a
fleet-wide placement/quota protocol with no mixed writer. A legacy rollback
mode must prove the same aggregate cross-pool constraints before one rollback
activation epoch enables it; if the retained legacy release cannot provide
that protocol, rollback remains safe no-scale and may only drain. A prepared
one-pool legacy-rollback admission therefore never receives new-claim authority
by itself.

Emergency rollback prefers a safe no-scale state over running two allocation
writers.

## Implementation Decomposition and Merge Gates

This document is the umbrella architecture and activation contract, not one
monolithic implementation change. Implementation is split into dependency-
ordered packages so independently useful review slices remain inert until
their prerequisites are proven:

1. **Management contracts and shadow ledger** add versioned central schema,
   configuration, reporting, accounting, allocator simulation, status, and
   audit surfaces. They have no executable grant, admission, or Slurm path.
2. **Protected environment admission** adds sealed requirements, assignment
   and claim records, trusted transition procedures, reporter/agent epochs,
   the fenced legacy-compatibility/rollback adapters, and the complete legacy
   transition inventory. It depends on the versioned management contracts and
   remains behind a fail-closed enforcement gate.
3. **Grant and pool-executor protocol** adds reservations, intents, permits,
   rate buckets, bootstrap registration, controller-local journals, ownership
   proof, and OLDLAB/GB10 executor dry runs. It depends on protected admission
   contracts and runs with the executable new-capacity ceiling fixed at zero.
4. **Development lifecycle and candidate isolation** converges the persistent
   shared infrastructure into `loom-dev`, removes `loom-dev-shared` and the
   static shared `development` application, and adds per-instance immutable
   personal-dev-only candidates, source snapshot/build isolation, complete
   `loom-dev-<name>` runtimes, physical dual-pool profiles, update/delete state
   machines, quotas, and the explicit `loom service up --environment`
   contract. It may be developed alongside executor internals only after their
   shared candidate/profile and protected binding contracts are fixed; it
   cannot activate capacity.
5. **Fleet migration and activation** removes the legacy mutation authorities
   only through the Phase 3 freeze/adoption protocol, then performs bounded
   activation. It depends on all earlier packages, their cross-version tests,
   the activation evidence in #906, and the containment gate in #896.

Each package has a separately reviewable implementation plan and test matrix.
No package may introduce a temporary allocator, direct claim path, or executor
that becomes an additional authority. Schema and protocol changes remain
backward compatible through the documented rollback window.

The merge gates are cumulative:

- **Contract gate**: schemas, canonical encodings, fencing fields, limits, and
  backward-compatible readers are committed and unknown versions fail closed.
- **Transition-closure gate**: all capacity-relevant workload mutation paths
  use the protected procedures, direct writes are denied, and workload,
  protected-claim, and physical terminality remain distinct in tests.
- **Zero-execution gate**: manager shadow decisions, reservation accounting,
  launch ordering, executor inventory, ownership proof, and crash recovery pass
  with no executable new capacity.
- **Lifecycle gate**: concurrent creates/updates/deletes, immutable candidates,
  dual-pool readiness, isolated builds, quotas, and rollback-retained artifacts
  pass without shared mutable state.
- **Activation gate**: fleet-wide freeze, complete legacy adoption, both pool
  executors, every protected environment acknowledgement, #896 evidence, and
  rollback rehearsal all pass before one transaction raises the executable
  ceiling above zero.

## Testing Strategy

### Allocator and property tests

Generated scenario tests must establish:

- no increase when any physical, tier, owner, environment, generation, or
  resource-vector constraint would be exceeded;
- no shape plan whose aggregate resources fit but whose node count, features,
  topology, or per-node vector cannot fit a configured resource domain;
- boundary values, unit conversion, and aggregate arithmetic cannot overflow or
  round an approved shape into a different Slurm resource vector;
- no proposal or launch when a global, tier, account, subject, or pool pending
  slot/job ceiling or applicable submission-rate limit would be exceeded;
- concurrent OLDLAB and GB10 launch transitions consume durable central rate
  tokens without exceeding any shared scope or reversing manager launch order;
- accepted or observed capacity is never reused before release;
- a conflicting accepted/observed binding or resource vector is charged both
  as the unreleased reservation and as quarantine until an audited recovery
  proves one exact physical commitment;
- changing a pool or worker-profile generation cannot reinterpret or undercharge
  an existing commitment;
- environment profiles cannot redefine controller, partition, node inventory,
  or pool envelope; valid profile constraints only narrow one referenced
  immutable physical-pool generation;
- proposed capacity can be reused only when atomic acceptance is impossible;
- duplicate, delayed, or out-of-order partial release acknowledgements cannot
  reduce an accepted reservation more than once;
- creating more environments does not increase an owner's top-level fair
  share;
- aggregate min/max spans both pools and all generations;
- strict priority has no cross-pool head-of-line blocking;
- single-pool demand is not stranded by flexible demand;
- placement allowances for partially overlapping capability sets have one
  joint matching to distinct accepted shape slots and cannot overbook their
  shared subset;
- a fixed claim on a draining worker stays charged but none of that worker's
  remaining slots can justify a new assignment;
- equivalent placement of higher-tier flexible demand preserves the maximum
  lexicographic service of lower-tier constrained demand;
- disjoint resource domains do not consume each other's priority or fairness
  turns;
- tasks sharing a pool but requiring different capability/shape classes are
  neither coalesced nor served by an incompatible worker shape;
- local task-priority bands order only one subject's fairness turn and cannot
  promote its tier or account share;
- demand-class overflow, an unmappable protected requirement, or an incomplete
  manifest invalidates the snapshot without silently dropping demand;
- no user or operator pool weights influence manager allocation or physical
  placement, and environment-local team scheduling weights cannot escape the
  subject's already granted allowance;
- deterministic results for identical state;
- rollout surge uses only residual headroom, never reclaims another subject's
  ordinary service, and is withdrawn first when that headroom is contested;
- rollout surge above `max_slots` never exceeds the approved concurrency of
  its distinct, drain-acknowledged old-shape backing, even when old and new
  worker shapes have different sizes;
- convergence after demand, health, quota, and capacity changes; and
- one subject's unready candidate/profile cannot make a physical pool unhealthy
  or block otherwise eligible subjects.

### Scheduler and placement concurrency tests

Tests must race multiple environment-agent replicas and workers to prove:

- a globally managed trial cannot be claimed while unassigned;
- one protected trial-admission row has one current assignment;
- a claimed trial cannot be reassigned;
- a capacity-requirement change racing allowance consumption or claim either
  clears the old assignment and waits for a new epoch or loses without changing
  the sealed requirement;
- candidate-owned pool, architecture, resource, or feature mutations cannot
  alter the protected requirement or authorize an incompatible claim;
- stale placement/admission epochs cannot authorize claims;
- admission expiry blocks new claims while preserving completion-only access
  for an existing protected claim;
- an owner cancellation request does not free a protected claim or worker slot
  before exact worker acknowledgement or terminal-job evidence;
- single-trial and batch cancellation may make the workload row terminal while
  the protected claim remains `cancel-pending`, charged, and completion-only;
- the trusted wrapper enforces an authenticated owner cancellation only against
  the exact attempt sandbox, and forged, stale, capacity-reclamation, or sibling
  cancellation cannot terminate a claim;
- every legacy direct terminal, cancellation, crash-reclaim, and retry/requeue
  writer is denied or routed through the trusted transition surface;
- draining workers cannot claim replacements;
- retry/requeue safely clears or renews assignment state;
- every protected attempt follows the explicit assignment/live/pending-terminal
  state machine, and no terminal or claimed attempt returns to an unclaimed
  state;
- every retry uses a fresh protected attempt identity, and delayed heartbeat,
  cancellation, result, or completion operations for an old attempt cannot
  mutate or close its replacement;
- candidate/generation mismatch is rejected;
- one accepted worker shape cannot acquire more protected concurrency leases
  than its declared slot count;
- bootstrap replay, worker-identity duplication, and requeue registration races
  leave at most one live worker identity per submission intent;
- a candidate-owned trial-state update without a protected capacity claim
  cannot grant work; and
- candidate scheduler proposals pass only through the trusted claim guard.

### Grant and executor tests

Tests must cover:

- a controller-enforced Loom Slurm association cannot submit outside its
  approved partitions, job limits, or TRES envelope;
- proposal acceptance and expiry races;
- final launch CAS racing configuration change, activation, or deletion admits
  only the operation that serialized first and never refunds an ambiguous rate
  token;
- manager leader loss and stale-writer rejection;
- allocator computation/serializable-commit failure publishes no executable
  half-epoch, revokes permits when possible, and otherwise reaches no-scale at
  bounded permit/allocation freshness expiry;
- executor local-lock exclusion;
- journal/key loss, corruption, or high-water regression fences the executor
  and cannot make an old intent or job absent;
- crash at every boundary among proposal acceptance/prepared-intent creation,
  local journal fsync, bootstrap registration/acknowledgement, `launch-ready`,
  central and journal `submitting-unknown`, `sbatch`, returned-job recording,
  and central/environment job binding;
- closing an unsubmitted intent cannot release until its bootstrap is revoked
  and acknowledged under a newer protected registration epoch, and neither a
  delayed registration request nor wrapper can register afterward;
- orphan adoption and unresolved-submission quarantine;
- clearly foreign identities do not rewrite the Loom envelope, while ambiguous
  in-scope jobs receive conservative resource and slot quarantine charges;
- duplicate and out-of-order cumulative release acknowledgements;
- partial release advances only named shape identities: retained shapes remain
  usable, while releasing/released shapes can never be reactivated;
- forged Loom job names/comments and signed-field tampering never authorize
  adoption, signalling, or cancellation;
- a valid manifest for another legacy Slurm submitter permits mutation only
  through its exact scoped authority; without one the job remains drain-only;
- an executor cannot substitute a different shape or resource request;
- Slurm-normalized resource mismatch is quarantined and cannot register a
  worker or claim a trial;
- a post-binding job/resource mutation enters nonrecoverable automatic
  quarantine and cannot return to `bound` through the pre-binding adoption path;
- pending cancellation failure;
- exact scale-down chooses unused/pending and idle identities before occupied
  workers when the same feasible reduction is available;
- pending-to-running transition racing cancellation never terminates a claimed
  trial;
- a malicious or wedged candidate that ignores drain is stopped by the trusted
  wrapper only after exact zero-live-claim proof, while unavailable or
  mismatched proof leaves the worker charged and un-terminated;
- an unexpectedly terminal authenticated Slurm job closes protected live claims
  only through the trusted infrastructure-loss transaction and remains charged
  while its environment is unavailable;
- demand returning during drain cannot reactivate a releasing worker/tranche or
  consume its capacity twice;
- partial worker concurrency;
- stale, missing, or regressing observations;
- duplicate, reordered, missing, or incomplete report pages cannot affect
  allocation before one matching manifest commits;
- same sequence or terminal identity with a conflicting payload digest fences
  the source and cannot overwrite or free capacity;
- database/local clock skew, host suspend, process restart, and monotonic lease
  expiry;
- management outage blocks `prepared`/`launch-ready` submission while allowing
  only a pre-authorized `submitting-unknown` intent to finish its single call;
- terminal proof;
- authority-incarnation recovery after management-database restore;
- environment-database restore behind worker, agent, and manager high-water
  marks;
- quarantine recovery refuses to free capacity from incomplete inventory; and
- signing-key rotation retains verification for old nonterminal jobs.

### Lifecycle tests

Tests must cover:

- simultaneous creates and updates by different users;
- invalid, confusable, oversized, or path-like display names cannot collide or
  influence a path, selector, credential, or authorization boundary;
- a live subject cannot rename or release its display name before its terminal
  tombstone permits new-subject reuse;
- crash/retry at every provisioning checkpoint, destroy racing provisioning,
  and stale cleanup that cannot delete a newer/shared fixture;
- retry after a failed provisioning or update attempt creates a fresh
  operation-attempt and replacement-generation identity; delayed readiness or
  cleanup from the failed attempt cannot revive it or affect the retry;
- retry either compare-and-set transfers an exact compatible singleton fixture
  to its fresh attempt epoch or waits for predecessor cleanup; stale cleanup
  cannot delete a transferred fixture and ambiguous objects are never adopted;
- crash/replay before and after protected local deletion application cannot
  restore submissions, omit charged capacity, or double-cancel work;
- once the central candidate-activation or deletion intent commits, operation
  failure/cancellation is rejected and replay can only converge that exact
  irreversible operation;
- the first deletion transaction closes proposals and unsubmitted intents so a
  delayed executor cannot launch against the deleted lifecycle epoch;
- multiple environments owned by one user without extra fair share;
- global, owner, provisioning-queue, build-rate, and retained-artifact limits;
- per-instance candidate selection;
- source snapshot change races, external symlinks, traversal, special files,
  and size/count overflow fail before publication;
- pathname/symlink replacement between source validation and copying cannot
  include an object outside the opened build-context root;
- immutable per-pool bindings;
- replacement-generation readiness failure leaves the old generation current;
- a failed, superseded, or pre-activation-deleted replacement generation fences
  late readiness and reaches terminal only after proving it has no commitment;
- crash/replay after central activation intent and before/after protected local
  generation activation converges the same operation without mixing candidate
  and capacity policy;
- an activating subject permits no old- or new-generation scale-up or placement
  mutation until central finalization, while existing work may complete;
- destroy racing an irreversible activation queues behind its convergence and
  then applies a newer deletion epoch;
- two-phase readiness cutover never stamps a trial to a merely ready or
  unrelated generation;
- aggregate capacity updates with default `min_slots=0`;
- a capacity-only min/max update retains the current deployment generation and
  healthy compatible workers while changing allocation through a new
  configuration epoch;
- old/new generation overlap with and without surge;
- rollout surge is paired only with draining old-generation replacement shapes,
  cannot launch before the exact protected old-worker drain acknowledgement,
  never raises claim concurrency or fair share, and converges to `max_slots`;
- one old worker identity cannot simultaneously back surge beyond its approved
  concurrency, and mismatched old/new shape sizes cannot create more surge
  slots than the distinct old concurrency selected for replacement;
- a conclusively unused closed surge intent may transfer its backing under a
  newer epoch, while ambiguous or physically nonterminal intent backing cannot
  be reused;
- nonzero warm minimum moves from old to current generation without retaining
  old warm workers forever or exceeding zero surge;
- queued task generation binding and explicit migration;
- explicit migration is unavailable while central and protected-local current
  generation epochs disagree;
- migration racing claim, stale target generation, and incompatible payload
  all preserve the old binding;
- drain-first destroy, no premature fixture/name reuse, and generation-scoped
  credential revocation;
- destroy cancels unclaimed work and suppresses retry without terminating an
  already claimed/running trial;
- deletion preserves every live protected claim's exact
  pool/generation/worker/attempt binding until that claim terminates;
- post-tombstone display-name reuse receives a new subject incarnation and
  cannot adopt old grants, jobs, paths, tokens, or artifacts;
- garbage collection never deletes an artifact referenced by a nonterminal or
  rollback-retained generation;
- malicious candidate attempts to alter trusted launch containment;
- malicious build steps cannot escape resource/network limits, receive publish
  credentials, or poison another owner's mutable cache;
- malicious worker attempts to reach Munge/Slurm controller authority or use
  `sbatch`, `srun`, `scontrol`, or `scancel` from its contained runtime;
- concurrent worker project/network/cgroup names cannot collide and trusted
  cleanup removes only the exact worker's sandbox;
- malicious candidate attempts to alter admission state, mint worker tokens,
  bypass the trusted claim transition, change protected object ownership, or
  use a candidate migration to disable the guard; and
- search-path, temporary-object, function-shadowing, role-membership, and
  ownership attacks against protected database transitions.

### Migration tests

Tests must exercise the real legacy and global code paths together and prove:

- shadow allocation uses measured envelopes while its independent executable
  ceiling remains zero, and no shadow row can become a grant or admission;
- the fleet-wide claim, placement, legacy scale-up, and worker-job submission
  freeze is effective on both pools before either local writer is stopped;
- pool/profile/quota/capacity/lifecycle mutations cannot change the frozen
  migration snapshot; queued destroy or capacity updates apply only after
  activation/rollback, and an emergency envelope reduction aborts to a new
  migration epoch;
- conflicting repeated legacy environment topology, including replacement-node
  drift, blocks migration until it is reconciled into one physical-pool
  generation; no environment copy wins implicitly;
- neutral work cannot move between pools while one executor is global and the
  other is legacy;
- new submissions wait or receive the documented retryable queue-full result
  without becoming claimable;
- activation occurs only after both imported inventories and admission epochs
  match;
- prepared activation, duplicate acknowledgements, and crashes before/after the
  central active commit cannot authorize an old/mismatched epoch or mixed
  writer; a delayed consumer remains frozen;
- legacy jobs without complete manifest evidence remain non-mutable quarantine,
  and no manifest entry can be added past the frozen high-water mark;
- protocol-incompatible legacy workers drain before activation rather than
  retaining a direct claim path;
- legacy-compatibility admission mirrors only exact pre-freeze legacy authority,
  cannot allocate or consume a global allowance, and cannot coexist with or
  survive global admission activation;
- a legacy trial made workload-terminal before its worker acknowledges
  cessation is imported as a charged protected/physical commitment rather than
  omitted by a workload-state filter;
- ambiguous or unsupported legacy task requirements remain unclaimable and
  cannot contribute scale-up demand;
- failed cutover and rollback never leave a legacy and global mutation
  authority active for the same pool;
- rollback installs a fresh protected legacy-rollback admission incarnation
  only after global admission is frozen/transferred; it never revives a
  pre-cutover claim epoch or bypasses an unreachable environment guard;
- a one-pool mixed rollback remains adoption/drain-only for pinned and flexible
  work, and a full legacy rollback enables claims/scale-up only with a verified
  fleet-wide aggregate placement/quota protocol; otherwise it stays no-scale;
  and
- rollback after the legacy decommission checkpoint uses a compatible global
  release or safe no-scale state, never an ad hoc restored legacy writer.

### End-to-end acceptance

A bounded test fleet must demonstrate:

1. one dynamic development environment runs x86-specific work on OLDLAB and
   ARM-specific work on GB10 under the same aggregate capacity policy;
2. multiple dynamic environments deploy and use both physical profiles
   concurrently without shared mutable candidate state;
3. neutral work is assigned once and uses healthy capacity without weights;
4. multiple owners share the development tier fairly;
5. one owner with multiple environments does not gain capacity;
6. aggregate environment and development-tier ceilings hold across both pools;
7. production demand drains borrowed lower-tier capacity without terminating a
   running trial;
8. manager, reporter, and executor outages do not double-allocate;
9. a candidate rollout does not mix trial or worker generations;
10. environment deletion waits for terminal release;
11. foreign and forged Loom-looking Slurm jobs remain untouched;
12. replayed reports and release acknowledgements do not double-allocate;
13. a two-pool migration cannot admit work during mixed authority;
14. one allocation epoch accounts for production, staging, and every dynamic
    development environment across both pools; and
15. a mixed-mode rollback admits no new pinned or flexible work, then either
    activates one verified fleet-wide rollback protocol or remains safe
    no-scale.

## Activation Boundary

Repository implementation is not live activation. Activation requires:

- management PostgreSQL and API availability independent of worker capacity;
- scoped reporter and executor RBAC;
- validated OLDLAB and GB10 controller-local executor installation;
- dedicated Slurm executor identities and protected ownership-signing keys,
  with crash-window adoption evidence;
- protected worker-bootstrap/claim-guard deployment for every participating
  environment and proof that no legacy worker bypass remains;
- a complete mutation-path inventory proving trial submission, assignment,
  claim, cancellation, terminal update, crash reclaim, retry, batch/family
  cancellation, and deletion all preserve protected claim accounting;
- exact candidate artifact and trusted launcher publication for both pools;
- wildcard DNS/TLS and shared fixture readiness;
- PostgreSQL, object-store, namespace, and provisioning concurrency limits;
- isolated-builder sandbox, credential separation, immutable export, and
  per-owner resource-limit evidence;
- measured physical resource envelopes and infrastructure/co-tenant headroom
  for both pools;
- one reviewed fleet-state generation for each pool, with every environment
  profile referencing or validly narrowing it and no conflicting duplicated
  controller/partition/node/envelope authority;
- validated controller-enforced Loom association partition, submit-job,
  running-job, and TRES/QoS bounds on both pools;
- #896 non-exclusive containment evidence;
- compute-node proof that candidate containers cannot reach Munge, Slurm
  controller commands/network, executor bindings, or sibling host processes;
- #896 compose-project/network isolation, trusted orphan cleanup, and real
  mixed-workload soak evidence for both non-exclusive pools;
- zero-new-global-capacity shadow and inventory evidence;
- management- and environment-database restore/recovery exercises plus
  quarantine-reconciliation evidence;
- signed legacy-adoption manifests for all nonterminal pre-cutover jobs;
- proof that every legacy writer is disabled;
- a one-slot architecture-specific and neutral acceptance sequence;
- alerting and operator status visibility; and
- an exercised no-dual-writer rollback procedure.

Issue #906 remains the appropriate carrier for live activation evidence, but
its current “per-environment external autoscaler supervisors” title and
checklist describe the authority this design removes. Before activation, #906
must be re-scoped to the one global manager, two pool-local executors,
fleet-wide migration/rollback fence, and acceptance evidence above; it must not
authorize the obsolete per-environment timer topology. The repository
implementation and activation package remain separate deliverables so code
review cannot be confused with permission to consume live capacity. Issue #896
remains the non-exclusive worker-containment evidence gate.

## Acceptance Criteria

The design is implemented when all of the following are true:

1. One fenced manager is the only allocation writer for production, staging,
   and dynamic development.
2. Exactly one executor per physical pool applies grants locally.
3. Dynamic development has physical OLDLAB and GB10 policies and no synthetic
   pool allocation authority; `loom-dev` is infrastructure, not a capacity
   subject.
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
12. Untrusted candidate code cannot change the trusted Slurm/container launch,
    protected database, bootstrap, or claim boundaries.
13. Only jobs with complete authenticated executor ownership proof or the
    one-time frozen legacy-adoption proof, plus exact Slurm mutation authority,
    can be adopted for mutation, signalled, or cancelled.
14. Reports, reservation acceptance, partial release, and submission recovery
    are monotonic and replay-idempotent.
15. Candidate generations become current only after complete trusted readiness
    and an epoch-bound, crash-replay-safe two-phase cutover.
16. Failure and restore paths preserve commitments and reject stale authority.
17. Migration proves that no legacy and global writer or cross-pool placer
    overlaps across the fleet-wide freeze.
18. `loom service up` requires an explicit environment and applies
    source-fresh personal-development or protected static-environment candidate
    rules without shared mutable deployment state.
19. New global-manager proposals and scale-up remain zero until the separate
    activation gate approves them; pre-existing legacy commitments remain
    charged and are imported rather than hidden.
20. Capacity-relevant task requirements are sealed in protected state, and all
    placement allowances have one joint matching to distinct eligible accepted
    shape slots.
21. Versioned central launch permits and durable rate buckets enforce manager
    launch order and global/account/subject/pool submission limits across both
    executors.
22. Rollout surge is exact replacement capacity, requires the paired old-worker
    drain acknowledgement, never raises claim concurrency or fair share, and
    converges to `max_slots`.
23. Environment deletion fences central launch before local teardown, preserves
    every live protected claim through terminal release, and retains a durable
    identity tombstone.
24. User-visible workload terminality, protected-claim terminality, and
    physical terminality are separate; every capacity-relevant transition path
    uses the trusted protected interface and no cancellation or retry path can
    release capacity early.
25. Each physical pool has one immutable fleet-state topology and envelope;
    environment profiles may only reference or narrow it, and conflicting
    legacy environment copies block even shadow allocation until reconciled.
26. `loom-dev` is the only persistent shared development namespace;
    `loom-dev-shared` is absent, every personal runtime is isolated in
    `loom-dev-<name>`, and personal deletion cannot mutate shared
    infrastructure.
27. Arbitrary personal source is deployed only after the exact sealed content
    and produced artifacts receive a `personal-dev-only` safety attestation;
    no base-commit CI result or personal attestation can authorize staging or
    production.
