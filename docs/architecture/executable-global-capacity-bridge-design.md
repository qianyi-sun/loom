# Executable Global Capacity Bridge Design

Status: approved for implementation under the delegated autonomous-development mandate
Package: 5B
Date: 2026-08-12
Live activation: prohibited by this package

## Decision

Loom will promote the existing global fleet capacity manager and its fenced
reservation protocol into the only executable allocation path for production,
staging, shared development, and every personal development subject. One
credential-scoped executor on each Slurm controller will apply the manager's
decisions. Environment-local autoscalers and the legacy global-development
supervisor will not remain as allocation authorities after cutover.

This package implements and verifies that executable path while every rendered
and deployed configuration retains an executable-new-capacity ceiling of zero.
It does not perform the live writer cutover or submit a live worker. Those
actions remain gated by the migration and activation evidence in issue #906.

This design is a focused executable continuation of
`archive/docs/architecture/global-fleet-capacity-manager-design.md`. The
archived umbrella design remains authoritative for allocation policy,
protected claim semantics, drain-first reclamation, personal candidate
isolation, and the final activation boundary. This document defines the
concrete bridge from the inert Package 5A implementation to that target.

## Why this approach

Three approaches were considered.

1. **Promote the global manager protocol.** Add separately versioned
   executable contracts and real pool-local executor services while preserving
   the existing epochs, reservations, ownership proofs, journals, and release
   fences. This is the selected approach because it creates one allocation
   authority without translating identities or weakening the dry-run proof.
2. **Adapt through the legacy global-development supervisor.** This would
   translate 64-character personal source identities into a 40-character
   Git-only lease model and retain a second allocator. It is rejected because
   the translation is not identity preserving and cannot prove a no-dual-writer
   cutover.
3. **Let each environment submit to a shared autoscaler directly.** This is
   rejected because independently computed submissions cannot provide one
   fleet allocation epoch, stable neutral placement, global fairness, or exact
   capacity accounting across both controllers.

## Required outcome

After this package, the repository must contain an executable but inert path
with these properties:

- one manager allocation covers all registered environment subjects and both
  physical pools;
- OLDLAB and GB10 each have exactly one pool-local executor implementation and
  service entry point;
- executable reservations and permits can drive a real Slurm backend in tests
  and controlled offline harnesses;
- worker launch, inventory, pending cancellation, drain, and terminal release
  preserve exact subject, candidate, deployment, profile, resource, pool, and
  authority bindings;
- personal source SHA-256 identities and protected Git commit identities are
  represented without conversion;
- architecture-specific demand constrains pool eligibility and
  architecture-neutral demand is placed by the manager;
- users configure aggregate subject limits, including `min_slots` defaulting
  to zero, but never pool weights, Slurm QoS, worker shapes, or priority tiers;
- loss, staleness, ambiguity, or contradiction fails closed and keeps capacity
  charged; and
- checked-in deployment state cannot raise the executable ceiling or mutate a
  live scheduler.

Completion of this package is necessary but not sufficient for a fully
operational multi-person development environment. Writer closure, adoption,
deployment rehearsal, bounded live activation, and multi-user acceptance
remain subsequent gates.

## Authority topology

```text
production   staging   shared-development   dev-<owner> ...
     \          |              |                 /
       trusted environment capacity agents
                       |
       authenticated demand and lifecycle evidence
                       |
                       v
          global manager in namespace loom-dev
      one PostgreSQL-fenced allocation authority
                 /                   \
      executable pool work      executable pool work
               /                       \
      OLDLAB executor             GB10 executor
      OLDLAB controller           GB10 controller
               \                       /
               protected worker bindings
                         |
             exact environment task admission
```

The manager is the allocation writer. It decides which subject receives which
approved shape on which eligible pool and in which order. A pool executor is a
scheduler mutation writer only for already-authorized operations in its exact
pool; it cannot allocate, reprioritize, change a shape, transfer a grant, or
invent demand. An environment agent is the admission writer for its protected
application database; it cannot allocate global capacity or call Slurm.

`loom-dev` is the shared infrastructure namespace that hosts the manager and
personal lifecycle; it is not the logical shared-development demand subject.
Personal application namespaces are `loom-dev-<owner>`, and no
`loom-dev-shared` namespace exists.

One process does not operate both Slurm controllers. This is a credential and
failure-domain boundary, not a reason to introduce two managers. Both
executors consume decisions from the same globally fenced allocation epoch.

## Contract versioning and identity

The current `DryRun*V1` contracts remain permanently non-executable. Their
`executable: Literal[False]` fields must not be widened to Boolean fields and no
adapter may reinterpret them as scheduler permission.

Executable operations use a new protocol version and distinct model names.
Shared nested value objects may be reused only when they have no execution
semantics. Every executable top-level contract includes:

- `executable: Literal[True]` and the executable protocol version;
- authority incarnation and writer epoch;
- configuration, allocation, and activation epochs;
- immutable activation-manifest digest;
- pool, pool generation, executor, and executor-incarnation identities;
- subject UUID, subject lifecycle incarnation, account, and priority tier;
- deployment, candidate, profile, and trusted-fleet-release bindings;
- exact resource vector, worker shape, nodes or resource domains, and
  concurrency slots;
- stable tranche, intent, permit, and idempotency identities as applicable;
- manager-database issue and expiry timestamps where authorization expires;
  and
- a canonical contract digest.

Candidate identity is a tagged value, not an inferred string:

- protected releases use `git-sha1` plus the full 40-character commit and
  their CI/release publication digest;
- personal releases use `source-sha256` plus the full 64-character immutable
  source digest and candidate-publication digest.

The tagged identity is carried from lifecycle projection through demand,
allocation, admission, bootstrap, ownership metadata, worker registration,
inventory, and release. Neither form is padded, truncated, rehashed, or
translated into the other. The trusted manager, agent, launcher, and executor
release is bound separately from the user candidate so deploying arbitrary
personal source never grants that source fleet-control authority.

## Activation boundary

Executable capability has three independent gates:

1. **Protocol capability:** code understands executable contracts and a real
   scheduler backend.
2. **Prepared authority:** a reviewed activation record binds the immutable
   fleet generation, all environment acknowledgements, both executor
   incarnations, the complete legacy-writer manifest, and rollback evidence.
3. **Bounded execution:** an active activation epoch has an operator-pinned,
   finite nonzero executable-new-capacity ceiling and rate envelope. The
   ceiling cannot exceed the sum of the exact configured OLDLAB and GB10 pool
   slot ceilings bound to that epoch.

Protocol capability alone authorizes nothing. The manager database rejects a
nonzero ceiling unless the activation record is active and all of its exact
bindings are current. Any configuration, candidate, executor, controller, or
owner-policy binding change requires the current epoch to drain and retire at
ceiling zero before a new epoch can be prepared. The first live activation
ceiling remains one slot; offline executable integration may use a larger
finite fixture ceiling, and every live expansion requires a later reviewed
epoch. The ceiling is operator-owned activation policy, not a subject, owner,
or candidate setting.

Package 5B adds the schema and validation needed to represent these states but
ships only the bootstrap state: no activation record, allocation epoch zero
for executable admission, and ceiling zero. Renderers, example configuration,
tests, and service installers must reject a command-line or environment
override that attempts to bypass this state.

### Zero-ceiling preparation boundary

Package 5C1 makes only the intermediate prepared state operational. The
manager may load one current-UID-owned, bounded, immutable and digest-pinned
`ExecutionPreparationPolicyV2`, accept an exactly matching preparation, and
return an effective ceiling and rate of zero. A separately scoped exact abort
append-only retires that preparation and restores shadow at zero. Both are
management-database mutations reserved for issue #906's operator window.

The production HTTP surface added for this boundary is deliberately closed:

- unbound `capacity:execution:prepare` may call `POST
  /v2/execution-preparations` with an idempotency UUID;
- each exact pool-bound `capacity:execute:pool` identity may call `PUT
  /v2/executors/{pool_id}/registration` only for itself;
- `capacity:read` may call `GET /v2/status/execution-preparation`; and
- a distinct unbound `capacity:execution:abort` identity may call `POST
  /v2/execution-preparations/{execution_epoch}/abort` for the exact prepared
  fence.

There is no production activation, generic transition, ceiling-change, apply,
drain, or retirement route in this package. Preparation credentials therefore
cannot be repurposed as activation authority.

The two controller-local prepared services register themselves, maintain their
leases, and publish complete journal-first read-only Slurm inventories. They
accept only the exact prepared manager context and never construct the
submission/cancellation backend. Manager readiness is derived from locked
database state and database time; it requires the complete subject set, exact
GB10 and OLDLAB bindings, fresh post-inventory heartbeats, confirmed journals,
and no foreign, unknown, ownership-missing, or quarantined record. Readiness is
an observation, never a state transition.

The canonical operating order is: retain live environment-local authority
until the #906 window; collect and pin complete legacy-freeze and rollback
evidence; render and shadow-deploy the manager at zero; prepare the exact epoch;
render and start each prepared-only controller timer; wait for prepared
readiness; stop both timers; and either retain the frozen prepared evidence or
abort exactly back to shadow at zero. No activation step is part of that
sequence.

The personal lifecycle's current assertion that the manager ceiling is zero
is replaced only by an activation-aware response validator: normal lifecycle
operations accept zero or a valid active epoch, but never infer that application
readiness means capacity readiness. Status reports the two conditions
separately.

### Active inputs and authority turnover

An active execution epoch freezes configuration and identity, not facts. Exact
monotonic demand snapshots and pool observations may continue while the
authority is active because they retain the already-bound subject, reporter,
candidate, deployment, pool, and configuration generations. Prepared and
drain-only epochs reject those inputs. Configuration proposals and
configuration activation, personal projection, candidate redeploy, subject
deletion, reporter replacement, and pool/executor rebinding are accepted only
in shadow state, even though prepared and drain-only also have ceiling zero.

An exact operator drain request compare-and-sets authority incarnation, writer
epoch, execution epoch, and manifest. It changes `active` to `drain-only`,
sets both effective ceiling and rate to zero, records durable idempotent drain
evidence, and preserves the incumbent writer. Writer replacement remains a
separate fail-closed path: it advances the writer fence and also forces an
active epoch to drain-only. Both paths reject all later increases while
allowing only monotonic close, protected drain, inventory, terminal
observation, and release work for retained commitments.

Retirement is a second explicit compare-and-set. It is permitted only when:

- the authority and epoch are still the exact drain-only writer and manifest;
- both pool executors are current and provide fresh, complete inventories plus
  exact heartbeat, command, journal, and inventory high-water evidence;
- every executable intent in the epoch is released;
- no quarantine, unresolved retained commitment, or nonterminal/ambiguous
  Loom-scoped physical record remains; and
- any record classified foreign remains foreign and is neither adopted nor
  mutated.

The retirement transaction locks and revalidates all of that evidence, records
the operator request and final per-pool checkpoints, marks the epoch retired,
and returns the global authority to shadow with ceiling and rate zero. The
increase freeze stays set until a later exact preparation and activation.
Failure or ambiguity leaves the epoch drain-only, its commitments unavailable
for reuse, and its physical work untouched. Only after retirement may a
personal redeploy, deletion, configuration change, or new execution epoch
proceed.

## Executable state machine

### Scale up

1. Environment agents publish complete, monotonic demand reports with current
   candidate and lifecycle bindings.
2. The manager computes and commits one deterministic allocation epoch across
   the complete fresh cohort. It reserves headroom before exposing work.
3. The executor fetches the next manager-authored proposal for its pool,
   verifies all fences locally, fsyncs the request, and atomically accepts it.
4. Acceptance creates one stable submission intent per exact worker shape.
5. The executor creates a one-time bootstrap capability, sends only its digest
   through the protected environment-agent transaction, and journals the
   prepared binding. The clear capability is never stored by the manager or
   candidate.
6. After the agent acknowledges prepared admission, the manager may issue the
   next ordered launch permit. Permit issue rechecks fresh inventory, headroom,
   activation ceiling, rate limits, subject lifecycle, candidate, profile,
   executor lease, and writer epoch using management-database time.
7. Permit consumption atomically moves the intent to `submitting-unknown` and
   consumes its rate token before any scheduler call. The executor fsyncs that
   state and only then invokes Slurm.
8. The executor records the returned physical job identity centrally and with
   the agent. The trusted bootstrap wrapper exchanges the one-time capability
   only after that exact binding exists and registers one exact worker.
9. Complete executor inventory advances the intent through bound, observed,
   and eventually terminal states.

The manager exposes pool-scoped, bounded work-queue APIs rather than requiring
operators to construct contracts or copy grant files. Work selection is
ordered centrally; an executor may accept only the current next operation for
its bound pool and command high-water.

### Ambiguous submission

The stable intent enters `submitting-unknown` before `sbatch`. Any timeout,
transport error, executor crash, or malformed scheduler response after that
transition is treated as possibly submitted. Recovery scans the dedicated
Slurm association for the signed stable operation identity:

- exactly one matching job is adopted and bound;
- no matching job remains quarantined until controller and accounting
  high-water evidence proves absence;
- multiple or conflicting jobs remain quarantined and charged; and
- the intent is never submitted again.

### Scale down and deletion

The manager first lowers the desired exact shape multiset and placement
allowances. The environment agent then advances its admission epoch in one
protected transaction, clears unclaimed assignments, and marks excess whole
workers draining. Only after that acknowledgement may the executor cancel a
conclusively owned job that is still pending.

Active workers are not killed for normal scale-down, priority reclamation,
rollout, or environment deletion. They stop receiving new claims and exit
after protected claims become terminal. Capacity remains charged until the
executor provides authoritative physical terminal evidence and the environment
agent publishes the matching protected release fence. Missing `squeue` output,
workload terminal state, lease expiry, or a deletion request is not release
evidence.

## Pool executor design

`loom_capacity_executor` gains a daemon-oriented executable layer with four
separate responsibilities:

- **Protocol driver:** fetches pool work, validates checkpoints and epochs,
  performs journal-first manager transitions, and reports complete inventory.
- **Scheduler backend:** observes, submits, and conditionally cancels Slurm
  jobs through structured typed requests. It has no allocation logic.
- **Trusted launch renderer:** converts one approved profile and shape into an
  argv-only Slurm request and trusted wrapper binding. Candidate strings,
  environment names, and task data never become shell fragments.
- **Recovery reconciler:** classifies every in-scope scheduler job as exact,
  foreign, ambiguous, terminal, or quarantined using controller observation,
  dedicated association identity, signed ownership metadata, and the local
  journal.

The OLDLAB and GB10 services use the same implementation with different
immutable pool manifests, controller fingerprints, credentials, keys, and
journal directories. Startup validates the exact controller, Slurm cluster,
partition, association, TRES/QoS envelope, executable paths, local UID, key
ownership, journal permissions, and singleton lock before registration.

The backend invokes subprocesses without a shell, under finite output and time
bounds. Controller-normalized resource requests must round-trip to the exact
approved resource vector. Unsupported conditional cancellation fails safe; an
apparently pending job that starts is admitted only as a draining worker and
is never killed by the pending-cancel path.

Every mutating command re-reads the central checkpoint and the physical job.
An expired lease forbids acceptance and scale-up. The still-locally-locked
incumbent may perform only explicitly defined monotonic drain actions during a
manager outage; a fenced or replaced incarnation cannot mutate at all.

The executable HTTP client carries the same exact v2 heartbeat contract as the
store route. Initial runtime-state creation uses the canonical zero central
journal checkpoint; every subsequent heartbeat names the last authenticated
central checkpoint and the current fsynced local journal head. A changed
receipt, sequence gap, regression, or equivocation fences the executor. Final
retirement evidence requires a heartbeat after the last complete inventory, so
an operator cannot retire against an executor journal that is merely assumed
current.

## Ownership and foreign-workload protection

Names and visible comments are diagnostic data, not ownership proof. Automatic
adoption, cancellation, or release requires all of the following:

- the dedicated executor submitter and approved Slurm association match;
- the stable intent exists in the current or retained central ledger;
- canonical ownership metadata verifies with a registered controller-local
  Ed25519 key;
- pool, subject, candidate, deployment, epochs, shape, resources, submit time,
  controller, and executor journal all agree; and
- immediately before mutation, the current scheduler record still agrees.

Unrelated jobs are foreign and untouched. An in-scope job with incomplete or
contradictory evidence is charged conservatively but treated as foreign for
mutation. Key rotation retains old verification keys until all corresponding
commitments are conclusively terminal.

## Environment-agent boundary

Every environment runs a trusted capacity agent installed from the fleet
release, outside candidate control. It owns only the protected admission schema
in that environment's application database. Candidate roles may propose work
through existing application interfaces but cannot mint grants, update
allowances, register workers, reopen drained identities, acknowledge release,
or bypass protected claims.

The manager stores binding digests, not environment database credentials or
bootstrap secrets. Controller-local executors receive owner-only,
environment-scoped connection material from the trusted lifecycle binding
channel. Dynamic personal lifecycle changes create or retire those bindings
atomically with the subject incarnation; stale credentials cannot attach to a
replacement environment with the same display name.

Package 5B implements the executable admission and bootstrap interfaces and
their cross-component tests. The later protected-writer-closure package audits
and fences every existing application mutation path before activation.

## Legacy compatibility and retirement

The legacy global-development supervisor and environment-local autoscalers may
remain live only while the global executable ceiling is zero. They do not
consume executable v2 contracts and cannot be used as executor adapters.

Both old and new paths gain reciprocal fail-closed coexistence checks:

- a legacy scale-up tick refuses to run when it observes any prepared or active
  global activation epoch for its pool; and
- the global manager refuses a nonzero ceiling without a signed manifest that
  captures and fences every legacy allocation, submission, claim, pressure,
  cancellation, and release writer at exact high-water marks.

Actual inventory adoption, timer shutdown, lock acquisition, and cutover are
not performed by Package 5B. They are the next migration package. The legacy
SQLite lease ledger is retained read-only through the rollback window and then
archived; it is never imported as a competing source of new grants.

## Deployment shape

The manager remains in the shared `loom-dev` infrastructure namespace and uses
management PostgreSQL that does not depend on worker capacity. Personal
applications remain in `loom-dev-<owner>` namespaces. There is no
`loom-dev-shared` namespace.

Each controller receives an owner-only systemd service, state directory,
journal, mTLS identity, bearer principal, Ed25519 ownership key, pool manifest,
and environment-binding directory. Installation and validation are separate
from service activation. Repository manifests and installers render inert
services with no activation epoch and ceiling zero.

For the zero-ceiling rehearsal, the controller also receives an independently
digest-pinned read-only inventory policy. Executor config, inventory policy,
service environment, bearer/TLS/ownership material, state, and journal paths
remain pool-local. Regular inputs are service-UID-owned mode `0600`; state and
journal directories are mode `0700`. The prepared oneshot has no install
target. Only its nonpersistent, nonoverlapping timer can be enabled, and only
inside #906's window. The manager's non-secret owner policy is rendered as an
immutable digest-addressed ConfigMap, copied by an init container into a
manager-UID-owned mode-`0600` memory-backed file, and mounted into the manager
read-only.

No secret, mutable tag, controller-local artifact, personal candidate source,
or untracked file is committed. Trusted fleet images use exact verified
digests. Personal candidates may contain arbitrary committed, modified,
deleted, and permitted untracked source, but remain owner-scoped,
personal-development-only, and non-promotable.

## Failure semantics

- **Manager or management database unavailable:** no new proposal, permit, or
  claim admission; existing work remains charged and may drain safely.
- **One executor or controller unavailable:** only that pool is ineligible for
  increases; its unresolved commitments remain charged. The other pool may
  continue within the same manager epoch and global ceilings.
- **Environment agent or database unavailable:** that subject receives no new
  launch or claim allowance; releases await protected evidence.
- **Stale or equivocal demand:** exclude the subject from increases without
  freeing existing commitments.
- **Journal missing, corrupt, permission-broadened, or behind central state:**
  fence the executor incarnation; never create an empty replacement journal.
- **Inventory incomplete or contradictory:** quarantine and conservatively
  charge capacity; do not mutate ambiguous jobs.
- **Candidate, deployment, profile, fleet, or activation mismatch:** reject
  the operation before external side effects.
- **Ceiling reduction:** stop increases immediately and converge drain-first;
  never reinterpret an expired authorization as physical release.
- **Drain or retirement evidence incomplete:** remain drain-only with ceiling
  and rate zero; do not return to shadow or make any retained slot reusable.
- **Active fact update changes an immutable binding:** reject it as stale or
  equivocal and freeze increases; never silently roll the execution manifest.

## Observability

Status and metrics expose manager health independently from worker health and
include authority/writer state, activation epoch, executable ceiling,
configuration and allocation epochs, fleet generation, per-pool executor lease
and inventory freshness, pending intents, quarantines, rate limits, committed
resources, desired resources, drain blockers, and legacy-writer fence state.

Application readiness, candidate readiness, capacity preparedness, and live
worker availability are distinct status fields. A personal deployment cannot
report capacity-ready merely because its application pods are ready.

Metrics use bounded operator-owned labels such as pool, tier, and reason.
Subject IDs, owner names, dynamic environment names, candidate identities,
Slurm job IDs, and unbounded error strings remain in access-controlled status
or audit records rather than metric labels.

## Verification

Implementation is accepted only when all of the following pass without live
Slurm mutation:

1. Contract tests prove dry-run v1 can never become executable and executable
   v2 cannot omit or alter an authority, activation, candidate, pool, profile,
   resource, executor, or idempotency binding.
2. Migration tests prove existing Package 5A databases upgrade
   deterministically, reject a nonzero unprepared or fleet-exceeding ceiling,
   enforce the explicit drain/retirement transitions, and restore with
   monotonic authority and journal high-water marks.
3. Allocator tests cover multiple owners, all priority tiers, both pools,
   minima defaulting to zero, aggregate maxima, architecture-specific demand,
   neutral placement, stable assignments, headroom, rates, and scale-to-zero.
4. Fake-controller tests exercise exact submission, conditional pending
   cancellation, inventory, resource round-trip, foreign jobs, ownership
   forgery, key rotation, and terminal accounting independently for OLDLAB and
   GB10.
5. Crash tests cover every boundary before and after proposal acceptance,
   bootstrap preparation, permit consumption, `sbatch`, physical binding,
   admission, drain, cancellation, and release.
6. Two-controller integration tests run one manager, two executors, protected
   agents, production/staging subjects, and at least two personal owners. They
   prove global fairness, pinned x86, pinned ARM, neutral placement, no double
   allocation, active demand/pool fact updates, rollout fencing, deletion,
   drain/retire/reprepare lifecycle, and full scale-to-zero.
7. Coexistence tests prove legacy writers refuse a prepared global epoch and
   the global manager refuses execution without the complete legacy fence
   manifest.
8. Render and installer tests prove manager and executor services remain
   inert, secrets are referenced rather than embedded, namespace topology is
   exact, and no configuration override can raise the ceiling.
9. Repository checks, type checking, security checks, image gates, cluster
   smoke, and staging smoke pass on the exact PR commit.

Tests may use deterministic fake Slurm processes and isolated PostgreSQL
containers. A command that can reach a live controller must default to
`--validate-only` and require a separate future operator activation artifact
before it exposes scheduler mutation methods.

## Subsequent gates to the final objective

Package 5B's zero-ceiling preparation follow-up is implemented by Package 5C1.
After that repository package is merged and exact-head CI is green, the live
gates remain, in order:

1. protected mutation-path closure plus authenticated legacy freeze/adoption
   evidence and signed restore/rollback rehearsal;
2. an explicit #906 window for the zero-ceiling manager, both prepared-only
   executor inventories, every environment acknowledgement, and abort/restore
   rehearsal;
3. a separately reviewed activation interlock and operator surface that
   consumes the passing prepared-readiness checkpoint;
4. an explicit operator window with a one-slot x86, ARM, and neutral sequence;
5. bounded expansion and mixed-workload soak; and
6. live concurrent acceptance in which at least two owners deploy different
   arbitrary local sources, execute real work fairly on shared OLDLAB/GB10
   capacity, redeploy safely, scale to zero, tear down, and pass artifact
   garbage collection without cross-owner leakage.

No repository merge authorizes any of those live operations.
