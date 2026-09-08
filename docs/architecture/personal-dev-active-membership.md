# Personal development membership under active capacity authority

Status: design for implementation; not live acceptance evidence.

## Outcome

A new owner can build and deploy while existing owners and staging continue.
Personal source may be committed, dirty or untracked. Shared releases remain
exact CI-approved publications. All physical work is charged to the existing
global OLDLAB/GB10 allocator and the owner's existing capacity account.

This follows `personal-dev-dynamic-build-placement.md`. The placement contract
does not grant execution authority. The current shadow-only development
projection cannot be enabled under active execution simply by removing its guard.

## Decisions

Keep the immutable base configuration and execution manifest. Prepare an explicit
successor policy that delegates a bounded personal-membership namespace to an
exact management principal. Record subsequent membership changes in an append-only
execution-scoped log, and seal its current revision into allocation input.

This is preferred to draining the entire fleet for every new owner, and to a
general active-to-active execution-epoch transition. Both alternatives change
more authority than an ordinary personal deployment needs. Old V2 epochs remain
ineligible for delegated admission, even if a newer manager binary is installed.

Use a genuine management build-worker service per owner, not a capacity subject
per source attempt. Its candidate is the real CI-published builder-runtime release;
its deployment generation identifies that service's runtime rollout. The private
source candidate, attempt and lease remain separate grant identities. They are
not an application deployment or a task/trial. One build attempt uses one cold
one-slot allocation initially; `min_slots=0`, with no warm-worker feature.

Both the application and build service charge `dev-owner-<owner UUID hex>`.
They must not receive independent service accounts that multiply the owner's
share. Build purpose is explicit and cannot confer task-worker credentials.

## Stable authority and delegated membership

An `ExecutionPreparationPolicyV3` and `ExecutionPreparationV3` retain all V2
executor, controller, rollback and legacy-writer evidence and add an exact
delegation policy. V2 canonical documents and parsers retain their meaning.
The execution fence can continue identifying the immutable manifest by digest;
old code must reject a V3 manifest, never silently parse away its delegation.

The delegation binds a nonzero namespace UUID, an exact unbound management
principal, the fleet development-template digest, a bounded subject count and
the exact pre-existing personal subject IDs that management may update.
Preparation verifies those IDs against management-owned personal projection
evidence; no staging, production or arbitrary static service can be delegated.
New subjects use the same protected personal-name and owner derivation rules.
Changing the delegate, template, fleet or executor trust roots requires a new
reviewed execution preparation, not a membership operation.

The log binds authority incarnation, writer/execution epoch, manifest digest,
namespace, global membership revision, previous digest, actor, idempotency key,
request digest, subject identity and complete resulting subject evidence.
Mutations serialize with the authority lock and compare the expected revision.
An exact replay returns its original result; reusing an operation/key with a
different actor, payload or identity is rejected. Revisions are monotonic and
entries cannot be updated, deleted or truncated through the runtime role.

Current subject/account rows are materialized projections of that log. Base
configuration rows and base subject-generation references remain unchanged.
Only delegated identities may have materialized successor generations. A reader
must verify materialized payloads against the latest log entry before using
them, including purpose, owner, candidate, reporter, profiles and limits.

Allocation input carries the base configuration plus a versioned membership
snapshot, not a rewritten document masquerading as the original configuration.
The allocator merges only authorized membership references, verifies exact
subject completeness and derives owner accounts from the unchanged fleet
template. Canonical input hashing and the existing SERIALIZABLE re-read before
commit cover membership changes. Static input/output canonical bytes remain
unchanged. A membership operation grants no CPU, memory, slot or launch permit.

## Application lifecycle

Creation requires the existing verified source publication, local activation,
protected admission and capacity-agent installation evidence, both physical
pool profiles and a fresh reporter identity. Management must authenticate the
protected acknowledgement to the current execution manifest before submission.
The manager cross-checks every acknowledgement field against the projection.
The request cannot select its account, tier, profiles or submission rate.

Update retains owner, namespace and subject identity; increases deployment and
configuration generations; rotates reporter credentials; and records the real
new publication and protected acknowledgement. Capacity-only changes preserve
candidate/deployment/reporter evidence. Owner aggregate minimum and maximum
subject limits apply across applications and build services.

Destroy appends a disabled generation and closes new admission immediately.
It does not erase the subject/account or any physical commitment. Existing
unknown, pending, running and terminal-but-unreleased intents stay charged until
authenticated cleanup. Old protected plans cannot submit or publish after
supersession. Recreating a deleted environment retains its subject ID, owner and
name but creates a fresh subject incarnation and reporter; deployment/candidate
generation restarts at 1 and configuration/operation generation keeps increasing.
This preserves the existing lifecycle ABA fence, not an in-place reactivation.
Other subjects retain their fences and continue serving work.

Recreation is a distinct manager-verified transition. The immediate predecessor
must be a disabled zero-capacity membership entry. Under the common authority
lock and SERIALIZABLE transaction, every intent for that predecessor identity
across epochs must be released through the existing authenticated release path;
every legacy reservation must also be released. Pending, proposed, unknown,
quarantined and terminal-but-unreleased work blocks recreation. The membership
writer never changes an intent to released itself. An empty release set is valid;
zero capacity alone is not release evidence. Current-generation checks fence
concurrent stale allocation commits and new admission after disable.

The immutable recreation event records its origin subject reference, exact
disabled predecessor reference/revision/head, successor incarnation and a digest
of the canonically ordered released-intent/reservation identities and durable
release witnesses. The store generates this evidence, never accepts a caller's
claimed release certificate. Membership materialization carries the evidence
through later updates of the successor. The pure resolver checks the evidence's
namespace/manifest/origin/predecessor/successor bindings; the store validates the
actual event chain and release witnesses when producing allocation input. A
typed digest by itself is not authentication. Repeated recreations preserve the
first origin and extend the immutable predecessor chain. Historical exact
identities remain available for status and cleanup; they cannot reopen admission.

Implementation is staged: durable ordinary membership first rejects every
incarnation change; the separately reviewed recreation task adds only this
explicit transition before the executable membership interface is delivered.

Protected acknowledgement lookup must use the exact admission generation:
current generation for increases, stored generation for authenticated cleanup.
It must not fall back to an old prepared acknowledgement when a delegated
subject has been disabled or superseded. Reporter authentication, pool work,
bootstrap, launch consumption, pending limits and retirement all use the same
resolved membership authority.

## Build service and runtime connection

The common membership mechanism will admit a typed `personal-build-worker`
service from an operator-pinned builder-runtime release. Its GB10-only profile
is a purpose-specific rule, not a reason to mislabel the work as staging.
Application admission remains dual-pool. The build-purpose executor path must
be implemented before that purpose is accepted for executable admission.

Management translates source attempts into build-capability demand buckets.
The existing global allocator and charged intent ledger handle placement and
fairness. The pool executor routes build intents to a management admission
adapter, not environment trial procedures. Builder launch profiles must match
the real CI publication, not merely contain an unrelated publication digest.

Submit a Slurm job held, durably bind it, then release under the exact current
fence. Reconcile ambiguous submissions before retrying. The allocation grant
binds owner, service subject, intent/slot, source candidate, attempt/lease,
Slurm job, node/boot and certified runtime. Build credentials can address only
that management build attempt; existing task-worker credential exchange rejects
them. Untrusted source containers never receive management credentials.

All runtime children descend from the actual Slurm job cgroup. Preserve the
KVM-gVisor separation, private state and capability-client boundary. Select
certified nodes from `trt-gb10-3` through `trt-gb10-15`; exclude nodes 1 and 2.
Cancellation closes source/publication capabilities for the exact lease and
retains its charge until cleanup is proved. Reboot or stale certification fences
the grant. Preserve OLDLAB's native build path and node 2's task-image reservation.

## Implementation boundaries and acceptance

1. Deliver executable delegated **application** membership: versioned policy,
   durable log/projection, authenticated lifecycle endpoint, common allocation
   and executor integration. Existing V2 active-mutation rejection remains.
   This independently closes new-owner application admission; it is not full
   build-provider readiness. Build purpose stays rejected until its bridge exists.
2. Connect typed per-owner build services, management grants, held submissions
   and allocation-contained runtime. Preserve the two distinct candidate identities.
3. Deploy reviewed, CI-approved trusted releases through a bounded adoption
   window. Old epochs do not opt in in place. Keep old runtime files intact for
   rollback; do not claim manager restart transparently preserves active authority.
4. Verify concurrently: existing staging/A work, previously unknown B admission,
   both owners' feature builds, actual app deployment, independent update,
   cancellation/retry, teardown and redeploy. Test stale-seal and ambiguous-job
   failures, isolation and retained charging; then observe build scale-to-zero.
5. Run real architecture-specific and architecture-neutral task acceptance on
   OLDLAB/GB10 and verify global scale-to-zero. No contract/unit-only result is
   sufficient to declare the multi-person environment operational.

Adoption/retirement must retain the membership history. Before a subsequent
shadow preparation, management explicitly projects any surviving deployments
into the new base configuration; it must not infer them from stale active rows.
Downgrade while delegated execution or unreleased work exists is rejected.
