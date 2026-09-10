# Personal development membership under active capacity authority

Status: manager-side application membership and durable lifecycle implemented;
service connection and reviewed successor recovery under validation.
Capacity-accounted builds, incarnation storage, operational enablement and live
acceptance remain incomplete.

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
every accepted legacy reservation must also be released. A legacy proposal that
was never accepted may instead have its existing authenticated expired/superseded
closure, with no remaining shapes or submission intents; this is not a physical
release and must be recorded as a distinct witness. Any outstanding attributed
observed commitment for the predecessor also blocks recreation until the existing
inventory/claim reconciliation clears it. Pending, proposed, unknown,
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

The manager implements only this explicit recreation transition; an ordinary
update cannot change incarnation or reactivate a disabled predecessor.

Protected acknowledgement lookup must use the exact admission generation:
current generation for increases, stored generation for authenticated cleanup.
It must not fall back to an old prepared acknowledgement when a delegated
subject has been disabled or superseded. Reporter authentication, pool work,
bootstrap, launch consumption, pending limits and retirement all use the same
resolved membership authority.

## Delivered manager interface

`POST /v3/execution-preparations` explicitly prepares delegated authority; the
V2 endpoint and its static manifest retain their original contract. Membership
management has a separate single-purpose, unbound `capacity:membership:manage`
principal whose identity must match the prepared delegation.

The protected operator preserves the explicit V2 or V3 policy through immutable
prerequisite artifact readback, preparation request construction and prepared
manifest verification. It selects the matching preparation endpoint using the
validated wire version; it does not infer delegation from an installed manager
binary or silently discard membership fields. A changed delegation changes both
the artifact identity and the prepared manifest. Unknown versions and a V3 policy
relabeled as V2 are rejected. This transport support does not opt the existing
rollout source into V3 or supply installed-generation activation, real legacy
writer freezes, lifecycle wiring, or live acceptance evidence.

`GET /v1/personal-memberships/checkpoint` returns a consistent active execution,
namespace and membership revision/head. `PUT /v1/personal-memberships/{subject_id}`
uses that checkpoint for compare-and-swap; an exact replay returns its original
checkpoint. Only a stale revision returns the typed `membership_revision_conflict`
code. Identity, authority and idempotency failures are not refresh instructions.

Committed `ExecutableEpochV3` allocations retain the exact authenticated membership
snapshot alongside the immutable base configuration. Executor increases check the
target's pinned configuration against current evidence. A change to B supersedes
B's old admission even without another allocation, but a membership revision for
B alone does not supersede A. Old permits cannot launch after target supersession.
Historical bootstrap, admission closure and protected release resolve the exact
original generation; outstanding work stays charged until authenticated release.

Personal agents authenticate through retained hashed reporter records on only the
protected subject routes. No static registry rewrite or management/executor scope
is granted. A legitimately rotated reporter can finish its exact old cleanup,
but cannot admit new work. After retirement, archive-only authentication is limited
to retained admission closure polling and acknowledgement, including exact retries;
it cannot return a new proposal. Demand authentication remains current-only.

Migrated PostgreSQL and HTTP tests exercise this manager boundary. They do not
establish a live rollout, personal lifecycle convergence or build-provider readiness.
The dependent lifecycle candidate implements the connection below. Runtime
enablement and live acceptance remain separate unfinished deliverables.

## Lifecycle candidate: durable replay and release-gated cleanup

Each local operation persists an explicit `shadow-v1` or `membership-v1` mode.
Membership uses a complete canonical request, original trusted installation
observation, idempotency key and independent manager checkpoint. A local lease
takeover preserves those bytes; it does not reinstall, rotate credentials or
re-attest the request. Only the typed same-authority revision conflict permits
a persisted checkpoint refresh. The environment's accepted mode changes only
with an exact current acknowledgement, never merely because a request was sent.

The trusted installer validates its operator-pinned execution before mutation,
reads installed protected database/agent identity, verifies credential separation
and authenticates the installed agent login. An actually empty legacy protected
inventory may attest high-water zero; existing legacy work requires its own
authenticated transition. A caller-supplied digest is not this observation.

`POST /v1/personal-memberships/operation-outcomes/query` resolves the original
actor, key and full request through a separately authorized, current unbound
`capacity:read` observer. An active/draining epoch's missing event is unresolved;
only authenticated irreversible retirement and exact non-conflicting history
can prove terminal non-commit. A recovered commit under unchanged authority
stays pending for exact replay. Recording it as historical resolution requires
an authenticated current-state observation proving an authority transition.
Historical resolution preserves the original evidence and does not mark a new
authority ready. Successor re-attestation remains a distinct transition.

### Reviewed historical continuation

After `membership_outcome_resolved`, a protected operator plan may continue the
exact original owner intent through one fresh linked operation. It does not
rewrite the original envelope, import historical subjects, or select whatever
authority is currently available. The reviewed V3 preparation must explicitly
adopt the exact active accepted subject into its managed base; a noncommitted
first create instead has no adopted subject. The manager still makes the final
atomic admission and identity decision.

Migration `0140` retains the predecessor and independently accepted source with
restricting foreign keys. The predecessor envelope and referenced accepted
operation/attempt evidence remain immutable. In one lease-fenced transaction,
the predecessor becomes `superseded` at `membership_successor_created`, a fresh
operation/key/attempt is created at the next epoch, and the environment points
to that running child. A deferred database check requires the complete runnable
handoff. Exact retries return the same child. Downgrade refuses to discard any
successor history.

An adopted create/update/capacity continuation is an ordinary fresh update, with
a higher deployment generation and newly verified installation. A noncommitted
first create remains create. A noncommitted destroy retains its exact accepted
deployment evidence and starts at retirement; it cannot seal or delete before
its new disabled receipt and authenticated release. An already committed
historical destroy keeps the existing release flow and gets no successor.

Optional service settings `LOOM_SVC_PERSONAL_DEV_MEMBERSHIP_SUCCESSOR_PLAN_FILE`
and `LOOM_SVC_PERSONAL_DEV_MEMBERSHIP_SUCCESSOR_PLAN_SHA256` must be paired. The
file is canonical `PersonalDevMembershipSuccessorPlanV1` JSON, owned by the
service identity with mode `0600`, not a symlink. The bounded plan contains
1–128 unique predecessor bindings and is limited to 8 MiB. Its independent
canonical digest and every binding's full current acceptance authority must
match protected service configuration. Both settings default empty: no plan
means no successor authority. Personal source and owner requests cannot provide
these settings or bindings.
Legacy modes reject either setting, including when personal deployments are
disabled; misplaced recovery authority is never silently ignored.

Each binding has its own reviewed window of at most 24 hours. The reconciler
checks the exact current delegate/checkpoint and review window across I/O;
non-destroy work also needs positive admission. Expired positive admission does
not prohibit reviewed destroy recovery. The retirement installer derives its
allowed starting generations only from the immutable reviewed adopted member and
failed predecessor. It advances the exact disabled guard and reporter registration
to the fresh generation without resetting reporter history. Secret/Deployment
retries accept only these exact retained manifests or the target; candidate build
failure does not block authenticated teardown. Both retained credential Secrets
must identify the independently retained accepted operation, not merely agree
with each other. These source-level ports are not
live enablement: operational adoption and end-to-end recovery must be verified
before provisioning the plan in a deployment.

Owner status exposes only the derived parent/child identifiers and original
continuation kind, not the protected plan. CLI replay follows the exact chain
from the original apply receipt and accepts readiness only for its matching
terminal operation and environment projection. A pending update may still show
the previous accepted deployment; that is not a completed new deployment.
Application readiness remains separate from observed worker availability.

`POST /v1/personal-memberships/subjects/status/query` and
`POST /v1/personal-memberships/subjects/release/query` bind an exact immutable
receipt and report current authority plus historical incarnation work. Historical
status explicitly reports `worker_available=false`. Release covers all retained
deployments and epochs: released lifetime history is streamed with bounded memory,
not truncated at a per-report claim limit. Unknown/unreleased executable intents,
legacy reservations and observed commitments continue to block cleanup.

Destroy persists its disabled receipt at `cleanup_pending`, observes authenticated
release, persists `release_verified`, and only then seals local authority and
deletes resources one lease-fenced checkpoint at a time. The database transition
and runtime sealing/deletion entrypoints both require the exact persisted release.
`keep_data` skips database and bucket deletion; recreation still requires fresh
incarnation-specific storage and allowlisted application-data transfer, not reuse
of the predecessor's protected authority. This retained-data successor path must
be completed before enabling recreation.

The pure `PersonalDevStorageBindingV1` contract distinguishes historical
`legacy-name-v1` storage from explicit `incarnation-v1` storage. The new layout
derives database/roles/buckets and object-store identities from the name plus
full subject incarnation, retaining stable namespaces and routes. Short purpose
suffixes keep every PostgreSQL and bucket name within 63 bytes. Bindings pin
owner user/team and subject identities; resource paths are never caller overrides.
Migration `0141` reserves the canonical binding and digest on each environment
and lifecycle operation before external work. Historical NULL bindings retain
the exact legacy mapping; there is no physical rename or historical backfill.
Retries and same-incarnation operations retain their binding. Recreation inherits
the incarnation layout even if management configuration rolls back, while
allocating fresh physical names and preserving retired operation history.
Database guards enforce canonical ownership, immutable history, current-operation
handoff and release-gated fresh incarnation transitions. Downgrade refuses to
discard any bound storage history.
These new append-only protections apply once an environment has incarnation-bound
history, including its retained legacy predecessor records. They do not redesign
legacy-only administrative deletion before opt-in. The lifecycle controller has
no environment/operation row-deletion path; destructive database administration
is outside the supported recreation workflow.

The identity-taking SQL planner and claim resolver retain this persisted binding
instead of deriving name-only physical targets. The Kubernetes credential vault
keys caches by the full immutable identity and rereads bound Secret sets before
reuse. Main, admin and protected-runtime Secrets carry canonical binding JSON and
SHA; database endpoints/roles, object-store tenant identity and namespace binding
must match. Namespace provenance uses annotations, since SHA256 exceeds the
Kubernetes label-value limit. Creating a bound namespace never adopts an existing
unbound namespace. Legacy Secret shapes remain unchanged and cannot reinterpret
bound credentials. The pinned MinIO integration test exercises maximum-length
static-IAM tenant names, cross-owner/incarnation access denial and old-tenant
cleanup without removing retained objects or newer tenants.
Bound Secrets use immutable, create-only writes. Main credentials persist first;
retries validate all existing material before filling missing admin/runtime
Secrets, preserving credentials if the API reply is lost after server persistence.

Storage-aware activation uses explicit intent version 2 with a full binding and
digest. Historical version-1 canonical bytes and its unversioned HTTP response
remain unchanged. Management checks the environment, operation and candidate
owner before emitting the new intent; the independent agent uses its bound
physical identity. Owner responses report those same persisted storage names.
Candidate fixture preparation now resolves the persisted claim before any external
work and uses the identity-taking SQL plan. Namespace bootstrap rejects unbound
or differently bound existing namespaces; same-incarnation namespace updates carry
the observed UID. Readiness covers that UID and the canonical storage digest.
Namespace cleanup captures the authenticated namespace UID and sends it as a
Kubernetes DELETE precondition, then waits until that UID is absent or replaced.
It never retries deletion by name against a replacement, and malformed readbacks
cannot count as successful cleanup. Disposable Kubernetes integration exercises
real raw-DELETE transport, stale-UID rejection, bootstrap rejection and partial
Secret recovery; no live cluster is used by these tests.

Capacity installation, verification, sealing, cleanup and membership retirement
resolve the same persisted claim binding. The capacity credential seed and agent
Secrets carry its canonical JSON and digest; bound reads verify namespace and
Secret provenance before using credentials. Owner GET/list records preserve the
binding, and capacity status validates namespace/database/subject coordinates
before selecting the incarnation-specific observer role. The legacy provisioner
rejects bound records before side effects, including public/queued reservation
entrypoints. Generic reservations refresh the ORM view under the row lock and
reject bound storage before changing lifecycle state, even when an older session
cached the previous legacy layout. A disposable PostgreSQL test migrates
three independently provisioned databases and proves full-length role names,
cross-owner/incarnation connection denial and retry isolation. Repeated sealing
retains application bytes but denies old logins; repeated final cleanup leaves
new-incarnation and other-owner databases and credentials intact.

The bound capacity credential seed uses two-phase Secret writes. Management
creates only an empty, owner-bound placeholder, then verifies the namespace UID
again after the acknowledged CREATE. Credential-bearing writes use update-only
PUT with that child object's UID and resource version. Delayed writes therefore
cannot create a missing Secret or overwrite a replacement UID; a same-object
concurrent writer loses on its stale resource version and stops before SQL.
Seed metadata also pins a monotonic operation epoch. A ready seed permits only
identical credential bytes within that epoch, so an already-prepared retry cannot
overwrite the winner by obtaining a fresh resource version. Credential rotation
requires a newer operation. Retries recover persisted
credentials after a lost final reply rather than rotating them. Only an exactly
validated empty placeholder from the same logical owner's former namespace may
be removed during recovery, with both UID and resource-version DELETE
preconditions. Populated, malformed, foreign-owned or concurrently changed
objects are never discarded as empty. Disposable Kubernetes tests exercise
those interleavings at the actual API boundary.

Bound main, admin, protected-runtime, capacity-agent and credential-seed Secrets
use their existing purpose name plus the full incarnation UUID hex. Logical
namespace and volume names remain stable. Vault reads/writes, persisted vault
references, application/migration manifests, capacity installation/status and
membership observation/retirement resolve those same names, without fixed-name
fallback. Legacy callers also reject a bound namespace before accessing old
names. Vault Secrets use the two-phase helper with immutable final bytes;
identical final retries are accepted, while conflicting credentials are rejected.
Agent convergence and retirement stage Secret writes before applying workloads.

Bound bootstrap installs a namespace Role granting management GET on only the
five resolved Secret names. Its general read ClusterRole omits legacy fixed-name
Secret access. The management principal's narrowly named Role bind/escalate
permission is constrained by fail-closed admission to that exact one-rule grant
and its exact service-account binding. It cannot add list/watch, unrelated names
or another principal. Admission resolves the corresponding per-purpose workload
allowlists and forbids changing/removing storage annotations on a namespace UID.
Bound preparation persists immutable vault credentials before mutating database
roles, so a losing credential writer cannot change the winner's SQL password.

Bound migration Jobs and runtime/capacity Deployments now use inert CREATE
followed by UID/resource-version-pinned update-only activation: Jobs begin
suspended with their final immutable template, and Deployments begin at zero
replicas. Management verifies the exact namespace UID and binding after CREATE.
An additional observation may accept only status/controller bookkeeping changes
on that acknowledged object; it cannot adopt a changed spec, identity or epoch.
Conflicts propagate without an internal fresh-authority overwrite. NetworkPolicies
and Services are installed before activation.

Each workload records its canonical requested spec and namespace provenance.
Server-side dry-run UPDATE normalizes defaults before comparison with persisted
spec; arbitrary observed defaults are not copied into requested intent. Jobs
retain only API-generated, UID-checked selectors/controller labels. Deployment
updates rebuild desired fields so removed configuration does not survive.
Attempt evidence is excluded from bound immutable selectors and Job templates.
The durable attempt sequence accompanies its UUID: at the same operation epoch,
an existing workload rejects older sequences and different UUIDs at the same
sequence. Capacity configuration updates retain their supported same-epoch
semantics and are not given fabricated attempt identity.

One exact Namespace owner reference, including its UID, is retained on all
workload PUTs. Kubernetes garbage-collects a delayed inert CREATE that refers to
the deleted Namespace UID, even when the namespace name has already been reused.
This provides asynchronous cleanup only, not activation authority. Disposable
Kubernetes coverage verifies Jobs and Deployments are collected and successor
objects survive; actual management RBAC/admission coverage exercises staging and
activation. Running-Pod tests separately verify that an old incarnation's Secret
name cannot mount a successor's credentials.

A terminal failed Job may be replaced on a newer authenticated attempt, using
foreground DELETE with both UID and resource-version preconditions, followed by
inert staging. Successful or active Jobs retain their UID during replay. Before
writing a bound Job, the writer reserves its canonical immutable intent and
monotonic `(operation epoch, attempt sequence, attempt UUID)` in a namespaced
`loom-workload-fence-<job-name>` ConfigMap. The record survives failed-Job deletion;
older attempts cannot reacquire authority in the absence gap. Bound migration
Jobs have no TTL; legacy migration Jobs retain their 600-second TTL. Namespace
teardown collects the Jobs and reservation records. A future per-generation
cleanup mechanism must retain durable completion/attempt evidence first.

Reservations carry the exact storage binding and Namespace owner reference, no
credentials. The writer checks the live Namespace UID and verifies the reservation
after creation, before activation and before failed-Job deletion. Reservation
updates use UID/resource-version preconditions without an internal mutation retry.
Admission restricts their names, shape, immutable intent and ownership, monotonic
ordering, and rejects management DELETE. CEL cannot read the live Namespace UID:
it checks equality of the submitted UID annotation and owner reference and pins
both on UPDATE; live UID authentication remains the writer's responsibility.

A lost CREATE reply or a concurrently created inert placeholder permits only one
readback, never a repeated CREATE. The current attempt may reuse that object only
after the same full Namespace, intent, phase and attempt authentication. This
also recovers a delayed older inert CREATE during a newer replacement. Missing,
malformed, foreign or changed-intent objects cannot authorize activation. Lost
final PUT replies still require a fresh caller replay.

The reservation and Job are not one atomic transaction. A final PUT racing a
reservation advance can still win on the same Job UID/resource-version and same
immutable intent; the current caller must retain an active/successful Job and can
replace only an authenticated terminal failure. These tests establish named
object ordering, not serialization of delayed descendant Pods or external effects.

Candidate preparation now defers one narrowly evidenced failure to normal durable
lease reclamation: kubectl must report its canonical single-line resource-version
conflict, and a bounded readback must show the same Namespace UID and exactly the
same workload authority/spec with only status/bookkeeping and resource-version
progress. Unknown diagnostics, admission-denial headers (including conflict text),
changed specs/UIDs/owners and unavailable readback do not qualify. This conservative
CLI diagnostic contract may fail to recognize a version conflict with warnings or
a changed kubectl message format; it never treats generic command failure plus
status progress as sufficient evidence.

The one-shot writer does not retry the mutation. The reconciler stops preparation
without failing the running attempt or publishing readiness. Once the durable
lease expires, a fresh claim rechecks current operation/attempt/incarnation and
reloads owner access and admission before preparing again. Revoked owner access
still fails preparation; stale lease holders cannot heartbeat after reclamation.
Ordinary candidate-preparation failures remain terminal. Membership/capacity
runtime errors already return to their existing lease-based service loop, and do
not use candidate preparation's owner-access loader. Real Kubernetes and isolated
PostgreSQL tests exercise these distinct boundaries.

The layout remains disabled. These management-write fences do not fence delayed
ReplicaSet/Pod writes by Kubernetes controllers, or already-started PostgreSQL
and MinIO effects. Namespace CEL checks cannot replace atomic fences: namespace
objects used by admission policies come from an informer cache, and a namespace
read is not atomic with a child write. External-effect fencing and full
concurrent-owner acceptance remain required before rollout.

Bound PostgreSQL administration uses a permanent
`ld_fence_<incarnation UUID hex>` NOLOGIN role, outside the disposable database
and runtime-role set. Its canonical COMMENT binds the complete storage identity
and an irreversible retired state. Guard creation and COMMENT are atomic;
retirement commits before revocation or cleanup. Unexpected role attributes,
password, memberships, settings or COMMENT fail closed rather than being adopted.
The guard is never included in runtime-role cleanup.

Primary and capacity cluster-DDL provision/seal/drop operations hold a session
advisory lock in the canonical `postgres` maintenance database on the same backend
that executes those changes. Bound administration requires CONNECT to that
database and permission to verify the protected role catalog; it does not grant
new privileges. Supplying another database in the admin URL cannot split the
lock. A delayed provisioner cannot reopen retired roles or recreate dropped
storage, including retirement before first provisioning. Failed retirement can
resume without clearing the marker. Bound sealing waits for protected-role
backend termination and rejects an incomplete result.

The privileged capacity target-database transaction takes `FOR SHARE` on that
permanent shared-catalog guard row before any effects or role switch. Retirement
takes `FOR UPDATE` on the same row before committing its marker, so it waits for
the actual target transaction to commit or roll back. Lock acquisition and marker
validation use separate READ COMMITTED statements: COMMENT changes a different
catalog, and a snapshot acquired before waiting must not authorize later work.
Absent, altered, retired, wrong-database or incompatible-isolation guards fail
closed. Migration-error/cancellation revocations use the maintenance session
lock too, without changing an active guard to retired or reopening a retired one.

The migrator subprocess, executor-surface/registration transactions, transient
migrator administration and owner bootstrap use incarnation-specific restricted
login roles instead; NOLOGIN and verified backend termination drain those roles.
The row lock is not a proxy lock around their separate connections or MinIO
requests. Requests already authorized by MinIO still require separate treatment.
Adoption must drain older unguarded writers; the guard cannot constrain code that
never consults it. These tests are not concurrent-owner acceptance, and the live
layout remains disabled.

Bound MinIO tenants no longer remove/recreate IAM users or detach policy mappings
during convergence. Their exact bucket-scoped Allow policy is immutable, and
credential updates/additive attachment preserve any retirement Deny. Retirement
first creates a permanent `loom-retired-v1-<storage-binding SHA256>` Deny policy,
then retains or creates the incarnation user, attaches that policy, checks both
readbacks, and verifies authenticated AccessDenied. A missing user cannot erase
the policy's retirement decision. Interrupted retirement can resume, including
retirement before first provisioning; a delayed credential update cannot remove
the installed denial. Unknown lookup errors, foreign policy authority and altered
policy bodies fail closed. Credentials continue to travel only through stdin.

These retained IAM records are retirement tombstones, not object data. Bucket
purge and retained-data transfer remain separate operations; the tombstones must
not be removed by ordinary cleanup. New request denial does not cancel an S3
request authorized before retirement. Controlled adoption must drain older
remove/recreate scripts, which do not implement this protocol. The pinned-MinIO
tests verify actual IAM denial, concurrent Allow attachment, delayed credential
updates, missing-user replay and continued administrator access to retained data.
They do not establish an in-flight-request drain or authorize live rollout.

These contracts do not enable the new layout in the live service. Full lifecycle
acceptance, stale namespace-child-write fencing and allowlisted data transfer
remain required before selecting it or lifting retained-data recreation's
interlock.

The initial retained-transfer binding is a pure, canonical lineage record. It
requires the same environment, subject and owner/team, a distinct incarnation,
a later distinct destination operation, and the predecessor release digest.
All three bucket mappings derive from the two complete incarnation bindings;
paths cannot be supplied independently. Legacy-name adoption is rejected.
Parsing checks bounded bytes and the exact canonical digest, including nested
binding validation. This record is not a transfer capability or proof of release:
management must authenticate and persist its relationship to the source destroy
and current successor operation. It does not lift the recreation interlock.

The object-capture primitive uses a separately configured, control-owned versioned
bucket. It conditionally copies exact source objects (including multipart objects
up to 64 GiB), pins the returned non-null VersionId, and streams that version to
verify its byte count and SHA-256 without a local object cache. Snapshot keys are
derived from the transfer, capture ID and object intent. Later writes to the same
key create new versions and cannot change a pinned capture. A lost completion
reply yields no success receipt; retry may leave an additional unreferenced
version for subsequent scoped cleanup. Part failures abort only their own upload.
Source replacement between multipart parts rejects an unversioned source; a
versioned source continues reading the originally pinned version. Suspending
snapshot-bucket versioning before or after completion yields no receipt, even
when the copy left unreferenced bytes. Unknown paths, inconsistent/truncated
readback and source changes before pinning reject capture. These are tested storage primitives, not a wired
snapshot ledger, restoration path, IAM policy deployment or in-flight drain.

Object inventory scans all three exact source buckets with bounded pagination;
missing buckets and malformed/cyclic responses fail rather than becoming empty
data. A complete empty three-bucket inventory is valid. Inventory is capped at
10,000 objects and 1 TiB, with bounded canonical documents. The snapshot manifest
requires one matching version-pinned capture for every selected object, in the
same canonical order and transfer/capture identity. Source mutation before a
conditional copy prevents completion. This defines selected captured bytes, not
an atomic S3 snapshot or a promise to preserve writes finishing after selection.
Durable management attachment and the application-data transfer policy remain
required before restoring or activating a successor.

The separate active acceptance binding pins the entire V3 preparation, exact
execution authority and a finite reviewed window; it cannot reinterpret old
zero-capacity acceptance or operational certificates. The service loop accepts
explicit membership ports and uses the same lease-fenced session authority for
the membership driver and resource cleanup. Candidate preparation, credential
bootstrap, pending admission and local readiness recheck the admission interlock
across slow I/O. Expiry preserves retry evidence rather than claiming readiness
or turning an authorization outage into a failed candidate. Historical lookup and
destroy release remain independent of new-admission availability. Stored accepted
membership reports capacity preparation only, never observed worker availability.

### Explicit service mode and recovery boundary

`LOOM_SVC_PERSONAL_DEV_RUNTIME_MODE=membership-v1` selects the active membership
driver explicitly; the default remains `shadow`. The separate
`PERSONAL_DEV_MEMBERSHIP_BINDING_JSON` and `PERSONAL_DEV_MEMBERSHIP_PLAN_SHA256`
settings carry the reviewed active acceptance document. These service settings
use the `LOOM_SVC_` prefix. Mixed legacy acceptance/operational bindings are
rejected before opening membership credentials.

`PERSONAL_DEV_MEMBERSHIP_OBSERVER_PRINCIPAL_ID` pins an independent current
read-only observer. `PERSONAL_DEV_CAPACITY_OBSERVER_{BEARER_TOKEN,CA,CERTIFICATE,PRIVATE_KEY}_FILE`
provides its transport and bearer credentials, separate from the delegated
lifecycle identity and installed capacity reporter. Admission authenticates the
observer identity/authority and the delegate's full execution checkpoint, then
rechecks the finite window across slow I/O. The legacy status identity alone is
not a complete active-execution fence.

Startup owns and closes all three HTTP clients, including partial-construction
failure, and stops reconciliation before closing them. Valid expired admission
does not prevent startup: historical lookup, stored-mode destroy and release
cleanup must continue. New application requests use the trusted service mode;
destroy uses the environment's persisted accepted mode, not the mode selected
by a later service restart. Retained-data recreation involving membership is
explicitly rejected until fresh incarnation storage and data transfer exist.

The existing Kubernetes/native source builder is **inert in membership mode**:
neither executor consumes an owner-charged membership allocation. Source intake
and apply report builder unavailability; legacy native polling cannot claim a
retained grant or issue new capabilities. Authenticated heartbeat/completion
remain available for retained evidence. This is not a legacy-build migration:
the old provider must be drained and reconciled before operational adoption.
Stored application convergence and release recovery can run without admitting
new source builds. Builder availability cannot be enabled by a caller flag.

No operator renderer selects this mode yet. Active operational promotion evidence
and the allocation-accounted build bridge must be delivered before live
enablement. A service health response or these source tests do not certify
multi-person readiness.

## Build service and runtime connection

The common membership mechanism will admit one typed `personal-build-worker`
service per owner from an operator-pinned builder-runtime release. It has two
operator-owned profiles: ARM64 builds use GB10, and AMD64 builds use OLDLAB.
Both executions charge the same `dev-owner-<owner UUID hex>` account as the
owner's applications. Every source attempt still requires both native platform
bundles. Build demand is explicitly architecture-specific, never arch-neutral;
the GB10-only restriction applies to the ARM64 profile, not the whole service.
Application task demand retains its architecture-specific and neutral choices.
The build-purpose executor paths must be implemented on both pools before that
purpose is accepted for executable admission.

Pure composition now has explicit V4 preparation contracts, a separate build
member, a combined application/build membership snapshot and delegated allocator
input. The build subject ID is deterministically derived from the existing
namespace, owner and build purpose; its reserved name is `dev-build-<ownerhex>`.
Both native profiles must have a single non-warm one-node/one-slot shape with
positive CPU/memory, the matching architecture capability and native fleet-domain
placement. The member's full runtime candidate must equal the operator template.
Build minimum and surge slots remain zero in this initial cold-only version.
Applications retain their configurable minimum, defaulting to zero.

Build members consume the same owner live-subject and capacity limits and the
same combined membership bound as applications. The build template itself must
fit the existing owner policy even before any build member exists. Application
and build configuration cannot overwrite each other's base identities or names.
Reincarnation retains stable owner/service identity with fresh incarnation and
reporter identities; its predecessor-release authentication is still a required
durable-store responsibility, not proved by structural validation.

Typed membership commands now use one execution/namespace/revision fence with
discriminated application or build-service projections. The build projection
does not accept a subject ID, account, name, feature-source publication, minimum
or surge: these are derived from the owner and operator preparation. Service
configuration generations follow service lifecycle operations; ordinary feature
build attempts emit demand without changing membership. Service deployment and
reporter rotation need not change the pinned runtime candidate. Build operation
and idempotency IDs must be independent of application/feature operation IDs.
Bounded, duplicate-safe parsing and pure result derivation validate the complete
configuration and acknowledgement, including reincarnation namespace and manifest
coordinates. They do not authenticate currentness, an event chain, predecessor
release, replay, or a build-service installation. Those checks remain required
in the durable admission and execution consumers. Legacy application request,
result and endpoint formats remain unchanged.

The successor event reader checks typed request/result semantics against every
indexed event column, the pinned delegate, request digest and original head
preimage. Its mixed-purpose prefix check enforces consecutive revision/head and
operation/idempotency-key uniqueness. Build history additionally checks service
generation monotonicity, reporter/token rotation versus retention, same-purpose
identity and the selected predecessor event for recreation. This is not yet the
durable history reader: it does not authenticate materialized reporter credentials,
retained application installation, actual predecessor release, latest-head
currentness or SQL insertion authority. The old
application event preimage is retained byte-for-byte, including its original
request format; old history is not translated into typed commands.

Fresh application history now also checks lifecycle monotonicity, globally fresh
reporters, complete source/installation/protocol retention during capacity and
teardown, and retained name uniqueness even after disablement. Its retained
generation reader authenticates installation attestation against the original
create/update operation independently of the reporter's latest configuration.
It verifies refreshed candidate/deployment/profile records canonically and can
validate a historically fenced reporter. Managed-base adoption still requires
its original immutable configuration and durable application provenance; neither
an acknowledgement nor current materialization can substitute for that origin.

The internal `import_retired_applications` operator helper imports an entire
application-only typed snapshot into the next immutable configuration. It requires
real activated/drained/retired evidence, the exact final history, and unchanged
materialized subjects, accounts and reporters. Existing installation records are
reused; fresh and updated applications are not passed through shadow installation
creation again. Static candidates remain bound to their pinned acknowledgements.
Disabled applications are retained. Build history and recreated applications are
rejected until successor import can preserve purpose and original incarnation
lineage. The helper composes ordinary configuration
proposals and activation in one transaction; its key identifies that derived
configuration activation, not a separate import receipt. Replay is historical and
is unavailable while execution is active. It does not activate V4 execution or
open a public endpoint.

Successor provenance now has separate structural contracts and an internal
read-only `export_retired_member_origins` helper. Under a SERIALIZABLE authority
lock, it authenticates retirement, the complete final snapshot, the actual last
event for each subject, original configuration root, and retained installations.
Its result distinguishes applications from pending build services and preserves
recreated incarnations. It creates no configuration proposal or execution grant.
Untouched operator-pinned applications retain their original origin without a
synthetic membership event; pending session edits are rejected without flushing.

`ManagedApplicationOriginV2` adds inherited provenance while preserving V1 bytes.
`ManagedBuildOriginV1` also pins the trusted runtime release, build template,
original installation and latest service projections; it cannot claim readiness.
The inherited source's final global head is distinct from the subject's own event
head. Old recreation certificates retain their original epoch and revision inside
that provenance, never rewritten as a successor epoch's certificate. Structural
contracts represent empty-epoch inheritance. V4 policy/preparation now represent
`managed_build_origins` and an explicit `retired_source`, alongside a discriminated
V1/V2 application-origin union that cannot strip inherited fields. All managed
identities have exactly one origin and matching acknowledgement; build origins
must retain the exact template and trusted runtime release. The immediate source
is explicit even when there are no membership events or inherited member fields.
These additions revise unpublished, unactivated V4 documents only; published
V1/V2/V3 documents keep their original bytes. Historical source-graph verification
works through the separate read-only preflight described below. Purpose-aware
runtime successor consumers and complete typed import remain unconnected.
The ordinary typed history reader still rejects source-bearing preparations;
import and execution interlocks remain closed.

The internal `verify_successor_source` preflight authenticates a retired source
chain and compares the proposed origins against the complete durable export.
Removing an application, build service, or every managed member remains invalid
even when the proposed policy and acknowledgements are internally consistent.
Source epochs must strictly descend; configuration advances without substituting
the fleet, namespace, development template or trusted build runtime. This preflight
is historical evidence only, not a receipt or admission path.

`load_retired_source_graph` follows one strictly descending immediate source per
epoch, then authenticates complete membership sets and immutable installations
from oldest to newest under the common SERIALIZABLE authority lock. Empty inherited
epochs preserve all application/build origins; untouched members change only the
immediate source reference, retaining the real own-event anchor and original root.
Historical reporter rows need not remain current. There is no global cache or
recursive Python traversal. Limits are 1,024 epochs, 64 MiB of canonical manifests,
65,536 oldest-leaf events and 64 MiB of stored event request/result JSON text.
Event payload sizes are streamed as scalar values before loading the leaf log;
exceeding a bound rejects the read. These are explicit read-work limits, not
retention/garbage-collection authority.

This graph reader is limited to inherited epochs with no local membership events.
It rejects any such event using an existence query before loading the log.
Ordinary typed runtime readers and SQL insertion continue rejecting source-bearing
preparations until purpose-aware mutation, allocation and admission consumers are
connected. Read-only graph verification does not activate a successor or complete
the development runtime acceptance criteria.

The pure typed prefix validator supports first capacity, update and destroy events
for inherited applications and build services without synthesizing local create
events. Later local recreation uses the original configuration root retained in
provenance, not the latest imported base. Reporter, token and operation reservations
cover both purposes. A first ordinary successor mutation has no local recreation
certificate; any older certificate remains unchanged in inherited provenance.
The pure command/prefix path also validates a first recreate of an imported disabled
member using explicit cross-epoch evidence, not a rewritten epoch-local certificate.
It matches the complete pinned source, own event, predecessor configuration,
original root, owner/purpose and current execution epoch/manifest. Later local
mutations retain that certificate; a subsequent locally witnessed recreation uses
a new V1 certificate against its real local event. These structural checks alone
do not open durable consumers or authenticate release witnesses.

`PersonalInheritedReincarnationEvidenceV2` is a versioned value for the pending
cross-epoch recreation path. It keeps the predecessor's real own epoch, manifest,
revision and head, the immediate retired source, the original root and the current
admission epoch/revision. It permits own revision N followed by successor revision
1 only through strictly descending source epochs; it does not authenticate a source
or release itself. Legacy V1 instance validation and serialization reject this
newer evidence instead of dropping its fields. Versioned application/build member
carriers preserve it in parsed values using explicit purpose and integer version
tags; legacy member constructors and serialization reject those newer carriers.
Durable store issuance, SQL and runtime consumers do not admit it yet, and pure
allocation explicitly rejects unconnected cross-epoch recreation in both local
members and inherited bases. Operational imported-disabled first-create therefore
remains closed. An original operator base without a real own-event anchor cannot
use this evidence path.

Pure allocation also resolves inherited build bases using the pinned build template
and native profiles, including empty successor snapshots. Build and application
overlays replace their own base reference once and retain a common owner ceiling;
they cannot switch purpose. Recreation references the inherited original root.
The internal durable materialization consumer accepts same-purpose inherited
application/build overlays and checks exact stored rows against the supplied
authenticated tip, including original-root recreation. Its caller remains
responsible for authenticating that tip; current successor mutation, import and
admission are still unconnected.

The internal build-generation stager accepts a first ordinary mutation against a
pinned inherited build base. It verifies the actual pending candidate, deployment,
profiles and current reporter generation/token before writing, retains reporter
high-water on capacity/destroy, and fences the old reporter on update. It does not
fabricate a predecessor request or claim readiness from a trusted runtime release.
The owning transaction must authenticate the complete source chain first; this
internal staging support is not a successor admission endpoint.

V4 preparation and operator policy now pin `managed_application_origins` with
exact coverage of the managed base identities. Each origin contains the complete
immutable base configuration, original create/update installation projection,
last base projection, and full acknowledgement matching the preparation. The
allocator joins the configuration to its immutable base reference. Historical
projection input epochs are preserved; a later capacity epoch must not rewrite
the original installation operation. This is an operator-authenticated origin,
not a digest reconstructed from mutable database rows. It creates no synthetic
membership revision. Durable adoption independently verifies these bindings and
current reporter evidence; executable V4 admission remains closed.
Persisted typed-history reads now verify these roots and installation records
even at revision zero, without consulting mutable shadow projection rows for
origin authority. Current materialization reuses the immutable base reader;
managed application overlays replace the base exactly once after authenticated
lifecycle validation. Static-base takeover remains rejected.
The pure typed event validator now checks an adopted application's first
update/capacity/destroy against that pinned base, without inventing a create
event. It preserves identity and non-deployment service evidence, requires update
to advance deployment and rotate reporting, and reserves base names, tokens and
installation operation IDs. The transaction and SQL guard additionally verify
the original retained installation and reporter state; passing this pure
validator alone grants no adoption.

Pure application recreation validation now mirrors build history: the successor
must have a fresh incarnation/reporter, restart service generations at one, and
bind its certificate to the exact disabled predecessor event and first immutable
origin. Later mutations must retain that certificate. The durable transaction and
SQL insertion guard independently verify the predecessor's actual released ledger
across execution epochs before accepting recreation. Observed or quarantined work
blocks it; accepted workers require protected and terminal release witnesses.
Release reads refresh retained ORM objects, reject unflushed ledger edits without
discarding them, normalize timestamps to UTC, and hash witness keys in fixed ASCII
order rather than database-locale order. This does not activate V4 execution.

The retained application reader exposes installation-only validation separately
from reporter validation. Historical installation reads still verify exact
candidate attestation, deployment and complete profile records after reporter
rotation; they do not claim the old reporter is current. Existing member callers
retain the combined check until cross-epoch reporter authority is connected.

Build-generation persistence now stages the exact native runtime candidate,
two worker-profile records and the service reporter in the membership transaction.
Candidate digests hash the complete runtime binding rather than padding a Git
SHA. Deployment rotation may reuse the same candidate; capacity and destroy retain
the reporter token and demand high-water. Retained facts are checked before later
mutations, and failed staging rolls back its partial writes. These deployments
remain `pending`: publication and membership acknowledgement alone do not prove
the management admission runtime is installed and executable. They contain no
synthetic application-agent installation or application activation evidence.

Migration `capacity_0018` adds the SQL boundary for typed service lifecycle events.
It retains the original V1 application insertion function and dispatches other
wire versions to a separate, private, fixed-search-path guard. V2 fresh application
and build `create`, `update`, `capacity`, and `destroy` commands under an exact active V4 authority
are accepted. The guard
checks the complete command/result and original event hash, deterministic UUID5
identity, operator runtime/profile bindings, pending generation evidence, current
reporter, owner/subject materialization, shared revision and membership limits.
It rejects retained identity/name collisions, reused operation or idempotency
keys, and noninteger numeric fields. Updates must advance deployment and rotate
the reporter; all retired reporters retain their exact fenced credentials and
generation. Capacity changes and teardown retain deployment, reporter and token.
Application candidate generation advances with deployment and retains the original
installation attestation across capacity changes and teardown. Build deployments
remain pending; application records require exact ready installation evidence.
Names and digest fields require JSON strings, not SQL stringification of numbers
or booleans. Application source/publication digests and operation/idempotency
identities must be nonzero. Managed-application adoption takes its predecessor
from this epoch's event or the exact operator-pinned origin, never from the
largest historical epoch. It verifies the immutable configuration root and
generation, fleet-derived base, original candidate/deployment/profile evidence
and reporter fencing. First capacity/teardown retains the original installation
attestation; first update fences the base reporter at its last base generation.
Global identity conflicts remain enforced with only exact same-identity base
exceptions and certified recreation lineage. This is not build execution admission.

SQL UUID5 uses the standard `uuid-ossp` extension at its existing schema, or
installs it in a new private `capacity_build_extensions` schema if absent.
Migration does not move an existing extension or change its privileges. Downgrade
refuses retained V4 epochs or typed history; otherwise it restores the original
application trigger and removes its own helpers, preserving the extension and
schema for unrelated dependents.

`CapacityTypedMembershipStore.apply` owns the combined fresh-application and
pending-build lifecycle transaction; `apply_build` remains a build-only wrapper.
It resolves the pinned management delegation, preparation and fleet
from current database authority under a SERIALIZABLE authority-first lock. Before
insertion or replay it validates the full typed event prefix, retained generation
facts, immutable base configuration, and indexed subject/account materialization.
Exact retries return their original receipt even after another owner advances the
shared head; stale new mutations must retry against the new revision. Concurrent
transactions cannot leave duplicate or partial build generations. Staging,
materialization and the guarded event insert roll back together, including on a
late SQL rejection. Retained reads refresh ORM objects and compare nested JSON
canonically, so cached values and integer/boolean/float aliases cannot authenticate
changed evidence. Historical generations are validated against the last event for
each reporter, including reporters fenced by a later update. Destroy disables new
subject capacity but retains the current reporter/token and demand high-water for
cleanup; it does not certify physical release or erase charges. Earlier receipts
remain replayable after update or destroy. This internal transaction is not yet
exposed as runtime admission. Recreation retains installation evidence keyed by
subject, incarnation and deployment generation, so restarting generation one does
not overwrite the predecessor's installation. Managed bases
are adopted without a synthetic create event and without double-counting their
allocation. Applications and builds share revision/replay identities,
owner live-subject limits and the same serializable transaction retry behavior.

Typed membership snapshots now read the exact persisted execution manifest,
fleet, full event chain and retained generation evidence. Historical prefixes
remain readable after reporter rotation and teardown; the full chain proves the
retired reporter's last generation. Current materialization validation separately
rejects an old snapshot. The mutation transaction reuses this same history reader.
Historical snapshots authenticate installation and event evidence, not mutable
reporter currentness. Current allocation, mutation/replay and materialization
checks additionally resolve the explicitly current activated authority and verify
every retained reporter against its last current-epoch event or pinned application
or build base binding. Inherited build bases participate even before the first
local event; source-bearing runtime admission remains separately closed.
A prepared epoch is not activation evidence. No reader selects authority
by the largest stored epoch, and historical installation reads do not recursively
require an obsolete reporter to remain current. A readable historical snapshot
cannot substitute for these current checks.
An equivocal current-tip reporter remains valid retained accounting evidence,
but cannot publish demand or authorize new work. Its safety state does not block
other owners. Recovery rotates to a new reporter and fences the old one; no
capacity-only update can reactivate an equivocal reporter.
The current-subject/demand consumer resolves fresh typed application members from
that authenticated history and exact materialization, including independent owner
accounts and generation supersession. It retains the legacy deployment-readiness
check: pending build services cannot publish executable demand. Source-bearing
epochs and V4 execution preparation/activation remain closed. Disposable SQL-only
execution fixtures exercise this demand boundary, not operational activation.
Allocation input loading explicitly returns the typed input with base applications
and owner build services, reusing the existing demand, pool, reservation, physical
commitment and fairness readers. Disabled build services and old physical jobs
remain represented for accounting. Cached writer authority is refreshed before
accepting a writer fence. Under authenticated active typed authority and an exact
operator V4 policy, the reconciler seals this input as `ExecutableEpochV4`, retaining
both purposes and the original immutable base. Writer/input CAS, pool freshness,
executor registrations and increase-freeze checks still apply. A legacy operator
policy cannot authorize typed promotion, and a typed allocation cannot be read as
a legacy executable artifact.

This does not activate V4 execution. Direct preparation-store calls, wire
endpoints and legacy runtime parsers remain closed while the purpose-aware
admission/execution consumers are connected. SQL-only active fixtures verify
sealing and authenticated provenance, not production preparation or activation.

Management translates source attempts into build-capability demand buckets.
The existing global allocator and charged intent ledger handle placement and
fairness. The pool executor routes build intents to a management admission
adapter, not environment trial procedures. Builder launch profiles must match
the real CI publication, not merely contain an unrelated publication digest.

The runtime-publication loader reuses the owner-only, digest-pinned trusted
release descriptor and its canonical CI evidence. It checks the evidence digest,
exact source commit/tree/repository/ref, component index references and both
native platform subject digests, then derives the builder and agent image
references. Its service candidate remains the trusted Git commit plus release
publication digest, not the feature-source hash. The existing CI assembler's
output is exercised against this consumer. The expected release digest must be
independently approved operator input; this loader does not query CI, certify a
node/runtime profile, or authorize executable membership.

A non-authorizing schema-3 controller-policy resolver supports an exact set of
purpose-tagged launch profiles under one pool authority root. Each entry commits
the complete profile, including shape/resources, image, node domains and launcher
configuration, excluding only its self-referencing controller-root field.
Resolution verifies the complete supplied set and the selected intent's purpose,
pool/generation, profile identity, resources, trusted release and node domain.
Legacy V2 resolution/rendering retains its original single-policy digest rule
and rejects the new root. Executable adoption still requires authenticated purpose
and this policy binding throughout signing, submission, reconciliation and cleanup;
this resolver is not wired into existing live execution.

Separate schema-3 signed ownership contracts preserve purpose, complete launch
profile digest, exact subject configuration reference, whole-acknowledgement
digest and the selected immutable personal-member event. Their signature binds
the protocol domain and signing-key ID; legacy V2 signing/verification rejects
these proofs. The event reference is historical provenance, not a requirement
that this owner remain the latest writer of the shared membership log. Signature
verification proves authenticity, not current admission. The trusted producer
must authenticate base/member selection and join the configuration, actual
candidate acknowledgement and event before signing. These contracts do not yet
enable journal/inventory adoption or build execution.

The launch-subject producer now derives those references through
the existing authenticated allocation/history reader. It returns the exact full
configuration and acknowledgement, chooses immutable-base versus member origin
from durable evidence, and binds a member's own event head rather than the latest
shared log head. Current resolution rejects configuration supersession even when
deployment generation is unchanged; historical resolution retains old evidence
for accounting and cleanup. Typed builds retain `personal-build-worker` purpose
and their own authenticated member event; pending build installations cannot
pass current launch resolution. The producer does not sign, submit, or replace
operation-specific intent, pool, executor and current-authority checks.

The separate typed launch renderer consumes those authenticated full subject
facts, joining their configuration and acknowledgement digests, candidate,
deployment, account, profile, shape and node cardinality to the intent before
signing. Purpose selects the exact approved native image under the complete
pool policy root. The schema-3 proof commits the selected profile and member
event; legacy rendering and proof verification do not accept this context.
Both renderers share scheduler-field translation without sharing authorization
rules. This renderer neither obtains manager authority nor enables submission:
the caller still needs current admission, purpose-aware journal/inventory
handling and the held-submit/bind/release path.

The executor-authenticated `GET /v3/executors/{pool_id}/intents/{intent_id}/launch-subject`
returns canonical full configuration, acknowledgement and provenance for the
exact persisted intent. The manager locks current authority, validates the
executor lease and management policy, and requires an unexpired, unconsumed
permit and a currently eligible subject. Reading facts neither consumes that
permit nor advances command high-water. The client checks its registration and
the exact returned intent. Serialization, parsing and streaming share an 8 MiB
launch-facts limit; other receipt limits remain unchanged. Runtime submission
still requires the purpose-preserving journal and inventory consumers; this
read endpoint does not open typed activation or authorize scheduler submission.

The initial management demand projector emits one cold slot per explicitly
requested native platform from a current, owner-matching running build lease.
Its deterministic work identity binds the source candidate, archive generation,
owner/team, lifecycle operation, whole attempt/lease and platform. It rejects
duplicates, conflicting records, missing/expired leases and unusable sources.
Build-specific capabilities keep these requests out of ordinary task shapes.
This pure projector does not authenticate database records or publish demand:
the durable caller must select unassigned platform requests under the current
fence and include assignments and cleanup-unproven commitments in the same
snapshot. Removing pending demand is not physical release. Intake stays disabled.

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
the grant. Preserve native AMD64 execution on OLDLAB, but do not reuse its
unaccounted Kubernetes executor in membership mode. That provider also needs
allocation-contained execution and authenticated cleanup. Preserve node 2's
task-image reservation.

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
