# Active personal application membership implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Enroll, update and disable personal applications under one active capacity epoch without interrupting existing subjects.

**Architecture:** Explicit V3 preparation delegates a bounded membership namespace. An append-only membership log supplies exact current subject evidence to the existing allocator and executor; the immutable base execution fence remains unchanged.

**Tech Stack:** Python, Pydantic, SQLAlchemy, PostgreSQL/Alembic, FastAPI, pytest.

**Spec:** `docs/architecture/personal-dev-active-membership.md`.

## Global Constraints

- V2 execution documents and V1 static allocation canonical bytes retain their meaning.
- Old V2 epochs remain ineligible for delegated admission.
- Keep the immutable base configuration and execution manifest.
- Both the application and build service charge `dev-owner-<owner UUID hex>`.
- Application admission remains dual-pool. Build purpose stays rejected until its bridge exists.
- Destroy does not erase any physical commitment; cleanup remains authenticated.
- No live activation, capacity change, staging mutation or task-image reservation change in this plan.
- No files under `docs/superpowers`; documentation belongs in architecture/implementation-plans.

## Task 1: Versioned delegation and allocator composition

**Files:**
- Create `src/loom_capacity_manager/membership_contracts.py`.
- Create `src/loom_capacity_manager/membership.py` (pure resolution only).
- Modify `src/loom_capacity_manager/allocator.py` and `execution_policy.py`.
- Create `tests/unit/test_capacity_membership.py`.

**Interfaces:**

```python
class PersonalMembershipPolicyV1(StrictV1Model):
    namespace_id: UUID
    management_principal_id: Identifier
    development_template_sha256: Digest
    max_subjects: Annotated[int, Field(ge=1, le=MAX_SUBJECTS)]
    managed_base_subject_ids: tuple[UUID, ...] = ()

class ExecutionPreparationPolicyV3(ExecutionPreparationPolicyV2):
    schema_version: Literal[3] = 3
    personal_membership: PersonalMembershipPolicyV1

class ExecutionPreparationV3(ExecutionPreparationV2):
    schema_version: Literal[3] = 3
    personal_membership: PersonalMembershipPolicyV1

class PersonalApplicationMemberV1(StrictV1Model):
    revision: PositiveQuantity
    owner_id: UUID
    purpose: Literal['personal-application'] = 'personal-application'
    configuration: SubjectConfigurationV1
    acknowledgement: SubjectExecutionAcknowledgementV2

class PersonalMembershipSnapshotV1(StrictV1Model):
    namespace_id: UUID
    revision: Quantity
    head_sha256: Digest
    members: tuple[PersonalApplicationMemberV1, ...] = ()

class DelegatedAllocationInputV2(AllocationInputV1):
    schema_version: Literal[2] = 2
    preparation: ExecutionPreparationV3
    managed_base_subjects: tuple[SubjectConfigurationV1, ...] = ()
    membership: PersonalMembershipSnapshotV1

def parse_execution_preparation(payload: str | bytes) -> ExecutionPreparationV2: ...
def parse_execution_preparation_policy(payload: str | bytes) -> ExecutionPreparationPolicyV2: ...
def resolved_subject_references(value: AllocationInputV1) -> tuple[ConfigurationGenerationRefV1, ...]: ...
```

The parsing functions return the concrete V3 subclass only for exact schema 3;
schema 2 retains the original parser, unknown/missing versions are rejected.
Use a discriminated Pydantic union/type adapter or an equivalent bounded strict
dispatch, not fallback parsing that drops extra fields.

All newly declared UUID fields and managed base IDs are nonzero. Canonicalize
unique base IDs and members; reject duplicate subject IDs, member revisions,
names, zero IDs and revisions above the snapshot revision. Revision zero means
zero head digest and no members; a nonzero revision requires a nonzero head.
Bound member/base collections by MAX_SUBJECTS and policy max_subjects; tombstones
remain counted until epoch retirement. Nested V2 acknowledgement identities must
exactly match subject incarnation, configuration/deployment generation and reporter.
Application candidates use `source-sha256` and an actual publication digest.

`resolved_subject_references` returns unchanged base references for V1 input.
For delegated input require matching namespace, base configuration/fleet generation
and fleet digest, an exact fleet template digest, and managed-base IDs contained
in the base manifest. Merge logged references by subject ID; a base reference may
be replaced only if policy names it. `managed_base_subjects` must exactly cover
those policy IDs and match each immutable base generation reference by digest;
use those original payloads when checking ownership/name/incarnation retention,
not the already-overlaid input. Count the union of managed base IDs and logged
subject IDs against policy max_subjects, including unmodified base members.
Every base managed subject must still be a
development-tier, canonical owner-account application with the protected `dev-`
name. New members must have the canonical owner account and personal-name rules,
development tier, exact template profiles, bounded max/min and owner-derived rate,
template pending/surge limits, and lifecycle active or disabled. Disabled entries
require zero min/max. An existing base subject cannot change owner/incarnation/name.
The full input subject set must match resolved references exactly, including
disabled members. Enforce owner max_live_subjects and aggregate reservation minima
across all resolved non-disabled subjects, not only new members.

Replace only the allocator's base-reference selection with this helper; preserve
its existing digest/profile/account/completeness and common resource/fairness
checks. Translate resolution errors to `ShadowAllocatorError`. Load pinned V3
policy files through the existing stable-file verification path; do not weaken it.

- [ ] Write tests before code. Removing the new composition must make the real
  allocator reject a valid additional owner as incomplete; replacing the base
  manifest with the resolved one is not an acceptable test setup.

```python
def test_new_owner_allocation_keeps_base_configuration():
    value = delegated_input_with_new_owner()  # fixture built from tests.capacity_fixtures
    original = canonical_bytes(value.configuration)
    result = allocate_shadow(value)
    assert canonical_bytes(result.configuration) == original
    assert new_owner_id in {item.subject_id for item in result.allocations}
```

- [ ] Add literal boundary cases for stale/mismatched namespace/template/base,
  forbidden static override, owner/name/incarnation substitution, excessive limits,
  wrong profiles, duplicate identities, disabled nonzero limits, bad acknowledgement
  and build-purpose rejection. Verify V2 parse/canonical round trips unchanged and
  V3 delegation changes the canonical policy/preparation digest.
- [ ] Run the focused unit file and record expected RED; implement the interfaces
  and allocator/policy-loader integration; rerun focused GREEN.
- [ ] Run existing allocator, executable allocator, policy and epoch unit tests,
  Ruff and diff checks; commit the exact task files.

## Task 2: Durable active membership and generation resolution

**Files:**
- Create `src/loom_capacity_manager/membership_store.py`.
- Modify `membership_contracts.py`, `store.py`, `models.py`, `preparation_readiness.py`.
- Create the next numbered migration under `capacity_migrations/versions`.
- Create `tests/integration/test_capacity_membership.py`.
- Extend `tests/capacity_execution_fixtures.py` with optional fleet/subject inputs.

**Interfaces:**

```python
class PersonalApplicationMembershipMutationV1(StrictV1Model):
    execution: ExecutionAuthorityV2
    namespace_id: UUID
    expected_revision: Quantity
    projection: DynamicDevelopmentSubjectProjectionV1
    acknowledgement: SubjectExecutionAcknowledgementV2

class PersonalApplicationMembershipResultV1(StrictV1Model):
    revision: PositiveQuantity
    head_sha256: Digest
    member: PersonalApplicationMemberV1
    replayed: bool

class CapacityMembershipStore:
    def __init__(self, management: CapacityManagementStore): ...
    async def apply(self, session, request, *, actor, idempotency_key) -> PersonalApplicationMembershipResultV1: ...
    async def snapshot(self, session, epoch) -> PersonalMembershipSnapshotV1: ...

async def resolve_subject_acknowledgement(session, epoch, *, subject_id, subject_incarnation,
                                        configuration_generation, deployment_generation,
                                        reporter_incarnation) -> SubjectExecutionAcknowledgementV2: ...
```

Persist an append-only `capacity_personal_membership_events` table keyed by
execution epoch/revision, with actor, request/idempotency/operation bindings,
previous/head digests, subject/owner and complete request/result payloads. Use
unique operation and idempotency keys. SQL guards reject update/delete/truncate;
insertion locks current authority and verifies active V3 manifest, exact namespace,
writer/execution fence, actor, consecutive revision/previous digest and bounded
payload. No rewriting the execution/configuration epoch guards.

The nested projection's `expected_configuration_epoch` must equal the immutable
base configuration epoch; it never predicts a global +1 here. The outer
`expected_revision` is the membership compare-and-swap. Exact replay requires the
same authenticated actor, full request and idempotency/operation bindings and a
still-valid execution fence; it returns its original checkpoint, not today's head.
Capacity/destroy operations still require an acknowledgement matching the new
configuration generation while retaining exact candidate/deployment/reporter and
installed-runtime evidence. Never reuse a base acknowledgement for a new generation.

Use the existing SERIALIZABLE write transaction and authority-lock order. In one
transaction verify policy/delegation, exact replay or expected revision, derive
owner/subject/profile values, validate transition and acknowledgement, persist
candidate/deployment/profile/reporter evidence, append the event and update only
the delegated materialized subject/account rows. Extract shared projection helpers
from the existing shadow implementation where needed; do not copy its full body.
Keep its public V1 shadow guard unchanged. Management principal must match the
prepared policy exactly; a matching free-form actor is not authentication at HTTP.

Creation requires unused identities; updates enforce monotonic generations and
reporter rotation; capacity/destroy retain candidate/deployment/reporter evidence.
Destroy projects disabled zero min/max and retains historical evidence and charges.
Task 2 rejects reactivation of a disabled subject and every incarnation change;
the explicit recreation transition is Task 3. Resolve original acknowledgements only for exact base generations;
resolve delegated generations from immutable events, never fallback across identity.

Extend preparation parsing/validation and readiness to V3, comparing the delegation
exactly with loaded policy. Verify managed-base IDs against existing personal
projection records. Prepare/activate still require exact complete base subjects;
post-activation validation distinguishes that base from delegated rows.
`load_allocation_input` verifies event/materialized correspondence and returns
`DelegatedAllocationInputV2` only for an explicitly prepared V3 epoch. Historical
event resolution remains available for release during drain/retirement. Reject
downgrade while delegated execution or unreleased delegated intents exist.

- [ ] Start with a migrated PostgreSQL integration failure proving new B admission
  under active authority keeps A/base/configuration/execution digests unchanged.

```python
before = await store.execution_authority(session)
result = await membership.apply(session, bob_create, actor=delegate, idempotency_key=key)
assert result.revision == 1
assert await store.execution_authority(session) == before
value = await store.load_allocation_input(session, writer)
assert canonical_bytes(value.configuration) == base_configuration_bytes
assert value.membership.members[0].owner_id == bob_owner_id
```

- [ ] Add exact replay/conflict, wrong actor/fence/revision, base-subject takeover,
  tampered materialization, quota, update/destroy, recreation rejection and retained-history tests.
  Test SQL immutable-log guards directly; run old V2 active-mutation rejection.
- [ ] Implement against those tests; prove allocation CAS rejects an input computed
  before a concurrent membership mutation. Use separate SERIALIZABLE sessions,
  not a mock lock. Record RED/GREEN and migration upgrade/downgrade outcomes.
- [ ] Run focused membership plus existing epoch/projection/reconciler integration
  and unit tests; Ruff/diff checks; commit exact task files.

## Task 3: Authenticated recreation after predecessor release

**Files:**
- Extend `membership_contracts.py`, `membership.py`, `membership_store.py`.
- Extend `tests/unit/test_capacity_membership.py` and
  `tests/integration/test_capacity_membership.py`.

Add `PersonalReincarnationEvidenceV1` and an optional `reincarnation` field to
`PersonalApplicationMemberV1`. Evidence binds namespace and execution-manifest
digest, origin `ConfigurationGenerationRefV1`, disabled predecessor
`SubjectConfigurationV1`, predecessor membership revision/head, admission revision,
successor incarnation and release-set digest. UUIDs/digests must be nonzero;
subject scope/ID must agree throughout; predecessor must be disabled with zero
min/max; successor must differ from predecessor; admission revision must exceed
predecessor revision and not exceed the containing member revision. Generation
must increase. Evidence is generated by the store, not a mutation request field.

For `create` with an existing disabled member, preserve owner/name/subject ID,
require fresh globally unused incarnation/reporter/token, configuration generation
greater than predecessor and candidate/deployment generation 1. Check all epochs'
exact predecessor identity intents and legacy reservations under authority-first
SERIALIZABLE locking. Reject unless all have reached their existing released
state and durable release evidence is valid; do not release them here. Canonically
hash IDs, identity/binding digests and existing release witnesses (including the
valid no-intents case), persist that certificate in the append-only event, and
retain all old candidate/deployment/reporter/acknowledgement history.

Carry the latest recreation evidence through ordinary updates. For a managed
base subject the origin reference must equal its immutable base reference;
otherwise origin must equal the first admitted configuration. Repeated recreation
uses the immediate prior disabled event and preserves that origin. The loader
verifies the immutable chain and certificate against durable records; the pure
resolver checks bindings and permits incarnation changes only with that evidence.
Owner/name retention remains mandatory even with evidence. Do not regard a
Pydantic-valid caller-made certificate as authenticated.

- [ ] Add failing unit tests for missing/wrong origin, namespace, manifest,
  predecessor, revision or successor evidence, and owner/name substitution.
- [ ] Add failing PostgreSQL tests: create → disable → recreate produces the
  same subject ID with a fresh incarnation and deployment 1; old exact lookup
  still returns only old evidence; repeated recreation preserves origin.
- [ ] Prove pending, proposed, unknown, quarantined and terminal-but-unreleased
  predecessor work each blocks recreation; valid released and empty sets pass.
  Race disable/recreate against stale allocation/admission in separate sessions;
  one must fence/retry, never acquire new predecessor work after release proof.
- [ ] Implement; run covering unit/integration tests, Ruff and diff checks;
  record RED/GREEN and commit only source/test files. Review this transition
  before proceeding to Task 4. No lifecycle runtime readiness claim.

## Task 4: Authenticated lifecycle and executor end-to-end admission

**Files:**
- Modify `src/loom_capacity_manager/api.py`, `auth.py`, `execution_store.py`,
  and `store.py`'s active-demand binding validator.
- Extend `tests/integration/test_capacity_membership.py` and
  `tests/integration/test_capacity_manager_api.py`.
- Update `docs/architecture/personal-dev-active-membership.md` with exact delivered scope.

Expose `/v3/execution-preparations` for the V3 contract and a separate
`/v1/personal-memberships/{subject_id}` endpoint for the mutation contract. Add
single-purpose unbound scope `capacity:membership:manage`; require the exact
prepared principal in the store as well. Preserve the old endpoint's shadow-only
behavior. Return an authenticated membership checkpoint for management callers
containing exact execution/delegation/revision. This plan delivers the manager
interface, not a silent adaptation of the old lifecycle client's epoch semantics.

Also expose `GET /v1/personal-memberships/checkpoint` to the exact prepared
membership principal, returning a strict `PersonalMembershipCheckpointV1` with
`execution: ExecutionAuthorityV2`, `namespace_id`, `revision`, and `head_sha256`.
Read these fields in one consistent transaction; V2/prepared/retired epochs do not
pretend to have active membership. Mutation responses wrap the original store
result with this same checkpoint shape, using the request's execution/namespace
and that result's revision/head, not a later concurrent head. Keep revision
conflicts distinguishable from identity, idempotency and execution fencing so
the lifecycle client refreshes only a stale revision within the same authority.
Introduce typed conflict exceptions at this integration boundary if Task 2 uses
the existing store conflict classes; do not classify by parsing error messages.

Replace the executor's prepared-only acknowledgement lookups with the shared
exact-generation resolver in bootstrap and protected admission. Every increase
revalidates current subject lifecycle/generation and membership materialization;
release may authenticate immutable historical bindings without reopening admission.
Pending/rate/account limits continue to use the same charged ledger and canonical
owner account. Unknown and terminal-but-unreleased allocations stay charged.
The active demand-report validator in `store.py` must use that same strict V2/V3
manifest parser and exact-generation resolver: a delegated owner cannot reach the
allocator if its reports are still restricted to prepared base acknowledgements.
Preserve its candidate/publication/reporter checks and reject disabled or stale
current membership before recording a demand snapshot.

- [ ] Test unauthenticated/wrong-scope/wrong-principal/subject-path substitution
  requests fail without log or reporter writes; valid delegate reaches real store.
- [ ] Run B's demand through real allocation commit, pool proposal, bootstrap and
  protected admission with A already live. Then update/destroy B: stale B launch
  is rejected while A remains admissible, and B charges persist through cleanup.

```python
await admit_and_publish_bob_demand()
result = await reconcile_shadow_once(session_factory, store, writer)
assert result.status == 'committed'
work = await execution_store.next_pool_work(session, gb10_executor)
assert work.subject_id == bob_subject_id
assert work.account_id == f'dev-owner-{bob_owner_id.hex}'
```

- [ ] Verify shared V2 behavior and personal V3 behavior remain distinct, without
  silent fallback. Preserve the old lifecycle client's explicit rejection until
  its durable checkpoint schema is extended in the following lifecycle task.
- [ ] Run affected unit/integration suites and review the full branch. Correct
  findings, rerun covering tests, then push/create PR under normal protected CI.
  No live readiness claim: typed build execution and concurrent-owner fleet
  acceptance remain required by the architecture spec.

## Following lifecycle task (separate dependent plan)

`src/loom/personal_dev_capacity.py`, `personal_dev_reconciler.py`,
`personal_dev_environment_store.py` and `src/loom/db/schema.py` currently require
projection to advance the configuration epoch by exactly one. A new versioned
membership checkpoint and environment migration must retain execution namespace,
expected/result membership revision and digest instead; never store a membership
revision in the configuration-epoch column. The protected runtime acknowledgement
must bind the execution manifest and operation lease before that checkpoint is
submitted. Extend `tests/unit/test_personal_dev_capacity.py` and
`tests/integration/test_personal_dev_capacity_runtime.py` for exact retries and
supersession. This is a known required integration, not covered by this plan's
manager-side acceptance or an environment-readiness claim.
