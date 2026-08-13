# Executable Global Capacity Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and verify the inert-but-real Package 5B path in which one
global manager can authorize exact capacity and one executor per Slurm pool can
apply it, without activating live capacity.

**Architecture:** Preserve the permanent dry-run v1 protocol and introduce a
separate executable v2 protocol guarded by a durable execution epoch, manifest,
writer fence, and finite ceiling. The manager remains the sole allocator;
trusted environment agents own protected task admission; OLDLAB and GB10 use
the same journal-first executor implementation with distinct controller-local
credentials and state.

**Tech Stack:** Python 3.11, Pydantic v2, FastAPI, SQLAlchemy 2, PostgreSQL 17,
Alembic, asyncio subprocesses, Ed25519, systemd, pytest, Hypothesis, Ruff, and
MyPy.

## Global Constraints

- `DryRun*V1` top-level contracts remain permanently
  `executable: Literal[False]`; no Boolean widening or conversion adapter is
  permitted.
- Executable v2 operations bind authority, writer, configuration, allocation,
  execution, pool, executor, subject, candidate, deployment, profile, trusted
  release, shape, resource, and stable operation identities.
- The checked-in and rendered executable-new-capacity ceiling remains exactly
  `0`; Package 5B performs no live `sbatch`, `scancel`, worker signal, registry
  push, Kubernetes apply, or systemd activation.
- `loom-dev` is shared infrastructure, personal applications use
  `loom-dev-<owner>`, and `loom-dev-shared` remains absent.
- Subject `min_slots` is configurable and defaults to `0`. Users do not
  configure pool weights, Slurm QoS, worker shapes, resource profiles, or
  priority tiers.
- The positive executable ceiling is a required finite operator policy bound
  into one immutable execution manifest; it has no positive runtime deployment
  or renderer default and cannot exceed the summed configured OLDLAB/GB10 slots.
- Architecture-specific demand is a hard eligibility constraint;
  architecture-neutral placement is deterministic and manager-owned.
- Configuration and candidate identity change only in shadow state. Exact
  monotonic demand and pool facts may change under the same active immutable
  configuration; prepared and drain-only states reject them.
- Returning from drain-only to shadow requires every executable intent
  released plus fresh, complete, exact final evidence from both executors;
  retained or ambiguous Loom work blocks retirement and remains charged.
- Foreign or ambiguous Slurm work is never mutated and never inferred to be
  released.
- Normal reclamation is drain-first and never terminates an active trial.
- Every filesystem secret and journal is a regular nonsymlink, owner-only file;
  subprocess execution never uses a shell.
- Existing changes outside this branch, especially the main checkout's
  `.codex/` directory, remain untouched.

## File Structure

New focused modules:

- `src/loom_capacity_manager/executable_contracts.py`: executable v2 wire
  contracts and tagged candidate identity.
- `src/loom_capacity_manager/execution_store.py`: executable reservation and
  work-queue transitions; authority epoch preparation remains in `store.py`.
- `src/loom_capacity_executor/slurm_contracts.py`: bounded scheduler requests
  and normalized observations.
- `src/loom_capacity_executor/slurm_backend.py`: argv-only Slurm observation,
  submission, and conditional pending cancellation.
- `src/loom_capacity_executor/launch_renderer.py`: trusted profile-to-Slurm
  rendering and signed ownership metadata.
- `src/loom_capacity_executor/executable.py`: journal-first executor protocol,
  recovery, and reconciliation.
- `src/loom_capacity_executor/config.py` and `__main__.py`: owner-only runtime
  configuration and inert-first daemon entry point.
- `src/loom_capacity_agent/executable_admission.py`: protected executable
  preparation, binding, drain, and release operations.
- `src/loom_control_plane/global_execution_fence.py`: reciprocal legacy-writer
  scale-up fence.

`store.py` also owns explicit operator drain and retirement compare-and-set
transitions. `execution_store.py` derives cryptographically checked per-pool
retirement safety from complete inventories; it does not make configuration
or owner-policy decisions.

Existing `grant_contracts.py` and `grant_store.py` stay dry-run-only. Shared
read-only helpers may be extracted, but executable mutation does not enter the
v1 store. The allocator gains a separately named promotion function rather
than changing `compute_shadow_epoch()` semantics.

---

### Task 1: Tagged Candidate and Executable v2 Contracts

**Files:**

- Create: `src/loom_capacity_manager/executable_contracts.py`
- Modify: `src/loom_capacity_manager/__init__.py`
- Test: `tests/unit/test_capacity_manager_executable_contracts.py`

**Interfaces:** Produces `CandidateBindingV2`, `ExecutionAuthorityV2`,
`ExecutionFenceV2`, and distinct executable proposal, acceptance, bootstrap, permit, consumption, close,
release, registration, heartbeat, and inventory contracts. Consumes bounded
value objects from `contracts.py` and non-executable nested shapes from
`grant_contracts.py`.

- [ ] **Step 1: Write identity and protocol-separation tests**

```python
def test_personal_identity_is_not_translated() -> None:
    binding = CandidateBindingV2(
        algorithm="source-sha256",
        identity="a" * 64,
        publication_sha256="b" * 64,
    )
    assert binding.identity == "a" * 64


def test_git_identity_rejects_source_length() -> None:
    with pytest.raises(ValidationError):
        CandidateBindingV2(
            algorithm="git-sha1",
            identity="a" * 64,
            publication_sha256="b" * 64,
        )


def test_v1_cannot_parse_v2_proposal() -> None:
    with pytest.raises(ValidationError):
        DryRunReservationProposalV1.model_validate(
            executable_proposal_fixture().model_dump(mode="json")
        )
```

- [ ] **Step 2: Prove the tests fail before implementation**

Run: `uv run --no-sync pytest -q tests/unit/test_capacity_manager_executable_contracts.py`

Expected: collection fails because `executable_contracts` does not exist.

- [ ] **Step 3: Implement the tagged identity and shared fence**

```python
class CandidateBindingV2(StrictV1Model):
    algorithm: Literal["git-sha1", "source-sha256"]
    identity: Annotated[str, Field(min_length=40, max_length=64)]
    publication_sha256: Digest

    @model_validator(mode="after")
    def _exact_length(self) -> "CandidateBindingV2":
        expected = 40 if self.algorithm == "git-sha1" else 64
        if len(self.identity) != expected or _LOWER_HEX.fullmatch(self.identity) is None:
            raise ValueError("candidate identity does not match its algorithm")
        return self


class ExecutionAuthorityV2(StrictV1Model):
    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
    configuration_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    executable_new_capacity_ceiling: Quantity
    trusted_fleet_release_sha256: Digest


class ExecutionFenceV2(ExecutionAuthorityV2):
    allocation_epoch: PositiveQuantity
```

- [ ] **Step 4: Implement every executable top-level contract**

Each class has `schema_version: Literal[2] = 2` and
`executable: Literal[True] = True`. Validators compare all nested shape,
ownership, candidate, subject, pool, executor, epoch, expiry, launch-rank, and
canonical-digest fields. No executable class inherits a dry-run top-level
class.

- [ ] **Step 5: Run new and permanent dry-run contract tests**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/unit/test_capacity_executor_dry_run.py \
  tests/unit/test_capacity_executor_remote.py
```

Expected: all pass; every v1 document still serializes
`"executable": false`.

- [ ] **Step 6: Commit**

```bash
git add src/loom_capacity_manager/executable_contracts.py \
  src/loom_capacity_manager/__init__.py \
  tests/unit/test_capacity_manager_executable_contracts.py
git commit -m "feat(capacity): define executable bridge contracts"
```

---

### Task 2: Durable Execution Epoch and Zero-Ceiling Migration

**Files:**

- Create: `capacity_migrations/versions/capacity_0004_executable_bridge.py`
- Modify: `src/loom_capacity_manager/models.py`
- Modify: `src/loom_capacity_manager/contracts.py`
- Modify: `src/loom_capacity_manager/store.py`
- Test: `tests/integration/test_capacity_manager_migrate.py`
- Test: `tests/integration/test_capacity_manager_api.py`
- Create: `tests/unit/test_capacity_manager_execution_epoch.py`

**Interfaces:** Produces `ExecutionPreparationV2`, `ExecutionActivationV2`,
`CapacityExecutionEpoch`, `CapacityStore.prepare_execution_epoch()`,
`activate_execution_epoch()`, and `execution_authority()`.

- [ ] **Step 1: Write Package 5A upgrade and nonzero rejection tests**

```python
async def test_capacity_0004_upgrades_at_zero(postgres_url: str) -> None:
    await migrate_to(postgres_url, "capacity_0003")
    await migrate_to(postgres_url, "capacity_0004")
    row = await fetch_authority(postgres_url)
    assert (row["execution_epoch"], row["execution_state"]) == (0, "shadow")
    assert row["executable_new_capacity_ceiling"] == 0


async def test_database_rejects_unprepared_nonzero_ceiling(postgres_url: str) -> None:
    with pytest.raises(IntegrityError):
        await set_authority_ceiling(postgres_url, 1)
```

- [ ] **Step 2: Run and confirm the missing revision failure**

Run: `uv run --no-sync pytest -q tests/integration/test_capacity_manager_migrate.py -k '0004 or unprepared_nonzero'`

Expected: fails because `capacity_0004` is absent.

- [ ] **Step 3: Add the execution epoch schema**

Add immutable execution epoch, state (`prepared`, `active`, `drain-only`,
`retired`), configuration/fleet, manifest, OLDLAB/GB10 executor,
environment-acknowledgement, legacy-writer, rollback, ceiling, actor,
idempotency, and database-time fields. Replace the authority zero-only check
with a constraint that permits a positive ceiling only for an exact active or
drain-only epoch and requires zero for shadow or prepared states.

- [ ] **Step 4: Implement prepare and activate compare-and-set methods**

```python
async def prepare_execution_epoch(
    self,
    session: AsyncSession,
    request: ExecutionPreparationV2,
    *,
    actor: str,
    idempotency_key: UUID,
) -> None:
    """Persist an immutable prepared epoch that remains non-executable."""


async def activate_execution_epoch(
    self,
    session: AsyncSession,
    request: ExecutionActivationV2,
    *,
    actor: str,
    idempotency_key: UUID,
) -> ExecutionAuthorityV2:
    """Activate only the exact prepared epoch under the current writer lock."""
```

Exact replay converges; any changed field conflicts. Activation rechecks
configuration, fleet, both executors, all subject acknowledgements, complete
legacy writer manifest, rollback digest, `increase_freeze=true`, and writer
ownership.

- [ ] **Step 5: Keep Package 5B publicly inert**

Do not add a renderable ceiling or activation CLI/API route. Health/status may
report execution state and epoch, but Package 5B readiness remains ceiling-zero
only.

- [ ] **Step 6: Run migration/store/API tests and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_manager_api.py
```

Then:

```bash
git add capacity_migrations/versions/capacity_0004_executable_bridge.py \
  src/loom_capacity_manager/models.py src/loom_capacity_manager/contracts.py \
  src/loom_capacity_manager/store.py \
  tests/unit/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_manager_api.py
git commit -m "feat(capacity): fence executable epochs at zero"
```

---

### Task 3: Executable Allocation Promotion

**Files:**

- Modify: `capacity_migrations/versions/capacity_0004_executable_bridge.py`
- Modify: `src/loom_capacity_manager/models.py`
- Modify: `src/loom_capacity_manager/allocator.py`
- Modify: `src/loom_capacity_manager/reconciler.py`
- Create: `tests/unit/test_capacity_manager_executable_allocator.py`
- Modify: `tests/integration/test_capacity_manager_api.py`

**Interfaces:** Produces `ExecutableEpochV2` and `promote_shadow_epoch()` while
preserving `compute_shadow_epoch()` and its permanent false flag.

- [ ] **Step 1: Write fence and plan-equivalence tests**

```python
def test_promotion_requires_active_authority() -> None:
    with pytest.raises(ExecutableAllocationError, match="active execution authority"):
        promote_shadow_epoch(shadow_epoch_fixture(), None, allocation_epoch=7)


def test_promotion_preserves_exact_placement() -> None:
    shadow = shadow_epoch_fixture()
    result = promote_shadow_epoch(shadow, execution_authority_fixture(), allocation_epoch=7)
    assert result.allocations == shadow.allocations
    assert result.input_digest == shadow.input_digest
    assert result.executable is True
```

- [ ] **Step 2: Run and prove the promotion is absent**

Run: `uv run --no-sync pytest -q tests/unit/test_capacity_manager_executable_allocator.py`

Expected: import failure for `ExecutableEpochV2`.

- [ ] **Step 3: Add explicit executable allocation database mode**

Executable allocation rows require non-null execution epoch/manifest and
`mode='executable'`; shadow rows require those fields null and remain
`mode='shadow', executable=false`.

- [ ] **Step 4: Implement pure exact promotion**

```python
def promote_shadow_epoch(
    shadow: ShadowEpochV1,
    authority: ExecutionAuthorityV2 | None,
    *,
    allocation_epoch: int,
) -> ExecutableEpochV2:
    if authority is None:
        raise ExecutableAllocationError("active execution authority is required")
    if shadow.configuration.configuration_epoch != authority.configuration_epoch:
        raise ExecutableAllocationError("configuration epoch changed")
    return ExecutableEpochV2.from_shadow(shadow, authority, allocation_epoch)
```

The reconciler computes once and commits an executable representation only in
the same serializable transaction in which writer, configuration, input digest,
and execution fence are revalidated. It never upgrades an older stored shadow
row.

- [ ] **Step 5: Cover two owners, x86, ARM, neutral, minimum zero, and
scale-to-zero**

Use one cohort containing all priority tiers, shared development,
`dev-alice`, and `dev-bob`; assert exact shadow/executable placement parity and
the execution ceiling.

- [ ] **Step 6: Run and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_executable_allocator.py \
  tests/ops/test_global_fleet_capacity_shadow_once.py \
  tests/integration/test_capacity_manager_api.py -k 'allocation or reconcile'
```

Then commit the six listed files with message
`feat(capacity): promote fenced executable allocations`.

---

### Task 4: Executable Reservation Store and Pool Work Queue

**Files:**

- Create: `src/loom_capacity_manager/execution_store.py`
- Modify: `src/loom_capacity_manager/models.py`
- Modify: `src/loom_capacity_manager/api.py`
- Modify: `src/loom_capacity_manager/auth.py`
- Create: `tests/integration/test_capacity_manager_execution_store.py`
- Modify: `tests/integration/test_capacity_manager_api.py`

**Interfaces:** Produces `CapacityExecutionStore.next_pool_work()`, acceptance,
bootstrap, permit issue/consumption, close, protected release, and physical
release transitions plus strict `/v2/executors/{pool_id}/...` routes.

- [ ] **Step 1: Write transactional fence and pool-isolation tests**

```python
async def test_permit_consumption_rechecks_execution_fence(
    execution_store: CapacityExecutionStore,
    session: AsyncSession,
) -> None:
    permit = await prepare_launch_ready_intent(execution_store, session)
    await freeze_execution(session)
    with pytest.raises(ExecutionConflictError, match="execution fence"):
        await execution_store.consume_launch_permit(session, consume(permit))


async def test_queue_never_crosses_pool() -> None:
    work = await execution_store.next_pool_work(session, gb10_binding())
    assert work is None or work.pool_id == "gb10"
```

- [ ] **Step 2: Run and confirm the store is absent**

Run: `uv run --no-sync pytest -q tests/integration/test_capacity_manager_execution_store.py`

Expected: collection fails for `CapacityExecutionStore`.

- [ ] **Step 3: Implement one shared locked execution guard**

```python
async def _locked_execution_context(
    session: AsyncSession,
    fence: ExecutionFenceV2,
    executor: ExecutorBinding,
) -> LockedExecutionContext:
    authority = await lock_authority(session)
    if not authority.matches(fence) or authority.execution_state != "active":
        raise ExecutionConflictError("execution fence changed")
    registered = await lock_exact_executor(session, executor)
    if registered.lease_expires_at <= await database_now(session):
        raise ExecutionConflictError("executor lease expired")
    return LockedExecutionContext(authority=authority, executor=registered)
```

Every increase transition additionally rechecks fresh complete inventory,
headroom, exact launch rank, pending/rate limits, subject lifecycle, candidate,
profile, and trusted release. Release additionally requires matching protected
and physical terminal evidence.

- [ ] **Step 4: Implement one-command bounded pool work selection**

Return at most one canonical proposal, bootstrap, permit, close, or release
command in central sequence order. At ceiling zero, return no capacity-increase
work but continue exact drain, close, inventory, and release work for retained
commitments. A stale executor or unresolved earlier command blocks every later
command.

- [ ] **Step 5: Add strict API routes and exact receipt validation**

Reuse bounded body parsing, mTLS actor checks, pool-bound bearer principals,
idempotency handling, and transaction error mapping. Do not expose an operator
activation route.

- [ ] **Step 6: Run and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/integration/test_capacity_manager_execution_store.py \
  tests/integration/test_capacity_manager_api.py \
  tests/unit/test_capacity_executor_dry_run.py
```

Commit the six listed files with message
`feat(capacity): add fenced executable work queue`.

---

### Task 5: Protected Executable Admission and Bootstrap

**Files:**

- Create: `src/loom_capacity_agent/executable_admission.py`
- Create: `capacity_guard_migrations/versions/guard_0011_executable_admission.py`
- Modify: `src/loom_capacity_agent/admission.py`
- Modify: `src/loom_capacity_agent/prepared_store.py`
- Modify: `src/loom_capacity_agent/claim_guard.py`
- Modify: `src/loom_capacity_agent/runtime.py`
- Create: `src/loom_capacity_executor/admission_client.py`
- Create: `tests/integration/test_capacity_agent_executable_admission.py`
- Modify: `tests/integration/test_capacity_agent_migrations.py`
- Create: `tests/unit/test_capacity_executor_admission_client.py`
- Modify: `tests/unit/test_capacity_agent_claim_guard.py`

**Interfaces:** Produces `ExecutableAdmissionStore.prepare_worker()`,
`bind_slurm_job()`, `register_worker()`, `begin_drain()`, and
`acknowledge_release()` plus exact protected receipts. Produces
`DatabaseExecutableAdmissionClient.from_database_url_file()` for an
environment-scoped executor binding. Consumes executable intent, tagged
candidate, protected attempt state, and bootstrap digest.

- [ ] **Step 1: Write privilege, order, and drain tests**

```python
async def test_candidate_role_cannot_prepare_worker() -> None:
    with pytest.raises(InsufficientPrivilege):
        await call_as_candidate_role("loom_capacity_guard.prepare_executable_worker")


async def test_draining_worker_cannot_claim_replacement(
    admission_store: ExecutableAdmissionStore,
) -> None:
    worker = await prepared_bound_worker(admission_store)
    await admission_store.begin_drain(worker.drain_request())
    assert await attempt_claim(worker.worker_id) is None
```

- [ ] **Step 2: Run and verify executable admission is absent**

Run: `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py`

Expected: collection fails for `ExecutableAdmissionStore`.

- [ ] **Step 3: Add `guard_0011` append-only protected records**

Records bind execution fence, subject/incarnation, candidate, deployment,
pool/profile/shape/resources, intent, bootstrap epoch, physical job, worker
incarnation, claim high-water, drain epoch, and release epoch. Candidate roles
receive no write privilege on their tables or functions.

- [ ] **Step 4: Implement prepare-bind-register ordering**

```python
async def prepare_worker(
    self,
    request: ExecutableBootstrapRegistrationV2,
    *,
    bootstrap_sha256: str,
) -> PreparedExecutableAdmissionV2:
    """Seal one unused bootstrap digest before scheduler submission."""


async def bind_slurm_job(
    self,
    request: PhysicalJobBindingV2,
) -> BoundExecutableWorkerV2:
    """Bind the exact returned or adopted Slurm job before exchange."""
```

The trusted wrapper exchanges the clear capability once, only after the
physical binding exists. Registration revokes it and creates one worker
credential; requeue advances worker incarnation and revokes the predecessor.

- [ ] **Step 5: Implement the environment-scoped database client**

`DatabaseExecutableAdmissionClient.from_database_url_file()` applies the same
regular-file, owner, mode-0600, bounded URL, timeout, and TLS checks as other
secret clients. Its database role can execute only the `guard_0011` procedures
for the exact subject/incarnation. It cannot select candidate tables, mint
grants, or administer the database. Every response binds the full request
digest and protected high-water.

- [ ] **Step 6: Implement monotonic drain and protected release**

`begin_drain()` stops new claims without altering live attempts.
`acknowledge_release()` requires zero live protected claims, revoked bootstrap
and worker credentials, and a newer protected registration epoch. The
append-only release fence rejects every delayed registration.

- [ ] **Step 7: Run and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/integration/test_capacity_agent_executable_admission.py \
  tests/integration/test_capacity_agent_migrations.py \
  tests/integration/test_capacity_agent_store.py \
  tests/unit/test_capacity_agent_claim_guard.py \
  tests/unit/test_capacity_agent_runtime.py \
  tests/unit/test_capacity_executor_admission_client.py
```

Commit the eleven listed files with message
`feat(capacity): protect executable worker admission`.

---

### Task 6: Typed Slurm Backend and Authority Validation

**Files:**

- Create: `src/loom_capacity_executor/slurm_contracts.py`
- Create: `src/loom_capacity_executor/slurm_backend.py`
- Create: `tests/unit/test_capacity_executor_slurm_backend.py`
- Create: `tests/support/fake_slurm.py`

**Interfaces:** Produces `SlurmAuthorityV2`, `SlurmLaunchRequestV2`,
`SlurmJobObservationV2`, `SlurmTerminalEvidenceV2`, and `AsyncSlurmBackend`
methods `validate_authority()`, `inventory()`, `submit()`, `cancel_pending()`,
and `accounting_high_water()`. It consumes typed scheduler values, never
manager allocation objects.

- [ ] **Step 1: Write argv, authority, bounds, and cancellation tests**

```python
async def test_submit_uses_argv_without_shell(fake_slurm: FakeSlurm) -> None:
    result = await fake_slurm.backend().submit(slurm_launch_request_fixture())
    assert result.job_id == "101"
    assert fake_slurm.calls[0].shell is False


async def test_cancel_pending_rechecks_state(fake_slurm: FakeSlurm) -> None:
    fake_slurm.set_job_state("101", "RUNNING")
    with pytest.raises(SlurmStateConflictError):
        await fake_slurm.backend().cancel_pending(cancel_request("101"))
    assert fake_slurm.scancel_calls == []
```

- [ ] **Step 2: Run and confirm the backend is absent**

Run: `uv run --no-sync pytest -q tests/unit/test_capacity_executor_slurm_backend.py`

Expected: missing `slurm_backend` module.

- [ ] **Step 3: Implement strict scheduler value objects**

Bound identifiers, paths, resources, timeouts, output sizes, node sets,
partitions, clusters, associations, TRES values, and states. Candidate strings
and scripts are not fields of `SlurmLaunchRequestV2`.

- [ ] **Step 4: Implement bounded argv-only subprocesses**

```python
process = await asyncio.create_subprocess_exec(
    *argv,
    stdin=asyncio.subprocess.DEVNULL,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    env=trusted_environment,
    start_new_session=True,
)
stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
```

Validate executable identities and controller/cluster facts at startup. Parse
fixed-field output and reject oversized, missing, duplicate, unknown, or
malformed records.

- [ ] **Step 5: Reobserve before conditional pending cancellation**

Require exact cluster/job/submitter/account and a pending state immediately
before `scancel`. If the predicate cannot be proved, return conflict without a
mutation.

- [ ] **Step 6: Run under ambient umask and commit**

Run:

```bash
umask 0002 && uv run --no-sync pytest -q \
  tests/unit/test_capacity_executor_slurm_backend.py
```

Expected: all pass and evidence files are mode `0600`. Commit the four files
with message `feat(capacity): add bounded Slurm executor backend`.

---

### Task 7: Trusted Launch Rendering and Ownership Proof

**Files:**

- Create: `src/loom_capacity_executor/launch_renderer.py`
- Modify: `src/loom_capacity_manager/ownership.py`
- Modify: `src/loom_capacity_executor/keys.py`
- Create: `tests/unit/test_capacity_executor_launch_renderer.py`
- Modify: `tests/unit/test_capacity_executor_keys.py`

**Interfaces:** Produces `render_launch_request()`,
`sign_executable_ownership()`, and `verify_executable_ownership()`. Consumes
only executable intent, operator profile, controller authority, trusted
launcher digest, and controller-local Ed25519 key.

- [ ] **Step 1: Write exact resource and hostile-input tests**

```python
def test_render_round_trips_resources() -> None:
    request = render_launch_request(launch_context_fixture())
    assert request.cpus == 16
    assert request.memory_bytes == 68_719_476_736
    assert request.nodes == ("oldlab-5",)


@pytest.mark.parametrize("value", ["$(id)", "a;scancel 1", "a\n--uid=root"])
def test_candidate_text_never_enters_argv(value: str) -> None:
    request = render_launch_request(launch_context_fixture(candidate_diagnostic=value))
    assert all(value not in argument for argument in request.argv())
```

- [ ] **Step 2: Run and confirm the renderer is absent**

Run: `uv run --no-sync pytest -q tests/unit/test_capacity_executor_launch_renderer.py`

Expected: missing `launch_renderer` module.

- [ ] **Step 3: Render only operator-owned profile fields**

Map exact controller, partition, association, QoS, features, nodes/domains,
CPUs, memory, GPUs, time, trusted wrapper, image digest, and stable operation
identity. Display names and candidate values become bounded diagnostic digests,
not argv or paths.

- [ ] **Step 4: Sign complete executable ownership metadata**

Canonical metadata binds execution fence, pool/controller, executor,
subject/incarnation, candidate, deployment, tranche/intent, shape/profile,
resources, submitter, and trusted release. Reject every changed field and
unregistered key; retain verification keys while owned work is nonterminal.

- [ ] **Step 5: Run and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_executor_launch_renderer.py \
  tests/unit/test_capacity_executor_keys.py
```

Commit the five files with message
`feat(capacity): render signed trusted worker launches`.

---

### Task 8: Journal-First Executable Pool Executor and Recovery

**Files:**

- Create: `src/loom_capacity_executor/executable.py`
- Modify: `src/loom_capacity_executor/journal.py`
- Modify: `src/loom_capacity_executor/client.py`
- Create: `tests/unit/test_capacity_executor_executable.py`
- Create: `tests/unit/test_capacity_executor_recovery.py`

**Interfaces:** Produces `ExecutablePoolExecutor.tick()` and `recover()`.
Consumes the manager client, protected admission client, Slurm backend, trusted
renderer, exact executor binding, and local journal.

- [ ] **Step 1: Write crash/no-resubmit and journal-regression tests**

```python
async def test_crash_after_submit_never_resubmits(
    executor_harness: ExecutableExecutorHarness,
) -> None:
    executor_harness.crash_after("slurm-submit")
    with pytest.raises(SimulatedCrash):
        await executor_harness.executor.tick()
    await executor_harness.restart().executor.recover()
    assert executor_harness.slurm.submit_count == 1


async def test_empty_replacement_journal_fences(
    executor_harness: ExecutableExecutorHarness,
) -> None:
    executor_harness.replace_journal_with_empty_file()
    with pytest.raises(JournalRegressionError):
        await executor_harness.restart().executor.tick()
    assert executor_harness.slurm.mutations == []
```

- [ ] **Step 2: Run and verify the executor is absent**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_executor_executable.py \
  tests/unit/test_capacity_executor_recovery.py
```

Expected: collection fails for `ExecutablePoolExecutor`.

- [ ] **Step 3: Extend the client with exact v2 methods**

Add checkpoint, next work, acceptance, bootstrap, permit consumption, physical
binding, inventory, close, protected-release observation, and release methods.
Validate exact client binding and canonical receipt digest for every response.

- [ ] **Step 4: Implement one-operation journal-first ticks**

```python
async def tick(self) -> ExecutorTickResult:
    checkpoint = await self.client.executable_checkpoint()
    self.journal.assert_covers(checkpoint.journal_sequence, checkpoint.journal_digest)
    work = await self.client.next_executable_work(checkpoint.command_sequence)
    if work is None:
        return await self._publish_inventory()
    return await self._apply_one(work)
```

Fsync a request before every central or external transition. Confirm only an
exact validated result. Verified 4xx conflicts become rejected; timeout/5xx
records remain unresolved for exact replay or recovery.

- [ ] **Step 5: Implement recovery and conservative classification**

Scan the dedicated association and verify signed operation identity. One exact
match binds; absent, duplicate, foreign, or resource-mismatched results remain
quarantined and charged. Never resubmit a `submitting-unknown` intent.

- [ ] **Step 6: Implement drain/cancel/release ordering**

Require protected drain before conditional pending cancellation. Never signal
an active worker for ordinary reclamation. Release requires both protected and
physical terminal evidence.

- [ ] **Step 7: Run and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_executor_executable.py \
  tests/unit/test_capacity_executor_recovery.py \
  tests/unit/test_capacity_executor_journal.py \
  tests/unit/test_capacity_executor_remote.py
```

Commit the five files with message
`feat(capacity): execute fenced pool work journal-first`.

---

### Task 9: Inert Executor Daemon and Controller Configuration

**Files:**

- Create: `src/loom_capacity_executor/config.py`
- Create: `src/loom_capacity_executor/__main__.py`
- Create: `scripts/ops/global_fleet_pool_executor_once.py`
- Create: `tests/unit/test_capacity_executor_config.py`
- Create: `tests/ops/test_global_fleet_pool_executor_once.py`

**Interfaces:** Produces `PoolExecutorConfig.from_files()`,
`run_executor_once()`, and `python -m loom_capacity_executor`. Consumes
owner-only secret files, immutable pool manifest, manager client, journal,
Slurm backend, and execution fence.

- [ ] **Step 1: Write permission, lock, binding, and zero-ceiling tests**

```python
def test_group_readable_bearer_is_rejected(tmp_path: Path) -> None:
    files = executor_files(tmp_path)
    files.bearer.chmod(0o640)
    with pytest.raises(ExecutorConfigError, match="0600"):
        PoolExecutorConfig.from_files(files.config)


async def test_zero_ceiling_never_constructs_mutating_backend() -> None:
    result = await run_executor_once(zero_ceiling_harness())
    assert result.mode == "inventory-only"
    assert result.scheduler_mutations == 0
```

- [ ] **Step 2: Run and verify the configuration module is absent**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_executor_config.py \
  tests/ops/test_global_fleet_pool_executor_once.py
```

Expected: import failure for `PoolExecutorConfig`.

- [ ] **Step 3: Implement strict owner-only loading**

Require regular nonsymlink files, exact UID, mode `0600`, bounded one-line
JSON/TOML, absolute executable paths, exact controller/pool/executor/key
fingerprints, state directory mode `0700`, and a nonblocking singleton lock.

- [ ] **Step 4: Implement inert-first construction**

Always validate authority and publish inventory. Expose scale-up mutation only
after a current active `ExecutionAuthorityV2` with positive ceiling. A
drain-only zero-ceiling authority exposes only exact commands for already
adopted commitments. Shadow zero state exposes no mutation method. In
`--validate-only`, do not construct the mutating backend. Preserve unresolved
journal entries on signals.

- [ ] **Step 5: Test OLDLAB and GB10 independently**

Use different controller, partition, association, executor, key, and state
fixtures. Cross-loading either fixture fails before registration.

- [ ] **Step 6: Run and commit**

Run the two listed test files; commit the five files with message
`feat(capacity): add inert pool executor daemon`.

---

### Task 10: Reciprocal Legacy-Writer Coexistence Fence

**Files:**

- Create: `src/loom_control_plane/global_execution_fence.py`
- Modify: `src/loom_control_plane/global_dev_fleet_autoscaler.py`
- Modify: `src/loom_control_plane/worker_pool_autoscaler.py`
- Modify: `scripts/ops/global_dev_fleet_autoscaler_external_once.py`
- Modify: `scripts/ops/worker_pool_autoscaler_external_once.py`
- Create: `tests/unit/test_global_execution_fence.py`
- Modify: `tests/ops/test_global_dev_fleet_autoscaler_external_once.py`
- Modify: `tests/ops/test_worker_pool_autoscaler_external_once.py`

**Interfaces:** Produces `GlobalExecutionWitness`,
`load_global_execution_witness()`, and `assert_legacy_scale_up_allowed()`.
Consumes an authenticated bounded manager witness with authority, pool,
execution epoch/state, ceiling, expiry, and canonical digest.

- [ ] **Step 1: Write prepared/active/stale/missing refusal tests**

```python
@pytest.mark.parametrize("state", ["prepared", "active", "drain-only"])
def test_legacy_scale_up_refuses_global_state(state: str) -> None:
    with pytest.raises(GlobalExecutionFenceError):
        assert_legacy_scale_up_allowed(witness_fixture(state=state))


def test_required_missing_witness_fails_closed() -> None:
    with pytest.raises(GlobalExecutionFenceError, match="unavailable"):
        assert_legacy_scale_up_allowed(None, required=True)
```

- [ ] **Step 2: Run and confirm the fence is absent**

Run: `uv run --no-sync pytest -q tests/unit/test_global_execution_fence.py`

Expected: missing `global_execution_fence` module.

- [ ] **Step 3: Implement authenticated witness parsing**

Only fresh shadow state at ceiling zero permits legacy scale-up. Prepared,
active, drain-only, stale, equivocal, wrong-pool, wrong-authority, or missing
required evidence clamps scale-up to zero before policy or Slurm mutation.
Existing workers may follow the current drain-safe path.

- [ ] **Step 4: Fence both legacy entry points**

Check before development grant calculation and before environment-local Slurm
scale-up. Preserve current candidate/generation grant and foreign-workload
checks.

- [ ] **Step 5: Run and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_global_execution_fence.py \
  tests/ops/test_global_dev_fleet_autoscaler_external_once.py \
  tests/ops/test_worker_pool_autoscaler_external_once.py
```

Commit the eight files with message
`feat(capacity): fence legacy writers before global execution`.

---

### Task 11: Finite Authority Envelope and Active Immutable Fact Flow

**Files:**

- Modify: `src/loom_capacity_manager/executable_contracts.py`
- Modify: `src/loom_capacity_manager/models.py`
- Modify: `src/loom_capacity_manager/store.py`
- Modify: `capacity_migrations/versions/capacity_0004_executable_bridge.py`
- Modify: `tests/capacity_execution_fixtures.py`
- Modify: `tests/unit/test_capacity_manager_executable_contracts.py`
- Modify: `tests/integration/test_capacity_manager_execution_epoch.py`
- Modify: `tests/integration/test_capacity_manager_migrate.py`
- Modify: `tests/integration/test_capacity_management_store.py`

**Interfaces:** Changes
`ExecutionPreparationPolicyV2.executable_new_capacity_ceiling` from
`Literal[1]` to a required `PositiveQuantity`. Produces separate exact
shadow-mutation and active-fact authority guards. No CLI, renderer, deployment,
or API route can select a positive ceiling.

- [ ] **Step 1: Write finite ceiling and fleet-bound RED tests**

```python
def test_owner_policy_accepts_finite_two_slot_ceiling() -> None:
    payload = execution_policy().model_dump(mode="python")
    payload["executable_new_capacity_ceiling"] = 2
    assert (
        ExecutionPreparationPolicyV2.model_validate(payload)
        .executable_new_capacity_ceiling
        == 2
    )


async def test_preparation_rejects_ceiling_above_exact_fleet_slots(
    capacity_session: AsyncSession,
) -> None:
    policy = execution_policy(ceiling=17)
    fixture = await setup_execution(capacity_session, execution_policy=policy)
    with pytest.raises(ExecutionConflictError, match="configured fleet capacity"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.preparation.model_copy(update={"requested_ceiling": 17}),
            actor="activation-operator",
            idempotency_key=UUID(int=11901),
        )
```

The default fixture's configured two-pool `max_slots` sum is `16`; add a
test-only `ceiling: int = 1` builder argument so the request and policy stay
exactly bound. Also prove `0`, a negative integer, Boolean, float, and a value
above `MAX_QUANTITY` are rejected by the strict policy contract.

- [ ] **Step 2: Write active-fact versus identity-mutation RED tests**

Activate an exact ceiling-two epoch, then ingest the next demand sequence and
next OLDLAB/GB10 pool-observation sequences. Assert both succeed and a fresh
executable reconciliation can consume their new input digest. Under that same
active authority, assert fleet/subject proposals, configuration activation,
personal projection/redeploy/deletion, and reporter replacement fail before
state mutation. Assert prepared and drain-only states reject fact ingestion
even though their ceiling is zero.

The production break each test catches is explicit: replacing the current
ceiling-only `_lock_authority()` with an over-broad permissive lock would allow
identity mutation, while retaining it unchanged would block required active
facts.

- [ ] **Step 3: Run the focused RED gate**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_management_store.py \
  -k 'ceiling or active_fact or active_identity or prepared_fact or drain_only_fact'
```

Expected: ceiling `2` fails Pydantic validation and active fact ingestion raises
`AuthorityRecoveryError`; the tests must fail for those missing capabilities,
not from fixture setup.

- [ ] **Step 4: Implement the exact ceiling envelope**

Make the positive policy ceiling required and strictly bounded by
`PositiveQuantity`; do not give it a positive default. Change the execution
epoch ORM and migration quantity checks from `requested_ceiling = 1` to
`requested_ceiling > 0`. During preparation, lock the exact configuration's two
`CapacityPool` rows, require pool IDs `oldlab` and `gb10`, sum their `max_slots`
with checked finite arithmetic, and reject a requested/policy ceiling above
that sum. Preserve exact policy/preparation/activation equality and the active
authority foreign-key binding.

- [ ] **Step 5: Split authority guards without widening identity mutation**

Replace the ambiguous ceiling-only guard with:

```python
async def _lock_shadow_authority(session: AsyncSession) -> CapacityAuthorityState:
    """Require exact shadow state, epoch zero, no manifest, and ceiling zero."""


async def _lock_fact_authority(
    session: AsyncSession,
) -> tuple[CapacityAuthorityState, int | None]:
    """Return no active epoch in shadow, or the exact active configuration epoch."""
```

All configuration and personal-lifecycle mutation paths use the first guard.
Demand and pool ingestion call the second before their existing reporter lock.
The active branch locks and validates the exact execution epoch/manifest,
positive ceiling/rate, and current writer, then returns its immutable
configuration epoch. Before accepting the fact, ingestion requires the exact
subject or pool row in that configuration to match the reporter and report
generation bindings. Prepared, drain-only, retired/contradictory, or changed
reporter/pool/candidate/deployment evidence fails closed.

- [ ] **Step 6: Prove migration/model parity and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_management_store.py \
  tests/integration/test_capacity_manager_api.py
uv run --no-sync ruff format --check \
  src/loom_capacity_manager/executable_contracts.py \
  src/loom_capacity_manager/models.py src/loom_capacity_manager/store.py \
  tests/capacity_execution_fixtures.py \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_management_store.py
uv run --no-sync ruff check \
  src/loom_capacity_manager/executable_contracts.py \
  src/loom_capacity_manager/models.py src/loom_capacity_manager/store.py \
  tests/capacity_execution_fixtures.py \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_management_store.py
uv run --no-sync mypy \
  src/loom_capacity_manager/executable_contracts.py \
  src/loom_capacity_manager/models.py src/loom_capacity_manager/store.py
```

Commit the listed files with message
`feat(capacity): support finite executable authority envelopes`.

---

### Task 12: Explicit Drain, Safe Retirement, and Executable Heartbeats

**Files:**

- Modify: `src/loom_capacity_manager/executable_contracts.py`
- Modify: `src/loom_capacity_manager/models.py`
- Modify: `src/loom_capacity_manager/store.py`
- Modify: `src/loom_capacity_manager/execution_store.py`
- Modify: `src/loom_capacity_manager/api.py`
- Modify: `src/loom_capacity_executor/client.py`
- Modify: `capacity_migrations/versions/capacity_0004_executable_bridge.py`
- Modify: `tests/unit/test_capacity_manager_executable_contracts.py`
- Modify: `tests/integration/test_capacity_manager_execution_epoch.py`
- Modify: `tests/integration/test_capacity_manager_execution_store.py`
- Modify: `tests/integration/test_capacity_manager_migrate.py`
- Modify: `tests/integration/test_capacity_manager_api.py`
- Modify: `tests/unit/test_capacity_executor_remote.py`

**Interfaces:** Produces `ExecutionDrainV2`,
`ExecutionRetirementExecutorCheckpointV2`, `ExecutionRetirementV2`,
`CapacityManagementStore.begin_execution_drain()`,
`retire_execution_epoch()`, and
`ExecutableCapacityExecutorClient.heartbeat_executable_executor()`. The app
factory accepts an optional already-constructed `management_store` for an exact
pre-start owner policy, but exposes no prepare, activate, drain, retire, apply,
start, enable, or ceiling-change route.

- [ ] **Step 1: Write contract, transition, and exact replay RED tests**

The new exact contracts are:

```python
class ExecutionDrainV2(StrictV2Model):
    authority_incarnation: UUID
    expected_writer_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    expected_executable_new_capacity_ceiling: PositiveQuantity
    expected_executable_new_capacity_rate_per_minute: PositiveQuantity
    executable: Literal[True] = True


class ExecutionRetirementExecutorCheckpointV2(StrictV2Model):
    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Literal["gb10", "oldlab"]
    pool_generation: PositiveQuantity
    heartbeat_sequence: PositiveQuantity
    command_sequence: Quantity
    journal_sequence: Quantity
    journal_digest: Digest
    inventory_sequence: PositiveQuantity
    inventory_digest: Digest


class ExecutionRetirementV2(StrictV2Model):
    authority_incarnation: UUID
    expected_writer_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    executor_checkpoints: Annotated[
        tuple[ExecutionRetirementExecutorCheckpointV2, ...],
        Field(min_length=2, max_length=2),
    ]
    executable: Literal[True] = True
```

The checkpoint validator requires exactly one canonical `gb10` and one
canonical `oldlab` entry. The store returns a frozen
`RetiredExecutionEpoch(execution_epoch, execution_manifest_sha256, retired_at,
replayed)` value after retirement.

```python
async def test_operator_drain_then_safe_retirement_returns_shadow(
    activated_execution: ActivatedExecutionFixture,
) -> None:
    drained = await activated_execution.store.begin_execution_drain(
        activated_execution.session,
        activated_execution.drain_request(),
        actor="activation-operator",
        idempotency_key=UUID(int=12001),
    )
    assert (drained.execution_state, drained.executable_new_capacity_ceiling) == (
        "drain-only",
        0,
    )
    await activated_execution.publish_final_safe_evidence()
    retired = await activated_execution.store.retire_execution_epoch(
        activated_execution.session,
        activated_execution.retirement_request(),
        actor="activation-operator",
        idempotency_key=UUID(int=12002),
    )
    assert retired.execution_epoch == drained.execution_epoch
    assert await activated_execution.store.execution_authority(
        activated_execution.session
    ) is None
```

Add strict contract tests for duplicate/missing pools, changed executor or
journal bindings, zero/negative sequences, and noncanonical order. Assert
exact drain replay converges while drain-only and exact retirement replay
converges after shadow restoration; changed payload, actor, or idempotency
identity conflicts.

- [ ] **Step 2: Write conservative retirement RED matrix**

Parameterize blockers for every non-released intent state, quarantine,
submitting-unknown, stale or missing executor lease, missing/old inventory,
heartbeat older than inventory, fenced/equivocal executor, journal digest or
sequence mismatch, inventory digest or sequence mismatch, proofless or invalid
Loom-scoped record, and nonterminal exact Loom-owned record. For every case,
assert retirement raises and the authority remains exact drain-only with zero
ceiling/rate and all intent charges unchanged. A foreign record is preserved
and does not itself block retirement once all Loom work is conclusively
released.

Add a regression proving a later terminal inventory never changes a released
intent back to `terminal`; retirement becomes eligible only after a fresh
post-release complete inventory and a later exact heartbeat from both pools.

- [ ] **Step 3: Write executable client heartbeat and assembly RED tests**

```python
async def test_executable_client_sends_exact_heartbeat() -> None:
    heartbeat = executable_heartbeat_fixture(heartbeat_sequence=1)
    receipt = await client.heartbeat_executable_executor(heartbeat)
    assert receipt.heartbeat_sequence == 1
    assert seen_request.url.path == "/v2/executors/oldlab/heartbeat"
    assert json.loads(seen_request.content) == heartbeat.model_dump(mode="json")
```

Reject a receipt with changed sequence, invalid lease, wrong executable flag,
oversized body, or changed executor response shape. Add an app-lifespan test
that injects one `CapacityManagementStore(execution_policy=policy)` before
writer registration, starts while shadow, then prepares/activates through that
same public store without startup draining it. Assert the production default
still constructs a policy-disabled store and that no lifecycle mutation route
exists.

- [ ] **Step 4: Run the focused RED gate**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_execution_store.py \
  tests/integration/test_capacity_manager_api.py \
  tests/unit/test_capacity_executor_remote.py \
  -k 'drain or retire or retirement or executable_heartbeat or injected_management_store'
```

Expected: missing contracts/methods and client heartbeat failures. Do not add
production code until each RED fails for the named missing behavior.

- [ ] **Step 5: Add durable lifecycle evidence and database guards**

Add bounded immutable drain and retirement actor, idempotency, request-digest,
and request-payload columns to `CapacityExecutionEpoch`, plus unique
idempotency constraints and finite JSON object byte checks. Add per-executor `retirement_safe` and
`retirement_inventory_digest` fields with a constraint that safety is true
only with a canonical digest. Update the migration trigger so:

- insert remains prepared at zero;
- prepared may activate or retire on writer replacement only with exact durable
  evidence;
- active may become drain-only under the same writer for an operator request,
  or under writer `+1` for fail-closed replacement;
- drain-only may only advance its writer by `+1` or become retired under the
  same exact writer; and
- retired evidence and every manifest/activation/drain field are immutable.

Migration/model drift tests must compare the new constraints, indexes, columns,
and trigger body. Direct SQL must be unable to skip drain, restore active,
change a ceiling/rate in place, or retire without evidence.

- [ ] **Step 6: Implement operator drain and conservative retirement**

`begin_execution_drain()` locks authority then epoch and compare-and-sets the
exact authority/writer/epoch/manifest plus expected positive ceiling and rate.
It writes the idempotent evidence, sets epoch ceiling/rate to zero, changes both
states to drain-only, and sets `increase_freeze=true`; it does not advance the
writer. Update writer replacement to write equally durable derived drain or
prepared-retirement evidence.

`retire_execution_epoch()` uses lock order authority, epoch, executor states in
pool order, then intents in launch-rank order. It validates the request's exact
two final checkpoints against current rows, fresh leases/inventories, current
executor state, `last_heartbeat_at >= last_inventory_at`, matching retirement
inventory digests, and `retirement_safe=true`. It requires every epoch intent
to be `released`. Only then does it atomically mark the epoch retired and reset
authority to exact shadow (`execution_epoch=0`, no manifest, ceiling zero)
while the retired epoch retains ceiling/rate zero and the authority retains the
increase freeze. Error paths commit none of the retirement transition.

- [ ] **Step 7: Derive retirement safety at the authenticated inventory boundary**

After normal signature, controller, association, binding, resource, and intent
validation, keep `released` monotonic. Set one pool's retirement safety true
only when every non-foreign inventory record is terminal, cryptographically
exact, and maps to a released intent, and every intent assigned to that pool is
released. Empty complete inventory is safe only when that pool has no retained
intent. Any unverified, ambiguous, quarantined, nonterminal, or unmatched
Loom-scoped record sets safety false and clears its evidence digest. Foreign
records remain untouched and excluded from Loom ownership decisions.

- [ ] **Step 8: Add heartbeat transport and exact store injection**

Add the v2 receipt model and client method using the existing bounded `_request`
path. Extend its exact binding guard only to recognize
`ExecutableExecutorHeartbeatV2`, comparing the full execution context and
executor/pool fields to the immutable registration; preserve the stricter
`ExecutionFenceV2` check for work contracts. Validate receipt sequence and a
timezone-aware lease. Add `management_store: CapacityManagementStore | None = None` to
`create_app()` and use that exact instance during lifespan registration and
request handling. The default remains `CapacityManagementStore(...)` without
an execution policy. Do not add any operator mutation route.

- [ ] **Step 9: Run full prerequisite gates and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_execution_store.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_manager_api.py \
  tests/unit/test_capacity_executor_remote.py \
  tests/unit/test_capacity_executor_executable.py \
  tests/ops/test_global_fleet_pool_executor_once.py
uv run --no-sync ruff format --check \
  src/loom_capacity_manager src/loom_capacity_executor/client.py \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_execution_store.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_manager_api.py \
  tests/unit/test_capacity_executor_remote.py
uv run --no-sync ruff check \
  src/loom_capacity_manager src/loom_capacity_executor/client.py \
  tests/unit/test_capacity_manager_executable_contracts.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_capacity_manager_execution_store.py \
  tests/integration/test_capacity_manager_migrate.py \
  tests/integration/test_capacity_manager_api.py \
  tests/unit/test_capacity_executor_remote.py
uv run --no-sync mypy \
  src/loom_capacity_manager src/loom_capacity_executor/client.py
git diff --check
```

Commit the listed files with message
`feat(capacity): add safe executable authority turnover`.

---

### Task 13: Two-Pool Multi-Owner Integration Harness

**Files:**

- Create: `tests/integration/test_executable_global_capacity_bridge.py`
- Create: `tests/support/executable_capacity_harness.py`
- Modify: `tests/support/fake_slurm.py`
- Modify: `tests/conftest.py`

**Interfaces:** Produces `ExecutableCapacityHarness` with one real manager
store/API, two executors, two fake Slurm controllers, protected agents, static
subjects, shared development, and two personal owners. Consumes only Tasks
1-12 public/database/wire boundaries.

- [ ] **Step 1: Write concurrent owner-isolation placement**

```python
async def test_two_owners_share_both_pools_without_cross_binding(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    bob = await executable_capacity_harness.add_owner("bob", "b" * 64)
    await alice.publish_x86_demand(1)
    await bob.publish_arm_demand(1)
    await executable_capacity_harness.converge()
    assert executable_capacity_harness.oldlab.owner_slots(alice.subject_id) == 1
    assert executable_capacity_harness.gb10.owner_slots(bob.subject_id) == 1
    assert executable_capacity_harness.cross_owner_bindings() == []
```

- [ ] **Step 2: Write neutral fairness and complete scale-to-zero tests**

Publish neutral work for both owners; verify deterministic stable placement and
progressive account fairness. Complete claims, protected releases, and physical
terminal evidence; converge to zero Loom jobs and zero reusable commitments on
both pools.

- [ ] **Step 3: Run and verify the harness is absent**

Run: `uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py`

Expected: collection fails for `ExecutableCapacityHarness`.

- [ ] **Step 4: Implement the harness through real boundaries**

Use PostgreSQL testcontainers, an ASGI client started while shadow with the
exact injected owner-policy store, real v2 executor HTTP clients, real executor
journals, real Ed25519 keys, protected stores, and deterministic fake Slurm
subprocesses. Bootstrap each executor runtime with the public heartbeat route
before its first checkpoint. Set the finite fixture ceiling to the exact summed
fixture pool slots; keep every repository/deployment ceiling zero. Do not invoke
private helpers to skip protocol transitions.

- [ ] **Step 5: Add failure/isolation matrix**

Cover manager outage, one-pool outage, stale agent/executor, duplicate
submission, foreign job, resource mismatch, wrong candidate, personal
redeploy, deletion, drain, and journal restart. Assert uncertainty remains
charged and cannot affect another owner or pool.

Give the two fake controllers distinct cluster, host, partition, association,
submitter, QoS, and executable authorities. Terminalization removes a job from
live inventory and adds exact accounting evidence. The trusted test launcher
performs the public protected worker registration after physical binding; the
executor itself does not receive that authority.

Redeploy and deletion use the real lifecycle:
`active -> drain-only -> released/final inventories/final heartbeats -> retired
-> shadow configuration change -> new prepared/active epoch`. Assert an
attempted active redeploy/delete fails, retained or ambiguous work blocks
retirement, the old candidate/incarnation cannot bind into the new epoch, and
no foreign job is changed.

- [ ] **Step 6: Run twice and commit**

Run the integration file twice and compare canonical allocation/inventory
digests. Commit the four files with message
`test(capacity): prove two-pool multi-owner execution`.

---

### Task 14: Inert Deployment, Status, Runbook, and Governance

**Files:**

- Create: `deploy/dev-fleet/capacity-pool-executor.toml.example`
- Create: `deploy/dev-fleet/loom-capacity-pool-executor.service`
- Modify: `deploy/dev-fleet/README.md`
- Modify: `src/loom_cli/capacity_control_plane.py`
- Modify: `src/loom_cli/capacity_control_plane_cmd.py`
- Modify: `src/loom/personal_dev_capacity.py`
- Modify: `src/loom/personal_dev_reconciler.py`
- Modify: `src/loom_service/routes/dev_instances.py`
- Modify: `tests/loom_cli/test_capacity_control_plane.py`
- Modify: `tests/loom_cli/test_capacity_control_plane_cmd.py`
- Modify: `tests/unit/test_personal_dev_capacity.py`
- Modify: `tests/unit/test_personal_dev_reconciler.py`
- Modify: `tests/unit/test_dev_instance_routes.py`
- Create: `docs/runbooks/executable-global-capacity-bridge-rehearsal.md`
- Modify: `docs/architecture/global-fleet-capacity-manager.md`
- Modify: `docs/architecture/multi-dev-environments.md`
- Modify: `docs/runbooks/global-fleet-pool-executor-dry-run.md`

**Interfaces:** Produces deterministic executor config/systemd rendering and
read-only manager/executor status. Adds no apply, activate, start, enable, or
ceiling-change command. Produces `PersonalDevCapacityManagerCheckpoint` and
separate application, capacity-prepared, and worker-available status fields.

- [ ] **Step 1: Write inert render and mutation-surface tests**

```python
def test_checked_in_executor_profile_is_inert() -> None:
    profile = load_pool_executor_profile(_EXECUTOR_PROFILE)
    assert profile.executable_new_capacity_ceiling == 0
    assert {pool.pool_id for pool in profile.pools} == {"oldlab", "gb10"}


def test_capacity_cli_has_no_activation_or_apply() -> None:
    help_text = build_parser().format_help()
    assert " activate " not in help_text
    assert " apply " not in help_text
```

- [ ] **Step 2: Run and verify the profile is absent**

Run the two `tests/loom_cli/test_capacity_control_plane*.py` files.

Expected: failure because the executor profile is absent.

- [ ] **Step 3: Add strict inert render inputs**

Bind exact pools, controllers, executors, image digests, service users, state
paths, and secret references. Reject mutable images, embedded secrets, nonzero
ceilings, shared credentials, duplicate pools, and `loom-dev-shared`.

- [ ] **Step 4: Add read-only status and rehearsal evidence**

Report manager execution state/ceiling, executor lease/checkpoint, inventory,
quarantine, and blockers separately from application readiness. The runbook
covers render, permissions, controller validation, zero-ceiling inventory, and
restart/fake-permit rehearsal without install or start.

- [ ] **Step 5: Separate personal application and capacity readiness**

```python
@dataclass(frozen=True, slots=True)
class PersonalDevCapacityManagerCheckpoint:
    configuration_epoch: int
    execution_state: Literal["shadow", "prepared", "active", "drain-only"]
    execution_epoch: int
    executable_new_capacity_ceiling: int
```

Replace the zero-only `current_configuration_epoch()` response assumption with
an exact checkpoint validator. A lifecycle operation may become application
`ready` after projection and initial demand publication, but API status reports
capacity as `shadow`, `prepared`, `waiting`, or `available` from manager and
worker evidence; it never derives availability from pod readiness.

- [ ] **Step 6: Update current behavior without weakening v1 docs**

Document that the scheduler backend exists but is inert in deployed state.
Keep the v1 dry-run runbook permanently non-executable and state that no merge
authorizes live mutation.

- [ ] **Step 7: Run governance checks and commit**

Run:

```bash
uv run --no-sync pytest -q \
  tests/loom_cli/test_capacity_control_plane.py \
  tests/loom_cli/test_capacity_control_plane_cmd.py \
  tests/ops/test_global_fleet_pool_executor_once.py \
  tests/unit/test_personal_dev_capacity.py \
  tests/unit/test_personal_dev_reconciler.py \
  tests/unit/test_dev_instance_routes.py
uv run --no-sync python scripts/ops/check_repo_hygiene.py
test ! -e docs/superpowers
```

Commit the listed deployment, CLI, test, architecture, and runbook files with
message `docs(capacity): package inert executable bridge`.

---

### Task 15: Full Verification, Review, PR, and CI

**Files:** Modify only files required by evidence-backed review findings.

**Interfaces:** Produces a reviewed branch, normal PR, exact-head CI evidence,
merge, and issue #906 evidence update. It performs no live activation.

- [ ] **Step 1: Run format, lint, and strict typing**

```bash
uv run --no-sync ruff format --check \
  src/loom_capacity_manager src/loom_capacity_executor src/loom_capacity_agent \
  src/loom_control_plane src/loom src/loom_service tests
uv run --no-sync ruff check \
  src/loom_capacity_manager src/loom_capacity_executor src/loom_capacity_agent \
  src/loom_control_plane src/loom src/loom_service tests
uv run --no-sync mypy \
  src/loom_capacity_manager src/loom_capacity_executor src/loom_capacity_agent \
  src/loom_control_plane src/loom src/loom_service
```

Expected: all exit zero.

- [ ] **Step 2: Run the focused capacity suite**

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_manager_*.py \
  tests/unit/test_capacity_executor_*.py \
  tests/unit/test_capacity_agent_*.py \
  tests/unit/test_personal_dev_capacity.py \
  tests/integration/test_capacity_manager_*.py \
  tests/integration/test_capacity_agent_*.py \
  tests/integration/test_executable_global_capacity_bridge.py \
  tests/ops/test_global_fleet_capacity_shadow_once.py \
  tests/ops/test_global_fleet_pool_executor_once.py \
  tests/ops/test_global_dev_fleet_autoscaler_external_once.py \
  tests/ops/test_worker_pool_autoscaler_external_once.py \
  tests/unit/test_global_execution_fence.py \
  tests/unit/test_dev_instance_routes.py \
  tests/unit/test_personal_dev_reconciler.py \
  tests/loom_cli/test_capacity_control_plane*.py
```

Expected: all pass with only pre-existing documented skips.

- [ ] **Step 3: Run authoritative local repository gates**

Read `.github/workflows/ci.yml` at the final head and run the same safe local
repository checks, capacity migration suite, and changed-file security checks.
Record exact commands/counts in the PR body.

- [ ] **Step 4: Iteratively self-review every design requirement**

Search for unbounded input, shell execution, widened v1 flags, nonzero
checked-in ceilings, missing epoch comparisons, dynamic metric labels,
candidate authority, foreign mutation, ambiguous release, duplicate writers,
secrets, and `docs/superpowers/`. For each concrete finding, add a failing
regression test, make one fix, rerun focused and full suites, and repeat until
no finding remains.

- [ ] **Step 5: Rebase current `origin/dev` and reverify**

```bash
git fetch origin dev
git rebase origin/dev
git status --short --branch
```

Resolve only branch-owned conflicts and repeat Steps 1-3 at the rebased head.

- [ ] **Step 6: Push and open the normal PR**

```bash
git push -u origin feat/global-capacity-executable-bridge
gh pr create --base dev --head feat/global-capacity-executable-bridge \
  --title "feat(capacity): add executable global capacity bridge" \
  --body-file /tmp/loom-capacity-bridge-pr.md
```

The body lists scope, zero-ceiling safety, migration behavior, evidence, and
remaining activation gates. Issue #906 remains open.

- [ ] **Step 7: Monitor and resolve authoritative checks**

Use bounded `gh pr checks --watch` intervals. For a failure, inspect the exact
job/log, reproduce or isolate its root cause, add a regression test, fix one
cause, rerun affected gates, commit, and push. Repeat until every required
current-head check succeeds.

- [ ] **Step 8: Final review, merge, and cleanup**

Confirm head identity, current approvals, all required checks, and absence of
live activation. Merge using repository policy, verify `origin/dev` contains
the merge, and remove only this merged branch/worktree. Do not touch unrelated
active worktrees or branches.

- [ ] **Step 9: Update issue evidence and continue the full goal**

Post PR, merge commit, exact CI runs, test counts, delivered capability, and
unchanged zero-ceiling boundary to #906. Reconcile #1278, #1280, #822, #896,
and #1120 using current evidence, then begin protected-writer closure and
migration tooling as the next gated slice.
