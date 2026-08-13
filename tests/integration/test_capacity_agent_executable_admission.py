"""Executable-v2 admission remains protected inside one environment database."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest
from psycopg.errors import InsufficientPrivilege
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom_capacity_agent.admission import (
    ExecutableDrainRequestV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    PhysicalJobBindingV2,
)
from loom_capacity_agent.claim_guard import (
    ExecutableClaimProposalV2,
    ExecutableClaimReceiptV2,
    InertAttemptTransitionV1,
)
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.executable_admission import ExecutableAdmissionStore
from loom_capacity_agent.lifecycle_store import CapacityAttemptLifecycleStore
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
)
from tests.integration.test_capacity_agent_store import (
    _initialize_and_register,
    _owner_session,
    _seed_trial,
)


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


@asynccontextmanager
async def _serializable_executor_session(
    database: dict[str, object],
) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(
        make_url(_value(database, "executor_url")), isolation_level="SERIALIZABLE"
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            yield session
    finally:
        await engine.dispose()


@asynccontextmanager
async def _agent_session(
    database: dict[str, object],
) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(make_url(_value(database, "agent_url")))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            yield session
    finally:
        await engine.dispose()


async def _seed_protected_attempt(
    database: dict[str, object],
    *,
    protected_attempt_id: UUID,
    execution_generation: int,
    requirements_digest: str,
) -> None:
    trial_id = _seed_trial(database)
    async with _owner_session(database) as (_, _, session):
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_requirements "
                "(trial_id, schema_version, requirements_digest, requirements) "
                "VALUES (:trial_id, 1, :requirements_digest, '{}'::jsonb)"
            ),
            {"trial_id": trial_id, "requirements_digest": requirements_digest},
        )
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, "
                "requirements_digest, claim_state) VALUES "
                "(:protected_attempt_id, :trial_id, :execution_generation, "
                ":requirements_digest, 'queued')"
            ),
            {
                "protected_attempt_id": protected_attempt_id,
                "trial_id": trial_id,
                "execution_generation": execution_generation,
                "requirements_digest": requirements_digest,
            },
        )


async def _prepare_claim_terminal_race(
    database: dict[str, object],
) -> tuple[
    AgentRegistrationV1,
    ExecutableWorkerRegistrationV2,
    ExecutableClaimProposalV2,
    InertAttemptTransitionV1,
]:
    _fence, registration = await _initialize_and_register(database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    capability = "single-use-bootstrap-capability"
    worker = _worker(request)
    protected_attempt_id = UUID(int=149)
    requirements_digest = "8" * 64
    await _seed_protected_attempt(
        database,
        protected_attempt_id=protected_attempt_id,
        execution_generation=14,
        requirements_digest=requirements_digest,
    )
    async with _serializable_executor_session(database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            request,
            bootstrap_sha256=hashlib.sha256(capability.encode("ascii")).hexdigest(),
        )
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)
    return (
        registration,
        worker,
        ExecutableClaimProposalV2(
            operation_id=UUID(int=150),
            protected_attempt_id=protected_attempt_id,
            execution_generation=14,
            requirements_digest=requirements_digest,
            worker_id=worker.worker_id,
            worker_incarnation=worker.worker_incarnation,
            expected_claim_high_water=0,
        ),
        InertAttemptTransitionV1(
            **registration.model_dump(mode="python"),
            transition_id=UUID(int=151),
            protected_attempt_id=protected_attempt_id,
            execution_generation=14,
            requirements_digest=requirements_digest,
            expected_transition_sequence=0,
            operation="cancel",
            expected_state="pending-unassigned",
            target_state="cancelled-terminal",
            transition_reason="claimed-attempt-terminal",
        ),
    )


# Production break caught: the executor credential could not read the exact
# protected intent state needed to construct a drain without gaining table access.
async def test_executor_observes_exact_protected_intent_without_table_privilege(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256="a" * 64)

        observed = await store.observe_intent(request.binding)

    assert observed.binding == request.binding
    assert observed.bootstrap_registration_epoch == request.bootstrap_registration_epoch
    assert observed.worker_id is None
    assert observed.drain is None
    assert observed.release is None


async def _run_claim_transaction(
    database: dict[str, object],
    registration: AgentRegistrationV1,
    claim: ExecutableClaimProposalV2,
    backend_pid: asyncio.Future[int],
) -> ExecutableClaimReceiptV2 | None | DBAPIError:
    try:
        async with _serializable_executor_session(database) as session:
            backend_pid.set_result(
                (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            )
            return await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).admit_claim(claim)
    except DBAPIError as exc:
        return exc


async def _run_terminal_transaction(
    database: dict[str, object],
    registration: AgentRegistrationV1,
    terminal: InertAttemptTransitionV1,
    backend_pid: asyncio.Future[int],
) -> InertAttemptTransitionV1:
    async with _agent_session(database) as session:
        backend_pid.set_result(
            (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        )
        return await CapacityAttemptLifecycleStore(
            session,
            registration=registration,
        ).apply_transition(terminal)


async def _backend_waited_for_lock(
    database: dict[str, object],
    *,
    backend_pid: int,
    task: asyncio.Task[object],
) -> bool:
    engine = create_async_engine(make_url(_value(database, "admin_url")))
    try:
        async with engine.connect() as connection, asyncio.timeout(10):
            while not task.done():
                wait_event_type = (
                    await connection.execute(
                        text(
                            "SELECT wait_event_type FROM pg_stat_activity WHERE pid = :backend_pid"
                        ),
                        {"backend_pid": backend_pid},
                    )
                ).scalar_one_or_none()
                if wait_event_type == "Lock":
                    return True
        return False
    finally:
        await engine.dispose()


async def _claim_terminal_counts(
    database: dict[str, object],
) -> tuple[int, int, int]:
    async with _owner_session(database) as (_, _, session):
        row = (
            await session.execute(
                text(
                    "SELECT count(lease.operation_id) AS admitted, "
                    "count(terminal.admitted_operation_id) AS terminal, "
                    "count(lease.operation_id) FILTER "
                    "(WHERE terminal.admitted_operation_id IS NULL) AS live "
                    "FROM loom_capacity_guard.executable_claim_leases AS lease "
                    "LEFT JOIN loom_capacity_guard.executable_claim_terminal_events AS terminal "
                    "ON terminal.admitted_operation_id = lease.operation_id"
                )
            )
        ).one()
    return row.admitted, row.terminal, row.live


def _bootstrap(subject_id: UUID, subject_incarnation: UUID) -> ExecutableBootstrapRegistrationV2:
    binding = ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            authority_incarnation=UUID(int=101),
            writer_epoch=3,
            configuration_epoch=5,
            execution_epoch=7,
            execution_manifest_sha256="1" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            trusted_fleet_release_sha256="2" * 64,
            allocation_epoch=11,
        ),
        tranche_id=UUID(int=102),
        intent_id=UUID(int=103),
        shape_instance_id="oldlab-shape-0001",
        subject_id=subject_id,
        subject_incarnation=subject_incarnation,
        account_id="owner-alice",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="git-sha1",
            identity="a" * 40,
            publication_sha256="a" * 64,
        ),
        candidate_generation=7,
        deployment_generation=7,
        pool_id="oldlab",
        pool_generation=13,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=104),
        shape_id="oldlab-cpu-small",
        profile_id="oldlab-default",
        profile_generation=17,
        profile_digest="3" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=1000,
            memory_bytes=1024,
        ),
        node_ids=("oldlab-node-01",),
    )
    return ExecutableBootstrapRegistrationV2(
        binding=binding,
        command_sequence=1,
        bootstrap_registration_epoch=19,
        bootstrap_evidence_sha256="4" * 64,
    )


def _physical(request: ExecutableBootstrapRegistrationV2) -> PhysicalJobBindingV2:
    return PhysicalJobBindingV2(
        operation_id=UUID(int=105),
        binding=request.binding,
        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        slurm_job_id="oldlab-12345",
        ownership_evidence_sha256="5" * 64,
    )


def _worker(
    request: ExecutableBootstrapRegistrationV2,
    *,
    operation_id: UUID | None = None,
    worker_id: UUID | None = None,
    worker_incarnation: UUID | None = None,
    protected_registration_epoch: int = 20,
    worker_credential: str = "worker-credential-one",
    predecessor_worker_incarnation: UUID | None = None,
) -> ExecutableWorkerRegistrationV2:
    return ExecutableWorkerRegistrationV2(
        operation_id=operation_id or UUID(int=106),
        binding=request.binding,
        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        protected_registration_epoch=protected_registration_epoch,
        slurm_job_id="oldlab-12345",
        worker_id=worker_id or UUID(int=107),
        worker_incarnation=worker_incarnation or UUID(int=108),
        worker_credential_sha256=hashlib.sha256(worker_credential.encode("ascii")).hexdigest(),
        predecessor_worker_incarnation=predecessor_worker_incarnation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "replacement"),
    (
        (("unexpected",), "field"),
        (("binding", "unexpected"), "field"),
        (("binding", "execution", "unexpected"), "field"),
        (("binding", "candidate", "unexpected"), "field"),
        (("binding", "resources", "unexpected"), "field"),
        (("command_sequence",), None),
        (("bootstrap_evidence_sha256",), None),
        (("binding", "execution", "authority_incarnation"), None),
        (("binding", "execution", "writer_epoch"), None),
        (("binding", "execution", "configuration_epoch"), None),
        (("binding", "execution", "allocation_epoch"), None),
        (("binding", "execution", "execution_epoch"), None),
        (("binding", "executor_incarnation"), None),
        (("binding", "pool_generation"), None),
        (("binding", "pool_generation"), "13"),
        (("binding", "profile_generation"), None),
        (("binding", "shape_id"), None),
        (("binding", "resources", "memory_bytes"), None),
        (("binding", "resources", "memory_bytes"), "1024"),
        (("binding", "intent_id"), None),
    ),
)
async def test_direct_sql_rejects_under_bound_or_extra_executable_payloads(
    capacity_guard_database: dict[str, object],
    path: tuple[str, ...],
    replacement: object,
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    payload = _bootstrap(
        registration.subject_id,
        registration.subject_incarnation,
    ).model_dump(mode="json", exclude_none=False)
    target = payload
    for segment in path[:-1]:
        nested = target[segment]
        assert isinstance(nested, dict)
        target = nested
    target[path[-1]] = replacement
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")

    async with _serializable_executor_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match=r"schema|binding|invalid"):
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.prepare_executable_worker("
                    ":subject_id, :subject_incarnation, CAST(:payload AS jsonb), "
                    "CAST(:canonical AS bytea), :digest, :bootstrap_sha256)"
                ),
                {
                    "subject_id": registration.subject_id,
                    "subject_incarnation": registration.subject_incarnation,
                    "payload": canonical.decode("ascii"),
                    "canonical": canonical,
                    "digest": hashlib.sha256(canonical).hexdigest(),
                    "bootstrap_sha256": "6" * 64,
                },
            )


@pytest.mark.asyncio
async def test_prepare_bind_register_is_ordered_exact_and_one_time(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    physical = _physical(request)
    worker = _worker(request)
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        with pytest.raises(DBAPIError, match="prepared executable admission"):
            await store.bind_slurm_job(physical)
        prepared = await store.prepare_worker(request, bootstrap_sha256=digest)
        assert prepared.request_digest == prepared.admission_digest
        assert prepared.protected_high_water == 1
        assert await store.prepare_worker(request, bootstrap_sha256=digest) == prepared
        with pytest.raises(DBAPIError, match="conflicting executable preparation"):
            await store.prepare_worker(request, bootstrap_sha256="8" * 64)
        with pytest.raises(DBAPIError, match="physical binding"):
            await store.register_worker(worker, bootstrap_capability=capability)
        bound = await store.bind_slurm_job(physical)
        assert bound.request_digest == bound.binding_digest
        assert bound.protected_high_water == 2
        with pytest.raises(DBAPIError, match="bootstrap capability"):
            await store.register_worker(worker, bootstrap_capability="wrong-capability")

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        registered = await store.register_worker(worker, bootstrap_capability=capability)
        assert registered.request_digest == registered.registration_digest
        assert registered.protected_high_water == 3
        assert await store.register_worker(worker, bootstrap_capability=capability) == registered
        with pytest.raises(DBAPIError, match="conflicting worker registration"):
            await store.register_worker(
                worker.model_copy(update={"worker_credential_sha256": "9" * 64}),
                bootstrap_capability=capability,
            )


@pytest.mark.asyncio
async def test_requeue_revokes_predecessor_and_drain_blocks_new_claims(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
    first = _worker(request)
    second = _worker(
        request,
        operation_id=UUID(int=109),
        worker_id=UUID(int=110),
        worker_incarnation=UUID(int=111),
        protected_registration_epoch=21,
        worker_credential="worker-credential-two",
        predecessor_worker_incarnation=first.worker_incarnation,
    )
    predecessor_attempt = UUID(int=135)
    drained_attempt = UUID(int=136)
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=predecessor_attempt,
        execution_generation=9,
        requirements_digest="c" * 64,
    )
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=drained_attempt,
        execution_generation=10,
        requirements_digest="d" * 64,
    )

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=digest)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(first, bootstrap_capability=capability)
        requeued = await store.register_worker(
            second,
            predecessor_worker_credential="worker-credential-one",
        )
        assert requeued.predecessor_worker_incarnation == first.worker_incarnation
        assert (
            await store.admit_claim(
                ExecutableClaimProposalV2(
                    operation_id=UUID(int=137),
                    protected_attempt_id=predecessor_attempt,
                    execution_generation=9,
                    requirements_digest="c" * 64,
                    worker_id=first.worker_id,
                    worker_incarnation=first.worker_incarnation,
                    expected_claim_high_water=0,
                )
            )
            is None
        )

        drain = ExecutableDrainRequestV2(
            operation_id=UUID(int=112),
            binding=request.binding,
            worker_id=second.worker_id,
            worker_incarnation=second.worker_incarnation,
            expected_claim_high_water=0,
            drain_epoch=1,
        )
        drained = await store.begin_drain(drain)
        assert drained.live_claim_count == 0
        assert (
            await store.admit_claim(
                ExecutableClaimProposalV2(
                    operation_id=UUID(int=138),
                    protected_attempt_id=drained_attempt,
                    execution_generation=10,
                    requirements_digest="d" * 64,
                    worker_id=second.worker_id,
                    worker_incarnation=second.worker_incarnation,
                    expected_claim_high_water=0,
                )
            )
            is None
        )


@pytest.mark.asyncio
async def test_claim_and_drain_share_one_locked_protected_transaction(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    capability = "single-use-bootstrap-capability"
    worker = _worker(request)
    first_attempt = UUID(int=130)
    second_attempt = UUID(int=131)
    requirements_digest = "b" * 64
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=first_attempt,
        execution_generation=7,
        requirements_digest=requirements_digest,
    )
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=second_attempt,
        execution_generation=8,
        requirements_digest=requirements_digest,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            request,
            bootstrap_sha256=hashlib.sha256(capability.encode("ascii")).hexdigest(),
        )
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)

    claim = ExecutableClaimProposalV2(
        operation_id=UUID(int=132),
        protected_attempt_id=first_attempt,
        execution_generation=7,
        requirements_digest=requirements_digest,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=0,
    )
    drain = ExecutableDrainRequestV2(
        operation_id=UUID(int=133),
        binding=request.binding,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=0,
        drain_epoch=1,
    )

    async def attempt_claim() -> object:
        async with _serializable_executor_session(capacity_guard_database) as session:
            return await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).admit_claim(claim)

    async def begin_drain() -> object:
        async with _serializable_executor_session(capacity_guard_database) as session:
            return await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).begin_drain(drain)

    claim_result, drain_result = await asyncio.gather(
        attempt_claim(),
        begin_drain(),
        return_exceptions=True,
    )
    claim_won = not isinstance(claim_result, BaseException) and claim_result is not None
    drain_won = not isinstance(drain_result, BaseException)
    assert claim_won != drain_won

    if claim_won:
        async with _serializable_executor_session(capacity_guard_database) as session:
            drained = await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).begin_drain(drain.model_copy(update={"expected_claim_high_water": 1}))
        assert drained.claim_high_water == 1
        assert drained.live_claim_count == 1

    second_claim = claim.model_copy(
        update={
            "operation_id": UUID(int=134),
            "protected_attempt_id": second_attempt,
            "execution_generation": 8,
            "expected_claim_high_water": 1 if claim_won else 0,
        }
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        denied = await ExecutableAdmissionStore(
            session,
            registration=registration,
        ).admit_claim(second_claim)
    async with _owner_session(capacity_guard_database) as (_, _, session):
        count = (
            await session.execute(
                text("SELECT count(*) FROM loom_capacity_guard.executable_claim_leases")
            )
        ).scalar_one()
    assert denied is None
    assert count == (1 if claim_won else 0)


@pytest.mark.asyncio
async def test_protected_terminal_lifecycle_closes_immutable_claim_and_allows_release(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch release treating every immutable admission row as live forever."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    capability = "single-use-bootstrap-capability"
    worker = _worker(request)
    protected_attempt_id = UUID(int=143)
    requirements_digest = "7" * 64
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=protected_attempt_id,
        execution_generation=13,
        requirements_digest=requirements_digest,
    )
    claim = ExecutableClaimProposalV2(
        operation_id=UUID(int=144),
        protected_attempt_id=protected_attempt_id,
        execution_generation=13,
        requirements_digest=requirements_digest,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=0,
    )
    terminal = InertAttemptTransitionV1(
        **registration.model_dump(mode="python"),
        transition_id=UUID(int=145),
        protected_attempt_id=protected_attempt_id,
        execution_generation=13,
        requirements_digest=requirements_digest,
        expected_transition_sequence=0,
        operation="cancel",
        expected_state="pending-unassigned",
        target_state="cancelled-terminal",
        transition_reason="claimed-attempt-terminal",
    )
    drain = ExecutableDrainRequestV2(
        operation_id=UUID(int=146),
        binding=request.binding,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=1,
        drain_epoch=1,
    )
    release = ExecutableReleaseRequestV2(
        operation_id=UUID(int=147),
        binding=request.binding,
        reporter_incarnation=registration.reporter_incarnation,
        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        expected_claim_high_water=1,
        protected_registration_epoch=worker.protected_registration_epoch,
        release_epoch=1,
    )

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            request,
            bootstrap_sha256=hashlib.sha256(capability.encode("ascii")).hexdigest(),
        )
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)
        admitted = await store.admit_claim(claim)
        assert admitted is not None
        drained = await store.begin_drain(drain)
        assert drained.live_claim_count == 1
        with pytest.raises(DBAPIError, match="zero live protected claims"):
            await store.acknowledge_release(
                release,
                current_worker_credential="worker-credential-one",
            )

    async with _agent_session(capacity_guard_database) as session:
        lifecycle = CapacityAttemptLifecycleStore(session, registration=registration)
        mismatched = terminal.model_copy(update={"execution_generation": 14})
        with pytest.raises(DBAPIError, match="compare-and-set"):
            await lifecycle.apply_transition(mismatched)
        assert await lifecycle.apply_transition(terminal) == terminal
        conflicting = terminal.model_copy(update={"transition_reason": "conflicting-replay"})
        with pytest.raises(DBAPIError, match="conflicting inert lifecycle replay"):
            await lifecycle.apply_transition(conflicting)
        delayed = terminal.model_copy(
            update={"transition_id": UUID(int=148), "expected_transition_sequence": 0}
        )
        with pytest.raises(DBAPIError, match="compare-and-set"):
            await lifecycle.apply_transition(delayed)

    async with _serializable_executor_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError) as forged:
            await session.execute(
                text(
                    "INSERT INTO loom_capacity_guard.executable_claim_terminal_events "
                    "DEFAULT VALUES"
                )
            )
        assert isinstance(forged.value.orig, InsufficientPrivilege)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        evidence = (
            (
                await session.execute(
                    text(
                        "SELECT admitted_operation_id, protected_attempt_id, "
                        "execution_generation, requirements_digest, intent_id, subject_id, "
                        "subject_incarnation, worker_id, worker_incarnation, terminal_state, "
                        "terminal_evidence_sha256, claim_high_water, terminal_high_water "
                        "FROM loom_capacity_guard.executable_claim_terminal_events"
                    )
                )
            )
            .mappings()
            .one()
        )
        assert dict(evidence) == {
            "admitted_operation_id": claim.operation_id,
            "protected_attempt_id": claim.protected_attempt_id,
            "execution_generation": claim.execution_generation,
            "requirements_digest": claim.requirements_digest,
            "intent_id": request.binding.intent_id,
            "subject_id": registration.subject_id,
            "subject_incarnation": registration.subject_incarnation,
            "worker_id": worker.worker_id,
            "worker_incarnation": worker.worker_incarnation,
            "terminal_state": "cancelled-terminal",
            "terminal_evidence_sha256": hashlib.sha256(
                json.dumps(
                    terminal.model_dump(mode="json", exclude_none=False),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("ascii")
            ).hexdigest(),
            "claim_high_water": 1,
            "terminal_high_water": 1,
        }

    async with _serializable_executor_session(capacity_guard_database) as session:
        receipt = await ExecutableAdmissionStore(
            session,
            registration=registration,
        ).acknowledge_release(
            release,
            current_worker_credential="worker-credential-one",
        )
        assert receipt.claim_high_water == 1
        assert receipt.live_claim_count == 0


@pytest.mark.asyncio
async def test_claim_first_serializes_terminal_projection_on_exact_attempt_head(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch a terminal trigger deciding no claim while admission is uncommitted."""

    registration, _worker_registration, claim, terminal = await _prepare_claim_terminal_race(
        capacity_guard_database
    )
    loop = asyncio.get_running_loop()
    claim_pid: asyncio.Future[int] = loop.create_future()
    terminal_pid: asyncio.Future[int] = loop.create_future()
    claim_task: asyncio.Task[object]
    terminal_task: asyncio.Task[object]
    async with _owner_session(capacity_guard_database) as (_, _, blocker):
        await blocker.execute(
            text("LOCK TABLE loom_capacity_guard.executable_claim_leases IN SHARE MODE")
        )
        claim_task = asyncio.create_task(
            _run_claim_transaction(
                capacity_guard_database,
                registration,
                claim,
                claim_pid,
            )
        )
        assert await _backend_waited_for_lock(
            capacity_guard_database,
            backend_pid=await claim_pid,
            task=claim_task,
        )
        terminal_task = asyncio.create_task(
            _run_terminal_transaction(
                capacity_guard_database,
                registration,
                terminal,
                terminal_pid,
            )
        )
        terminal_waited_for_claim = await _backend_waited_for_lock(
            capacity_guard_database,
            backend_pid=await terminal_pid,
            task=terminal_task,
        )

    claim_result, terminal_result = await asyncio.gather(claim_task, terminal_task)
    assert terminal_waited_for_claim is True
    assert isinstance(claim_result, ExecutableClaimReceiptV2)
    assert terminal_result == terminal
    assert await _claim_terminal_counts(capacity_guard_database) == (1, 1, 0)


@pytest.mark.asyncio
async def test_terminal_first_serializes_claim_rejection_on_exact_attempt_head(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch admission seeing a stale nonterminal head during terminal commit."""

    registration, _worker_registration, claim, terminal = await _prepare_claim_terminal_race(
        capacity_guard_database
    )
    agent_engine = create_async_engine(make_url(_value(capacity_guard_database, "agent_url")))
    agent_factory = async_sessionmaker(agent_engine, expire_on_commit=False)
    loop = asyncio.get_running_loop()
    claim_pid: asyncio.Future[int] = loop.create_future()
    claim_task: asyncio.Task[object]
    try:
        async with agent_factory() as terminal_session, terminal_session.begin():
            assert (
                await CapacityAttemptLifecycleStore(
                    terminal_session,
                    registration=registration,
                ).apply_transition(terminal)
                == terminal
            )
            claim_task = asyncio.create_task(
                _run_claim_transaction(
                    capacity_guard_database,
                    registration,
                    claim,
                    claim_pid,
                )
            )
            claim_waited_for_terminal = await _backend_waited_for_lock(
                capacity_guard_database,
                backend_pid=await claim_pid,
                task=claim_task,
            )
        claim_result = await claim_task
    finally:
        await agent_engine.dispose()

    assert claim_waited_for_terminal is True
    assert isinstance(claim_result, DBAPIError)
    assert await _claim_terminal_counts(capacity_guard_database) == (0, 0, 0)


@pytest.mark.asyncio
async def test_claimability_is_independent_for_concurrent_intents(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    first_request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    second_request = first_request.model_copy(
        update={
            "binding": first_request.binding.model_copy(
                update={
                    "tranche_id": UUID(int=118),
                    "intent_id": UUID(int=119),
                    "shape_instance_id": "oldlab-shape-0002",
                }
            ),
            "command_sequence": 2,
            "bootstrap_registration_epoch": 20,
        }
    )
    first_capability = "first-bootstrap-capability"
    second_capability = "second-bootstrap-capability"
    first_worker = _worker(first_request)
    second_worker = _worker(
        second_request,
        operation_id=UUID(int=120),
        worker_id=UUID(int=121),
        worker_incarnation=UUID(int=122),
        protected_registration_epoch=21,
        worker_credential="worker-credential-two",
    )
    first_attempt = UUID(int=139)
    second_attempt = UUID(int=140)
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=first_attempt,
        execution_generation=11,
        requirements_digest="e" * 64,
    )
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=second_attempt,
        execution_generation=12,
        requirements_digest="f" * 64,
    )

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        for request, capability, worker, physical_operation, slurm_job_id in (
            (
                first_request,
                first_capability,
                first_worker,
                UUID(int=105),
                "oldlab-12345",
            ),
            (
                second_request,
                second_capability,
                second_worker,
                UUID(int=123),
                "oldlab-12346",
            ),
        ):
            await store.prepare_worker(
                request,
                bootstrap_sha256=hashlib.sha256(capability.encode("ascii")).hexdigest(),
            )
            await store.bind_slurm_job(
                _physical(request).model_copy(
                    update={
                        "operation_id": physical_operation,
                        "slurm_job_id": slurm_job_id,
                    }
                )
            )
            await store.register_worker(
                worker.model_copy(update={"slurm_job_id": slurm_job_id}),
                bootstrap_capability=capability,
            )

        first_claim = await store.admit_claim(
            ExecutableClaimProposalV2(
                operation_id=UUID(int=141),
                protected_attempt_id=first_attempt,
                execution_generation=11,
                requirements_digest="e" * 64,
                worker_id=first_worker.worker_id,
                worker_incarnation=first_worker.worker_incarnation,
                expected_claim_high_water=0,
            )
        )
        second_claim = await store.admit_claim(
            ExecutableClaimProposalV2(
                operation_id=UUID(int=142),
                protected_attempt_id=second_attempt,
                execution_generation=12,
                requirements_digest="f" * 64,
                worker_id=second_worker.worker_id,
                worker_incarnation=second_worker.worker_incarnation,
                expected_claim_high_water=0,
            )
        )
        assert first_claim is not None
        assert first_claim.intent_id == first_request.binding.intent_id
        assert second_claim is not None
        assert second_claim.intent_id == second_request.binding.intent_id


@pytest.mark.asyncio
async def test_physical_job_cannot_bind_two_intents_in_the_same_pool(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    first_request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    second_request = first_request.model_copy(
        update={
            "binding": first_request.binding.model_copy(
                update={
                    "tranche_id": UUID(int=118),
                    "intent_id": UUID(int=119),
                    "shape_instance_id": "oldlab-shape-0002",
                }
            ),
            "command_sequence": 2,
            "bootstrap_registration_epoch": 20,
        }
    )

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(first_request, bootstrap_sha256="6" * 64)
        await store.prepare_worker(second_request, bootstrap_sha256="7" * 64)
        await store.bind_slurm_job(_physical(first_request))
        with pytest.raises(DBAPIError, match="physical binding"):
            await store.bind_slurm_job(
                _physical(second_request).model_copy(update={"operation_id": UUID(int=120)})
            )


@pytest.mark.asyncio
async def test_release_requires_revocation_newer_epoch_and_fences_delayed_registration(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
    worker = _worker(request)

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=digest)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)
        drain = ExecutableDrainRequestV2(
            operation_id=UUID(int=113),
            binding=request.binding,
            worker_id=worker.worker_id,
            worker_incarnation=worker.worker_incarnation,
            expected_claim_high_water=0,
            drain_epoch=1,
        )
        await store.begin_drain(drain)
        stale = ExecutableReleaseRequestV2(
            operation_id=UUID(int=114),
            binding=request.binding,
            reporter_incarnation=registration.reporter_incarnation,
            bootstrap_registration_epoch=request.bootstrap_registration_epoch,
            expected_claim_high_water=0,
            protected_registration_epoch=request.bootstrap_registration_epoch,
            release_epoch=1,
        )
        with pytest.raises(DBAPIError, match="newer protected registration epoch"):
            await store.acknowledge_release(
                stale,
                current_worker_credential="worker-credential-one",
            )
        invented = stale.model_copy(update={"protected_registration_epoch": 22})
        invented_bytes = json.dumps(
            invented.model_dump(mode="json", exclude_none=False),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        with pytest.raises(DBAPIError, match="registration evidence"):
            async with session.begin_nested():
                await session.execute(
                    text(
                        "SELECT loom_capacity_guard.acknowledge_executable_release("
                        ":subject_id, :subject_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical AS bytea), :digest, :worker_credential)"
                    ),
                    {
                        "subject_id": registration.subject_id,
                        "subject_incarnation": registration.subject_incarnation,
                        "payload": invented_bytes.decode("ascii"),
                        "canonical": invented_bytes,
                        "digest": hashlib.sha256(invented_bytes).hexdigest(),
                        "worker_credential": "worker-credential-one",
                    },
                )
        release = stale.model_copy(
            update={"protected_registration_epoch": worker.protected_registration_epoch}
        )
        receipt = await store.acknowledge_release(
            release,
            current_worker_credential="worker-credential-one",
        )
        assert receipt.bootstrap_revoked is True
        assert receipt.worker_credentials_revoked is True
        assert receipt.live_claim_count == 0
        assert receipt.protected_high_water == 5
        delayed = _worker(
            request,
            operation_id=UUID(int=115),
            worker_id=UUID(int=116),
            worker_incarnation=UUID(int=117),
            protected_registration_epoch=23,
        )
        with pytest.raises(DBAPIError, match="release fence"):
            await store.register_worker(delayed, bootstrap_capability=capability)


def test_candidate_role_cannot_prepare_worker(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    candidate = f"candidate_executable_test_{uuid4().hex[:12]}"
    quoted = engine.dialect.identifier_preparer.quote(candidate)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted} NOLOGIN NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
        with engine.connect() as connection:
            connection.exec_driver_sql(f"SET ROLE {quoted}")
            with pytest.raises(DBAPIError) as denied:
                connection.execute(
                    text(
                        "SELECT loom_capacity_guard.prepare_executable_worker("
                        ":subject_id, :subject_incarnation, '{}'::jsonb, ''::bytea, "
                        ":digest, :bootstrap)"
                    ),
                    {
                        "subject_id": uuid4(),
                        "subject_incarnation": uuid4(),
                        "digest": "0" * 64,
                        "bootstrap": "0" * 64,
                    },
                )
            assert isinstance(denied.value.orig, InsufficientPrivilege)
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted}")
        engine.dispose()
