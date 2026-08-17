"""Executable-v2 admission remains protected inside one environment database."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from psycopg.errors import InsufficientPrivilege
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom_capacity_agent.admission import (
    ExecutableDrainRequestV2,
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    ExecutableWorkerWithdrawalRequestV2,
    NeverConvergedAdmissionPlanV1,
    PhysicalJobBindingV2,
    PreparedAdmissionPlanV1,
    PreparedPlacementAllowanceV1,
    PreparedWorkerShapeV1,
    ProtectedReleasePublicationCheckpointV2,
    PublishableExecutableProtectedReleaseV2,
)
from loom_capacity_agent.admission_convergence import (
    ProtectedAdmissionPlanCoordinator,
    ProtectedAdmissionPlanError,
    _build_protected_admission_convergence,
)
from loom_capacity_agent.claim_guard import (
    ExecutableClaimProposalV2,
    ExecutableClaimReceiptV2,
    InertAttemptTransitionV1,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.executable_admission import (
    ExecutableAdmissionError,
    ExecutableAdmissionStore,
)
from loom_capacity_agent.executable_bootstrap import (
    ProtectedExecutableBootstrapCoordinator,
)
from loom_capacity_agent.lifecycle_store import CapacityAttemptLifecycleStore
from loom_capacity_agent.prepared_store import CapacityPreparedAdmissionStore
from loom_capacity_agent.store import (
    acknowledge_executable_protected_release_publication,
    capture_lifecycle_demand_observation,
    read_next_executable_protected_release,
)
from loom_capacity_guard.contracts import (
    GuardFenceV1,
    ProtectedAttemptV1,
    SealedRequirementsV1,
)
from loom_capacity_guard.contracts import canonical_bytes as guard_canonical_bytes
from loom_capacity_guard.contracts import canonical_digest as guard_canonical_digest
from loom_capacity_manager.contracts import ResourceVectorV1, WorkerShapeV1
from loom_capacity_manager.contracts import (
    canonical_digest as manager_canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableAdmissionAllowanceV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableAdmissionShapeV2,
    ExecutableBootstrapProposalV2,
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.integration.test_capacity_agent_store import (
    _fence,
    _initialize_and_register,
    _owner_session,
    _registration,
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


@asynccontextmanager
async def _serializable_agent_session(
    database: dict[str, object],
) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(
        make_url(_value(database, "agent_url")), isolation_level="SERIALIZABLE"
    )
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


def _admission_configuration(
    registration: AgentRegistrationV1,
) -> ReporterConfigurationV1:
    return ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        protected_admission_sha256="e" * 64,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-test-capability",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


async def _seed_lifecycle_attempt(
    database: dict[str, object],
    *,
    protected_attempt_id: UUID,
    execution_generation: int = 7,
    required_pool: Literal["oldlab", "gb10"] = "oldlab",
) -> ProtectedAttemptV1:
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="x86_64",
        gpu_vendor="none",
        network_policies=("public",),
        required_pool=required_pool,
    )
    attempt = ProtectedAttemptV1(
        trial_id=_seed_trial(database),
        protected_attempt_id=protected_attempt_id,
        execution_generation=execution_generation,
        requirements_digest=guard_canonical_digest(requirements),
    )
    async with _owner_session(database) as (_, guard_store, _session):
        await guard_store.register_trial_attempt(attempt, requirements)
    return attempt


async def _initialize_manager_bound_admission_agent(
    database: dict[str, object],
    *,
    binding: ExecutableIntentBindingV2,
    reporter_incarnation: UUID,
    protected_admission_sha256: str,
) -> tuple[AgentRegistrationV1, ReporterConfigurationV1]:
    """Create a real guard authority bound to one manager-owned subject."""

    fence = GuardFenceV1(
        environment_id="dev-e2e",
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        authority_incarnation=UUID(int=701),
        reporter_incarnation=reporter_incarnation,
        deployment_generation=binding.deployment_generation,
        configuration_generation=1,
        candidate_digest=binding.candidate.publication_sha256,
    )
    registration = _registration(fence).model_copy(
        update={
            "candidate_identity_algorithm": binding.candidate.algorithm,
            "candidate_identity": binding.candidate.identity,
            "candidate_publication_sha256": binding.candidate.publication_sha256,
        }
    )
    configuration = ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        protected_admission_sha256=protected_admission_sha256,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id=f"{binding.pool_id}-manager-guard-e2e",
                pool_id=binding.pool_id,
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )
    async with _owner_session(database) as (agent_store, guard_store, _session):
        await guard_store.initialize_disabled_authority(fence)
        await agent_store.register_agent(registration)
    return registration, configuration


def _admission_proposal(
    configuration: ReporterConfigurationV1,
    attempts: tuple[ProtectedAttemptV1, ...],
    *,
    request: ExecutableBootstrapRegistrationV2 | None = None,
) -> ExecutableAdmissionPlanProposalV2:
    template = (
        request
        or _bootstrap(
            configuration.subject_id,
            configuration.subject_incarnation,
        )
    ).binding
    resources = ResourceVectorV1(
        slots=max(1, len(attempts)),
        cpu_millicores=1000 * max(1, len(attempts)),
        memory_bytes=1024 * max(1, len(attempts)),
    )
    binding = template.model_copy(
        update={
            "concurrency_slots": max(1, len(attempts)),
            "resources": resources,
        }
    )
    worker_shape = WorkerShapeV1(
        shape_id=binding.shape_id,
        concurrency_slots=binding.concurrency_slots,
        total_resources=resources,
        node_resources=(resources,),
        compatible_domain_ids=("oldlab-x86",),
        capabilities=(
            "cpu_arch.x86_64",
            "gpu_vendor.none",
            "network.public",
            "os.linux",
        ),
    )
    shape = ExecutableAdmissionShapeV2(
        binding=binding,
        protocol_generation=1,
        protocol_digest="5" * 64,
        worker_shape=worker_shape,
        worker_shape_digest=manager_canonical_digest(worker_shape),
        bootstrap_registration_epoch=1,
    )
    return ExecutableAdmissionPlanProposalV2(
        proposal_id=uuid4(),
        plan_id=uuid4(),
        admission_incarnation=uuid4(),
        reporter_incarnation=configuration.reporter_incarnation,
        protected_admission_sha256=configuration.protected_admission_sha256,
        manager_input_digest="6" * 64,
        manager_allocation_digest="7" * 64,
        lease_not_after=datetime.now(UTC) + timedelta(minutes=5),
        shapes=(shape,),
        allowances=tuple(
            ExecutableAdmissionAllowanceV2(
                allowance_id=uuid4(),
                protected_attempt_id=attempt.protected_attempt_id,
                shape_instance_id=binding.shape_instance_id,
                shape_slot_index=index,
                submission_intent_id=binding.intent_id,
            )
            for index, attempt in enumerate(attempts)
        ),
    )


async def _capture_lifecycle(
    database: dict[str, object],
    registration: AgentRegistrationV1,
    *,
    expected_high_water: int,
) -> GuardLifecycleDemandObservationV2:
    async with _serializable_agent_session(database) as session:
        return await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=expected_high_water,
            max_attempts=100,
        )


async def _prepare_abandonment_claim_race(
    database: dict[str, object],
) -> tuple[
    AgentRegistrationV1,
    ReporterConfigurationV1,
    ExecutableClaimProposalV2,
    ExecutableAdmissionPlanClosureV2,
    GuardLifecycleDemandObservationV2,
    UUID,
]:
    _fence, registration = await _initialize_and_register(database)
    configuration = _admission_configuration(registration)
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    attempt = await _seed_lifecycle_attempt(
        database,
        protected_attempt_id=UUID(int=341),
    )
    proposal = _admission_proposal(
        configuration,
        (attempt,),
        request=request,
    )
    pending = await _capture_lifecycle(
        database,
        registration,
        expected_high_water=0,
    )
    async with _serializable_agent_session(database) as session:
        await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)
    assigned = await _capture_lifecycle(
        database,
        registration,
        expected_high_water=1,
    )
    async with _serializable_executor_session(database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)
    return (
        registration,
        configuration,
        ExecutableClaimProposalV2(
            operation_id=UUID(int=342),
            protected_attempt_id=attempt.protected_attempt_id,
            execution_generation=attempt.execution_generation,
            requirements_digest=attempt.requirements_digest,
            worker_id=worker.worker_id,
            worker_incarnation=worker.worker_incarnation,
            expected_claim_high_water=0,
        ),
        ExecutableAdmissionPlanClosureV2(
            closure_id=UUID(int=343),
            proposal=proposal,
            close_reason="manager-closed",
        ),
        assigned,
        request.binding.intent_id,
    )


@pytest.mark.asyncio
async def test_protected_admission_convergence_commits_and_exactly_replays(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempts = (
        await _seed_lifecycle_attempt(
            capacity_guard_database,
            protected_attempt_id=UUID(int=301),
        ),
        await _seed_lifecycle_attempt(
            capacity_guard_database,
            protected_attempt_id=UUID(int=302),
        ),
    )
    proposal = _admission_proposal(configuration, attempts)
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        first = await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    assigned = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=1,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        replay = await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, assigned)

    assert replay == first
    assert first.acknowledgement.assignment_count == 2
    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.prepared_admission_plans) "
                        "AS plans, "
                        "(SELECT count(*) FROM loom_capacity_guard.prepared_worker_shapes) "
                        "AS shapes, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.prepared_placement_allowances) AS allowances, "
                        "(SELECT count(*) FROM loom_capacity_guard.attempt_lifecycle_heads "
                        "WHERE lifecycle_state = 'assigned') AS assigned"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"plans": 1, "shapes": 1, "allowances": 2, "assigned": 2}


@pytest.mark.asyncio
async def test_failed_publication_rejects_retry_after_local_withdrawal(
    capacity_guard_database: dict[str, object],
) -> None:
    """A withdrawal after a failed send must invalidate the cached acknowledgement."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=303),
    )
    proposal = _admission_proposal(configuration, (attempt,))
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    convergence = _build_protected_admission_convergence(
        configuration,
        proposal,
        pending,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        work = await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    with pytest.raises(RuntimeError, match="manager unavailable"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            authorized = await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).authorize_publication(work)
            assert authorized == work
            raise RuntimeError("manager unavailable")

    assignment = convergence.transitions[0]
    withdrawal = assignment.model_copy(
        update={
            "transition_id": uuid4(),
            "expected_transition_sequence": 1,
            "operation": "withdraw",
            "expected_state": "assigned",
            "target_state": "pending-unassigned",
            "transition_reason": "manager-admission-closed",
        }
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await CapacityAttemptLifecycleStore(
            session,
            registration=registration,
        ).apply_transition(withdrawal)

    with pytest.raises(DBAPIError, match="current protected assignment"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).authorize_publication(work)


@pytest.mark.asyncio
async def test_protected_admission_convergence_rejects_assigned_replay_after_terminal(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch an obsolete assigned observation authorizing an acknowledgement."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=303),
    )
    proposal = _admission_proposal(configuration, (attempt,))
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    convergence = _build_protected_admission_convergence(
        configuration,
        proposal,
        pending,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    assigned = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=1,
    )
    terminal = convergence.transitions[0].model_copy(
        update={
            "transition_id": UUID(int=304),
            "expected_transition_sequence": 1,
            "operation": "cancel",
            "expected_state": "assigned",
            "target_state": "cancelled-terminal",
            "transition_reason": "owner-cancelled-assigned",
        }
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await CapacityAttemptLifecycleStore(
            session,
            registration=registration,
        ).apply_transition(terminal)

    with pytest.raises(DBAPIError, match="current protected assignment"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).converge(proposal, assigned)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        head = (
            await session.execute(
                text(
                    "SELECT lifecycle_state, transition_sequence "
                    "FROM loom_capacity_guard.attempt_lifecycle_heads "
                    "WHERE protected_attempt_id = :protected_attempt_id"
                ),
                {"protected_attempt_id": attempt.protected_attempt_id},
            )
        ).one()
    assert head == ("cancelled-terminal", 2)


@pytest.mark.asyncio
async def test_protected_admission_convergence_rejects_assigned_replay_after_claim(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch a claimed attempt replaying its pre-claim acknowledgement."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=305),
    )
    proposal = _admission_proposal(
        configuration,
        (attempt,),
        request=request,
    )
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    assigned = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=1,
    )
    claim = ExecutableClaimProposalV2(
        operation_id=UUID(int=306),
        protected_attempt_id=attempt.protected_attempt_id,
        execution_generation=attempt.execution_generation,
        requirements_digest=attempt.requirements_digest,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=0,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)
        assert await store.admit_claim(claim) is not None

    with pytest.raises(DBAPIError, match="current protected assignment"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).converge(proposal, assigned)

    assert await _claim_terminal_counts(capacity_guard_database) == (1, 0, 1)


@pytest.mark.asyncio
async def test_protected_admission_abandonment_rejects_existing_executable_claim(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch cleanup withdrawing an assignment after its claim committed."""

    registration, configuration, claim, closure, assigned, _intent_id = (
        await _prepare_abandonment_claim_race(capacity_guard_database)
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        admitted = await ExecutableAdmissionStore(
            session,
            registration=registration,
        ).admit_claim(claim)
        assert admitted is not None

    with pytest.raises(DBAPIError, match="executable claim"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).abandon(closure, assigned)

    assert await _abandonment_claim_outcome(
        capacity_guard_database,
        protected_attempt_id=claim.protected_attempt_id,
        plan_id=closure.proposal.plan_id,
    ) == ("assigned", 1, 0)


@pytest.mark.asyncio
async def test_never_converged_tombstone_rejects_recomputed_noncanonical_nested_digest(
    capacity_guard_database: dict[str, object],
) -> None:
    """Direct SQL cannot redefine a closure digest over non-canonical bytes."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="6" * 64,
    )
    proposal = _admission_proposal(configuration, (), request=request)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="manager-closed",
    )
    tombstone = NeverConvergedAdmissionPlanV1(
        **registration.model_dump(mode="python", exclude_none=False),
        registration_digest=guard_canonical_digest(registration),
        closure=closure,
        closure_digest=canonical_executable_digest(closure),
        proposal_digest=canonical_executable_digest(proposal),
    )
    noncanonical_closure = canonical_executable_bytes(closure) + b" "
    forged = tombstone.model_copy(
        update={"closure_digest": hashlib.sha256(noncanonical_closure).hexdigest()}
    )

    with pytest.raises(DBAPIError, match=r"canonical|tombstone payload changed"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard."
                    "tombstone_never_converged_admission_plan("
                    ":agent_incarnation, CAST(:payload AS jsonb), "
                    "CAST(:canonical_payload AS bytea), :payload_digest, "
                    "CAST(:registration_payload AS bytea), :registration_digest, "
                    "CAST(:closure_payload AS bytea), :closure_digest, "
                    "CAST(:proposal_payload AS bytea), :proposal_digest)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "payload": guard_canonical_bytes(forged).decode("ascii"),
                    "canonical_payload": guard_canonical_bytes(forged),
                    "payload_digest": guard_canonical_digest(forged),
                    "registration_payload": guard_canonical_bytes(registration),
                    "registration_digest": tombstone.registration_digest,
                    "closure_payload": noncanonical_closure,
                    "closure_digest": forged.closure_digest,
                    "proposal_payload": canonical_executable_bytes(proposal),
                    "proposal_digest": tombstone.proposal_digest,
                },
            )


@pytest.mark.parametrize(
    "changed_field",
    (
        "deployment_generation",
        "candidate",
        "protected_admission_sha256",
        "pool_id",
    ),
)
@pytest.mark.asyncio
async def test_never_converged_tombstone_sql_rejects_untrusted_local_binding(
    capacity_guard_database: dict[str, object],
    changed_field: str,
) -> None:
    """The SECURITY DEFINER boundary must rebind every local proposal fact."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="6" * 64,
    )
    proposal = _admission_proposal(configuration, (), request=request)
    binding_update: dict[str, object] = {}
    proposal_update: dict[str, object] = {}
    if changed_field == "deployment_generation":
        binding_update[changed_field] = proposal.shapes[0].binding.deployment_generation + 1
    elif changed_field == "candidate":
        binding_update[changed_field] = CandidateBindingV2(
            algorithm="git-sha1",
            identity="1" * 40,
            publication_sha256=proposal.shapes[0].binding.candidate.publication_sha256,
        )
    elif changed_field == "protected_admission_sha256":
        proposal_update[changed_field] = "f" * 64
    elif changed_field == "pool_id":
        binding_update[changed_field] = "gb10"
    else:  # pragma: no cover - parametrization is closed above
        raise AssertionError(changed_field)
    if binding_update:
        proposal_update["shapes"] = (
            proposal.shapes[0].model_copy(
                update={
                    "binding": proposal.shapes[0].binding.model_copy(
                        update=binding_update
                    )
                }
            ),
        )
    changed = proposal.model_copy(update=proposal_update)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=changed,
        close_reason="manager-closed",
    )
    tombstone = NeverConvergedAdmissionPlanV1(
        **registration.model_dump(mode="python", exclude_none=False),
        registration_digest=guard_canonical_digest(registration),
        closure=closure,
        closure_digest=canonical_executable_digest(closure),
        proposal_digest=canonical_executable_digest(changed),
    )

    with pytest.raises(DBAPIError, match=r"binding|bootstrap|tombstone payload changed"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(
                session,
                registration=configuration,
            ).tombstone_never_converged_plan(tombstone)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forgery",
    ("deployment", "candidate", "pool", "bootstrap-registration"),
)
async def test_never_converged_tombstone_sql_rejects_untrusted_later_shape(
    capacity_guard_database: dict[str, object],
    forgery: str,
) -> None:
    """Every shape, not only the canonical anchor, must match protected bootstrap."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    first_request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="6" * 64,
    )
    second_template = first_request.model_copy(
        update={
            "binding": first_request.binding.model_copy(
                update={
                    "intent_id": uuid4(),
                    "shape_instance_id": (
                        f"{first_request.binding.shape_instance_id}-second"
                    ),
                }
            )
        }
    )
    second_request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="7" * 64,
        request=second_template,
    )
    first = _admission_proposal(configuration, (), request=first_request)
    second_shape = first.shapes[0].model_copy(
        update={
            "binding": second_request.binding,
            "bootstrap_registration_epoch": (
                second_request.bootstrap_registration_epoch
            ),
        }
    )
    proposal = ExecutableAdmissionPlanProposalV2.model_validate(
        first.model_dump(mode="python")
        | {"shapes": (first.shapes[0], second_shape)}
    )
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="manager-closed",
    )
    tombstone = NeverConvergedAdmissionPlanV1(
        **registration.model_dump(mode="python", exclude_none=False),
        registration_digest=guard_canonical_digest(registration),
        closure=closure,
        closure_digest=canonical_executable_digest(closure),
        proposal_digest=canonical_executable_digest(proposal),
    )
    forged_proposal_payload = proposal.model_dump(mode="json", exclude_none=False)
    forged_shape = forged_proposal_payload["shapes"][1]
    if forgery == "deployment":
        forged_shape["binding"]["deployment_generation"] += 1
    elif forgery == "candidate":
        forged_shape["binding"]["candidate"]["identity"] = "1" * 40
    elif forgery == "pool":
        forged_shape["binding"]["pool_id"] = (
            "gb10" if forged_shape["binding"]["pool_id"] != "gb10" else "oldlab"
        )
    elif forgery == "bootstrap-registration":
        forged_shape["bootstrap_registration_epoch"] += 1
    else:  # pragma: no cover - parametrization is closed above
        raise AssertionError(forgery)
    proposal_payload = json.dumps(
        forged_proposal_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    proposal_digest = hashlib.sha256(proposal_payload).hexdigest()
    forged_closure_payload = closure.model_dump(mode="json", exclude_none=False)
    forged_closure_payload["proposal"] = forged_proposal_payload
    closure_payload = json.dumps(
        forged_closure_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    closure_digest = hashlib.sha256(closure_payload).hexdigest()
    forged_tombstone_payload = tombstone.model_dump(mode="json", exclude_none=False)
    forged_tombstone_payload.update(
        {
            "closure": forged_closure_payload,
            "closure_digest": closure_digest,
            "proposal_digest": proposal_digest,
        }
    )
    canonical_payload = json.dumps(
        forged_tombstone_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")

    with pytest.raises(DBAPIError, match=r"bootstrap|binding|tombstone payload changed"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard."
                    "tombstone_never_converged_admission_plan("
                    ":agent_incarnation, CAST(:payload AS jsonb), "
                    "CAST(:canonical_payload AS bytea), :payload_digest, "
                    "CAST(:registration_payload AS bytea), :registration_digest, "
                    "CAST(:closure_payload AS bytea), :closure_digest, "
                    "CAST(:proposal_payload AS bytea), :proposal_digest)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "payload": canonical_payload.decode("ascii"),
                    "canonical_payload": canonical_payload,
                    "payload_digest": hashlib.sha256(canonical_payload).hexdigest(),
                    "registration_payload": guard_canonical_bytes(registration),
                    "registration_digest": tombstone.registration_digest,
                    "closure_payload": closure_payload,
                    "closure_digest": closure_digest,
                    "proposal_payload": proposal_payload,
                    "proposal_digest": proposal_digest,
                },
            )


@pytest.mark.asyncio
async def test_never_converged_tombstone_exactly_replays_and_blocks_later_prepare(
    capacity_guard_database: dict[str, object],
) -> None:
    """An exact negative receipt is append-only and permanently excludes its plan."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="6" * 64,
    )
    proposal = _admission_proposal(configuration, (), request=request)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="manager-closed",
    )

    async with _serializable_agent_session(capacity_guard_database) as session:
        first = await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).close(closure, None)
    async with _serializable_agent_session(capacity_guard_database) as session:
        replay = await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).close(closure, None)

    assert replay == first
    assert isinstance(first.disposition, NeverConvergedAdmissionPlanV1)
    assert first.acknowledgement.disposition_kind == "never-converged"
    tombstone = first.disposition
    exact_registration = AgentRegistrationV1.model_validate(
        {
            field: getattr(tombstone, field)
            for field in AgentRegistrationV1.model_fields
        }
    )
    with pytest.raises(DBAPIError, match=r"tombstone payload changed"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard."
                    "tombstone_never_converged_admission_plan("
                    ":agent_incarnation, CAST(:payload AS jsonb), "
                    "CAST(:canonical_payload AS bytea), :payload_digest, "
                    "CAST(:registration_payload AS bytea), :registration_digest, "
                    "CAST(:closure_payload AS bytea), :closure_digest, "
                    "CAST(:proposal_payload AS bytea), :proposal_digest)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "payload": guard_canonical_bytes(tombstone).decode("ascii"),
                    "canonical_payload": guard_canonical_bytes(tombstone),
                    "payload_digest": guard_canonical_digest(tombstone),
                    "registration_payload": guard_canonical_bytes(exact_registration),
                    "registration_digest": tombstone.registration_digest,
                    "closure_payload": canonical_executable_bytes(closure) + b" ",
                    "closure_digest": tombstone.closure_digest,
                    "proposal_payload": canonical_executable_bytes(proposal),
                    "proposal_digest": tombstone.proposal_digest,
                },
            )
    with pytest.raises(DBAPIError, match=r"never-converged|tombstone|closed"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).converge(
                proposal,
                GuardLifecycleDemandObservationV2(
                    **registration.model_dump(mode="python"),
                    sequence=1,
                    source_observed_at=datetime.now(UTC),
                    attempts=(),
                ),
            )

    changed = closure.model_copy(update={"closure_id": uuid4()})
    with pytest.raises(DBAPIError, match=r"replay|changed|tombstone"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).close(changed, None)

    for statement in (
        "UPDATE loom_capacity_guard.never_converged_admission_plans "
        "SET proposal_digest = proposal_digest",
        "DELETE FROM loom_capacity_guard.never_converged_admission_plans",
        "TRUNCATE loom_capacity_guard.never_converged_admission_plans CASCADE",
    ):
        with pytest.raises(DBAPIError, match="append-only"):
            async with _owner_session(capacity_guard_database) as (_, _, session):
                await session.execute(text(statement))


async def _run_prepare_or_tombstone(
    database: dict[str, object],
    *,
    configuration: ReporterConfigurationV1,
    proposal: ExecutableAdmissionPlanProposalV2,
    closure: ExecutableAdmissionPlanClosureV2,
    prepare: bool,
    backend_pid: asyncio.Future[int],
) -> object:
    try:
        async with _serializable_agent_session(database) as session:
            backend_pid.set_result(
                (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            )
            if prepare:
                convergence = _build_protected_admission_convergence(
                    configuration,
                    proposal,
                    GuardLifecycleDemandObservationV2(
                        **{
                            field: getattr(configuration, field)
                            for field in AgentRegistrationV1.model_fields
                        },
                        sequence=1,
                        source_observed_at=datetime.now(UTC),
                        attempts=(),
                    ),
                )
                return await CapacityPreparedAdmissionStore(
                    session,
                    registration=configuration,
                ).prepare_plan(convergence.plan)
            return await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).close(closure, None)
    except DBAPIError as exc:
        return exc


@pytest.mark.asyncio
async def test_prepare_and_never_converged_tombstone_allow_only_one_commit(
    capacity_guard_database: dict[str, object],
) -> None:
    """Both commits serialize on the guard authority and enforce mutual exclusion."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="6" * 64,
    )
    proposal = _admission_proposal(configuration, (), request=request)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="manager-closed",
    )
    loop = asyncio.get_running_loop()
    prepare_pid: asyncio.Future[int] = loop.create_future()
    tombstone_pid: asyncio.Future[int] = loop.create_future()
    prepare_task: asyncio.Task[object] | None = None
    tombstone_task: asyncio.Task[object] | None = None
    try:
        async with _owner_session(capacity_guard_database) as (_, _, blocker):
            blocker_pid = (
                await blocker.execute(text("SELECT pg_backend_pid()"))
            ).scalar_one()
            await blocker.execute(
                text(
                    "SELECT singleton_id FROM loom_capacity_guard.agent_runtime_authority "
                    "WHERE singleton_id = 1 FOR UPDATE"
                )
            )
            prepare_task = asyncio.create_task(
                _run_prepare_or_tombstone(
                    capacity_guard_database,
                    configuration=configuration,
                    proposal=proposal,
                    closure=closure,
                    prepare=True,
                    backend_pid=prepare_pid,
                )
            )
            assert await _backend_waited_for_backend(
                capacity_guard_database,
                blocked_pid=await prepare_pid,
                blocking_pid=blocker_pid,
                task=prepare_task,
            )
            tombstone_task = asyncio.create_task(
                _run_prepare_or_tombstone(
                    capacity_guard_database,
                    configuration=configuration,
                    proposal=proposal,
                    closure=closure,
                    prepare=False,
                    backend_pid=tombstone_pid,
                )
            )
            assert await _backend_waited_for_backend(
                capacity_guard_database,
                blocked_pid=await tombstone_pid,
                blocking_pid=await prepare_pid,
                task=tombstone_task,
            )
        outcomes = await asyncio.gather(prepare_task, tombstone_task)
    finally:
        for task in (prepare_task, tombstone_task):
            if task is not None and not task.done():
                await task

    assert sum(isinstance(outcome, DBAPIError) for outcome in outcomes) == 1
    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            await session.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM loom_capacity_guard.prepared_admission_plans) "
                    "AS prepared, "
                    "(SELECT count(*) FROM "
                    "loom_capacity_guard.never_converged_admission_plans) AS tombstoned"
                )
            )
        ).one()
    assert counts.prepared + counts.tombstoned == 1


@pytest.mark.asyncio
async def test_claim_first_serializes_plan_abandonment_before_withdrawal(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch cleanup missing a claim that is committing behind its lifecycle lock."""

    registration, configuration, claim, closure, assigned, intent_id = (
        await _prepare_abandonment_claim_race(capacity_guard_database)
    )
    loop = asyncio.get_running_loop()
    claim_pid: asyncio.Future[int] = loop.create_future()
    cleanup_pid: asyncio.Future[int] = loop.create_future()
    claim_task: asyncio.Task[object] | None = None
    cleanup_task: asyncio.Task[object] | None = None
    try:
        async with _owner_session(capacity_guard_database) as (_, _, blocker):
            blocker_backend_pid = (
                await blocker.execute(text("SELECT pg_backend_pid()"))
            ).scalar_one()
            locked_intent = await blocker.execute(
                text(
                    "SELECT intent_id FROM loom_capacity_guard.executable_claim_state "
                    "WHERE intent_id = :intent_id FOR UPDATE"
                ),
                {"intent_id": intent_id},
            )
            assert locked_intent.scalar_one() == intent_id

            claim_task = asyncio.create_task(
                _run_claim_transaction(
                    capacity_guard_database,
                    registration,
                    claim,
                    claim_pid,
                )
            )
            claim_backend_pid = await claim_pid
            assert await _backend_waited_for_backend(
                capacity_guard_database,
                blocked_pid=claim_backend_pid,
                blocking_pid=blocker_backend_pid,
                task=claim_task,
            )

            cleanup_task = asyncio.create_task(
                _run_abandonment_transaction(
                    capacity_guard_database,
                    registration,
                    configuration,
                    closure,
                    assigned,
                    cleanup_pid,
                )
            )
            assert await _backend_waited_for_backend(
                capacity_guard_database,
                blocked_pid=await cleanup_pid,
                blocking_pid=claim_backend_pid,
                task=cleanup_task,
            )

        claim_result, cleanup_result = await asyncio.gather(
            claim_task,
            cleanup_task,
        )
    finally:
        if claim_task is not None and not claim_task.done():
            await claim_task
        if cleanup_task is not None and not cleanup_task.done():
            await cleanup_task

    assert isinstance(claim_result, ExecutableClaimReceiptV2)
    assert isinstance(cleanup_result, DBAPIError)
    assert await _abandonment_claim_outcome(
        capacity_guard_database,
        protected_attempt_id=claim.protected_attempt_id,
        plan_id=closure.proposal.plan_id,
    ) == ("assigned", 1, 0)


@pytest.mark.asyncio
async def test_current_assignment_assertion_rejects_payload_equivocation(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch a historical transition identity being paired with changed bytes."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=307),
    )
    proposal = _admission_proposal(configuration, (attempt,))
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    convergence = _build_protected_admission_convergence(
        configuration,
        proposal,
        pending,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    equivocated = convergence.transitions[0].model_copy(
        update={"transition_reason": "equivocated-manager-placement"}
    )
    with pytest.raises(DBAPIError, match="current protected assignment"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await CapacityAttemptLifecycleStore(
                session,
                registration=registration,
            ).assert_current_assignment(equivocated)


@pytest.mark.asyncio
async def test_current_assignment_assertion_requires_serializable_transaction(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=308),
    )
    proposal = _admission_proposal(configuration, (attempt,))
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    convergence = _build_protected_admission_convergence(
        configuration,
        proposal,
        pending,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    with pytest.raises(DBAPIError, match="SERIALIZABLE"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityAttemptLifecycleStore(
                session,
                registration=registration,
            ).assert_current_assignment(convergence.transitions[0])


@pytest.mark.asyncio
async def test_current_assignment_assertion_rejects_expired_prepared_plan(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=309),
    )
    proposal = _admission_proposal(configuration, (attempt,)).model_copy(
        update={"lease_not_after": datetime.now(UTC) + timedelta(seconds=1)}
    )
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    convergence = _build_protected_admission_convergence(
        configuration,
        proposal,
        pending,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    await asyncio.sleep(1.1)
    with pytest.raises(DBAPIError, match="current protected assignment"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await CapacityAttemptLifecycleStore(
                session,
                registration=registration,
            ).assert_current_assignment(convergence.transitions[0])


@pytest.mark.asyncio
async def test_current_assignment_assertion_rejects_cancelled_public_trial(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=310),
    )
    proposal = _admission_proposal(configuration, (attempt,))
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    convergence = _build_protected_admission_convergence(
        configuration,
        proposal,
        pending,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await ProtectedAdmissionPlanCoordinator(
            session,
            configuration=configuration,
        ).converge(proposal, pending)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.trials SET cancellation_requested_at = now() "
                    "WHERE id = :trial_id"
                ),
                {"trial_id": attempt.trial_id},
            )
    finally:
        admin.dispose()

    with pytest.raises(DBAPIError, match="current protected assignment"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await CapacityAttemptLifecycleStore(
                session,
                registration=registration,
            ).assert_current_assignment(convergence.transitions[0])


@pytest.mark.asyncio
async def test_protected_admission_convergence_rolls_back_every_new_row_on_late_conflict(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempts = (
        await _seed_lifecycle_attempt(
            capacity_guard_database,
            protected_attempt_id=UUID(int=311),
        ),
        await _seed_lifecycle_attempt(
            capacity_guard_database,
            protected_attempt_id=UUID(int=312),
        ),
    )
    proposal = _admission_proposal(configuration, attempts)
    stale = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    prepared = _build_protected_admission_convergence(configuration, proposal, stale)
    second = prepared.transitions[1]
    first_attempt_id = next(
        attempt.protected_attempt_id
        for attempt in attempts
        if attempt.protected_attempt_id != second.protected_attempt_id
    )
    terminal = InertAttemptTransitionV1(
        **registration.model_dump(mode="python"),
        transition_id=uuid4(),
        protected_attempt_id=second.protected_attempt_id,
        execution_generation=second.execution_generation,
        requirements_digest=second.requirements_digest,
        expected_transition_sequence=0,
        operation="cancel",
        expected_state="pending-unassigned",
        target_state="cancelled-terminal",
        transition_reason="owner-cancelled-unclaimed",
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        await CapacityAttemptLifecycleStore(
            session,
            registration=registration,
        ).apply_transition(terminal)

    with pytest.raises(DBAPIError, match="lifecycle"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).converge(proposal, stale)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.prepared_admission_plans) "
                        "AS plans, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.prepared_placement_allowances) AS allowances, "
                        "(SELECT count(*) FROM loom_capacity_guard.attempt_lifecycle_heads "
                        "WHERE protected_attempt_id = :first_attempt "
                        "AND lifecycle_state = 'pending-unassigned') AS first_pending, "
                        "(SELECT count(*) FROM loom_capacity_guard.attempt_lifecycle_heads "
                        "WHERE protected_attempt_id = :second_attempt "
                        "AND lifecycle_state = 'cancelled-terminal') AS second_terminal"
                    ),
                    {
                        "first_attempt": first_attempt_id,
                        "second_attempt": second.protected_attempt_id,
                    },
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {
        "plans": 0,
        "allowances": 0,
        "first_pending": 1,
        "second_terminal": 1,
    }


@pytest.mark.asyncio
async def test_protected_admission_convergence_rejects_nonserializable_caller(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=321),
    )
    proposal = _admission_proposal(configuration, (attempt,))
    observation = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    with pytest.raises(ProtectedAdmissionPlanError, match="SERIALIZABLE"):
        async with _agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).converge(proposal, observation)


@pytest.mark.asyncio
async def test_protected_admission_convergence_rejects_stale_local_generation(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    configuration = _admission_configuration(registration)
    attempt = await _seed_lifecycle_attempt(
        capacity_guard_database,
        protected_attempt_id=UUID(int=331),
    )
    proposal = _admission_proposal(configuration, (attempt,))
    observation = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )
    stale_attempt = observation.attempts[0].model_copy(
        update={"execution_generation": attempt.execution_generation + 1}
    )
    stale = observation.model_copy(update={"attempts": (stale_attempt,)})

    with pytest.raises(DBAPIError, match="allowance"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).converge(proposal, stale)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        assert (
            await session.execute(
                text("SELECT count(*) FROM loom_capacity_guard.prepared_admission_plans")
            )
        ).scalar_one() == 0


async def _assign_protected_attempts(
    database: dict[str, object],
    *,
    registration: AgentRegistrationV1,
    assignments: tuple[
        tuple[ExecutableBootstrapRegistrationV2, UUID, int, str],
        ...,
    ],
) -> PreparedAdmissionPlanV1:
    if not assignments:
        raise ValueError("protected assignment fixture requires work")
    binding = assignments[0][0].binding
    prepared_shapes: list[PreparedWorkerShapeV1] = []
    allowances: list[PreparedPlacementAllowanceV1] = []
    for request, protected_attempt_id, execution_generation, requirements_digest in assignments:
        item = request.binding
        if (
            item.execution != binding.execution
            or item.pool_id != binding.pool_id
            or item.pool_generation != binding.pool_generation
            or item.profile_id != binding.profile_id
            or item.profile_generation != binding.profile_generation
            or item.profile_digest != binding.profile_digest
        ):
            raise ValueError("protected assignment fixture requires one manager plan")
        worker_shape = WorkerShapeV1(
            shape_id=item.shape_id,
            concurrency_slots=item.concurrency_slots,
            total_resources=item.resources,
            node_resources=(item.resources,),
            compatible_domain_ids=(item.pool_id,),
            capabilities=(
                "cpu_arch.x86_64",
                "gpu_vendor.none",
                "network.public",
                "os.linux",
            ),
        )
        prepared_shapes.append(
            PreparedWorkerShapeV1(
                shape_instance_id=item.shape_instance_id,
                submission_intent_id=item.intent_id,
                pool_id=item.pool_id,
                pool_generation=item.pool_generation,
                profile_id=item.profile_id,
                profile_generation=item.profile_generation,
                profile_digest=item.profile_digest,
                protocol_generation=1,
                protocol_digest="5" * 64,
                worker_shape=worker_shape,
                worker_shape_digest=manager_canonical_digest(worker_shape),
                bootstrap_registration_epoch=request.bootstrap_registration_epoch,
            )
        )
        allowances.append(
            PreparedPlacementAllowanceV1(
                allowance_id=uuid4(),
                protected_attempt_id=protected_attempt_id,
                execution_generation=execution_generation,
                requirements_digest=requirements_digest,
                pool_id=item.pool_id,
                shape_instance_id=item.shape_instance_id,
                shape_slot_index=0,
                submission_intent_id=item.intent_id,
            )
        )
    protocol_generation = prepared_shapes[0].protocol_generation
    protocol_digest = prepared_shapes[0].protocol_digest
    plan_id = uuid4()
    admission_incarnation = uuid4()
    plan = PreparedAdmissionPlanV1(
        **registration.model_dump(mode="python"),
        plan_id=plan_id,
        admission_incarnation=admission_incarnation,
        manager_authority_incarnation=binding.execution.authority_incarnation,
        manager_writer_epoch=binding.execution.writer_epoch,
        manager_allocation_epoch=binding.execution.allocation_epoch,
        manager_input_digest="6" * 64,
        manager_allocation_digest="7" * 64,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        profile_id=binding.profile_id,
        profile_generation=binding.profile_generation,
        profile_digest=binding.profile_digest,
        protocol_generation=protocol_generation,
        protocol_digest=protocol_digest,
        lease_not_after=datetime.now(UTC) + timedelta(hours=1),
        worker_shapes=tuple(prepared_shapes),
        placement_allowances=tuple(allowances),
    )
    async with _agent_session(database) as session:
        await CapacityPreparedAdmissionStore(
            session,
            registration=registration,
        ).prepare_plan(plan)
        lifecycle = CapacityAttemptLifecycleStore(
            session,
            registration=registration,
        )
        for allowance in allowances:
            await lifecycle.apply_transition(
                InertAttemptTransitionV1(
                    **registration.model_dump(mode="python"),
                    transition_id=uuid4(),
                    protected_attempt_id=allowance.protected_attempt_id,
                    execution_generation=allowance.execution_generation,
                    requirements_digest=allowance.requirements_digest,
                    expected_transition_sequence=0,
                    operation="assign",
                    expected_state="pending-unassigned",
                    target_state="assigned",
                    allowance_id=allowance.allowance_id,
                    plan_id=plan_id,
                    admission_incarnation=admission_incarnation,
                    manager_allocation_epoch=plan.manager_allocation_epoch,
                    pool_id=plan.pool_id,
                    shape_instance_id=allowance.shape_instance_id,
                    submission_intent_id=allowance.submission_intent_id,
                    transition_reason="manager-placement",
                )
            )
    return plan


async def _assign_protected_attempt(
    database: dict[str, object],
    *,
    registration: AgentRegistrationV1,
    request: ExecutableBootstrapRegistrationV2,
    protected_attempt_id: UUID,
    execution_generation: int,
    requirements_digest: str,
) -> PreparedAdmissionPlanV1:
    return await _assign_protected_attempts(
        database,
        registration=registration,
        assignments=(
            (
                request,
                protected_attempt_id,
                execution_generation,
                requirements_digest,
            ),
        ),
    )


async def _prepare_claim_terminal_race(
    database: dict[str, object],
    *,
    assigned: bool = True,
) -> tuple[
    AgentRegistrationV1,
    ExecutableWorkerRegistrationV2,
    ExecutableClaimProposalV2,
    InertAttemptTransitionV1,
]:
    _fence, registration = await _initialize_and_register(database)
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    protected_attempt_id = UUID(int=149)
    requirements_digest = "8" * 64
    await _seed_protected_attempt(
        database,
        protected_attempt_id=protected_attempt_id,
        execution_generation=14,
        requirements_digest=requirements_digest,
    )
    plan = (
        await _assign_protected_attempt(
            database,
            registration=registration,
            request=request,
            protected_attempt_id=protected_attempt_id,
            execution_generation=14,
            requirements_digest=requirements_digest,
        )
        if assigned
        else None
    )
    async with _serializable_executor_session(database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            request,
            bootstrap_sha256=bootstrap_sha256,
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
            expected_transition_sequence=1 if assigned else 0,
            operation="cancel",
            expected_state="assigned" if assigned else "pending-unassigned",
            target_state="cancelled-terminal",
            allowance_id=plan.placement_allowances[0].allowance_id if plan else None,
            plan_id=plan.plan_id if plan else None,
            admission_incarnation=plan.admission_incarnation if plan else None,
            manager_allocation_epoch=plan.manager_allocation_epoch if plan else None,
            pool_id=plan.pool_id if plan else None,
            shape_instance_id=(plan.placement_allowances[0].shape_instance_id if plan else None),
            submission_intent_id=(
                plan.placement_allowances[0].submission_intent_id if plan else None
            ),
            transition_reason="claimed-attempt-terminal",
        ),
    )


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


async def _run_abandonment_transaction(
    database: dict[str, object],
    registration: AgentRegistrationV1,
    configuration: ReporterConfigurationV1,
    closure: ExecutableAdmissionPlanClosureV2,
    observation: GuardLifecycleDemandObservationV2,
    backend_pid: asyncio.Future[int],
) -> object:
    try:
        async with _serializable_agent_session(database) as session:
            backend_pid.set_result(
                (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            )
            return await ProtectedAdmissionPlanCoordinator(
                session,
                configuration=configuration,
            ).abandon(closure, observation)
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


async def _backend_waited_for_backend(
    database: dict[str, object],
    *,
    blocked_pid: int,
    blocking_pid: int,
    task: asyncio.Task[object],
) -> bool:
    engine = create_async_engine(make_url(_value(database, "admin_url")))
    try:
        async with engine.connect() as connection, asyncio.timeout(10):
            while not task.done():
                blocking_pids = (
                    await connection.execute(
                        text("SELECT pg_blocking_pids(:blocked_pid)"),
                        {"blocked_pid": blocked_pid},
                    )
                ).scalar_one()
                if blocking_pid in blocking_pids:
                    return True
                await asyncio.sleep(0.01)
        return False
    finally:
        await engine.dispose()


async def _application_waited_for_lock(
    database: dict[str, object],
    *,
    application_name: str,
    task: asyncio.Task[object],
) -> bool:
    engine = create_async_engine(make_url(_value(database, "admin_url")))
    try:
        async with engine.connect() as connection, asyncio.timeout(10):
            while not task.done():
                wait_event_type = (
                    await connection.execute(
                        text(
                            "SELECT wait_event_type FROM pg_stat_activity "
                            "WHERE application_name = :application_name"
                        ),
                        {"application_name": application_name},
                    )
                ).scalar_one_or_none()
                if wait_event_type == "Lock":
                    return True
                await asyncio.sleep(0.01)
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


async def _abandonment_claim_outcome(
    database: dict[str, object],
    *,
    protected_attempt_id: UUID,
    plan_id: UUID,
) -> tuple[str, int, int]:
    async with _owner_session(database) as (_, _, session):
        row = (
            await session.execute(
                text(
                    "SELECT head.lifecycle_state, "
                    "(SELECT count(*) FROM loom_capacity_guard.executable_claim_leases "
                    "WHERE protected_attempt_id = :protected_attempt_id) AS claims, "
                    "(SELECT count(*) FROM loom_capacity_guard.abandoned_admission_plans "
                    "WHERE plan_id = :plan_id) AS abandonments "
                    "FROM loom_capacity_guard.attempt_lifecycle_heads AS head "
                    "WHERE head.protected_attempt_id = :protected_attempt_id"
                ),
                {
                    "protected_attempt_id": protected_attempt_id,
                    "plan_id": plan_id,
                },
            )
        ).one()
    return row.lifecycle_state, row.claims, row.abandonments


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
            algorithm="source-sha256",
            identity="a" * 64,
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


async def _protect_bootstrap(
    database: dict[str, object],
    registration: AgentRegistrationV1,
    *,
    bootstrap_sha256: str,
    request: ExecutableBootstrapRegistrationV2 | None = None,
) -> ExecutableBootstrapRegistrationV2:
    template = request or _bootstrap(
        registration.subject_id,
        registration.subject_incarnation,
    )
    configuration = ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        protected_admission_sha256="e" * 64,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id=f"{template.binding.pool_id}-test-capability",
                pool_id=template.binding.pool_id,
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )
    proposal = ExecutableBootstrapProposalV2(
        binding=template.binding,
        command_sequence=template.command_sequence,
        proposal_epoch=1,
        bootstrap_sha256=bootstrap_sha256,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    async with _serializable_agent_session(database) as session:
        protected = await ProtectedExecutableBootstrapCoordinator(
            session,
            configuration=configuration,
        ).protect(proposal)
    return ExecutableBootstrapRegistrationV2(
        binding=protected.acknowledgement.binding,
        command_sequence=proposal.command_sequence,
        bootstrap_registration_epoch=(protected.acknowledgement.bootstrap_registration_epoch),
        bootstrap_evidence_sha256=(protected.acknowledgement.bootstrap_evidence_sha256),
    )


async def _reconfigure_registration(
    database: dict[str, object],
    fence: GuardFenceV1,
    registration: AgentRegistrationV1,
) -> AgentRegistrationV1:
    replacement_fence = fence.model_copy(
        update={
            "reporter_incarnation": UUID(int=204),
            "candidate_digest": "b" * 64,
            "deployment_generation": 8,
            "configuration_generation": 12,
        }
    )
    replacement_registration = registration.model_copy(
        update={
            "reporter_incarnation": replacement_fence.reporter_incarnation,
            "candidate_digest": replacement_fence.candidate_digest,
            "deployment_generation": replacement_fence.deployment_generation,
            "configuration_generation": replacement_fence.configuration_generation,
        }
    )
    async with _owner_session(database) as (agent_store, guard_store, _session):
        await guard_store.reconfigure_disabled_authority(
            replacement_fence,
            expected_configuration_generation=registration.configuration_generation,
        )
        await agent_store.reconfigure_agent(
            replacement_registration,
            expected_configuration_generation=registration.configuration_generation,
        )
    return replacement_registration


def _guard_downgrade_config(
    database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    *,
    application_name: str,
) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    migrator_url = make_url(_value(database, "migrator_url")).update_query_dict(
        {"application_name": application_name}
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_DB_URL",
        migrator_url.render_as_string(hide_password=False),
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OWNER_ROLE",
        _value(database, "owner_role"),
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_AGENT_ROLE",
        _value(database, "agent_role"),
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE",
        _value(database, "executor_role"),
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OBSERVER_ROLE",
        _value(database, "observer_role"),
    )
    return config


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
async def test_prepare_requires_official_protected_bootstrap_registration(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)

    async with _serializable_executor_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match="protected bootstrap"):
            await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).prepare_worker(request, bootstrap_sha256="6" * 64)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch",
    (
        "subject_id",
        "subject_incarnation",
        "intent_id",
        "binding",
        "command_sequence",
        "bootstrap_registration_epoch",
        "bootstrap_sha256",
        "bootstrap_evidence_sha256",
    ),
)
async def test_prepare_requires_every_official_protected_bootstrap_field(
    capacity_guard_database: dict[str, object],
    mismatch: str,
) -> None:
    """Catch executable preparation trusting any caller field over guard_0012."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    bootstrap_sha256 = "6" * 64
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    changed_bootstrap_sha256 = bootstrap_sha256
    if mismatch == "subject_id":
        request = request.model_copy(
            update={"binding": request.binding.model_copy(update={"subject_id": UUID(int=201)})}
        )
    elif mismatch == "subject_incarnation":
        request = request.model_copy(
            update={
                "binding": request.binding.model_copy(update={"subject_incarnation": UUID(int=202)})
            }
        )
    elif mismatch == "intent_id":
        request = request.model_copy(
            update={"binding": request.binding.model_copy(update={"intent_id": UUID(int=203)})}
        )
    elif mismatch == "binding":
        request = request.model_copy(
            update={
                "binding": request.binding.model_copy(
                    update={"shape_instance_id": "oldlab-shape-changed"}
                )
            }
        )
    elif mismatch == "command_sequence":
        request = request.model_copy(update={"command_sequence": 2})
    elif mismatch == "bootstrap_registration_epoch":
        request = request.model_copy(update={"bootstrap_registration_epoch": 2})
    elif mismatch == "bootstrap_sha256":
        changed_bootstrap_sha256 = "7" * 64
    elif mismatch == "bootstrap_evidence_sha256":
        request = request.model_copy(update={"bootstrap_evidence_sha256": "8" * 64})
    else:  # pragma: no cover - exhaustive parameter guard
        raise AssertionError(f"unknown protected-bootstrap mismatch: {mismatch}")

    canonical = json.dumps(
        request.model_dump(mode="json", exclude_none=False),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    async with _serializable_executor_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match=r"protected bootstrap|binding|schema"):
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
                    "bootstrap_sha256": changed_bootstrap_sha256,
                },
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt_subject_field", ("subject_id", "subject_incarnation"))
async def test_store_rejects_replay_receipt_for_another_subject_incarnation(
    capacity_guard_database: dict[str, object],
    receipt_subject_field: str,
) -> None:
    """Catch any executable procedure returning a cross-subject receipt."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    bootstrap_sha256 = "6" * 64
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        await ExecutableAdmissionStore(
            session,
            registration=registration,
        ).prepare_worker(request, bootstrap_sha256=bootstrap_sha256)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE loom_capacity_guard.executable_admission_events DISABLE TRIGGER USER"
            )
            connection.execute(
                text(
                    "UPDATE loom_capacity_guard.executable_admission_events "
                    f"SET receipt = jsonb_set(receipt, '{{{receipt_subject_field}}}', "
                    "to_jsonb(CAST(:wrong_subject AS text)), false) "
                    "WHERE operation_id = :operation_id"
                ),
                {
                    "wrong_subject": UUID(int=219),
                    "operation_id": request.binding.intent_id,
                },
            )
            connection.exec_driver_sql(
                "ALTER TABLE loom_capacity_guard.executable_admission_events ENABLE TRIGGER USER"
            )
    finally:
        admin.dispose()

    async with _serializable_executor_session(capacity_guard_database) as session:
        with pytest.raises(ExecutableAdmissionError, match="subject"):
            await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).prepare_worker(request, bootstrap_sha256=bootstrap_sha256)


@pytest.mark.asyncio
async def test_store_rejects_drain_receipt_for_another_intent(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch drain acknowledgement crossing its protected executable intent."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    drain = ExecutableDrainRequestV2(
        operation_id=UUID(int=220),
        binding=request.binding,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=0,
        drain_epoch=1,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)
        await store.begin_drain(drain)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE loom_capacity_guard.executable_admission_events DISABLE TRIGGER USER"
            )
            connection.execute(
                text(
                    "UPDATE loom_capacity_guard.executable_admission_events "
                    "SET receipt = jsonb_set(receipt, '{intent_id}', "
                    "to_jsonb(CAST(:wrong_intent AS text)), false) "
                    "WHERE operation_id = :operation_id"
                ),
                {
                    "wrong_intent": UUID(int=221),
                    "operation_id": drain.operation_id,
                },
            )
            connection.exec_driver_sql(
                "ALTER TABLE loom_capacity_guard.executable_admission_events ENABLE TRIGGER USER"
            )
    finally:
        admin.dispose()

    async with _serializable_executor_session(capacity_guard_database) as session:
        with pytest.raises(ExecutableAdmissionError, match="drain receipt"):
            await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).begin_drain(drain)


@pytest.mark.asyncio
async def test_guard_0020_downgrade_serializes_committing_executable_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catch downgrade checking empty tables before an overlapping writer commits."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    bootstrap_sha256 = "6" * 64
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    application_name = f"guard-0020-downgrade-race-{uuid4().hex}"
    config = _guard_downgrade_config(
        capacity_guard_database,
        monkeypatch,
        application_name=application_name,
    )

    executor_engine = create_async_engine(
        make_url(_value(capacity_guard_database, "executor_url")),
        isolation_level="SERIALIZABLE",
    )
    executor_factory = async_sessionmaker(executor_engine, expire_on_commit=False)
    downgrade_task: asyncio.Task[None] | None = None
    transaction = None
    try:
        async with executor_factory() as session:
            transaction = await session.begin()
            await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
            downgrade_task = asyncio.create_task(
                asyncio.to_thread(command.downgrade, config, "guard_0019")
            )

            admin = create_engine(_value(capacity_guard_database, "admin_url"))
            try:
                async with asyncio.timeout(10):
                    while True:
                        with admin.connect() as connection:
                            downgrade_wait = connection.execute(
                                text(
                                    "SELECT wait_event_type FROM pg_stat_activity "
                                    "WHERE application_name = :application_name"
                                ),
                                {"application_name": application_name},
                            ).scalar_one_or_none()
                        if downgrade_wait == "Lock":
                            break
                        if downgrade_task.done():
                            pytest.fail(
                                "guard_0020 downgrade completed before overlapping the "
                                f"executable writer: {downgrade_task.exception()!r}"
                            )
                        await asyncio.sleep(0.01)
            finally:
                admin.dispose()

            await transaction.commit()
            with pytest.raises(
                (DBAPIError, RuntimeError),
                match=r"cannot downgrade guard_0020.*executable",
            ):
                await downgrade_task

        admin = create_engine(_value(capacity_guard_database, "admin_url"))
        try:
            with admin.connect() as connection:
                version = connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                evidence = connection.execute(
                    text("SELECT count(*) FROM loom_capacity_guard.executable_admission_events")
                ).scalar_one()
        finally:
            admin.dispose()
    finally:
        if transaction is not None and transaction.is_active:
            await transaction.rollback()
        if downgrade_task is not None and not downgrade_task.done():
            await downgrade_task
        await executor_engine.dispose()

    assert version == "guard_0021"
    assert evidence == 1


@pytest.mark.asyncio
async def test_guard_0020_downgrade_gates_new_executor_calls_before_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catch a new executor call entering evidence tables behind downgrade."""

    _fence, registration = await _initialize_and_register(capacity_guard_database)
    bootstrap_sha256 = "6" * 64
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        prepared = await ExecutableAdmissionStore(
            session,
            registration=registration,
        ).prepare_worker(request, bootstrap_sha256=bootstrap_sha256)

    application_name = f"guard-0020-executor-gate-{uuid4().hex}"
    config = _guard_downgrade_config(
        capacity_guard_database,
        monkeypatch,
        application_name=application_name,
    )
    executor_engine = create_async_engine(
        make_url(_value(capacity_guard_database, "executor_url")),
        isolation_level="SERIALIZABLE",
    )
    executor_factory = async_sessionmaker(executor_engine, expire_on_commit=False)
    downgrade_task: asyncio.Task[None] | None = None
    writer_task: asyncio.Task[object] | None = None
    writer_pid: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def replay_preparation() -> object:
        async with executor_factory() as session, session.begin():
            writer_pid.set_result(
                (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            )
            return await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).prepare_worker(request, bootstrap_sha256=bootstrap_sha256)

    try:
        async with _owner_session(capacity_guard_database) as (_, _, blocker):
            await blocker.execute(
                text("LOCK TABLE loom_capacity_guard.executable_claim_leases IN ACCESS SHARE MODE")
            )
            downgrade_task = asyncio.create_task(
                asyncio.to_thread(command.downgrade, config, "guard_0019")
            )
            assert await _application_waited_for_lock(
                capacity_guard_database,
                application_name=application_name,
                task=downgrade_task,
            )

            writer_task = asyncio.create_task(replay_preparation())
            assert await _backend_waited_for_lock(
                capacity_guard_database,
                backend_pid=await writer_pid,
                task=writer_task,
            )
            observer = create_async_engine(make_url(_value(capacity_guard_database, "admin_url")))
            try:
                async with observer.connect() as connection:
                    waiting_relation = (
                        await connection.execute(
                            text(
                                "SELECT relation.relname FROM pg_locks AS lock "
                                "JOIN pg_class AS relation ON relation.oid = lock.relation "
                                "JOIN pg_namespace AS namespace "
                                "ON namespace.oid = relation.relnamespace "
                                "WHERE lock.pid = :writer_pid AND lock.granted IS FALSE "
                                "AND namespace.nspname = 'loom_capacity_guard'"
                            ),
                            {"writer_pid": await writer_pid},
                        )
                    ).scalar_one()
            finally:
                await observer.dispose()
            # guard_0021 first drops the abandoned-plan table, whose registration
            # foreign key gates this writer before guard_0020 takes its own locks.
            assert waiting_relation == "agent_registrations"

        downgrade_result, writer_result = await asyncio.gather(
            downgrade_task,
            writer_task,
            return_exceptions=True,
        )
    finally:
        if downgrade_task is not None and not downgrade_task.done():
            await downgrade_task
        if writer_task is not None and not writer_task.done():
            await writer_task
        await executor_engine.dispose()

    assert isinstance(downgrade_result, RuntimeError)
    assert "cannot downgrade guard_0020" in str(downgrade_result)
    assert writer_result == prepared


@pytest.mark.asyncio
async def test_guard_0020_downgrade_does_not_deadlock_terminal_projection(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catch downgrade holding claim state while waiting behind a terminal lease read."""

    registration, _worker_registration, claim, terminal = await _prepare_claim_terminal_race(
        capacity_guard_database
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        admitted = await ExecutableAdmissionStore(
            session,
            registration=registration,
        ).admit_claim(claim)
        assert admitted is not None

    application_name = f"guard-0020-terminal-race-{uuid4().hex}"
    config = _guard_downgrade_config(
        capacity_guard_database,
        monkeypatch,
        application_name=application_name,
    )
    downgrade_task: asyncio.Task[None] | None = None
    terminal_task: asyncio.Task[InertAttemptTransitionV1] | None = None
    terminal_pid: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    try:
        async with _owner_session(capacity_guard_database) as (_, _, blocker):
            # Pause terminal projection on the lease row after it has acquired
            # the lease relation lock but before it reaches claim state.  The
            # old downgrade order then forms claim-state -> lease while the
            # trigger holds lease -> claim-state.
            locked_lease = await blocker.execute(
                text(
                    "SELECT operation_id FROM "
                    "loom_capacity_guard.executable_claim_leases "
                    "WHERE operation_id = :operation_id FOR UPDATE"
                ),
                {"operation_id": claim.operation_id},
            )
            assert locked_lease.scalar_one() == claim.operation_id

            terminal_task = asyncio.create_task(
                _run_terminal_transaction(
                    capacity_guard_database,
                    registration,
                    terminal,
                    terminal_pid,
                )
            )
            assert await _backend_waited_for_lock(
                capacity_guard_database,
                backend_pid=await terminal_pid,
                task=terminal_task,
            )

            downgrade_task = asyncio.create_task(
                asyncio.to_thread(command.downgrade, config, "guard_0019")
            )
            assert await _application_waited_for_lock(
                capacity_guard_database,
                application_name=application_name,
                task=downgrade_task,
            )

        downgrade_result, terminal_result = await asyncio.gather(
            downgrade_task,
            terminal_task,
            return_exceptions=True,
        )
    finally:
        if downgrade_task is not None and not downgrade_task.done():
            await downgrade_task
        if terminal_task is not None and not terminal_task.done():
            await terminal_task

    assert isinstance(downgrade_result, RuntimeError)
    assert "cannot downgrade guard_0020" in str(downgrade_result)
    assert terminal_result == terminal
    assert await _claim_terminal_counts(capacity_guard_database) == (1, 1, 0)


@pytest.mark.asyncio
async def test_reconfiguration_denies_new_claims_on_stale_worker_registration(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch claim admission trusting a worker registered under an old candidate."""

    fence, registration = await _initialize_and_register(capacity_guard_database)
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    protected_attempt_id = UUID(int=205)
    requirements_digest = "9" * 64
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=protected_attempt_id,
        execution_generation=15,
        requirements_digest=requirements_digest,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)

    replacement = await _reconfigure_registration(
        capacity_guard_database,
        fence,
        registration,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        denied = await ExecutableAdmissionStore(
            session,
            registration=replacement,
        ).admit_claim(
            ExecutableClaimProposalV2(
                operation_id=UUID(int=206),
                protected_attempt_id=protected_attempt_id,
                execution_generation=15,
                requirements_digest=requirements_digest,
                worker_id=worker.worker_id,
                worker_incarnation=worker.worker_incarnation,
                expected_claim_high_water=0,
            )
        )
    async with _owner_session(capacity_guard_database) as (_, _, session):
        lease_count = (
            await session.execute(
                text("SELECT count(*) FROM loom_capacity_guard.executable_claim_leases")
            )
        ).scalar_one()

    assert denied is None
    assert lease_count == 0


@pytest.mark.asyncio
async def test_claim_rejects_attempt_without_exact_assigned_executable_intent(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch a registered worker consuming an unassigned protected attempt."""

    registration, _worker_registration, claim, _terminal = await _prepare_claim_terminal_race(
        capacity_guard_database,
        assigned=False,
    )

    with pytest.raises(DBAPIError, match="exact assigned executable intent"):
        async with _serializable_executor_session(capacity_guard_database) as session:
            await ExecutableAdmissionStore(
                session,
                registration=registration,
            ).admit_claim(claim)

    assert await _claim_terminal_counts(capacity_guard_database) == (0, 0, 0)


@pytest.mark.asyncio
async def test_guard_0020_upgrade_rejects_preexisting_claim_without_temporal_assignment_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catch migration inferring temporal safety from a later assignment head."""

    config = _guard_downgrade_config(
        capacity_guard_database,
        monkeypatch,
        application_name=f"guard-0020-upgrade-audit-{uuid4().hex}",
    )
    await asyncio.to_thread(command.downgrade, config, "guard_0019")
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    capability = "pre-exact-assignment-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    protected_attempt_id = UUID(int=153)
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=protected_attempt_id,
        execution_generation=16,
        requirements_digest="4" * 64,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)
        admitted = await store.admit_claim(
            ExecutableClaimProposalV2(
                operation_id=UUID(int=154),
                protected_attempt_id=protected_attempt_id,
                execution_generation=16,
                requirements_digest="4" * 64,
                worker_id=worker.worker_id,
                worker_incarnation=worker.worker_incarnation,
                expected_claim_high_water=0,
            )
        )
        assert admitted is not None

    await _assign_protected_attempt(
        capacity_guard_database,
        registration=registration,
        request=request,
        protected_attempt_id=protected_attempt_id,
        execution_generation=16,
        requirements_digest="4" * 64,
    )

    with pytest.raises(DBAPIError, match="pre-exact-assignment executable claim cannot prove"):
        await asyncio.to_thread(command.upgrade, config, "head")


@pytest.mark.asyncio
async def test_reconfiguration_preserves_drain_and_release_cleanup_path(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch fail-closed reconfiguration permanently stranding an old worker."""

    fence, registration = await _initialize_and_register(capacity_guard_database)
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
        await store.bind_slurm_job(_physical(request))
        await store.register_worker(worker, bootstrap_capability=capability)

    replacement = await _reconfigure_registration(
        capacity_guard_database,
        fence,
        registration,
    )
    drain = ExecutableDrainRequestV2(
        operation_id=UUID(int=207),
        binding=request.binding,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=0,
        drain_epoch=1,
    )
    release = ExecutableReleaseRequestV2(
        operation_id=UUID(int=208),
        binding=request.binding,
        reporter_incarnation=replacement.reporter_incarnation,
        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        expected_claim_high_water=0,
        protected_registration_epoch=worker.protected_registration_epoch,
        release_epoch=1,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=replacement)
        drained = await store.begin_drain(drain)
        released = await store.acknowledge_release(
            release,
            current_worker_credential="worker-credential-one",
        )

    assert drained.worker_state == "draining"
    assert released.release_state == "acknowledged"
    assert released.worker_credentials_revoked is True


@pytest.mark.asyncio
async def test_claim_replay_cannot_cross_subject_scope(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch one admitted operation replaying under another protected subject."""

    registration, _worker_registration, claim, _terminal = await _prepare_claim_terminal_race(
        capacity_guard_database
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        admitted = await store.admit_claim(claim)
        assert admitted is not None
        assert await store.admit_claim(claim) == admitted

        cross_subject = ExecutableAdmissionStore(
            session,
            subject_id=UUID(int=209),
            subject_incarnation=UUID(int=210),
        )
        with pytest.raises(DBAPIError, match="conflicting executable claim replay"):
            await cross_subject.admit_claim(claim)


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
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=digest,
    )
    physical = _physical(request)
    worker = _worker(request)

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        with pytest.raises(DBAPIError, match="prepared executable admission"):
            await store.bind_slurm_job(physical)
        prepared = await store.prepare_worker(request, bootstrap_sha256=digest)
        assert prepared.request_digest == prepared.admission_digest
        assert prepared.protected_high_water == 1
        assert await store.prepare_worker(request, bootstrap_sha256=digest) == prepared
        with pytest.raises(DBAPIError, match="protected bootstrap"):
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
        with pytest.raises(DBAPIError, match="bootstrap capability"):
            await store.register_worker(
                worker,
                bootstrap_capability="wrong-capability",
            )
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
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=digest,
    )
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
            await store.register_worker(
                second,
                predecessor_worker_credential="worker-credential-one",
            )
            == requeued
        )
        with pytest.raises(DBAPIError, match="requeue predecessor credential"):
            await store.register_worker(
                second,
                predecessor_worker_credential="wrong-predecessor-credential",
            )
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
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
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
    await _assign_protected_attempt(
        capacity_guard_database,
        registration=registration,
        request=request,
        protected_attempt_id=first_attempt,
        execution_generation=7,
        requirements_digest=requirements_digest,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            request,
            bootstrap_sha256=bootstrap_sha256,
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
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    worker = _worker(request)
    protected_attempt_id = UUID(int=143)
    requirements_digest = "7" * 64
    await _seed_protected_attempt(
        capacity_guard_database,
        protected_attempt_id=protected_attempt_id,
        execution_generation=13,
        requirements_digest=requirements_digest,
    )
    plan = await _assign_protected_attempt(
        capacity_guard_database,
        registration=registration,
        request=request,
        protected_attempt_id=protected_attempt_id,
        execution_generation=13,
        requirements_digest=requirements_digest,
    )
    allowance = plan.placement_allowances[0]
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
        expected_transition_sequence=1,
        operation="cancel",
        expected_state="assigned",
        target_state="cancelled-terminal",
        allowance_id=allowance.allowance_id,
        plan_id=plan.plan_id,
        admission_incarnation=plan.admission_incarnation,
        manager_allocation_epoch=plan.manager_allocation_epoch,
        pool_id=plan.pool_id,
        shape_instance_id=allowance.shape_instance_id,
        submission_intent_id=allowance.submission_intent_id,
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
            bootstrap_sha256=bootstrap_sha256,
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
            update={"transition_id": UUID(int=148), "expected_transition_sequence": 1}
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
    first_template = _bootstrap(registration.subject_id, registration.subject_incarnation)
    second_template = first_template.model_copy(
        update={
            "binding": first_template.binding.model_copy(
                update={
                    "tranche_id": UUID(int=118),
                    "intent_id": UUID(int=119),
                    "shape_instance_id": "oldlab-shape-0002",
                }
            ),
            "command_sequence": 2,
        }
    )
    first_capability = "first-bootstrap-capability"
    second_capability = "second-bootstrap-capability"
    first_request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=hashlib.sha256(first_capability.encode("ascii")).hexdigest(),
        request=first_template,
    )
    second_request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=hashlib.sha256(second_capability.encode("ascii")).hexdigest(),
        request=second_template,
    )
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
    await _assign_protected_attempts(
        capacity_guard_database,
        registration=registration,
        assignments=(
            (first_request, first_attempt, 11, "e" * 64),
            (second_request, second_attempt, 12, "f" * 64),
        ),
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

        with pytest.raises(DBAPIError, match="exact assigned executable intent"):
            await store.admit_claim(
                ExecutableClaimProposalV2(
                    operation_id=UUID(int=152),
                    protected_attempt_id=second_attempt,
                    execution_generation=12,
                    requirements_digest="f" * 64,
                    worker_id=first_worker.worker_id,
                    worker_incarnation=first_worker.worker_incarnation,
                    expected_claim_high_water=0,
                )
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
    first_template = _bootstrap(registration.subject_id, registration.subject_incarnation)
    second_template = first_template.model_copy(
        update={
            "binding": first_template.binding.model_copy(
                update={
                    "tranche_id": UUID(int=118),
                    "intent_id": UUID(int=119),
                    "shape_instance_id": "oldlab-shape-0002",
                }
            ),
            "command_sequence": 2,
        }
    )
    first_request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="6" * 64,
        request=first_template,
    )
    second_request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="7" * 64,
        request=second_template,
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
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=digest,
    )
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
        assert (
            await store.acknowledge_release(
                release,
                current_worker_credential="worker-credential-one",
            )
            == receipt
        )
        with pytest.raises(DBAPIError, match="worker credential"):
            await store.acknowledge_release(
                release,
                current_worker_credential="wrong-worker-credential",
            )
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


def test_all_executable_admission_functions_are_executor_only_fixed_definers(
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch any executable procedure inheriting PUBLIC, agent, or candidate execute."""

    signatures = (
        "loom_capacity_guard.prepare_executable_worker(uuid,uuid,jsonb,bytea,text,text)",
        "loom_capacity_guard.bind_executable_slurm_job(uuid,uuid,jsonb,bytea,text)",
        "loom_capacity_guard.register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
        "loom_capacity_guard.begin_executable_worker_drain(uuid,uuid,jsonb,bytea,text)",
        "loom_capacity_guard.acknowledge_executable_release(uuid,uuid,jsonb,bytea,text,text)",
        "loom_capacity_guard.admit_executable_claim(uuid,uuid,jsonb,bytea,text)",
    )
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    candidate = f"candidate_executable_acl_test_{uuid4().hex[:12]}"
    quoted_candidate = engine.dialect.identifier_preparer.quote(candidate)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_candidate} NOLOGIN NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
        with engine.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        "WITH requested(signature) AS (SELECT unnest(CAST(:signatures AS text[]))) "
                        "SELECT requested.signature, procedure.prosecdef, procedure.proconfig, "
                        "pg_get_userbyid(procedure.proowner) AS owner, "
                        "has_function_privilege(:executor, procedure.oid, 'EXECUTE') "
                        "AS executor_execute, "
                        "has_function_privilege(:agent, procedure.oid, 'EXECUTE') "
                        "AS agent_execute, "
                        "has_function_privilege(:candidate, procedure.oid, 'EXECUTE') "
                        "AS candidate_execute, "
                        "EXISTS (SELECT 1 FROM aclexplode(COALESCE("
                        "procedure.proacl, acldefault('f', procedure.proowner))) AS privilege "
                        "WHERE privilege.grantee = 0 "
                        "AND privilege.privilege_type = 'EXECUTE') AS public_execute "
                        "FROM requested JOIN pg_proc AS procedure "
                        "ON procedure.oid = to_regprocedure(requested.signature)"
                    ),
                    {
                        "signatures": list(signatures),
                        "executor": _value(capacity_guard_database, "executor_role"),
                        "agent": _value(capacity_guard_database, "agent_role"),
                        "candidate": candidate,
                    },
                )
                .mappings()
                .all()
            )
        assert {row["signature"] for row in rows} == set(signatures)
        for row in rows:
            assert row["prosecdef"] is True
            assert row["proconfig"] == ["search_path=pg_catalog"]
            assert row["owner"] == _value(capacity_guard_database, "owner_role")
            assert row["executor_execute"] is True
            assert row["agent_execute"] is False
            assert row["candidate_execute"] is False
            assert row["public_execute"] is False
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_candidate}")
        engine.dispose()


def _withdrawal(request: ExecutableBootstrapRegistrationV2) -> ExecutableWorkerWithdrawalRequestV2:
    physical = _physical(request)
    return ExecutableWorkerWithdrawalRequestV2(
        operation_id=UUID(int=121),
        binding=request.binding,
        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        protected_registration_epoch=request.bootstrap_registration_epoch + 1,
        slurm_job_id=physical.slurm_job_id,
        ownership_evidence_sha256=physical.ownership_evidence_sha256,
        expected_claim_high_water=0,
    )


def _prepared_revocation(
    request: ExecutableBootstrapRegistrationV2,
) -> ExecutablePreparedBootstrapRevocationV2:
    return ExecutablePreparedBootstrapRevocationV2(
        operation_id=UUID(int=122),
        binding=request.binding,
        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        protected_registration_epoch=request.bootstrap_registration_epoch + 1,
        expected_claim_high_water=0,
    )


async def _prepare_release_event(
    database: dict[str, object],
    *,
    event_kind: str,
) -> tuple[
    AgentRegistrationV1,
    PublishableExecutableProtectedReleaseV2,
    ProtectedReleasePublicationCheckpointV2 | None,
]:
    _fence, registration = await _initialize_and_register(database)
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        database,
        registration,
        bootstrap_sha256=digest,
    )

    async with _serializable_executor_session(database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=digest)
        if event_kind == "prepared-revoked":
            revocation = await store.revoke_prepared_bootstrap(_prepared_revocation(request))
            expected_digest = revocation.protected_release_sha256
            expected_epoch = revocation.protected_registration_epoch
        else:
            await store.bind_slurm_job(_physical(request))
            if event_kind == "withdrawn":
                withdrawal = await store.withdraw_unregistered_worker(_withdrawal(request))
                expected_digest = withdrawal.withdrawal_digest
                expected_epoch = withdrawal.protected_registration_epoch
            else:
                worker = _worker(request)
                await store.register_worker(worker, bootstrap_capability=capability)
                await store.begin_drain(
                    ExecutableDrainRequestV2(
                        operation_id=UUID(int=113),
                        binding=request.binding,
                        worker_id=worker.worker_id,
                        worker_incarnation=worker.worker_incarnation,
                        expected_claim_high_water=0,
                        drain_epoch=1,
                    )
                )
                release = await store.acknowledge_release(
                    ExecutableReleaseRequestV2(
                        operation_id=UUID(int=114),
                        binding=request.binding,
                        reporter_incarnation=registration.reporter_incarnation,
                        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
                        expected_claim_high_water=0,
                        protected_registration_epoch=worker.protected_registration_epoch,
                        release_epoch=1,
                    ),
                    current_worker_credential="worker-credential-one",
                )
                expected_digest = release.protected_release_sha256
                expected_epoch = release.protected_registration_epoch

    async with _serializable_agent_session(database) as session:
        publication = await read_next_executable_protected_release(
            session,
            registration=registration,
        )
        assert publication is not None
        assert publication.event_kind == event_kind
        assert publication.release.binding == request.binding
        assert publication.release.reporter_incarnation == registration.reporter_incarnation
        assert publication.release.bootstrap_registration_epoch == (
            request.bootstrap_registration_epoch
        )
        assert publication.release.protected_registration_epoch == expected_epoch
        assert publication.release.bootstrap_revoked is True
        assert publication.release.protected_release_sha256 == expected_digest
        assert publication.publication_digest == canonical_executable_digest(publication.release)
        return registration, publication, None


async def _release_publication_cursor_and_evidence_count(
    database: dict[str, object],
    *,
    agent_incarnation: UUID,
) -> tuple[int, int]:
    async with _owner_session(database) as (_, _, session):
        row = (
            await session.execute(
                text(
                    "SELECT "
                    "COALESCE((SELECT state.last_event_id "
                    "FROM loom_capacity_guard.executable_release_publication_state AS state "
                    "WHERE state.agent_incarnation = :agent_incarnation), 0) "
                    "AS last_event_id, "
                    "(SELECT count(*) "
                    "FROM loom_capacity_guard.executable_release_publication_events AS event "
                    "WHERE event.agent_incarnation = :agent_incarnation) AS evidence"
                ),
                {"agent_incarnation": agent_incarnation},
            )
        ).one()
        return row.last_event_id, row.evidence


async def test_executor_observes_exact_protected_intent_without_table_privilege(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    bootstrap_sha256 = "a" * 64
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)

        observed = await store.observe_intent(request.binding)

    assert observed.binding == request.binding
    assert observed.bootstrap_registration_epoch == request.bootstrap_registration_epoch
    assert observed.worker_id is None
    assert observed.drain is None
    assert observed.release is None


@pytest.mark.asyncio
@pytest.mark.parametrize("event_kind", ("released", "withdrawn", "prepared-revoked"))
async def test_agent_release_outbox_normalizes_exact_protected_events(
    capacity_guard_database: dict[str, object],
    event_kind: str,
) -> None:
    registration, publication, _ = await _prepare_release_event(
        capacity_guard_database,
        event_kind=event_kind,
    )

    async with _serializable_agent_session(capacity_guard_database) as session:
        reread = await read_next_executable_protected_release(session, registration=registration)
        assert reread == publication
        checkpoint = await acknowledge_executable_protected_release_publication(
            session,
            registration=registration,
            publication=publication,
            manager_acknowledgement_digest="9" * 64,
        )
        assert checkpoint.event_id == publication.event_id
        assert checkpoint.event_kind == event_kind
        assert checkpoint.publication_digest == publication.publication_digest
        assert checkpoint.manager_acknowledgement_digest == "9" * 64
        assert (
            await acknowledge_executable_protected_release_publication(
                session,
                registration=registration,
                publication=publication,
                manager_acknowledgement_digest="9" * 64,
            )
            == checkpoint
        )
        assert (
            await read_next_executable_protected_release(session, registration=registration) is None
        )


@pytest.mark.asyncio
async def test_agent_release_outbox_rejects_wrong_authority_and_changed_replay(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, publication, _ = await _prepare_release_event(
        capacity_guard_database,
        event_kind="withdrawn",
    )
    other_registration = registration.model_copy(update={"agent_incarnation": uuid4()})

    async with _serializable_agent_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match="not registered"):
            await read_next_executable_protected_release(
                session,
                registration=other_registration,
            )

    async with _serializable_agent_session(capacity_guard_database) as session:
        changed_release = publication.release.model_copy(
            update={"protected_release_sha256": "8" * 64}
        )
        changed = PublishableExecutableProtectedReleaseV2(
            event_id=publication.event_id,
            event_kind=publication.event_kind,
            release=changed_release,
            publication_digest=canonical_executable_digest(changed_release),
        )
        with pytest.raises(DBAPIError, match="publication"):
            await acknowledge_executable_protected_release_publication(
                session,
                registration=registration,
                publication=changed,
                manager_acknowledgement_digest="9" * 64,
            )

    async with _serializable_agent_session(capacity_guard_database) as session:
        canonical_payload = canonical_executable_bytes(publication.release)
        changed_canonical_payload = canonical_payload.replace(
            b'"executable":true', b'"executable":true '
        )
        with pytest.raises(DBAPIError, match=r"digest|canonical|publication"):
            await session.execute(
                text(
                    "SELECT loom_capacity_guard."
                    "acknowledge_executable_protected_release_publication("
                    ":agent_incarnation, :event_id, CAST(:publication_payload AS jsonb), "
                    "CAST(:canonical_payload AS bytea), :publication_digest, "
                    ":manager_acknowledgement_digest)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "event_id": publication.event_id,
                    "publication_payload": canonical_payload.decode("ascii"),
                    "canonical_payload": changed_canonical_payload,
                    "publication_digest": "8" * 64,
                    "manager_acknowledgement_digest": "9" * 64,
                },
            )

    async with _serializable_agent_session(capacity_guard_database) as session:
        assert await read_next_executable_protected_release(session, registration=registration) == (
            publication
        )
    async with _owner_session(capacity_guard_database) as (_, _, session):
        row = (
            await session.execute(
                text(
                    "SELECT state.last_event_id, count(event.publication_event_id) AS evidence "
                    "FROM loom_capacity_guard.executable_release_publication_state AS state "
                    "LEFT JOIN loom_capacity_guard.executable_release_publication_events AS event "
                    "ON event.agent_incarnation = state.agent_incarnation "
                    "WHERE state.agent_incarnation = :agent_incarnation "
                    "GROUP BY state.last_event_id"
                ),
                {"agent_incarnation": registration.agent_incarnation},
            )
        ).one()
        assert row.last_event_id == 0
        assert row.evidence == 0

    async with _serializable_agent_session(capacity_guard_database) as session:
        skipped = publication.model_copy(update={"event_id": publication.event_id + 1})
        with pytest.raises(DBAPIError, match="next event"):
            await acknowledge_executable_protected_release_publication(
                session,
                registration=registration,
                publication=skipped,
                manager_acknowledgement_digest="9" * 64,
            )

    async with _serializable_agent_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match="not registered"):
            await acknowledge_executable_protected_release_publication(
                session,
                registration=other_registration,
                publication=publication,
                manager_acknowledgement_digest="9" * 64,
            )

    async with _serializable_agent_session(capacity_guard_database) as session:
        checkpoint = await acknowledge_executable_protected_release_publication(
            session,
            registration=registration,
            publication=publication,
            manager_acknowledgement_digest="9" * 64,
        )
        with pytest.raises(DBAPIError, match="conflicting"):
            await acknowledge_executable_protected_release_publication(
                session,
                registration=registration,
                publication=publication,
                manager_acknowledgement_digest="8" * 64,
            )
        assert checkpoint.event_id == publication.event_id


# Production break caught: direct SQL callers could provide JSONB-equivalent
# bytes whose digest matched those noncanonical bytes, causing evidence to bind
# a release digest the manager never canonicalized.


@pytest.mark.asyncio
async def test_release_outbox_sql_rejects_json_equivalent_noncanonical_bytes_without_mutation(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, publication, _ = await _prepare_release_event(
        capacity_guard_database,
        event_kind="released",
    )
    canonical_payload = canonical_executable_bytes(publication.release)
    noncanonical_payload = json.dumps(
        publication.release.model_dump(mode="json", exclude_none=False),
        indent=2,
        sort_keys=False,
    ).encode("ascii")
    assert noncanonical_payload != canonical_payload

    async with _serializable_agent_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match=r"canonical|publication|invalid"):
            await session.execute(
                text(
                    "SELECT loom_capacity_guard."
                    "acknowledge_executable_protected_release_publication("
                    ":agent_incarnation, :event_id, CAST(:publication_payload AS jsonb), "
                    "CAST(:canonical_payload AS bytea), :publication_digest, "
                    ":manager_acknowledgement_digest)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "event_id": publication.event_id,
                    "publication_payload": canonical_payload.decode("ascii"),
                    "canonical_payload": noncanonical_payload,
                    "publication_digest": hashlib.sha256(noncanonical_payload).hexdigest(),
                    "manager_acknowledgement_digest": "9" * 64,
                },
            )

    assert await _release_publication_cursor_and_evidence_count(
        capacity_guard_database,
        agent_incarnation=registration.agent_incarnation,
    ) == (0, 0)
    async with _serializable_agent_session(capacity_guard_database) as session:
        assert await read_next_executable_protected_release(session, registration=registration) == (
            publication
        )


# Production break caught: acknowledgement must fail closed if the disabled
# authority's current agent binding has advanced since the release was read.


@pytest.mark.asyncio
async def test_release_outbox_ack_rejects_disabled_current_agent_reconfiguration_without_mutation(
    capacity_guard_database: dict[str, object],
) -> None:
    fence, registration = await _initialize_and_register(capacity_guard_database)
    bootstrap_sha256 = "a" * 64
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)
        revoked = await store.revoke_prepared_bootstrap(_prepared_revocation(request))

    async with _serializable_agent_session(capacity_guard_database) as session:
        publication = await read_next_executable_protected_release(
            session,
            registration=registration,
        )
        assert publication is not None
        assert publication.release.protected_release_sha256 == revoked.protected_release_sha256

    replacement_fence = fence.model_copy(
        update={
            "reporter_incarnation": uuid4(),
            "candidate_digest": "b" * 64,
            "deployment_generation": fence.deployment_generation + 1,
            "configuration_generation": fence.configuration_generation + 1,
        }
    )
    replacement_registration = registration.model_copy(
        update={
            "reporter_incarnation": replacement_fence.reporter_incarnation,
            "candidate_digest": replacement_fence.candidate_digest,
            "candidate_identity": replacement_fence.candidate_digest,
            "candidate_publication_sha256": replacement_fence.candidate_digest,
            "deployment_generation": replacement_fence.deployment_generation,
            "configuration_generation": replacement_fence.configuration_generation,
        }
    )
    async with _owner_session(capacity_guard_database) as (agent_store, guard_store, _):
        await guard_store.reconfigure_disabled_authority(
            replacement_fence,
            expected_configuration_generation=fence.configuration_generation,
        )
        await agent_store.reconfigure_agent(
            replacement_registration,
            expected_configuration_generation=registration.configuration_generation,
        )

    async with _serializable_agent_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match=r"stale|changed|next event|registered"):
            await acknowledge_executable_protected_release_publication(
                session,
                registration=replacement_registration,
                publication=publication,
                manager_acknowledgement_digest="9" * 64,
            )

    assert await _release_publication_cursor_and_evidence_count(
        capacity_guard_database,
        agent_incarnation=registration.agent_incarnation,
    ) == (0, 0)


@pytest.mark.asyncio
async def test_release_outbox_privileges_are_bounded_to_agent_functions(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, publication, _ = await _prepare_release_event(
        capacity_guard_database,
        event_kind="prepared-revoked",
    )
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        "SELECT routine_name FROM information_schema.role_routine_grants "
                        "WHERE grantee = :agent AND routine_schema = 'loom_capacity_guard' "
                        "AND routine_name LIKE '%executable_protected_release%' "
                        "ORDER BY routine_name"
                    ),
                    {"agent": _value(capacity_guard_database, "agent_role")},
                )
                .scalars()
                .all()
            )
            assert rows == [
                "acknowledge_executable_protected_release_publication",
                "read_next_executable_protected_release",
            ]
            for role in ("executor_role", "observer_role"):
                assert (
                    connection.execute(
                        text(
                            "SELECT has_function_privilege(:role, "
                            "'loom_capacity_guard.read_next_executable_protected_release(uuid)', "
                            "'EXECUTE')"
                        ),
                        {"role": _value(capacity_guard_database, role)},
                    ).scalar_one()
                    is False
                )
                assert (
                    connection.execute(
                        text(
                            "SELECT has_function_privilege(:role, "
                            "'loom_capacity_guard."
                            "acknowledge_executable_protected_release_publication"
                            "(uuid,bigint,jsonb,bytea,text,text)', "
                            "'EXECUTE')"
                        ),
                        {"role": _value(capacity_guard_database, role)},
                    ).scalar_one()
                    is False
                )
    finally:
        admin.dispose()

    for statement in (
        "SELECT * FROM loom_capacity_guard.executable_release_publication_state",
        "UPDATE loom_capacity_guard.executable_release_publication_state "
        "SET last_event_id = last_event_id",
        "SELECT * FROM loom_capacity_guard.executable_release_publication_events",
        "INSERT INTO loom_capacity_guard.executable_release_publication_events "
        "(agent_incarnation, admission_event_id, publication_payload, "
        "publication_canonical_payload, publication_digest, manager_acknowledgement_digest) "
        "VALUES (:agent_incarnation, :event_id, '{}'::jsonb, CAST(:canonical AS bytea), "
        ":digest, :digest)",
    ):
        async with _serializable_agent_session(capacity_guard_database) as session:
            with pytest.raises(DBAPIError) as denied:
                await session.execute(
                    text(statement),
                    {
                        "agent_incarnation": registration.agent_incarnation,
                        "event_id": publication.event_id,
                        "canonical": b"{}",
                        "digest": "0" * 64,
                    },
                )
            assert isinstance(denied.value.orig, InsufficientPrivilege)

    for url_key in ("executor_url", "observer_url"):
        engine = create_async_engine(
            make_url(_value(capacity_guard_database, url_key)), isolation_level="SERIALIZABLE"
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session, session.begin():
                with pytest.raises(DBAPIError) as denied:
                    await session.execute(
                        text(
                            "SELECT loom_capacity_guard."
                            "read_next_executable_protected_release(:agent_incarnation)"
                        ),
                        {"agent_incarnation": registration.agent_incarnation},
                    )
                assert isinstance(denied.value.orig, InsufficientPrivilege)
        finally:
            await engine.dispose()

    canonical_payload = canonical_executable_bytes(publication.release)
    for url_key in ("executor_url", "observer_url"):
        engine = create_async_engine(
            make_url(_value(capacity_guard_database, url_key)), isolation_level="SERIALIZABLE"
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session, session.begin():
                with pytest.raises(DBAPIError) as denied:
                    await session.execute(
                        text(
                            "SELECT loom_capacity_guard."
                            "acknowledge_executable_protected_release_publication("
                            ":agent_incarnation, :event_id, CAST(:publication_payload AS jsonb), "
                            "CAST(:canonical_payload AS bytea), :publication_digest, "
                            ":manager_acknowledgement_digest)"
                        ),
                        {
                            "agent_incarnation": registration.agent_incarnation,
                            "event_id": publication.event_id,
                            "publication_payload": canonical_payload.decode("ascii"),
                            "canonical_payload": canonical_payload,
                            "publication_digest": publication.publication_digest,
                            "manager_acknowledgement_digest": "9" * 64,
                        },
                    )
                assert isinstance(denied.value.orig, InsufficientPrivilege)
        finally:
            await engine.dispose()


# Production break caught: the guard SQL selector must not publish a stale
# executable release whose JSON binding no longer matches the current protected
# agent registration's candidate/deployment.


@pytest.mark.asyncio
async def test_release_outbox_sql_rejects_stale_candidate_binding(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    stale_binding = request.binding.model_copy(
        update={
            "deployment_generation": registration.deployment_generation + 1,
            "candidate": CandidateBindingV2(
                algorithm=registration.candidate_identity_algorithm,
                identity="8" * 64,
                publication_sha256=registration.candidate_publication_sha256,
            ),
        }
    )
    stale_release = {
        "schema_version": 2,
        "binding": stale_binding.model_dump(mode="json", exclude_none=False),
        "reporter_incarnation": str(registration.reporter_incarnation),
        "bootstrap_registration_epoch": request.bootstrap_registration_epoch,
        "protected_registration_epoch": request.bootstrap_registration_epoch + 1,
        "bootstrap_revoked": True,
        "protected_release_sha256": "7" * 64,
        "executable": True,
    }
    async with _owner_session(capacity_guard_database) as (_, _, session):
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.executable_admission_events "
                "(operation_id, event_kind, agent_incarnation, subject_id, subject_incarnation, "
                "intent_id, bootstrap_registration_epoch, protected_registration_epoch, "
                "worker_id, worker_incarnation, worker_credential_sha256, claim_high_water, "
                "release_epoch, bootstrap_revoked, worker_credential_revoked, binding, "
                "request_payload, request_digest, receipt) "
                "VALUES (:operation_id, 'released', :agent_incarnation, :subject_id, "
                ":subject_incarnation, :intent_id, :bootstrap_registration_epoch, "
                ":protected_registration_epoch, :worker_id, :worker_incarnation, "
                ":worker_credential_sha256, 0, 1, true, true, CAST(:binding AS jsonb), "
                "'{}'::jsonb, :request_digest, CAST(:receipt AS jsonb))"
            ),
            {
                "operation_id": UUID(int=170),
                "agent_incarnation": registration.agent_incarnation,
                "subject_id": registration.subject_id,
                "subject_incarnation": registration.subject_incarnation,
                "intent_id": stale_binding.intent_id,
                "bootstrap_registration_epoch": request.bootstrap_registration_epoch,
                "protected_registration_epoch": request.bootstrap_registration_epoch + 1,
                "worker_id": UUID(int=171),
                "worker_incarnation": UUID(int=172),
                "worker_credential_sha256": "6" * 64,
                "binding": json.dumps(
                    stale_binding.model_dump(mode="json", exclude_none=False),
                    sort_keys=True,
                ),
                "request_digest": "7" * 64,
                "receipt": json.dumps(stale_release, sort_keys=True),
            },
        )

    async with _serializable_agent_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match="binding"):
            await session.execute(
                text(
                    "SELECT loom_capacity_guard."
                    "read_next_executable_protected_release(:agent_incarnation)"
                ),
                {"agent_incarnation": registration.agent_incarnation},
            )


@pytest.mark.asyncio
async def test_executable_admission_separates_candidate_source_and_publication(
    capacity_guard_database: dict[str, object],
) -> None:
    """Protected admission compares tagged source identity and publication independently."""

    fence = _fence()
    registration = AgentRegistrationV1.model_validate(
        {
            **_registration(fence).model_dump(mode="python"),
            "candidate_identity_algorithm": "git-sha1",
            "candidate_identity": "b" * 40,
            "candidate_publication_sha256": "c" * 64,
        }
    )
    async with _owner_session(capacity_guard_database) as (agent_store, guard_store, _):
        await guard_store.initialize_disabled_authority(fence)
        await agent_store.register_agent(registration)

    request = _bootstrap(registration.subject_id, registration.subject_incarnation)
    exact = request.model_copy(
        update={
            "binding": request.binding.model_copy(
                update={
                    "candidate": CandidateBindingV2(
                        algorithm="git-sha1",
                        identity="b" * 40,
                        publication_sha256="c" * 64,
                    )
                }
            )
        }
    )
    exact = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256="d" * 64,
        request=exact,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        receipt = await ExecutableAdmissionStore(session, registration=registration).prepare_worker(
            exact,
            bootstrap_sha256="d" * 64,
        )

    assert receipt.intent_id == exact.binding.intent_id


@pytest.mark.asyncio
async def test_withdraw_unregistered_physical_binding_revokes_bootstrap_and_fences_registration(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    capability = "single-use-bootstrap-capability"
    digest = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=digest,
    )
    physical = _physical(request)
    withdrawal = _withdrawal(request)
    worker = _worker(request)

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=digest)
        await store.bind_slurm_job(physical)

        receipt = await store.withdraw_unregistered_worker(withdrawal)

        assert receipt.intent_id == request.binding.intent_id
        assert receipt.slurm_job_id == physical.slurm_job_id
        assert receipt.ownership_evidence_sha256 == physical.ownership_evidence_sha256
        assert receipt.bootstrap_registration_epoch == request.bootstrap_registration_epoch
        assert receipt.protected_registration_epoch == request.bootstrap_registration_epoch + 1
        assert receipt.claim_high_water == 0
        assert receipt.live_claim_count == 0
        assert receipt.bootstrap_revoked is True
        assert receipt.request_digest == receipt.withdrawal_digest
        assert await store.withdraw_unregistered_worker(withdrawal) == receipt
        observation = await store.observe_intent(request.binding)
        assert observation.withdrawal == receipt
        assert observation.release is None
        assert observation.prepared_revocation is None
        with pytest.raises(DBAPIError, match="delayed registration"):
            await store.register_worker(worker, bootstrap_capability=capability)


@pytest.mark.asyncio
async def test_prepared_bootstrap_revocation_fences_physical_binding_and_registration(
    capacity_guard_database: dict[str, object],
) -> None:
    _fence, registration = await _initialize_and_register(capacity_guard_database)
    capability = "single-use-bootstrap-capability"
    bootstrap_sha256 = hashlib.sha256(capability.encode("ascii")).hexdigest()
    request = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=bootstrap_sha256,
    )
    revocation = ExecutablePreparedBootstrapRevocationV2(
        operation_id=UUID(int=161),
        binding=request.binding,
        bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        protected_registration_epoch=request.bootstrap_registration_epoch + 1,
    )

    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            request,
            bootstrap_sha256=bootstrap_sha256,
        )

        receipt = await store.revoke_prepared_bootstrap(revocation)

        assert receipt.binding == request.binding
        assert receipt.reporter_incarnation == registration.reporter_incarnation
        assert receipt.bootstrap_registration_epoch == request.bootstrap_registration_epoch
        assert receipt.protected_registration_epoch == request.bootstrap_registration_epoch + 1
        assert receipt.claim_high_water == 0
        assert receipt.live_claim_count == 0
        assert receipt.bootstrap_revoked is True
        assert receipt.request_digest == receipt.protected_release_sha256
        assert await store.revoke_prepared_bootstrap(revocation) == receipt
        observed = await store.observe_intent(request.binding)
        assert observed.prepared_revocation == receipt
        with pytest.raises(DBAPIError, match="revoked"):
            await store.bind_slurm_job(_physical(request))
        with pytest.raises(DBAPIError, match="revoked"):
            await store.register_worker(_worker(request), bootstrap_capability=capability)
