"""Registration, monotonic capture, and overlap fencing for the trusted agent."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_capacity_agent.admission import (
    PreparedAdmissionPlanV1,
    PreparedBootstrapBindingV1,
    PreparedPlacementAllowanceV1,
    PreparedProtectedReleaseV1,
    PreparedWorkerBindingV1,
    PreparedWorkerShapeV1,
)
from loom_capacity_agent.claim_guard import ClaimProposalV1, InertAttemptTransitionV1
from loom_capacity_agent.claim_guard_store import DatabaseClaimGuard
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.lifecycle_store import CapacityAttemptLifecycleStore
from loom_capacity_agent.prepared_store import CapacityPreparedAdmissionStore
from loom_capacity_agent.store import (
    CapacityAgentStore,
    CapacityAgentStoreError,
    capture_demand_observation,
    capture_lifecycle_demand_observation,
    read_agent_lifecycle_demand_observation,
    read_agent_reporter_high_water,
)
from loom_capacity_guard.contracts import (
    GuardFenceV1,
    ProtectedAttemptV1,
    SealedRequirementsV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_guard.store import CapacityGuardStore
from loom_capacity_manager.contracts import (
    ResourceVectorV1,
    WorkerShapeV1,
)
from loom_capacity_manager.contracts import (
    canonical_digest as manager_canonical_digest,
)


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


def _fence() -> GuardFenceV1:
    return GuardFenceV1(
        environment_id="dev-alice",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        deployment_generation=7,
        configuration_generation=11,
        candidate_digest="a" * 64,
    )


def _registration(fence: GuardFenceV1) -> AgentRegistrationV1:
    return AgentRegistrationV1(
        environment_id=fence.environment_id,
        subject_id=fence.subject_id,
        subject_incarnation=fence.subject_incarnation,
        authority_incarnation=fence.authority_incarnation,
        agent_incarnation=uuid4(),
        reporter_incarnation=fence.reporter_incarnation,
        candidate_digest=fence.candidate_digest,
        deployment_generation=fence.deployment_generation,
        configuration_generation=fence.configuration_generation,
    )


def _seed_trial(database: dict[str, object], *, priority: int = 100) -> UUID:
    engine = create_engine(_value(database, "admin_url"))
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"guard-agent-task-{uuid4().hex}"
    try:
        with engine.begin() as connection:
            connection.execute(Team.__table__.insert().values(id=team_id, name=f"agent-{team_id}"))
            connection.execute(TeamQuota.__table__.insert().values(team_id=team_id))
            connection.execute(
                Task.__table__.insert().values(
                    id=task_id,
                    checksum="0" * 64,
                    config={"schema_version": "1"},
                )
            )
            connection.execute(
                Trial.__table__.insert().values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                    submit_priority=priority,
                )
            )
    finally:
        engine.dispose()
    return trial_id


@asynccontextmanager
async def _owner_session(
    database: dict[str, object],
) -> AsyncIterator[tuple[CapacityAgentStore, CapacityGuardStore, AsyncSession]]:
    engine = create_async_engine(
        make_url(_value(database, "migrator_url")), isolation_level="SERIALIZABLE"
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    owner_role = _value(database, "owner_role")
    quoted_owner = engine.sync_engine.dialect.identifier_preparer.quote(owner_role)
    try:
        async with factory() as session, session.begin():
            await session.execute(text(f"SET LOCAL ROLE {quoted_owner}"))
            yield (
                CapacityAgentStore(
                    session,
                    expected_owner_role=owner_role,
                    expected_agent_role=_value(database, "agent_role"),
                ),
                CapacityGuardStore(session, expected_owner_role=owner_role),
                session,
            )
    finally:
        await engine.dispose()


@asynccontextmanager
async def _agent_session(database: dict[str, object]) -> AsyncIterator[AsyncSession]:
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


async def _initialize_and_register(
    database: dict[str, object],
) -> tuple[GuardFenceV1, AgentRegistrationV1]:
    fence = _fence()
    registration = _registration(fence)
    async with _owner_session(database) as (agent_store, guard_store, _):
        await guard_store.initialize_disabled_authority(fence)
        await agent_store.register_agent(registration)
    return fence, registration


@pytest.mark.asyncio
async def test_agent_registration_is_exact_replay_and_audited_once(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    for _ in range(2):
        async with _owner_session(capacity_guard_database) as (
            agent_store,
            guard_store,
            _,
        ):
            await guard_store.initialize_disabled_authority(fence)
            assert await agent_store.register_agent(registration) == registration

    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.agent_registrations) "
                        "AS registrations, "
                        "(SELECT count(*) FROM loom_capacity_guard.agent_reporter_state) "
                        "AS states, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        " WHERE event_type = 'agent_registered.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"registrations": 1, "states": 1, "audits": 1}

    conflicting = registration.model_copy(update={"candidate_digest": "b" * 64})
    with pytest.raises(CapacityAgentStoreError, match="binding"):
        async with _owner_session(capacity_guard_database) as (store, _, _):
            await store.register_agent(conflicting)


@pytest.mark.asyncio
async def test_capture_is_monotonic_persisted_and_exactly_bound(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    async with _agent_session(capacity_guard_database) as session:
        first = await capture_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )
    assert first.sequence == 1
    assert first.attempts == ()
    assert first.source_observed_at.tzinfo == UTC

    with pytest.raises(DBAPIError, match="compare-and-set"):
        async with _agent_session(capacity_guard_database) as session:
            await capture_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=100,
            )

    async with _agent_session(capacity_guard_database) as session:
        second = await capture_demand_observation(
            session,
            registration=registration,
            expected_high_water=1,
            max_attempts=100,
        )
    assert second.sequence == 2

    async with _owner_session(capacity_guard_database) as (_, _, session):
        row = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, count(o.observation_id) AS observations "
                        "FROM loom_capacity_guard.agent_reporter_state s "
                        "JOIN loom_capacity_guard.demand_observations o "
                        "ON o.agent_incarnation = s.agent_incarnation "
                        "GROUP BY s.high_water"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(row) == {"high_water": 2, "observations": 2}


@pytest.mark.asyncio
async def test_agent_can_resume_from_protected_high_water_after_restart(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    async with _serializable_agent_session(capacity_guard_database) as session:
        observation = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )
    async with _agent_session(capacity_guard_database) as restarted:
        assert (
            await read_agent_reporter_high_water(
                restarted,
                registration=registration,
            )
            == 1
        )
        assert (
            await read_agent_lifecycle_demand_observation(
                restarted,
                registration=registration,
                sequence=1,
            )
            == observation
        )


@pytest.mark.asyncio
async def test_owner_reconfigures_disabled_agent_monotonically_without_resetting_sequence(
    capacity_guard_database: dict[str, object],
) -> None:
    fence, registration = await _initialize_and_register(capacity_guard_database)
    async with _serializable_agent_session(capacity_guard_database) as session:
        first_observation = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )
    capacity_fence = fence.model_copy(update={"configuration_generation": 12})
    capacity_registration = registration.model_copy(update={"configuration_generation": 12})
    async with _owner_session(capacity_guard_database) as (agent_store, guard_store, _):
        await guard_store.reconfigure_disabled_authority(
            capacity_fence,
            expected_configuration_generation=11,
        )
        assert (
            await agent_store.reconfigure_agent(
                capacity_registration,
                expected_configuration_generation=11,
            )
            == capacity_registration
        )
    async with _agent_session(capacity_guard_database) as session:
        assert (
            await read_agent_lifecycle_demand_observation(
                session,
                registration=capacity_registration,
                sequence=1,
            )
            == first_observation
        )

    replacement_fence = capacity_fence.model_copy(
        update={
            "reporter_incarnation": uuid4(),
            "candidate_digest": "b" * 64,
            "deployment_generation": 14,
            "configuration_generation": 15,
        }
    )
    replacement_registration = capacity_registration.model_copy(
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
            expected_configuration_generation=12,
        )
        await agent_store.reconfigure_agent(
            replacement_registration,
            expected_configuration_generation=12,
        )

    async with _serializable_agent_session(capacity_guard_database) as session:
        assert (
            await read_agent_reporter_high_water(
                session,
                registration=replacement_registration,
            )
            == 1
        )
        observation = await capture_lifecycle_demand_observation(
            session,
            registration=replacement_registration,
            expected_high_water=1,
            max_attempts=100,
        )
    assert observation.sequence == 2
    assert observation.reporter_incarnation == replacement_fence.reporter_incarnation


@pytest.mark.asyncio
async def test_agent_reconfiguration_rejects_reporter_rotation_without_deployment_change(
    capacity_guard_database: dict[str, object],
) -> None:
    fence, _registration = await _initialize_and_register(capacity_guard_database)
    invalid = fence.model_copy(
        update={"configuration_generation": 12, "reporter_incarnation": uuid4()}
    )
    with pytest.raises(DBAPIError, match="reporter"):
        async with _owner_session(capacity_guard_database) as (_, guard_store, _):
            await guard_store.reconfigure_disabled_authority(
                invalid,
                expected_configuration_generation=11,
            )


@pytest.mark.asyncio
async def test_authority_reconfiguration_requires_updated_at_to_advance(
    capacity_guard_database: dict[str, object],
) -> None:
    await _initialize_and_register(capacity_guard_database)
    with pytest.raises(DBAPIError, match="timestamp"):
        async with _owner_session(capacity_guard_database) as (_, _, session):
            await session.execute(
                text(
                    "UPDATE loom_capacity_guard.authority_state SET "
                    "configuration_generation = configuration_generation + 1, "
                    "updated_at = updated_at WHERE singleton_id = 1"
                )
            )


@pytest.mark.asyncio
async def test_capture_reads_only_registered_runnable_attempts_and_fences_legacy_overlap(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    trial_id = _seed_trial(capacity_guard_database, priority=123)
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="x86_64",
        gpu_vendor="none",
        network_policies=("public",),
    )
    attempt = ProtectedAttemptV1(
        trial_id=trial_id,
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements_digest=canonical_digest(requirements),
    )
    async with _owner_session(capacity_guard_database) as (_, guard_store, _):
        await guard_store.register_trial_attempt(attempt, requirements)

    async with _agent_session(capacity_guard_database) as session:
        observation = await capture_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )
    assert len(observation.attempts) == 1
    observed = observation.attempts[0]
    assert observed.protected_attempt_id == attempt.protected_attempt_id
    assert observed.requirements == requirements
    assert observed.submit_priority == 123
    assert isinstance(observed.submitted_at, datetime)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update()
                .where(Trial.id == trial_id)
                .values(
                    autoscaler_pool_name="oldlab", autoscaler_pool_assigned_at=datetime.now(UTC)
                )
            )
    finally:
        admin.dispose()
    with pytest.raises(DBAPIError, match="legacy pool assignment"):
        async with _agent_session(capacity_guard_database) as session:
            await capture_demand_observation(
                session,
                registration=registration,
                expected_high_water=1,
                max_attempts=100,
            )


@pytest.mark.asyncio
async def test_capture_row_bound_rolls_back_high_water(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("public",),
    )
    for generation in (1, 2):
        trial_id = _seed_trial(capacity_guard_database)
        attempt = ProtectedAttemptV1(
            trial_id=trial_id,
            protected_attempt_id=uuid4(),
            execution_generation=generation,
            requirements_digest=canonical_digest(requirements),
        )
        async with _owner_session(capacity_guard_database) as (_, guard_store, _):
            await guard_store.register_trial_attempt(attempt, requirements)

    with pytest.raises(DBAPIError, match="row bound"):
        async with _agent_session(capacity_guard_database) as session:
            await capture_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=1,
            )
    async with _agent_session(capacity_guard_database) as session:
        observation = await capture_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=2,
        )
    assert observation.sequence == 1
    assert len(observation.attempts) == 2


@pytest.mark.asyncio
async def test_agent_records_are_append_only_and_reporter_state_cannot_skip(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    async with _agent_session(capacity_guard_database) as session:
        await capture_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )

    immutable_statements = (
        "UPDATE loom_capacity_guard.agent_runtime_authority SET agent_role_name = agent_role_name",
        "DELETE FROM loom_capacity_guard.agent_registrations",
        "TRUNCATE loom_capacity_guard.demand_observations",
    )
    for statement in immutable_statements:
        with pytest.raises(DBAPIError, match="append-only"):
            async with _owner_session(capacity_guard_database) as (_, _, session):
                await session.execute(text(statement))

    invalid_state_statements = (
        "UPDATE loom_capacity_guard.agent_reporter_state SET high_water = high_water + 2",
        "DELETE FROM loom_capacity_guard.agent_reporter_state",
        "TRUNCATE loom_capacity_guard.agent_reporter_state",
    )
    for statement in invalid_state_statements:
        with pytest.raises(DBAPIError, match=r"reporter state|high-water"):
            async with _owner_session(capacity_guard_database) as (_, _, session):
                await session.execute(text(statement))


@pytest.mark.asyncio
async def test_invalid_captured_contract_rolls_back_the_protected_transition(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    trial_id = _seed_trial(capacity_guard_database)
    protected_attempt_id = uuid4()
    malformed_digest = "f" * 64
    async with _owner_session(capacity_guard_database) as (_, _, session):
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_requirements "
                "(trial_id, schema_version, requirements_digest, requirements) VALUES "
                "(:trial_id, 1, :digest, CAST(:requirements AS jsonb))"
            ),
            {
                "trial_id": trial_id,
                "digest": malformed_digest,
                "requirements": '{"unexpected":true}',
            },
        )
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, requirements_digest, "
                "claim_state) VALUES (:attempt_id, :trial_id, 1, :digest, 'queued')"
            ),
            {
                "attempt_id": protected_attempt_id,
                "trial_id": trial_id,
                "digest": malformed_digest,
            },
        )

    async with _agent_session(capacity_guard_database) as session:
        with pytest.raises(CapacityAgentStoreError, match="invalid contract"):
            await capture_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=100,
            )

    async with _owner_session(capacity_guard_database) as (_, _, session):
        row = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.demand_observations) "
                        "AS observations FROM loom_capacity_guard.agent_reporter_state AS s"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(row) == {"high_water": 0, "observations": 0}


@pytest.mark.asyncio
async def test_oversize_capture_is_rejected_before_aggregation_and_rolls_back(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    trial_id = _seed_trial(capacity_guard_database)
    protected_attempt_id = uuid4()
    oversized_digest = "e" * 64
    async with _owner_session(capacity_guard_database) as (_, _, session):
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_requirements "
                "(trial_id, schema_version, requirements_digest, requirements) VALUES "
                "(:trial_id, 1, :digest, "
                "jsonb_build_object('unexpected', repeat('x', 8372000)))"
            ),
            {"trial_id": trial_id, "digest": oversized_digest},
        )
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, requirements_digest, "
                "claim_state) VALUES (:attempt_id, :trial_id, 1, :digest, 'queued')"
            ),
            {
                "attempt_id": protected_attempt_id,
                "trial_id": trial_id,
                "digest": oversized_digest,
            },
        )

    async with _agent_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError, match="pre-aggregation payload bound"):
            await capture_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=100,
            )

    async with _owner_session(capacity_guard_database) as (_, _, session):
        row = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.demand_observations) "
                        "AS observations FROM loom_capacity_guard.agent_reporter_state AS s"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(row) == {"high_water": 0, "observations": 0}


def _prepared_plan(
    registration: AgentRegistrationV1,
    attempt: ProtectedAttemptV1,
) -> PreparedAdmissionPlanV1:
    worker_shape = WorkerShapeV1(
        shape_id="oldlab-x86-none-2",
        concurrency_slots=2,
        total_resources=ResourceVectorV1(
            slots=2,
            cpu_millicores=4000,
            memory_bytes=8_000_000_000,
        ),
        node_resources=(
            ResourceVectorV1(
                slots=2,
                cpu_millicores=4000,
                memory_bytes=8_000_000_000,
            ),
        ),
        compatible_domain_ids=("oldlab-x86",),
        capabilities=(
            "cpu_arch.x86_64",
            "gpu_vendor.none",
            "network.public",
            "os.linux",
        ),
    )
    prepared_shape = PreparedWorkerShapeV1(
        shape_instance_id="shape-oldlab-0001",
        submission_intent_id=uuid4(),
        pool_id="oldlab",
        pool_generation=3,
        profile_id="dev-oldlab",
        profile_generation=5,
        profile_digest="b" * 64,
        protocol_generation=2,
        protocol_digest="c" * 64,
        worker_shape=worker_shape,
        worker_shape_digest=manager_canonical_digest(worker_shape),
        bootstrap_registration_epoch=1,
    )
    allowance = PreparedPlacementAllowanceV1(
        allowance_id=uuid4(),
        protected_attempt_id=attempt.protected_attempt_id,
        execution_generation=attempt.execution_generation,
        requirements_digest=attempt.requirements_digest,
        pool_id="oldlab",
        shape_instance_id=prepared_shape.shape_instance_id,
        shape_slot_index=0,
        submission_intent_id=prepared_shape.submission_intent_id,
    )
    return PreparedAdmissionPlanV1(
        **registration.model_dump(mode="python"),
        plan_id=uuid4(),
        admission_incarnation=uuid4(),
        manager_authority_incarnation=uuid4(),
        manager_writer_epoch=0,
        manager_allocation_epoch=1,
        manager_input_digest="e" * 64,
        manager_allocation_digest="f" * 64,
        pool_id="oldlab",
        pool_generation=prepared_shape.pool_generation,
        profile_id=prepared_shape.profile_id,
        profile_generation=prepared_shape.profile_generation,
        profile_digest=prepared_shape.profile_digest,
        protocol_generation=prepared_shape.protocol_generation,
        protocol_digest=prepared_shape.protocol_digest,
        lease_not_after=datetime.now(UTC) + timedelta(hours=1),
        worker_shapes=(prepared_shape,),
        placement_allowances=(allowance,),
    )


async def _initialize_prepared_plan(
    database: dict[str, object],
    *,
    cpu_arch: Literal["x86_64", "arm64", "any"] = "x86_64",
) -> tuple[AgentRegistrationV1, ProtectedAttemptV1, PreparedAdmissionPlanV1]:
    _, registration = await _initialize_and_register(database)
    trial_id = _seed_trial(database)
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch=cpu_arch,
        gpu_vendor="none",
        network_policies=("public",),
    )
    attempt = ProtectedAttemptV1(
        trial_id=trial_id,
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements_digest=canonical_digest(requirements),
    )
    async with _owner_session(database) as (_, guard_store, _):
        await guard_store.register_trial_attempt(attempt, requirements)
    return registration, attempt, _prepared_plan(registration, attempt)


def _prepared_worker_bindings(
    registration: AgentRegistrationV1,
    plan: PreparedAdmissionPlanV1,
) -> tuple[PreparedBootstrapBindingV1, PreparedWorkerBindingV1]:
    shape = plan.worker_shapes[0]
    registration_fields = {
        field: getattr(registration, field) for field in AgentRegistrationV1.model_fields
    }
    bootstrap = PreparedBootstrapBindingV1(
        **registration_fields,
        bootstrap_id=uuid4(),
        plan_id=plan.plan_id,
        admission_incarnation=plan.admission_incarnation,
        manager_allocation_epoch=plan.manager_allocation_epoch,
        pool_id=plan.pool_id,
        shape_instance_id=shape.shape_instance_id,
        submission_intent_id=shape.submission_intent_id,
        bootstrap_registration_epoch=shape.bootstrap_registration_epoch,
        bootstrap_digest="1" * 64,
        expires_at=plan.lease_not_after,
    )
    worker = PreparedWorkerBindingV1(
        **registration_fields,
        worker_id=uuid4(),
        worker_incarnation=uuid4(),
        bootstrap_id=bootstrap.bootstrap_id,
        plan_id=plan.plan_id,
        admission_incarnation=plan.admission_incarnation,
        manager_allocation_epoch=plan.manager_allocation_epoch,
        pool_id=plan.pool_id,
        shape_instance_id=shape.shape_instance_id,
        submission_intent_id=shape.submission_intent_id,
        bootstrap_registration_epoch=shape.bootstrap_registration_epoch,
        slurm_job_id="oldlab-12345",
        ownership_evidence_digest="2" * 64,
        worker_credential_digest="3" * 64,
    )
    return bootstrap, worker


def _protected_release(
    registration: AgentRegistrationV1,
    plan: PreparedAdmissionPlanV1,
    *,
    bootstrap_registration_epoch: int,
) -> PreparedProtectedReleaseV1:
    shape = plan.worker_shapes[0]
    registration_fields = {
        field: getattr(registration, field) for field in AgentRegistrationV1.model_fields
    }
    return PreparedProtectedReleaseV1(
        **registration_fields,
        release_id=uuid4(),
        plan_id=plan.plan_id,
        admission_incarnation=plan.admission_incarnation,
        manager_authority_incarnation=plan.manager_authority_incarnation,
        manager_writer_epoch=plan.manager_writer_epoch,
        manager_configuration_epoch=5,
        manager_allocation_epoch=plan.manager_allocation_epoch,
        tranche_id=uuid4(),
        pool_id=plan.pool_id,
        pool_generation=plan.pool_generation,
        shape_instance_id=shape.shape_instance_id,
        submission_intent_id=shape.submission_intent_id,
        bootstrap_registration_epoch=bootstrap_registration_epoch,
        protected_registration_epoch=bootstrap_registration_epoch + 1,
        bootstrap_revoked=True,
    )


@pytest.mark.asyncio
async def test_prepared_plan_is_exact_replay_and_persists_normalized_bindings(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    for _ in range(2):
        async with _agent_session(capacity_guard_database) as session:
            store = CapacityPreparedAdmissionStore(session, registration=registration)
            assert await store.prepare_plan(plan) == plan

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
                        "(SELECT count(*) FROM loom_capacity_guard.prepared_placement_allowances) "
                        "AS allowances, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        "WHERE event_type = 'admission_plan_prepared.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"plans": 1, "shapes": 1, "allowances": 1, "audits": 1}

    conflicting = plan.model_copy(update={"manager_allocation_digest": "0" * 64})
    with pytest.raises(DBAPIError, match="conflicting prepared admission"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(
                conflicting
            )


@pytest.mark.asyncio
async def test_concurrent_prepared_plan_replay_converges(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, plan = await _initialize_prepared_plan(capacity_guard_database)

    async def prepare_once() -> PreparedAdmissionPlanV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).prepare_plan(plan)

    assert await asyncio.gather(prepare_once(), prepare_once()) == [plan, plan]

    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.prepared_admission_plans) "
                        "AS plans, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        "WHERE event_type = 'admission_plan_prepared.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"plans": 1, "audits": 1}


@pytest.mark.asyncio
async def test_concurrent_shape_rebinding_allows_only_one_plan(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, first = await _initialize_prepared_plan(capacity_guard_database)
    original_shape = first.worker_shapes[0]
    changed_contract = original_shape.worker_shape.model_copy(
        update={"warm_approved": not original_shape.worker_shape.warm_approved}
    )
    changed_shape = PreparedWorkerShapeV1.model_validate(
        {
            **original_shape.model_dump(mode="python"),
            "worker_shape": changed_contract,
            "worker_shape_digest": manager_canonical_digest(changed_contract),
        }
    )
    second = PreparedAdmissionPlanV1.model_validate(
        {
            **first.model_dump(mode="python"),
            "plan_id": uuid4(),
            "admission_incarnation": uuid4(),
            "manager_allocation_epoch": 2,
            "manager_input_digest": "4" * 64,
            "manager_allocation_digest": "5" * 64,
            "worker_shapes": (changed_shape,),
            "placement_allowances": (),
        }
    )

    async def prepare(plan: PreparedAdmissionPlanV1) -> PreparedAdmissionPlanV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).prepare_plan(plan)

    results = await asyncio.gather(prepare(first), prepare(second), return_exceptions=True)
    assert sum(isinstance(result, PreparedAdmissionPlanV1) for result in results) == 1
    assert sum(isinstance(result, DBAPIError) for result in results) == 1

    async with _owner_session(capacity_guard_database) as (_, _, session):
        assert (
            await session.execute(
                text("SELECT count(*) FROM loom_capacity_guard.prepared_admission_plans")
            )
        ).scalar_one() == 1


@pytest.mark.asyncio
async def test_prepared_plan_rejects_unregistered_or_mismatched_attempt(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    allowance = plan.placement_allowances[0]
    missing = allowance.model_copy(update={"protected_attempt_id": uuid4()})
    missing_plan = plan.model_copy(update={"placement_allowances": (missing,)})
    with pytest.raises(DBAPIError, match="allowance binding"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(
                missing_plan
            )

    mismatched = allowance.model_copy(
        update={"execution_generation": attempt.execution_generation + 1}
    )
    mismatch_plan = plan.model_copy(update={"placement_allowances": (mismatched,)})
    with pytest.raises(DBAPIError, match="allowance binding"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(
                mismatch_plan
            )


@pytest.mark.asyncio
async def test_later_epoch_can_retain_but_not_rebind_a_shape_identity(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, first = await _initialize_prepared_plan(capacity_guard_database)
    second = PreparedAdmissionPlanV1.model_validate(
        {
            **first.model_dump(mode="python"),
            "plan_id": uuid4(),
            "admission_incarnation": uuid4(),
            "manager_allocation_epoch": 2,
            "manager_input_digest": "4" * 64,
            "manager_allocation_digest": "5" * 64,
            "placement_allowances": (),
        }
    )
    async with _agent_session(capacity_guard_database) as session:
        store = CapacityPreparedAdmissionStore(session, registration=registration)
        await store.prepare_plan(first)
        await store.prepare_plan(second)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        assert (
            await session.execute(
                text("SELECT count(*) FROM loom_capacity_guard.prepared_worker_shapes")
            )
        ).scalar_one() == 2

    old_shape = first.worker_shapes[0]
    changed_shape_contract = old_shape.worker_shape.model_copy(
        update={"warm_approved": not old_shape.worker_shape.warm_approved}
    )
    rebound_shape = PreparedWorkerShapeV1.model_validate(
        {
            **old_shape.model_dump(mode="python"),
            "worker_shape": changed_shape_contract,
            "worker_shape_digest": manager_canonical_digest(changed_shape_contract),
        }
    )
    rebound = PreparedAdmissionPlanV1.model_validate(
        {
            **first.model_dump(mode="python"),
            "plan_id": uuid4(),
            "admission_incarnation": uuid4(),
            "manager_allocation_epoch": 3,
            "manager_input_digest": "6" * 64,
            "manager_allocation_digest": "7" * 64,
            "worker_shapes": (rebound_shape,),
            "placement_allowances": (),
        }
    )
    with pytest.raises(DBAPIError, match="shape identity was rebound"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(
                rebound
            )


@pytest.mark.asyncio
async def test_database_recomputes_prepared_payload_digest_before_insert(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    payload = canonical_bytes(plan)
    with pytest.raises(DBAPIError, match="payload is invalid"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.prepare_inert_admission_plan("
                    ":agent_incarnation, CAST(:payload AS jsonb), "
                    "CAST(:canonical_payload AS bytea), :payload_digest)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "payload": payload.decode("ascii"),
                    "canonical_payload": payload,
                    "payload_digest": "0" * 64,
                },
            )

    async with _agent_session(capacity_guard_database) as session:
        assert (
            await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(
                plan
            )
            == plan
        )


@pytest.mark.asyncio
async def test_bootstrap_and_worker_are_hash_only_exact_replays(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    bootstrap, worker = _prepared_worker_bindings(registration, plan)
    async with _agent_session(capacity_guard_database) as session:
        store = CapacityPreparedAdmissionStore(session, registration=registration)
        await store.prepare_plan(plan)

    async def register_bootstrap() -> PreparedBootstrapBindingV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).register_bootstrap(bootstrap)

    assert await asyncio.gather(register_bootstrap(), register_bootstrap()) == [
        bootstrap,
        bootstrap,
    ]

    async def record_worker() -> PreparedWorkerBindingV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).record_prepared_worker(worker)

    assert await asyncio.gather(record_worker(), record_worker()) == [worker, worker]

    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.prepared_bootstrap_bindings) "
                        "AS bootstraps, "
                        "(SELECT count(*) FROM loom_capacity_guard.prepared_worker_bindings) "
                        "AS workers, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events WHERE "
                        "event_type IN ('bootstrap_prepared.v1', 'worker_prepared.v1')) AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"bootstraps": 1, "workers": 1, "audits": 2}


@pytest.mark.asyncio
async def test_protected_release_is_concurrent_append_only_and_fences_late_registration(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, unbound_plan = await _initialize_prepared_plan(capacity_guard_database)
    plan = unbound_plan.model_copy(update={"manager_writer_epoch": 1})
    bootstrap, _worker = _prepared_worker_bindings(registration, plan)
    release = _protected_release(
        registration,
        plan,
        bootstrap_registration_epoch=0,
    )
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(plan)

    async def acknowledge() -> PreparedProtectedReleaseV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).acknowledge_protected_release(release)

    assert await asyncio.gather(acknowledge(), acknowledge()) == [release, release]

    with pytest.raises(DBAPIError, match="protected release fence"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).register_bootstrap(bootstrap)

    async with _owner_session(capacity_guard_database) as (_, _, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_release_acknowledgements) AS releases, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events WHERE "
                        "event_type = 'protected_release_acknowledged.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"releases": 1, "audits": 1}

    immutable_statements = (
        "UPDATE loom_capacity_guard.protected_release_acknowledgements SET executable = true",
        "DELETE FROM loom_capacity_guard.protected_release_acknowledgements",
        "TRUNCATE loom_capacity_guard.protected_release_acknowledgements",
    )
    for statement in immutable_statements:
        with pytest.raises(DBAPIError, match="append-only"):
            async with _owner_session(capacity_guard_database) as (_, _, session):
                await session.execute(text(statement))


@pytest.mark.asyncio
async def test_protected_release_requires_exact_bootstrap_high_water_and_no_worker(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, unbound_plan = await _initialize_prepared_plan(capacity_guard_database)
    plan = unbound_plan.model_copy(update={"manager_writer_epoch": 1})
    bootstrap, worker = _prepared_worker_bindings(registration, plan)
    async with _agent_session(capacity_guard_database) as session:
        store = CapacityPreparedAdmissionStore(session, registration=registration)
        await store.prepare_plan(plan)
        await store.register_bootstrap(bootstrap)

    stale_release = _protected_release(
        registration,
        plan,
        bootstrap_registration_epoch=0,
    )
    with pytest.raises(DBAPIError, match="bootstrap high-water changed"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).acknowledge_protected_release(stale_release)

    current_release = _protected_release(
        registration,
        plan,
        bootstrap_registration_epoch=bootstrap.bootstrap_registration_epoch,
    )
    async with _agent_session(capacity_guard_database) as session:
        assert (
            await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).acknowledge_protected_release(current_release)
            == current_release
        )
    with pytest.raises(DBAPIError, match="protected release fence"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).record_prepared_worker(worker)


@pytest.mark.asyncio
async def test_protected_release_rejects_an_existing_prepared_worker(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, unbound_plan = await _initialize_prepared_plan(capacity_guard_database)
    plan = unbound_plan.model_copy(update={"manager_writer_epoch": 1})
    bootstrap, worker = _prepared_worker_bindings(registration, plan)
    async with _agent_session(capacity_guard_database) as session:
        store = CapacityPreparedAdmissionStore(session, registration=registration)
        await store.prepare_plan(plan)
        await store.register_bootstrap(bootstrap)
        await store.record_prepared_worker(worker)

    release = _protected_release(
        registration,
        plan,
        bootstrap_registration_epoch=bootstrap.bootstrap_registration_epoch,
    )
    with pytest.raises(DBAPIError, match="prepared worker binding"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityPreparedAdmissionStore(
                session, registration=registration
            ).acknowledge_protected_release(release)


@pytest.mark.asyncio
async def test_owner_cannot_turn_prepared_records_executable_or_mutate_them(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    bootstrap, worker = _prepared_worker_bindings(registration, plan)
    async with _agent_session(capacity_guard_database) as session:
        store = CapacityPreparedAdmissionStore(session, registration=registration)
        await store.prepare_plan(plan)
        await store.register_bootstrap(bootstrap)
        await store.record_prepared_worker(worker)

    statements = (
        "UPDATE loom_capacity_guard.prepared_admission_plans SET executable = true",
        "UPDATE loom_capacity_guard.prepared_worker_shapes SET shape_state = 'accepted'",
        "DELETE FROM loom_capacity_guard.prepared_placement_allowances",
        "DELETE FROM loom_capacity_guard.prepared_bootstrap_bindings",
        "UPDATE loom_capacity_guard.prepared_worker_bindings SET executable = true",
        "TRUNCATE loom_capacity_guard.prepared_admission_plans CASCADE",
    )
    for statement in statements:
        with pytest.raises(DBAPIError, match=r"append-only|check constraint"):
            async with _owner_session(capacity_guard_database) as (_, _, session):
                await session.execute(text(statement))


def _attempt_transition(
    registration: AgentRegistrationV1,
    attempt: ProtectedAttemptV1,
    plan: PreparedAdmissionPlanV1,
    *,
    operation: str,
    expected_sequence: int,
    expected_state: str | None = None,
    transition_id: UUID | None = None,
) -> InertAttemptTransitionV1:
    allowance = plan.placement_allowances[0]
    state_pairs = {
        "assign": ("pending-unassigned", "assigned"),
        "withdraw": ("assigned", "pending-unassigned"),
        "cancel": (expected_state or "pending-unassigned", "cancelled-terminal"),
    }
    source, target = state_pairs[operation]
    assignment = operation in {"assign", "withdraw"} or source == "assigned"
    return InertAttemptTransitionV1(
        **registration.model_dump(mode="python"),
        transition_id=transition_id or uuid4(),
        protected_attempt_id=attempt.protected_attempt_id,
        execution_generation=attempt.execution_generation,
        requirements_digest=attempt.requirements_digest,
        expected_transition_sequence=expected_sequence,
        operation=operation,
        expected_state=source,
        target_state=target,
        allowance_id=allowance.allowance_id if assignment else None,
        plan_id=plan.plan_id if assignment else None,
        admission_incarnation=plan.admission_incarnation if assignment else None,
        manager_allocation_epoch=plan.manager_allocation_epoch if assignment else None,
        pool_id=plan.pool_id if assignment else None,
        shape_instance_id=allowance.shape_instance_id if assignment else None,
        submission_intent_id=allowance.submission_intent_id if assignment else None,
        transition_reason={
            "assign": "manager-placement",
            "withdraw": "allowance-withdrawn",
            "cancel": "owner-cancelled-unclaimed",
        }[operation],
    )


@pytest.mark.asyncio
async def test_inert_attempt_lifecycle_is_exact_monotonic_and_terminal(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    assign = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    withdraw = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="withdraw",
        expected_sequence=1,
    )
    cancel = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="cancel",
        expected_sequence=2,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        prepared = CapacityPreparedAdmissionStore(session, registration=registration)
        lifecycle = CapacityAttemptLifecycleStore(session, registration=registration)
        await prepared.prepare_plan(plan)
        assert await lifecycle.apply_transition(assign) == assign
        assert await lifecycle.apply_transition(assign) == assign
        conflicting = assign.model_copy(update={"transition_reason": "conflicting-replay"})
        with pytest.raises(DBAPIError, match="conflicting inert lifecycle replay"):
            await lifecycle.apply_transition(conflicting)
        assert await lifecycle.apply_transition(withdraw) == withdraw
        assert await lifecycle.apply_transition(cancel) == cancel

    async with _owner_session(capacity_guard_database) as (_, _, session):
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT transition_sequence, lifecycle_state, executable "
                        "FROM loom_capacity_guard.attempt_lifecycle_events "
                        "ORDER BY transition_sequence"
                    )
                )
            )
            .mappings()
            .all()
        )
        assert [dict(row) for row in rows] == [
            {
                "transition_sequence": 0,
                "lifecycle_state": "pending-unassigned",
                "executable": False,
            },
            {"transition_sequence": 1, "lifecycle_state": "assigned", "executable": False},
            {
                "transition_sequence": 2,
                "lifecycle_state": "pending-unassigned",
                "executable": False,
            },
            {
                "transition_sequence": 3,
                "lifecycle_state": "cancelled-terminal",
                "executable": False,
            },
        ]
        head = (
            (
                await session.execute(
                    text(
                        "SELECT protected_attempt_id, transition_sequence, lifecycle_state, "
                        "executable FROM loom_capacity_guard.attempt_lifecycle_heads"
                    )
                )
            )
            .mappings()
            .one()
        )
        assert dict(head) == {
            "protected_attempt_id": attempt.protected_attempt_id,
            "transition_sequence": 3,
            "lifecycle_state": "cancelled-terminal",
            "executable": False,
        }
        assert (
            await session.execute(
                text("SELECT count(*) FROM loom_capacity_guard.protected_claim_leases")
            )
        ).scalar_one() == 0

    late = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=3,
    )
    with pytest.raises(DBAPIError, match="lifecycle compare-and-set"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityAttemptLifecycleStore(
                session, registration=registration
            ).apply_transition(late)


@pytest.mark.asyncio
async def test_concurrent_inert_assignment_allows_one_transition(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    first = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    second = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(plan)

    async def assign(transition: InertAttemptTransitionV1) -> InertAttemptTransitionV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityAttemptLifecycleStore(
                session, registration=registration
            ).apply_transition(transition)

    results = await asyncio.gather(assign(first), assign(second), return_exceptions=True)
    assert sum(isinstance(result, InertAttemptTransitionV1) for result in results) == 1
    assert sum(isinstance(result, DBAPIError) for result in results) == 1


@pytest.mark.asyncio
async def test_lifecycle_capture_reports_pending_then_assigned_and_fences_v1(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    async with _serializable_agent_session(capacity_guard_database) as session:
        pending = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )
    assert pending.sequence == 1
    assert len(pending.attempts) == 1
    assert pending.attempts[0].protected_attempt_id == attempt.protected_attempt_id
    assert pending.attempts[0].lifecycle_state == "pending-unassigned"
    assert pending.attempts[0].allowance_id is None

    assignment = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(plan)
        await CapacityAttemptLifecycleStore(session, registration=registration).apply_transition(
            assignment
        )

    with pytest.raises(DBAPIError, match="cannot represent the protected lifecycle"):
        async with _agent_session(capacity_guard_database) as session:
            await capture_demand_observation(
                session,
                registration=registration,
                expected_high_water=1,
                max_attempts=100,
            )

    async with _serializable_agent_session(capacity_guard_database) as session:
        assigned = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=1,
            max_attempts=100,
        )
    assert assigned.sequence == 2
    assert len(assigned.attempts) == 1
    observed = assigned.attempts[0]
    assert observed.lifecycle_state == "assigned"
    assert observed.lifecycle_sequence == 1
    assert observed.allowance_id == plan.placement_allowances[0].allowance_id
    assert observed.plan_id == plan.plan_id
    assert observed.pool_id == plan.pool_id
    assert observed.pool_generation == plan.pool_generation
    assert observed.profile_id == plan.profile_id
    assert observed.profile_generation == plan.profile_generation
    assert observed.profile_digest == plan.profile_digest
    assert observed.shape_id == plan.worker_shapes[0].worker_shape.shape_id
    assert observed.shape_instance_id == plan.worker_shapes[0].shape_instance_id
    assert observed.executable is False

    async with _owner_session(capacity_guard_database) as (_, _, session):
        state = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, count(o.observation_id) AS observations "
                        "FROM loom_capacity_guard.agent_reporter_state AS s "
                        "JOIN loom_capacity_guard.demand_observations AS o "
                        "ON o.agent_incarnation = s.agent_incarnation "
                        "GROUP BY s.high_water"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"high_water": 2, "observations": 2}


@pytest.mark.asyncio
async def test_lifecycle_capture_rejects_terminal_public_contradiction_without_advancing(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    assignment = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    cancellation = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="cancel",
        expected_sequence=1,
        expected_state="assigned",
    )
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(plan)
        lifecycle = CapacityAttemptLifecycleStore(session, registration=registration)
        await lifecycle.apply_transition(assignment)
        await lifecycle.apply_transition(cancellation)

    with pytest.raises(DBAPIError, match="terminal protected lifecycle contradicts"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await capture_lifecycle_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=100,
            )

    async with _owner_session(capacity_guard_database) as (_, _, session):
        state = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.demand_observations) "
                        "AS observations "
                        "FROM loom_capacity_guard.agent_reporter_state AS s"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"high_water": 0, "observations": 0}

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update()
                .where(Trial.id == attempt.trial_id)
                .values(cancellation_requested_at=datetime.now(UTC))
            )
    finally:
        admin.dispose()

    async with _serializable_agent_session(capacity_guard_database) as session:
        resolved = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )
    assert resolved.sequence == 1
    assert resolved.attempts == ()
    async with _owner_session(capacity_guard_database) as (_, _, session):
        projection = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.attempt_lifecycle_projection_blockers) "
                        "AS blockers, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.attempt_lifecycle_projection_resolutions) "
                        "AS resolutions, "
                        "(SELECT blocker.resolved_at = resolution.resolved_at "
                        "FROM loom_capacity_guard.attempt_lifecycle_projection_blockers "
                        "AS blocker JOIN "
                        "loom_capacity_guard.attempt_lifecycle_projection_resolutions "
                        "AS resolution USING (transition_id)) AS resolution_projected"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(projection) == {
        "blockers": 1,
        "resolutions": 1,
        "resolution_projected": True,
    }


@pytest.mark.asyncio
async def test_concurrent_lifecycle_capture_advances_exactly_once(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)

    async def capture_once() -> object:
        async with _serializable_agent_session(capacity_guard_database) as session:
            return await capture_lifecycle_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=100,
            )

    results = await asyncio.gather(capture_once(), capture_once(), return_exceptions=True)
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(isinstance(result, DBAPIError) for result in results) == 1

    async with _owner_session(capacity_guard_database) as (_, _, session):
        state = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.demand_observations) "
                        "AS observations "
                        "FROM loom_capacity_guard.agent_reporter_state AS s"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"high_water": 1, "observations": 1}


@pytest.mark.asyncio
async def test_lifecycle_capture_rejects_incompatible_assigned_shape_without_advancing(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(
        capacity_guard_database,
        cpu_arch="arm64",
    )
    assignment = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(plan)
        await CapacityAttemptLifecycleStore(session, registration=registration).apply_transition(
            assignment
        )

    with pytest.raises(DBAPIError, match="invalid prepared binding"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await capture_lifecycle_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=100,
            )

    async with _owner_session(capacity_guard_database) as (_, _, session):
        state = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.demand_observations) "
                        "AS observations "
                        "FROM loom_capacity_guard.agent_reporter_state AS s"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"high_water": 0, "observations": 0}


@pytest.mark.asyncio
async def test_lifecycle_capture_omits_only_deferred_pending_demand(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, _plan = await _initialize_prepared_plan(capacity_guard_database)
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update()
                .where(Trial.id == attempt.trial_id)
                .values(next_attempt_at=datetime.now(UTC) + timedelta(hours=1))
            )
    finally:
        admin.dispose()

    async with _serializable_agent_session(capacity_guard_database) as session:
        deferred = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )
    assert deferred.sequence == 1
    assert deferred.attempts == ()

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update()
                .where(Trial.id == attempt.trial_id)
                .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
            )
    finally:
        admin.dispose()

    async with _serializable_agent_session(capacity_guard_database) as session:
        ready = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=1,
            max_attempts=100,
        )
    assert ready.sequence == 2
    assert tuple(item.protected_attempt_id for item in ready.attempts) == (
        attempt.protected_attempt_id,
    )


@pytest.mark.asyncio
async def test_lifecycle_capture_row_bound_includes_deferred_source_rows(
    capacity_guard_database: dict[str, object],
) -> None:
    _, registration = await _initialize_and_register(capacity_guard_database)
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="x86_64",
        gpu_vendor="none",
        network_policies=("public",),
    )
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        for generation in (1, 2):
            trial_id = _seed_trial(capacity_guard_database)
            with admin.begin() as connection:
                connection.execute(
                    Trial.__table__.update()
                    .where(Trial.id == trial_id)
                    .values(next_attempt_at=datetime.now(UTC) + timedelta(hours=1))
                )
            attempt = ProtectedAttemptV1(
                trial_id=trial_id,
                protected_attempt_id=uuid4(),
                execution_generation=generation,
                requirements_digest=canonical_digest(requirements),
            )
            async with _owner_session(capacity_guard_database) as (_, guard_store, _):
                await guard_store.register_trial_attempt(attempt, requirements)
    finally:
        admin.dispose()

    with pytest.raises(DBAPIError, match="source row bound"):
        async with _serializable_agent_session(capacity_guard_database) as session:
            await capture_lifecycle_demand_observation(
                session,
                registration=registration,
                expected_high_water=0,
                max_attempts=1,
            )

    async with _owner_session(capacity_guard_database) as (_, _, session):
        state = (
            (
                await session.execute(
                    text(
                        "SELECT s.high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.demand_observations) "
                        "AS observations FROM loom_capacity_guard.agent_reporter_state AS s"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"high_water": 0, "observations": 0}


@pytest.mark.asyncio
async def test_inert_assignment_rechecks_current_workload_eligibility(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(plan)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update()
                .where(Trial.id == attempt.trial_id)
                .values(cancellation_requested_at=datetime.now(UTC))
            )
    finally:
        admin.dispose()

    transition = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    with pytest.raises(DBAPIError, match="prepared allowance"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityAttemptLifecycleStore(
                session, registration=registration
            ).apply_transition(transition)


@pytest.mark.asyncio
async def test_lifecycle_records_and_disabled_activation_are_append_only(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    assign = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    cancel = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="cancel",
        expected_sequence=1,
        expected_state="assigned",
    )
    async with _agent_session(capacity_guard_database) as session:
        await CapacityPreparedAdmissionStore(session, registration=registration).prepare_plan(plan)
        lifecycle = CapacityAttemptLifecycleStore(session, registration=registration)
        await lifecycle.apply_transition(assign)
        await lifecycle.apply_transition(cancel)

    statements = (
        "UPDATE loom_capacity_guard.claim_guard_activation SET activation_state = 'enabled'",
        "DELETE FROM loom_capacity_guard.attempt_lifecycle_events",
        "UPDATE loom_capacity_guard.attempt_lifecycle_heads "
        "SET transition_sequence = transition_sequence",
        "DELETE FROM loom_capacity_guard.attempt_lifecycle_heads",
        "TRUNCATE loom_capacity_guard.attempt_lifecycle_heads CASCADE",
        "UPDATE loom_capacity_guard.attempt_lifecycle_projection_blockers SET resolved_at = now()",
        "DELETE FROM loom_capacity_guard.attempt_lifecycle_projection_blockers",
        "TRUNCATE loom_capacity_guard.attempt_lifecycle_projection_resolutions",
        "TRUNCATE loom_capacity_guard.protected_claim_leases",
    )
    for statement in statements:
        with pytest.raises(
            DBAPIError,
            match=r"append-only|check constraint|lifecycle head|projection blocker",
        ):
            async with _owner_session(capacity_guard_database) as (_, _, session):
                await session.execute(text(statement))


@pytest.mark.asyncio
async def test_database_claim_guard_inspects_exact_bindings_but_always_denies(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, attempt, plan = await _initialize_prepared_plan(capacity_guard_database)
    bootstrap, worker = _prepared_worker_bindings(registration, plan)
    assignment = _attempt_transition(
        registration,
        attempt,
        plan,
        operation="assign",
        expected_sequence=0,
    )
    async with _agent_session(capacity_guard_database) as session:
        prepared = CapacityPreparedAdmissionStore(session, registration=registration)
        await prepared.prepare_plan(plan)
        await prepared.register_bootstrap(bootstrap)
        await prepared.record_prepared_worker(worker)
        await CapacityAttemptLifecycleStore(session, registration=registration).apply_transition(
            assignment
        )

    allowance = plan.placement_allowances[0]
    proposal = ClaimProposalV1(
        **registration.model_dump(mode="python"),
        proposal_id=uuid4(),
        protected_attempt_id=attempt.protected_attempt_id,
        execution_generation=attempt.execution_generation,
        requirements_digest=attempt.requirements_digest,
        expected_transition_sequence=1,
        allowance_id=allowance.allowance_id,
        plan_id=plan.plan_id,
        admission_incarnation=plan.admission_incarnation,
        manager_allocation_epoch=plan.manager_allocation_epoch,
        pool_id=plan.pool_id,
        shape_instance_id=allowance.shape_instance_id,
        submission_intent_id=allowance.submission_intent_id,
        worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation,
        bootstrap_id=worker.bootstrap_id,
        proposed_claim_epoch=1,
    )
    async with _serializable_agent_session(capacity_guard_database) as session:
        guard = DatabaseClaimGuard(session, registration=registration)
        exact = await guard.evaluate(proposal)
        assert exact.reason == "activation-disabled"
        assert exact.admitted is False
        assert exact.claim_id is None
        mismatch = await guard.evaluate(proposal.model_copy(update={"worker_id": uuid4()}))
        assert mismatch.reason == "not-admitted"
        assert mismatch.admitted is False

    with pytest.raises(DBAPIError, match="SERIALIZABLE"):
        async with _agent_session(capacity_guard_database) as session:
            await DatabaseClaimGuard(session, registration=registration).evaluate(proposal)
