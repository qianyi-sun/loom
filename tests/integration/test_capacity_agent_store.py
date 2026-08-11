"""Registration, monotonic capture, and overlap fencing for the trusted agent."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.store import (
    CapacityAgentStore,
    CapacityAgentStoreError,
    capture_demand_observation,
)
from loom_capacity_guard.contracts import (
    GuardFenceV1,
    ProtectedAttemptV1,
    SealedRequirementsV1,
    canonical_digest,
)
from loom_capacity_guard.store import CapacityGuardStore


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
                .values(autoscaler_pool_name="oldlab", autoscaler_pool_assigned_at=datetime.now(UTC))
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
        "UPDATE loom_capacity_guard.agent_runtime_authority "
        "SET agent_role_name = agent_role_name",
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
