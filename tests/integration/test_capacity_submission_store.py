"""Trusted, zero-executable initial trial registration boundary."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial, Worker
from loom_capacity_agent import contracts as agent_contracts
from loom_capacity_agent.claim_guard import InertAttemptTransitionV1
from loom_capacity_agent.contracts import (
    AgentRegistrationV1,
    InertTrialSubmissionV1,
    atomic_submission_bytes,
    atomic_submission_digest,
    database_canonical_json_bytes,
)
from loom_capacity_agent.lifecycle_store import CapacityAttemptLifecycleStore
from loom_capacity_agent.runtime import create_capacity_agent_engine
from loom_capacity_agent.store import CapacityAgentStore, capture_lifecycle_demand_observation
from loom_capacity_agent.submission_store import (
    CapacityTrialSubmissionError,
    CapacityTrialSubmissionStore,
)
from loom_capacity_guard.contracts import (
    GuardFenceV1,
    SealedRequirementsV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_guard.store import CapacityGuardStore
from loom_control_plane.scheduler.claim import claim_one


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


def _fence(*, environment_id: str = "dev-alice") -> GuardFenceV1:
    return GuardFenceV1(
        environment_id=environment_id,
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


def _seed_trial(database: dict[str, object], *, state: str = "queued") -> UUID:
    engine = create_engine(_value(database, "admin_url"))
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"guard-submission-task-{uuid4().hex}"
    try:
        with engine.begin() as connection:
            connection.execute(Team.__table__.insert().values(id=team_id, name=f"sub-{team_id}"))
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
                        "cpu_arch": "any",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state=state,
                )
            )
    finally:
        engine.dispose()
    return trial_id


@asynccontextmanager
async def _owner_session(database: dict[str, object]) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(
        make_url(_value(database, "migrator_url")), isolation_level="SERIALIZABLE"
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    owner = _value(database, "owner_role")
    quoted_owner = engine.sync_engine.dialect.identifier_preparer.quote(owner)
    try:
        async with factory() as session, session.begin():
            await session.execute(text(f"SET LOCAL ROLE {quoted_owner}"))
            yield session
    finally:
        await engine.dispose()


@asynccontextmanager
async def _agent_session(database: dict[str, object]) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(
        make_url(_value(database, "agent_url")), isolation_level="SERIALIZABLE"
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            yield session
    finally:
        await engine.dispose()


async def _initialize(
    database: dict[str, object],
) -> tuple[AgentRegistrationV1, UUID, InertTrialSubmissionV1]:
    fence = _fence()
    registration = _registration(fence)
    trial_id = _seed_trial(database)
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("public",),
    )
    submission = InertTrialSubmissionV1(
        **registration.model_dump(mode="python"),
        trial_id=trial_id,
        protected_attempt_id=uuid4(),
        attempt_sequence=0,
        execution_generation=registration.deployment_generation,
        requirements=requirements,
        requirements_digest=canonical_digest(requirements),
    )
    async with _owner_session(database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(database, "owner_role"),
            expected_agent_role=_value(database, "agent_role"),
        ).register_agent(registration)
    return registration, trial_id, submission


def _seed_trial_inputs(database: dict[str, object]) -> tuple[UUID, str]:
    engine = create_engine(_value(database, "admin_url"))
    team_id = uuid4()
    task_id = f"guard-atomic-submission-task-{uuid4().hex}"
    try:
        with engine.begin() as connection:
            connection.execute(Team.__table__.insert().values(id=team_id, name=f"sub-{team_id}"))
            connection.execute(TeamQuota.__table__.insert().values(team_id=team_id))
            connection.execute(
                Task.__table__.insert().values(
                    id=task_id,
                    checksum="0" * 64,
                    config={"schema_version": "1"},
                )
            )
    finally:
        engine.dispose()
    return team_id, task_id


def _atomic_submission(
    registration: AgentRegistrationV1,
    *,
    team_id: UUID,
    task_id: str,
    idempotency_key: str | None = None,
) -> agent_contracts.AtomicTrialSubmissionV1:
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("public",),
    )
    return agent_contracts.AtomicTrialSubmissionV1(
        **registration.model_dump(mode="python"),
        trial_id=uuid4(),
        protected_attempt_id=uuid4(),
        execution_generation=registration.deployment_generation,
        requirements=requirements,
        requirements_digest=canonical_digest(requirements),
        team_id=team_id,
        task_id=task_id,
        config={"agent_name": "oracle", "agent_model": None},
        submit_priority=100,
        idempotency_key=idempotency_key,
    )


@pytest.mark.parametrize(
    "value",
    (
        {"ascii": "café", "é": "雪", "combining": "e\u0301"},
        {"bbb": {"zz": 0.2, "a": [1e-7, -0.0]}, "aa": {"🧵": "loom", "k": True}},
        {"small": 1e-7, "negative_zero": -0.0, "decimal": 0.2},
    ),
)
def test_database_canonical_json_matches_postgresql_jsonb_text(
    capacity_guard_database: dict[str, object],
    value: object,
) -> None:
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            postgresql_text = connection.execute(
                text("SELECT CAST(CAST(:payload AS jsonb) AS text)"),
                {"payload": json.dumps(value, ensure_ascii=False, allow_nan=False)},
            ).scalar_one()
    finally:
        admin.dispose()

    assert database_canonical_json_bytes(value) == postgresql_text.encode("utf-8")


@pytest.mark.asyncio
async def test_capacity_agent_runtime_engine_is_serializable(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_capacity_agent_engine(_value(capacity_guard_database, "agent_url"))
    try:
        async with engine.connect() as connection:
            isolation = (await connection.execute(text("SHOW transaction_isolation"))).scalar_one()
    finally:
        await engine.dispose()

    assert isolation == "serializable"


@pytest.mark.asyncio
async def test_agent_atomically_creates_public_trial_and_protected_attempt(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    trial_id = uuid4()
    protected_attempt_id = uuid4()
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("public",),
    )
    submission = agent_contracts.AtomicTrialSubmissionV1(
        **registration.model_dump(mode="python"),
        trial_id=trial_id,
        protected_attempt_id=protected_attempt_id,
        execution_generation=registration.deployment_generation,
        requirements=requirements,
        requirements_digest=canonical_digest(requirements),
        team_id=team_id,
        task_id=task_id,
        config={"agent_name": "oracle", "agent_model": None},
        submit_priority=100,
        idempotency_key="atomic-submission-1",
    )
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    async with _agent_session(capacity_guard_database) as session:
        receipt = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    assert receipt.trial_id == trial_id
    assert receipt.protected_attempt_id == protected_attempt_id
    assert receipt.requirements_digest == submission.requirements_digest
    assert receipt.lifecycle_authority_id is not None
    assert receipt.replayed is False
    assert receipt.executable is False

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT t.id, t.team_id, t.task_id, t.config, t.requires_caps, "
                        "t.state, t.submit_priority, t.idempotency_key, "
                        "t.lifecycle_authority_id, l.environment AS lifecycle_environment, "
                        "l.namespace AS lifecycle_namespace, l.team_id AS lifecycle_team_id, "
                        "l.data_class, l.owner_kind, l.owner_id, "
                        "l.created_at AS lifecycle_created_at, l.expires_at, l.pinned, "
                        "l.state AS lifecycle_authority_state, "
                        "a.protected_attempt_id, a.execution_generation, a.attempt_sequence, "
                        "a.claim_state, h.lifecycle_state, h.executable "
                        "FROM public.trials AS t "
                        "JOIN loom_capacity_guard.trial_attempts AS a ON a.trial_id = t.id "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS h "
                        "ON h.protected_attempt_id = a.protected_attempt_id "
                        "JOIN public.data_lifecycle_authorities AS l "
                        "ON l.id = t.lifecycle_authority_id "
                        "WHERE t.id = :trial_id"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(row) == {
        "id": trial_id,
        "team_id": team_id,
        "task_id": task_id,
        "config": {"agent_name": "oracle", "agent_model": None},
        "requires_caps": {
            "os": "linux",
            "cpu_arch": "any",
            "gpu_vendor": "none",
            "network_policies": ["public"],
        },
        "state": "protected-pending",
        "submit_priority": 100,
        "idempotency_key": "atomic-submission-1",
        "lifecycle_authority_id": receipt.lifecycle_authority_id,
        "lifecycle_environment": "dev-alice",
        "lifecycle_namespace": "loom-dev-alice",
        "lifecycle_team_id": team_id,
        "data_class": "trial",
        "owner_kind": "trial",
        "owner_id": str(trial_id),
        "lifecycle_created_at": receipt.submitted_at,
        "expires_at": None,
        "pinned": True,
        "lifecycle_authority_state": "active",
        "protected_attempt_id": protected_attempt_id,
        "execution_generation": registration.deployment_generation,
        "attempt_sequence": 0,
        "claim_state": "queued",
        "lifecycle_state": "pending-unassigned",
        "executable": False,
    }


@pytest.mark.asyncio
async def test_atomic_submission_is_not_claimable_by_the_legacy_worker_path(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    worker_id = uuid4()
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Worker.__table__.insert().values(
                    id=worker_id,
                    hostname=f"legacy-worker-{worker_id}",
                    version="test",
                    capabilities=[],
                    registered_at=datetime.now(UTC),
                    last_seen_at=datetime.now(UTC),
                    status="active",
                )
            )
    finally:
        admin.dispose()

    engine = create_async_engine(_value(capacity_guard_database, "admin_url"))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            claimed = await claim_one(
                session,
                worker_id=worker_id,
                worker_os=["linux"],
                worker_cpu_arches=["x86_64"],
                worker_gpu_vendors=["none"],
                worker_network_policies=["public"],
            )
        async with engine.connect() as connection:
            state = (
                await connection.execute(
                    text("SELECT state FROM public.trials WHERE id = :trial_id"),
                    {"trial_id": submission.trial_id},
                )
            ).scalar_one()
    finally:
        await engine.dispose()

    assert claimed is None
    assert state == "protected-pending"


@pytest.mark.asyncio
async def test_atomic_submission_remains_visible_to_disabled_shadow_demand(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)
    async with _agent_session(capacity_guard_database) as session:
        observation = await capture_lifecycle_demand_observation(
            session,
            registration=registration,
            expected_high_water=0,
            max_attempts=100,
        )

    assert observation.executable is False
    assert len(observation.attempts) == 1
    attempt = observation.attempts[0]
    assert attempt.protected_attempt_id == submission.protected_attempt_id
    assert attempt.lifecycle_state == "pending-unassigned"
    assert attempt.executable is False
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            state = connection.execute(
                text("SELECT state FROM public.trials WHERE id = :trial_id"),
                {"trial_id": submission.trial_id},
            ).scalar_one()
    finally:
        admin.dispose()
    assert state == "protected-pending"


@pytest.mark.asyncio
async def test_atomic_submission_ledger_is_append_only(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        statements = (
            "UPDATE loom_capacity_guard.atomic_trial_submissions "
            "SET payload_digest = payload_digest WHERE trial_id = :trial_id",
            "DELETE FROM loom_capacity_guard.atomic_trial_submissions WHERE trial_id = :trial_id",
            "TRUNCATE loom_capacity_guard.atomic_trial_submissions CASCADE",
        )
        for statement in statements:
            with pytest.raises(DBAPIError, match="append-only"):
                with admin.begin() as connection:
                    connection.execute(text(statement), {"trial_id": submission.trial_id})
    finally:
        admin.dispose()


@pytest.mark.asyncio
async def test_atomic_submission_blocks_guard_downgrade_without_data_loss(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        receipt = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_DB_URL", _value(capacity_guard_database, "migrator_url")
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OWNER_ROLE", _value(capacity_guard_database, "owner_role")
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_AGENT_ROLE", _value(capacity_guard_database, "agent_role")
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE",
        _value(capacity_guard_database, "executor_role"),
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OBSERVER_ROLE",
        _value(capacity_guard_database, "observer_role"),
    )
    with pytest.raises(DBAPIError, match="atomic trial submissions exist"):
        command.downgrade(config, "guard_0010")

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT version.version_num, trial.state, "
                        "trial.lifecycle_authority_id, submission.protected_attempt_id, "
                        "head.executable FROM "
                        "loom_capacity_guard.capacity_guard_alembic_version AS version "
                        "JOIN public.trials AS trial ON trial.id = :trial_id "
                        "JOIN loom_capacity_guard.atomic_trial_submissions AS submission "
                        "ON submission.trial_id = trial.id "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = submission.protected_attempt_id"
                    ),
                    {"trial_id": submission.trial_id},
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(row) == {
        "version_num": "guard_0020",
        "state": "protected-pending",
        "lifecycle_authority_id": receipt.lifecycle_authority_id,
        "protected_attempt_id": submission.protected_attempt_id,
        "executable": False,
    }


@pytest.mark.asyncio
async def test_concurrent_guard_downgrade_observes_committing_atomic_submission(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    application_name = f"guard-downgrade-race-{uuid4().hex}"
    migrator_url = make_url(_value(capacity_guard_database, "migrator_url")).update_query_dict(
        {"application_name": application_name}
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_DB_URL", migrator_url.render_as_string(hide_password=False)
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OWNER_ROLE", _value(capacity_guard_database, "owner_role")
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_AGENT_ROLE", _value(capacity_guard_database, "agent_role")
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE",
        _value(capacity_guard_database, "executor_role"),
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OBSERVER_ROLE",
        _value(capacity_guard_database, "observer_role"),
    )

    agent_engine = create_async_engine(
        make_url(_value(capacity_guard_database, "agent_url")),
        isolation_level="SERIALIZABLE",
    )
    agent_factory = async_sessionmaker(agent_engine, expire_on_commit=False)
    downgrade_task: asyncio.Task[None] | None = None
    transaction = None
    try:
        async with agent_factory() as session:
            transaction = await session.begin()
            receipt = await CapacityTrialSubmissionStore(
                session, registration=registration
            ).create_initial_submission(submission)
            downgrade_task = asyncio.create_task(
                asyncio.to_thread(command.downgrade, config, "guard_0010")
            )

            admin = create_engine(_value(capacity_guard_database, "admin_url"))
            try:
                async with asyncio.timeout(10):
                    while True:
                        with admin.connect() as connection:
                            waiting_on_guard_relation = connection.execute(
                                text(
                                    "SELECT EXISTS ("
                                    "SELECT 1 FROM pg_locks AS lock "
                                    "JOIN pg_stat_activity AS activity ON activity.pid = lock.pid "
                                    "JOIN pg_class AS relation ON relation.oid = lock.relation "
                                    "JOIN pg_namespace AS namespace "
                                    "ON namespace.oid = relation.relnamespace "
                                    "WHERE activity.application_name = :application_name "
                                    "AND namespace.nspname = 'loom_capacity_guard' "
                                    "AND lock.granted IS FALSE)"
                                ),
                                {"application_name": application_name},
                            ).scalar_one()
                        if waiting_on_guard_relation:
                            break
                        if downgrade_task.done():
                            pytest.fail(
                                "guard downgrade completed before overlapping the open "
                                f"atomic submission: {downgrade_task.exception()!r}"
                            )
                        await asyncio.sleep(0.01)
            finally:
                admin.dispose()

            await transaction.commit()
            with pytest.raises(DBAPIError, match="atomic trial submissions exist"):
                await downgrade_task

        admin = create_engine(_value(capacity_guard_database, "admin_url"))
        try:
            with admin.connect() as connection:
                row = (
                    connection.execute(
                        text(
                            "SELECT version.version_num, trial.state, "
                            "trial.lifecycle_authority_id, submission.protected_attempt_id, "
                            "head.executable FROM "
                            "loom_capacity_guard.capacity_guard_alembic_version AS version "
                            "JOIN public.trials AS trial ON trial.id = :trial_id "
                            "JOIN loom_capacity_guard.atomic_trial_submissions AS submission "
                            "ON submission.trial_id = trial.id "
                            "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                            "ON head.protected_attempt_id = submission.protected_attempt_id"
                        ),
                        {"trial_id": submission.trial_id},
                    )
                    .mappings()
                    .one()
                )
        finally:
            admin.dispose()
    finally:
        if transaction is not None and transaction.is_active:
            await transaction.rollback()
        if downgrade_task is not None and not downgrade_task.done():
            await downgrade_task
        await agent_engine.dispose()

    assert dict(row) == {
        "version_num": "guard_0020",
        "state": "protected-pending",
        "lifecycle_authority_id": receipt.lifecycle_authority_id,
        "protected_attempt_id": submission.protected_attempt_id,
        "executable": False,
    }


@pytest.mark.parametrize(
    ("environment_id", "expected_namespace"),
    (
        ("development", "loom-dev"),
        ("production", "loom-prod"),
        ("dev-alice", "loom-dev-alice"),
    ),
)
@pytest.mark.asyncio
async def test_atomic_submission_uses_generated_lifecycle_scope(
    capacity_guard_database: dict[str, object],
    environment_id: str,
    expected_namespace: str,
) -> None:
    fence = _fence(environment_id=environment_id)
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        receipt = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            lifecycle_scope = connection.execute(
                text(
                    "SELECT environment, namespace "
                    "FROM public.data_lifecycle_authorities WHERE id = :authority_id"
                ),
                {"authority_id": receipt.lifecycle_authority_id},
            ).one()
    finally:
        admin.dispose()
    assert lifecycle_scope == (environment_id, expected_namespace)


@pytest.mark.asyncio
async def test_atomic_submission_rejects_staging_until_protected_retention_exists(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence(environment_id="staging")
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    with pytest.raises(DBAPIError, match="protected retention is unavailable"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).create_initial_submission(submission)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            counts = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM public.trials WHERE id = :trial_id) "
                        "AS trials, (SELECT count(*) FROM public.data_lifecycle_authorities "
                        "WHERE owner_kind = 'trial' AND owner_id = :owner_id) AS authorities, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id) AS attempts"
                    ),
                    {"trial_id": submission.trial_id, "owner_id": str(submission.trial_id)},
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(counts) == {"trials": 0, "authorities": 0, "attempts": 0}


@pytest.mark.asyncio
async def test_atomic_submission_database_rejects_caller_controlled_lifecycle_scope(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    raw_payload = submission.model_dump(mode="json")
    raw_payload["lifecycle_environment"] = "staging"
    raw_payload["lifecycle_namespace"] = "loom-staging"
    payload_bytes = database_canonical_json_bytes(raw_payload)
    protected = InertTrialSubmissionV1.model_validate(
        {field: getattr(submission, field) for field in InertTrialSubmissionV1.model_fields}
    )
    protected_bytes = canonical_bytes(protected)
    with pytest.raises(DBAPIError, match="payload is invalid"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.submit_inert_trial_projection("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:protected AS jsonb), CAST(:protected_canonical AS bytea), "
                    ":protected_digest, CAST(:requirements AS bytea), "
                    ":requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": payload_bytes.decode("utf-8"),
                    "canonical": payload_bytes,
                    "digest": hashlib.sha256(payload_bytes).hexdigest(),
                    "protected": protected_bytes.decode("ascii"),
                    "protected_canonical": protected_bytes,
                    "protected_digest": canonical_digest(protected),
                    "requirements": canonical_bytes(submission.requirements),
                    "requirements_digest": submission.requirements_digest,
                },
            )


@pytest.mark.asyncio
async def test_atomic_submission_exact_replay_survives_lifecycle_progress(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        first = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    transition = InertAttemptTransitionV1(
        **registration.model_dump(mode="python"),
        transition_id=uuid4(),
        protected_attempt_id=first.protected_attempt_id,
        execution_generation=registration.deployment_generation,
        requirements_digest=submission.requirements_digest,
        expected_transition_sequence=0,
        operation="cancel",
        expected_state="pending-unassigned",
        target_state="cancelled-terminal",
        transition_reason="owner-cancelled-unclaimed",
    )
    async with _agent_session(capacity_guard_database) as session:
        await CapacityAttemptLifecycleStore(session, registration=registration).apply_transition(
            transition
        )
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.trials SET state = 'cancelled', "
                    "cancellation_requested_at = now(), attempt_count = 1, "
                    "next_attempt_at = now(), autoscaler_pool_name = 'oldlab', "
                    "autoscaler_pool_assigned_at = now() "
                    "WHERE id = :trial_id"
                ),
                {"trial_id": first.trial_id},
            )
    finally:
        admin.dispose()

    async with _agent_session(capacity_guard_database) as session:
        replay = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    assert replay.model_dump(exclude={"replayed"}) == first.model_dump(exclude={"replayed"})
    assert replay.replayed is True


@pytest.mark.asyncio
async def test_atomic_submission_idempotency_reuses_the_winning_protected_identity(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("public",),
    )

    def submission() -> agent_contracts.AtomicTrialSubmissionV1:
        return agent_contracts.AtomicTrialSubmissionV1(
            **registration.model_dump(mode="python"),
            trial_id=uuid4(),
            protected_attempt_id=uuid4(),
            execution_generation=registration.deployment_generation,
            requirements=requirements,
            requirements_digest=canonical_digest(requirements),
            team_id=team_id,
            task_id=task_id,
            config={"agent_name": "oracle", "agent_model": None},
            submit_priority=100,
            idempotency_key="atomic-idempotency-race",
        )

    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    first_submission = submission()
    second_submission = submission()
    async with _agent_session(capacity_guard_database) as session:
        first = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(first_submission)
    async with _agent_session(capacity_guard_database) as session:
        replay = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(second_submission)

    assert replay.trial_id == first.trial_id
    assert replay.protected_attempt_id == first.protected_attempt_id
    assert replay.requirements_digest == first.requirements_digest
    assert replay.submitted_at == first.submitted_at
    assert replay.replayed is True

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            counts = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM public.trials "
                        "WHERE idempotency_key = 'atomic-idempotency-race') AS trials, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts) AS attempts"
                    )
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(counts) == {"trials": 1, "attempts": 1}


@pytest.mark.asyncio
async def test_concurrent_atomic_idempotency_converges_after_serializable_retry(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submissions = tuple(
        _atomic_submission(
            registration,
            team_id=team_id,
            task_id=task_id,
            idempotency_key="atomic-concurrent-idempotency",
        )
        for _ in range(2)
    )
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    async def create(
        submission: agent_contracts.AtomicTrialSubmissionV1,
    ) -> agent_contracts.AtomicTrialSubmissionReceiptV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityTrialSubmissionStore(
                session, registration=registration
            ).create_initial_submission(submission)

    raced = await asyncio.gather(*(create(item) for item in submissions), return_exceptions=True)
    winners = [
        item for item in raced if isinstance(item, agent_contracts.AtomicTrialSubmissionReceiptV1)
    ]
    assert winners
    assert all(
        isinstance(item, (agent_contracts.AtomicTrialSubmissionReceiptV1, OperationalError))
        for item in raced
    )

    converged = [await create(item) for item in submissions]
    identities = {
        (item.trial_id, item.protected_attempt_id, item.lifecycle_authority_id)
        for item in converged
    }
    assert len(identities) == 1
    assert all(item.replayed for item in converged)


@pytest.mark.asyncio
async def test_atomic_idempotency_replay_revalidates_the_protected_payload(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    first_submission = _atomic_submission(
        registration,
        team_id=team_id,
        task_id=task_id,
        idempotency_key="atomic-protected-payload-replay",
    )
    replay_submission = _atomic_submission(
        registration,
        team_id=team_id,
        task_id=task_id,
        idempotency_key="atomic-protected-payload-replay",
    )
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(first_submission)

    protected = InertTrialSubmissionV1.model_validate(
        {field: getattr(replay_submission, field) for field in InertTrialSubmissionV1.model_fields}
    ).model_dump(mode="json")
    protected["attempt_sequence"] = 1
    protected_bytes = json.dumps(
        protected,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    payload_bytes = atomic_submission_bytes(replay_submission)
    with pytest.raises(DBAPIError, match="protected payload is invalid"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.submit_inert_trial_projection("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:protected AS jsonb), CAST(:protected_canonical AS bytea), "
                    ":protected_digest, CAST(:requirements AS bytea), "
                    ":requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": payload_bytes.decode("utf-8"),
                    "canonical": payload_bytes,
                    "digest": atomic_submission_digest(replay_submission),
                    "protected": protected_bytes.decode("ascii"),
                    "protected_canonical": protected_bytes,
                    "protected_digest": hashlib.sha256(protected_bytes).hexdigest(),
                    "requirements": canonical_bytes(replay_submission.requirements),
                    "requirements_digest": replay_submission.requirements_digest,
                },
            )


@pytest.mark.asyncio
async def test_atomic_idempotency_replay_rejects_equal_requested_identities(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    first_submission = _atomic_submission(
        registration,
        team_id=team_id,
        task_id=task_id,
        idempotency_key="atomic-equal-replay-identities",
    )
    replay_submission = _atomic_submission(
        registration,
        team_id=team_id,
        task_id=task_id,
        idempotency_key="atomic-equal-replay-identities",
    )
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(first_submission)

    requested_identity = str(replay_submission.trial_id)
    raw_payload = replay_submission.model_dump(mode="json")
    raw_payload["protected_attempt_id"] = requested_identity
    payload_bytes = database_canonical_json_bytes(raw_payload)
    raw_protected = {
        field: replay_submission.model_dump(mode="json")[field]
        for field in InertTrialSubmissionV1.model_fields
    }
    raw_protected["protected_attempt_id"] = requested_identity
    protected_bytes = json.dumps(
        raw_protected,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    with pytest.raises(DBAPIError, match="distinct"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.submit_inert_trial_projection("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:protected AS jsonb), CAST(:protected_canonical AS bytea), "
                    ":protected_digest, CAST(:requirements AS bytea), "
                    ":requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": payload_bytes.decode("utf-8"),
                    "canonical": payload_bytes,
                    "digest": hashlib.sha256(payload_bytes).hexdigest(),
                    "protected": protected_bytes.decode("ascii"),
                    "protected_canonical": protected_bytes,
                    "protected_digest": hashlib.sha256(protected_bytes).hexdigest(),
                    "requirements": canonical_bytes(replay_submission.requirements),
                    "requirements_digest": replay_submission.requirements_digest,
                },
            )


@pytest.mark.asyncio
async def test_atomic_submission_database_rejects_wrong_projection_json_types(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    raw_payload = submission.model_dump(mode="json")
    raw_payload["task_id"] = 7
    payload_bytes = database_canonical_json_bytes(raw_payload)
    protected = InertTrialSubmissionV1.model_validate(
        {field: getattr(submission, field) for field in InertTrialSubmissionV1.model_fields}
    )
    protected_bytes = canonical_bytes(protected)
    with pytest.raises(DBAPIError, match="atomic trial submission payload is invalid"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.submit_inert_trial_projection("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:protected AS jsonb), CAST(:protected_canonical AS bytea), "
                    ":protected_digest, CAST(:requirements AS bytea), "
                    ":requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": payload_bytes.decode("utf-8"),
                    "canonical": payload_bytes,
                    "digest": hashlib.sha256(payload_bytes).hexdigest(),
                    "protected": protected_bytes.decode("ascii"),
                    "protected_canonical": protected_bytes,
                    "protected_digest": canonical_digest(protected),
                    "requirements": canonical_bytes(submission.requirements),
                    "requirements_digest": submission.requirements_digest,
                },
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("integer_field", "invalid_value"),
    (
        ("submit_priority", 1.5),
        ("sample_idx", 1.5),
        ("combination_idx", 1.5),
        ("submit_priority", 1 << 100),
    ),
)
async def test_atomic_submission_database_rejects_invalid_integer_fields(
    capacity_guard_database: dict[str, object],
    integer_field: str,
    invalid_value: float | int,
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    raw_payload = submission.model_dump(mode="json")
    raw_payload[integer_field] = invalid_value
    payload_bytes = database_canonical_json_bytes(raw_payload)
    protected = InertTrialSubmissionV1.model_validate(
        {field: getattr(submission, field) for field in InertTrialSubmissionV1.model_fields}
    )
    protected_bytes = canonical_bytes(protected)
    with pytest.raises(DBAPIError, match="payload is invalid"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.submit_inert_trial_projection("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:protected AS jsonb), CAST(:protected_canonical AS bytea), "
                    ":protected_digest, CAST(:requirements AS bytea), "
                    ":requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": payload_bytes.decode("utf-8"),
                    "canonical": payload_bytes,
                    "digest": hashlib.sha256(payload_bytes).hexdigest(),
                    "protected": protected_bytes.decode("ascii"),
                    "protected_canonical": protected_bytes,
                    "protected_digest": canonical_digest(protected),
                    "requirements": canonical_bytes(submission.requirements),
                    "requirements_digest": submission.requirements_digest,
                },
            )


@pytest.mark.asyncio
async def test_atomic_submission_database_rejects_noncanonical_outer_payload(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    payload_bytes = json.dumps(
        submission.model_dump(mode="json"),
        sort_keys=False,
        indent=1,
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    protected = InertTrialSubmissionV1.model_validate(
        {field: getattr(submission, field) for field in InertTrialSubmissionV1.model_fields}
    )
    protected_bytes = canonical_bytes(protected)
    with pytest.raises(DBAPIError, match="canonical"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.submit_inert_trial_projection("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:protected AS jsonb), CAST(:protected_canonical AS bytea), "
                    ":protected_digest, CAST(:requirements AS bytea), "
                    ":requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": payload_bytes.decode("ascii"),
                    "canonical": payload_bytes,
                    "digest": hashlib.sha256(payload_bytes).hexdigest(),
                    "protected": protected_bytes.decode("ascii"),
                    "protected_canonical": protected_bytes,
                    "protected_digest": canonical_digest(protected),
                    "requirements": canonical_bytes(submission.requirements),
                    "requirements_digest": submission.requirements_digest,
                },
            )


@pytest.mark.asyncio
async def test_atomic_submission_exact_replay_is_singleton_without_idempotency_key(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    async with _agent_session(capacity_guard_database) as session:
        first = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)
    async with _agent_session(capacity_guard_database) as session:
        replay = await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(submission)

    assert replay.model_dump(exclude={"replayed"}) == first.model_dump(exclude={"replayed"})
    assert first.replayed is False
    assert replay.replayed is True
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            counts = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM public.trials WHERE id = :trial_id) "
                        "AS trials, (SELECT count(*) FROM public.data_lifecycle_authorities "
                        "WHERE owner_kind = 'trial' AND owner_id = :owner_id) AS authorities, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id) AS attempts, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        "WHERE event_type = 'trial_submission_registered.v1' "
                        "AND trial_id = :trial_id) AS audits"
                    ),
                    {"trial_id": submission.trial_id, "owner_id": str(submission.trial_id)},
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(counts) == {"trials": 1, "authorities": 1, "attempts": 1, "audits": 1}


@pytest.mark.asyncio
async def test_atomic_submission_rejects_matching_public_row_without_atomic_provenance(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.insert().values(
                    id=submission.trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config=submission.config,
                    requires_caps={
                        "os": "linux",
                        "cpu_arch": "any",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                    submit_priority=submission.submit_priority,
                )
            )
    finally:
        admin.dispose()
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)

    with pytest.raises(DBAPIError, match="atomic submission provenance"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).create_initial_submission(submission)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT t.lifecycle_authority_id, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = t.id) AS attempts "
                        "FROM public.trials AS t WHERE t.id = :trial_id"
                    ),
                    {"trial_id": submission.trial_id},
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(row) == {"lifecycle_authority_id": None, "attempts": 0}


@pytest.mark.asyncio
async def test_atomic_idempotency_collision_across_teams_fails_without_identity_leak(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    first_team_id, first_task_id = _seed_trial_inputs(capacity_guard_database)
    second_team_id, second_task_id = _seed_trial_inputs(capacity_guard_database)
    first_submission = _atomic_submission(
        registration,
        team_id=first_team_id,
        task_id=first_task_id,
        idempotency_key="atomic-cross-team-collision",
    )
    second_submission = _atomic_submission(
        registration,
        team_id=second_team_id,
        task_id=second_task_id,
        idempotency_key="atomic-cross-team-collision",
    )
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(first_submission)

    with pytest.raises(DBAPIError, match="conflicting atomic trial submission replay"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).create_initial_submission(second_submission)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT id, team_id FROM public.trials "
                        "WHERE idempotency_key = 'atomic-cross-team-collision'"
                    )
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(row) == {"id": first_submission.trial_id, "team_id": first_team_id}


@pytest.mark.asyncio
async def test_atomic_submission_rolls_back_every_fragment_on_protected_identity_conflict(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    registration = _registration(fence)
    team_id, task_id = _seed_trial_inputs(capacity_guard_database)
    first_submission = _atomic_submission(registration, team_id=team_id, task_id=task_id)
    conflicting_submission = _atomic_submission(
        registration, team_id=team_id, task_id=task_id
    ).model_copy(update={"protected_attempt_id": first_submission.protected_attempt_id})
    async with _owner_session(capacity_guard_database) as session:
        await CapacityGuardStore(
            session, expected_owner_role=_value(capacity_guard_database, "owner_role")
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(capacity_guard_database, "owner_role"),
            expected_agent_role=_value(capacity_guard_database, "agent_role"),
        ).register_agent(registration)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).create_initial_submission(first_submission)

    with pytest.raises(DBAPIError, match="conflicting inert submission replay"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).create_initial_submission(conflicting_submission)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.connect() as connection:
            counts = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM public.trials WHERE id = :trial_id) "
                        "AS trials, (SELECT count(*) FROM public.data_lifecycle_authorities "
                        "WHERE owner_kind = 'trial' AND owner_id = :owner_id) AS authorities, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_requirements "
                        "WHERE trial_id = :trial_id) AS requirements, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id) AS attempts"
                    ),
                    {
                        "trial_id": conflicting_submission.trial_id,
                        "owner_id": str(conflicting_submission.trial_id),
                    },
                )
                .mappings()
                .one()
            )
    finally:
        admin.dispose()
    assert dict(counts) == {
        "trials": 0,
        "authorities": 0,
        "requirements": 0,
        "attempts": 0,
    }


@pytest.mark.asyncio
async def test_agent_registers_initial_submission_and_exact_replay_once(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, trial_id, submission = await _initialize(capacity_guard_database)

    async def register() -> InertTrialSubmissionV1:
        async with _agent_session(capacity_guard_database) as session:
            return await CapacityTrialSubmissionStore(
                session, registration=registration
            ).register_initial_submission(submission)

    concurrent = await asyncio.gather(register(), register(), return_exceptions=True)
    assert sum(result == submission for result in concurrent) >= 1
    assert all(
        result == submission or isinstance(result, OperationalError) for result in concurrent
    )
    # SERIALIZABLE overlap may roll one exact replay back. Retrying the same
    # immutable contract must converge without adding another row or audit.
    assert await register() == submission
    async with _owner_session(capacity_guard_database) as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT a.trial_id, a.protected_attempt_id, "
                        "a.execution_generation, a.attempt_sequence, h.lifecycle_state, "
                        "h.executable, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        "WHERE event_type = 'trial_submission_registered.v1') AS audits "
                        "FROM loom_capacity_guard.trial_attempts AS a "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS h "
                        "ON h.protected_attempt_id = a.protected_attempt_id "
                        "WHERE a.trial_id = :trial_id"
                    ),
                    {"trial_id": trial_id},
                )
            )
            .mappings()
            .one()
        )
    assert dict(row) == {
        "trial_id": trial_id,
        "protected_attempt_id": submission.protected_attempt_id,
        "execution_generation": registration.deployment_generation,
        "attempt_sequence": 0,
        "lifecycle_state": "pending-unassigned",
        "executable": False,
        "audits": 1,
    }


@pytest.mark.asyncio
async def test_submission_rejects_conflict_generation_and_public_state(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, trial_id, submission = await _initialize(capacity_guard_database)
    async with _agent_session(capacity_guard_database) as session:
        store = CapacityTrialSubmissionStore(session, registration=registration)
        await store.register_initial_submission(submission)

    conflict = submission.model_copy(update={"protected_attempt_id": uuid4()})
    with pytest.raises(DBAPIError, match="conflicting inert submission replay"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).register_initial_submission(conflict)

    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update().where(Trial.id == trial_id).values(state="failed")
            )
    finally:
        admin.dispose()
    with pytest.raises(DBAPIError, match="public trial is not an initial runnable submission"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).register_initial_submission(submission)


@pytest.mark.asyncio
async def test_submission_rejects_public_requirement_drift_and_noncanonical_payload(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, trial_id, submission = await _initialize(capacity_guard_database)
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update()
                .where(Trial.id == trial_id)
                .values(
                    requires_caps={
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    }
                )
            )
    finally:
        admin.dispose()
    with pytest.raises(DBAPIError, match="public requirements differ"):
        async with _agent_session(capacity_guard_database) as session:
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).register_initial_submission(submission)

    payload = canonical_bytes(submission)
    with pytest.raises(DBAPIError, match="payload is invalid"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.register_inert_trial_submission("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:requirements AS bytea), :requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": payload.decode("ascii"),
                    "canonical": payload + b" ",
                    "digest": canonical_digest(submission),
                    "requirements": canonical_bytes(submission.requirements),
                    "requirements_digest": submission.requirements_digest,
                },
            )

    noncanonical_requirements = canonical_bytes(submission.requirements) + b" "
    noncanonical_requirements_digest = hashlib.sha256(noncanonical_requirements).hexdigest()
    raw_payload = submission.model_dump(mode="json")
    raw_payload["requirements_digest"] = noncanonical_requirements_digest
    raw_payload_bytes = json.dumps(
        raw_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    with pytest.raises(DBAPIError, match="not canonically encoded"):
        async with _agent_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.register_inert_trial_submission("
                    ":agent, CAST(:payload AS jsonb), CAST(:canonical AS bytea), :digest, "
                    "CAST(:requirements AS bytea), :requirements_digest)"
                ),
                {
                    "agent": registration.agent_incarnation,
                    "payload": raw_payload_bytes.decode("ascii"),
                    "canonical": raw_payload_bytes,
                    "digest": hashlib.sha256(raw_payload_bytes).hexdigest(),
                    "requirements": noncanonical_requirements,
                    "requirements_digest": noncanonical_requirements_digest,
                },
            )


@pytest.mark.asyncio
async def test_submission_store_rejects_registration_mismatch_before_database(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, _trial_id, submission = await _initialize(capacity_guard_database)
    mismatched = submission.model_copy(update={"candidate_digest": "b" * 64})
    async with _agent_session(capacity_guard_database) as session:
        with pytest.raises(CapacityTrialSubmissionError, match="binding mismatch"):
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).register_initial_submission(mismatched)


@pytest.mark.asyncio
async def test_submission_normalizes_an_explicit_physical_pool_pin(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, trial_id, submission = await _initialize(capacity_guard_database)
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin.begin() as connection:
            connection.execute(
                Trial.__table__.update()
                .where(Trial.id == trial_id)
                .values(
                    requires_caps={
                        "os": "linux",
                        "cpu_arch": "any",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                        "worker_pool": "oldlab",
                    }
                )
            )
    finally:
        admin.dispose()
    requirements = submission.requirements.model_copy(update={"required_pool": "oldlab"})
    pinned = submission.model_copy(
        update={
            "requirements": requirements,
            "requirements_digest": canonical_digest(requirements),
        }
    )
    async with _agent_session(capacity_guard_database) as session:
        assert (
            await CapacityTrialSubmissionStore(
                session, registration=registration
            ).register_initial_submission(pinned)
            == pinned
        )


@pytest.mark.asyncio
async def test_retry_identity_can_reuse_deployment_generation_but_not_sequence(
    capacity_guard_database: dict[str, object],
) -> None:
    registration, trial_id, submission = await _initialize(capacity_guard_database)
    async with _agent_session(capacity_guard_database) as session:
        await CapacityTrialSubmissionStore(
            session, registration=registration
        ).register_initial_submission(submission)

    retry_attempt_id = uuid4()
    async with _owner_session(capacity_guard_database) as session:
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.attempt_lifecycle_events "
                "(transition_id, protected_attempt_id, execution_generation, "
                "requirements_digest, transition_sequence, operation, previous_state, "
                "lifecycle_state, executable, payload, payload_digest) VALUES "
                "(:transition_id, :attempt_id, :generation, :digest, 1, 'cancel', "
                "'pending-unassigned', 'cancelled-terminal', false, "
                "jsonb_build_object('schema_version', 1, 'operation', 'cancel'), "
                ":payload_digest)"
            ),
            {
                "transition_id": uuid4(),
                "attempt_id": submission.protected_attempt_id,
                "generation": registration.deployment_generation,
                "digest": submission.requirements_digest,
                "payload_digest": "c" * 64,
            },
        )
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, "
                "attempt_sequence, requirements_digest, claim_state) VALUES "
                "(:attempt_id, :trial_id, :generation, 1, :digest, 'queued')"
            ),
            {
                "attempt_id": retry_attempt_id,
                "trial_id": trial_id,
                "generation": registration.deployment_generation,
                "digest": submission.requirements_digest,
            },
        )

    async with _owner_session(capacity_guard_database) as session:
        attempts = (
            (
                await session.execute(
                    text(
                        "SELECT protected_attempt_id, execution_generation, attempt_sequence "
                        "FROM loom_capacity_guard.trial_attempts WHERE trial_id = :trial_id "
                        "ORDER BY attempt_sequence"
                    ),
                    {"trial_id": trial_id},
                )
            )
            .mappings()
            .all()
        )
    assert [dict(row) for row in attempts] == [
        {
            "protected_attempt_id": submission.protected_attempt_id,
            "execution_generation": registration.deployment_generation,
            "attempt_sequence": 0,
        },
        {
            "protected_attempt_id": retry_attempt_id,
            "execution_generation": registration.deployment_generation,
            "attempt_sequence": 1,
        },
    ]

    with pytest.raises(IntegrityError):
        async with _owner_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "INSERT INTO loom_capacity_guard.trial_attempts "
                    "(protected_attempt_id, trial_id, execution_generation, "
                    "attempt_sequence, requirements_digest, claim_state) VALUES "
                    "(:attempt_id, :trial_id, :generation, 1, :digest, 'queued')"
                ),
                {
                    "attempt_id": uuid4(),
                    "trial_id": trial_id,
                    "generation": registration.deployment_generation,
                    "digest": submission.requirements_digest,
                },
            )
