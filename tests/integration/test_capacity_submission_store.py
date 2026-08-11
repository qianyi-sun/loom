"""Trusted, zero-executable initial trial registration boundary."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_capacity_agent.contracts import AgentRegistrationV1, InertTrialSubmissionV1
from loom_capacity_agent.store import CapacityAgentStore
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
