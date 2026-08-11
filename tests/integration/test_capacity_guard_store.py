"""Replay and privilege tests for the disabled protected-admission store."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_capacity_guard.contracts import (
    GuardFenceV1,
    ProtectedAttemptV1,
    SealedRequirementsV1,
    canonical_digest,
)
from loom_capacity_guard.store import (
    CapacityGuardStore,
    GuardDataIntegrityError,
    GuardNotInitializedError,
    GuardOwnerSessionError,
    GuardReplayConflictError,
)


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


def _seed_trial(database: dict[str, object]) -> UUID:
    engine = create_engine(_value(database, "admin_url"))
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"guard-store-task-{uuid4().hex}"
    try:
        with engine.begin() as connection:
            connection.execute(Team.__table__.insert().values(id=team_id, name=f"guard-{team_id}"))
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
                )
            )
    finally:
        engine.dispose()
    return trial_id


def _fence(**changes: Any) -> GuardFenceV1:
    values: dict[str, Any] = {
        "environment_id": "dev-alice",
        "subject_id": uuid4(),
        "subject_incarnation": uuid4(),
        "authority_incarnation": uuid4(),
        "reporter_incarnation": uuid4(),
        "deployment_generation": 1,
        "configuration_generation": 1,
        "candidate_digest": "a" * 64,
    }
    values.update(changes)
    return GuardFenceV1.model_validate(values)


def _requirements(**changes: Any) -> SealedRequirementsV1:
    values: dict[str, Any] = {
        "os": "linux",
        "cpu_arch": "x86_64",
        "gpu_vendor": "none",
        "network_policies": ("public",),
        "required_pool": None,
    }
    values.update(changes)
    return SealedRequirementsV1.model_validate(values)


def _attempt(
    trial_id: UUID,
    requirements: SealedRequirementsV1,
    **changes: Any,
) -> ProtectedAttemptV1:
    values: dict[str, Any] = {
        "trial_id": trial_id,
        "protected_attempt_id": uuid4(),
        "execution_generation": 1,
        "requirements_digest": canonical_digest(requirements),
    }
    values.update(changes)
    return ProtectedAttemptV1.model_validate(values)


@asynccontextmanager
async def _store_session(
    database: dict[str, object],
    *,
    isolation_level: str = "SERIALIZABLE",
    set_owner_role: bool = True,
    expected_owner_role: str | None = None,
) -> AsyncIterator[tuple[CapacityGuardStore, AsyncSession]]:
    url = make_url(_value(database, "migrator_url"))
    engine = create_async_engine(url, isolation_level=isolation_level)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    owner_role = _value(database, "owner_role")
    quoted_owner = engine.sync_engine.dialect.identifier_preparer.quote(owner_role)
    try:
        async with factory() as session, session.begin():
            if set_owner_role:
                await session.execute(text(f"SET LOCAL ROLE {quoted_owner}"))
            yield (
                CapacityGuardStore(
                    session,
                    expected_owner_role=expected_owner_role or owner_role,
                ),
                session,
            )
    finally:
        await engine.dispose()


async def _initialize(database: dict[str, object], fence: GuardFenceV1) -> GuardFenceV1:
    async with _store_session(database) as (store, _):
        return await store.initialize_disabled_authority(fence)


@pytest.mark.asyncio
async def test_store_requires_exact_nonlogin_owner_and_serializable_session(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    with pytest.raises(GuardOwnerSessionError, match="current role"):
        async with _store_session(
            capacity_guard_database,
            set_owner_role=False,
        ) as (store, _):
            await store.initialize_disabled_authority(fence)

    with pytest.raises(GuardOwnerSessionError, match="non-login"):
        async with _store_session(
            capacity_guard_database,
            set_owner_role=False,
            expected_owner_role=_value(capacity_guard_database, "migrator_role"),
        ) as (store, _):
            await store.initialize_disabled_authority(fence)

    with pytest.raises(GuardOwnerSessionError, match="expected owner role"):
        async with _store_session(
            capacity_guard_database,
            expected_owner_role=f"different_owner_{uuid4().hex[:8]}",
        ) as (store, _):
            await store.initialize_disabled_authority(fence)

    with pytest.raises(GuardOwnerSessionError, match="SERIALIZABLE"):
        async with _store_session(
            capacity_guard_database,
            isolation_level="READ COMMITTED",
        ) as (store, _):
            await store.initialize_disabled_authority(fence)


@pytest.mark.asyncio
async def test_authority_initialization_is_exact_replay_and_audited_once(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()

    assert await _initialize(capacity_guard_database, fence) == fence
    assert await _initialize(capacity_guard_database, fence) == fence
    async with _store_session(capacity_guard_database) as (store, session):
        assert await store.read_guard_fence() == fence
        audit = (
            (
                await session.execute(
                    text(
                        "SELECT event_type, trial_id, protected_attempt_id, payload, payload_digest "
                        "FROM loom_capacity_guard.audit_events ORDER BY event_id"
                    )
                )
            )
            .mappings()
            .all()
        )
    assert len(audit) == 1
    assert audit[0]["event_type"] == "authority_initialized.v1"
    assert audit[0]["trial_id"] is None
    assert audit[0]["protected_attempt_id"] is None
    assert audit[0]["payload"] == fence.model_dump(mode="json", exclude_none=False)
    encoded = json.dumps(
        audit[0]["payload"], sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    assert audit[0]["payload_digest"] == hashlib.sha256(encoded).hexdigest()

    conflicting = fence.model_copy(update={"configuration_generation": 2})
    with pytest.raises(GuardReplayConflictError, match="authority"):
        await _initialize(capacity_guard_database, conflicting)


@pytest.mark.asyncio
async def test_uninitialized_reads_and_registration_fail_closed(
    capacity_guard_database: dict[str, object],
) -> None:
    trial_id = _seed_trial(capacity_guard_database)
    requirements = _requirements()
    attempt = _attempt(trial_id, requirements)
    async with _store_session(capacity_guard_database) as (store, _):
        with pytest.raises(GuardNotInitializedError):
            await store.read_guard_fence()
        with pytest.raises(GuardNotInitializedError):
            await store.register_trial_attempt(attempt, requirements)
        with pytest.raises(GuardNotInitializedError):
            await store.read_protected_attempt(attempt.protected_attempt_id)


@pytest.mark.asyncio
async def test_trial_registration_is_exact_replay_and_audited_once(
    capacity_guard_database: dict[str, object],
) -> None:
    fence = _fence()
    trial_id = _seed_trial(capacity_guard_database)
    requirements = _requirements(required_pool="oldlab")
    attempt = _attempt(trial_id, requirements)
    await _initialize(capacity_guard_database, fence)

    for _ in range(2):
        async with _store_session(capacity_guard_database) as (store, _):
            assert await store.register_trial_attempt(attempt, requirements) == attempt

    async with _store_session(capacity_guard_database) as (store, session):
        assert await store.read_protected_attempt(attempt.protected_attempt_id) == attempt
        assert await store.read_protected_attempt(uuid4()) is None
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_requirements) AS requirements, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts) AS attempts, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        " WHERE event_type = 'trial_registered.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
        audit = (
            (
                await session.execute(
                    text(
                        "SELECT trial_id, protected_attempt_id, payload, payload_digest "
                        "FROM loom_capacity_guard.audit_events "
                        "WHERE event_type = 'trial_registered.v1'"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(rows) == {"requirements": 1, "attempts": 1, "audits": 1}
    assert audit["trial_id"] == trial_id
    assert audit["protected_attempt_id"] == attempt.protected_attempt_id
    assert audit["payload"] == attempt.model_dump(mode="json", exclude_none=False)
    encoded = json.dumps(
        audit["payload"], sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    assert audit["payload_digest"] == hashlib.sha256(encoded).hexdigest()


@pytest.mark.asyncio
async def test_registration_identity_conflicts_leave_no_partial_rows(
    capacity_guard_database: dict[str, object],
) -> None:
    trial_id = _seed_trial(capacity_guard_database)
    other_trial_id = _seed_trial(capacity_guard_database)
    requirements = _requirements()
    attempt = _attempt(trial_id, requirements)
    await _initialize(capacity_guard_database, _fence())
    async with _store_session(capacity_guard_database) as (store, _):
        await store.register_trial_attempt(attempt, requirements)

    different_requirements = _requirements(cpu_arch="arm64")
    conflicts = (
        (
            attempt.model_copy(
                update={"requirements_digest": canonical_digest(different_requirements)}
            ),
            different_requirements,
        ),
        (
            attempt.model_copy(update={"requirements_digest": "b" * 64}),
            requirements,
        ),
        (
            attempt.model_copy(update={"protected_attempt_id": uuid4()}),
            requirements,
        ),
        (
            attempt.model_copy(update={"execution_generation": 2}),
            requirements,
        ),
    )
    for conflicting_attempt, conflicting_requirements in conflicts:
        with pytest.raises(GuardReplayConflictError):
            async with _store_session(capacity_guard_database) as (store, _):
                await store.register_trial_attempt(
                    conflicting_attempt,
                    conflicting_requirements,
                )

    conflicting_trial_attempt = attempt.model_copy(
        update={
            "trial_id": other_trial_id,
            "execution_generation": 1,
        }
    )
    async with _store_session(capacity_guard_database) as (store, session):
        with pytest.raises(GuardReplayConflictError):
            await store.register_trial_attempt(conflicting_trial_attempt, requirements)
        partial_count = (
            await session.execute(
                text(
                    "SELECT count(*) FROM loom_capacity_guard.trial_requirements "
                    "WHERE trial_id = :trial_id"
                ),
                {"trial_id": other_trial_id},
            )
        ).scalar_one()
        assert partial_count == 0

    async with _store_session(capacity_guard_database) as (_, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_requirements) AS requirements, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts) AS attempts, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        " WHERE event_type = 'trial_registered.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"requirements": 1, "attempts": 1, "audits": 1}


@pytest.mark.asyncio
async def test_missing_public_trial_rolls_back_requirement_attempt_and_audit(
    capacity_guard_database: dict[str, object],
) -> None:
    await _initialize(capacity_guard_database, _fence())
    requirements = _requirements()
    attempt = _attempt(uuid4(), requirements)

    with pytest.raises(IntegrityError):
        async with _store_session(capacity_guard_database) as (store, _):
            await store.register_trial_attempt(attempt, requirements)

    async with _store_session(capacity_guard_database) as (_, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_requirements) AS requirements, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts) AS attempts, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        " WHERE event_type = 'trial_registered.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"requirements": 0, "attempts": 0, "audits": 0}


@pytest.mark.asyncio
async def test_concurrent_exact_registration_converges_after_serialization_retry(
    capacity_guard_database: dict[str, object],
) -> None:
    trial_id = _seed_trial(capacity_guard_database)
    requirements = _requirements()
    attempt = _attempt(trial_id, requirements)
    await _initialize(capacity_guard_database, _fence())

    async def register_with_retry() -> ProtectedAttemptV1:
        for retry in range(3):
            try:
                async with _store_session(capacity_guard_database) as (store, _):
                    return await store.register_trial_attempt(attempt, requirements)
            except DBAPIError as exc:
                sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
                if sqlstate != "40001" or retry == 2:
                    raise
        raise AssertionError("unreachable")

    import asyncio

    results = await asyncio.gather(register_with_retry(), register_with_retry())
    assert results == [attempt, attempt]
    async with _store_session(capacity_guard_database) as (_, session):
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_requirements) AS requirements, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts) AS attempts, "
                        "(SELECT count(*) FROM loom_capacity_guard.audit_events "
                        " WHERE event_type = 'trial_registered.v1') AS audits"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"requirements": 1, "attempts": 1, "audits": 1}


@pytest.mark.asyncio
async def test_store_detects_requirement_digest_corruption(
    capacity_guard_database: dict[str, object],
) -> None:
    trial_id = _seed_trial(capacity_guard_database)
    await _initialize(capacity_guard_database, _fence())
    requirements = _requirements()
    corrupted = deepcopy(requirements.model_dump(mode="json", exclude_none=False))
    corrupted["cpu_arch"] = "arm64"
    async with _store_session(capacity_guard_database) as (_, session):
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_requirements "
                "(trial_id, schema_version, requirements_digest, requirements) "
                "VALUES (:trial_id, 1, :digest, :requirements)"
            ),
            {
                "trial_id": trial_id,
                "digest": canonical_digest(requirements),
                "requirements": json.dumps(corrupted),
            },
        )

    attempt = _attempt(trial_id, requirements)
    with pytest.raises(GuardDataIntegrityError, match="digest"):
        async with _store_session(capacity_guard_database) as (store, _):
            await store.register_trial_attempt(attempt, requirements)


@pytest.mark.asyncio
async def test_read_attempt_requires_its_exact_audit_binding(
    capacity_guard_database: dict[str, object],
) -> None:
    trial_id = _seed_trial(capacity_guard_database)
    await _initialize(capacity_guard_database, _fence())
    requirements = _requirements()
    attempt = _attempt(trial_id, requirements)
    async with _store_session(capacity_guard_database) as (_, session):
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_requirements "
                "(trial_id, schema_version, requirements_digest, requirements) "
                "VALUES (:trial_id, 1, :digest, CAST(:requirements AS jsonb))"
            ),
            {
                "trial_id": trial_id,
                "digest": canonical_digest(requirements),
                "requirements": json.dumps(
                    requirements.model_dump(mode="json", exclude_none=False)
                ),
            },
        )
        await session.execute(
            text(
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, "
                "requirements_digest, claim_state) "
                "VALUES (:protected_attempt_id, :trial_id, :execution_generation, "
                ":requirements_digest, :claim_state)"
            ),
            attempt.model_dump(mode="python", exclude_none=False),
        )

    with pytest.raises(GuardDataIntegrityError, match="exactly one trial_registered"):
        async with _store_session(capacity_guard_database) as (store, _):
            await store.read_protected_attempt(attempt.protected_attempt_id)

    audit_payload = attempt.model_dump(mode="json", exclude_none=False)
    audit_parameters = {
        "trial_id": trial_id,
        "protected_attempt_id": attempt.protected_attempt_id,
        "payload": json.dumps(audit_payload),
        "payload_digest": canonical_digest(attempt),
    }
    insert_audit = text(
        "INSERT INTO loom_capacity_guard.audit_events "
        "(event_type, trial_id, protected_attempt_id, payload, payload_digest) "
        "VALUES ('trial_registered.v1', :trial_id, :protected_attempt_id, "
        "CAST(:payload AS jsonb), :payload_digest)"
    )
    async with _store_session(capacity_guard_database) as (_, session):
        await session.execute(insert_audit, audit_parameters)
    async with _store_session(capacity_guard_database) as (store, _):
        assert await store.read_protected_attempt(attempt.protected_attempt_id) == attempt

    async with _store_session(capacity_guard_database) as (_, session):
        await session.execute(insert_audit, audit_parameters)
    with pytest.raises(GuardDataIntegrityError, match="exactly one trial_registered"):
        async with _store_session(capacity_guard_database) as (store, _):
            await store.read_protected_attempt(attempt.protected_attempt_id)
