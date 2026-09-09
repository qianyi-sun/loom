"""Management persists storage identity before any external provisioning."""

from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import DevInstance, DevLifecycleOperation, PersonalDevCandidate, Team, User
from loom.personal_dev_environment import PersonalDevEnvironmentApplyRequest
from loom.personal_dev_environment_store import SqlAlchemyPersonalDevEnvironmentAuthority
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_membership_successor import _row_values
from tests.unit.test_personal_dev_reconciler import _NOW, _claim


async def _candidate(sessions):
    claim = _claim(state="activating")
    candidate = claim.candidate
    async with sessions() as session:
        session.add(Team(id=candidate.owner_team_id, name="storage-owner"))
        session.add(User(id=candidate.owner_user_id, email="storage@example.test", username="storage-owner",
                         username_normalized="storage-owner", status="active"))
        await session.flush()
        values = _row_values(PersonalDevCandidate, candidate)
        values.update(manifest_json={"schema_version": 1}, object_key=(
            f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/"
            f"{candidate.candidate_sha}/{candidate.archive_sha256}.tar"
        ))
        session.add(PersonalDevCandidate(**values))
        await session.commit()
    request = PersonalDevEnvironmentApplyRequest(
        name="alice", owner_user_id=candidate.owner_user_id, owner_team_id=candidate.owner_team_id,
        candidate_id=candidate.id, candidate_sha=candidate.candidate_sha,
        min_slots=0, max_slots=1, expected_operation_epoch=0, idempotency_key=uuid4(),
    )
    return request, claim.attempt.access_binding


@pytest.mark.asyncio
async def test_storage_layout_is_reserved_once_and_replay_cannot_downgrade(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            result = await SqlAlchemyPersonalDevEnvironmentAuthority(
                session, storage_layout="incarnation-v1",
            ).apply(request, access_binding=access, now=_NOW)
        binding = result.operation.storage_binding
        assert binding is not None and binding.layout == "incarnation-v1"
        assert binding.subject_incarnation == result.operation.subject_incarnation
        assert binding.owner_user_id == request.owner_user_id
        assert binding.owner_team_id == request.owner_team_id
        assert result.environment.storage_binding == binding
        async with sessions() as session:
            environment = await session.get(DevInstance, request.name)
            operation = await session.get(DevLifecycleOperation, result.operation.id)
            assert environment.capacity_database == binding.identity.database
            assert environment.capacity_namespace == "loom-dev-alice"
            assert environment.storage_binding_sha256 == operation.storage_binding_sha256 == canonical_digest(binding)
            assert environment.storage_binding == operation.storage_binding == binding.model_dump(mode="json")
        async with sessions() as session:
            replay = await SqlAlchemyPersonalDevEnvironmentAuthority(session).apply(
                request, access_binding=access, now=_NOW,
            )
            assert not replay.acquired
            assert replay.operation.storage_binding == binding
            assert replay.operation.id == result.operation.id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("table", ("dev_instances", "dev_lifecycle_operations"))
async def test_same_incarnation_storage_binding_is_immutable(isolated_migration_postgres_url, table):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            result = await SqlAlchemyPersonalDevEnvironmentAuthority(
                session, storage_layout="incarnation-v1",
            ).apply(request, access_binding=access, now=_NOW)
        async with sessions() as session:
            with pytest.raises(DBAPIError):
                await session.execute(text(
                    f"UPDATE {table} SET storage_binding = NULL, storage_binding_sha256 = NULL"
                ))
                await session.commit()
            await session.rollback()
            operation = (await session.scalars(select(DevLifecycleOperation).where(
                DevLifecycleOperation.id == result.operation.id,
            ))).one()
            assert operation.storage_binding == result.operation.storage_binding.model_dump(mode="json")
    finally:
        await engine.dispose()
