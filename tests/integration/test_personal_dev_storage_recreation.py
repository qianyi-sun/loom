"""Upgrading a legacy name must preserve its exact retired cleanup evidence."""

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import DevInstance, DevLifecycleOperation
from loom.personal_dev_environment import PersonalDevEnvironmentDestroyRequest
from loom.personal_dev_environment_store import SqlAlchemyPersonalDevEnvironmentAuthority
from tests.integration.test_personal_dev_incarnation_storage import _candidate
from tests.unit.test_personal_dev_reconciler import _NOW


async def _retired_legacy(sessions):
    request, access = await _candidate(sessions)
    async with sessions() as session:
        authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
        created = await authority.apply(request, access_binding=access, now=_NOW)
        claim = await authority.claim_next_reconciliation(reconciler_id="storage-legacy", now=_NOW, lease_seconds=60)
        await authority.fail_pre_activation(
            operation_id=created.operation.id, operation_epoch=created.operation.operation_epoch,
            attempt_id=claim.attempt.id, reconciler_id="storage-legacy",
            lease_epoch=claim.attempt.lease_epoch, failure_reason="candidate_build_failed", now=_NOW,
        )
        retired = await authority.destroy(PersonalDevEnvironmentDestroyRequest(
            name=request.name, owner_user_id=request.owner_user_id, owner_team_id=request.owner_team_id,
            expected_operation_epoch=created.operation.operation_epoch, idempotency_key=uuid4(), keep_data=False,
        ), access_binding=access, now=_NOW)
    return replace(request, expected_operation_epoch=retired.operation.operation_epoch, idempotency_key=uuid4()), access, retired


@pytest.mark.asyncio
async def test_legacy_history_survives_transition_to_incarnation_storage(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access, retired = await _retired_legacy(sessions)
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1")
            recreated = await authority.apply(request, access_binding=access, now=_NOW)
            assert recreated.operation.storage_binding is not None
            assert (await authority.get_operation(retired.operation.id)).storage_binding is None
            with pytest.raises(DBAPIError, match="storage"):
                await session.execute(text("DELETE FROM dev_lifecycle_operation_attempts WHERE operation_id = :id"),
                                      {"id": retired.operation.id})
                await session.execute(text("DELETE FROM dev_lifecycle_operations WHERE id = :id"),
                                      {"id": retired.operation.id})
                await session.commit()
            await session.rollback()
            assert (await authority.get_operation(retired.operation.id)).storage_binding is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ("subject_id", "keep_data"))
async def test_legacy_upgrade_requires_matching_retirement_evidence(isolated_migration_postgres_url, defect):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access, retired = await _retired_legacy(sessions)
        async with sessions() as session:
            # A historical NULL binding alone does not couple these fields.
            if defect == "subject_id":
                await session.execute(update(DevInstance).where(DevInstance.name == request.name).values(subject_id=uuid4()))
            else:
                await session.execute(update(DevLifecycleOperation).where(
                    DevLifecycleOperation.id == retired.operation.id,
                ).values(keep_data=True))
            await session.commit()
        async with sessions() as session:
            with pytest.raises(DBAPIError, match="storage"):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1").apply(
                    request, access_binding=access, now=_NOW,
                )
            await session.rollback()
    finally:
        await engine.dispose()
