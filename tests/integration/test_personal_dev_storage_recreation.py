"""Upgrading a legacy name must preserve its exact retired cleanup evidence."""

import asyncio
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


@pytest.mark.asyncio
async def test_legacy_deletion_serializes_with_incarnation_opt_in(isolated_migration_postgres_url, monkeypatch):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    prepared, deletion_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    backend_pids = {}
    tasks = []
    try:
        request, access, retired = await _retired_legacy(sessions)

        async def recreate():
            async with sessions() as session:
                backend_pids["recreate"] = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                commit = session.commit

                async def held_commit():
                    prepared.set()
                    await release.wait()
                    await commit()

                monkeypatch.setattr(session, "commit", held_commit)
                return await SqlAlchemyPersonalDevEnvironmentAuthority(session, storage_layout="incarnation-v1").apply(
                    request, access_binding=access, now=_NOW,
                )

        async def delete_retired():
            async with sessions() as session:
                backend_pids["delete"] = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                await session.execute(text("DELETE FROM dev_lifecycle_operation_attempts WHERE operation_id = :id"),
                                      {"id": retired.operation.id})
                deletion_started.set()
                await session.execute(text("DELETE FROM dev_lifecycle_operations WHERE id = :id"),
                                      {"id": retired.operation.id})
                await session.commit()

        creating = asyncio.create_task(recreate())
        tasks.append(creating)
        await asyncio.wait_for(prepared.wait(), timeout=10)
        deleting = asyncio.create_task(delete_retired())
        tasks.append(deleting)
        await asyncio.wait_for(deletion_started.wait(), timeout=10)
        # Observe the actual database lock, not a guessed scheduling delay.
        async with asyncio.timeout(10), engine.connect() as observer:
            while True:
                if deleting.done():
                    await deleting
                    pytest.fail("legacy deletion escaped the concurrent storage opt-in lock")
                blocked = (await observer.execute(text(
                    "SELECT :recreate = ANY(pg_blocking_pids(:delete))"
                ), backend_pids)).scalar_one()
                if blocked:
                    break
                await asyncio.sleep(0.02)
        release.set()
        assert (await asyncio.wait_for(creating, timeout=10)).operation.storage_binding is not None
        with pytest.raises(DBAPIError, match="storage"):
            await asyncio.wait_for(deleting, timeout=10)
        async with sessions() as session:
            assert await session.get(DevLifecycleOperation, retired.operation.id) is not None
    finally:
        release.set()
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=15)
        await engine.dispose()
