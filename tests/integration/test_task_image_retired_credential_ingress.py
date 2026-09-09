from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import timedelta

import pytest
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db.schema import TaskImageAttemptRetention, TaskImageRegistryCredentialGeneration
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW, _issue_first
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retirement_races import _blocked
from tests.integration.test_task_image_retirement_snapshot import _setup
from tests.integration.test_task_image_retirement_store import observe, store


async def _prepared_insert(factory, issuer):
    _, attempt, options = await _setup(factory, issuer)
    async with factory() as session:
        # Use actual issuer-produced public/audit values, then roll back only
        # this disposable issuance. No credential is committed before retirement.
        await _issue_first(session, **options)
        row = (await session.scalars(select(TaskImageRegistryCredentialGeneration))).one()
        values = deepcopy({name: getattr(row, name) for name in row.__table__.columns.keys()})
        await session.rollback()
    return attempt.id, values


async def _retire(factory, attempt_id):
    await observe(factory, attempt_id, NOW + timedelta(hours=1))
    result = await observe(factory, attempt_id, NOW + timedelta(hours=25))
    assert result.status == "retired"


async def _insert(session, values):
    await session.execute(insert(TaskImageRegistryCredentialGeneration).values(**values))


async def test_retired_attempt_rejects_direct_credential_insert(
    registry_authority_session, registry_issuer,
):
    factory = registry_authority_session
    attempt_id, values = await _prepared_insert(factory, registry_issuer)
    await _retire(factory, attempt_id)
    async with factory() as session:
        with pytest.raises(IntegrityError) as error:
            await _insert(session, values)
        assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_not_retired"
        await session.rollback()
        assert await session.scalar(select(func.count()).select_from(TaskImageRegistryCredentialGeneration)) == 0


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_stale_snapshot_cannot_hide_committed_retirement(
    registry_authority_session, registry_issuer, isolation,
):
    factory = registry_authority_session
    attempt_id, values = await _prepared_insert(factory, registry_issuer)
    await observe(factory, attempt_id, NOW + timedelta(hours=1))
    async with factory() as stale:
        await stale.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
        assert (await stale.get(TaskImageAttemptRetention, attempt_id)).retired_at is None
        assert (await observe(factory, attempt_id, NOW + timedelta(hours=25))).status == "retired"
        with pytest.raises(IntegrityError) as error:
            await _insert(stale, values)
        assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_read_committed"


@pytest.mark.parametrize("commit", [False, True])
async def test_raw_insert_waiting_on_retirement_observes_commit_or_rollback(
    registry_authority_session, registry_issuer, monkeypatch, commit,
):
    factory = registry_authority_session
    module = store()
    attempt_id, values = await _prepared_insert(factory, registry_issuer)
    await observe(factory, attempt_id, NOW + timedelta(hours=1))
    reached, release = asyncio.Event(), asyncio.Event()
    pids = {}
    original = module._pins

    async def pause(session, *args, **kwargs):
        result = await original(session, *args, **kwargs)
        pids["retirement"] = await session.scalar(text("SELECT pg_backend_pid()"))
        reached.set()
        await release.wait()
        return result

    monkeypatch.setattr(module, "_pins", pause)
    async with factory() as raw, factory() as probe:
        raw_pid = await raw.scalar(text("SELECT pg_backend_pid()"))
        retirement = asyncio.create_task(observe(factory, attempt_id, NOW + timedelta(hours=25)))
        insertion = None
        try:
            async with asyncio.timeout(5):
                await reached.wait()
            insertion = asyncio.create_task(_insert(raw, values))
            await _blocked(probe, waiter=raw_pid, blocker=pids["retirement"], operation=insertion)
            if commit:
                release.set()
                assert (await retirement).status == "retired"
                with pytest.raises(IntegrityError) as error:
                    await insertion
                assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_not_retired"
                await raw.rollback()
            else:
                retirement.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await retirement
                await insertion
                await raw.commit()
        finally:
            release.set()
            if not retirement.done():
                retirement.cancel()
            if insertion is not None and not insertion.done():
                insertion.cancel()
            await asyncio.gather(retirement, *(() if insertion is None else (insertion,)), return_exceptions=True)
            await raw.rollback()
        count = await probe.scalar(select(func.count()).select_from(TaskImageRegistryCredentialGeneration))
        assert count == (0 if commit else 1)


async def test_raw_insert_winner_blocks_retirement_and_enters_next_inventory(
    registry_authority_session, registry_issuer,
):
    factory = registry_authority_session
    attempt_id, values = await _prepared_insert(factory, registry_issuer)
    await observe(factory, attempt_id, NOW + timedelta(hours=1))
    async with factory() as raw:
        await _insert(raw, values)
        with pytest.raises(DBAPIError) as error:
            await observe(factory, attempt_id, NOW + timedelta(hours=25))
        assert error.value.orig.sqlstate == "55P03"
        await raw.commit()
    assert (await observe(factory, attempt_id, NOW + timedelta(hours=25))).status == "retired"
    async with factory() as session:
        marker = await session.get(TaskImageAttemptRetention, attempt_id)
        assert b'"credential_count":1' in marker.canonical_inventory
