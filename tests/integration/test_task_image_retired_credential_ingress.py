from __future__ import annotations

import asyncio
import sys
from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

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
    # Retirement is setup for the direct-ingress assertion, not the behavior
    # under test. A loaded runner can let PostgreSQL abort the bounded setup
    # transaction. Retry that exact known rollback once in a fresh transaction;
    # never relax the production timeout or retry ambiguous/other failures.
    for instant in (NOW + timedelta(hours=1), NOW + timedelta(hours=25)):
        for retry in range(2):
            try:
                result = await observe(factory, attempt_id, instant)
                break
            except DBAPIError as error:
                if retry or getattr(error.orig, "sqlstate", None) != "25P03":
                    raise
    assert result.status == "retired"


async def _insert(session, values):
    await session.execute(insert(TaskImageRegistryCredentialGeneration).values(**values))


@pytest.mark.parametrize("sqlstate,calls_expected", (("25P03", 2), ("55P03", 1), ("08006", 1), ("23505", 1)))
async def test_retirement_setup_retry_is_bounded_and_only_for_server_abort(monkeypatch, sqlstate, calls_expected):
    class ServerError(Exception):
        pass

    original_error = ServerError()
    original_error.sqlstate = sqlstate
    error = DBAPIError("fixture", None, original_error)
    calls = []

    async def fail(*args, **kwargs):
        calls.append(1)
        raise error

    monkeypatch.setattr(sys.modules[__name__], "observe", fail)
    with pytest.raises(DBAPIError) as raised:
        await _retire(None, uuid4())
    assert raised.value is error
    assert len(calls) == calls_expected


@pytest.mark.parametrize("expire_setup", (False, True))
async def test_retired_attempt_rejects_direct_credential_insert(
    registry_authority_session, registry_issuer, monkeypatch, expire_setup,
):
    factory = registry_authority_session
    attempt_id, values = await _prepared_insert(factory, registry_issuer)
    module = store()
    original = module.revalidate_retirement_inventory
    expired_backends = []

    async def expire_first_setup_transaction(session, *, prepared):
        if expire_setup and not expired_backends:
            backend = await session.scalar(text("SELECT pg_backend_pid()"))
            assert await session.scalar(text("SHOW idle_in_transaction_session_timeout")) == "1s"
            async with factory.kw["bind"].connect() as probe:
                await probe.execution_options(isolation_level="AUTOCOMMIT")
                async with asyncio.timeout(3):
                    while await probe.scalar(
                        text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = :pid)"),
                        {"pid": backend},
                    ):
                        await asyncio.sleep(0.02)
            expired_backends.append(backend)
        await original(session, prepared=prepared)

    monkeypatch.setattr(module, "revalidate_retirement_inventory", expire_first_setup_transaction)
    await _retire(factory, attempt_id)
    assert len(expired_backends) == int(expire_setup)
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
