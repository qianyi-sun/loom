"""Durable source publication, exact version retirement and reference fencing."""

from __future__ import annotations

import asyncio
import importlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.trajectory.storage import ObjectWriteResult
from tests.unit.test_task_bundle_registration import _bundle

NOW = datetime(2026, 9, 10, tzinfo=UTC)


def _module():
    return importlib.import_module("loom.task_bundle_source_journal")


@pytest.fixture
async def journal(postgres_url):
    engine = create_async_engine(postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def _spec(tmp_path):
    from loom.task_bundle_source import TaskBundleSourceSpecV1

    registration = prepare_task_bundle_registration(_bundle(tmp_path), task_id=f"benchmark/{uuid4()}")
    return TaskBundleSourceSpecV1.from_registration(registration, bucket="task-sources")


async def _upload(factory, spec):
    module = _module()
    async with factory.begin() as session:
        ticket = await module.begin_task_bundle_upload(session, spec=spec, now=NOW,
            upload_id=uuid4(), expires_at=NOW + timedelta(minutes=5))
    return ticket


async def _receipts(factory, ticket):
    module = _module()
    receipts = []
    for intent in ticket.intents:
        async with factory.begin() as session:
            issued = await module.issue_task_bundle_write(session, intent_id=intent.id, now=NOW)
            assert issued == intent
        receipt = ObjectWriteResult(uri=intent.uri, version_id=str(uuid4()))
        async with factory.begin() as session:
            await module.record_task_bundle_write(session, intent_id=intent.id, receipt=receipt, now=NOW)
        receipts.append(receipt)
    return receipts


async def _publish(factory, ticket, owner="catalog"):
    async with factory.begin() as session:
        return await _module().publish_task_bundle_source(session, incarnation_id=ticket.incarnation_id,
            reference_kind="catalog", owner_id=owner, now=NOW)


async def test_publication_requires_complete_intent_receipts_and_atomic_reference(journal, tmp_path):
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    assert len(ticket.intents) == len(spec.manifest.files) + 3
    with pytest.raises(ValueError, match="incomplete"):
        await _publish(journal, ticket)
    async with journal() as session:
        assert await session.scalar(text("SELECT count(*) FROM task_bundle_source_references WHERE source_id=:id"), {"id": spec.id}) == 0
    await _receipts(journal, ticket)
    published = await _publish(journal, ticket)
    assert published == spec
    assert await _publish(journal, ticket) == spec
    async with journal.begin() as session:
        assert not await _module().retire_task_bundle_source(session, incarnation_id=ticket.incarnation_id, now=NOW)


async def test_retired_incarnation_never_revives_and_late_receipt_targets_only_old_versions(journal, tmp_path):
    module, spec = _module(), _spec(tmp_path)
    old = await _upload(journal, spec)
    old_receipts = await _receipts(journal, old)
    await _publish(journal, old)
    async with journal.begin() as session:
        await module.release_task_bundle_reference(session, source_id=spec.id, reference_kind="catalog", owner_id="catalog")
        assert await module.retire_task_bundle_source(session, incarnation_id=old.incarnation_id, now=NOW)
    new = await _upload(journal, spec)
    assert new.incarnation_id != old.incarnation_id
    new_receipts = await _receipts(journal, new)
    await _publish(journal, new)
    late = ObjectWriteResult(uri=old.intents[0].uri, version_id="late-old-version")
    async with journal.begin() as session:
        await module.record_task_bundle_write(session, intent_id=old.intents[0].id, receipt=late, now=NOW)
        deletions = await module.claim_task_bundle_version_deletions(session, incarnation_id=old.incarnation_id, now=NOW)
    assert {item.version_id for item in deletions} == {item.version_id for item in old_receipts} | {late.version_id}
    assert not {item.version_id for item in deletions} & {item.version_id for item in new_receipts}
    with pytest.raises(ValueError, match="retir"):
        await _publish(journal, old)
    async with journal.begin() as session:
        await module.attach_task_bundle_reference(session, source_id=spec.id, reference_kind="trial", owner_id="historical")
        await module.release_task_bundle_reference(session, source_id=spec.id, reference_kind="catalog", owner_id="catalog")
        assert not await module.retire_task_bundle_source(session, incarnation_id=new.incarnation_id, now=NOW)


async def test_reference_attach_and_retirement_share_one_database_lock(journal, tmp_path):
    module, spec = _module(), _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        await module.release_task_bundle_reference(session, source_id=spec.id, reference_kind="catalog", owner_id="catalog")
    retirement = journal()
    await retirement.begin()
    assert await module.retire_task_bundle_source(retirement, incarnation_id=ticket.incarnation_id, now=NOW)
    attached = asyncio.Event()

    async def attach():
        async with journal.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='5s'"))
            attached.set()
            await module.attach_task_bundle_reference(session, source_id=spec.id, reference_kind="trial", owner_id="late")

    task = asyncio.create_task(attach())
    try:
        await attached.wait()
        # Read the actual PostgreSQL lock wait; don't infer contention from sleep.
        async with journal() as session:
            for _ in range(100):
                blocked = await session.scalar(text("SELECT count(*) FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0"))
                if blocked:
                    break
                await asyncio.sleep(0.01)
            assert blocked
        await retirement.commit()
        with pytest.raises(ValueError, match="available"):
            await task
    finally:
        await retirement.rollback()
        await retirement.close()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("version", [None, "null"])
async def test_journal_rejects_nonimmutable_receipts(journal, tmp_path, version):
    module, spec = _module(), _spec(tmp_path)
    ticket = await _upload(journal, spec)
    intent = ticket.intents[0]
    async with journal.begin() as session:
        await module.issue_task_bundle_write(session, intent_id=intent.id, now=NOW)
    async with journal.begin() as session:
        with pytest.raises(ValueError, match="version"):
            await module.record_task_bundle_write(session, intent_id=intent.id,
                receipt=ObjectWriteResult(uri=intent.uri, version_id=version), now=NOW)


async def test_expired_partial_upload_can_retire_and_cannot_publish_or_issue_writes(journal, tmp_path):
    module, spec = _module(), _spec(tmp_path)
    ticket = await _upload(journal, spec)
    expired = NOW + timedelta(minutes=6)
    async with journal.begin() as session:
        assert not await module.retire_task_bundle_source(session, incarnation_id=ticket.incarnation_id, now=NOW)
        assert await module.retire_task_bundle_source(session, incarnation_id=ticket.incarnation_id, now=expired)
    with pytest.raises(ValueError):
        async with journal.begin() as session:
            await module.issue_task_bundle_write(session, intent_id=ticket.intents[0].id, now=expired)
    with pytest.raises(ValueError):
        await _publish(journal, ticket)
