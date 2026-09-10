"""Compose real PostgreSQL admission with pinned TLS MinIO source recovery."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom.data_lifecycle_gc_s3 import S3ExactObjectDeleter
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_bundle_source_journal import (
    publish_task_bundle_source,
    release_task_bundle_reference,
    retire_task_bundle_source,
)
from loom.task_bundle_source_publisher import TaskBundleSourcePublisher, TaskBundleSourceRecovery
from loom.task_bundle_source_storage import (
    S3TaskBundleVersionInventory,
    write_task_bundle_source_object,
)
from tests.integration.test_task_bundle_source_journal import NOW
from tests.integration.test_task_bundle_source_journal import journal as journal
from tests.integration.test_task_bundle_source_storage import _store
from tests.integration.test_task_image_bundle_minio_signing import minio_tls as minio_tls
from tests.unit.test_task_bundle_registration import _bundle

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def _prepare(tmp_path, minio_tls, *, versioning=True):
    bucket = "journal-source-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    if versioning:
        admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    task_dir = _bundle(tmp_path)
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(task_dir, task_id="benchmark/" + uuid4().hex),
        bucket=bucket,
    )
    return task_dir, spec, _store(minio_tls)


async def _publish(journal, ticket, owner="catalog"):
    async with journal.begin() as session:
        return await publish_task_bundle_source(
            session,
            incarnation_id=ticket.incarnation_id,
            reference_kind="catalog",
            owner_id=owner,
            now=NOW,
        )


@pytest.mark.parametrize("factory_isolation", ["READ COMMITTED", "SERIALIZABLE", "AUTOCOMMIT"])
async def test_publisher_commits_intent_before_put_and_attaches_only_with_catalog_transaction(
    journal,
    tmp_path,
    minio_tls,
    monkeypatch,
    factory_isolation,
):
    task_dir, spec, store = _prepare(tmp_path, minio_tls)
    original = store.put_object_with_metadata
    observed = []

    async def checked_put(**kwargs):
        intent_id = UUID(kwargs["metadata"]["loom-source-write-id"])
        async with journal.begin() as session:
            # A separate committed view sees the issuance, and can acquire the
            # source lock: no source transaction is held through this PUT.
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            assert (
                await session.scalar(
                    text("SELECT id FROM task_bundle_sources WHERE id=:id FOR UPDATE"),
                    {"id": spec.id},
                )
                == spec.id
            )
            assert (
                await session.scalar(
                    text("SELECT issued_at FROM task_bundle_source_writes WHERE id=:id"),
                    {"id": intent_id},
                )
                == NOW
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM task_bundle_source_references WHERE source_id=:id"),
                    {"id": spec.id},
                )
                == 0
            )
        observed.append(intent_id)
        return await original(**kwargs)

    monkeypatch.setattr(store, "put_object_with_metadata", checked_put)
    writer_sessions = async_sessionmaker(
        journal.kw["bind"].execution_options(isolation_level=factory_isolation),
        expire_on_commit=False,
    )
    publisher = TaskBundleSourcePublisher(writer_sessions, store, clock=lambda: NOW)
    ticket = await publisher.prepare(spec, task_dir)
    assert observed == [intent.id for intent in ticket.intents]
    async with journal() as session:
        await publish_task_bundle_source(
            session,
            incarnation_id=ticket.incarnation_id,
            reference_kind="catalog",
            owner_id="rolled-back",
            now=NOW,
        )
        await session.rollback()
    assert await _publish(journal, ticket) == spec
    # An available ticket is reuse, not another PUT or an implicit catalog pin.
    replay = await publisher.prepare(spec, task_dir)
    assert replay.available and replay.incarnation_id == ticket.incarnation_id
    assert len(observed) == len(ticket.intents)
    async with journal.begin() as session:
        await release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog"
        )
        assert await retire_task_bundle_source(
            session, incarnation_id=ticket.incarnation_id, now=NOW
        )
    with pytest.raises(ValueError, match="retiring"):
        await _publish(journal, replay)
    replacement = await publisher.prepare(spec, task_dir)
    assert replacement.incarnation_id != ticket.incarnation_id
    assert await _publish(journal, replacement) == spec


async def test_lost_receipt_recovers_after_restart_and_delayed_old_cleanup_preserves_republication(
    journal,
    tmp_path,
    minio_tls,
    monkeypatch,
):
    task_dir, spec, store = _prepare(tmp_path, minio_tls)
    original = store.put_object_with_metadata
    upload_id, lost = uuid4(), []

    async def lost_response(**kwargs):
        lost.append(await original(**kwargs))
        raise TimeoutError("storage accepted PUT but caller lost its receipt")

    monkeypatch.setattr(store, "put_object_with_metadata", lost_response)
    with pytest.raises(TimeoutError, match="lost its receipt"):
        await TaskBundleSourcePublisher(journal, store, clock=lambda: NOW).prepare(
            spec, task_dir, upload_id=upload_id
        )
    async with journal.begin() as session:
        intent_id = await session.scalar(
            text(
                "SELECT id FROM task_bundle_source_writes WHERE incarnation_id=:id AND issued_at IS NOT NULL"
            ),
            {"id": upload_id},
        )
        assert (
            await session.scalar(
                text("SELECT count(*) FROM task_bundle_source_versions WHERE write_id=:id"),
                {"id": intent_id},
            )
            == 0
        )
        assert await retire_task_bundle_source(
            session, incarnation_id=upload_id, now=NOW + timedelta(hours=2)
        )
    admin = minio_tls[3]
    reader = S3TaskBundleVersionInventory(admin, max_versions=1)
    deleter = S3ExactObjectDeleter(admin)
    recovery = TaskBundleSourceRecovery(
        journal, reader, deleter, clock=lambda: NOW + timedelta(hours=2)
    )
    assert await recovery.inventory_batch(intent_id)
    monkeypatch.setattr(store, "put_object_with_metadata", original)
    newer = await TaskBundleSourcePublisher(journal, store, clock=lambda: NOW).prepare(
        spec, task_dir
    )
    await _publish(journal, newer)
    # Crash after the external deletion but before committing absence. Restart
    # finishes that exact claim instead of deleting whatever is currently at key.
    first = True
    original_delete = deleter.delete_exact

    def lose_delete_response(item):
        nonlocal first
        original_delete(item)
        if first:
            first = False
            raise TimeoutError("delete committed without response")

    monkeypatch.setattr(deleter, "delete_exact", lose_delete_response)
    with pytest.raises(TimeoutError, match="delete committed"):
        await recovery.delete_batch(upload_id)
    recovery = TaskBundleSourceRecovery(
        journal, reader, deleter, clock=lambda: NOW + timedelta(hours=2)
    )
    assert await recovery.delete_batch(upload_id) == 1
    # An empty observation must not discard the old intent: model a still-live
    # retry arriving after the old version is gone and the replacement is ready.
    assert await recovery.inventory_batch(intent_id)
    async with journal.begin() as session:
        from loom.task_bundle_source_journal import task_bundle_inventory_checkpoint

        intent, _, _ = await task_bundle_inventory_checkpoint(session, intent_id=intent_id)
    late = await write_task_bundle_source_object(
        store, intent, spec.read_object(task_dir, intent.object_key)
    )
    for _ in range(10):
        recovery = TaskBundleSourceRecovery(
            journal, reader, deleter, clock=lambda: NOW + timedelta(hours=2)
        )
        if await recovery.inventory_batch(intent_id):
            break
    else:
        pytest.fail("restarted bounded inventory never reached its observational end")
    assert late.version_id != lost[0].version_id
    assert await recovery.delete_batch(upload_id) == 1
    for planned in newer.intents:
        recovered = S3TaskBundleVersionInventory(admin).scan_batch(planned)
        assert len(recovered.versions) == 1
        body = admin.get_object(Bucket=planned.bucket, Key=planned.object_key)["Body"]
        try:
            assert body.read() == spec.read_object(task_dir, planned.object_key)
        finally:
            body.close()


async def test_shared_global_manifest_keeps_separately_owned_versions(journal, tmp_path, minio_tls):
    task_dir, first_spec, store = _prepare(tmp_path, minio_tls)
    second_spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(task_dir, task_id="other/" + uuid4().hex),
        bucket=first_spec.bucket,
    )
    publisher = TaskBundleSourcePublisher(journal, store, clock=lambda: NOW)
    first, second = (
        await publisher.prepare(first_spec, task_dir),
        await publisher.prepare(second_spec, task_dir),
    )
    assert first.intents[-1].object_key == second.intents[-1].object_key
    await _publish(journal, first)
    await _publish(journal, second)
    async with journal.begin() as session:
        await release_task_bundle_reference(
            session, source_id=first_spec.id, reference_kind="catalog", owner_id="catalog"
        )
        assert await retire_task_bundle_source(
            session, incarnation_id=first.incarnation_id, now=NOW
        )
    admin = minio_tls[3]
    recovery = TaskBundleSourceRecovery(
        journal, S3TaskBundleVersionInventory(admin), S3ExactObjectDeleter(admin), clock=lambda: NOW
    )
    assert await recovery.delete_batch(first.incarnation_id) == len(first.intents)
    assert await _publish(journal, second, owner="still-readable") == second_spec
    for intent in second.intents:
        assert len(S3TaskBundleVersionInventory(admin).scan_batch(intent).versions) == 1


async def test_unversioned_publication_leaves_only_unreachable_recovery_intents(
    journal, tmp_path, minio_tls
):
    task_dir, spec, store = _prepare(tmp_path, minio_tls, versioning=False)
    with pytest.raises(ValueError, match="versioning"):
        await TaskBundleSourcePublisher(journal, store, clock=lambda: NOW).prepare(spec, task_dir)
    async with journal() as session:
        assert (
            await session.scalar(
                text("SELECT state FROM task_bundle_source_incarnations WHERE source_id=:id"),
                {"id": spec.id},
            )
            == "uploading"
        )
        assert (
            await session.scalar(
                text("SELECT count(*) FROM task_bundle_source_references WHERE source_id=:id"),
                {"id": spec.id},
            )
            == 0
        )
    assert "Contents" not in minio_tls[3].list_objects_v2(Bucket=spec.bucket)
