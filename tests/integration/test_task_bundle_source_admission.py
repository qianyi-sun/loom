"""Materialization admission pins registered sources, not merely digest strings."""

import asyncio
import importlib

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, TaskImageMaterialization
from loom.task_image_materialization import ensure_task_image_materializations
from tests.integration.test_task_bundle_source_journal import (
    NOW,
    _module,
    _publish,
    _receipts,
    _spec,
    _upload,
)


@pytest.fixture
async def journal(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def _task(spec):
    return Task(
        id=spec.catalog_task_id,
        checksum=spec.manifest.task_checksum,
        config=spec.task_config,
        source=spec.source_uri,
        source_provenance=spec.provenance,
    )


@pytest.mark.parametrize("boundary", ["catalog", "ensure", "retry"])
@pytest.mark.parametrize("isolation", ["AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
async def test_strong_admission_rejects_unsafe_transaction_before_any_catalog_writes(
    journal, tmp_path, boundary, isolation,
):
    from loom.task_bundle_catalog import publish_task_bundle_catalog

    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    if boundary == "retry":
        from loom_control_plane.task_image_materializations import (
            TaskImageRetryConflictError,
            retry_task_image_materialization,
        )

        async with journal.begin() as session:
            image = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
            image.state = "failed"
            image_id = image.id
    unsafe = async_sessionmaker(
        journal.kw["bind"].execution_options(isolation_level=isolation), expire_on_commit=False,
    )
    async with unsafe() as session:
        task = _task(spec)
        session.add(task)
        error_type = TaskImageRetryConflictError if boundary == "retry" else ValueError
        with pytest.raises(error_type, match="explicit READ COMMITTED"):
            if boundary == "catalog":
                await publish_task_bundle_catalog(
                    session, tasks=[task], uploads={task.id: ticket}, now=NOW,
                )
            elif boundary == "ensure":
                await ensure_task_image_materializations(session, task_row=task)
            else:
                await retry_task_image_materialization(session, materialization_id=image_id)
        assert task in session.new, "transaction preflight flushed the caller's Task"
        await session.rollback()
    async with journal() as session:
        assert await session.get(Task, spec.catalog_task_id) is None
        if boundary == "retry":
            assert (await session.get(TaskImageMaterialization, image_id)).state == "failed"
        else:
            assert await session.scalar(select(TaskImageMaterialization.id)) is None


async def test_ensure_refuses_unregistered_manifest_source_without_enqueuing(journal, tmp_path):
    spec = _spec(tmp_path)
    async with journal.begin() as session:
        with pytest.raises(ValueError, match=r"registered.*source"):
            await ensure_task_image_materializations(session, task_row=_task(spec))
        await session.rollback()
    async with journal() as session:
        assert (
            await session.scalar(
                select(TaskImageMaterialization.id).where(
                    TaskImageMaterialization.task_id == spec.catalog_task_id
                )
            )
            is None
        )


async def test_ensure_pins_source_and_cannot_revive_it_after_retirement(journal, tmp_path):
    module, spec = _module(), _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        rows = await ensure_task_image_materializations(session, task_row=_task(spec))
        identities = tuple(row.id for row in rows)
        assert identities
    async with journal.begin() as session:
        await module.release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog"
        )
        assert not await module.retire_task_bundle_source(
            session, incarnation_id=ticket.incarnation_id, now=NOW
        )
        assert set(
            (
                await session.execute(
                    text(
                        "SELECT owner_id FROM task_bundle_source_references WHERE source_id=:id AND kind='materialization'"
                    ),
                    {"id": spec.id},
                )
            ).scalars()
        ) == {str(key) for key in identities}
    # Model the later owning materialization retirement transition. It holds
    # image rows before source locks; ordinary ensure must not revive lost input.
    async with journal.begin() as session:
        rows = (
            await session.scalars(
                select(TaskImageMaterialization)
                .where(TaskImageMaterialization.id.in_(identities))
                .order_by(TaskImageMaterialization.id)
                .with_for_update()
            )
        ).all()
        for row in rows:
            row.state = "retired"
            await module.release_task_bundle_reference(
                session, source_id=spec.id, reference_kind="materialization", owner_id=str(row.id)
            )
        assert await module.retire_task_bundle_source(
            session, incarnation_id=ticket.incarnation_id, now=NOW
        )
    async with journal() as session:
        with pytest.raises(ValueError, match="available"):
            await ensure_task_image_materializations(session, task_row=_task(spec))
        await session.rollback()
        assert set(
            await session.scalars(
                select(TaskImageMaterialization.state).where(
                    TaskImageMaterialization.id.in_(identities)
                )
            )
        ) == {"retired"}
    # Identical re-publication has the same logical source and image keys.
    replacement = await _upload(journal, spec)
    await _receipts(journal, replacement)
    await _publish(journal, replacement)
    async with journal.begin() as session:
        revived = await ensure_task_image_materializations(session, task_row=_task(spec))
        assert tuple(row.id for row in revived) == identities
        assert all(row.state == "queued" for row in revived)


@pytest.mark.parametrize(
    "changed", ["config", "checksum", "manifest", "identity", "service-input", "source"]
)
async def test_ensure_requires_exact_registered_snapshot(journal, tmp_path, changed):
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    task = _task(spec)
    if changed == "config":
        task.config = {**task.config, "task": {**task.config["task"], "name": "different"}}
    elif changed == "checksum":
        task.checksum = "e" * 64
    elif changed == "manifest":
        task.source_provenance = {
            **task.source_provenance,
            "bundle_content_manifest_sha256": "e" * 64,
        }
    elif changed == "identity":
        task.source_provenance = {**task.source_provenance, "bundle_task_identity": {}}
    elif changed == "service-input":
        task.source_provenance = {**task.source_provenance, "service_execution_input": {}}
    else:
        task.source = task.source + "other/"
    async with journal() as session:
        with pytest.raises(ValueError, match=r"registered.*source"):
            await ensure_task_image_materializations(session, task_row=task)
        await session.rollback()


@pytest.mark.parametrize("state", ["failed", "ready"])
async def test_admin_retry_requires_available_registered_source_before_resetting_state(
    journal, tmp_path, state
):
    from loom_control_plane.task_image_materializations import (
        TaskImageRetryConflictError,
        retry_task_image_materialization,
    )

    module, spec = _module(), _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        rows = await ensure_task_image_materializations(session, task_row=_task(spec))
        row = rows[0]
        identity = row.id
        row.state, row.attempt_count = state, 3
        await module.release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog"
        )
        await module.release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="materialization", owner_id=str(row.id)
        )
        assert await module.retire_task_bundle_source(
            session, incarnation_id=ticket.incarnation_id, now=NOW
        )
    async with journal() as session:
        with pytest.raises(TaskImageRetryConflictError, match="source"):
            await retry_task_image_materialization(session, materialization_id=identity)
        await session.rollback()
        row = await session.get(TaskImageMaterialization, identity)
        assert row.state == state and row.attempt_count == 3
    replacement = await _upload(journal, spec)
    await _receipts(journal, replacement)
    await _publish(journal, replacement)
    async with journal.begin() as session:
        row = await retry_task_image_materialization(session, materialization_id=identity)
        assert row.state == "queued" and row.attempt_count == 0


@pytest.mark.parametrize("cached_state", ["failed", "ready"])
@pytest.mark.parametrize("current_state", ["claimed", "retiring"])
async def test_admin_retry_refreshes_locked_state_without_regressing_lease(
    journal, tmp_path, cached_state, current_state
):
    from loom_control_plane.task_image_materializations import (
        TaskImageRetryConflictError,
        retry_task_image_materialization,
    )

    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        row = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        row.state = cached_state
        identity = row.id
    async with journal() as cached:
        old = await cached.get(TaskImageMaterialization, identity)
        assert old.state == cached_state and old.lease_epoch == 0
        async with journal.begin() as newer:
            await newer.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == identity)
                .values(state=current_state, lease_epoch=10, claimed_by="current-worker")
            )
        with pytest.raises(TaskImageRetryConflictError, match="state"):
            await retry_task_image_materialization(cached, materialization_id=identity)
        await cached.rollback()
    async with journal() as session:
        row = await session.get(TaskImageMaterialization, identity)
        assert (row.state, row.lease_epoch, row.claimed_by) == (current_state, 10, "current-worker")


async def test_admin_retry_rejects_pending_writes_without_discarding_them(journal, tmp_path):
    from loom_control_plane.task_image_materializations import (
        TaskImageRetryConflictError,
        retry_task_image_materialization,
    )

    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        row = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        row.state = "failed"
        identity = row.id
    async with journal() as session:
        row = await session.get(TaskImageMaterialization, identity)
        row.failure_message = "caller-owned amendment"
        with session.no_autoflush, pytest.raises(TaskImageRetryConflictError, match="pending"):
            await retry_task_image_materialization(session, materialization_id=identity)
        assert row.failure_message == "caller-owned amendment"
        await session.rollback()


async def test_catalog_publication_waits_for_image_before_taking_source_lock(journal, tmp_path):
    from loom.task_image_materialization import admit_task_image_source

    publisher = importlib.import_module("loom.task_bundle_catalog")
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    async with journal.begin() as session:
        task = _task(spec)
        session.add(task)
        first = await publisher.publish_task_bundle_catalog(
            session, tasks=(task,), uploads={task.id: ticket}, now=NOW
        )
        identity = first[task.id][0].id
    owner = journal()
    await owner.begin()
    row = await owner.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == identity)
        .with_for_update()
    )
    begun = asyncio.Event()
    publisher_pid = None

    async def publish():
        nonlocal publisher_pid
        async with journal.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='5s'"))
            publisher_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            task = await session.get(Task, spec.catalog_task_id)
            begun.set()
            return await publisher.publish_task_bundle_catalog(
                session, tasks=(task,), uploads={task.id: ticket}, now=NOW
            )

    pending = asyncio.create_task(publish())
    try:
        await begun.wait()
        async with journal() as observer:
            for _ in range(100):
                blocked = await observer.scalar(
                    text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": publisher_pid}
                )
                if blocked:
                    break
                await asyncio.sleep(0.01)
            assert blocked, "publisher did not wait on the held image row"
        await owner.execute(text("SET LOCAL lock_timeout='1s'"))
        await admit_task_image_source(owner, row=row)
        await owner.commit()
        assert (await pending)[spec.catalog_task_id][0].id == identity
    finally:
        await owner.rollback()
        await owner.close()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


async def test_catalog_replacement_releases_only_catalog_pin_not_historical_images(
    journal, tmp_path
):
    from loom.task_bundle_registration import prepare_task_bundle_registration
    from loom.task_bundle_source import TaskBundleSourceSpecV1

    publisher = importlib.import_module("loom.task_bundle_catalog")
    module, spec = _module(), _spec(tmp_path)
    original = await _upload(journal, spec)
    await _receipts(journal, original)
    async with journal.begin() as session:
        task = _task(spec)
        session.add(task)
        old_images = (
            await publisher.publish_task_bundle_catalog(
                session, tasks=(task,), uploads={task.id: original}, now=NOW
            )
        )[task.id]
    root = tmp_path / "bundle"
    (root / "Dockerfile").chmod(0o755)
    replacement_spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(root, task_id=spec.catalog_task_id), bucket=spec.bucket
    )
    replacement = await _upload(journal, replacement_spec)
    await _receipts(journal, replacement)
    async with journal.begin() as session:
        task = await session.get(Task, spec.catalog_task_id)
        task.source, task.source_provenance = (
            replacement_spec.source_uri,
            replacement_spec.provenance,
        )
        new_images = (
            await publisher.publish_task_bundle_catalog(
                session, tasks=(task,), uploads={task.id: replacement}, now=NOW
            )
        )[task.id]
    assert {row.id for row in old_images}.isdisjoint(row.id for row in new_images)
    async with journal.begin() as session:
        catalog_pins = set(
            await session.scalars(
                text(
                    "SELECT source_id FROM task_bundle_source_references WHERE kind='catalog' AND owner_id=:id"
                ),
                {"id": spec.catalog_task_id},
            )
        )
        assert catalog_pins == {replacement_spec.id}
        assert not await module.retire_task_bundle_source(
            session, incarnation_id=original.incarnation_id, now=NOW
        )
        assert not await module.retire_task_bundle_source(
            session, incarnation_id=replacement.incarnation_id, now=NOW
        )


@pytest.mark.parametrize("commit", [False, True])
async def test_catalog_source_and_image_publication_share_one_transaction(
    journal, tmp_path, commit
):
    publisher = importlib.import_module("loom.task_bundle_catalog")
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    async with journal() as session:
        task = _task(spec)
        session.add(task)
        result = await publisher.publish_task_bundle_catalog(
            session, tasks=(task,), uploads={task.id: ticket}, now=NOW
        )
        assert result[task.id] and all(row.state == "queued" for row in result[task.id])
        if commit:
            await session.commit()
        else:
            await session.rollback()
    async with journal() as session:
        assert (await session.get(Task, spec.catalog_task_id) is not None) is commit
        references = list(
            await session.scalars(
                text("SELECT kind FROM task_bundle_source_references WHERE source_id=:id"),
                {"id": spec.id},
            )
        )
        assert set(references) == ({"catalog", "materialization"} if commit else set())
        assert await session.scalar(
            text("SELECT state FROM task_bundle_source_incarnations WHERE id=:id"),
            {"id": ticket.incarnation_id},
        ) == ("available" if commit else "uploading")
        assert (
            bool(
                await session.scalar(
                    select(TaskImageMaterialization.id).where(
                        TaskImageMaterialization.task_id == spec.catalog_task_id
                    )
                )
            )
            is commit
        )
