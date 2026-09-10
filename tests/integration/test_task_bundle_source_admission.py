"""Materialization admission pins registered sources, not merely digest strings."""

import pytest
from sqlalchemy import select, text
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
