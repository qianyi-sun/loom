"""Reference ensure must refresh retired state and serialize retirement with new x86 admission."""

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import select, text, update

from loom.db.schema import Task, TaskImageMaterialization
from loom.task_image_materialization import ensure_task_image_materializations
from tests.integration.test_task_image_materialization_store import _task_values
from tests.integration.test_task_image_publication_jobs import _blocked
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)


async def _prepared(factory):
    async with factory() as session:
        task = Task(**_task_values(task_id="ensure-fence/task", checksum="1" * 64))
        session.add(task)
        await session.flush()
        rows = await ensure_task_image_materializations(session, task_row=task)
        await session.commit()
        return task, {row.cpu_arch: row.id for row in rows}


async def test_cached_ready_state_cannot_survive_committed_retirement(registry_authority_session):
    task, ids = await _prepared(registry_authority_session)
    async with registry_authority_session() as session:
        await session.execute(
            update(TaskImageMaterialization)
            .where(TaskImageMaterialization.id == ids["x86_64"])
            .values(
                state="ready",
                ready_at=datetime.now(UTC),
                registry_images={"task": "registry.example/loom/task@sha256:" + "a" * 64},
            )
        )
        await session.commit()
    async with registry_authority_session() as referrer, registry_authority_session() as retire:
        cached = await referrer.get(TaskImageMaterialization, ids["x86_64"])
        assert cached.state == "ready" and cached.registry_images
        await retire.execute(
            update(TaskImageMaterialization)
            .where(TaskImageMaterialization.id == cached.id)
            .values(state="retired", registry_images={}, ready_at=None)
        )
        await retire.commit()
        ensured = await ensure_task_image_materializations(referrer, task_row=task)
        assert [row.cpu_arch for row in ensured] == ["x86_64"]
        current = next(row for row in ensured if row.id == cached.id)
        assert (
            current.state == "queued" and current.registry_images == {} and current.ready_at is None
        )
        await referrer.commit()
    async with registry_authority_session() as session:
        assert (
            await session.scalar(
                select(TaskImageMaterialization.state).where(
                    TaskImageMaterialization.id == ids["x86_64"]
                )
            )
            == "queued"
        )


async def test_ensure_waits_for_committed_retirement_before_requeue(
    registry_authority_session,
):
    task, ids = await _prepared(registry_authority_session)
    async with registry_authority_session() as earlier, registry_authority_session() as ensurer:
        retiring = await earlier.scalar(
            select(TaskImageMaterialization)
            .where(TaskImageMaterialization.id == ids["x86_64"])
            .with_for_update()
        )
        pid = await ensurer.scalar(text("SELECT pg_backend_pid()"))
        waiting = asyncio.create_task(ensure_task_image_materializations(ensurer, task_row=task))
        try:
            await _blocked(earlier, pid, waiting)
            retiring.state = "retired"
            await earlier.commit()
            rows = await asyncio.wait_for(waiting, 5)
            assert [row.cpu_arch for row in rows] == ["x86_64"]
            assert rows[0].state == "queued"
            await ensurer.commit()
        finally:
            await earlier.rollback()
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


async def test_suppressed_autoflush_cannot_discard_pending_materialization_changes(
    registry_authority_session,
):
    task, ids = await _prepared(registry_authority_session)
    async with registry_authority_session() as session:
        row = await session.get(TaskImageMaterialization, ids["x86_64"])
        row.failure_message = "pending caller change"
        with session.no_autoflush, pytest.raises(RuntimeError, match="pending materialization"):
            await ensure_task_image_materializations(session, task_row=task)
        assert row.failure_message == "pending caller change" and row in session.dirty


async def test_detached_submitted_revision_is_not_replaced_by_current_task(
    registry_authority_session,
):
    submitted, _ = await _prepared(registry_authority_session)
    async with registry_authority_session() as registrar:
        await registrar.execute(
            update(Task).where(Task.id == submitted.id).values(checksum="2" * 64)
        )
        await registrar.commit()
    async with registry_authority_session() as session:
        rows = await ensure_task_image_materializations(session, task_row=submitted)
        assert {row.task_checksum for row in rows} == {"1" * 64}
        assert (
            await session.scalar(select(Task.checksum).where(Task.id == submitted.id)) == "2" * 64
        )
