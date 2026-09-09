from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom.db.schema import Task, TaskImageAttemptRetention, TaskImageMaterialization
from loom.task_image_materialization import ensure_task_image_materializations
from loom_control_plane.routes.trials import _ensure_trial_task_image_links
from loom_task_image_authority.materializations import TaskImageSessionMaterializationConflictError
from tests.integration.test_service_execution_leases import _seed_ready_trial
from tests.integration.test_task_image_publication_completion import _complete, _signed_job
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW, _issue_first
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retirement_snapshot import _setup
from tests.integration.test_task_image_retirement_store import observe, store


async def _blocked(probe, *, waiter, blocker, operation):
    async with asyncio.timeout(3):
        while not await probe.scalar(
            text("SELECT :blocker = ANY(pg_blocking_pids(:waiter))"),
            {"blocker": blocker, "waiter": waiter},
        ):
            if operation.done():
                await operation
                pytest.fail("reference writer did not wait on the retirement fence")
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("writer", ["catalog", "credential", "trial"])
@pytest.mark.parametrize("winner", ["writer", "retirement"])
async def test_actual_admission_winner_orders(
    registry_authority_session,
    registry_issuer,
    monkeypatch,
    writer,
    winner,
):
    factory = registry_authority_session
    module = store()
    if writer == "credential":
        materialization, attempt, options = await _setup(factory, registry_issuer)
        attempt_id = attempt.id
    else:
        async with factory() as session:
            receipt = await _complete(session, await _signed_job(session, registry_issuer))
            materialization = await session.get(
                TaskImageMaterialization, UUID(receipt.materialization_id)
            )
            if writer == "trial":
                # Current catalog revision differs; the submitted Trial retains
                # its earlier immutable task snapshot at the actual link writer.
                trial_id, _ = await _seed_ready_trial(
                    session, now=NOW, task_id=materialization.task_id
                )
            await session.commit()
        attempt_id = UUID(receipt.attempt_id)
    instant = NOW + timedelta(hours=1)
    await observe(factory, attempt_id, instant)
    later = instant + timedelta(days=7)

    async def write(session):
        if writer == "credential":
            return await _issue_first(session, **options)
        task = Task(
            id=materialization.task_id,
            checksum=materialization.task_checksum,
            config=materialization.task_config,
            source=materialization.task_source,
            source_provenance=materialization.task_source_provenance,
        )
        if writer == "trial":
            await _ensure_trial_task_image_links(session, trial_id=trial_id, task_row=task)
            return [
                await session.get(
                    TaskImageMaterialization, materialization.id, populate_existing=True
                )
            ]
        session.add(task)
        await session.flush()
        return await ensure_task_image_materializations(session, task_row=task)

    async with factory() as writer_session, factory() as probe:
        if winner == "writer":
            await write(writer_session)
            with pytest.raises(DBAPIError) as error:
                await observe(factory, attempt_id, later)
            assert error.value.orig.sqlstate == "55P03"
            await writer_session.commit()
            if writer in ("catalog", "trial"):
                pin = "current_task" if writer == "catalog" else "nonterminal_trial"
                assert (await observe(factory, attempt_id, later)).pins == (pin,)
            else:
                assert (await observe(factory, attempt_id, later)).status == "retired"
            return

        original = module._pins
        reached, release = asyncio.Event(), asyncio.Event()
        pids = {}

        async def pause(session, *args, **kwargs):
            result = await original(session, *args, **kwargs)
            pids["retirement"] = await session.scalar(text("SELECT pg_backend_pid()"))
            reached.set()
            await release.wait()
            return result

        monkeypatch.setattr(module, "_pins", pause)
        waiter_pid = await writer_session.scalar(text("SELECT pg_backend_pid()"))
        retirement = asyncio.create_task(observe(factory, attempt_id, later))
        pending = None
        try:
            async with asyncio.timeout(5):
                await reached.wait()
            pending = asyncio.create_task(write(writer_session))
            await _blocked(probe, waiter=waiter_pid, blocker=pids["retirement"], operation=pending)
            release.set()
            assert (await retirement).status == "retired"
            if writer == "credential":
                with pytest.raises(TaskImageSessionMaterializationConflictError, match="retired"):
                    await pending
                await writer_session.rollback()
            else:
                rows = await pending
                rebuilt = next(item for item in rows if item.id == materialization.id)
                assert rebuilt.state == "queued" and rebuilt.registry_images == {}
                assert rebuilt.ready_publication_operation_id is None
                await writer_session.commit()
        finally:
            release.set()
            if not retirement.done():
                retirement.cancel()
            if pending is not None and not pending.done():
                pending.cancel()
            await asyncio.gather(
                retirement, *(() if pending is None else (pending,)), return_exceptions=True
            )
            await writer_session.rollback()
        marker = await probe.get(TaskImageAttemptRetention, attempt_id)
        assert marker.retired_at == later


async def test_completion_between_preparation_and_fence_rejects_stale_observation(
    registry_authority_session,
    registry_issuer,
    monkeypatch,
):
    factory = registry_authority_session
    module = store()
    async with factory() as session:
        values = await _signed_job(session, registry_issuer)
        await session.commit()
    original = module._prepare_publication

    async def complete_after(*args):
        result = await original(*args)
        async with factory() as session:
            await _complete(session, values)
            await session.commit()
        return result

    monkeypatch.setattr(module, "_prepare_publication", complete_after)
    attempt_id = UUID(values[0].snapshot.attempt_id)
    with pytest.raises(module.RetirementInventoryChangedError):
        await observe(factory, attempt_id, NOW + timedelta(days=2))
    async with factory() as session:
        assert await session.get(TaskImageAttemptRetention, attempt_id) is None
        row = await session.get(
            TaskImageMaterialization, UUID(values[0].snapshot.materialization_id)
        )
        assert row.state == "ready" and row.ready_publication_operation_id == UUID(
            values[0].operation_id
        )
