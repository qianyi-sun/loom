from __future__ import annotations

import pytest
from sqlalchemy import select

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from loom_control_plane.task_image_materializations import (
    TaskImageCompletionError,
    claim_task_image_materialization,
    claim_task_image_registry_gc,
    complete_task_image_materialization,
    record_task_image_publication,
    retry_task_image_materialization,
    start_task_image_materialization,
)
from tests.integration.test_task_image_publication_completion import _complete, _signed_job
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


async def test_gc_refreshes_cached_rootless_map_after_phase1_rebuild(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as cached:
        values = await _signed_job(cached, registry_issuer)
        await _complete(cached, values)
        await cached.commit()
        row = (await cached.scalars(select(TaskImageMaterialization))).one()
        await cached.commit()
        rootless_images = dict(row.registry_images)
        async with registry_authority_session() as writer:
            await retry_task_image_materialization(writer, materialization_id=row.id)
            current = await claim_task_image_materialization(
                writer, builder_id="phase1", cpu_arch="arm64"
            )
            assert current is not None
            await start_task_image_materialization(
                writer,
                materialization_id=row.id,
                builder_id="phase1",
                lease_epoch=current.lease_epoch,
            )
            images = {"task": "registry.example:5443/loom/task-images/legacy@sha256:" + "b" * 64}
            await complete_task_image_materialization(
                writer,
                materialization_id=row.id,
                builder_id="phase1",
                lease_epoch=current.lease_epoch,
                registry_images=images,
            )
            await writer.commit()
        assert row.registry_images == rootless_images
        claimed = await claim_task_image_registry_gc(cached, gc_id="gc", grace_hours=0)
        assert claimed is row and claimed.state == "retiring"
        assert claimed.registry_images == images
        assert claimed.ready_publication_operation_id is None
        await cached.commit()


async def test_gc_rejects_pending_materialization_writes_before_refresh(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await _complete(session, values)
        await session.commit()
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        row.failure_message = "uncommitted caller edit"
        with session.no_autoflush:
            with pytest.raises(ValueError, match="pending materialization"):
                await claim_task_image_registry_gc(session, gc_id="gc", grace_hours=0)
        assert row.failure_message == "uncommitted caller edit" and row in session.dirty


async def test_stale_legacy_publication_cannot_add_rootless_attempt_to_legacy_history(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await _complete(session, values)
        await session.commit()
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        attempt = (await session.scalars(select(TaskImageMaterializationAttempt))).one()
        images = dict(row.registry_images)
        await retry_task_image_materialization(session, materialization_id=row.id)
        await session.commit()
        with pytest.raises(TaskImageCompletionError, match="rootless"):
            async with session.begin_nested():
                await record_task_image_publication(
                    session,
                    materialization_id=row.id,
                    builder_id=attempt.builder_id,
                    attempt_count=attempt.attempt_number,
                    lease_epoch=attempt.lease_epoch,
                    component="task",
                    registry_image=images["task"],
                )
        await session.refresh(row)
        assert row.registry_image_history == []
