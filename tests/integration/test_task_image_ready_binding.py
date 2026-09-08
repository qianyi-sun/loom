from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from loom.db.schema import TaskImageMaterialization, TaskImagePublicationJob
from loom_control_plane.task_image_materializations import (
    claim_task_image_materialization,
    claim_task_image_registry_gc,
    complete_task_image_materialization,
    retry_task_image_materialization,
    start_task_image_materialization,
)
from tests.integration.test_task_image_authority_materializations import _queued_materialization
from tests.integration.test_task_image_publication_completion import (
    _complete,
    _signed_job,
    completion,
)
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    NOW,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


@pytest.mark.parametrize("legacy_history", [False, True])
async def test_legacy_gc_does_not_claim_rootless_ready(
    registry_authority_session, registry_issuer, legacy_history
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await _complete(session, values)
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        if legacy_history:
            row.registry_image_history = [{"registry_images": {"task": "legacy"}}]
        await session.commit()
        before = (dict(row.registry_images), row.ready_at, row.unreferenced_at, row.updated_at)
        for _ in range(2):
            assert (
                await claim_task_image_registry_gc(session, gc_id="legacy-gc", grace_hours=0)
                is None
            )
            await session.commit()
        await session.refresh(row)
        assert row.state == "ready"
        assert (row.registry_images, row.ready_at, row.unreferenced_at, row.updated_at) == before
        assert row.ready_publication_operation_id == UUID(values[0].operation_id)
        stored = await session.get(TaskImagePublicationJob, row.ready_publication_operation_id)
        assert stored.materialization_id == row.id
        assert str(stored.materialization_attempt_id) == values[0].snapshot.attempt_id


async def test_retry_and_phase1_completion_preserve_historical_rootless_receipt(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        receipt = await _complete(session, values)
        await session.commit()
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        assert row.ready_publication_operation_id == UUID(values[0].operation_id)
        row = await retry_task_image_materialization(session, materialization_id=row.id)
        assert row.ready_publication_operation_id is None
        assert row.ready_at is None and row.registry_images == {}
        await session.commit()
        row = await claim_task_image_materialization(session, builder_id="phase1", cpu_arch="arm64")
        assert row is not None
        await start_task_image_materialization(
            session, materialization_id=row.id, builder_id="phase1", lease_epoch=row.lease_epoch
        )
        images = {"task": "registry.example:5443/loom/task-images/legacy@sha256:" + "a" * 64}
        await complete_task_image_materialization(
            session,
            materialization_id=row.id,
            builder_id="phase1",
            lease_epoch=row.lease_epoch,
            registry_images=images,
        )
        await session.commit()
        ready_at = row.ready_at
        assert (
            await completion().replay_completed_publication(
                session, operation_id=values[0].operation_id
            )
            == receipt
        )
        await session.refresh(row)
        assert row.registry_images == images and row.ready_at == ready_at
        assert row.ready_publication_operation_id is None
        assert await claim_task_image_registry_gc(session, gc_id="legacy-gc", grace_hours=0) is None
        await session.commit()
        claimed = await claim_task_image_registry_gc(session, gc_id="legacy-gc", grace_hours=0)
        assert claimed is not None and claimed.id == row.id and claimed.state == "retiring"


async def test_database_preserves_ready_binding_and_requires_explicit_reset(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await _complete(session, values)
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        other = await _queued_materialization(session, task_id="unrelated")
        await session.commit()
        identity = {"id": row.id, "other": other.id, "operation": UUID(values[0].operation_id)}
        for assignment in (
            'registry_images = \'{"task": "changed"}\'::jsonb',
            "ready_at = ready_at + interval '1 second'",
            "ready_publication_operation_id = NULL",
            "ready_publication_operation_id = '00000000-0000-0000-0000-000000000001'::uuid",
            "state = 'retiring'",
        ):
            with pytest.raises(IntegrityError):
                async with session.begin_nested():
                    await session.execute(
                        text(f"UPDATE task_image_materializations SET {assignment} WHERE id = :id"),
                        identity,
                    )
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                await session.execute(
                    text("""UPDATE task_image_materializations
                    SET state = 'ready', registry_images = '{"task":"copied"}'::jsonb,
                    ready_at = :instant, ready_publication_operation_id = :operation
                    WHERE id = :other"""),
                    {**identity, "instant": NOW + timedelta(seconds=14)},
                )
        await session.execute(
            text("""UPDATE task_image_materializations
            SET state = 'queued', registry_images = '{}'::jsonb, ready_at = NULL,
                ready_publication_operation_id = NULL WHERE id = :id"""),
            identity,
        )
        await session.commit()
        await session.refresh(row)
        assert row.ready_publication_operation_id is None and row.registry_images == {}
        assert row.ready_at is None and row.state == "queued"
