"""Retained signed publication evidence stays readable after hosted retirement."""
from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError

from loom.db.schema import (
    TaskImageBuildGrant,
    TaskImageMaterialization,
    TaskImagePublicationCandidate,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationKey,
    TaskImagePublicationState,
)
from loom_control_plane.task_image_materializations import retry_task_image_materialization
from loom_task_image_authority.publication_completion import replay_completed_publication
from loom_task_image_authority.publication_receipts import canonical_receipt_bytes
from tests.support.historical_task_images import NOW, restore_rows
from tests.support.historical_task_images import (
    registry_authority_session as registry_authority_session,
)


async def _completed(session):
    await restore_rows(session, "completed_task_image_publication")
    await session.commit()
    job = (await session.scalars(select(TaskImagePublicationJob))).one()
    return job.operation_id, await replay_completed_publication(session, operation_id=job.operation_id)


async def test_historical_replay_preserves_timestamp_precision(registry_authority_session):
    async with registry_authority_session() as session:
        operation, receipt = await _completed(session)
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        assert row.ready_at == NOW + timedelta(seconds=14, microseconds=999999)
        assert receipt.completed_at == "2026-09-02T14:00:14Z"
        assert await replay_completed_publication(session, operation_id=operation) == receipt


async def test_revoked_keys_and_locks_do_not_block_historical_replay(registry_authority_session):
    async with registry_authority_session() as session:
        operation, receipt = await _completed(session)
        await session.execute(update(TaskImagePublicationKey).values(
            status="revoked", revoked_at=NOW + timedelta(seconds=16),
        ))
        await session.execute(update(TaskImagePublicationState).values(keyset_version=2))
        await session.commit()
    async with registry_authority_session() as blocker, registry_authority_session() as history:
        await blocker.scalar(select(TaskImagePublicationState).with_for_update())
        await blocker.scalar(select(TaskImagePublicationKey).with_for_update())
        await history.scalar(select(TaskImageBuildGrant).with_for_update())
        async with asyncio.timeout(2):
            assert await replay_completed_publication(history, operation_id=operation) == receipt


@pytest.mark.parametrize("change", ["receipt", "signature", "candidate"])
async def test_historical_replay_rejects_corrupted_evidence(registry_authority_session, change):
    async with registry_authority_session() as session:
        operation, receipt = await _completed(session)
        table = {"receipt": "task_image_publication_jobs", "signature": "task_image_publication_envelopes",
                 "candidate": "task_image_publication_candidates"}[change]
        # Fault injection only in this disposable database.
        await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
        if change == "receipt":
            encoded = canonical_receipt_bytes(receipt.model_copy(update={"publication_set_sha256": "e" * 64}))
            await session.execute(update(TaskImagePublicationJob).values(
                canonical_receipt=encoded, receipt_sha256=hashlib.sha256(encoded).hexdigest(),
            ))
        elif change == "signature":
            await session.execute(update(TaskImagePublicationEnvelope).values(signature="A" * 86))
        else:
            await session.execute(update(TaskImagePublicationCandidate).values(oci_file_sha256="e" * 64))
        await session.commit()
        with pytest.raises((RuntimeError, ValueError)):
            await replay_completed_publication(session, operation_id=operation)


async def test_completed_receipt_immutable_across_materialization_retry(registry_authority_session):
    async with registry_authority_session() as session:
        operation, receipt = await _completed(session)
        for changes in (
            {"completed_at": NOW + timedelta(seconds=15)}, {"canonical_receipt": b"{}"},
            {"receipt_sha256": "e" * 64}, {"state": "queued"},
        ):
            with pytest.raises(IntegrityError):
                await session.execute(update(TaskImagePublicationJob).values(**changes))
            await session.rollback()
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        await retry_task_image_materialization(session, materialization_id=row.id)
        await session.commit()
        assert await replay_completed_publication(session, operation_id=operation) == receipt
