"""Claim replay must not invert the shared materialization/attempt fence."""

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, text

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from loom_task_image_authority.materializations import (
    TaskImageSessionMaterializationAuthorizationError,
    TaskImageSessionMaterializationConflictError,
    claim_session_materialization,
)
from tests.integration.test_task_image_authority_materializations import (
    CLAIM_ID,
    NOW,
    _active_authorization,
    _queued_materialization,
)
from tests.integration.test_task_image_publication_jobs import _blocked
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)


@pytest.mark.parametrize("condition", ["unchanged", "claim_rebound", "cached_plan_changed"])
async def test_replay_waits_for_parent_without_locking_attempt(
    registry_authority_session,
    condition,
):
    async with registry_authority_session() as session:
        authorization, *_ = await _active_authorization(session)
        row = await _queued_materialization(session)
        original = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        materialization_id = row.id
        attempt_id = await session.scalar(
            select(TaskImageMaterializationAttempt.id).where(
                TaskImageMaterializationAttempt.claim_id == CLAIM_ID,
            )
        )
        original_plan = original[1]
        await session.commit()
    async with registry_authority_session() as parent_owner, registry_authority_session() as replay:
        await parent_owner.scalar(
            select(TaskImageMaterialization)
            .where(
                TaskImageMaterialization.id == materialization_id,
            )
            .with_for_update()
        )
        pid = await replay.scalar(text("SELECT pg_backend_pid()"))
        cached_attempt = await replay.get(TaskImageMaterializationAttempt, attempt_id)
        assert cached_attempt is not None
        task = asyncio.create_task(
            claim_session_materialization(
                replay,
                authorization=authorization,
                claim_id=CLAIM_ID,
                now=NOW + timedelta(seconds=11),
                lease_seconds=300,
            )
        )
        try:
            await _blocked(parent_owner, pid, task)
            # With the old child-first replay this raises LockNotAvailable:
            # waiting instead would close the actual two-transaction deadlock.
            # Parent-first replay must leave this child lock available.
            locked = await parent_owner.scalar(
                select(TaskImageMaterializationAttempt)
                .where(
                    TaskImageMaterializationAttempt.id == attempt_id,
                )
                .with_for_update(nowait=True)
            )
            assert locked is not None
            if condition == "claim_rebound":
                locked.claim_id = uuid4()
            elif condition == "cached_plan_changed":
                locked.claim_plan_sha256 = "f" * 64
            await parent_owner.commit()
            if condition != "unchanged":
                # Neither a changed discovery binding nor stale ORM evidence
                # can survive the locked attempt reload after the parent wait.
                expected = (
                    TaskImageSessionMaterializationConflictError
                    if condition == "claim_rebound"
                    else TaskImageSessionMaterializationAuthorizationError
                )
                with pytest.raises(expected):
                    await asyncio.wait_for(task, 5)
                return
            result = await asyncio.wait_for(task, 5)
            assert result is not None
            assert result[0].id == materialization_id and result[1] == original_plan
            await replay.commit()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
