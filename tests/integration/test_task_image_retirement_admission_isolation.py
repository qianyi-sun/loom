"""Actual admission must not mistake an old transaction snapshot for fresh retirement state."""

from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TaskImageAttemptRetention
from loom_task_image_authority.materializations import (
    claim_session_materialization,
    get_session_materialization_build_plan,
)
from tests.integration.test_task_image_publication_completion import _complete, _signed_job
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retired_credential_ingress import _retire
from tests.integration.test_task_image_retirement_snapshot import _setup


async def _admit(session, row, attempt, options, surface):
    if surface == "claim_replay":
        return await claim_session_materialization(
            session, authorization=options["authorization"],
            claim_id=attempt.claim_id, now=NOW + timedelta(seconds=12), lease_seconds=300,
        )
    return await get_session_materialization_build_plan(
        session, authorization=options["authorization"], materialization_id=row.id,
        attempt_id=attempt.id, lease_epoch=attempt.lease_epoch,
        now=NOW + timedelta(seconds=12),
    )


@pytest.mark.parametrize("surface", ["claim_replay", "plan"])
@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
async def test_admission_cannot_revive_retired_attempt_from_old_transaction_snapshot(
    registry_authority_session, registry_issuer, surface, isolation,
):
    factory = registry_authority_session
    row, attempt, options = await _setup(factory, registry_issuer)
    async with factory() as stale:
        await stale.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
        assert await stale.scalar(select(TaskImageAttemptRetention.attempt_id)) is None
        # Real owned retirement changes the marker but preserves the abandoned
        # attempt/parent. Acquiring their locks cannot refresh a fixed snapshot.
        await _retire(factory, attempt.id)
        async with factory() as probe:
            assert await probe.scalar(select(TaskImageAttemptRetention.retired_at)) is not None
        # Deliberately old otherwise-live caller time proves retirement is not
        # merely a lease TTL. The production entrypoint must reject authority.
        with pytest.raises(RuntimeError, match=r"retired|READ COMMITTED"):
            await _admit(stale, row, attempt, options, surface)


async def test_autocommit_cannot_release_parent_fence_between_admission_queries(
    registry_authority_session, registry_issuer,
):
    factory = registry_authority_session
    row, attempt, options = await _setup(factory, registry_issuer)
    engine = factory.kw["bind"].execution_options(isolation_level="AUTOCOMMIT")
    async with AsyncSession(engine, expire_on_commit=False) as session:
        with pytest.raises(RuntimeError, match="transaction"):
            await _admit(session, row, attempt, options, "plan")


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
async def test_publication_completion_cannot_admit_from_old_retirement_snapshot(
    registry_authority_session, registry_issuer, isolation,
):
    factory = registry_authority_session
    async with factory() as session:
        values = await _signed_job(session, registry_issuer)
        await session.commit()
    async with factory() as stale:
        await stale.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
        assert await stale.scalar(select(TaskImageAttemptRetention.attempt_id)) is None
        await _retire(factory, UUID(values[0].snapshot.attempt_id))
        with pytest.raises(RuntimeError, match=r"retired|READ COMMITTED"):
            await _complete(stale, values)
