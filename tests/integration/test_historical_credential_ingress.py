"""Published database guards protect retained attempts without legacy issuers."""
from datetime import timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from loom.db.schema import TaskImageAttemptRetention, TaskImageRegistryCredentialGeneration
from tests.support.historical_task_images import NOW, _insert, _prepared_insert, _retire
from tests.support.historical_task_images import (
    registry_authority_session as registry_authority_session,
)


async def test_retired_attempt_rejects_direct_insert(registry_authority_session):
    attempt, values = await _prepared_insert(registry_authority_session)
    await _retire(registry_authority_session, attempt)
    async with registry_authority_session() as session:
        with pytest.raises(IntegrityError) as error:
            await _insert(session, values)
        assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_not_retired"
        await session.rollback()
        assert await session.scalar(select(TaskImageRegistryCredentialGeneration.credential_id)) is None


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_stale_snapshot_cannot_hide_retirement(registry_authority_session, isolation):
    attempt, values = await _prepared_insert(registry_authority_session)
    async with registry_authority_session() as session:
        session.add(TaskImageAttemptRetention(attempt_id=attempt, observed_at=NOW + timedelta(hours=1)))
        await session.commit()
    async with registry_authority_session() as stale:
        await stale.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
        assert (await stale.get(TaskImageAttemptRetention, attempt)).retired_at is None
        await _retire(registry_authority_session, attempt)
        with pytest.raises(IntegrityError) as error:
            await _insert(stale, values)
        assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_read_committed"
