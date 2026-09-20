"""New signed build claims reject ARM without invalidating stored receipts."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom.task_image_build_plan import derive_task_image_build_plan
from loom_task_image_authority import materializations
from tests.unit.test_task_image_build_plan import NOW, _authorization, _row


@pytest.mark.parametrize("cpu_arch", ["arm64", "x86_64"])
async def test_signed_session_new_claim_architecture_boundary(monkeypatch, cpu_arch):
    session = AsyncMock()
    session.scalar.return_value = None
    lock = AsyncMock()
    replay = AsyncMock(return_value=None)
    monkeypatch.setattr(materializations, "lock_current_task_image_build_session_authority", lock)
    monkeypatch.setattr(materializations, "_claim_replay", replay)
    authorization = _authorization(cpu_arch=cpu_arch)
    request = dict(session=session, authorization=authorization, claim_id=uuid4(), now=NOW,
                   lease_seconds=300)
    if cpu_arch == "arm64":
        with pytest.raises(materializations.TaskImageSessionMaterializationAuthorizationError,
                           match="x86_64 only"):
            await materializations.claim_session_materialization(**request)
        session.scalar.assert_not_awaited()
    else:
        assert await materializations.claim_session_materialization(**request) is None
        assert session.scalar.await_count == 2
    lock.assert_awaited_once()
    replay.assert_awaited_once()
    session.add.assert_not_called()


async def test_signed_session_historical_arm_claim_replay_stays_readable(monkeypatch):
    session = AsyncMock()
    authorization = _authorization()
    row = _row()
    historical_receipt = (row, derive_task_image_build_plan(row, authorization))
    lock = AsyncMock()
    replay = AsyncMock(return_value=historical_receipt)
    monkeypatch.setattr(materializations, "lock_current_task_image_build_session_authority", lock)
    monkeypatch.setattr(materializations, "_claim_replay", replay)
    result = await materializations.claim_session_materialization(
        session, authorization=authorization, claim_id=uuid4(), now=NOW, lease_seconds=300,
    )
    assert result is historical_receipt
    assert result[1].cpu_arch == "arm64"
    lock.assert_awaited_once()
    replay.assert_awaited_once()
    session.scalar.assert_not_awaited()
    session.add.assert_not_called()
