"""New admission follows trusted runtime mode; destroy follows accepted authority."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response

from loom.db.schema import DevInstance
from loom.dev_instance_provisioner import DevInstanceRecord
from loom.dev_instance_store import SqlAlchemyDevInstanceStore
from loom_service.routes import dev_instances as routes
from tests.unit.test_dev_instance_routes import _NOW, _OWNER, _TEAM, _ctx, _request, _Store


class Captured(BaseException):
    pass


@pytest.mark.parametrize("mode", ("shadow-v1", "membership-v1"))
async def test_store_get_preserves_persisted_accepted_mode(mode):
    row = DevInstance(
        name="alice", owner_user_id=_OWNER, owner_team_id=_TEAM,
        min_slots=0, max_slots=2, status="ready", deployment_generation=1,
        candidate_sha="a" * 64, operation_epoch=4, operation_id=uuid4(),
        created_at=_NOW, updated_at=_NOW, accepted_capacity_mode=mode,
    )
    session = AsyncMock()
    session.get.return_value = row
    result = await SqlAlchemyDevInstanceStore(session).get("alice")
    assert result.accepted_capacity_mode == mode
    session.get.assert_awaited_once_with(DevInstance, "alice")


@pytest.mark.parametrize("ready", (False, True))
async def test_active_apply_requires_interlock_and_persists_trusted_mode(ready):
    request = _request(_Store(), configured=False)
    request.app.state.settings = SimpleNamespace(dev_instances_enabled=True)
    request.app.state.personal_dev_builder_available = True
    request.app.state.personal_dev_runtime_mode = "membership-v1"
    captured = []

    class Guard:
        async def assert_admission_ready(self, *, now):
            captured.append("guard")

    class Authority:
        async def apply(self, requested, **kwargs):
            captured.append(kwargs)
            raise Captured

    request.app.state.personal_dev_environment_authority_factory = lambda _: Authority()
    if ready:
        request.app.state.personal_dev_membership_admission = Guard()
    payload = routes.PersonalDevEnvironmentApplyPayload(
        candidate_id=uuid4(), candidate_sha="a" * 64, min_slots=0, max_slots=2,
        expected_operation_epoch=0, idempotency_key=uuid4(),
    )
    with pytest.raises(Captured if ready else HTTPException) as error:
        await routes.apply_personal_dev_environment(
            "alice", payload, request, (object(), _ctx(_OWNER)), Response(),
        )
    if ready:
        assert captured[0] == "guard"
        assert captured[1]["capacity_mode"] == "membership-v1"
    else:
        assert error.value.status_code == 503
        assert not captured


async def test_destroy_uses_stored_membership_even_when_runtime_mode_changes():
    store = _Store()
    store.rows["alice"] = DevInstanceRecord(
        name="alice", owner_user_id=_OWNER, owner_team_id=_TEAM,
        min_slots=0, max_slots=2, status="ready", deployment_generation=1,
        candidate_sha="a" * 64, candidate_id=uuid4(), operation_epoch=4,
        operation_id=uuid4(), created_at=_NOW, updated_at=_NOW,
        accepted_capacity_mode="membership-v1",
    )
    request = _request(store, configured=False)
    request.app.state.settings = SimpleNamespace(dev_instances_enabled=True)
    request.app.state.personal_dev_runtime_mode = "operational"
    captured = []

    class Authority:
        async def destroy(self, requested, **kwargs):
            captured.append(kwargs)
            raise Captured

    request.app.state.personal_dev_environment_authority_factory = lambda _: Authority()
    with pytest.raises(Captured):
        await routes.delete_dev_instance(
            "alice", request, (object(), _ctx(_OWNER)), Response(),
            keep_data=True, expected_operation_epoch=4, idempotency_key=uuid4(),
        )
    assert captured[0]["capacity_mode"] == "membership-v1"
