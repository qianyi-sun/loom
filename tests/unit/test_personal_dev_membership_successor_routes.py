"""Owner-facing status exposes lineage without publishing operator authority."""

from dataclasses import replace
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response

from loom.personal_dev_environment import PersonalDevApplyReservation
from loom_service.routes.dev_instances import _personal_operation_response
from loom_service.routes.dev_instances import (
    PersonalDevEnvironmentApplyPayload,
    apply_personal_dev_environment,
)
from tests.unit.test_dev_instance_routes import _OWNER, _Store, _ctx, _request
from tests.unit.test_personal_dev_membership_successor import successor_case


def test_superseded_operation_is_an_honest_nonready_status():
    claim, _, _ = successor_case()
    child_id = uuid4()
    operation = replace(
        claim.operation, state="superseded", checkpoint="membership_successor_created",
        membership_successor_operation_id=child_id,
    )
    response = _personal_operation_response(operation).model_dump(mode="json")
    assert response["state"] == "superseded"
    assert response["checkpoint"] == "membership_successor_created"
    assert response["membership_successor_operation_id"] == str(child_id)
    assert "membership_successor_binding" not in response


def test_child_status_exposes_only_lineage_identifiers_and_original_intent():
    claim, _, _ = successor_case()
    parent_id = uuid4()
    operation = replace(
        claim.operation, membership_predecessor_operation_id=parent_id,
        membership_continuation_kind="capacity",
    )
    response = _personal_operation_response(operation).model_dump(mode="json")
    assert response["membership_predecessor_operation_id"] == str(parent_id)
    assert response["membership_continuation_kind"] == "capacity"
    assert "membership_successor_binding" not in response


@pytest.mark.asyncio
async def test_committed_apply_with_unavailable_lineage_returns_retryable_status_error():
    claim, _, _ = successor_case()
    operation = replace(claim.operation, state="superseded", checkpoint="membership_successor_created")
    calls = []

    class Authority:
        async def apply(self, *args, **kwargs):
            calls.append("committed apply")
            return PersonalDevApplyReservation(
                environment=claim.environment, operation=operation, acquired=False, requires_build_binding=False,
            )

        async def get_operation(self, operation_id):
            assert operation_id == operation.id
            raise RuntimeError("injected post-commit read outage")

    request = _request(_Store(), configured=False)
    request.app.state.settings = type("Settings", (), {"dev_instances_enabled": True})()
    request.app.state.personal_dev_builder_available = True
    request.app.state.personal_dev_environment_authority_factory = lambda _: Authority()
    with pytest.raises(HTTPException) as caught:
        await apply_personal_dev_environment(
            operation.environment_name,
            PersonalDevEnvironmentApplyPayload(
                candidate_id=operation.candidate_id, candidate_sha=operation.candidate_sha,
                min_slots=operation.min_slots, max_slots=operation.max_slots,
                expected_operation_epoch=operation.expected_operation_epoch, idempotency_key=operation.idempotency_key,
            ),
            request, (object(), _ctx(_OWNER)), Response(),
        )
    assert calls == ["committed apply"]
    assert caught.value.status_code == 503
    assert caught.value.detail == "personal-dev apply was retained; recovery status is temporarily unavailable"
