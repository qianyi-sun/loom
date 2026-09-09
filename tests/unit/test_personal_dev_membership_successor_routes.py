"""Owner-facing status exposes lineage without publishing operator authority."""

from dataclasses import replace
from uuid import uuid4

from loom_service.routes.dev_instances import _personal_operation_response
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
