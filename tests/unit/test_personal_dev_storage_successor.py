"""Recovery adoption must retain the same physical storage authority."""

import json
from dataclasses import replace

import pytest

from loom.personal_dev_incarnation_storage import PersonalDevStorageBindingV1
from loom.personal_dev_membership_successor import (
    PersonalDevMembershipSuccessorBindingV1,
    validate_membership_successor,
)
from tests.unit.test_personal_dev_membership_successor import _NOW, successor_case


@pytest.mark.parametrize("changed", ("environment", "operation", "accepted"))
def test_successor_rejects_storage_disagreement_before_child_reservation(changed):
    claim, accepted, values = successor_case("update", "terminal-not-committed")
    storage = PersonalDevStorageBindingV1(
        layout="incarnation-v1", environment_name=claim.operation.environment_name,
        subject_id=claim.operation.subject_id, subject_incarnation=claim.operation.subject_incarnation,
        owner_user_id=claim.operation.owner_user_id, owner_team_id=claim.operation.owner_team_id,
    )
    claim = replace(claim, environment=replace(claim.environment, storage_binding=storage),
                    operation=replace(claim.operation, storage_binding=storage))
    accepted = replace(accepted, storage_binding=storage)
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    validate_membership_successor(binding, claim=claim, accepted_operation=accepted, now=_NOW)
    if changed == "accepted":
        accepted = replace(accepted, storage_binding=None)
    else:
        claim = replace(claim, **{changed: replace(getattr(claim, changed), storage_binding=None)})
    with pytest.raises(ValueError, match="storage"):
        validate_membership_successor(binding, claim=claim, accepted_operation=accepted, now=_NOW)
