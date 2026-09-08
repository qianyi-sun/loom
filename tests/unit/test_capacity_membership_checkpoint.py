"""Strict public membership checkpoint and response contracts."""

from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipResponseV1,
    PersonalApplicationMembershipResultV1,
    PersonalMembershipCheckpointV1,
)
from tests.unit.test_capacity_manager_executable_allocator import execution_authority_fixture
from tests.unit.test_capacity_membership import delegated_input_with_new_owner


@pytest.mark.parametrize("invalid", ("namespace", "empty_head", "nonempty_head", "drain"))
def test_membership_checkpoint_rejects_invalid_active_position(invalid: str) -> None:
    authority = execution_authority_fixture()
    values = {
        "execution": authority,
        "namespace_id": UUID(int=1),
        "revision": 0,
        "head_sha256": "0" * 64,
    }
    values.update(
        {
            "namespace": {"namespace_id": UUID(int=0)},
            "empty_head": {"head_sha256": "a" * 64},
            "nonempty_head": {"revision": 1},
            "drain": {"execution": execution_authority_fixture(execution_state="drain-only")},
        }[invalid]
    )
    with pytest.raises(ValidationError):
        PersonalMembershipCheckpointV1.model_validate(values)


@pytest.mark.parametrize("mismatch", (None, "revision", "head_sha256"))
def test_membership_response_carries_original_result_checkpoint(mismatch: str | None) -> None:
    value = delegated_input_with_new_owner()
    member = value.membership.members[-1]
    result = PersonalApplicationMembershipResultV1(
        revision=member.revision,
        head_sha256="a" * 64,
        member=member,
        replayed=True,
    )
    checkpoint = PersonalMembershipCheckpointV1(
        execution=execution_authority_fixture(),
        namespace_id=value.membership.namespace_id,
        revision=result.revision,
        head_sha256=result.head_sha256,
    )
    if mismatch:
        checkpoint = checkpoint.model_copy(
            update={
                mismatch: (checkpoint.revision + 1 if mismatch == "revision" else "b" * 64),
            }
        )
        with pytest.raises(ValidationError, match="checkpoint differs"):
            PersonalApplicationMembershipResponseV1(checkpoint=checkpoint, result=result)
    else:
        response = PersonalApplicationMembershipResponseV1(checkpoint=checkpoint, result=result)
        assert (
            PersonalApplicationMembershipResponseV1.model_validate_json(response.model_dump_json())
            == response
        )
