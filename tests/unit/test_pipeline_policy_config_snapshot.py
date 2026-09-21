from __future__ import annotations

from uuid import UUID

import pytest

from loom.pipeline.policy_config import (
    PipelineScopedPolicyActivationV1,
)


def test_scoped_activation_is_authority_bound_and_fail_closed() -> None:
    value = {
        "schema_version": "loom.pipeline-scoped-policy-activation.v1",
        "environment": "staging",
        "policy_id": "behavior-cpu-data",
        "policy_config_sha256": "sha256:" + "a" * 64,
        "authority_kind": "acceptance",
        "authority_id": UUID("9e8174fa-7ad2-4386-869b-aadcfcc2cfa6"),
        "activation_epoch": 3,
        "state": "active",
        "desired_slots": 2,
    }
    assert PipelineScopedPolicyActivationV1.model_validate(value).activation_epoch == 3
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate({**value, "activation_epoch": 0})
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate({**value, "state": "disabled"})
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate({**value, "environment": " staging"})
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate(
            {**value, "authority_kind": "profile_calibration", "desired_slots": 2}
        )
