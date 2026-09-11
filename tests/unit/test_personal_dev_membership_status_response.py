"""Stored admission receipts describe preparation, never observed worker readiness."""

from dataclasses import replace

import pytest

from loom_service.routes.dev_instances import _personal_environment_response
from tests.unit.test_personal_dev_membership_reconciler import _pending_claim


@pytest.mark.parametrize("accepted", (False, True))
def test_membership_status_uses_accepted_checkpoint_without_claiming_a_worker(accepted):
    claim = _pending_claim()
    checkpoint = claim.operation.capacity_membership_envelope.expected_checkpoint
    record = replace(
        claim.environment,
        accepted_capacity_mode="membership-v1",
        capacity_configuration_epoch=None,
        accepted_capacity_membership_checkpoint=checkpoint if accepted else None,
    )
    response = _personal_environment_response(record)
    assert response.capacity_prepared is accepted
    assert response.capacity_status == ("prepared" if accepted else "shadow")
    assert response.worker_available is False


def test_pending_membership_does_not_hide_prior_accepted_shadow_preparation():
    claim = _pending_claim()
    record = replace(claim.environment, capacity_configuration_epoch=7)
    response = _personal_environment_response(record)
    assert response.capacity_prepared is True
    assert response.capacity_status == "prepared"
    assert response.worker_available is False
