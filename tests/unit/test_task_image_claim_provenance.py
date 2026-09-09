"""Renewal changes live authority, never the original claim's provenance."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.db.schema import TaskImageMaterializationAttempt
from loom_task_image_authority.materializations import (
    TaskImageSessionMaterializationAuthorizationError,
    _plan_snapshot,
    _stored_attempt_claim_plan,
    _stored_claim_plan,
)
from tests.unit.test_task_image_bundle_capability import _plan


def _receipt():
    plan = _plan()
    payload, digest = _plan_snapshot(plan)
    attempt = TaskImageMaterializationAttempt(
        grant_id=plan.grant_id, session_id=plan.session_id,
        session_generation=plan.session_generation, builder_id=plan.builder_id,
        materialization_id=plan.materialization_id,
        claim_plan_json=payload, claim_plan_sha256=digest,
    )
    authorization = SimpleNamespace(
        grant_id=plan.grant_id, session_id=plan.session_id,
        session_generation=plan.session_generation, cpu_arch=plan.cpu_arch,
    )
    return plan, attempt, authorization


def test_successor_can_validate_attempt_provenance_but_not_replay_original_claim():
    plan, attempt, authorization = _receipt()
    assert _stored_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id) == plan
    authorization.session_id = uuid4()
    authorization.session_generation += 1
    assert _stored_attempt_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id) == plan
    with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
        _stored_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id)


@pytest.mark.parametrize("changed", [
    "grant_id", "session_id", "session_generation", "builder_id", "materialization_id",
    "cpu_arch", "digest", "noncanonical_payload", "missing_payload",
])
def test_original_attempt_provenance_is_required_even_with_matching_receipt_digest(changed):
    plan, attempt, authorization = _receipt()
    if changed in {"grant_id", "session_id", "materialization_id"}:
        setattr(attempt, changed, uuid4())
    elif changed == "session_generation":
        attempt.session_generation += 1
    elif changed == "builder_id":
        attempt.builder_id = "rootless:" + uuid4().hex
    elif changed == "cpu_arch":
        authorization.cpu_arch = "x86_64"
    elif changed == "digest":
        attempt.claim_plan_sha256 = "f" * 64
    elif changed == "noncanonical_payload":
        attempt.claim_plan_json = dict(attempt.claim_plan_json, authorization_expires_at="2026-09-03T14:00:40+00:00")
    else:
        attempt.claim_plan_json = None
    with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
        _stored_attempt_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id)
