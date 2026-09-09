"""Renewal changes live authority, never the original claim's provenance."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.db.schema import TaskImageMaterializationAttempt
from loom.task_image_build_plan import TaskImageBuildComponentV1, parse_task_image_build_plan
from loom_task_image_authority.materializations import (
    TaskImageSessionMaterializationAuthorizationError,
    _plan_snapshot,
    _stored_attempt_claim_plan,
    _stored_claim_plan,
)
from tests.unit.test_task_image_build_plan_versions import strong_payload
from tests.unit.test_task_image_bundle_capability import _plan


def _receipt(strong=False):
    plan = parse_task_image_build_plan(json.dumps(strong_payload())) if strong else _plan()
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


@pytest.mark.parametrize("strong", [False, True])
def test_successor_can_validate_attempt_provenance_but_not_replay_original_claim(strong):
    plan, attempt, authorization = _receipt(strong)
    assert _stored_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id) == plan
    authorization.session_id = uuid4()
    authorization.session_generation += 1
    assert _stored_attempt_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id) == plan
    with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
        _stored_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id)


def test_stored_receipt_budget_counts_compact_utf8_not_ascii_reencoding():
    _, attempt, authorization = _receipt()
    plan = _plan(components=tuple(
        TaskImageBuildComponentV1(
            name=name, dockerfile_path="x/" + "☃" * 4090, context_path=".",
            oci_output_path=f"oci/{index:04d}.tar",
        )
        for index, name in enumerate(("task", "sidecar:a", "sidecar:b"))
    ))
    payload, digest = _plan_snapshot(plan)
    assert len(plan.model_dump_json().encode()) < 64 * 1024
    assert len(json.dumps(payload)) > 64 * 1024
    attempt.claim_plan_json, attempt.claim_plan_sha256 = payload, digest
    assert _stored_attempt_claim_plan(attempt, authorization=authorization, materialization_id=plan.materialization_id) == plan


@pytest.mark.parametrize("changed", [
    "grant_id", "session_id", "session_generation", "builder_id", "materialization_id",
    "cpu_arch", "digest", "noncanonical_payload", "missing_payload",
])
@pytest.mark.parametrize("strong", [False, True])
def test_original_attempt_provenance_is_required_even_with_matching_receipt_digest(changed, strong):
    plan, attempt, authorization = _receipt(strong)
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
