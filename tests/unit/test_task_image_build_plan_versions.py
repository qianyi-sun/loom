"""Strong frozen-plan readers must preserve historical canonical claim receipts."""

import hashlib
import importlib
import json

import pytest
import rfc8785

from loom.task_image_build_plan import TaskImageBuildPlanV1
from tests.unit.test_task_image_bundle_capability import _FakeBundleBackend, _plan, _provider


def _module():
    return importlib.import_module("loom.task_image_build_plan")


def strong_payload():
    return dict(
        _plan().model_dump(mode="json"),
        schema_version="loom.task-image-build-plan.v2",
        bundle_content_manifest_sha256="6" * 64,
        bundle_prefix=f"bench/revision/{'6' * 64}/",
    )


def test_versioned_parser_retains_exact_v1_serialization_and_hash():
    historical = _plan()
    before = historical.model_dump_json()
    parsed = _module().parse_task_image_build_plan(before)
    assert type(parsed) is TaskImageBuildPlanV1
    assert parsed.model_dump_json() == before
    assert parsed.model_dump(mode="json") == historical.model_dump(mode="json")
    assert "bundle_content_manifest_sha256" not in parsed.model_dump()
    assert parsed.content_manifest_digest == ""
    assert hashlib.sha256(rfc8785.dumps(parsed.model_dump(mode="json"))).digest() == hashlib.sha256(rfc8785.dumps(historical.model_dump(mode="json"))).digest()


def test_v2_carries_registered_digest_without_changing_common_plan_fields():
    payload = strong_payload()
    plan = _module().parse_task_image_build_plan(json.dumps(payload))
    assert type(plan) is _module().TaskImageBuildPlanV2
    assert plan.model_dump(mode="json") == payload
    assert plan.content_manifest_digest == "6" * 64
    with pytest.raises(ValueError):
        TaskImageBuildPlanV1.model_validate_json(plan.model_dump_json())
    with pytest.raises(ValueError):
        plan.bundle_content_manifest_sha256 = "7" * 64


@pytest.mark.parametrize("change", [
    {"bundle_content_manifest_sha256": None}, {"bundle_content_manifest_sha256": ""},
    {"bundle_content_manifest_sha256": "A" * 64}, {"bundle_content_manifest_sha256": "0" * 64},
    {"bundle_prefix": "bench/legacy/"}, {"bundle_prefix": f"bench/{'7' * 64}/"},
    {"bundle_prefix": f"bench/{'6' * 64}/extra/"},
    {"schema_version": "loom.task-image-build-plan.v1"},
    {"schema_version": "loom.task-image-build-plan.v3"},
    {"cpu_arch": "x86_64"}, {"builder_id": "rootless:" + "a" * 32},
])
def test_versioned_plan_rejects_digest_prefix_version_and_common_binding_drift(change):
    with pytest.raises(ValueError):
        _module().parse_task_image_build_plan(json.dumps(dict(strong_payload(), **change)))


@pytest.mark.parametrize("field", ["schema_version", "bundle_content_manifest_sha256"])
def test_v2_parser_never_infers_missing_version_or_registered_digest(field):
    payload = strong_payload()
    del payload[field]
    with pytest.raises(ValueError):
        _module().parse_task_image_build_plan(json.dumps(payload))


def test_versioned_parser_bounds_input_before_parsing():
    with pytest.raises(ValueError):
        _module().parse_task_image_build_plan(b" " * (64 * 1024 + 1))


def test_v1_bundle_provider_cannot_issue_weaker_capability_for_v2_plan():
    plan = _module().parse_task_image_build_plan(json.dumps(strong_payload()))
    backend = _FakeBundleBackend(())
    with pytest.raises(RuntimeError):
        _provider(backend).issue(plan, now=plan.authorization_expires_at)
    assert not backend.list_bounds and not backend.presign_expiries
