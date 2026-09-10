"""V4 successor wire coverage retains a source edge even for empty histories."""

import copy
import json

import pytest

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from tests.unit.test_capacity_build_membership import build_membership_input
from tests.unit.test_capacity_successor_member_origins import successor_payload


def preparation_payload():
    preparation = build_membership_input().preparation.model_dump(mode="json")
    build = successor_payload()
    app = successor_payload(build=False)
    source = dict(build["inherited"]["source"], revision=3, head_sha256="e" * 64)
    build["inherited"]["source"] = copy.deepcopy(source)
    app["inherited"]["source"] = copy.deepcopy(source)
    preparation["managed_application_origins"].append(app)
    preparation["managed_build_origins"] = [build]
    preparation["retired_source"] = source
    preparation["configuration_epoch"] += 1
    preparation["personal_membership"]["managed_base_subject_ids"] = [
        origin["configuration"]["subject_id"]
        for origin in (*preparation["managed_application_origins"], build)]
    preparation["subject_acknowledgements"].extend(copy.deepcopy((app["acknowledgement"], build["acknowledgement"])))
    return preparation


def parse(payload):
    return ExecutionPreparationV4.model_validate_json(json.dumps(payload))


def test_successor_preparation_retains_full_typed_origin_without_base_class_truncation():
    value = parse(preparation_payload())
    encoded = value.model_dump(mode="json")
    app = next(origin for origin in encoded["managed_application_origins"] if origin["schema_version"] == 2)
    assert app["inherited"]["source"] == encoded["retired_source"]
    assert len(value.managed_build_origins) == 1
    assert parse(encoded) == value


@pytest.mark.parametrize("boundary", (
    "missing-source", "missing-build", "duplicate-build", "missing-ack", "changed-ack",
    "different-source", "different-namespace", "release-substitution", "template-substitution",
))
def test_successor_preparation_requires_complete_consistent_managed_coverage(boundary):
    payload = preparation_payload()
    if boundary == "missing-source":
        del payload["retired_source"]
    elif boundary == "missing-build":
        payload["managed_build_origins"] = []
    elif boundary == "duplicate-build":
        payload["managed_build_origins"] *= 2
    elif boundary == "missing-ack":
        payload["subject_acknowledgements"].pop()
    elif boundary == "changed-ack":
        payload["subject_acknowledgements"][-1]["acknowledgement_sha256"] = "9" * 64
    elif boundary == "different-source":
        payload["retired_source"]["head_sha256"] = "f" * 64
    elif boundary == "different-namespace":
        payload["retired_source"]["namespace_id"] = "00000000-0000-0000-0000-000000000999"
    elif boundary == "release-substitution":
        payload["trusted_fleet_release_sha256"] = "9" * 64
    else:
        payload["personal_builds"]["max_pending_jobs_per_subject"] += 1
    with pytest.raises(ValueError):
        parse(payload)


def test_empty_successor_source_is_explicit_even_without_inherited_member_fields():
    payload = preparation_payload()
    original = build_membership_input().preparation.model_dump(mode="json")
    original["retired_source"] = dict(payload["retired_source"], revision=0, head_sha256="0" * 64)
    value = parse(original)
    assert value.retired_source.revision == 0
    assert all(origin.schema_version == 1 for origin in value.managed_application_origins)
    assert not value.managed_build_origins
