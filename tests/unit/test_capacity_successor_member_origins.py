"""Successor origins retain purpose, installation and inherited member lineage."""

import json
from importlib import import_module
from uuid import UUID

import pytest

from tests.unit.test_capacity_retired_member_origins import origin_payload
from tests.unit.test_capacity_typed_membership_events import _next_build_row, event_row


def successor_payload(*, build=True, operation="create"):
    value, _request, _result, first = event_row(build=build)
    changes = ({"deployment_generation": 2, "candidate_generation": 2,
        "demand_reporter_incarnation": UUID(int=988), "demand_reporter_token_sha256": "e" * 64}
        if operation == "update" else {})
    row = first if operation == "create" else _next_build_row(first, build=build, operation=operation, **changes)
    member = row.result_payload["member"]
    inherited = origin_payload(build=build)
    inherited["source"].update(revision=row.revision, head_sha256=row.head_sha256)
    inherited["anchor"].update(revision=row.revision, head_sha256=row.head_sha256, member=member)
    payload = {
        "schema_version": 1 if build else 2,
        "configuration": member["configuration"],
        "acknowledgement": member["acknowledgement"],
        "installation_projection": (row if operation == "update" else first).request_payload["command"]["projection"],
        "base_projection": row.request_payload["command"]["projection"],
        "inherited": inherited,
    }
    if build:
        payload.update(template=value.preparation.personal_builds.model_dump(mode="json"),
            trusted_fleet_release_sha256=value.preparation.trusted_fleet_release_sha256,
            readiness_state="pending")
    return payload


def parse(payload, *, build=True):
    module = import_module("loom_capacity_manager.successor_origin_contracts")
    model = module.ManagedBuildOriginV1 if build else module.ManagedApplicationOriginV2
    return model.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("operation", ("create", "update", "capacity", "destroy"))
def test_successor_retains_original_installation_and_last_member(build, operation):
    payload = successor_payload(build=build, operation=operation)
    value = parse(payload, build=build)
    assert value.model_dump(mode="json") == payload
    assert value.configuration == value.inherited.anchor.member.configuration
    assert value.installation_projection.operation_kind == ("update" if operation == "update" else "create")
    assert value.inherited.original_origin.generation == 1
    assert value.base_projection.operation_kind == operation
    if build:
        assert value.readiness_state == "pending"
        assert value.acknowledgement.candidate == value.template.runtime_candidate


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("boundary", ("missing-history", "acknowledgement", "old-member", "float-version"))
def test_successor_requires_exact_inherited_member(build, boundary):
    payload = successor_payload(build=build, operation="capacity")
    if boundary == "missing-history":
        del payload["inherited"]
    elif boundary == "acknowledgement":
        assert payload["acknowledgement"]["acknowledgement_sha256"] != "9" * 64
        payload["acknowledgement"] = dict(payload["acknowledgement"], acknowledgement_sha256="9" * 64)
    elif boundary == "old-member":
        payload["inherited"] = origin_payload(build=build)
    else:
        payload["schema_version"] = float(payload["schema_version"])
    with pytest.raises(ValueError):
        parse(payload, build=build)


@pytest.mark.parametrize("boundary", (
    "ready", "zero-release", "runtime", "profiles", "pending-limit", "maximum",
    "installation-owner", "installation-incarnation", "installation-candidate", "installation-deployment",
    "installation-reporter", "installation-token", "installation-operation", "installation-generation",
    "base-owner", "base-incarnation", "base-candidate", "base-deployment", "base-reporter", "base-maximum",
))
def test_build_origin_rejects_installation_or_policy_substitution(boundary):
    payload = successor_payload(operation="capacity")
    if boundary == "ready":
        payload["readiness_state"] = "ready"
    elif boundary == "zero-release":
        payload["trusted_fleet_release_sha256"] = "0" * 64
    elif boundary == "runtime":
        payload["template"]["runtime_candidate"]["identity"] = "e" * 40
    elif boundary == "profiles":
        payload["configuration"] = dict(payload["configuration"], profiles=[])
    elif boundary == "pending-limit":
        payload["template"]["max_pending_jobs_per_subject"] += 1
    elif boundary == "maximum":
        payload["template"]["max_slots_per_subject"] = 1
    else:
        which, field = boundary.split("-", 1)
        field, changed = {
            "owner": ("owner_id", str(UUID(int=987))),
            "incarnation": ("subject_incarnation", str(UUID(int=987))),
            "candidate": ("candidate_generation", 9),
            "deployment": ("deployment_generation", 9),
            "reporter": ("demand_reporter_incarnation", str(UUID(int=987))),
            "token": ("demand_reporter_token_sha256", "e" * 64),
            "operation": ("operation_kind", "capacity"),
            "generation": ("configuration_generation", 9),
            "maximum": ("max_slots", 1),
        }[field]
        payload[f"{which}_projection"] = dict(payload[f"{which}_projection"], **{field: changed})
    with pytest.raises(ValueError):
        parse(payload)


def test_old_application_origin_bytes_remain_unchanged():
    from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
    from loom_capacity_manager.contracts import canonical_bytes
    payload = successor_payload(build=False)
    del payload["inherited"]
    payload["schema_version"] = 1
    before = ManagedApplicationOriginV1.model_validate_json(json.dumps(payload))
    import_module("loom_capacity_manager.successor_origin_contracts")
    assert canonical_bytes(before) == json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    assert "inherited" not in before.model_dump()


@pytest.mark.parametrize("build", (False, True))
def test_successor_after_recreation_keeps_old_certificate_without_rewriting(build):
    from loom_capacity_manager.contracts import ConfigurationGenerationRefV1, SubjectConfigurationV1
    from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
    payload = successor_payload(build=build)
    _value, _request, _result, first = event_row(build=build)
    disabled = _next_build_row(first, operation="destroy", build=build)
    proof = PersonalReincarnationEvidenceV1(namespace_id=first.namespace_id,
        execution_manifest_sha256=first.execution_manifest_sha256,
        origin=ConfigurationGenerationRefV1.model_validate_json(json.dumps(payload["inherited"]["original_origin"])),
        predecessor=SubjectConfigurationV1.model_validate_json(json.dumps(disabled.result_payload["member"]["configuration"])),
        predecessor_revision=disabled.revision, predecessor_head_sha256=disabled.head_sha256,
        admission_revision=3, successor_incarnation=UUID(int=995), release_set_sha256="f" * 64)
    recreated = _next_build_row(disabled, operation="create", subject_incarnation=UUID(int=995),
        demand_reporter_incarnation=UUID(int=996), demand_reporter_token_sha256="e" * 64,
        reincarnation=proof, build=build)
    member = recreated.result_payload["member"]
    projection = recreated.request_payload["command"]["projection"]
    payload.update(configuration=member["configuration"], acknowledgement=member["acknowledgement"],
        installation_projection=projection, base_projection=projection)
    payload["inherited"]["anchor"].update(revision=3, head_sha256=recreated.head_sha256, member=member)
    # The immediately retired epoch imported this member but had no local events.
    payload["inherited"]["source"].update(execution_epoch=first.execution_epoch + 1,
        execution_manifest_sha256="d" * 64, revision=0, head_sha256="0" * 64)
    value = parse(payload, build=build)
    assert value.inherited.anchor.member.reincarnation == proof
    assert value.inherited.original_origin.subject_incarnation != value.configuration.subject_incarnation
    assert "reincarnation" not in value.model_dump()


def test_build_origin_cannot_hide_oversized_installation_behind_smaller_current_limit():
    payload = successor_payload(operation="capacity")
    payload["installation_projection"] = dict(payload["installation_projection"], max_slots=99)
    with pytest.raises(ValueError):
        parse(payload)


def test_destroy_origin_preserves_unused_input_maximum_without_granting_capacity():
    payload = successor_payload(operation="destroy")
    payload["base_projection"] = dict(payload["base_projection"], max_slots=99)
    value = parse(payload)
    assert value.configuration.max_slots == 0
    assert value.configuration.lifecycle_state == "disabled"
    assert value.base_projection.max_slots == 99
