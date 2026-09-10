"""New member versions preserve cross-epoch proof without widening legacy values."""

import json
from importlib import import_module
from uuid import UUID

import pytest
from pydantic import TypeAdapter
from pydantic_core import PydanticSerializationError

from loom_capacity_manager.build_membership_contracts import PersonalMembershipSnapshotV2
from loom_capacity_manager.build_value_contracts import PersonalBuildMemberV1
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.typed_membership_commands import (
    PersonalMembershipResultV2,
    parse_typed_membership_result,
)
from tests.unit.test_capacity_inherited_reincarnation import inherited_evidence_payload, parse
from tests.unit.test_capacity_successor_member_origins import successor_payload


def member_payload(*, build):
    origin = successor_payload(build=build, operation="destroy")
    evidence = parse(inherited_evidence_payload(build=build))
    member = origin["inherited"]["anchor"]["member"]
    member.update(schema_version=2, revision=1, reincarnation=evidence.model_dump(mode="json"))
    member["configuration"].update(subject_incarnation=str(evidence.successor_incarnation), configuration_generation=3,
        demand_reporter_incarnation=str(UUID(int=99703)), lifecycle_state="active", max_slots=2)
    member["acknowledgement"].update(subject_incarnation=str(evidence.successor_incarnation), configuration_generation=3,
        reporter_incarnation=str(UUID(int=99703)))
    return member


def new_member(*, build):
    module = import_module("loom_capacity_manager.build_value_contracts")
    model = module.PersonalBuildMemberV2 if build else module.PersonalApplicationMemberV2
    return model.model_validate_json(json.dumps(member_payload(build=build)))


@pytest.mark.parametrize("build", (False, True))
def test_new_member_roundtrip_keeps_complete_inherited_evidence(build):
    member = new_member(build=build)
    result = PersonalMembershipResultV2(revision=1, head_sha256="f" * 64, member=member, replayed=False)
    restored = parse_typed_membership_result(result.model_dump_json())
    assert type(restored.member) is type(member)
    assert restored.member.model_dump(mode="json") == member_payload(build=build)
    snapshot = PersonalMembershipSnapshotV2(namespace_id=member.reincarnation.namespace_id,
        revision=1, head_sha256="f" * 64, members=(member,))
    assert type(PersonalMembershipSnapshotV2.model_validate_json(snapshot.model_dump_json()).members[0]) is type(member)


@pytest.mark.parametrize("build", (False, True))
def test_old_member_rejects_new_instance_and_serialization(build):
    member = new_member(build=build)
    model = PersonalBuildMemberV1 if build else PersonalApplicationMemberV1
    with pytest.raises(ValueError):
        model.model_validate(member)
    with pytest.raises(ValueError):
        model.model_validate_json(member.model_dump_json())
    with pytest.raises(PydanticSerializationError):
        TypeAdapter(model).dump_json(member)


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("boundary", ("missing", "null", "local", "float"))
def test_new_carrier_requires_its_explicit_evidence_version(build, boundary):
    member = new_member(build=build)
    payload = member.model_dump(mode="json")
    if boundary == "missing":
        del payload["reincarnation"]
    elif boundary == "null":
        payload["reincarnation"] = None
    elif boundary == "float":
        payload["schema_version"] = 2.0
    else:
        payload["reincarnation"]["schema_version"] = 1
    with pytest.raises(ValueError):
        type(member).model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("build", (False, True))
def test_unconnected_inherited_recreation_cannot_enter_allocation(build):
    from loom_capacity_manager.executable_contracts import canonical_executable_digest
    from loom_capacity_manager.membership import resolved_subject_references
    from tests.unit.test_capacity_successor_allocation import successor_allocation
    value = successor_allocation()
    member = new_member(build=build)
    member = member.model_copy(update={"reincarnation": member.reincarnation.model_copy(update={
        "execution_manifest_sha256": canonical_executable_digest(value.preparation)})})
    result = PersonalMembershipResultV2(revision=1, head_sha256="f" * 64, member=member, replayed=False)
    value = successor_allocation(result=result)
    with pytest.raises(ValueError, match="cross-epoch recreation allocation is not yet connected"):
        resolved_subject_references(value)


@pytest.mark.parametrize("purpose", ({}, [], None, True, "unknown"))
def test_typed_member_discriminator_rejects_malformed_purpose_as_contract_error(purpose):
    member = new_member(build=True)
    result = PersonalMembershipResultV2(revision=1, head_sha256="f" * 64, member=member, replayed=False).model_dump(mode="json")
    result["member"]["purpose"] = purpose
    with pytest.raises(ValueError):
        parse_typed_membership_result(json.dumps(result))


@pytest.mark.parametrize("build", (False, True))
def test_historical_anchor_requires_new_evidence_current_epoch(build):
    from loom_capacity_manager.retired_member_origin_contracts import PersonalMemberEventAnchorV1
    member = new_member(build=build)
    payload = dict(execution_epoch=member.reincarnation.execution_epoch,
        execution_manifest_sha256=member.reincarnation.execution_manifest_sha256,
        revision=1, head_sha256="f" * 64, member=member)
    anchor = PersonalMemberEventAnchorV1(**payload)
    assert anchor.member == member
    payload["execution_epoch"] += 1
    with pytest.raises(ValueError, match="another event epoch"):
        PersonalMemberEventAnchorV1(**payload)


@pytest.mark.parametrize("build", (False, True))
def test_unconnected_evidence_in_inherited_base_cannot_enter_empty_allocation(build):
    from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
    from loom_capacity_manager.membership import resolved_subject_references
    from tests.unit.test_capacity_successor_allocation import successor_allocation
    from tests.unit.test_capacity_successor_preparation_origins import preparation_payload
    payload = preparation_payload()
    member = new_member(build=build)
    source = dict(payload["retired_source"], execution_epoch=member.reincarnation.execution_epoch,
        execution_manifest_sha256=member.reincarnation.execution_manifest_sha256, revision=1, head_sha256="f" * 64)
    payload["retired_source"] = source
    payload["configuration_epoch"] += 1
    for origin in (*payload["managed_application_origins"], *payload["managed_build_origins"]):
        if "inherited" in origin:
            origin["inherited"]["source"] = source
    origin = payload["managed_build_origins"][0] if build else payload["managed_application_origins"][-1]
    origin["configuration"] = member.configuration.model_dump(mode="json")
    origin["acknowledgement"] = member.acknowledgement.model_dump(mode="json")
    projection = dict(origin["installation_projection"], operation_kind="create", operation_id=str(UUID(int=99705)),
        subject_incarnation=str(member.configuration.subject_incarnation), configuration_generation=3, operation_epoch=3,
        demand_reporter_incarnation=str(member.configuration.demand_reporter_incarnation), demand_reporter_token_sha256="9" * 64)
    origin["base_projection"] = origin["installation_projection"] = projection
    origin["inherited"]["anchor"].update(execution_epoch=source["execution_epoch"],
        execution_manifest_sha256=source["execution_manifest_sha256"], revision=1, head_sha256="f" * 64,
        member=member.model_dump(mode="json"))
    payload["subject_acknowledgements"] = [origin["acknowledgement"] if ack["subject_id"] == origin["configuration"]["subject_id"] else ack
        for ack in payload["subject_acknowledgements"]]
    preparation = ExecutionPreparationV4.model_validate_json(json.dumps(payload))
    value = successor_allocation(preparation=preparation)
    assert not value.membership.members
    with pytest.raises(ValueError, match="cross-epoch recreation allocation is not yet connected"):
        resolved_subject_references(value)
