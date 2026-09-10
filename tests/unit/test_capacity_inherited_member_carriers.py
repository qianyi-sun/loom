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
from loom_capacity_manager.typed_membership_commands import PersonalMembershipResultV2, parse_typed_membership_result
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
