"""Typed control-owned commands cannot borrow feature-source/application authority."""

from importlib import import_module
import json
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import ExecutionAuthorityV2, canonical_executable_digest
from loom_capacity_manager.membership_contracts import PersonalApplicationMembershipMutationV1
from tests.unit.test_capacity_build_membership import build_membership_input
from tests.capacity_fixtures import development_projection


def typed_build_mutation():
    module = import_module("loom_capacity_manager.typed_membership_commands")
    value = build_membership_input()
    prep = value.preparation
    member = value.membership.members[-1]
    config = member.configuration
    execution = ExecutionAuthorityV2(
        authority_incarnation=prep.authority_incarnation, writer_epoch=prep.expected_writer_epoch,
        configuration_epoch=prep.configuration_epoch, execution_epoch=42,
        execution_manifest_sha256=canonical_executable_digest(prep), execution_state="active",
        executable_new_capacity_ceiling=prep.requested_ceiling,
        executable_new_capacity_rate_per_minute=prep.requested_rate_per_minute,
        trusted_fleet_release_sha256=prep.trusted_fleet_release_sha256,
    )
    projection = module.PersonalBuildProjectionV1(
        owner_id=member.owner_id, subject_incarnation=config.subject_incarnation,
        operation_kind="create", operation_id=UUID(int=777), operation_epoch=1,
        configuration_generation=config.configuration_generation, candidate_generation=config.candidate_generation,
        deployment_generation=config.deployment_generation,
        demand_reporter_incarnation=config.demand_reporter_incarnation,
        demand_reporter_token_sha256="f" * 64, max_slots=2,
    )
    request = module.PersonalMembershipMutationV2(
        execution=execution, namespace_id=value.membership.namespace_id, expected_revision=1,
        command=module.PersonalBuildCommandV2(projection=projection, acknowledgement=member.acknowledgement),
    )
    return module, value, request


def test_build_command_derives_its_service_runtime_and_owner_configuration():
    module, value, request = typed_build_mutation()
    member = module.derive_build_member(request, value.preparation, value.fleet)
    assert member == value.membership.members[-1]
    assert member.acknowledgement.candidate == value.preparation.personal_builds.runtime_candidate
    assert member.configuration.account_id == f"dev-owner-{request.command.projection.owner_id.hex}"
    assert module.parse_typed_membership_mutation(canonical_bytes(request)) == request
    with pytest.raises(ValueError):
        PersonalApplicationMembershipMutationV1.model_validate_json(request.model_dump_json())


@pytest.mark.parametrize("field,value", (
    ("subject_id", str(UUID(int=990))), ("account_id", "independent-build-budget"), ("environment_name", "bob"),
    ("candidate_sha256", "a" * 64), ("candidate_publication_sha256", "b" * 64),
    ("min_slots", 0), ("rollout_surge_slots", 0), ("expected_configuration_epoch", 1),
))
def test_build_projection_has_no_caller_selected_authority_fields(field, value):
    module, _value, request = typed_build_mutation()
    payload = request.command.projection.model_dump(mode="json") | {field: value}
    import json
    with pytest.raises(ValueError):
        module.PersonalBuildProjectionV1.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("changes", (
    {"owner_id": UUID(int=0)}, {"subject_incarnation": UUID(int=0)}, {"operation_id": UUID(int=0)},
    {"demand_reporter_incarnation": UUID(int=0)}, {"demand_reporter_token_sha256": "0" * 64},
    {"operation_epoch": 0}, {"operation_epoch": 2}, {"configuration_generation": 0},
    {"candidate_generation": 0}, {"deployment_generation": 0},
))
def test_build_projection_rejects_invalid_or_inconsistent_service_generations(changes):
    module, _value, request = typed_build_mutation()
    changed = request.command.projection.model_copy(update=changes)
    with pytest.raises(ValueError):
        module.PersonalBuildProjectionV1.model_validate_json(changed.model_dump_json())


@pytest.mark.parametrize("boundary", ("subject", "incarnation", "configuration", "deployment", "reporter", "candidate", "publication"))
def test_build_derivation_rejects_substituted_acknowledgement(boundary):
    module, value, request = typed_build_mutation()
    ack = request.command.acknowledgement
    if boundary in {"candidate", "publication"}:
        field = "identity" if boundary == "candidate" else "publication_sha256"
        ack = ack.model_copy(update={"candidate": ack.candidate.model_copy(update={field: "e" * (40 if field == "identity" else 64)})})
    else:
        field = {"subject": "subject_id", "incarnation": "subject_incarnation", "configuration": "configuration_generation", "deployment": "deployment_generation", "reporter": "reporter_incarnation"}[boundary]
        ack = ack.model_copy(update={field: 99 if boundary in {"configuration", "deployment"} else UUID(int=999)})
    changed = request.model_copy(update={"command": request.command.model_copy(update={"acknowledgement": ack})})
    with pytest.raises(ValueError):
        module.derive_build_member(changed, value.preparation, value.fleet)


@pytest.mark.parametrize("boundary", ("namespace", "manifest", "configuration", "release", "owner_maximum"))
def test_build_derivation_remains_bound_to_preparation_and_fleet(boundary):
    module, value, request = typed_build_mutation()
    if boundary == "namespace":
        request = request.model_copy(update={"namespace_id": UUID(int=888)})
    elif boundary == "owner_maximum":
        request = request.model_copy(update={"command": request.command.model_copy(update={"projection": request.command.projection.model_copy(update={"max_slots": 3})})})
    else:
        field, changed = {"manifest": ("execution_manifest_sha256", "e" * 64), "configuration": ("configuration_epoch", 2), "release": ("trusted_fleet_release_sha256", "e" * 64)}[boundary]
        request = request.model_copy(update={"execution": request.execution.model_copy(update={field: changed})})
    with pytest.raises(ValueError):
        module.derive_build_member(request, value.preparation, value.fleet)


def test_destroy_derivation_disables_capacity_without_changing_runtime_identity():
    module, value, request = typed_build_mutation()
    projection = request.command.projection.model_copy(update={"operation_kind": "destroy", "max_slots": 0})
    request = request.model_copy(update={"command": request.command.model_copy(update={"projection": projection})})
    member = module.derive_build_member(request, value.preparation, value.fleet)
    assert member.configuration.lifecycle_state == "disabled"
    assert member.configuration.min_slots == member.configuration.max_slots == 0
    assert member.acknowledgement == request.command.acknowledgement


def test_typed_result_binds_member_revision_and_full_build_derivation():
    module, value, request = typed_build_mutation()
    result = module.PersonalMembershipResultV2(revision=2, head_sha256="e" * 64, member=value.membership.members[-1], replayed=False)
    module.validate_typed_membership_result(request, result, value.preparation, value.fleet)
    assert module.parse_typed_membership_result(canonical_bytes(result)) == result
    for changed in (result.model_copy(update={"revision": 3}), result.model_copy(update={"member": value.membership.members[0]})):
        with pytest.raises(ValueError):
            module.validate_typed_membership_result(request, changed, value.preparation, value.fleet)


def test_build_operation_identity_is_committed_in_the_whole_request():
    _module, _value, request = typed_build_mutation()
    projection = request.command.projection.model_copy(update={"operation_id": UUID(int=778)})
    changed = request.model_copy(update={"command": request.command.model_copy(update={"projection": projection})})
    assert canonical_digest(changed) != canonical_digest(request)


@pytest.mark.parametrize("version", (2.0, "2", True, 1))
def test_typed_request_wire_version_is_exact(version):
    module, _value, request = typed_build_mutation()
    with pytest.raises(ValueError):
        module.parse_typed_membership_mutation(request.model_copy(update={"schema_version": version}).model_dump_json().encode())


def test_application_command_preserves_projection_and_has_one_execution_fence():
    module, value, request = typed_build_mutation()
    member = value.membership.members[0]
    config, ack = member.configuration, member.acknowledgement
    projection = development_projection(
        expected_configuration_epoch=request.execution.configuration_epoch,
        subject_id=config.subject_id, subject_incarnation=config.subject_incarnation,
        owner_id=member.owner_id, environment_name=config.display_name.removeprefix("dev-"),
        candidate_sha256=ack.candidate.identity, candidate_publication_sha256=ack.candidate.publication_sha256,
        protected_admission_sha256=ack.protected_admission_sha256,
        demand_reporter_incarnation=config.demand_reporter_incarnation,
    )
    command = module.PersonalApplicationCommandV2(projection=projection, acknowledgement=ack)
    request = request.model_copy(update={"command": command, "expected_revision": 0})
    assert module.parse_typed_membership_mutation(canonical_bytes(request)) == request
    result = module.PersonalMembershipResultV2(revision=1, head_sha256="e" * 64, member=member, replayed=False)
    module.validate_typed_membership_result(request, result, value.preparation, value.fleet)
    command = command.model_copy(update={"projection": projection.model_copy(update={"expected_configuration_epoch": 2})})
    with pytest.raises(ValueError):
        module.parse_typed_membership_mutation(request.model_copy(update={"command": command}).model_dump_json())


@pytest.mark.parametrize("boundary", ("duplicate", "oversized", "nested_version", "missing_version"))
def test_typed_parser_rejects_ambiguous_or_unbounded_wire(boundary):
    module, _value, request = typed_build_mutation()
    payload = request.model_dump_json()
    if boundary == "duplicate":
        payload = '{"expected_revision":999,' + payload[1:]
    elif boundary == "oversized":
        payload += " " * (8 * 1024 * 1024)
    else:
        value = json.loads(payload)
        if boundary == "nested_version":
            value["command"]["acknowledgement"]["schema_version"] = 2.0
        else:
            del value["schema_version"]
        payload = json.dumps(value)
    with pytest.raises(ValueError):
        module.parse_typed_membership_mutation(payload)
