"""Managed V4 bases pin installation origins, not mutable-row self-attestation."""

from importlib import import_module
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.capacity_build_membership_fixtures import application_origin_payload
from tests.unit.test_capacity_membership import OWNER_A, _member, delegated_input_with_new_owner


def origin_payload():
    value = delegated_input_with_new_owner()
    configuration = value.managed_base_subjects[0]
    return application_origin_payload(configuration, _member(configuration, OWNER_A).acknowledgement,
        configuration_epoch=value.configuration.configuration_epoch)


def origin(**changes):
    module = import_module("loom_capacity_manager.application_origin_contracts")
    return module.ManagedApplicationOriginV1.model_validate(origin_payload() | changes)


def parse(value):
    return type(value).model_validate_json(canonical_bytes(value))


def resized_origin(*, cross_epoch=False, destroyed=False):
    value = origin()
    projection = value.base_projection.model_copy(update={
        "operation_kind": "destroy" if destroyed else "capacity", "operation_id": UUID(int=777701),
        "operation_epoch": 2, "configuration_generation": 2, "max_slots": 1,
        "expected_configuration_epoch": value.base_projection.expected_configuration_epoch + int(cross_epoch),
    })
    configuration = value.configuration.model_copy(update={"configuration_generation": 2,
        "max_slots": 0 if destroyed else 1, "lifecycle_state": "disabled" if destroyed else "active"})
    return value.model_copy(update={"base_projection": projection, "configuration": configuration,
        "acknowledgement": value.acknowledgement.model_copy(update={"configuration_generation": 2})})


@pytest.mark.parametrize("cross_epoch,destroyed", ((False, False), (True, False), (True, True)))
def test_managed_origin_keeps_original_installation_across_capacity_epochs(cross_epoch, destroyed):
    value = parse(resized_origin(cross_epoch=cross_epoch, destroyed=destroyed))
    assert value.installation_projection.operation_kind == "create"
    assert value.installation_projection.configuration_generation == 1
    assert value.base_projection.configuration_generation == 2
    assert value.configuration.configuration_generation == 2
    assert "revision" not in value.model_dump()


def test_managed_origin_initial_projection_is_exact():
    value = origin()
    assert canonical_bytes(value.installation_projection) == canonical_bytes(value.base_projection)


@pytest.mark.parametrize("field,changed", (
    ("operation_kind", "capacity"), ("owner_id", UUID(int=777799)),
    ("environment_name", "someone"), ("subject_id", UUID(int=777799)),
    ("subject_incarnation", UUID(int=777799)), ("candidate_sha256", "f" * 64),
    ("candidate_publication_sha256", "f" * 64), ("local_activation_sha256", "f" * 64),
    ("capacity_agent_installation_sha256", "f" * 64), ("protected_admission_sha256", "f" * 64),
    ("demand_reporter_incarnation", UUID(int=777799)), ("demand_reporter_token_sha256", "f" * 64),
    ("protocol_versions", {"capacity-agent": "v1", "claim-guard": "v1", "control-plane-worker": "v1", "extra": "v1"}),
))
def test_managed_origin_rejects_substituted_installation_fields(field, changed):
    value = resized_origin()
    with pytest.raises(ValueError):
        parse(value.model_copy(update={"installation_projection": value.installation_projection.model_copy(update={field: changed})}))


@pytest.mark.parametrize("field,changed", (
    ("subject_id", UUID(int=777799)), ("account_id", "other-owner"),
    ("display_name", "dev-other"), ("lifecycle_state", "disabled"),
    ("configuration_generation", 3), ("candidate_generation", 3), ("deployment_generation", 3),
    ("demand_reporter_incarnation", UUID(int=777799)), ("max_slots", 3),
))
def test_managed_origin_configuration_must_match_complete_projection(field, changed):
    value = origin()
    with pytest.raises(ValueError):
        parse(value.model_copy(update={"configuration": value.configuration.model_copy(update={field: changed})}))


@pytest.mark.parametrize("field,changed", (
    ("configuration_generation", 2), ("reporter_incarnation", UUID(int=777799)),
    ("protected_admission_sha256", "f" * 64),
))
def test_managed_origin_acknowledgement_must_match_base(field, changed):
    value = origin()
    with pytest.raises(ValueError):
        parse(value.model_copy(update={"acknowledgement": value.acknowledgement.model_copy(update={field: changed})}))


@pytest.mark.parametrize("case", ("same-operation", "future-installation", "same-generation", "zero-operation", "zero-source", "zero-token"))
def test_managed_origin_rejects_inconsistent_operation_coordinates(case):
    value = resized_origin(cross_epoch=True)
    installation = value.installation_projection
    base = value.base_projection
    if case == "same-operation":
        base = base.model_copy(update={"operation_id": installation.operation_id})
    elif case == "future-installation":
        installation = installation.model_copy(update={"expected_configuration_epoch": base.expected_configuration_epoch + 1})
    elif case == "same-generation":
        installation = installation.model_copy(update={"operation_epoch": 2, "configuration_generation": 2})
    elif case == "zero-operation":
        installation = installation.model_copy(update={"operation_id": UUID(int=0)})
    else:
        field = "candidate_sha256" if case == "zero-source" else "demand_reporter_token_sha256"
        installation = installation.model_copy(update={field: "0" * 64})
        base = base.model_copy(update={field: "0" * 64})
    with pytest.raises(ValueError):
        parse(value.model_copy(update={"installation_projection": installation, "base_projection": base}))


def test_v4_preparation_pins_whole_installation_not_only_configuration_digest():
    from tests.unit.test_capacity_build_membership import build_membership_input
    value = build_membership_input().preparation
    binding = value.managed_application_origins[0]
    changed_projection = binding.installation_projection.model_copy(update={"operation_id": UUID(int=777799)})
    changed = binding.model_copy(update={"installation_projection": changed_projection, "base_projection": changed_projection})
    other = parse(value.model_copy(update={"managed_application_origins": (changed,)}))
    assert canonical_digest(binding.configuration) == canonical_digest(changed.configuration)
    assert canonical_executable_digest(value) != canonical_executable_digest(other)


@pytest.mark.parametrize("target", ("preparation", "policy"))
@pytest.mark.parametrize("case", ("missing", "duplicate", "foreign-id", "missing-ack", "changed-ack"))
def test_v4_operator_document_requires_exact_origin_and_acknowledgement_coverage(target, case):
    from loom_capacity_manager.build_membership_contracts import ExecutionPreparationPolicyV4
    from tests.capacity_execution_fixtures import execution_policy
    from tests.unit.test_capacity_build_membership import build_membership_input
    value = build_membership_input().preparation
    if target == "policy":
        value = ExecutionPreparationPolicyV4.model_validate(execution_policy().model_dump(mode="python") | {
            "schema_version": 4, "personal_membership": value.personal_membership, "personal_builds": value.personal_builds,
            "managed_application_origins": value.managed_application_origins, "subject_acknowledgements": value.subject_acknowledgements,
        })
    changes = {}
    if case == "missing":
        changes["managed_application_origins"] = ()
    elif case == "duplicate":
        changes["managed_application_origins"] = value.managed_application_origins * 2
    elif case == "foreign-id":
        changes["personal_membership"] = value.personal_membership.model_copy(update={"managed_base_subject_ids": (UUID(int=777799),)})
    elif case == "missing-ack":
        changes["subject_acknowledgements"] = ()
    else:
        changes["subject_acknowledgements"] = (value.subject_acknowledgements[0].model_copy(update={"acknowledgement_sha256": "f" * 64}),)
    with pytest.raises(ValueError):
        parse(value.model_copy(update=changes))


def test_v4_preparation_cannot_import_a_future_projection_epoch():
    from tests.unit.test_capacity_build_membership import build_membership_input
    value = build_membership_input().preparation
    with pytest.raises(ValueError):
        parse(value.model_copy(update={"configuration_epoch": value.managed_application_origins[0].base_projection.expected_configuration_epoch - 1}))


def test_allocator_managed_origin_must_match_immutable_base_configuration():
    from loom_capacity_manager.membership import resolved_subject_references
    from tests.unit.test_capacity_build_membership import build_membership_input
    value = build_membership_input()
    binding = value.preparation.managed_application_origins[0]
    # Internally consistent replacement, but it no longer matches the pinned base.
    projection = binding.base_projection.model_copy(update={"max_slots": 1})
    binding = parse(binding.model_copy(update={"configuration": binding.configuration.model_copy(update={"max_slots": 1}),
        "installation_projection": projection, "base_projection": projection}))
    with pytest.raises(ValueError):
        resolved_subject_references(value.model_copy(update={"preparation": value.preparation.model_copy(update={"managed_application_origins": (binding,)})}))
