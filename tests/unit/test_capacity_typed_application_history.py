"""Mixed history authenticates application lifecycle, not just event hashes."""

import json
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import CandidateBindingV2, canonical_executable_digest
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.store import _derive_development_subject
from loom_capacity_manager.typed_membership_commands import parse_typed_membership_mutation
from loom_capacity_manager.typed_membership_events import validate_typed_membership_event_prefix
from tests.unit.test_capacity_typed_membership_events import event_row


def application_row(previous=None, *, operation="create", managed=False, **changes):
    revision = 1 if previous is None else previous.revision + 1
    value, request, result, row = event_row(build=False, revision=revision,
        previous="0" * 64 if previous is None else previous.head_sha256,
        operation_id=UUID(int=81000 + revision), key=UUID(int=82000 + revision))
    if previous is not None:
        request = parse_typed_membership_mutation(json.dumps(previous.request_payload))
    elif managed:
        origin = value.preparation.managed_application_origins[0]
        request = request.model_copy(update={"command": request.command.model_copy(update={
            "projection": origin.base_projection, "acknowledgement": origin.acknowledgement,
        }), "execution": request.execution.model_copy(update={"configuration_epoch": request.execution.configuration_epoch + 1})})
    preparation = value.preparation.model_copy(update={"configuration_epoch": request.execution.configuration_epoch})
    value = value.model_copy(update={"preparation": preparation})
    request = request.model_copy(update={"execution": request.execution.model_copy(update={
        "execution_manifest_sha256": canonical_executable_digest(preparation),
    })})
    row.execution_manifest_sha256 = request.execution.execution_manifest_sha256
    old = request.command.projection
    generation = old.configuration_generation + (previous is not None or managed)
    fields = dict(operation_kind=operation, operation_id=row.operation_id,
        operation_epoch=generation, configuration_generation=generation,
        expected_configuration_epoch=request.execution.configuration_epoch)
    if (previous is not None or managed) and operation == "update":
        fields.update(candidate_generation=old.candidate_generation + 1,
            deployment_generation=old.deployment_generation + 1,
            demand_reporter_incarnation=UUID(int=83000 + revision),
            demand_reporter_token_sha256=f"{84000 + revision:064x}")
    projection = old.model_copy(update=fields | changes)
    ack = request.command.acknowledgement.model_copy(update={
        "subject_id": projection.subject_id, "subject_incarnation": projection.subject_incarnation,
        "configuration_generation": projection.configuration_generation,
        "deployment_generation": projection.deployment_generation,
        "reporter_incarnation": projection.demand_reporter_incarnation,
        "protected_admission_sha256": projection.protected_admission_sha256,
        "candidate": CandidateBindingV2(algorithm="source-sha256", identity=projection.candidate_sha256,
            publication_sha256=projection.candidate_publication_sha256),
    })
    request = request.model_copy(update={"expected_revision": revision - 1,
        "command": request.command.model_copy(update={"projection": projection, "acknowledgement": ack})})
    member = PersonalApplicationMemberV1(revision=revision, owner_id=projection.owner_id,
        configuration=_derive_development_subject(value.fleet, projection), acknowledgement=ack)
    row.subject_id, row.subject_incarnation, row.owner_id = projection.subject_id, projection.subject_incarnation, projection.owner_id
    row.configuration_generation, row.deployment_generation = projection.configuration_generation, projection.deployment_generation
    row.reporter_incarnation = projection.demand_reporter_incarnation
    row.request_payload, row.request_digest = request.model_dump(mode="json"), canonical_digest(request)
    row.head_sha256 = canonical_membership_event_head(actor=row.actor, execution_epoch=row.execution_epoch,
        idempotency_key=row.idempotency_key, operation_id=row.operation_id, previous_sha256=row.previous_sha256,
        request_digest=row.request_digest, request_payload=row.request_payload, member=member, revision=revision)
    row.result_payload = result.model_copy(update={"revision": revision, "head_sha256": row.head_sha256, "member": member}).model_dump(mode="json")
    return value, row


def validate(value, *rows):
    return validate_typed_membership_event_prefix(rows, value.preparation, value.fleet, execution_epoch=42)


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
def test_typed_managed_application_first_event_uses_pinned_base_without_fake_create(operation):
    value, row = application_row(operation=operation, managed=True)
    result, = validate(value, row)
    assert result.revision == 1
    assert result.member.configuration.configuration_generation == 2
    assert result.member.configuration.subject_id == value.preparation.managed_application_origins[0].configuration.subject_id


def test_typed_managed_application_history_adopts_then_rotates_and_disables():
    value, resized = application_row(operation="capacity", managed=True, max_slots=1)
    _, updated = application_row(resized, operation="update")
    _, destroyed = application_row(updated, operation="destroy")
    results = validate(value, resized, updated, destroyed)
    assert len(results) == 3
    assert results[-1].member.configuration.lifecycle_state == "disabled"


@pytest.mark.parametrize("operation,changes", (
    ("create", {}), ("capacity", {"owner_id": UUID(int=85200)}),
    ("capacity", {"subject_incarnation": UUID(int=85201)}),
    ("capacity", {"environment_name": "renamed"}),
    ("capacity", {"configuration_generation": 1, "operation_epoch": 1}),
    ("capacity", {"candidate_sha256": "f" * 64}),
    ("capacity", {"demand_reporter_token_sha256": "f" * 64}),
    ("capacity", {"local_activation_sha256": "f" * 64}),
    ("capacity", {"capacity_agent_installation_sha256": "f" * 64}),
    ("update", {"candidate_generation": 1, "deployment_generation": 1}),
))
def test_typed_managed_application_first_transition_cannot_replace_base_evidence(operation, changes):
    value, row = application_row(operation=operation, managed=True, **changes)
    with pytest.raises(ValueError):
        validate(value, row)


@pytest.mark.parametrize("field", ("demand_reporter_incarnation", "demand_reporter_token_sha256"))
def test_typed_managed_application_update_must_rotate_base_reporter(field):
    value, _ = application_row(operation="update", managed=True)
    base = value.preparation.managed_application_origins[0].base_projection
    value, row = application_row(operation="update", managed=True, **{field: getattr(base, field)})
    with pytest.raises(ValueError):
        validate(value, row)


def test_typed_fresh_application_cannot_reuse_pinned_base_token():
    value, _ = application_row()
    token = value.preparation.managed_application_origins[0].base_projection.demand_reporter_token_sha256
    value, row = application_row(demand_reporter_token_sha256=token)
    with pytest.raises(ValueError):
        validate(value, row)


def test_typed_application_history_create_capacity_update_destroy():
    value, created = application_row()
    _, resized = application_row(created, operation="capacity", max_slots=1)
    _, updated = application_row(resized, operation="update")
    _, destroyed = application_row(updated, operation="destroy")
    results = validate(value, created, resized, updated, destroyed)
    assert len(results) == 4
    assert results[-1].member.configuration.lifecycle_state == "disabled"


@pytest.mark.parametrize("operation", ("update", "capacity", "destroy"))
def test_typed_application_initial_history_requires_create(operation):
    value, row = application_row(operation=operation)
    with pytest.raises(ValueError):
        validate(value, row)


@pytest.mark.parametrize("changes", (
    {"subject_incarnation": UUID(int=85000)}, {"owner_id": UUID(int=85001)},
    {"environment_name": "renamed"}, {"configuration_generation": 1, "operation_epoch": 1},
    {"candidate_generation": 2, "deployment_generation": 2},
    {"demand_reporter_incarnation": UUID(int=85002)}, {"demand_reporter_token_sha256": "1" * 64},
    {"candidate_sha256": "1" * 64}, {"candidate_publication_sha256": "1" * 64},
    {"local_activation_sha256": "1" * 64}, {"protected_admission_sha256": "1" * 64},
    {"capacity_agent_installation_sha256": "1" * 64},
    {"protocol_versions": {"capacity-agent": "v1", "claim-guard": "v1", "control-plane-worker": "v1", "foreign": "v1"}},
))
def test_typed_application_capacity_cannot_replace_service_evidence(changes):
    value, first = application_row()
    _, second = application_row(first, operation="capacity", **changes)
    with pytest.raises(ValueError):
        validate(value, first, second)


@pytest.mark.parametrize("boundary", ("reporter", "token", "generation", "create_again"))
def test_typed_application_update_requires_fresh_deployment_and_reporter(boundary):
    value, first = application_row()
    old = parse_typed_membership_mutation(json.dumps(first.request_payload)).command.projection
    changes = {
        "reporter": {"demand_reporter_incarnation": old.demand_reporter_incarnation},
        "token": {"demand_reporter_token_sha256": old.demand_reporter_token_sha256},
        "generation": {"candidate_generation": 1, "deployment_generation": 1},
        "create_again": {},
    }[boundary]
    _, second = application_row(first, operation="create" if boundary == "create_again" else "update", **changes)
    with pytest.raises(ValueError):
        validate(value, first, second)


@pytest.mark.parametrize("operation", ("create", "update", "capacity", "destroy"))
def test_typed_application_disabled_history_cannot_reactivate_without_release(operation):
    value, first = application_row()
    _, destroyed = application_row(first, operation="destroy")
    _, third = application_row(destroyed, operation=operation)
    with pytest.raises(ValueError):
        validate(value, first, destroyed, third)


@pytest.mark.parametrize("disabled", (False, True))
def test_typed_application_name_cannot_be_reused_by_a_different_subject(disabled):
    value, first = application_row()
    rows = [first]
    if disabled:
        _, destroyed = application_row(first, operation="destroy")
        rows.append(destroyed)
    _, other = application_row(rows[-1], subject_id=UUID(int=85100),
        subject_incarnation=UUID(int=85101), demand_reporter_incarnation=UUID(int=85102),
        demand_reporter_token_sha256="2" * 64)
    with pytest.raises(ValueError):
        validate(value, *rows, other)
