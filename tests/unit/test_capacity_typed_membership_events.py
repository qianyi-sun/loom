"""Persisted typed events preserve one exact shared immutable history."""

import copy
import json
from importlib import import_module
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.membership_contracts import PersonalApplicationMembershipMutationV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.models import CapacityPersonalMembershipEvent
from tests.unit.test_capacity_typed_membership_commands import (
    typed_application_mutation,
    typed_build_mutation,
)


def event_row(*, build=True, revision=1, previous="0" * 64, operation_id=None, key=None):
    operation_id = UUID(int=771) if operation_id is None else operation_id
    key = UUID(int=772) if key is None else key
    module, value, request = typed_build_mutation() if build else typed_application_mutation()
    projection = request.command.projection.model_copy(update={"operation_id": operation_id})
    request = request.model_copy(update={"expected_revision": revision - 1, "command": request.command.model_copy(update={"projection": projection})})
    member = value.membership.members[-1 if build else 0].model_copy(update={"revision": revision})
    digest = canonical_digest(request)
    actor = value.preparation.personal_membership.management_principal_id
    head = canonical_membership_event_head(actor=actor, execution_epoch=request.execution.execution_epoch,
        idempotency_key=key, operation_id=operation_id, previous_sha256=previous, request_digest=digest,
        request_payload=request.model_dump(mode="json"), member=member, revision=revision)
    result = module.PersonalMembershipResultV2(revision=revision, head_sha256=head, member=member, replayed=False)
    row = CapacityPersonalMembershipEvent(
        execution_epoch=request.execution.execution_epoch, execution_manifest_sha256=request.execution.execution_manifest_sha256,
        authority_incarnation=request.execution.authority_incarnation, writer_epoch=request.execution.writer_epoch,
        namespace_id=request.namespace_id, revision=revision, previous_sha256=previous, head_sha256=head,
        actor=actor, idempotency_key=key, operation_id=operation_id, request_digest=digest,
        request_payload=request.model_dump(mode="json"), result_payload=result.model_dump(mode="json"),
        subject_id=member.configuration.subject_id, subject_incarnation=projection.subject_incarnation,
        owner_id=projection.owner_id, configuration_generation=projection.configuration_generation,
        deployment_generation=projection.deployment_generation, reporter_incarnation=projection.demand_reporter_incarnation,
    )
    return value, request, result, row


def _events():
    return import_module("loom_capacity_manager.typed_membership_events")


def test_typed_event_rejects_resealed_zero_idempotency_identity():
    value, _request, _result, row = event_row(key=UUID(int=0))
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event(row, value.preparation, value.fleet)


@pytest.mark.parametrize("build", (False, True))
def test_exact_typed_event_row_is_bound_to_its_original_request(build):
    value, request, result, row = event_row(build=build)
    assert _events().validate_typed_membership_event(row, value.preparation, value.fleet) == (request, result)


@pytest.mark.parametrize("field,changed", (
    ("execution_epoch", 900), ("execution_manifest_sha256", "1" * 64),
    ("authority_incarnation", UUID(int=900)), ("writer_epoch", 900), ("namespace_id", UUID(int=900)),
    ("revision", 2), ("previous_sha256", "1" * 64), ("head_sha256", "1" * 64),
    ("actor", "other-delegate"), ("idempotency_key", UUID(int=900)), ("operation_id", UUID(int=900)),
    ("request_digest", "1" * 64), ("subject_id", UUID(int=900)), ("subject_incarnation", UUID(int=900)),
    ("owner_id", UUID(int=900)), ("configuration_generation", 900), ("deployment_generation", 900),
    ("reporter_incarnation", UUID(int=900)),
))
def test_changed_typed_event_column_is_rejected(field, changed):
    value, _request, _result, row = event_row()
    assert getattr(row, field) != changed
    setattr(row, field, changed)
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event(row, value.preparation, value.fleet)


@pytest.mark.parametrize("boundary", ("replayed", "request_digest", "member_purpose", "derived_budget"))
def test_resealed_event_cannot_change_result_semantics(boundary):
    value, _request, _result, row = event_row()
    if boundary == "replayed":
        row.result_payload["replayed"] = True
    elif boundary == "request_digest":
        row.request_payload["command"]["projection"]["operation_id"] = str(UUID(int=999))
    elif boundary == "member_purpose":
        row.result_payload["member"]["purpose"] = "personal-application"
    else:
        row.result_payload["member"]["configuration"]["max_pending_slots"] = 1
    # The verifier must check derived semantics, not only recompute this head.
    import hashlib
    import json
    preimage = dict(actor=row.actor, execution_epoch=row.execution_epoch, idempotency_key=str(row.idempotency_key),
        operation_id=str(row.operation_id), previous_sha256=row.previous_sha256, request_digest=row.request_digest,
        request_payload=row.request_payload, result_member=row.result_payload["member"], revision=row.revision)
    row.head_sha256 = hashlib.sha256(json.dumps(preimage, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    row.result_payload["head_sha256"] = row.head_sha256
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event(row, value.preparation, value.fleet)


def test_application_and_build_events_form_one_shared_revision_prefix():
    value, _request, first, app = event_row(build=False)
    _value, _request, second, build = event_row(revision=2, previous=app.head_sha256, operation_id=UUID(int=773), key=UUID(int=774))
    assert _events().validate_typed_membership_event_prefix(
        (app, build), value.preparation, value.fleet, execution_epoch=42,
    ) == (first, second)
    assert _events().validate_typed_membership_event_prefix((), value.preparation, value.fleet, execution_epoch=42) == ()


@pytest.mark.parametrize("boundary", ("missing", "reordered", "wrong_epoch", "operation_reused", "key_reused"))
def test_shared_prefix_rejects_gaps_and_cross_purpose_replay_identity_reuse(boundary):
    value, _request, _result, app = event_row(build=False)
    _value, _request, _result, build = event_row(revision=2, previous=app.head_sha256,
        operation_id=app.operation_id if boundary == "operation_reused" else UUID(int=773),
        key=app.idempotency_key if boundary == "key_reused" else UUID(int=774))
    rows = (build,) if boundary == "missing" else (build, app) if boundary == "reordered" else (app, build)
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event_prefix(rows, value.preparation, value.fleet, execution_epoch=900 if boundary == "wrong_epoch" else 42)


def test_event_validation_does_not_mutate_persisted_payloads():
    value, _request, _result, row = event_row()
    before = copy.deepcopy((row.request_payload, row.result_payload))
    _events().validate_typed_membership_event(row, value.preparation, value.fleet)
    assert before == (row.request_payload, row.result_payload)


def test_legacy_application_event_hash_preimage_is_unchanged():
    _module, value, request = typed_application_mutation()
    legacy = PersonalApplicationMembershipMutationV1(execution=request.execution, namespace_id=request.namespace_id,
        expected_revision=0, projection=request.command.projection, acknowledgement=request.command.acknowledgement)
    assert canonical_membership_event_head(actor="legacy-delegate", execution_epoch=42,
        idempotency_key=UUID(int=123), operation_id=legacy.projection.operation_id,
        previous_sha256="0" * 64, request_digest=canonical_digest(legacy),
        request_payload=legacy.model_dump(mode="json"), member=value.membership.members[0], revision=1,
    ) == "e089567425f306b26e1853ae7f7780fbec7fdc83c007316edfdf585f9f15f8bf"


def _next_build_row(first, *, operation="capacity", reincarnation=None, **changes):
    from loom_capacity_manager.typed_membership_commands import (
        derive_build_member,
        parse_typed_membership_mutation,
    )
    value, _request, _result, row = event_row(revision=first.revision + 1, previous=first.head_sha256,
        operation_id=UUID(int=780 + first.revision), key=UUID(int=790 + first.revision))
    request = parse_typed_membership_mutation(json.dumps(first.request_payload))
    projection = request.command.projection.model_copy(update={
        "operation_kind": operation, "operation_id": row.operation_id,
        "operation_epoch": first.configuration_generation + 1, "configuration_generation": first.configuration_generation + 1,
        **changes,
    })
    ack = request.command.acknowledgement.model_copy(update={
        "configuration_generation": projection.configuration_generation,
        "deployment_generation": projection.deployment_generation,
        "subject_incarnation": projection.subject_incarnation,
        "reporter_incarnation": projection.demand_reporter_incarnation,
    })
    request = request.model_copy(update={"expected_revision": first.revision, "command": request.command.model_copy(update={"projection": projection, "acknowledgement": ack})})
    member = derive_build_member(request, value.preparation, value.fleet, reincarnation=reincarnation)
    row.configuration_generation = projection.configuration_generation
    row.deployment_generation = projection.deployment_generation
    row.reporter_incarnation = projection.demand_reporter_incarnation
    row.subject_incarnation = projection.subject_incarnation
    row.request_payload = request.model_dump(mode="json")
    row.request_digest = canonical_digest(request)
    row.head_sha256 = canonical_membership_event_head(actor=row.actor, execution_epoch=row.execution_epoch,
        idempotency_key=row.idempotency_key, operation_id=row.operation_id, previous_sha256=row.previous_sha256,
        request_digest=row.request_digest, request_payload=row.request_payload, member=member, revision=row.revision)
    row.result_payload.update(member=member.model_dump(mode="json"), head_sha256=row.head_sha256)
    return row


@pytest.mark.parametrize("operation", ("capacity", "destroy"))
def test_build_capacity_and_destroy_keep_the_original_service_evidence(operation):
    value, _request, _result, first = event_row()
    second = _next_build_row(first, operation=operation)
    results = _events().validate_typed_membership_event_prefix((first, second), value.preparation, value.fleet, execution_epoch=42)
    assert results[-1].member.configuration.lifecycle_state == ("disabled" if operation == "destroy" else "active")


@pytest.mark.parametrize("operation,changes", (
    ("capacity", {"demand_reporter_token_sha256": "a" * 64}),
    ("destroy", {"demand_reporter_token_sha256": "a" * 64}),
    ("capacity", {"deployment_generation": 2}),
    ("destroy", {"candidate_generation": 2}),
    ("capacity", {"demand_reporter_incarnation": UUID(int=999)}),
    ("update", {}),
    ("update", {"deployment_generation": 2}),
    ("update", {"deployment_generation": 2, "demand_reporter_incarnation": UUID(int=999)}),
    ("create", {}),
))
def test_build_history_rejects_lifecycle_and_credential_substitution(operation, changes):
    value, _request, _result, first = event_row()
    second = _next_build_row(first, operation=operation, **changes)
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event_prefix((first, second), value.preparation, value.fleet, execution_epoch=42)


def test_build_service_update_can_rotate_deployment_without_changing_runtime():
    value, _request, _result, first = event_row()
    second = _next_build_row(first, operation="update", deployment_generation=2,
        demand_reporter_incarnation=UUID(int=999), demand_reporter_token_sha256="a" * 64)
    results = _events().validate_typed_membership_event_prefix((first, second), value.preparation, value.fleet, execution_epoch=42)
    assert results[0].member.acknowledgement.candidate == results[1].member.acknowledgement.candidate


@pytest.mark.parametrize("operation", ("capacity", "update", "create"))
def test_disabled_build_requires_fresh_authenticated_recreation(operation):
    value, _request, _result, first = event_row()
    second = _next_build_row(first, operation="destroy")
    third = _next_build_row(second, operation=operation)
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event_prefix((first, second, third), value.preparation, value.fleet, execution_epoch=42)


@pytest.mark.parametrize("wrong_head", (False, True))
def test_build_recreation_structure_binds_its_own_predecessor_event(wrong_head):
    from loom_capacity_manager.contracts import ConfigurationGenerationRefV1, SubjectConfigurationV1
    from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
    value, _request, first_result, first = event_row()
    second = _next_build_row(first, operation="destroy")
    predecessor = SubjectConfigurationV1.model_validate_json(json.dumps(second.result_payload["member"]["configuration"]))
    origin = first_result.member.configuration
    evidence = PersonalReincarnationEvidenceV1(namespace_id=first.namespace_id,
        execution_manifest_sha256=first.execution_manifest_sha256,
        origin=ConfigurationGenerationRefV1(scope="subject", subject_id=origin.subject_id,
            subject_incarnation=origin.subject_incarnation, generation=origin.configuration_generation, digest=canonical_digest(origin)),
        predecessor=predecessor, predecessor_revision=second.revision,
        predecessor_head_sha256="e" * 64 if wrong_head else second.head_sha256,
        admission_revision=3, successor_incarnation=UUID(int=995), release_set_sha256="f" * 64)
    third = _next_build_row(second, operation="create", subject_incarnation=UUID(int=995),
        demand_reporter_incarnation=UUID(int=996), demand_reporter_token_sha256="a" * 64, reincarnation=evidence)
    if wrong_head:
        with pytest.raises(ValueError):
            _events().validate_typed_membership_event_prefix((first, second, third), value.preparation, value.fleet, execution_epoch=42)
    else:
        # This only proves event structure. Actual release facts remain a mandatory store check.
        assert len(_events().validate_typed_membership_event_prefix((first, second, third), value.preparation, value.fleet, execution_epoch=42)) == 3
        fourth = _next_build_row(third, reincarnation=evidence)
        assert len(_events().validate_typed_membership_event_prefix((first, second, third, fourth), value.preparation, value.fleet, execution_epoch=42)) == 4


@pytest.mark.parametrize("changes", (
    {"configuration_generation": 1, "operation_epoch": 1},
    {"subject_incarnation": UUID(int=999)},
))
def test_build_history_rejects_nonmonotonic_or_changed_incarnation(changes):
    value, _request, _result, first = event_row()
    second = _next_build_row(first, **changes)
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event_prefix((first, second), value.preparation, value.fleet, execution_epoch=42)


def test_build_update_cannot_reuse_a_previously_rotated_reporter_token():
    value, _request, _result, first = event_row()
    second = _next_build_row(first, operation="update", deployment_generation=2,
        demand_reporter_incarnation=UUID(int=990), demand_reporter_token_sha256="a" * 64)
    third = _next_build_row(second, operation="update", deployment_generation=3,
        demand_reporter_incarnation=UUID(int=991), demand_reporter_token_sha256="f" * 64)
    with pytest.raises(ValueError):
        _events().validate_typed_membership_event_prefix((first, second, third), value.preparation, value.fleet, execution_epoch=42)
