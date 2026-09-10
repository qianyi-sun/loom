"""Successor events adopt real inherited services without synthetic local events."""

import json
from uuid import UUID

import pytest

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import CandidateBindingV2, canonical_executable_digest
from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.typed_membership_commands import (
    derive_application_member,
    derive_build_member,
    parse_typed_membership_mutation,
    parse_typed_membership_result,
)
from loom_capacity_manager.typed_membership_events import validate_typed_membership_event_prefix
from tests.unit.test_capacity_successor_preparation_origins import preparation_payload
from tests.unit.test_capacity_successor_member_origins import successor_payload
from tests.unit.test_capacity_typed_membership_events import event_row


def successor_row(*, build, operation="capacity", previous=None, reincarnation=None, **changes):
    payload = preparation_payload()
    for purpose, field in ((False, "managed_application_origins"), (True, "managed_build_origins")):
        inherited = successor_payload(build=purpose, operation="capacity")
        inherited["inherited"]["source"] = payload["retired_source"]
        payload[field][-1] = inherited
        payload["subject_acknowledgements"] = [inherited["acknowledgement"]
            if ack["subject_id"] == inherited["configuration"]["subject_id"] else ack
            for ack in payload["subject_acknowledgements"]]
    preparation = ExecutionPreparationV4.model_validate_json(json.dumps(payload))
    base = preparation.managed_build_origins[0] if build else preparation.managed_application_origins[-1]
    revision = 1 if previous is None else previous.revision + 1
    value, request, result, row = event_row(build=build, revision=revision,
        previous="0" * 64 if previous is None else previous.head_sha256,
        operation_id=UUID(int=96000 + revision), key=UUID(int=97000 + revision))
    old = base.base_projection if previous is None else parse_typed_membership_mutation(json.dumps(previous.request_payload)).command.projection
    ack = base.acknowledgement if previous is None else parse_typed_membership_result(json.dumps(previous.result_payload)).member.acknowledgement
    fields = dict(operation_kind=operation, operation_id=row.operation_id,
        configuration_generation=old.configuration_generation + 1, operation_epoch=old.operation_epoch + 1)
    if not build:
        fields["expected_configuration_epoch"] = preparation.configuration_epoch
    if operation == "update":
        fields.update(candidate_generation=old.candidate_generation + 1,
            deployment_generation=old.deployment_generation + 1,
            demand_reporter_incarnation=UUID(int=98000 + revision), demand_reporter_token_sha256=f"{99000 + revision:064x}")
    projection = old.model_copy(update=fields | changes)
    ack_fields = dict(configuration_generation=projection.configuration_generation,
        deployment_generation=projection.deployment_generation, subject_incarnation=projection.subject_incarnation,
        reporter_incarnation=projection.demand_reporter_incarnation)
    if not build:
        ack_fields["candidate"] = CandidateBindingV2(algorithm="source-sha256", identity=projection.candidate_sha256,
            publication_sha256=projection.candidate_publication_sha256)
    ack = ack.model_copy(update=ack_fields)
    execution = request.execution.model_copy(update={"execution_epoch": 43,
        "configuration_epoch": preparation.configuration_epoch,
        "execution_manifest_sha256": canonical_executable_digest(preparation)})
    request = request.model_copy(update={"execution": execution,
        "command": request.command.model_copy(update={"projection": projection, "acknowledgement": ack})})
    derive = derive_build_member if build else derive_application_member
    member = derive(request, preparation, value.fleet, reincarnation=reincarnation)
    row.execution_epoch, row.execution_manifest_sha256 = 43, execution.execution_manifest_sha256
    row.subject_id, row.subject_incarnation = member.configuration.subject_id, projection.subject_incarnation
    row.owner_id, row.reporter_incarnation = projection.owner_id, projection.demand_reporter_incarnation
    row.configuration_generation, row.deployment_generation = projection.configuration_generation, projection.deployment_generation
    row.request_payload, row.request_digest = request.model_dump(mode="json"), canonical_digest(request)
    row.head_sha256 = canonical_membership_event_head(actor=row.actor, execution_epoch=43,
        idempotency_key=row.idempotency_key, operation_id=row.operation_id, previous_sha256=row.previous_sha256,
        request_digest=row.request_digest, request_payload=row.request_payload, member=member, revision=revision)
    row.result_payload = result.model_copy(update={"head_sha256": row.head_sha256, "member": member}).model_dump(mode="json")
    return preparation, value.fleet, row


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
def test_inherited_first_event_adopts_existing_service(build, operation):
    preparation, fleet, row = successor_row(build=build, operation=operation)
    result, = validate_typed_membership_event_prefix((row,), preparation, fleet, execution_epoch=43)
    assert result.revision == 1
    assert result.member.reincarnation is None
    assert result.member.configuration.lifecycle_state == ("disabled" if operation == "destroy" else "active")


@pytest.mark.parametrize("build", (False, True))
def test_inherited_service_local_recreation_uses_original_root(build):
    preparation, fleet, first = successor_row(build=build, operation="destroy")
    origin = preparation.managed_build_origins[0] if build else preparation.managed_application_origins[-1]
    predecessor = parse_typed_membership_result(json.dumps(first.result_payload)).member.configuration
    evidence = PersonalReincarnationEvidenceV1(namespace_id=first.namespace_id,
        execution_manifest_sha256=first.execution_manifest_sha256, origin=origin.inherited.original_origin,
        predecessor=predecessor, predecessor_revision=first.revision, predecessor_head_sha256=first.head_sha256,
        admission_revision=2, successor_incarnation=UUID(int=99500), release_set_sha256="f" * 64)
    _, _, second = successor_row(build=build, operation="create", previous=first, reincarnation=evidence,
        subject_incarnation=evidence.successor_incarnation, candidate_generation=1, deployment_generation=1,
        demand_reporter_incarnation=UUID(int=99501), demand_reporter_token_sha256="9" * 64)
    results = validate_typed_membership_event_prefix((first, second), preparation, fleet, execution_epoch=43)
    assert results[-1].member.reincarnation.origin == origin.inherited.original_origin


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("changes", (
    {"demand_reporter_token_sha256": "9" * 64}, {"demand_reporter_incarnation": UUID(int=99901)},
    {"candidate_generation": 8, "deployment_generation": 8},
    {"subject_incarnation": UUID(int=99902)},
))
def test_inherited_capacity_cannot_replace_service_evidence(build, changes):
    preparation, fleet, row = successor_row(build=build, **changes)
    with pytest.raises(ValueError):
        validate_typed_membership_event_prefix((row,), preparation, fleet, execution_epoch=43)
