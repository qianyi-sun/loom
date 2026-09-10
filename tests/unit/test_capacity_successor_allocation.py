"""Imported services stay in the common owner budget and native pool allocator."""

import json

import pytest

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.build_membership_contracts import (
    DelegatedAllocationInputV3,
    ExecutionPreparationV4,
    PersonalMembershipSnapshotV2,
)
from loom_capacity_manager.contracts import ConfigurationGenerationRefV1, canonical_digest
from loom_capacity_manager.membership import resolved_subject_references
from loom_capacity_manager.typed_membership_commands import parse_typed_membership_result
from tests.unit.test_capacity_build_membership import build_membership_input
from tests.unit.test_capacity_successor_preparation_origins import preparation_payload
from tests.unit.test_capacity_typed_successor_history import successor_row


def successor_allocation(*, preparation=None, result=None):
    value = build_membership_input()
    preparation = preparation or ExecutionPreparationV4.model_validate_json(json.dumps(preparation_payload()))
    bases = tuple(origin.configuration for origin in (*preparation.managed_application_origins, *preparation.managed_build_origins))
    configs = {subject.subject_id: subject for subject in bases}
    if result is not None:
        configs[result.member.configuration.subject_id] = result.member.configuration
    subjects = []
    for original in value.subjects:
        configuration = configs[original.configuration.subject_id]
        demand = original.last_demand.model_copy(update={"subject_incarnation": configuration.subject_incarnation,
            "configuration_generation": configuration.configuration_generation,
            "deployment_generation": configuration.deployment_generation,
            "reporter_incarnation": configuration.demand_reporter_incarnation})
        subjects.append(original.model_copy(update={"configuration": configuration, "last_demand": demand,
            "freshness": original.freshness.model_copy(update={"last_payload_digest": canonical_digest(demand)})}))
    references = tuple(ConfigurationGenerationRefV1(scope="subject", subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation, generation=subject.configuration_generation,
        digest=canonical_digest(subject)) for subject in bases)
    snapshot = PersonalMembershipSnapshotV2(namespace_id=preparation.personal_membership.namespace_id,
        revision=0 if result is None else result.revision,
        head_sha256="0" * 64 if result is None else result.head_sha256,
        members=() if result is None else (result.member,))
    return DelegatedAllocationInputV3.model_validate(value.model_dump(mode="python") | {
        "preparation": preparation, "managed_base_subjects": bases, "membership": snapshot,
        "configuration": value.configuration.model_copy(update={"configuration_epoch": preparation.configuration_epoch, "subjects": references}),
        "subjects": tuple(subjects)})


def test_empty_successor_keeps_native_build_routing_without_local_member_events():
    value = successor_allocation()
    assert not value.membership.members
    references = resolved_subject_references(value)
    assert {reference.subject_id for reference in references} == {subject.subject_id for subject in value.managed_base_subjects}
    result = allocate_shadow(value)
    build_id = value.preparation.managed_build_origins[0].configuration.subject_id
    placements = {allowance.attempt_id: allocation.pool_id for allocation in result.allocations
        if allocation.subject_id == build_id for allowance in allocation.placement_allowances}
    assert placements == {"build-arm": "gb10", "build-amd": "oldlab"}


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
def test_successor_allocation_resolves_typed_first_mutation(build, operation):
    preparation, _, row = successor_row(build=build, operation=operation)
    result = parse_typed_membership_result(json.dumps(row.result_payload))
    value = successor_allocation(preparation=preparation, result=result)
    refs = resolved_subject_references(value)
    actual = next(ref for ref in refs if ref.subject_id == result.member.configuration.subject_id)
    assert actual.digest == canonical_digest(result.member.configuration)
    assert len(refs) == len(value.managed_base_subjects)
    allocate_shadow(value)


def test_inherited_build_payload_cannot_change_purpose_profile():
    value = successor_allocation()
    build = value.preparation.managed_build_origins[0].configuration
    changed = build.model_copy(update={"profiles": value.fleet.development_subject_template.profiles})
    value = value.model_copy(update={"managed_base_subjects": tuple(changed if subject.subject_id == build.subject_id else subject
        for subject in value.managed_base_subjects)})
    with pytest.raises(ValueError, match="origin|immutable"):
        resolved_subject_references(value)
