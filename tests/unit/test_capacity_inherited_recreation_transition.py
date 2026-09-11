"""Typed recreation binds current admission to the exact imported disabled event."""

import json
from uuid import UUID

import pytest

from loom_capacity_manager.inherited_reincarnation_contracts import (
    PersonalInheritedReincarnationEvidenceV2,
)
from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
from loom_capacity_manager.typed_membership_commands import parse_typed_membership_result
from loom_capacity_manager.typed_membership_events import validate_typed_membership_event_prefix
from tests.unit.test_capacity_typed_successor_history import successor_row


def inherited_create(*, build, evidence_changes=None):
    changes = dict(subject_incarnation=UUID(int=99800), demand_reporter_incarnation=UUID(int=99801), demand_reporter_token_sha256="9" * 64)
    preparation, fleet, first = successor_row(build=build, source_operation="destroy", operation="create", **changes)
    base = preparation.managed_build_origins[0] if build else preparation.managed_application_origins[-1]
    anchor = base.inherited.anchor
    proof = PersonalInheritedReincarnationEvidenceV2(namespace_id=preparation.personal_membership.namespace_id,
        execution_epoch=43, execution_manifest_sha256=first.execution_manifest_sha256,
        source=preparation.retired_source, origin=base.inherited.original_origin,
        predecessor=base.configuration, predecessor_execution_epoch=anchor.execution_epoch,
        predecessor_execution_manifest_sha256=anchor.execution_manifest_sha256,
        predecessor_revision=anchor.revision, predecessor_head_sha256=anchor.head_sha256,
        admission_revision=1, successor_incarnation=changes["subject_incarnation"], release_set_sha256="f" * 64)
    if evidence_changes:
        proof = proof.model_copy(update=evidence_changes)
    preparation, fleet, first = successor_row(build=build, source_operation="destroy", operation="create", reincarnation=proof, **changes)
    return preparation, fleet, first, proof


@pytest.mark.parametrize("build", (False, True))
def test_first_imported_disabled_create_keeps_source_revision_and_certificate(build):
    preparation, fleet, first, proof = inherited_create(build=build)
    result, = validate_typed_membership_event_prefix((first,), preparation, fleet, execution_epoch=43)
    assert result.member.schema_version == 2
    assert result.member.reincarnation.predecessor_revision == 2
    assert result.member.revision == 1
    _, _, second = successor_row(build=build, source_operation="destroy", operation="capacity", previous=first, reincarnation=proof)
    results = validate_typed_membership_event_prefix((first, second), preparation, fleet, execution_epoch=43)
    assert results[-1].member.reincarnation == proof


@pytest.mark.parametrize("build", (False, True))
def test_later_local_recreation_uses_real_local_event_and_v1_certificate(build):
    preparation, fleet, first, proof = inherited_create(build=build)
    _, _, disabled = successor_row(build=build, source_operation="destroy", operation="destroy", previous=first, reincarnation=proof)
    old = parse_typed_membership_result(json.dumps(disabled.result_payload)).member.configuration
    local = PersonalReincarnationEvidenceV1(namespace_id=proof.namespace_id,
        execution_manifest_sha256=proof.execution_manifest_sha256, origin=proof.origin, predecessor=old,
        predecessor_revision=2, predecessor_head_sha256=disabled.head_sha256, admission_revision=3,
        successor_incarnation=UUID(int=99802), release_set_sha256="e" * 64)
    _, _, last = successor_row(build=build, source_operation="destroy", operation="create", previous=disabled, reincarnation=local,
        subject_incarnation=local.successor_incarnation, demand_reporter_incarnation=UUID(int=99803), demand_reporter_token_sha256="a" * 64)
    results = validate_typed_membership_event_prefix((first, disabled, last), preparation, fleet, execution_epoch=43)
    assert [result.member.schema_version for result in results] == [2, 2, 1]
    assert results[-1].member.reincarnation == local


@pytest.mark.parametrize("build", (False, True))
def test_inherited_certificate_current_epoch_is_bound_not_only_manifest(build):
    with pytest.raises(ValueError, match="inherited recreation"):
        inherited_create(build=build, evidence_changes={"execution_epoch": 44})


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("boundary", ("source", "own-head", "own-revision", "root", "predecessor"))
def test_inherited_recreation_matches_exact_pinned_predecessor(build, boundary):
    _, _, _, proof = inherited_create(build=build)
    changes = {
        "source": {"source": proof.source.model_copy(update={"head_sha256": "9" * 64})},
        "own-head": {"predecessor_head_sha256": "9" * 64},
        "own-revision": {"predecessor_revision": 1},
        "root": {"origin": proof.origin.model_copy(update={"digest": "9" * 64})},
        "predecessor": {"predecessor": proof.predecessor.model_copy(update={"configuration_generation": proof.predecessor.configuration_generation + 1})},
    }[boundary]
    changed = proof.model_copy(update=changes)
    # Force structurally valid forgeries to reach the pinned-context comparison.
    PersonalInheritedReincarnationEvidenceV2.model_validate_json(changed.model_dump_json())
    with pytest.raises(ValueError, match="differs from pinned predecessor"):
        inherited_create(build=build, evidence_changes=changes)


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
def test_importing_a_v2_member_keeps_old_certificate_only_in_provenance(build, operation):
    from tests.unit.test_capacity_inherited_member_carriers import inherited_base_preparation
    preparation = inherited_base_preparation(build=build)
    origin = preparation.managed_build_origins[0] if build else preparation.managed_application_origins[-1]
    old_proof = origin.inherited.anchor.member.reincarnation
    preparation, fleet, row = successor_row(build=build, operation=operation, preparation_override=preparation, execution_epoch=44)
    result, = validate_typed_membership_event_prefix((row,), preparation, fleet, execution_epoch=44)
    assert result.member.schema_version == 1 and result.member.reincarnation is None
    assert origin.inherited.anchor.member.reincarnation == old_proof
    assert old_proof.execution_epoch == 43
