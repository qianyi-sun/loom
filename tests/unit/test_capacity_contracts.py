"""Strict contract tests for the inert global capacity manager."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from loom_capacity_manager.contracts import (
    MAX_QUANTITY,
    CapacityContractError,
    DemandSnapshotV1,
    FleetManifestV1,
    ResourceVectorV1,
    ShadowAllocationV1,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
    checked_add,
)
from tests.capacity_fixtures import (
    DEMAND_REPORTER_ID,
    SUBJECT_ID,
    SUBJECT_INCARNATION,
    fleet_payload,
    resource_vector,
    resource_vector_payload,
    shape,
    subject_configuration,
)


def test_unknown_version_extra_field_float_and_bool_fail_closed() -> None:
    valid = resource_vector_payload()
    for patch in (
        {"schema_version": 2},
        {"unexpected": 1},
        {"cpu_millicores": 1.5},
        {"memory_bytes": True},
    ):
        with pytest.raises(ValidationError):
            ResourceVectorV1.model_validate(valid | patch)


def test_canonical_digest_ignores_mapping_insertion_order() -> None:
    left = ResourceVectorV1.model_validate(
        resource_vector_payload(generic={"fpga": 1, "scratch_bytes": 4096})
    )
    right = ResourceVectorV1.model_validate(
        resource_vector_payload(generic={"scratch_bytes": 4096, "fpga": 1})
    )
    assert canonical_digest(left) == canonical_digest(right)
    assert canonical_bytes(left) == canonical_bytes(right)


def test_checked_add_rejects_uint63_overflow() -> None:
    with pytest.raises(CapacityContractError, match="overflow"):
        checked_add(MAX_QUANTITY, 1)


def test_checked_add_rejects_bool_and_negative_values() -> None:
    for left, right in ((True, 1), (-1, 1), (1, -1)):
        with pytest.raises(CapacityContractError):
            checked_add(left, right)


def test_multi_node_shape_requires_exact_per_node_sum() -> None:
    with pytest.raises(ValidationError, match="node resources"):
        shape(
            concurrency_slots=2,
            total=resource_vector(slots=2, cpu_millicores=4_000),
            per_node=(resource_vector(slots=2, cpu_millicores=3_000),),
        )


def test_user_weight_fields_are_not_part_of_any_contract() -> None:
    with pytest.raises(ValidationError, match="pool_weight"):
        FleetManifestV1.model_validate(fleet_payload() | {"pool_weight": 2})
    allocation = {
        "schema_version": 1,
        "subject_id": SUBJECT_ID,
        "subject_incarnation": SUBJECT_INCARNATION,
        "deployment_generation": 1,
        "pool_id": "gb10",
        "desired_slots": 0,
        "requested_slots": 0,
        "new_allowance_slots": 0,
        "retained_commitment_slots": 0,
        "desired_shapes": (),
        "protected_claim_slots": 0,
        "physical_committed_shape_slots": 0,
        "draining_shape_ids": (),
        "placement_allowances": (),
        "claim_slot_matches": (),
        "matching_witness": None,
        "blockers": (),
        "executable": False,
    }
    with pytest.raises(ValidationError, match="account_weight"):
        ShadowAllocationV1.model_validate(allocation | {"account_weight": 3})


def test_semantically_unordered_collections_have_canonical_order() -> None:
    forward = FleetManifestV1.model_validate(fleet_payload())
    reverse = FleetManifestV1.model_validate(
        fleet_payload(
            pools=list(reversed(fleet_payload()["pools"])),
            account_policies=list(reversed(fleet_payload()["account_policies"])),
        )
    )
    assert tuple(pool.pool_id for pool in reverse.pools) == ("gb10", "oldlab")
    assert canonical_bytes(forward) == canonical_bytes(reverse)


def test_duplicate_stable_id_is_rejected() -> None:
    payload = fleet_payload()
    payload["pools"] = [payload["pools"][0], payload["pools"][0]]
    with pytest.raises(ValidationError, match="duplicate pool_id"):
        FleetManifestV1.model_validate(payload)


def test_subject_min_slots_defaults_to_zero_and_limits_are_finite() -> None:
    subject = subject_configuration().model_dump(mode="python")
    subject.pop("min_slots")
    parsed = type(subject_configuration()).model_validate(subject)
    assert parsed.min_slots == 0

    subject["min_slots"] = 9
    with pytest.raises(ValidationError, match="min_slots"):
        type(parsed).model_validate(subject)


def test_demand_source_time_normalizes_to_utc_but_is_diagnostic() -> None:
    source_time = datetime(2026, 8, 10, 8, 0, tzinfo=timezone(timedelta(hours=-4)))
    report = DemandSnapshotV1(
        subject_id=SUBJECT_ID,
        subject_incarnation=SUBJECT_INCARNATION,
        configuration_generation=1,
        deployment_generation=1,
        reporter_incarnation=DEMAND_REPORTER_ID,
        sequence=1,
        source_observed_at=source_time,
        pending_unassigned=(),
        current_assignments=(),
        fixed_claims=(),
    )
    assert report.source_observed_at.utcoffset() == timedelta(0)
    assert report.source_observed_at.hour == 12


def test_generic_resource_names_are_canonical() -> None:
    for key in ("GPU", "bad key", "", "a" * 64):
        with pytest.raises(ValidationError, match="generic"):
            ResourceVectorV1.model_validate(resource_vector_payload(generic={key: 1}))


def test_strict_base_model_is_frozen() -> None:
    vector: StrictV1Model = resource_vector()
    with pytest.raises(ValidationError):
        vector.schema_version = 1
