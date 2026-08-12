"""Executable promotion preserves the allocator's exact global placement."""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.allocator import (
    ExecutableAllocationError,
    ExecutableEpochV2,
    allocate_shadow,
    promote_shadow_epoch,
)
from loom_capacity_manager.contracts import ObservedCommitmentV1, ShadowEpochV1
from loom_capacity_manager.executable_contracts import ExecutionAuthorityV2
from tests.capacity_fixtures import allocator_input, allocator_subject, resource_vector


def execution_authority_fixture(
    *,
    configuration_epoch: int = 1,
    execution_state: str = "active",
) -> ExecutionAuthorityV2:
    return ExecutionAuthorityV2(
        authority_incarnation=UUID(int=1),
        writer_epoch=2,
        configuration_epoch=configuration_epoch,
        execution_epoch=3,
        execution_manifest_sha256="a" * 64,
        execution_state=execution_state,
        executable_new_capacity_ceiling=1 if execution_state == "active" else 0,
        executable_new_capacity_rate_per_minute=1 if execution_state == "active" else 0,
        trusted_fleet_release_sha256="b" * 64,
    )


def shadow_epoch_fixture() -> ShadowEpochV1:
    subject = allocator_subject(
        1,
        account_id="shared-development",
        pending=(("neutral", ("gb10", "oldlab"), ("cpu",)),),
        min_slots=0,
        max_slots=1,
    )
    return allocate_shadow(allocator_input((subject,), gb10_slots=1, oldlab_slots=1))


def test_allocation_promotion_requires_active_authority() -> None:
    with pytest.raises(ExecutableAllocationError, match="active execution authority"):
        promote_shadow_epoch(shadow_epoch_fixture(), None, allocation_epoch=7)

    with pytest.raises(ExecutableAllocationError, match="active execution authority"):
        promote_shadow_epoch(
            shadow_epoch_fixture(),
            execution_authority_fixture(execution_state="drain-only"),
            allocation_epoch=7,
        )
    with pytest.raises(ValidationError, match="active execution authority"):
        ExecutableEpochV2.from_shadow(
            shadow_epoch_fixture(),
            execution_authority_fixture(execution_state="drain-only"),
            allocation_epoch=7,
        )


def test_allocation_promotion_preserves_exact_placement() -> None:
    shadow = shadow_epoch_fixture()
    result = promote_shadow_epoch(shadow, execution_authority_fixture(), allocation_epoch=7)

    assert isinstance(result, ExecutableEpochV2)
    assert result.allocations == shadow.allocations
    assert result.input_digest == shadow.input_digest
    assert result.execution.allocation_epoch == 7
    assert result.execution.execution_epoch == 3
    assert result.execution.execution_manifest_sha256 == "a" * 64
    assert result.executable_new_capacity_ceiling == 1
    assert result.executable is True


@pytest.mark.parametrize(
    "update, message",
    [
        (
            {
                "execution": execution_authority_fixture(execution_state="drain-only").model_dump(
                    mode="python"
                )
                | {"allocation_epoch": 7}
            },
            "active execution authority",
        ),
        ({"executable_new_capacity_ceiling": 0}, "execution ceiling"),
        ({"executable_new_capacity_rate_per_minute": 0}, "execution rate"),
        (
            {
                "configuration": shadow_epoch_fixture().configuration.model_copy(
                    update={"configuration_epoch": 2}
                )
            },
            "configuration epoch",
        ),
    ],
)
def test_allocation_epoch_rejects_contradictory_execution_bindings(
    update: dict[str, object],
    message: str,
) -> None:
    executable = promote_shadow_epoch(
        shadow_epoch_fixture(),
        execution_authority_fixture(),
        allocation_epoch=7,
    )

    with pytest.raises(ValidationError, match=message):
        ExecutableEpochV2.model_validate(executable.model_dump(mode="python") | update)


def test_allocation_promotion_rejects_a_changed_configuration_epoch() -> None:
    with pytest.raises(ExecutableAllocationError, match="configuration epoch changed"):
        promote_shadow_epoch(
            shadow_epoch_fixture(),
            execution_authority_fixture(configuration_epoch=2),
            allocation_epoch=7,
        )


def test_allocation_promotion_covers_tiers_owners_architectures_and_neutral() -> None:
    subjects = (
        allocator_subject(
            10,
            account_id="production",
            tier_id="production",
            pending=(("production-x86", ("oldlab",), ("cpu",)),),
            min_slots=0,
            max_slots=1,
        ),
        allocator_subject(
            11,
            account_id="staging",
            tier_id="staging",
            pending=(("staging-arm", ("gb10",), ("cpu",)),),
            min_slots=0,
            max_slots=1,
        ),
        allocator_subject(
            12,
            account_id="shared-development",
            pending=(("shared-neutral", ("gb10", "oldlab"), ("cpu",)),),
            min_slots=0,
            max_slots=1,
        ),
        allocator_subject(
            13,
            account_id="dev-alice",
            pending=(("alice-neutral", ("gb10", "oldlab"), ("cpu",)),),
            min_slots=0,
            max_slots=1,
        ),
        allocator_subject(
            14,
            account_id="dev-bob",
            pending=(("bob-neutral", ("gb10", "oldlab"), ("cpu",)),),
            min_slots=0,
            max_slots=1,
        ),
    )
    shadow = allocate_shadow(allocator_input(subjects, gb10_slots=3, oldlab_slots=2))
    executable = promote_shadow_epoch(
        shadow,
        execution_authority_fixture(),
        allocation_epoch=7,
    )
    placements = {
        allowance.attempt_id: allocation.pool_id
        for allocation in executable.allocations
        for allowance in allocation.placement_allowances
    }

    assert executable.allocations == shadow.allocations
    assert placements["production-x86"] == "oldlab"
    assert placements["staging-arm"] == "gb10"
    assert placements == {
        "alice-neutral": "gb10",
        "bob-neutral": "gb10",
        "production-x86": "oldlab",
        "shared-neutral": "oldlab",
        "staging-arm": "gb10",
    }
    assert executable.executable_new_capacity_ceiling == 1
    assert all(subject.configuration.min_slots == 0 for subject in subjects)


def test_allocation_scale_to_zero_preserves_draining_commitments_exactly() -> None:
    subjects = (
        allocator_subject(20, account_id="dev-alice", min_slots=0, max_slots=1),
        allocator_subject(21, account_id="dev-bob", min_slots=0, max_slots=1),
    )
    commitments = tuple(
        ObservedCommitmentV1(
            kind="physical",
            commitment_id=f"prior-{pool_id}",
            physical_identity=f"worker-{pool_id}",
            subject_id=subject.configuration.subject_id,
            subject_incarnation=subject.configuration.subject_incarnation,
            deployment_generation=subject.configuration.deployment_generation,
            pool_id=pool_id,
            pool_generation=1,
            profile_id="one-slot",
            profile_generation=1,
            profile_digest="a" * 64,
            shape_id="one-slot",
            resources=resource_vector(),
            state="live",
            node_ids=(f"{pool_id}-node",),
        )
        for subject, pool_id in zip(subjects, ("gb10", "oldlab"), strict=True)
    )
    shadow = allocate_shadow(
        allocator_input(
            subjects,
            gb10_slots=1,
            oldlab_slots=1,
            observed_commitments=commitments,
        )
    )
    executable = promote_shadow_epoch(
        shadow,
        execution_authority_fixture(),
        allocation_epoch=8,
    )

    assert executable.allocations == shadow.allocations
    assert all(allocation.desired_slots == 0 for allocation in executable.allocations)
    assert sum(allocation.retained_commitment_slots for allocation in executable.allocations) == 2
    assert {
        shape_id
        for allocation in executable.allocations
        for shape_id in allocation.draining_shape_ids
    } == {"prior-gb10", "prior-oldlab"}
    assert executable.hypothetical_launch_rank == ()
