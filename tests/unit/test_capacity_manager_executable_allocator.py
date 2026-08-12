"""Executable promotion preserves the allocator's exact global placement."""

from __future__ import annotations

from uuid import UUID

import pytest

from loom_capacity_manager.allocator import (
    ExecutableAllocationError,
    ExecutableEpochV2,
    allocate_shadow,
    promote_shadow_epoch,
)
from loom_capacity_manager.contracts import ShadowEpochV1
from loom_capacity_manager.executable_contracts import ExecutionAuthorityV2
from tests.capacity_fixtures import allocator_input, allocator_subject


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


def test_promotion_requires_active_authority() -> None:
    with pytest.raises(ExecutableAllocationError, match="active execution authority"):
        promote_shadow_epoch(shadow_epoch_fixture(), None, allocation_epoch=7)

    with pytest.raises(ExecutableAllocationError, match="active execution authority"):
        promote_shadow_epoch(
            shadow_epoch_fixture(),
            execution_authority_fixture(execution_state="drain-only"),
            allocation_epoch=7,
        )


def test_promotion_preserves_exact_placement() -> None:
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


def test_promotion_rejects_a_changed_configuration_epoch() -> None:
    with pytest.raises(ExecutableAllocationError, match="configuration epoch changed"):
        promote_shadow_epoch(
            shadow_epoch_fixture(),
            execution_authority_fixture(configuration_epoch=2),
            allocation_epoch=7,
        )


def test_global_cohort_promotion_covers_tiers_owners_architectures_and_neutral() -> None:
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
    assert set(placements) == {
        "production-x86",
        "staging-arm",
        "shared-neutral",
        "alice-neutral",
        "bob-neutral",
    }
    assert executable.executable_new_capacity_ceiling == 1
    assert all(subject.configuration.min_slots == 0 for subject in subjects)


def test_scale_to_zero_promotes_the_exact_empty_desired_plan() -> None:
    subjects = (
        allocator_subject(20, account_id="dev-alice", min_slots=0, max_slots=1),
        allocator_subject(21, account_id="dev-bob", min_slots=0, max_slots=1),
    )
    shadow = allocate_shadow(allocator_input(subjects, gb10_slots=1, oldlab_slots=1))
    executable = promote_shadow_epoch(
        shadow,
        execution_authority_fixture(),
        allocation_epoch=8,
    )

    assert executable.allocations == shadow.allocations
    assert all(allocation.desired_slots == 0 for allocation in executable.allocations)
    assert executable.hypothetical_launch_rank == ()


for _allocation_test in (
    test_promotion_requires_active_authority,
    test_promotion_preserves_exact_placement,
    test_promotion_rejects_a_changed_configuration_epoch,
    test_global_cohort_promotion_covers_tiers_owners_architectures_and_neutral,
    test_scale_to_zero_promotes_the_exact_empty_desired_plan,
):
    _allocation_test.allocation = True  # type: ignore[attr-defined]
