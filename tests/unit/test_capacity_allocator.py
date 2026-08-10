"""Examples for the pure hierarchical global shadow allocator."""

from __future__ import annotations

from collections import defaultdict
from uuid import UUID

import pytest

from loom_capacity_manager.allocator import (
    AllocatorSearchBounds,
    ShadowAllocatorError,
    allocate_shadow,
)
from loom_capacity_manager.contracts import (
    FairnessCursorV1,
    FixedClaimV1,
    ObservedCommitmentV1,
    ShadowEpochV1,
    canonical_bytes,
)
from tests.capacity_fixtures import allocator_input, allocator_subject, resource_vector


def _pending(prefix: str, count: int) -> tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...]:
    return tuple((f"{prefix}-{index}", ("gb10", "oldlab"), ("cpu",)) for index in range(count))


def _subject_slots(result: ShadowEpochV1, subject_id: UUID) -> int:
    return sum(
        allocation.desired_slots
        for allocation in result.allocations
        if allocation.subject_id == subject_id
    )


def _account_slots(result: ShadowEpochV1, account_by_subject: dict[UUID, str]) -> dict[str, int]:
    totals: defaultdict[str, int] = defaultdict(int)
    for allocation in result.allocations:
        totals[account_by_subject[allocation.subject_id]] += allocation.desired_slots
    return dict(totals)


def _allowance_pools(result: ShadowEpochV1) -> dict[str, str]:
    return {
        allowance.attempt_id: allocation.pool_id
        for allocation in result.allocations
        for allowance in allocation.placement_allowances
    }


def test_environment_splitting_does_not_multiply_owner_share() -> None:
    subjects = (
        allocator_subject(1, account_id="owner-a", pending=_pending("a1", 8)),
        allocator_subject(2, account_id="owner-a", pending=_pending("a2", 8)),
        allocator_subject(3, account_id="owner-a", pending=_pending("a3", 8)),
        allocator_subject(4, account_id="owner-b", pending=_pending("b1", 8)),
    )
    result = allocate_shadow(allocator_input(subjects, gb10_slots=4, oldlab_slots=4))
    accounts = _account_slots(
        result,
        {
            subject.configuration.subject_id: subject.configuration.account_id
            for subject in subjects
        },
    )

    assert accounts == {"owner-a": 4, "owner-b": 4}


def test_progressive_fairness_counts_already_delivered_service() -> None:
    owner_a = allocator_subject(
        5,
        account_id="owner-a",
        assigned=tuple((f"assigned-a-{index}", "gb10") for index in range(4)),
        pending=tuple((f"pending-a-{index}", ("gb10",), ("cpu",)) for index in range(4)),
        max_slots=8,
    )
    owner_b = allocator_subject(
        6,
        account_id="owner-b",
        pending=tuple((f"pending-b-{index}", ("gb10",), ("cpu",)) for index in range(8)),
        max_slots=8,
    )
    config = owner_a.configuration
    accepted = tuple(
        ObservedCommitmentV1(
            kind="physical",
            commitment_id=f"accepted-a-{index}",
            physical_identity=f"accepted-worker-a-{index}",
            subject_id=config.subject_id,
            subject_incarnation=config.subject_incarnation,
            deployment_generation=config.deployment_generation,
            pool_id="gb10",
            pool_generation=1,
            profile_id="one-slot",
            profile_generation=1,
            profile_digest="a" * 64,
            shape_id="one-slot",
            resources=resource_vector(),
            state="accepted",
            node_ids=("gb10-node",),
        )
        for index in range(4)
    )
    result = allocate_shadow(
        allocator_input(
            (owner_a, owner_b),
            gb10_slots=8,
            oldlab_slots=0,
            observed_commitments=accepted,
        )
    )
    accounts = _account_slots(
        result,
        {
            owner_a.configuration.subject_id: "owner-a",
            owner_b.configuration.subject_id: "owner-b",
        },
    )

    assert accounts == {"owner-a": 4, "owner-b": 4}


def test_assigned_attempt_is_not_counted_twice() -> None:
    subject = allocator_subject(
        10,
        account_id="owner-a",
        assigned=(("attempt-assigned", "gb10"),),
        pending=(("attempt-pending", ("gb10",), ("cpu",)),),
        max_slots=2,
    )
    result = allocate_shadow(allocator_input((subject,), gb10_slots=2, oldlab_slots=0))
    allocations = tuple(
        item for item in result.allocations if item.subject_id == subject.configuration.subject_id
    )

    assert sum(item.requested_slots for item in allocations) == 2
    assert sum(item.new_allowance_slots for item in allocations) == 1
    assert {
        allowance.attempt_id for item in allocations for allowance in item.placement_allowances
    } == {"attempt-pending"}


def test_claim_and_backing_worker_consume_one_physical_shape() -> None:
    claim = FixedClaimV1(
        claim_id="claim-a",
        attempt_id="claimed-attempt",
        worker_identity="worker-a",
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        deployment_generation=1,
        concurrency_slots=1,
        resources=resource_vector(),
        state="live",
    )
    subject = allocator_subject(
        15,
        account_id="owner-a",
        assigned=(("claimed-attempt", "gb10"),),
        fixed_claims=(claim,),
        max_slots=2,
    )
    config = subject.configuration
    commitment = ObservedCommitmentV1(
        kind="physical",
        commitment_id="worker-a-observed",
        physical_identity="worker-a",
        subject_id=config.subject_id,
        subject_incarnation=config.subject_incarnation,
        deployment_generation=config.deployment_generation,
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        resources=resource_vector(),
        state="observed",
        node_ids=("gb10-node",),
    )
    retained_claim = ObservedCommitmentV1(
        kind="claim",
        commitment_id=claim.claim_id,
        physical_identity=claim.worker_identity,
        attempt_id=claim.attempt_id,
        concurrency_slots=claim.concurrency_slots,
        subject_id=config.subject_id,
        subject_incarnation=config.subject_incarnation,
        deployment_generation=config.deployment_generation,
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        resources=resource_vector(),
        state="observed",
    )
    result = allocate_shadow(
        allocator_input(
            (subject,),
            gb10_slots=1,
            oldlab_slots=0,
            observed_commitments=(retained_claim, commitment),
        )
    )
    gb10 = next(
        item
        for item in result.allocations
        if item.subject_id == config.subject_id and item.pool_id == "gb10"
    )

    assert gb10.protected_claim_slots == 1
    assert gb10.physical_committed_shape_slots == 1
    assert len(gb10.claim_slot_matches) == 1
    assert (
        sum(
            item.requested_slots
            for item in result.allocations
            if item.subject_id == config.subject_id
        )
        == 1
    )
    assert "current_assignment_not_preserved" not in gb10.blockers
    assert result.hypothetical_launch_rank == ()


def test_unmatched_claim_reserves_capacity_and_freezes_pool_increase() -> None:
    claim = FixedClaimV1(
        claim_id="claim-unknown",
        attempt_id="claimed-attempt",
        worker_identity="worker-unknown",
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        deployment_generation=1,
        concurrency_slots=1,
        resources=resource_vector(),
        state="unknown",
    )
    subject = allocator_subject(
        16,
        account_id="owner-a",
        fixed_claims=(claim,),
        pending=(("pending-a", ("gb10",), ("cpu",)),),
        max_slots=2,
    )
    result = allocate_shadow(allocator_input((subject,), gb10_slots=2, oldlab_slots=0))
    gb10 = next(
        item
        for item in result.allocations
        if item.subject_id == subject.configuration.subject_id and item.pool_id == "gb10"
    )

    assert gb10.retained_commitment_slots >= 1
    assert gb10.physical_committed_shape_slots == 0
    assert gb10.new_allowance_slots == 0
    assert "claim_binding_quarantined" in gb10.blockers


def test_quarantined_physical_evidence_freezes_only_its_pool() -> None:
    subject = allocator_subject(
        17,
        account_id="owner-a",
        pending=(
            ("gb10-task", ("gb10",), ("cpu",)),
            ("oldlab-task", ("oldlab",), ("cpu",)),
        ),
        max_slots=2,
    )
    config = subject.configuration
    quarantined = ObservedCommitmentV1(
        kind="physical",
        commitment_id="quarantined-worker",
        physical_identity="quarantined-worker",
        subject_id=config.subject_id,
        subject_incarnation=config.subject_incarnation,
        deployment_generation=config.deployment_generation,
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        resources=resource_vector(),
        state="quarantined",
        node_ids=("gb10-node",),
    )
    result = allocate_shadow(
        allocator_input(
            (subject,),
            gb10_slots=2,
            oldlab_slots=1,
            observed_commitments=(quarantined,),
        )
    )

    assert _allowance_pools(result) == {"oldlab-task": "oldlab"}
    gb10 = next(
        item
        for item in result.allocations
        if item.subject_id == config.subject_id and item.pool_id == "gb10"
    )
    assert "commitment_state_quarantined" in gb10.blockers


def test_commitment_for_unknown_pool_fails_the_complete_allocation() -> None:
    subject = allocator_subject(18, account_id="owner-a")
    config = subject.configuration
    unknown = ObservedCommitmentV1(
        kind="physical",
        commitment_id="retired-pool-worker",
        physical_identity="retired-pool-worker",
        subject_id=config.subject_id,
        subject_incarnation=config.subject_incarnation,
        deployment_generation=config.deployment_generation,
        pool_id="retired-pool",
        pool_generation=1,
        profile_id="retired-profile",
        profile_generation=1,
        profile_digest="d" * 64,
        shape_id="retired-shape",
        resources=resource_vector(),
        state="observed",
    )

    with pytest.raises(ShadowAllocatorError, match="unknown pool"):
        allocate_shadow(
            allocator_input(
                (subject,),
                gb10_slots=1,
                oldlab_slots=1,
                observed_commitments=(unknown,),
            )
        )


def test_higher_tier_reclaims_only_compatible_resource_domain() -> None:
    production = allocator_subject(
        20,
        account_id="prod-service",
        tier_id="production",
        pending=(("prod-x86", ("oldlab",), ("cpu",)),),
        max_slots=1,
    )
    development = allocator_subject(
        21,
        account_id="owner-a",
        pending=(("dev-arm", ("gb10",), ("cpu",)),),
        max_slots=1,
    )
    result = allocate_shadow(
        allocator_input((development, production), gb10_slots=1, oldlab_slots=1)
    )

    assert _allowance_pools(result) == {
        "prod-x86": "oldlab",
        "dev-arm": "gb10",
    }


def test_constrained_demand_is_placed_before_neutral_demand() -> None:
    subject = allocator_subject(
        30,
        account_id="owner-a",
        pending=(
            ("attempt-neutral", ("gb10", "oldlab"), ("cpu",)),
            ("attempt-x86", ("oldlab",), ("cpu",)),
        ),
        max_slots=2,
    )
    result = allocate_shadow(allocator_input((subject,), gb10_slots=1, oldlab_slots=1))

    assert _allowance_pools(result) == {
        "attempt-x86": "oldlab",
        "attempt-neutral": "gb10",
    }


def test_odd_residual_slot_rotates_between_equal_owner_accounts() -> None:
    subjects = (
        allocator_subject(40, account_id="owner-a", pending=_pending("a", 3)),
        allocator_subject(41, account_id="owner-b", pending=_pending("b", 3)),
    )
    first = allocate_shadow(
        allocator_input(
            subjects,
            gb10_slots=3,
            oldlab_slots=0,
            fairness_cursors=(
                FairnessCursorV1(
                    tier_id="development",
                    phase="demand",
                    account_id="owner-a",
                ),
            ),
        )
    )
    next_cursor = next(
        cursor
        for cursor in first.next_fairness_cursors
        if cursor.tier_id == "development"
        and cursor.phase == "demand"
        and cursor.subject_id is None
    )
    second = allocate_shadow(
        allocator_input(
            subjects,
            gb10_slots=3,
            oldlab_slots=0,
            fairness_cursors=(next_cursor,),
        )
    )
    subject_accounts = {
        subject.configuration.subject_id: subject.configuration.account_id for subject in subjects
    }

    assert _account_slots(first, subject_accounts)["owner-a"] == 2
    assert _account_slots(second, subject_accounts)["owner-b"] == 2


def test_odd_residual_slot_rotates_between_one_owners_subjects() -> None:
    subjects = (
        allocator_subject(42, account_id="owner-a", pending=_pending("a1", 2)),
        allocator_subject(43, account_id="owner-a", pending=_pending("a2", 2)),
    )
    first = allocate_shadow(
        allocator_input(
            subjects,
            gb10_slots=1,
            oldlab_slots=0,
            fairness_cursors=(
                FairnessCursorV1(
                    tier_id="development",
                    phase="demand",
                    account_id="owner-a",
                    subject_id=subjects[1].configuration.subject_id,
                ),
            ),
        )
    )
    subject_cursor = next(
        cursor
        for cursor in first.next_fairness_cursors
        if cursor.tier_id == "development"
        and cursor.phase == "demand"
        and cursor.subject_id is not None
    )
    second = allocate_shadow(
        allocator_input(
            subjects,
            gb10_slots=1,
            oldlab_slots=0,
            fairness_cursors=first.next_fairness_cursors,
        )
    )

    assert _subject_slots(first, subjects[1].configuration.subject_id) == 1
    assert subject_cursor.subject_id == subjects[0].configuration.subject_id
    assert _subject_slots(second, subjects[0].configuration.subject_id) == 1


def test_subject_cursor_does_not_change_the_tier_account_cursor() -> None:
    subjects = (
        allocator_subject(44, account_id="owner-a", pending=_pending("a", 1)),
        allocator_subject(45, account_id="owner-b", pending=_pending("b", 1)),
    )
    result = allocate_shadow(
        allocator_input(
            subjects,
            gb10_slots=1,
            oldlab_slots=0,
            fairness_cursors=(
                FairnessCursorV1(
                    tier_id="development",
                    phase="demand",
                    account_id="owner-b",
                    subject_id=subjects[1].configuration.subject_id,
                ),
            ),
        )
    )

    assert _subject_slots(result, subjects[0].configuration.subject_id) == 1
    assert _subject_slots(result, subjects[1].configuration.subject_id) == 0


def test_pending_job_ceiling_blocks_only_hypothetical_increase() -> None:
    subject = allocator_subject(
        50,
        account_id="owner-a",
        pending=(("attempt-a", ("gb10",), ("cpu",)),),
        max_slots=1,
    )
    result = allocate_shadow(
        allocator_input(
            (subject,),
            gb10_slots=1,
            oldlab_slots=0,
            global_pending_slots=0,
            global_pending_jobs=0,
        )
    )

    assert _subject_slots(result, subject.configuration.subject_id) == 1
    assert result.hypothetical_launch_rank == ()
    assert "global_pending_job_ceiling" in result.blockers


def test_warm_minimum_uses_approved_shape_with_best_normalized_headroom() -> None:
    subject = allocator_subject(
        55,
        account_id="owner-a",
        min_slots=1,
        max_slots=1,
    )
    result = allocate_shadow(allocator_input((subject,), gb10_slots=1, oldlab_slots=2))
    desired = {
        allocation.pool_id: allocation.desired_slots
        for allocation in result.allocations
        if allocation.subject_id == subject.configuration.subject_id
    }

    assert desired == {"gb10": 0, "oldlab": 1}


def test_rollout_surge_is_backed_by_distinct_old_generation_capacity() -> None:
    claim = FixedClaimV1(
        claim_id="old-claim",
        attempt_id="old-attempt",
        worker_identity="old-worker",
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        deployment_generation=1,
        concurrency_slots=1,
        resources=resource_vector(),
        state="live",
    )
    subject = allocator_subject(
        56,
        account_id="owner-a",
        fixed_claims=(claim,),
        max_slots=1,
        rollout_surge_slots=1,
        deployment_generation=2,
    )
    config = subject.configuration
    old_worker = ObservedCommitmentV1(
        kind="physical",
        commitment_id="old-worker-observed",
        physical_identity="old-worker",
        subject_id=config.subject_id,
        subject_incarnation=config.subject_incarnation,
        deployment_generation=1,
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        resources=resource_vector(),
        state="live",
        node_ids=("gb10-node",),
    )
    result = allocate_shadow(
        allocator_input(
            (subject,),
            gb10_slots=2,
            oldlab_slots=0,
            observed_commitments=(old_worker,),
        )
    )
    gb10 = next(
        item
        for item in result.allocations
        if item.subject_id == config.subject_id and item.pool_id == "gb10"
    )

    assert len(gb10.surge_pairings) == 1
    assert gb10.surge_pairings[0].old_commitment_id == "old-worker-observed"
    assert gb10.surge_pairings[0].backed_slots == 1
    assert "old-worker-observed" in gb10.draining_shape_ids


def test_one_multislot_old_worker_can_back_multiple_new_shapes() -> None:
    claims = tuple(
        FixedClaimV1(
            claim_id=f"old-multislot-claim-{index}",
            attempt_id=f"old-multislot-attempt-{index}",
            worker_identity="old-multislot-worker",
            pool_id="gb10",
            pool_generation=1,
            profile_id="legacy-two-slot",
            profile_generation=1,
            profile_digest="c" * 64,
            shape_id="legacy-two-slot",
            deployment_generation=1,
            concurrency_slots=2,
            resources=resource_vector(slots=2),
            state="live",
        )
        for index in range(2)
    )
    subject = allocator_subject(
        57,
        account_id="owner-a",
        fixed_claims=claims,
        max_slots=2,
        rollout_surge_slots=2,
        deployment_generation=2,
    )
    config = subject.configuration
    old_worker = ObservedCommitmentV1(
        kind="physical",
        commitment_id="old-multislot-worker-observed",
        physical_identity="old-multislot-worker",
        subject_id=config.subject_id,
        subject_incarnation=config.subject_incarnation,
        deployment_generation=1,
        pool_id="gb10",
        pool_generation=1,
        profile_id="legacy-two-slot",
        profile_generation=1,
        profile_digest="c" * 64,
        shape_id="legacy-two-slot",
        resources=resource_vector(slots=2),
        state="live",
        node_ids=("gb10-node",),
    )
    result = allocate_shadow(
        allocator_input(
            (subject,),
            gb10_slots=4,
            oldlab_slots=0,
            observed_commitments=(old_worker,),
        )
    )
    gb10 = next(
        item
        for item in result.allocations
        if item.subject_id == config.subject_id and item.pool_id == "gb10"
    )

    assert len(gb10.surge_pairings) == 2
    assert {pairing.old_commitment_id for pairing in gb10.surge_pairings} == {
        "old-multislot-worker-observed"
    }
    assert sum(pairing.backed_slots for pairing in gb10.surge_pairings) == 2


def test_stale_subject_retains_scope_but_valid_subject_can_grow() -> None:
    stale = allocator_subject(
        60,
        account_id="owner-a",
        pending=_pending("stale", 2),
        freshness="stale",
    )
    valid = allocator_subject(
        61,
        account_id="owner-b",
        pending=_pending("valid", 2),
    )
    result = allocate_shadow(allocator_input((stale, valid), gb10_slots=2, oldlab_slots=0))

    assert _subject_slots(result, stale.configuration.subject_id) == 0
    assert _subject_slots(result, valid.configuration.subject_id) == 2
    stale_allocations = tuple(
        item for item in result.allocations if item.subject_id == stale.configuration.subject_id
    )
    assert all("subject_input_stale" in item.blockers for item in stale_allocations)


def test_shadow_output_is_canonical_and_never_executable() -> None:
    subject = allocator_subject(70, account_id="owner-a", pending=_pending("task", 2))
    value = allocator_input((subject,), gb10_slots=1, oldlab_slots=1)

    first = allocate_shadow(value)
    second = allocate_shadow(value)

    assert canonical_bytes(first) == canonical_bytes(second)
    assert first.executable is False
    assert first.executable_new_capacity_ceiling == 0
    assert {item.pool_id for item in first.pool_witnesses} == {"gb10", "oldlab"}
    assert all(item.executable is False for item in first.allocations)
    assert all(
        item.rate_state == "unavailable_package_1" for item in first.hypothetical_launch_rank
    )


def test_explicit_allocator_work_bounds_fail_closed() -> None:
    subject = allocator_subject(71, account_id="owner-a", pending=_pending("task", 1))
    value = allocator_input((subject,), gb10_slots=1, oldlab_slots=1)

    with pytest.raises(ShadowAllocatorError, match="decision limit"):
        allocate_shadow(
            value,
            bounds=AllocatorSearchBounds(max_allocation_decisions=0),
        )
    with pytest.raises(ShadowAllocatorError, match="topology search state limit"):
        allocate_shadow(
            value,
            bounds=AllocatorSearchBounds(topology_max_states=0),
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_allocation_decisions": True},
        {"topology_max_states": -1},
        {"topology_deadline_seconds": float("inf")},
    ],
)
def test_allocator_work_bounds_reject_noncanonical_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        AllocatorSearchBounds(**kwargs)  # type: ignore[arg-type]
