"""Generated invariants for hierarchical fleet shadow allocation."""

from __future__ import annotations

from collections import defaultdict

from hypothesis import given, settings
from hypothesis import strategies as st

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.contracts import AllocationInputV1, canonical_bytes
from tests.capacity_fixtures import allocator_input, allocator_subject


def _pending(prefix: str, count: int):  # type: ignore[no-untyped-def]
    return tuple((f"{prefix}-{index}", ("gb10", "oldlab"), ("cpu",)) for index in range(count))


@settings(max_examples=40, deadline=None)
@given(
    owner_a_subjects=st.integers(min_value=1, max_value=4),
    owner_b_subjects=st.integers(min_value=1, max_value=4),
    capacity=st.integers(min_value=1, max_value=8),
)
def test_owner_fairness_is_split_safe_and_rotationally_bounded(
    owner_a_subjects: int,
    owner_b_subjects: int,
    capacity: int,
) -> None:
    subjects = tuple(
        allocator_subject(
            index + 1,
            account_id="owner-a",
            pending=_pending(f"a{index}", 8),
        )
        for index in range(owner_a_subjects)
    ) + tuple(
        allocator_subject(
            100 + index,
            account_id="owner-b",
            pending=_pending(f"b{index}", 8),
        )
        for index in range(owner_b_subjects)
    )
    result = allocate_shadow(allocator_input(subjects, gb10_slots=capacity, oldlab_slots=0))
    account_by_subject = {
        item.configuration.subject_id: item.configuration.account_id for item in subjects
    }
    totals: defaultdict[str, int] = defaultdict(int)
    for allocation in result.allocations:
        totals[account_by_subject[allocation.subject_id]] += allocation.desired_slots

    assert sum(totals.values()) == capacity
    assert abs(totals["owner-a"] - totals["owner-b"]) <= 1


@settings(max_examples=30, deadline=None)
@given(
    production_demand=st.integers(min_value=0, max_value=6),
    development_demand=st.integers(min_value=0, max_value=6),
    capacity=st.integers(min_value=0, max_value=6),
)
def test_strict_tier_priority_and_pool_limit(
    production_demand: int,
    development_demand: int,
    capacity: int,
) -> None:
    production = allocator_subject(
        500,
        account_id="production-service",
        tier_id="production",
        pending=tuple((f"prod-{index}", ("gb10",), ("cpu",)) for index in range(production_demand)),
    )
    development = allocator_subject(
        501,
        account_id="owner-a",
        pending=tuple((f"dev-{index}", ("gb10",), ("cpu",)) for index in range(development_demand)),
    )
    result = allocate_shadow(
        allocator_input(
            (development, production),
            gb10_slots=capacity,
            oldlab_slots=0,
        )
    )
    desired = defaultdict(int)
    for allocation in result.allocations:
        desired[allocation.subject_id] += allocation.desired_slots

    expected_production = min(production_demand, capacity)
    assert desired[production.configuration.subject_id] == expected_production
    assert desired[development.configuration.subject_id] == min(
        development_demand,
        capacity - expected_production,
    )
    assert sum(desired.values()) <= capacity


@settings(max_examples=30, deadline=None)
@given(
    subjects_count=st.integers(min_value=1, max_value=5),
    demand_per_subject=st.integers(min_value=0, max_value=4),
    gb10_slots=st.integers(min_value=0, max_value=5),
    oldlab_slots=st.integers(min_value=0, max_value=5),
)
def test_permutation_matching_and_shadow_only_invariants(
    subjects_count: int,
    demand_per_subject: int,
    gb10_slots: int,
    oldlab_slots: int,
) -> None:
    subjects = tuple(
        allocator_subject(
            700 + index,
            account_id=f"owner-{index % 2}",
            pending=_pending(f"task-{index}", demand_per_subject),
        )
        for index in range(subjects_count)
    )
    value = allocator_input(
        subjects,
        gb10_slots=gb10_slots,
        oldlab_slots=oldlab_slots,
    )
    payload = value.model_dump(mode="python")
    payload["subjects"] = tuple(reversed(payload["subjects"]))
    payload["pools"] = tuple(reversed(payload["pools"]))
    permuted = AllocationInputV1.model_validate(payload)

    first = allocate_shadow(value)
    second = allocate_shadow(permuted)

    assert canonical_bytes(first) == canonical_bytes(second)
    allowances = [
        allowance
        for allocation in first.allocations
        for allowance in allocation.placement_allowances
    ]
    assert len({item.attempt_id for item in allowances}) == len(allowances)
    for allocation in first.allocations:
        witness = allocation.matching_witness
        if witness is not None:
            assert witness.matched_slots == len(witness.attempt_ids)
            assert witness.matched_slots == len(set(witness.shape_instance_ids))
            slot_by_attempt = dict(
                zip(
                    witness.attempt_ids,
                    witness.shape_instance_ids,
                    strict=True,
                )
            )
            assert all(
                slot_by_attempt[allowance.attempt_id].startswith(
                    f"{allowance.shape_instance_id}-slot-"
                )
                for allowance in allocation.placement_allowances
            )
    for subject in subjects:
        requested = sum(
            allocation.requested_slots
            for allocation in first.allocations
            if allocation.subject_id == subject.configuration.subject_id
        )
        assert requested == min(
            subject.configuration.max_slots,
            max(
                subject.configuration.min_slots,
                demand_per_subject,
            ),
        )
    assert first.executable is False
    assert first.executable_new_capacity_ceiling == 0
    assert all(item.executable is False for item in first.allocations)
    assert all(
        item.rate_state == "unavailable_package_1" for item in first.hypothetical_launch_rank
    )
