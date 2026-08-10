"""Deterministic, bounded physical topology packing tests."""

from __future__ import annotations

import pytest

from loom_capacity_manager.contracts import ObservedCommitmentV1, canonical_bytes
from loom_capacity_manager.topology import (
    SearchBudget,
    TopologyInfeasible,
    TopologySearchLimit,
    pack_topology,
)
from tests.capacity_fixtures import (
    SUBJECT_ID,
    SUBJECT_INCARNATION,
    fragmented_request,
    node,
    packing_request,
    request_with_old_generation_commitment_over_limit,
    resource_vector,
    shape,
)


def test_aggregate_resources_do_not_hide_per_node_infeasibility() -> None:
    wide = shape(
        "wide",
        total=resource_vector(slots=1, cpu_millicores=12, memory_bytes=8),
        per_node=(resource_vector(slots=1, cpu_millicores=12, memory_bytes=8),),
        compatible_domain_ids=("gb10-arm",),
    )
    request = packing_request(
        nodes=(
            node("a", cpu=8, memory=16, slots=1),
            node("b", cpu=8, memory=16, slots=1),
        ),
        shapes=(wide,),
    )
    with pytest.raises(TopologyInfeasible):
        pack_topology(request)


def test_same_input_has_byte_identical_witness_across_orderings() -> None:
    first = pack_topology(fragmented_request(reverse=False))
    second = pack_topology(fragmented_request(reverse=True))
    assert canonical_bytes(first) == canonical_bytes(second)


def test_old_commitment_above_new_envelope_is_charged_over_limit() -> None:
    witness = pack_topology(request_with_old_generation_commitment_over_limit())
    assert witness.over_limit_slots == 2
    assert witness.new_placement_allowed is False
    assert witness.charged_commitment_ids == ("old-worker-a",)


def test_commitment_without_unique_node_identity_reserves_entire_pool() -> None:
    commitment = ObservedCommitmentV1(
        commitment_id="unknown-node-worker",
        physical_identity="worker-unknown",
        subject_id=SUBJECT_ID,
        subject_incarnation=SUBJECT_INCARNATION,
        deployment_generation=1,
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        shape_id="one-slot",
        resources=resource_vector(),
        state="unknown",
        node_ids=(),
    )

    witness = pack_topology(
        packing_request(
            nodes=(node("node-a"), node("node-b")),
            fixed_commitments=(commitment,),
        )
    )

    assert witness.new_placement_allowed is False
    assert witness.blockers == ("fixed_commitment_mapping_ambiguous",)
    assert all(residual.residual.slots == 0 for residual in witness.residuals)


def test_shape_is_placed_only_in_compatible_domain() -> None:
    pinned = shape(
        "pinned",
        compatible_domain_ids=("gb10-arm",),
    )
    witness = pack_topology(packing_request(shapes=(pinned,)))
    assert witness.placements[0].domain_id == "gb10-arm"


def test_shape_domain_constraints_are_enforced() -> None:
    constrained = shape(
        "fabric-pinned",
        compatible_domain_ids=("gb10-arm",),
        placement_constraints={"fabric": "nvlink"},
    )
    with pytest.raises(TopologyInfeasible):
        pack_topology(packing_request(shapes=(constrained,)))


def test_multi_node_shape_uses_distinct_nodes() -> None:
    part = resource_vector(slots=1, cpu_millicores=2, memory_bytes=2)
    multi_node = shape(
        "two-node",
        concurrency_slots=2,
        total=resource_vector(slots=2, cpu_millicores=4, memory_bytes=4),
        per_node=(part, part),
        compatible_domain_ids=("gb10-arm",),
    )

    witness = pack_topology(
        packing_request(
            nodes=(
                node("node-a", cpu=2, memory=2, slots=1),
                node("node-b", cpu=2, memory=2, slots=1),
            ),
            shapes=(multi_node,),
        )
    )

    assert witness.placements[0].node_ids == ("node-a", "node-b")


def test_state_limit_never_returns_partial_witness() -> None:
    with pytest.raises(TopologySearchLimit, match="state"):
        pack_topology(fragmented_request(), budget=SearchBudget(max_states=0))


def test_deadline_never_returns_partial_witness() -> None:
    ticks = iter((0.0, 2.0, 2.0, 2.0))
    with pytest.raises(TopologySearchLimit, match="deadline"):
        pack_topology(
            fragmented_request(),
            budget=SearchBudget(deadline_seconds=1.0),
            monotonic=lambda: next(ticks, 2.0),
        )


@pytest.mark.parametrize("deadline", [float("nan"), float("inf")])
def test_search_budget_requires_finite_deadline(deadline: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        SearchBudget(deadline_seconds=deadline)
