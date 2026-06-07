"""Property: pure-simulation DRF fairness.

We model the scheduler's preference function `in_flight / weight` and assert
that while every team still has queued work, the picked team is always
one with minimum weighted in-flight (definition of DRF) AND after running
a bounded number of claims (so no team's queue drains), the
weighted-in-flight counts stay within 1 of each other.

The 'no team drains' bound matters: once a team's queue is empty, the
algorithm rightly funnels remaining picks to its peers, and weighted-share
parity is no longer reachable in finite steps."""

from hypothesis import given, settings
from hypothesis import strategies as st


def _drf_pick(teams: dict[str, dict[str, float]]) -> str | None:
    candidates = {tid: t for tid, t in teams.items() if t["queued"] > 0}
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda tid: candidates[tid]["in_flight"] / candidates[tid]["weight"],
    )


@given(
    weights=st.lists(
        st.floats(min_value=0.5, max_value=4.0, allow_nan=False),
        min_size=2, max_size=5,
    ),
    workload=st.integers(min_value=50, max_value=200),
    iterations=st.integers(min_value=10, max_value=40),
)
@settings(max_examples=30, deadline=None)
def test_drf_keeps_teams_proportionally_fair(
    weights: list[float], workload: int, iterations: int,
) -> None:
    teams: dict[str, dict[str, float]] = {
        f"t{i}": {
            "queued": float(workload),
            "in_flight": 0.0,
            "weight": w,
        }
        for i, w in enumerate(weights)
    }
    for _ in range(iterations):
        chosen = _drf_pick(teams)
        assert chosen is not None
        # Per-pick invariant: the chosen team's ratio is the minimum.
        chosen_ratio = teams[chosen]["in_flight"] / teams[chosen]["weight"]
        for t in teams.values():
            assert t["in_flight"] / t["weight"] >= chosen_ratio
        teams[chosen]["queued"] -= 1
        teams[chosen]["in_flight"] += 1

    # No team drained (workload >> iterations).
    assert all(t["queued"] > 0 for t in teams.values())
    # Discrete-allocation residual: between the moment the disadvantaged
    # team gets its next +1 and the next time the algorithm rebalances,
    # ratios can drift by up to 1 / min(weight). With weights in
    # [0.5, 4.0], that's at most 2.0.
    ratios = sorted(t["in_flight"] / t["weight"] for t in teams.values())
    bound = 1.0 / min(weights)
    assert ratios[-1] - ratios[0] <= bound + 1e-9


@given(
    weights=st.lists(
        st.floats(min_value=0.5, max_value=4.0, allow_nan=False),
        min_size=2, max_size=4,
    ),
)
@settings(max_examples=20, deadline=None)
def test_drf_returns_none_when_all_queues_empty(weights: list[float]) -> None:
    teams: dict[str, dict[str, float]] = {
        f"t{i}": {"queued": 0.0, "in_flight": 0.0, "weight": w}
        for i, w in enumerate(weights)
    }
    assert _drf_pick(teams) is None
