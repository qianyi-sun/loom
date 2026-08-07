from __future__ import annotations

from loom_control_plane.autoscaler_demand_router import (
    PoolDemandState,
    choose_neutral_pool,
    requires_neutral_pool_assignment,
)


def test_only_explicit_architecture_neutral_demand_requires_assignment() -> None:
    assert requires_neutral_pool_assignment({"cpu_arch": "any"}) is True
    assert (
        requires_neutral_pool_assignment(
            {"cpu_arch": "any", "worker_pool": "oldlab"},
        )
        is False
    )
    assert requires_neutral_pool_assignment({"cpu_arch": "x86_64"}) is False
    assert requires_neutral_pool_assignment({"cpu_arch": "arm64"}) is False
    assert requires_neutral_pool_assignment({}) is False


def test_neutral_router_prefers_existing_free_worker_slot() -> None:
    states = (
        PoolDemandState(
            pool_name="gb10",
            enabled=True,
            max_slots=150,
            active_slots=0,
            occupied_slots=0,
            pending_slots=0,
            assigned_queued_slots=0,
        ),
        PoolDemandState(
            pool_name="oldlab",
            enabled=True,
            max_slots=5,
            active_slots=2,
            occupied_slots=1,
            pending_slots=0,
            assigned_queued_slots=0,
        ),
    )

    assert choose_neutral_pool(states) == "oldlab"
