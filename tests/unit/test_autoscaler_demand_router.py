from __future__ import annotations

from loom_control_plane.autoscaler_demand_router import (
    PoolDemandState,
    choose_neutral_pool,
    choose_neutral_pool_selection,
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
    selection = choose_neutral_pool_selection(states)
    assert selection is not None
    assert selection.reason == "fresh_executable_capacity"


def test_neutral_router_labels_configured_headroom_without_fresh_capacity() -> None:
    states = (
        PoolDemandState(
            pool_name="gb10",
            enabled=True,
            max_slots=150,
            active_slots=100,
            occupied_slots=20,
            pending_slots=0,
            assigned_queued_slots=0,
            capacity_is_fresh=False,
        ),
        PoolDemandState(
            pool_name="oldlab",
            enabled=True,
            max_slots=18,
            active_slots=0,
            occupied_slots=0,
            pending_slots=0,
            assigned_queued_slots=0,
            capacity_is_fresh=False,
        ),
    )
    selection = choose_neutral_pool_selection(states)
    assert selection is not None
    assert selection.pool_name == "oldlab"
    assert selection.reason == "configured_scale_headroom"


def test_operator_weight_changes_only_eligible_pool_order() -> None:
    states = (
        PoolDemandState(
            pool_name="gb10",
            enabled=True,
            max_slots=150,
            active_slots=10,
            occupied_slots=0,
            pending_slots=0,
            assigned_queued_slots=0,
            operator_weight=0,
        ),
        PoolDemandState(
            pool_name="oldlab",
            enabled=True,
            max_slots=18,
            active_slots=1,
            occupied_slots=0,
            pending_slots=0,
            assigned_queued_slots=0,
            operator_weight=10,
        ),
    )
    assert choose_neutral_pool(states) == "oldlab"

    blocked = (
        states[0],
        PoolDemandState(
            **{
                **states[1].__dict__,
                "blocked_reason": "maintenance",
            }
        ),
    )
    assert choose_neutral_pool(blocked) == "gb10"


def test_cost_orders_equal_candidates_and_budget_block_cannot_be_overridden() -> None:
    states = (
        PoolDemandState(
            pool_name="gb10",
            enabled=True,
            max_slots=10,
            active_slots=2,
            occupied_slots=0,
            pending_slots=0,
            assigned_queued_slots=0,
            estimated_cost_microusd_per_slot_hour=50,
        ),
        PoolDemandState(
            pool_name="oldlab",
            enabled=True,
            max_slots=10,
            active_slots=2,
            occupied_slots=0,
            pending_slots=0,
            assigned_queued_slots=0,
            estimated_cost_microusd_per_slot_hour=25,
        ),
    )
    assert choose_neutral_pool(states) == "oldlab"
    assert (
        choose_neutral_pool(
            (
                states[0],
                PoolDemandState(**{**states[1].__dict__, "budget_eligible": False}),
            )
        )
        == "gb10"
    )
