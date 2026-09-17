from __future__ import annotations

from loom_service.routes.monitor import _resource_trials_stmt


def test_resource_trials_stmt_limits_rows_to_active_states() -> None:
    compiled = str(_resource_trials_stmt().compile(compile_kwargs={"literal_binds": True}))

    assert "trials.state IN ('queued', 'protected-pending', 'claimed', 'running')" in compiled


def test_ordinary_user_capacity_exposes_logical_target_without_private_binding() -> None:
    from loom_service.routes.monitor import _public_execution_capacity

    target = {
        "target_id": "nebius-eu-north1-integration",
        "pool_id": "nebius-cpu",
        "environment": "development",
        "cluster_id": "private-cluster",
        "provider_id": "private-provider",
        "kubernetes_api_server": "https://private.invalid",
        "observation": {"active_nodes": 0, "node_group_id": "private-group"},
    }
    projected = _public_execution_capacity({"targets": [target]}, {}, admin=False)
    assert projected[0]["target_id"] == "nebius-eu-north1-integration"
    assert projected[0]["environment"] == "development"
    assert not {"cluster_id", "provider_id", "kubernetes_api_server"} & projected[0].keys()
    observation = projected[0]["observation"]
    assert isinstance(observation, dict)
    assert observation["active_nodes"] == 0
    assert "node_group_id" not in observation


def test_node_activity_distinguishes_occupancy_from_capacity_and_drain_intent() -> None:
    from loom_control_plane.execution_capacity import observed_node_activity
    from loom_service.routes.monitor import _public_execution_capacity

    payload = {"placement": {"nodes": [
        {"uid": "private-node-1", "managed_pods": [{"uid": "private-pod"}], "draining": True},
        {"uid": "private-node-2", "managed_pods": [], "draining": False},
        {"uid": "private-node-3", "managed_pods": [], "draining": False},
    ]}}
    activity = observed_node_activity(payload)
    assert activity == {"occupied_nodes": 1, "draining_nodes": 1}
    public = _public_execution_capacity({"targets": [{
        "observation": {"active_nodes": 9, **activity},
    }]}, {}, admin=False)[0]["observation"]
    assert public["active_nodes"] == 9
    assert public["occupied_nodes"] == 1
    assert public["draining_nodes"] == 1
    assert "private-node" not in str(public)


def test_node_activity_preserves_unknown_for_older_or_missing_observations() -> None:
    from loom_control_plane.execution_capacity import observed_node_activity

    assert observed_node_activity({}) == {"occupied_nodes": None, "draining_nodes": None}
    assert observed_node_activity({"placement": {"nodes": [
        {"managed_pods": [], "deleting": False},
    ]}}) == {"occupied_nodes": 0, "draining_nodes": None}
    assert observed_node_activity({"placement": {"nodes": []}}) == {
        "occupied_nodes": 0, "draining_nodes": 0,
    }
