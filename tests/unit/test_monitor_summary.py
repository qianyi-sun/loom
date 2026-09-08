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
