from loom.pipeline.policy_config import PolicyConfigRegistry
from loom.pipeline.resource_profiles import load_resource_profiles


def test_acceptance_never_activates_repo_policy_capacity() -> None:
    registry = PolicyConfigRegistry.load(resource_profiles=load_resource_profiles())
    rows = registry.disabled_autoscaler_rows(environment="staging")
    assert rows
    assert all(row["enabled"] is False for row in rows)
    assert all(row["min_slots"] == 0 for row in rows)
    assert all(row["disabled_reason"] == "pipeline_gpu_policy_not_activated" for row in rows)
