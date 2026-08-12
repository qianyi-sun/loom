from __future__ import annotations

from loom.db.schema import WorkerPoolAutoscalerPolicy
from loom.pipeline.policy_config import PolicyConfigRegistry
from loom.pipeline.resource_profiles import load_resource_profiles
from loom_control_plane.worker_pool_autoscaler import (
    AutoscalerObservation,
    AutoscalerPolicyConfig,
    _queued_pipeline_attempt_matches_policy,
    compute_autoscaler_decision,
)

DIGEST = "sha256:" + "a" * 64


def _row(pool_name: str, cluster_id: str) -> WorkerPoolAutoscalerPolicy:
    return WorkerPoolAutoscalerPolicy(
        environment="test",
        pool_name=pool_name,
        actuator="slurm",
        enabled=True,
        min_slots=0,
        max_slots=1,
        actuator_config={
            "policy_id": pool_name,
            "policy_config_sha256": DIGEST,
            "slurm_cluster_config_sha256": DIGEST,
            "slurm_cluster_id": cluster_id,
        },
    )


def test_frozen_gpu_selection_creates_demand_for_exactly_one_writer() -> None:
    profile = load_resource_profiles().get("behavior-sim-local-none@1").profile
    spec = {
        "execution_variant_id": "gb10-shared-1gpu",
        "gpu_backend_selection_sha256": DIGEST,
    }
    gb10 = _row("behavior-gpu-gb10", "gb10")
    oldlab = _row("behavior-gpu-oldlab", "oldlab")
    assert _queued_pipeline_attempt_matches_policy(
        profile.model_dump(mode="json"),
        spec,
        gb10,
        selected_variant_id="gb10-shared-1gpu",
        selected_policy_id="behavior-gpu-gb10",
        persisted_selection_digest=DIGEST,
    )
    assert not _queued_pipeline_attempt_matches_policy(
        profile.model_dump(mode="json"),
        spec,
        oldlab,
        selected_variant_id="gb10-shared-1gpu",
        selected_policy_id="behavior-gpu-gb10",
        persisted_selection_digest=DIGEST,
    )
    assert not _queued_pipeline_attempt_matches_policy(
        profile.model_dump(mode="json"),
        spec,
        gb10,
        selected_variant_id="gb10-shared-1gpu",
        selected_policy_id="behavior-gpu-gb10",
        persisted_selection_digest="sha256:" + "b" * 64,
    )


def test_all_pipeline_policies_merge_disabled_with_zero_desired_capacity() -> None:
    registry = PolicyConfigRegistry.load(resource_profiles=load_resource_profiles())
    rows = registry.disabled_autoscaler_rows(environment="staging")
    observation = AutoscalerObservation(
        active_slots=0,
        pending_slots=0,
        draining_slots=0,
        occupied_slots=0,
        queued_slots=1,
        idle_worker_ids=(),
        drained_worker_ids=(),
    )
    for row in rows:
        assert row["enabled"] is False
        assert row["min_slots"] == 0
        decision = compute_autoscaler_decision(
            AutoscalerPolicyConfig(
                environment=str(row["environment"]),
                pool_name=str(row["pool_name"]),
                actuator="slurm",
                enabled=False,
                min_slots=0,
                max_slots=int(row["max_slots"]),
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                disabled_reason=str(row["disabled_reason"]),
                actuator_config=dict(row["actuator_config"]),
            ),
            observation,
        )
        assert decision.action == "noop"
        assert decision.desired_slots == 0
