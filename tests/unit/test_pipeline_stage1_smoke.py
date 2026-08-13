from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.integrations.behavior.contracts import BehaviorRolloutParametersV1
from loom.pipeline.keys import canonical_digest, digest_bytes
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.pipeline.spec import RunBudgetV1, StageBudgetV1
from loom.pipeline.stage1_smoke import (
    STAGE1_SMOKE_RECIPE_NAME,
    Stage1SmokeCandidateV1,
    Stage1SmokeInputV1,
    Stage1SmokeOutputV1,
    Stage1SmokePreviewPolicyV1,
    build_stage1_smoke_graph,
    load_behavior_renderer_lock,
    stage1_smoke_recipe_digest,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DIGEST = "sha256:" + "1" * 64
IMAGE_INDEX = "ghcr.io/qianyi-sun/loom-behavior-sim@sha256:" + "2" * 64
IMAGE_CHILD = "sha256:" + "3" * 64


def _candidate(**updates: object) -> Stage1SmokeCandidateV1:
    profile = ResourceProfileRegistry.load().get("behavior-sim-local-none@1")
    parameters = BehaviorRolloutParametersV1(
        eval_instance_index=0,
        episode_index=0,
        seed=7,
        record_depth=False,
        recording_fps=30,
    ).model_dump(mode="json")
    stage_budget = StageBudgetV1(
        provider=None,
        gpu_seconds_limit=28_870,
        final_output_bytes_limit=67_108_864,
        checkpoint_bytes_limit=0,
        timeout_seconds=14_400,
        max_attempts=2,
    )
    now = datetime(2026, 8, 13, 16, tzinfo=UTC)
    renderer_digest = canonical_digest(load_behavior_renderer_lock(REPO_ROOT))
    schema_digest = digest_bytes(
        (REPO_ROOT / "src/loom/pipeline/renderers/schemas/behavior.stage-request.v1.json").read_bytes()
    )
    value: dict[str, object] = {
        "schema_version": "loom.behavior-stage1-smoke-candidate.v1",
        "loom_commit_sha": "a" * 40,
        "environment": "staging",
        "team_id": UUID("00000000-0000-4000-8000-000000000001"),
        "operator_user_id": UUID("00000000-0000-4000-8000-000000000002"),
        "backend_variant_id": "oldlab-rtx5080-2gpu",
        "slurm_cluster_id": "oldlab",
        "slurm_cluster_config_sha256": DIGEST,
        "policy_id": "behavior-gpu-oldlab",
        "policy_config_sha256": DIGEST,
        "policy_activation_epoch": 1,
        "image_index_digest": IMAGE_INDEX,
        "platform": "linux/amd64",
        "platform_child_digest": IMAGE_CHILD,
        "image_runtime_contract_sha256": DIGEST,
        "resource_profile_sha256": profile.snapshot_sha256,
        "renderer_lock_sha256": renderer_digest,
        "stage_request_schema_sha256": schema_digest,
        "compatibility_manifest_sha256": DIGEST,
        "recipe_digest": stage1_smoke_recipe_digest(
            renderer_lock_sha256=renderer_digest,
            stage_request_schema_sha256=schema_digest,
            resource_profile_sha256=profile.snapshot_sha256,
            image_index_digest=IMAGE_INDEX,
            platform_child_digest=IMAGE_CHILD,
            compatibility_manifest_sha256=DIGEST,
        ),
        "inputs": [
            Stage1SmokeInputV1(
                name=name,
                artifact_type=artifact_type,
                required=True,
                artifact_id=UUID(int=index),
                manifest_sha256="sha256:" + str(index) * 64,
                content_sha256="sha256:" + str(index + 3) * 64,
                stored_size_bytes=index * 100,
                unpacked_size_bytes=index * 200,
                file_count=index,
            )
            for index, (name, artifact_type) in enumerate(
                (
                    ("task_instance", "behavior_task_instance.v1"),
                    ("dataset", "behavior_dataset_snapshot.v1"),
                    ("policy", "behavior_policy_checkpoint.v1"),
                ),
                start=1,
            )
        ],
        "parameters": parameters,
        "run_budget": RunBudgetV1(
            max_provider_cost_usd="0",
            max_gpu_seconds=28_870,
            max_wall_seconds=14_400,
            max_artifact_bytes=67_108_864,
            max_stage_runs=1,
            max_attempts_total=2,
        ),
        "stage_budget": stage_budget,
        "expected_outputs": [
            Stage1SmokeOutputV1(
                name="rollout",
                artifact_type="behavior_rollout_bundle.v1",
                producer="container",
                required=True,
                max_bytes=67_108_864,
            )
        ],
        "expected_domain_outcome": "rollout_success",
        "preview_policy": Stage1SmokePreviewPolicyV1(
            schema_version="loom.behavior-stage1-preview-policy.v1",
            min_interval_ms=500,
            ttl_seconds=300,
            max_frame_bytes=524_288,
            max_frames_per_attempt=64,
            max_total_bytes_per_attempt=33_554_432,
            width=672,
            height=448,
            media_type="image/jpeg",
            label="LIVE / UNVERIFIED",
        ),
        "start_by": now + timedelta(minutes=10),
        "cleanup_deadline": now + timedelta(hours=5),
    }
    value.update(updates)
    return Stage1SmokeCandidateV1.model_validate(value)


def test_candidate_is_canonical_bounded_and_digest_bound() -> None:
    candidate = _candidate()
    assert len(candidate.canonical_bytes) < 1_048_576
    assert candidate.canonical_bytes.endswith(b"\n")
    assert candidate.candidate_sha256 == digest_bytes(candidate.canonical_bytes)


def test_internal_graph_is_exact_one_stage_network_none() -> None:
    candidate = _candidate()
    graph = build_stage1_smoke_graph(candidate, repo_root=REPO_ROOT)
    assert graph.recipe.name == STAGE1_SMOKE_RECIPE_NAME
    assert [item.name for item in graph.inputs] == ["task_instance", "dataset", "policy"]
    assert len(graph.nodes) == 1
    node = graph.nodes[0]
    assert node.node_kind == "container"
    assert node.node_key == "rollout"
    assert node.network_profile == "none"
    assert node.image == IMAGE_INDEX
    assert node.request_renderer is not None
    assert node.request_renderer.name == "behavior_stage_request"
    assert [item.name for item in node.outputs] == ["rollout"]


@pytest.mark.parametrize(
    "update",
    [
        {"slurm_cluster_id": "gb10"},
        {"policy_id": "behavior-gpu-gb10"},
        {"platform": "linux/arm64"},
        {"parameters": {"episode_index": 0}},
        {"run_budget": {"max_provider_cost_usd": "1"}},
        {"cleanup_deadline": datetime(2026, 8, 13, 16, tzinfo=UTC)},
    ],
)
def test_candidate_rejects_authority_or_contract_drift(update: dict[str, object]) -> None:
    with pytest.raises((ValidationError, ValueError)):
        _candidate(**update)


def test_candidate_rejects_input_reorder() -> None:
    candidate = _candidate()
    value = candidate.model_dump(mode="python")
    value["inputs"] = list(reversed(value["inputs"]))
    with pytest.raises(ValidationError, match="exact graph order"):
        Stage1SmokeCandidateV1.model_validate(value)
