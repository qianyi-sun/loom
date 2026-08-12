from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from tests.integrations.behavior.test_rollout_adapter import (
    ATTEMPT_ID,
    D0,
    D2,
    D3,
    IMAGE,
    RUN_ID,
    _Composite,
    _dataset_document,
    _Hdf5,
    _NoopSupervisor,
    _policy_document,
    _runtime,
    _task_document,
    _transition_document,
    _Videos,
)
from tests.integrations.behavior.test_rollout_adapter import (
    _request as _rollout_request,
)

from loom.integrations.behavior.canonical_json import (
    canonical_digest,
    canonical_document,
    digest_bytes,
)
from loom.integrations.behavior.contracts import (
    BehaviorDatasetSnapshotArtifactV1,
    BehaviorInspectionReportArtifactV1,
    BehaviorRolloutBundleArtifactV1,
    BehaviorTaskInstanceArtifactV1,
    EmptyParametersV1,
    ProviderUsageV1,
    StageRequestV1,
)
from loom.integrations.behavior.errors import BehaviorContractError
from loom.integrations.behavior.offline_judge_assets import (
    PROVIDER_ROOT,
    ProviderAssetBundle,
    bundled_provider_asset_root,
)
from loom.integrations.behavior.offline_runner import (
    LockedCodexOfflineJudgeRunner,
    LockedCodexPassResult,
)
from loom.integrations.behavior.stages.offline_judge import (
    OFFLINE_JUDGE_OUTPUT_DECLARATIONS,
    MountedFrozenJudgeAuthorityReader,
    OfflineJudgeAdapter,
    OfflineJudgePaths,
    OfflineJudgeRunRequest,
    OfflineJudgeRunResult,
    load_mounted_offline_judge_inputs,
    validate_offline_judge_request,
)
from loom.integrations.behavior.stages.rollout import RolloutAdapter, RolloutPaths
from loom.pipeline.control_bindings import (
    JudgeExecutionProfileV1,
    control_snapshot_digest,
    registered_judge_adapter_digest,
)
from loom_worker.pipeline_attempt_workspace import AttemptWorkspace

JUDGE_STAGE_RUN_ID = UUID(int=102)
JUDGE_ATTEMPT_ID = UUID(int=103)
TASK_ARTIFACT_ID = UUID(int=10)
ROLLOUT_ARTIFACT_ID = UUID("00000000-0000-4000-8000-00000000006f")
DATASET_ARTIFACT_ID = UUID(int=11)
PROFILE_ID = UUID(int=120)
USAGE = ProviderUsageV1(
    request_count=2,
    input_tokens=1_200,
    cache_read_tokens=300,
    output_tokens=450,
    cost_microusd=12_345,
)
TASK_CARD = b"# placing_can\n\nPut the can on the table without dropping it.\n"
DEMO_BYTES = {
    "head": b"demo-head",
    "left_wrist": b"demo-left",
    "right_wrist": b"demo-right",
}
SESSION_ID = UUID("018f65d5-53c2-7d80-b4a8-2b67db937c8a")


def _dataset() -> BehaviorDatasetSnapshotArtifactV1:
    value = _dataset_document(b"task").model_dump(mode="json")
    compatibility = value["payload"]["compatibility"]
    card = compatibility["agentic_task_cards"][0]
    card.update(
        {
            "relative_path": "agentic_sweep/task_cards/task-0007.md",
            "sha256": digest_bytes(TASK_CARD),
            "size_bytes": len(TASK_CARD),
        }
    )
    episode = compatibility["agentic_demo_video_sets"][0]["episodes"][0]
    episode["episode_id"] = "episode_00000001"
    for descriptor in episode["files"]:
        camera = descriptor["camera"]
        encoded = DEMO_BYTES[camera]
        descriptor.update(
            {
                "relative_path": (
                    f"videos/task-0007/observation.images.rgb.{camera}/episode_00000001.mp4"
                ),
                "sha256": digest_bytes(encoded),
                "size_bytes": len(encoded),
            }
        )
    compatibility["agentic_task_cards_sha256"] = canonical_digest(
        compatibility["agentic_task_cards"], persisted=False
    )
    compatibility["agentic_demo_video_sets_sha256"] = canonical_digest(
        compatibility["agentic_demo_video_sets"], persisted=False
    )
    declared = [
        {
            "name": "task_card",
            "relative_path": "payload/agentic_sweep/task_cards/task-0007.md",
            "sha256": digest_bytes(TASK_CARD),
            "size_bytes": len(TASK_CARD),
            "media_type": "text/markdown",
            "required": True,
        }
    ]
    for camera, encoded in DEMO_BYTES.items():
        declared.append(
            {
                "name": f"demo_{camera}",
                "relative_path": (
                    f"payload/videos/task-0007/observation.images.rgb.{camera}/episode_00000001.mp4"
                ),
                "sha256": digest_bytes(encoded),
                "size_bytes": len(encoded),
                "media_type": "video/mp4",
                "required": True,
            }
        )
    value["files"].extend(declared)
    value["files"].sort(key=lambda item: item["relative_path"].encode("utf-8"))
    return BehaviorDatasetSnapshotArtifactV1.model_validate_json(canonical_document(value))


def _task(dataset: BehaviorDatasetSnapshotArtifactV1) -> BehaviorTaskInstanceArtifactV1:
    value = _task_document().model_dump(mode="json")
    value["payload"]["lineage"]["dataset_content_sha256"] = canonical_digest(dataset)
    return BehaviorTaskInstanceArtifactV1.model_validate_json(canonical_document(value))


def _write_dataset_files(root: Path) -> None:
    card = root / "payload/agentic_sweep/task_cards/task-0007.md"
    card.parent.mkdir(parents=True)
    card.write_bytes(TASK_CARD)
    for camera, encoded in DEMO_BYTES.items():
        target = (
            root / f"payload/videos/task-0007/observation.images.rgb.{camera}/episode_00000001.mp4"
        )
        target.parent.mkdir(parents=True)
        target.write_bytes(encoded)
    bddl = root / "payload/omnigibson/2025-challenge-task-instances/task.bddl"
    bddl.parent.mkdir(parents=True)
    bddl.write_bytes(b"task")


def _rollout(
    tmp_path: Path,
    task: BehaviorTaskInstanceArtifactV1,
    dataset: BehaviorDatasetSnapshotArtifactV1,
) -> BehaviorRolloutBundleArtifactV1:
    policy = _policy_document(b"checkpoint")
    request = _rollout_request(task, dataset, policy)
    input_root = tmp_path / "rollout-inputs"
    for name, document in (
        ("task_instance", task),
        ("dataset", dataset),
        ("policy", policy),
    ):
        target = input_root / name
        target.mkdir(parents=True)
        (target / "artifact.json").write_bytes(canonical_document(document))
    _write_dataset_files(input_root / "dataset")
    checkpoint = input_root / "policy/payload/checkpoint/weights.bin"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"checkpoint")

    child = task.payload
    engine_root = tmp_path / "engine"
    task_tag = f"task-{child.behavior_task_id:04d}"
    stem = child.demo_stem
    payloads = {
        engine_root / f"payload/trajectories/{task_tag}/{stem}.hdf5": b"hdf5",
        engine_root
        / f"payload/meta/episodes/{task_tag}/{stem}_bddl_transitions.json": canonical_document(
            _transition_document(task, True)
        ),
        engine_root / f"payload/videos/{task_tag}/observation.images.rgb.head/{stem}.mp4": b"head",
        engine_root
        / f"payload/videos/{task_tag}/observation.images.rgb.left_wrist/{stem}.mp4": b"left",
        engine_root
        / f"payload/videos/{task_tag}/observation.images.rgb.right_wrist/{stem}.mp4": b"right",
    }
    for path, encoded in payloads.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encoded)
    output_root = tmp_path / "rollout-outputs"
    output_root.mkdir()
    workspace = _workspace(output_root, request, {"rollout": "behavior_rollout_bundle.v1"})
    result = RolloutAdapter(
        runtime=_runtime(),
        paths=RolloutPaths(input_root, engine_root, tmp_path / "rollout-scratch"),
        supervisor=_NoopSupervisor(),
        hdf5=_Hdf5(child.seed),
        video=_Videos(),
        composite=_Composite(),
    )(request, workspace)
    assert result.domain_outcome == "rollout_success"
    artifact_root = workspace.partial_root / "artifacts/rollout"
    return BehaviorRolloutBundleArtifactV1.model_validate_json(
        (artifact_root / "artifact.json").read_bytes()
    )


def _profile(
    assets: ProviderAssetBundle,
    *,
    alternate: bool = False,
) -> JudgeExecutionProfileV1:
    adapter = "synthetic_judge_v1" if alternate else "codex_pipeline_locked_home_v1"
    now = datetime(2026, 8, 12, tzinfo=UTC)
    asset_locks = [
        {
            "role": f"asset_{index:02d}",
            "image_path": f"{PROVIDER_ROOT}/{relative}",
            "sha256": assets.digests[relative],
        }
        for index, relative in enumerate(sorted(assets.digests, key=lambda item: item.encode()))
    ]
    return JudgeExecutionProfileV1.model_validate(
        {
            "schema_version": "loom.judge-execution-profile.v1",
            "profile_id": PROFILE_ID,
            "profile_name": "alternate_profile"
            if alternate
            else "behavior-judge-codex-gpt-5.6-sol-v1",
            "version": 2 if alternate else 1,
            "status": "active",
            "recipe_name": "behavior-recovery",
            "recipe_version": 1,
            "recipe_digest": D0,
            "node_key": "offline_judge",
            "environment": "staging",
            "agent_name": "synthetic_judge" if alternate else "codex",
            "agent_version": "1.0.0" if alternate else "0.146.0",
            "agent_adapter": adapter,
            "agent_adapter_digest": registered_judge_adapter_digest(adapter),
            "provider_connection_id": UUID(int=121),
            "provider": "anthropic" if alternate else "openai",
            "model": "claude-sonnet-4-6" if alternate else "gpt-5.6-sol",
            "wire_api": "messages" if alternate else "responses",
            "runner_lock_sha256": assets.digests["runner-lock.json"],
            "provider_asset_manifest_sha256": assets.manifest_sha256,
            "provider_asset_locks": asset_locks,
            "mcp_server_locks": list(assets.mcp_locks),
            "provider_request_limit_per_attempt": 2,
            "provider_cost_limit_microusd_per_attempt": 20_000,
            "per_call_timeout_seconds": 60,
            "allowed_team_ids": [],
            "created_by": UUID(int=122),
            "created_at": now,
            "updated_by": UUID(int=123),
            "updated_at": now,
        }
    )


def _judge_request(
    task: BehaviorTaskInstanceArtifactV1,
    rollout: BehaviorRolloutBundleArtifactV1,
    dataset: BehaviorDatasetSnapshotArtifactV1,
    profile: JudgeExecutionProfileV1,
) -> StageRequestV1:
    documents: list[Any] = [task, rollout, dataset]
    names = ["task_instance", "rollout", "dataset"]
    types = [
        "behavior_task_instance.v1",
        "behavior_rollout_bundle.v1",
        "behavior_dataset_snapshot.v1",
    ]
    ids = [TASK_ARTIFACT_ID, ROLLOUT_ARTIFACT_ID, DATASET_ARTIFACT_ID]
    inputs = [
        {
            "binding_name": name,
            "artifact_type": artifact_type,
            "cardinality": "one",
            "items": [
                {
                    "artifact_id": str(artifact_id),
                    "content_sha256": canonical_digest(document),
                    "file_count": len(document.files),
                    "item_key": "singleton",
                    "manifest_sha256": D0,
                    "stored_size_bytes": 1,
                    "unpacked_size_bytes": 1,
                }
            ],
        }
        for name, artifact_type, artifact_id, document in zip(
            names, types, ids, documents, strict=True
        )
    ]
    resolved = canonical_digest(inputs)
    preimage = {
        "attempt_id": str(JUDGE_ATTEMPT_ID),
        "execution_spec_digest": D2,
        "resolved_input_bindings_digest": resolved,
        "stage_run_id": str(JUDGE_STAGE_RUN_ID),
    }
    return StageRequestV1.model_validate_json(
        canonical_document(
            {
                "schema_version": "behavior.stage-request.v1",
                "stage": "offline_judge",
                "run_id": str(RUN_ID),
                "stage_run_id": str(JUDGE_STAGE_RUN_ID),
                "attempt_id": str(JUDGE_ATTEMPT_ID),
                "idempotency_key": canonical_digest(preimage, persisted=False),
                "inputs": inputs,
                "parameters": {"inspection_mode": "whole_episode_predicate_log"},
                "budget": {
                    "provider": {
                        "provider_request_limit_per_attempt": 2,
                        "provider_cost_limit_microusd_per_attempt": 20_000,
                        "per_call_timeout_seconds": 60,
                    },
                    "gpu_seconds_limit": 0,
                    "final_output_bytes_limit": 16_777_216,
                    "checkpoint_bytes_limit": 0,
                    "timeout_seconds": 3_600,
                    "max_attempts": 2,
                },
                "provenance": {
                    "recipe_digest": D0,
                    "resolved_input_bindings_digest": resolved,
                    "execution_spec_digest": D2,
                    "image_digest": IMAGE,
                    "loom_commit_sha": "5" * 40,
                    "control_binding": {
                        "logical_name": "behavior_offline_judge",
                        "kind": "judge_profile",
                        "node_key": "offline_judge",
                        "object_id": str(profile.profile_id),
                        "version": profile.version,
                        "snapshot_sha256": control_snapshot_digest(profile),
                        "provider_asset_manifest_sha256": (profile.provider_asset_manifest_sha256),
                        "judge_profile_id": str(profile.profile_id),
                        "judge_profile_version": profile.version,
                        "agent": profile.agent_name,
                        "agent_version": profile.agent_version,
                        "provider": profile.provider,
                        "model": profile.model,
                    },
                    "compatibility_manifest_sha256": D3,
                },
                "orchestration": None,
            }
        )
    )


def _workspace(
    root: Path,
    request: StageRequestV1,
    declarations: dict[str, str] | None = None,
) -> AttemptWorkspace:
    return AttemptWorkspace(
        root,
        request.attempt_id,
        request.idempotency_key,
        output_declarations=declarations or OFFLINE_JUDGE_OUTPUT_DECLARATIONS,
        final_output_bytes_limit=request.budget.final_output_bytes_limit,
        resolved_input_bindings_digest=request.provenance.resolved_input_bindings_digest,
        execution_spec_digest=request.provenance.execution_spec_digest,
        recipe_digest=request.provenance.recipe_digest,
        image_digest=request.provenance.image_digest,
    )


def _valid_report() -> bytes:
    report: bytes = (
        "# placing_can/37/70371 n_steps=2 fps=30\n\n"
        "## Timeline\n\n"
        "| first | last | primitive | object -> target | arm | verdict | learn | seed | why |\n"
        "|---|---|---|---|---|---|---|---|---|\n"
        "| 0 | 1 | `pick up from` | can -> table | right | execution | — | 1 | "
        "the can stopped moving at frame 1 before reaching the table |\n"
    ).encode()
    return report


def _valid_seed() -> bytes:
    return canonical_document(
        {
            "chunks": [
                {
                    "span": [0, 1],
                    "learn": None,
                    "seed": 1,
                    "reason": "the can stopped moving at frame 1 before reaching the table",
                    "skill_label": "pick up from",
                    "object": "can",
                    "target": "table",
                    "arm": "right",
                }
            ]
        }
    )


class _Runner:
    def __init__(self, usage: ProviderUsageV1 = USAGE) -> None:
        self.usage = usage
        self.calls: list[OfflineJudgeRunRequest] = []

    def run(self, run: OfflineJudgeRunRequest) -> OfflineJudgeRunResult:
        self.calls.append(run)
        return OfflineJudgeRunResult(_valid_report(), _valid_seed(), self.usage)


@dataclass
class JudgeCase:
    task: BehaviorTaskInstanceArtifactV1
    rollout: BehaviorRolloutBundleArtifactV1
    dataset: BehaviorDatasetSnapshotArtifactV1
    profile: JudgeExecutionProfileV1
    request: StageRequestV1
    paths: OfflineJudgePaths
    output_root: Path


def _case(tmp_path: Path, *, alternate: bool = False) -> JudgeCase:
    dataset = _dataset()
    task = _task(dataset)
    rollout = _rollout(tmp_path, task, dataset)
    asset_root = tmp_path / "provider-assets"
    shutil.copytree(bundled_provider_asset_root(), asset_root)
    assets = ProviderAssetBundle.load(asset_root)
    profile = _profile(assets, alternate=alternate)
    request = _judge_request(task, rollout, dataset, profile)
    input_root = tmp_path / "judge-inputs"
    for name, document in (("task_instance", task), ("dataset", dataset)):
        target = input_root / name
        target.mkdir(parents=True)
        (target / "artifact.json").write_bytes(canonical_document(document))
    _write_dataset_files(input_root / "dataset")
    rollout_source = tmp_path / "rollout-outputs/.partial" / str(ATTEMPT_ID) / "artifacts/rollout"
    shutil.copytree(rollout_source, input_root / "rollout")
    (input_root / "control-binding.json").write_bytes(
        canonical_document(profile.model_dump(mode="json"))
    )
    output_root = tmp_path / "judge-outputs"
    output_root.mkdir()
    return JudgeCase(
        task,
        rollout,
        dataset,
        profile,
        request,
        OfflineJudgePaths(
            input_root, asset_root, tmp_path / "judge", input_root / "control-binding.json"
        ),
        output_root,
    )


def _request_with_inputs(request: StageRequestV1, inputs: list[dict[str, Any]]) -> StageRequestV1:
    value = request.model_dump(mode="json")
    value["inputs"] = inputs
    resolved = canonical_digest(inputs)
    value["provenance"]["resolved_input_bindings_digest"] = resolved
    value["idempotency_key"] = canonical_digest(
        {
            "attempt_id": str(request.attempt_id),
            "execution_spec_digest": request.provenance.execution_spec_digest,
            "resolved_input_bindings_digest": resolved,
            "stage_run_id": str(request.stage_run_id),
        },
        persisted=False,
    )
    return StageRequestV1.model_validate_json(canonical_document(value))


def _adapter(case: JudgeCase, runner: _Runner) -> OfflineJudgeAdapter:
    return OfflineJudgeAdapter(
        runner=runner,
        authority_reader=MountedFrozenJudgeAuthorityReader(),
        paths=case.paths,
    )


def test_exact_three_scalar_bindings_parameters_and_full_signed_join(tmp_path: Path) -> None:
    case = _case(tmp_path)
    mounted = load_mounted_offline_judge_inputs(case.request, case.paths)
    assert mounted.task_instance == case.task
    assert mounted.rollout == case.rollout
    assert mounted.dataset == case.dataset
    assert mounted.task_card == TASK_CARD

    raw_inputs = case.request.model_dump(mode="json")["inputs"]
    for inputs in (raw_inputs[:-1], list(reversed(raw_inputs))):
        with pytest.raises(BehaviorContractError, match="exact ordered three"):
            validate_offline_judge_request(_request_with_inputs(case.request, inputs))
    with pytest.raises(BehaviorContractError, match="predicate-log"):
        validate_offline_judge_request(
            case.request.model_copy(update={"parameters": EmptyParametersV1()})
        )


def test_valid_output_commits_exact_artifact_and_stage_result(tmp_path: Path) -> None:
    case = _case(tmp_path)
    runner = _Runner()
    workspace = _workspace(case.output_root, case.request)
    result = _adapter(case, runner)(case.request, workspace)
    committed = workspace.commit(result)

    assert len(runner.calls) == 1
    assert runner.calls[0].inputs.task_card == TASK_CARD
    assert runner.calls[0].profile == case.profile
    assert result.domain_outcome == "judged"
    assert result.reason_code == "inspection_complete"
    assert [(item.name, item.artifact_type) for item in result.outputs] == [
        ("inspection", "behavior_inspection_report.v1")
    ]
    assert [(item.binding_name, item.artifact_id) for item in result.inputs] == [
        ("task_instance", TASK_ARTIFACT_ID),
        ("rollout", ROLLOUT_ARTIFACT_ID),
        ("dataset", DATASET_ARTIFACT_ID),
    ]
    artifact = BehaviorInspectionReportArtifactV1.model_validate_json(
        (committed.root / "artifacts/inspection/artifact.json").read_bytes()
    )
    assert artifact.payload.usage == USAGE
    assert artifact.payload.rollout_artifact_id == ROLLOUT_ARTIFACT_ID
    assert artifact.payload.task_card_sha256 == digest_bytes(TASK_CARD)
    assert artifact.payload.judge_profile_sha256 == control_snapshot_digest(case.profile)
    assert artifact.provenance.control_binding == case.request.provenance.control_binding
    assert [item.relative_path for item in artifact.files] == [
        "payload/report.md",
        "payload/seed.json",
    ]
    assert (
        committed.root / "artifacts/inspection/payload/report.md"
    ).read_bytes() == _valid_report()


@pytest.mark.parametrize(
    ("target", "match"),
    [
        ("task-drift", "content digest drift"),
        ("card-drift", "manifest"),
        ("video-symlink", "cannot be opened"),
        ("transition-drift", "manifest"),
        ("scene-drift", "manifest"),
        ("asset-extra", "extra or missing path"),
        ("profile-drift", "profile disagrees"),
    ],
)
def test_drift_symlink_extra_profile_fail_before_runner_or_output(
    tmp_path: Path, target: str, match: str
) -> None:
    case = _case(tmp_path)
    if target == "task-drift":
        path = case.paths.input_root / "task_instance/artifact.json"
        value = case.task.model_dump(mode="json")
        value["payload"]["loom_task_id"] = "drifted-task"
        path.write_bytes(canonical_document(value))
    elif target == "card-drift":
        (
            case.paths.input_root / "dataset/payload/agentic_sweep/task_cards/task-0007.md"
        ).write_bytes(b"drift")
    elif target == "video-symlink":
        path = (
            case.paths.input_root
            / "dataset/payload/videos/task-0007/observation.images.rgb.head/episode_00000001.mp4"
        )
        path.unlink()
        path.symlink_to("/dev/null")
    elif target in {"transition-drift", "scene-drift"}:
        suffix = "bddl_transitions.json" if target == "transition-drift" else "scene.json"
        path = next((case.paths.input_root / "rollout/payload/meta/episodes").rglob(f"*_{suffix}"))
        path.write_bytes(b"{}\n")
    elif target == "asset-extra":
        (case.paths.asset_root / "unexpected.txt").write_text("extra")
    elif target == "profile-drift":
        value = case.profile.model_dump(mode="json")
        value["version"] = 9
        (case.paths.control_binding_path).write_bytes(canonical_document(value))

    runner = _Runner()
    workspace = _workspace(case.output_root, case.request)
    with pytest.raises(BehaviorContractError, match=match):
        _adapter(case, runner)(case.request, workspace)
    assert runner.calls == []
    assert not workspace.partial_root.exists()


def test_signed_task_rollout_identity_mismatch_fails_before_runner_output(tmp_path: Path) -> None:
    case = _case(tmp_path)
    value = case.rollout.model_dump(mode="json")
    value["payload"]["loom_task_id"] = "different-loom-task"
    drifted = BehaviorRolloutBundleArtifactV1.model_validate_json(canonical_document(value))
    (case.paths.input_root / "rollout/artifact.json").write_bytes(canonical_document(drifted))
    inputs = case.request.model_dump(mode="json")["inputs"]
    inputs[1]["items"][0]["content_sha256"] = canonical_digest(drifted)
    request = _request_with_inputs(case.request, inputs)
    runner = _Runner()
    workspace = _workspace(case.output_root, request)
    with pytest.raises(BehaviorContractError, match="signed identities disagree"):
        _adapter(case, runner)(request, workspace)
    assert runner.calls == []
    assert not workspace.partial_root.exists()


@pytest.mark.parametrize("target", ["dataset-ref", "source-type"])
def test_rollout_artifact_joins_are_exact_before_runner(tmp_path: Path, target: str) -> None:
    case = _case(tmp_path)
    value = case.rollout.model_dump(mode="json")
    if target == "dataset-ref":
        value["payload"]["dataset"]["manifest_sha256"] = "sha256:" + "a" * 64
        expected_error = "dataset lineage disagrees"
    else:
        source = next(
            item
            for item in value["provenance"]["source_artifacts"]
            if item["artifact_id"] == str(TASK_ARTIFACT_ID)
        )
        source["artifact_type"] = "behavior_dataset_snapshot.v1"
        expected_error = "source Artifact identity drift"
    drifted = BehaviorRolloutBundleArtifactV1.model_validate_json(canonical_document(value))
    (case.paths.input_root / "rollout/artifact.json").write_bytes(canonical_document(drifted))
    inputs = case.request.model_dump(mode="json")["inputs"]
    inputs[1]["items"][0]["content_sha256"] = canonical_digest(drifted)
    request = _request_with_inputs(case.request, inputs)
    runner = _Runner()
    workspace = _workspace(case.output_root, request)
    with pytest.raises(BehaviorContractError, match=expected_error):
        _adapter(case, runner)(request, workspace)
    assert runner.calls == []
    assert not workspace.partial_root.exists()


def test_gateway_settlement_over_budget_fails_before_output(tmp_path: Path) -> None:
    case = _case(tmp_path)
    runner = _Runner(
        ProviderUsageV1(
            request_count=3,
            input_tokens=0,
            cache_read_tokens=0,
            output_tokens=0,
            cost_microusd=0,
        )
    )
    workspace = _workspace(case.output_root, case.request)
    with pytest.raises(BehaviorContractError, match="exceeds"):
        _adapter(case, runner)(case.request, workspace)
    assert len(runner.calls) == 1
    assert not workspace.partial_root.exists()


def test_gateway_settlement_below_budget_is_persisted_exactly(tmp_path: Path) -> None:
    case = _case(tmp_path)
    settlement = ProviderUsageV1(
        request_count=1,
        input_tokens=1,
        cache_read_tokens=0,
        output_tokens=1,
        cost_microusd=1,
    )
    runner = _Runner(settlement)
    workspace = _workspace(case.output_root, case.request)
    _adapter(case, runner)(case.request, workspace)
    artifact = BehaviorInspectionReportArtifactV1.model_validate_json(
        (workspace.partial_root / "artifacts/inspection/artifact.json").read_bytes()
    )
    assert len(runner.calls) == 1
    assert artifact.payload.usage == settlement


def test_alternate_registered_profile_uses_same_adapter_seam(tmp_path: Path) -> None:
    case = _case(tmp_path, alternate=True)
    runner = _Runner()
    workspace = _workspace(case.output_root, case.request)
    result = _adapter(case, runner)(case.request, workspace)
    artifact = BehaviorInspectionReportArtifactV1.model_validate_json(
        (workspace.partial_root / "artifacts/inspection/artifact.json").read_bytes()
    )
    assert result.domain_outcome == "judged"
    assert runner.calls[0].profile.agent_adapter == "synthetic_judge_v1"
    assert artifact.payload.agent == "synthetic_judge"
    assert artifact.payload.provider == "anthropic"
    assert artifact.payload.model == "claude-sonnet-4-6"


class _LockedExecutor:
    def __init__(self, passes: list[LockedCodexPassResult]) -> None:
        self.passes = passes
        self.verified: list[tuple[str, str, str | None]] = []
        self.executed: list[Any] = []
        self.cleaned: list[tuple[tuple[str, ...], int, bool]] = []

    def verify_binary(self, path: str, sha256: str, *, version: str | None) -> None:
        assert self.executed == []
        self.verified.append((path, sha256, version))

    def execute(self, spec: Any) -> LockedCodexPassResult:
        assert len(self.verified) == 2
        self.executed.append(spec)
        return self.passes.pop(0)

    def cleanup(
        self, *, paths: tuple[str, ...], term_grace_seconds: int, kill_after_grace: bool
    ) -> None:
        self.cleaned.append((paths, term_grace_seconds, kill_after_grace))


class _Settlement:
    def __init__(self, usage: ProviderUsageV1 = USAGE) -> None:
        self.usage = usage
        self.calls: list[tuple[UUID, str]] = []

    def read(self, *, attempt_id: UUID, control_binding_sha256: str) -> ProviderUsageV1:
        self.calls.append((attempt_id, control_binding_sha256))
        return self.usage


def _thread_events(session: UUID = SESSION_ID) -> bytes:
    return canonical_document({"type": "thread.started", "thread_id": str(session)})


def test_locked_codex_runner_verifies_then_resumes_once_and_reads_settlement(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    mounted = load_mounted_offline_judge_inputs(case.request, case.paths)
    executor = _LockedExecutor(
        [
            LockedCodexPassResult(_thread_events(), _valid_report(), None, 1),
            LockedCodexPassResult(b"", None, _valid_seed(), 0),
        ]
    )
    settlement = _Settlement()
    runner = LockedCodexOfflineJudgeRunner(
        gateway_responses_url="https://gateway.example.test/v1/responses",
        shim_port=4444,
        executor=executor,
        settlement_reader=settlement,
    )
    result = runner.run(
        OfflineJudgeRunRequest(
            case.request,
            mounted,
            case.profile,
            ProviderAssetBundle.load(case.paths.asset_root),
            b"locked prompt\n",
        )
    )
    assert result.report == _valid_report()
    assert result.seed == _valid_seed()
    assert result.usage == USAGE
    assert len(executor.executed) == 2
    assert executor.executed[0].resume_session_id is None
    assert str(executor.executed[1].resume_session_id) in executor.executed[1].argv
    assert executor.executed[1].stdin == (
        b"Write only the missing output file(s): seed.json. "
        b"Do not re-investigate or rewrite an existing output.\n"
    )
    assert executor.cleaned[0][1:] == (30, True)
    assert settlement.calls == [
        (JUDGE_ATTEMPT_ID, case.request.provenance.control_binding.snapshot_sha256)
    ]


@pytest.mark.parametrize(
    ("passes", "error"),
    [
        (
            [LockedCodexPassResult(_thread_events() + _thread_events(), None, None, 1)],
            BehaviorContractError,
        ),
        (
            [LockedCodexPassResult(_thread_events(), None, None, 1, "provider_429")],
            RuntimeError,
        ),
    ],
)
def test_locked_codex_runner_fails_closed_and_always_cleans(
    tmp_path: Path, passes: list[LockedCodexPassResult], error: type[Exception]
) -> None:
    case = _case(tmp_path)
    executor = _LockedExecutor(passes)
    runner = LockedCodexOfflineJudgeRunner(
        gateway_responses_url="https://gateway.example.test/v1/responses",
        shim_port=4444,
        executor=executor,
        settlement_reader=_Settlement(),
    )
    with pytest.raises(error):
        runner.run(
            OfflineJudgeRunRequest(
                case.request,
                load_mounted_offline_judge_inputs(case.request, case.paths),
                case.profile,
                ProviderAssetBundle.load(case.paths.asset_root),
                b"locked prompt\n",
            )
        )
    assert executor.cleaned[0][1:] == (30, True)
