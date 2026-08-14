from __future__ import annotations

import hashlib
import signal
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.integrations.behavior.canonical_json import (
    canonical_digest,
    canonical_document,
    digest_bytes,
)
from loom.integrations.behavior.cli import dispatch_stage
from loom.integrations.behavior.contracts import (
    BehaviorDatasetSnapshotArtifactV1,
    BehaviorPolicyCheckpointArtifactV1,
    BehaviorRolloutParametersV1,
    BehaviorTaskInstanceArtifactV1,
    StageRequestV1,
)
from loom.integrations.behavior.errors import BehaviorContractError, BehaviorInterruptedError
from loom.integrations.behavior.stages.pipeline_mode import (
    SEED_CALL_ORDER,
    SeedAuthority,
    apply_seed,
)
from loom.integrations.behavior.stages.rollout import (
    BddlTransitionsDocumentV1,
    FfmpegCompositeBuilder,
    Hdf5Facts,
    RolloutAdapter,
    RolloutGpuV1,
    RolloutPaths,
    RolloutRuntimeContractV1,
    RolloutSupervisor,
    VideoFacts,
    build_engine_argv,
    build_simulator_env,
    build_vla_argv,
    build_vla_env,
    load_mounted_inputs,
    project_scene,
    validate_rollout_request,
    wait_for_vla_ready,
)
from loom.integrations.behavior.stages.rollout_engine import (
    LoadedTaskInstance,
    execute_one_episode,
)
from loom.pipeline.keys import canonical_identity

D0 = "sha256:" + "0" * 64
D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
D3 = "sha256:" + "3" * 64
_TRO_BYTES = (
    b'{"robot_poses":{"R1Pro":[{"orientation":[0.0,0.0,0.0,1.0],'
    b'"position":[1.0,2.0,3.0]}]}}\n'
)
IMAGE = "registry.example.com/loom/behavior@sha256:" + "4" * 64
RUN_ID = UUID(int=1)
STAGE_RUN_ID = UUID(int=2)
ATTEMPT_ID = UUID(int=3)


def _control_provenance() -> dict[str, object]:
    return {
        "producer_kind": "control",
        "loom_commit_sha": "5" * 40,
        "control_event_id": str(UUID(int=90)),
        "actor_id": str(UUID(int=91)),
        "recipe_digest": D0,
        "source_artifacts": [],
    }


def _dataset_document(bddl: bytes) -> BehaviorDatasetSnapshotArtifactV1:
    task_ids = [7]
    cards = [
        {
            "behavior_task_id": 7,
            "task_name": "placing_can",
            "relative_path": "agentic_sweep/task_cards/task-0007.md",
            "sha256": D0,
            "size_bytes": 1,
        }
    ]
    videos = [
        {
            "behavior_task_id": 7,
            "episodes": [
                {
                    "episode_id": "episode_00070371",
                    "files": [
                        {
                            "camera": camera,
                            "relative_path": f"demo/{camera}.mp4",
                            "sha256": D0,
                            "size_bytes": 1,
                            "frame_count": 2,
                        }
                        for camera in ("head", "left_wrist", "right_wrist")
                    ],
                }
            ],
        }
    ]
    instances = [
        {
            "behavior_task_id": 7,
            "task_name": "placing_can",
            "engine_task_instance_ids": [37],
        }
    ]
    compatibility = {
        "kind": "dataset",
        "omnigibson_version": "3.8",
        "dataset_schema_version": "behavior_dataset_snapshot.v1",
        "omnigibson_dataset_layout_version": "behavior_1k_runtime_v1",
        "omnigibson_dataset_root": "payload/omnigibson",
        "runtime_roots": [
            {
                "name": "behavior-1k-assets",
                "relative_path": "omnigibson/behavior-1k-assets",
                "revision": "r1",
                "tree_sha256": D0,
            },
            {
                "name": "2025-challenge-task-instances",
                "relative_path": "omnigibson/2025-challenge-task-instances",
                "revision": "r1",
                "tree_sha256": D0,
                "episodes_jsonl_sha256": D0,
                "test_instances_csv_sha256": D0,
                "scenes_tree_sha256": D0,
            },
        ],
        "task_universe_sha256": canonical_digest(task_ids, persisted=False),
        "task_count": 1,
        "agentic_task_cards": cards,
        "agentic_task_cards_sha256": canonical_digest(cards, persisted=False),
        "agentic_demo_video_sets": videos,
        "agentic_demo_video_sets_sha256": canonical_digest(videos, persisted=False),
        "test_instance_sets": instances,
        "test_instance_sets_sha256": canonical_digest(instances, persisted=False),
    }
    return BehaviorDatasetSnapshotArtifactV1.model_validate_json(
        canonical_document(
            {
                "schema_version": "behavior_dataset_snapshot.v1",
                "payload": {
                    "name": "dataset",
                    "version": "1",
                    "source_provenance": {"type": "artifact", "locator": "local", "revision": "r1"},
                    "compatibility": compatibility,
                },
                "files": [
                    {
                        "name": "stage1_tro",
                        "relative_path": (
                            "payload/omnigibson/2025-challenge-task-instances/scenes/"
                            "house_double_floor_lower/json/"
                            "house_double_floor_lower_task_placing_can_instances/"
                            "house_double_floor_lower_task_placing_can_0_37_"
                            "template-tro_state.json"
                        ),
                        "sha256": digest_bytes(_TRO_BYTES),
                        "size_bytes": len(_TRO_BYTES),
                        "media_type": "application/json",
                        "required": True,
                    },
                    {
                        "name": "source_bddl",
                        "relative_path": (
                            "payload/omnigibson/2025-challenge-task-instances/task.bddl"
                        ),
                        "sha256": digest_bytes(bddl),
                        "size_bytes": len(bddl),
                        "media_type": "text/plain",
                        "required": True,
                    }
                ],
                "provenance": _control_provenance(),
            }
        )
    )


def _policy_document(checkpoint: bytes) -> BehaviorPolicyCheckpointArtifactV1:
    tree = [
        {
            "relative_path": "weights.bin",
            "sha256": digest_bytes(checkpoint),
            "size_bytes": len(checkpoint),
        }
    ]
    return BehaviorPolicyCheckpointArtifactV1.model_validate_json(
        canonical_document(
            {
                "schema_version": "behavior_policy_checkpoint.v1",
                "payload": {
                    "name": "policy",
                    "version": "1",
                    "source_provenance": {"type": "artifact", "locator": "local", "revision": "r1"},
                    "compatibility": {
                        "kind": "policy",
                        "architecture": "pi_behavior_b1k_fast",
                        "action_dim": 23,
                        "state_dim": 23,
                        "robot_action_dim": 25,
                        "checkpoint_format": "openpi_checkpoint_directory_v1",
                        "checkpoint_root": "payload/checkpoint",
                        "checkpoint_tree_sha256": canonical_digest(tree, persisted=False),
                        "model_identifier": "pi-b1k",
                        "vla_interface_version": "behavior_b1k_websocket_v1",
                        "controller_adapter_version": "r1pro_25_to_pi23_v1",
                    },
                },
                "files": [
                    {
                        "name": "weights",
                        "relative_path": "payload/checkpoint/weights.bin",
                        "sha256": digest_bytes(checkpoint),
                        "size_bytes": len(checkpoint),
                        "media_type": "application/octet-stream",
                        "required": True,
                    }
                ],
                "provenance": _control_provenance(),
            }
        )
    )


def _task_document(seed_base: int = 11) -> BehaviorTaskInstanceArtifactV1:
    task_checksum = D1
    bundle_digest = D2
    eval_index = 0
    engine_id = 37
    episode_index = 1
    seed_preimage = {
        "engine_task_instance_id": engine_id,
        "episode_index": episode_index,
        "eval_instance_index": eval_index,
        "seed_base": seed_base,
        "task_checksum": task_checksum,
    }
    seed = int.from_bytes(hashlib.sha256(canonical_identity(seed_preimage)).digest()[:4], "big")
    demo_id = 7 * 10_000 + engine_id * 10 + episode_index
    identity = canonical_digest(
        {
            "behavior_task_id": 7,
            "demo_id": demo_id,
            "engine_task_instance_id": engine_id,
            "episode_index": episode_index,
            "eval_instance_index": eval_index,
            "recipe_digest": D0,
            "seed": seed,
            "task_bundle_digest": bundle_digest,
        },
        persisted=False,
    ).removeprefix("sha256:")
    return BehaviorTaskInstanceArtifactV1.model_validate_json(
        canonical_document(
            {
                "schema_version": "behavior_task_instance.v1",
                "payload": {
                    "source_task_set": {
                        "task_set_id": "taskset",
                        "owning_team_id": str(UUID(int=80)),
                        "manifest_generation": 1,
                        "manifest_sha256": D0,
                        "intents": ["evaluation", "trajectory_generation"],
                        "evaluation_ready": True,
                    },
                    "loom_task_id": "loom-task-7",
                    "behavior_task_id": 7,
                    "task_name": "placing_can",
                    "semantic_task_id": "placing_can",
                    "task_checksum": task_checksum,
                    "task_bundle_digest": bundle_digest,
                    "task_bundle": {
                        "kind": "object",
                        "object_sha256": bundle_digest,
                        "size_bytes": 1,
                    },
                    "source_bddl_path": ("omnigibson/2025-challenge-task-instances/task.bddl"),
                    "eval_instance_index": eval_index,
                    "engine_task_instance_id": engine_id,
                    "episode_index": episode_index,
                    "demo_id": demo_id,
                    "demo_stem": f"episode_{demo_id:08d}",
                    "seed": seed,
                    "task_instance_identity": identity,
                    "materialization": {
                        "episodes_per_instance": 1,
                        "seed_base": seed_base,
                        "request_sha256": D0,
                    },
                    "recipe": {"name": "behavior-recovery", "version": 1, "digest": D0},
                    "lineage": {
                        "source_task_set_manifest_sha256": D0,
                        "task_bundle": {
                            "kind": "object",
                            "object_sha256": bundle_digest,
                            "size_bytes": 1,
                        },
                        "materialization_request_sha256": D0,
                        "dataset_content_sha256": D0,
                        "policy_content_sha256": D0,
                        "mop_bank_content_sha256": D0,
                    },
                },
                "files": [],
                "provenance": _control_provenance(),
            }
        )
    )


def _request(
    task: BehaviorTaskInstanceArtifactV1,
    dataset: BehaviorDatasetSnapshotArtifactV1,
    policy: BehaviorPolicyCheckpointArtifactV1,
) -> StageRequestV1:
    documents: list[Any] = [task, dataset, policy]
    names = ["task_instance", "dataset", "policy"]
    types = [
        "behavior_task_instance.v1",
        "behavior_dataset_snapshot.v1",
        "behavior_policy_checkpoint.v1",
    ]
    bindings = [
        {
            "binding_name": name,
            "artifact_type": artifact_type,
            "cardinality": "one",
            "items": [
                {
                    "artifact_id": str(UUID(int=10 + index)),
                    "content_sha256": canonical_digest(document),
                    "file_count": len(document.files),
                    "item_key": "singleton",
                    "manifest_sha256": D0,
                    "stored_size_bytes": 1,
                    "unpacked_size_bytes": 1,
                }
            ],
        }
        for index, (name, artifact_type, document) in enumerate(
            zip(names, types, documents, strict=True)
        )
    ]
    resolved = canonical_digest(bindings)
    preimage = {
        "attempt_id": str(ATTEMPT_ID),
        "execution_spec_digest": D2,
        "resolved_input_bindings_digest": resolved,
        "stage_run_id": str(STAGE_RUN_ID),
    }
    child = task.payload
    return StageRequestV1.model_validate_json(
        canonical_document(
            {
                "schema_version": "behavior.stage-request.v1",
                "stage": "rollout",
                "run_id": str(RUN_ID),
                "stage_run_id": str(STAGE_RUN_ID),
                "attempt_id": str(ATTEMPT_ID),
                "idempotency_key": canonical_digest(preimage, persisted=False),
                "inputs": bindings,
                "parameters": {
                    "eval_instance_index": child.eval_instance_index,
                    "episode_index": child.episode_index,
                    "seed": child.seed,
                    "record_depth": False,
                    "recording_fps": 30,
                },
                "budget": {
                    "provider": None,
                    "gpu_seconds_limit": 0,
                    "final_output_bytes_limit": 16_777_216,
                    "checkpoint_bytes_limit": 0,
                    "timeout_seconds": 8_220,
                    "max_attempts": 2,
                },
                "provenance": {
                    "recipe_digest": D0,
                    "resolved_input_bindings_digest": resolved,
                    "execution_spec_digest": D2,
                    "image_digest": IMAGE,
                    "loom_commit_sha": "5" * 40,
                    "control_binding": None,
                    "compatibility_manifest_sha256": D3,
                },
                "orchestration": None,
            }
        )
    )


def _write_inputs(root: Path, task: Any, dataset: Any, policy: Any) -> None:
    for name, document in (("task_instance", task), ("dataset", dataset), ("policy", policy)):
        (root / name).mkdir(parents=True)
        (root / name / "artifact.json").write_bytes(canonical_document(document))
    bddl_path = (
        root / "dataset" / "payload" / "omnigibson" / "2025-challenge-task-instances" / "task.bddl"
    )
    bddl_path.parent.mkdir(parents=True)
    bddl_path.write_bytes(b"task")
    tro_path = (
        root
        / "dataset"
        / "payload"
        / "omnigibson"
        / "2025-challenge-task-instances"
        / "scenes"
        / "house_double_floor_lower"
        / "json"
        / "house_double_floor_lower_task_placing_can_instances"
        / "house_double_floor_lower_task_placing_can_0_37_template-tro_state.json"
    )
    tro_path.parent.mkdir(parents=True)
    tro_path.write_bytes(_TRO_BYTES)
    checkpoint = root / "policy" / "payload" / "checkpoint" / "weights.bin"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"checkpoint")


def _runtime(platform: Literal["oldlab", "gb10"] = "oldlab") -> RolloutRuntimeContractV1:
    if platform == "oldlab":
        devices = [
            RolloutGpuV1(logical_index=0, model="RTX 5080", roles=["sim"]),
            RolloutGpuV1(logical_index=1, model="RTX 5080", roles=["vla"]),
        ]
    else:
        devices = [RolloutGpuV1(logical_index=0, model="GB10", roles=["sim", "vla"])]
    return RolloutRuntimeContractV1(platform=platform, devices=devices, system_env={"LANG": "C"})


def _scene_source() -> dict[str, Any]:
    return {
        "state": {
            "registry": {
                "object_registry": {
                    "decoy": {"joint_pos": [0, 1]},
                    "robot": {"joint_pos": list(range(28))},
                }
            }
        },
        "metadata": {"inst_to_name": {"z_scope": "extra", "a_scope": "decoy"}},
    }


def _transition_document(task: BehaviorTaskInstanceArtifactV1, success: bool) -> dict[str, Any]:
    child = task.payload
    return {
        "task_name": child.task_name,
        "instance_id": child.engine_task_instance_id,
        "demo_id": child.demo_id,
        "total_steps": 2,
        "success": success,
        "transitions": [
            {
                "step_idx": 0,
                "predicate_id": "p0",
                "predicate_name": "OnTop",
                "old_value": False,
                "new_value": True,
                "obj_a": "can",
                "obj_b": "table",
                "args": None,
            },
            {
                "step_idx": 0,
                "predicate_id": "p1",
                "predicate_name": "IsGrasping",
                "old_value": False,
                "new_value": True,
                "args": [
                    {"scope_name": "robot_arm", "scene_name": "robot"},
                    {"scope_name": "object", "scene_name": "can"},
                ],
                "obj_a": None,
                "obj_b": None,
            },
        ],
        "grasp_history": [{"step_idx": 0, "arm": "left", "old_value": None, "new_value": "can"}],
    }


class _NoopSupervisor:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def run(self, **kwargs: object) -> None:
        self.calls.append(kwargs)


class _Hdf5:
    def __init__(self, seed: int) -> None:
        self.seed = seed

    def inspect(self, _path: Path) -> Hdf5Facts:
        return Hdf5Facts(step_count=2, seed=self.seed, scene_source=_scene_source())


class _Videos:
    def __init__(self, *, mismatch: bool = False) -> None:
        self.mismatch = mismatch

    def inspect(self, path: Path) -> VideoFacts:
        composite = path.name == "rgb_composite.mp4"
        timestamps = ("0.000000", "0.033333")
        if self.mismatch and "right_wrist" in str(path):
            timestamps = ("0.000000", "0.040000")
        return VideoFacts(
            frame_count=2,
            timestamps=timestamps,
            fps=30,
            codec="h264",
            pixel_format="yuv420p",
            width=672 if composite else 224,
            height=448 if composite else 224,
        )


class _Composite:
    def build(self, _left: Path, _right: Path, _head: Path, output: Path) -> None:
        output.write_bytes(b"composite")


def _adapter_fixture(
    tmp_path: Path,
    *,
    success: bool = True,
    mismatch: bool = False,
    engine_uses_output_root: bool = False,
) -> tuple[StageRequestV1, Path, RolloutAdapter, _NoopSupervisor]:
    task = _task_document()
    dataset = _dataset_document(b"task")
    policy = _policy_document(b"checkpoint")
    request = _request(task, dataset, policy)
    input_root = tmp_path / "inputs"
    output_root = tmp_path / "outputs"
    output_root.mkdir()
    engine_root = output_root if engine_uses_output_root else tmp_path / "engine"
    _write_inputs(input_root, task, dataset, policy)
    child = task.payload
    task_tag = f"task-{child.behavior_task_id:04d}"
    stem = child.demo_stem
    files = {
        engine_root / "payload" / "trajectories" / task_tag / f"{stem}.hdf5": b"hdf5",
        engine_root
        / "payload"
        / "meta"
        / "episodes"
        / task_tag
        / f"{stem}_bddl_transitions.json": canonical_document(_transition_document(task, success)),
        engine_root
        / "payload"
        / "videos"
        / task_tag
        / "observation.images.rgb.head"
        / f"{stem}.mp4": b"head",
        engine_root
        / "payload"
        / "videos"
        / task_tag
        / "observation.images.rgb.left_wrist"
        / f"{stem}.mp4": b"left",
        engine_root
        / "payload"
        / "videos"
        / task_tag
        / "observation.images.rgb.right_wrist"
        / f"{stem}.mp4": b"right",
    }
    for path, value in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    supervisor = _NoopSupervisor()
    adapter = RolloutAdapter(
        runtime=_runtime(),
        paths=RolloutPaths(input_root, engine_root, tmp_path / "scratch" / "rollout"),
        supervisor=supervisor,  # type: ignore[arg-type]
        hdf5=_Hdf5(child.seed),
        video=_Videos(mismatch=mismatch),
        composite=_Composite(),
    )
    return request, output_root, adapter, supervisor


def test_request_requires_exact_three_scalar_bindings_and_selector_identity(tmp_path: Path) -> None:
    task = _task_document()
    dataset = _dataset_document(b"task")
    policy = _policy_document(b"checkpoint")
    request = _request(task, dataset, policy)
    assert validate_rollout_request(request).task_instance.binding_name == "task_instance"
    input_root = tmp_path / "inputs"
    _write_inputs(input_root, task, dataset, policy)
    mounted = load_mounted_inputs(
        request, RolloutPaths(input_root, tmp_path / "engine", tmp_path / "scratch" / "rollout")
    )
    assert mounted.task_instance.payload.engine_task_instance_id == 37

    for inputs in (request.inputs[:-1], list(reversed(request.inputs))):
        with pytest.raises(BehaviorContractError, match="exact three"):
            validate_rollout_request(request.model_copy(update={"inputs": inputs}))

    wrong_task = deepcopy(task.model_dump(mode="json"))
    wrong_task["payload"]["engine_task_instance_id"] = 38
    with pytest.raises(ValidationError, match="demo_id"):
        BehaviorTaskInstanceArtifactV1.model_validate_json(canonical_document(wrong_task))


def test_transition_union_order_bounds_and_stale_shape_are_closed() -> None:
    task = _task_document()
    value = _transition_document(task, False)
    parsed = BddlTransitionsDocumentV1.model_validate(value)
    assert [item.predicate_id for item in parsed.transitions] == ["p0", "p1"]
    stale = deepcopy(value)
    stale["grasp_history"][0]["object"] = "can"
    with pytest.raises(ValidationError, match="Extra inputs"):
        BddlTransitionsDocumentV1.model_validate(stale)
    dormant = deepcopy(value)
    dormant["transitions"][0]["args"] = [
        {"scope_name": "a", "scene_name": "a"},
        {"scope_name": "b", "scene_name": "b"},
    ]
    with pytest.raises(ValidationError, match="dormant"):
        BddlTransitionsDocumentV1.model_validate(dormant)


def test_scene_projection_preserves_registry_order_and_sorts_only_identity_map() -> None:
    projected = project_scene(
        _scene_source(), task_name="placing_can", instance_id=37, demo_id=70371
    )
    assert [item.scene_name for item in projected.state_objects] == ["decoy", "robot", "extra"]
    assert [item.scope_name for item in projected.inst_to_name] == ["a_scope", "z_scope"]
    assert projected.robot_scene_name == "robot"
    duplicate = _scene_source()
    duplicate["metadata"]["inst_to_name"] = {"a": "same", "b": "same"}
    with pytest.raises(ValidationError, match="unique"):
        project_scene(duplicate, task_name="placing_can", instance_id=37, demo_id=70371)
    no_robot = _scene_source()
    no_robot["state"]["registry"]["object_registry"]["robot"]["joint_pos"] = [0]
    with pytest.raises(BehaviorContractError, match="exactly one"):
        project_scene(no_robot, task_name="placing_can", instance_id=37, demo_id=70371)


def test_seed_order_same_seed_replay_and_vla_reseed_rejection() -> None:
    calls: list[tuple[str, int]] = []
    numpy = SimpleNamespace(random=SimpleNamespace(seed=lambda seed: calls.append(("numpy", seed))))
    torch = SimpleNamespace(
        manual_seed=lambda seed: calls.append(("torch", seed)),
        cuda=SimpleNamespace(manual_seed_all=lambda seed: calls.append(("cuda", seed))),
    )
    kwargs: dict[str, Any] = {
        "random_seed": lambda seed: calls.append(("random", seed)),
        "numpy_module": numpy,
        "torch_module": torch,
        "simulator_seed": lambda seed: calls.append(("sim", seed)),
    }
    receipt = apply_seed(0, **kwargs)
    assert receipt.calls == SEED_CALL_ORDER
    assert [name for name, _ in calls] == ["random", "numpy", "torch", "cuda", "sim"]
    simulator = SeedAuthority(allow_same_seed_replay=True)
    assert simulator.apply(1, **kwargs) == simulator.apply(1, **kwargs)
    with pytest.raises(BehaviorContractError, match="drift"):
        simulator.apply(2, **kwargs)
    vla = SeedAuthority(allow_same_seed_replay=False)
    vla.apply(1, **kwargs)
    with pytest.raises(BehaviorContractError, match="only once"):
        vla.apply(1, **kwargs)


def test_fixed_argv_env_gpu_topologies_and_ffmpeg_options(tmp_path: Path) -> None:
    oldlab = _runtime("oldlab")
    gb10 = _runtime("gb10")
    assert build_vla_argv(7, 1) == (
        "/opt/loom/venv-vla/bin/python",
        "-m",
        "loom.integrations.behavior.vla.server",
        "--task-id",
        "7",
        "--seed",
        "1",
        "--port",
        "8000",
        "policy:checkpoint",
        "--policy.config",
        "pi_behavior_b1k_fast",
        "--policy.dir",
        "/inputs/policy/payload/checkpoint",
    )
    assert build_engine_argv() == (
        "/opt/loom/bin/sim-python",
        "-m",
        "loom.integrations.behavior.stages.rollout_engine",
        "--request",
        "/inputs/stage-request.json",
        "--output-dir",
        "/outputs",
        "--scratch",
        "/scratch/rollout",
    )
    assert build_vla_env(oldlab)["CUDA_VISIBLE_DEVICES"] == "1"
    assert build_vla_env(oldlab)["OMNIGIBSON_DATA_PATH"] == (
        "/inputs/dataset/payload/omnigibson"
    )
    assert build_simulator_env(oldlab)["CUDA_VISIBLE_DEVICES"] == "0,1"
    assert build_vla_env(gb10)["CUDA_VISIBLE_DEVICES"] == "0"
    assert build_simulator_env(gb10)["CUDA_VISIBLE_DEVICES"] == "0"
    assert "SLURM_JOB_ID" not in build_simulator_env(oldlab)
    command = FfmpegCompositeBuilder("ffmpeg").command(
        tmp_path / "left", tmp_path / "right", tmp_path / "head", tmp_path / "out"
    )
    output_start = command.index("-c:v")
    assert command[output_start:-1] == FfmpegCompositeBuilder.OUTPUT_OPTIONS


class _Process:
    def __init__(self, pid: int, polls: list[int | None]) -> None:
        self.pid = pid
        self.polls = polls
        self.index = 0
        self.waited = False

    def poll(self) -> int | None:
        result = self.polls[min(self.index, len(self.polls) - 1)]
        self.index += 1
        return result

    def wait(self, timeout: float | None = None) -> int:
        self.waited = True
        return self.polls[-1] or 0


class _ManagedProcess:
    def __init__(self, pid: int, *, exit_immediately: bool = False) -> None:
        self.pid = pid
        self.stopped = exit_immediately
        self.waited = False

    def poll(self) -> int | None:
        return 0 if self.stopped else None

    def wait(self, timeout: float | None = None) -> int:
        self.waited = True
        return 0


class _Launcher:
    def __init__(self, vla: _ManagedProcess, engine: _ManagedProcess) -> None:
        self.processes = [vla, engine]
        self.calls: list[tuple[tuple[str, ...], int | None]] = []

    def start(self, argv: Any, *, env: Any, process_group: int | None) -> _ManagedProcess:
        self.calls.append((tuple(argv), process_group))
        return self.processes[len(self.calls) - 1]


def test_vla_readiness_exact_180_tcp_probes_and_early_exit() -> None:
    attempts: list[tuple[str, int]] = []
    process = _Process(10, [None])

    def never_ready(host: str, port: int) -> bool:
        attempts.append((host, port))
        return False

    with pytest.raises(BehaviorContractError, match="180"):
        wait_for_vla_ready(
            process,
            connect=never_ready,
            sleep=lambda _seconds: None,
        )
    assert attempts == [("127.0.0.1", 8000)] * 180
    with pytest.raises(BehaviorContractError, match="exited"):
        wait_for_vla_ready(_Process(10, [1]), connect=lambda _host, _port: True)


def test_supervisor_joins_one_process_group_and_reaps_on_success_and_timeout() -> None:
    vla = _ManagedProcess(40)
    engine = _ManagedProcess(41, exit_immediately=True)
    launcher = _Launcher(vla, engine)
    signals: list[tuple[int, int]] = []

    def stop(group: int, signum: int) -> None:
        signals.append((group, signum))
        for process in launcher.processes:
            process.stopped = True

    RolloutSupervisor(
        launcher=launcher,
        connect=lambda _host, _port: True,
        sleep=lambda _seconds: None,
        signal_group=stop,
    ).run(vla_argv=("vla",), vla_env={}, engine_argv=("engine",), engine_env={})
    assert launcher.calls == [(("vla",), None), (("engine",), 40)]
    assert signals == [(40, 2)]
    assert all(process.waited for process in launcher.processes)

    vla = _ManagedProcess(50)
    engine = _ManagedProcess(51)
    launcher = _Launcher(vla, engine)
    signals = []
    times = iter((0.0, 8_100.0, 8_101.0))

    def stop_timeout(group: int, signum: int) -> None:
        signals.append((group, signum))
        for process in launcher.processes:
            process.stopped = True

    with pytest.raises(BehaviorContractError, match="8100"):
        RolloutSupervisor(
            launcher=launcher,
            connect=lambda _host, _port: True,
            monotonic=lambda: next(times),
            sleep=lambda _seconds: None,
            signal_group=stop_timeout,
        ).run(vla_argv=("vla",), vla_env={}, engine_argv=("engine",), engine_env={})
    assert signals == [(50, 2)]
    assert all(process.waited for process in launcher.processes)

    vla = _ManagedProcess(60)
    engine = _ManagedProcess(61)
    launcher = _Launcher(vla, engine)
    signals = []

    def stop_cancelled(group: int, signum: int) -> None:
        signals.append((group, signum))
        for process in launcher.processes:
            process.stopped = True

    with pytest.raises(BehaviorInterruptedError) as interrupted:
        RolloutSupervisor(
            launcher=launcher,
            connect=lambda _host, _port: True,
            cancelled=lambda: True,
            sleep=lambda _seconds: None,
            signal_group=stop_cancelled,
        ).run(vla_argv=("vla",), vla_env={}, engine_argv=("engine",), engine_env={})
    assert interrupted.value.signum == signal.SIGTERM
    assert signals == [(60, 2)]
    assert all(process.waited for process in launcher.processes)


def test_engine_uses_signed_episode_index_not_range_one() -> None:
    task = _task_document()
    request = _request(task, _dataset_document(b"task"), _policy_document(b"checkpoint"))
    events: list[object] = []

    class Driver:
        def load_task_instance(self, value: StageRequestV1) -> LoadedTaskInstance:
            events.append("load")
            params = cast(BehaviorRolloutParametersV1, value.parameters)
            return LoadedTaskInstance(
                eval_instance_index=params.eval_instance_index,
                engine_task_instance_id=37,
                episode_index=params.episode_index,
                seed=params.seed,
            )

        def reset_episode(self, episode_index: int) -> None:
            events.append(("reset", episode_index))

        def run_episode(self, episode_index: int, **_kwargs: object) -> int:
            events.append(("run", episode_index))
            return 0

        def close(self) -> None:
            events.append("close")

    authority = SimpleNamespace(apply=lambda seed: events.append(("seed", seed)))
    assert (
        execute_one_episode(
            request,
            Driver(),
            output_dir=Path("/outputs"),
            scratch=Path("/scratch/rollout"),
            seed_authority=authority,  # type: ignore[arg-type]
        )
        == 0
    )
    assert ("reset", 1) in events and ("run", 1) in events
    assert [item for item in events if isinstance(item, tuple) and item[0] == "seed"] == [
        ("seed", task.payload.seed),
        ("seed", task.payload.seed),
    ]


def test_engine_offers_preview_only_through_additive_image_driver_seam() -> None:
    task = _task_document()
    request = _request(task, _dataset_document(b"task"), _policy_document(b"checkpoint"))
    offered: list[tuple[int, bytes]] = []

    class Preview:
        def offer(self, *, step_idx: int, jpeg: bytes) -> None:
            offered.append((step_idx, jpeg))

    class Driver:
        def load_task_instance(self, value: StageRequestV1) -> LoadedTaskInstance:
            params = cast(BehaviorRolloutParametersV1, value.parameters)
            return LoadedTaskInstance(
                eval_instance_index=params.eval_instance_index,
                engine_task_instance_id=37,
                episode_index=params.episode_index,
                seed=params.seed,
            )

        def reset_episode(self, _episode_index: int) -> None:
            pass

        def run_episode(self, _episode_index: int, **_kwargs: object) -> int:
            pytest.fail("preview-capable driver must receive the explicit sink")

        def run_episode_with_live_preview(
            self,
            _episode_index: int,
            *,
            live_preview: Preview,
            **_kwargs: object,
        ) -> int:
            live_preview.offer(step_idx=7, jpeg=b"composite")
            return 0

        def close(self) -> None:
            pass

    authority = SimpleNamespace(apply=lambda _seed: None)
    assert (
        execute_one_episode(
            request,
            Driver(),
            output_dir=Path("/outputs"),
            scratch=Path("/scratch/rollout"),
            seed_authority=authority,  # type: ignore[arg-type]
            live_preview=Preview(),
        )
        == 0
    )
    assert offered == [(7, b"composite")]


@pytest.mark.parametrize(
    ("success", "outcome"), [(True, "rollout_success"), (False, "rollout_failure")]
)
def test_adapter_commits_success_and_policy_failure_atomically(
    tmp_path: Path, success: bool, outcome: str
) -> None:
    request, output_root, adapter, supervisor = _adapter_fixture(tmp_path, success=success)
    committed = dispatch_stage(
        request,
        output_root,
        adapter=adapter,
        output_declarations={"rollout": "behavior_rollout_bundle.v1"},
    )
    assert committed.stage_result.domain_outcome == outcome
    artifact = output_root / "artifacts" / "rollout" / "artifact.json"
    document = canonical_document(__import__("json").loads(artifact.read_bytes()))
    assert artifact.read_bytes() == document
    assert (output_root / "artifacts" / "rollout" / "payload" / "rgb_composite.mp4").is_file()
    assert not (tmp_path / "scratch" / "rollout").exists()
    assert not (tmp_path / "scratch" / "cache").exists()
    parameters = cast(BehaviorRolloutParametersV1, request.parameters)
    assert supervisor.calls[0]["vla_argv"] == build_vla_argv(7, parameters.seed)

    # Exact-key replay returns the committed attempt without invoking the adapter again.
    replay = dispatch_stage(
        request,
        output_root,
        adapter=lambda *_args: pytest.fail("replay must not start children"),
        output_declarations={"rollout": "behavior_rollout_bundle.v1"},
    )
    assert replay.complete == committed.complete


def test_camera_timestamp_mismatch_fails_without_partial_rollout(tmp_path: Path) -> None:
    request, output_root, adapter, _supervisor = _adapter_fixture(tmp_path, mismatch=True)
    with pytest.raises(BehaviorContractError, match="camera"):
        dispatch_stage(
            request,
            output_root,
            adapter=adapter,
            output_declarations={"rollout": "behavior_rollout_bundle.v1"},
        )
    assert not (output_root / "COMPLETE.json").exists()
    assert not (output_root / ".partial").exists()


def test_adapter_removes_engine_staging_before_attempt_commit(tmp_path: Path) -> None:
    request, output_root, adapter, _supervisor = _adapter_fixture(
        tmp_path, engine_uses_output_root=True
    )
    committed = dispatch_stage(
        request,
        output_root,
        adapter=adapter,
        output_declarations={"rollout": "behavior_rollout_bundle.v1"},
    )
    assert committed.stage_result.domain_outcome == "rollout_success"
    assert not (output_root / "payload").exists()
    assert (output_root / "artifacts" / "rollout" / "artifact.json").is_file()


def test_runtime_topology_is_closed() -> None:
    with pytest.raises(ValidationError, match="count/model/roles"):
        RolloutRuntimeContractV1(
            platform="oldlab",
            devices=[RolloutGpuV1(logical_index=0, model="RTX 5080", roles=["sim", "vla"])],
            system_env={},
        )
    with pytest.raises(ValidationError, match="unapproved"):
        RolloutRuntimeContractV1(
            platform="gb10",
            devices=[RolloutGpuV1(logical_index=0, model="GB10", roles=["sim", "vla"])],
            system_env={"API_KEY": "not-allowed"},
        )
