"""Deterministic one-episode rollout adapter for BEHAVIOR Stage 1.

The adapter owns request validation, mounted-input identity, process topology,
bounded artifact extraction, and the final closed rollout document.  It never
submits Slurm work, performs fan-out, fetches source/data, or uploads outputs.
"""

from __future__ import annotations

import contextlib
import json
import operator
import os
import shutil
import signal
import socket
import stat
import subprocess
import time
import unicodedata
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal, Protocol, cast

from pydantic import Field, field_validator, model_validator

from loom.integrations.behavior.canonical_json import (
    canonical_digest,
    canonical_document,
    digest_bytes,
    load_canonical_document,
)
from loom.integrations.behavior.cli import StageAdapterBinding
from loom.integrations.behavior.contracts import (
    ArtifactFileV1,
    ArtifactRefV1,
    BehaviorDatasetSnapshotArtifactV1,
    BehaviorPolicyCheckpointArtifactV1,
    BehaviorRolloutBundleArtifactV1,
    BehaviorRolloutBundlePayloadV1,
    BehaviorRolloutParametersV1,
    BehaviorStage,
    BehaviorTaskInstanceArtifactV1,
    ContentArtifactRefV1,
    DatasetCompatibilityV1,
    PipelineArtifactProvenanceV1,
    PolicyCompatibilityV1,
    RolloutDataFileDescriptorV1,
    RolloutPolicyIdentityV1,
    RolloutRuntimeV1,
    RolloutVideoFileDescriptorV1,
    StageRequestV1,
)
from loom.integrations.behavior.errors import BehaviorContractError, BehaviorInterruptedError
from loom.pipeline.spec import BindingSetV1, PipelineModel
from loom.pipeline.state import (
    RetryClass,
    StageResultInputV1,
    StageResultOutputV1,
    StageResultProvenanceV1,
    StageResultV1,
)
from loom_worker.pipeline_attempt_workspace import AttemptWorkspace

SIMULATOR_PYTHON = "/opt/loom/bin/sim-python"
VLA_PYTHON = "/opt/loom/venv-vla/bin/python"
WORKDIR = "/opt/loom"
INPUT_ROOT = Path("/inputs")
OUTPUT_ROOT = Path("/outputs")
SCRATCH_ROOT = Path("/scratch/rollout")
VLA_PORT = 8000
VLA_PROBE_ATTEMPTS = 180
VLA_PROBE_INTERVAL_SECONDS = 1.0
ENGINE_DEADLINE_SECONDS = 8_100.0
ENGINE_KILL_GRACE_SECONDS = 120.0
SCENE_BYTES_LIMIT = 16_777_216
MAX_TRANSITION_RECORDS = 10_000_000
ROLLOUT_OUTPUT_DECLARATIONS: Mapping[str, str] = {
    "rollout": "behavior_rollout_bundle.v1"
}

_BASE_SYSTEM_ENV_KEYS = frozenset(
    {
        "LANG",
        "LC_ALL",
        "PATH",
        "LD_LIBRARY_PATH",
        "NVIDIA_DRIVER_CAPABILITIES",
        "TZ",
    }
)
_CACHE_DIRECTORY_PARTS = (
    ("omnigibson", "appdata"),
    ("home",),
    ("cache", "xdg"),
    ("cache", "mpl"),
    ("cache", "inductor"),
    ("cache", "triton"),
    ("cache", "gl-shader"),
)


def _nfc(value: str, *, label: str, max_bytes: int = 512) -> str:
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{label} must already be NFC")
    if not value or len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{label} is empty or exceeds {max_bytes} UTF-8 bytes")
    return value


class RolloutGpuV1(PipelineModel):
    logical_index: Annotated[int, Field(strict=True, ge=0, le=1)]
    model: Literal["RTX 5080", "GB10"]
    roles: Annotated[list[Literal["sim", "vla"]], Field(min_length=1, max_length=2)]

    @field_validator("roles")
    @classmethod
    def roles_are_fixed(cls, values: list[str]) -> list[str]:
        if values not in (["sim"], ["vla"], ["sim", "vla"]):
            raise ValueError("GPU roles are not in the fixed order")
        return values


class RolloutRuntimeContractV1(PipelineModel):
    platform: Literal["oldlab", "gb10"]
    devices: Annotated[list[RolloutGpuV1], Field(min_length=1, max_length=2)]
    system_env: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_topology(self) -> RolloutRuntimeContractV1:
        expected = (
            [
                RolloutGpuV1(logical_index=0, model="RTX 5080", roles=["sim"]),
                RolloutGpuV1(logical_index=1, model="RTX 5080", roles=["vla"]),
            ]
            if self.platform == "oldlab"
            else [RolloutGpuV1(logical_index=0, model="GB10", roles=["sim", "vla"])]
        )
        if self.devices != expected:
            raise ValueError("GPU count/model/roles disagree with the selected platform")
        if set(self.system_env) - _BASE_SYSTEM_ENV_KEYS:
            raise ValueError("runtime contract contains an unapproved system environment key")
        for key, value in self.system_env.items():
            _nfc(key, label="environment key", max_bytes=128)
            _nfc(value, label="environment value", max_bytes=4096)
        return self

    @property
    def vla_visible_devices(self) -> str:
        return "1" if self.platform == "oldlab" else "0"

    @property
    def simulator_visible_devices(self) -> str:
        return "0,1" if self.platform == "oldlab" else "0"


@dataclass(frozen=True)
class RolloutPaths:
    input_root: Path = INPUT_ROOT
    engine_output_root: Path = OUTPUT_ROOT
    scratch_root: Path = SCRATCH_ROOT

    def artifact_root(self, binding_name: str) -> Path:
        return self.input_root / binding_name


@dataclass(frozen=True)
class RolloutBindings:
    task_instance: BindingSetV1
    dataset: BindingSetV1
    policy: BindingSetV1


@dataclass(frozen=True)
class MountedRolloutInputs:
    task_instance: BehaviorTaskInstanceArtifactV1
    dataset: BehaviorDatasetSnapshotArtifactV1
    policy: BehaviorPolicyCheckpointArtifactV1
    bindings: RolloutBindings


def validate_rollout_request(request: StageRequestV1) -> RolloutBindings:
    if request.stage is not BehaviorStage.ROLLOUT or not isinstance(
        request.parameters, BehaviorRolloutParametersV1
    ):
        raise BehaviorContractError("rollout adapter accepts only rollout StageRequestV1")
    expected = [
        ("task_instance", "behavior_task_instance.v1"),
        ("dataset", "behavior_dataset_snapshot.v1"),
        ("policy", "behavior_policy_checkpoint.v1"),
    ]
    actual = [(item.binding_name, item.artifact_type) for item in request.inputs]
    if actual != expected:
        raise BehaviorContractError("rollout requires the exact three scalar bindings")
    if any(
        item.cardinality != "one"
        or len(item.items) != 1
        or item.items[0].item_key != "singleton"
        for item in request.inputs
    ):
        raise BehaviorContractError("rollout inputs must be singleton scalar bindings")
    parameters = request.parameters
    if parameters.record_depth is not False or parameters.recording_fps != 30:
        raise BehaviorContractError("rollout recording parameters drifted")
    return RolloutBindings(*request.inputs)


def build_vla_argv(task_id: int, seed: int) -> tuple[str, ...]:
    if isinstance(task_id, bool) or not 0 <= task_id <= 9_999:
        raise BehaviorContractError("behavior_task_id is outside 0..9999")
    if isinstance(seed, bool) or not 0 <= seed <= 4_294_967_295:
        raise BehaviorContractError("seed is outside uint32")
    return (
        VLA_PYTHON,
        "-m",
        "loom.integrations.behavior.vla.server",
        "--task-id",
        str(task_id),
        "--seed",
        str(seed),
        "--port",
        "8000",
        "policy:checkpoint",
        "--policy.config",
        "pi_behavior_b1k_fast",
        "--policy.dir",
        "/inputs/policy/payload/checkpoint",
    )


def build_engine_argv() -> tuple[str, ...]:
    return (
        SIMULATOR_PYTHON,
        "-m",
        "loom.integrations.behavior.stages.rollout_engine",
        "--request",
        "/inputs/stage-request.json",
        "--output-dir",
        "/outputs",
        "--scratch",
        "/scratch/rollout",
    )


def _closed_env(runtime: RolloutRuntimeContractV1, values: Mapping[str, str]) -> dict[str, str]:
    overlap = set(values) & set(runtime.system_env)
    if overlap:
        raise BehaviorContractError(f"system environment overlaps stage keys: {sorted(overlap)}")
    return {**runtime.system_env, **values}


def build_vla_env(runtime: RolloutRuntimeContractV1) -> dict[str, str]:
    return _closed_env(
        runtime,
        {
            "CUDA_VISIBLE_DEVICES": runtime.vla_visible_devices,
            "HOME": "/scratch/home",
            "OMNIGIBSON_DATA_PATH": "/inputs/dataset/payload/omnigibson",
            "PYTHONPATH": "/opt/loom/src",
            "TMPDIR": "/scratch/tmp",
            "XDG_CACHE_HOME": "/scratch/cache/xdg",
        },
    )


def build_simulator_env(runtime: RolloutRuntimeContractV1) -> dict[str, str]:
    return _closed_env(
        runtime,
        {
            "CUDA_VISIBLE_DEVICES": runtime.simulator_visible_devices,
            "OMNIGIBSON_GPU_ID": "0",
            "OMNIGIBSON_DATA_PATH": "/inputs/dataset/payload/omnigibson",
            "OMNIGIBSON_HEADLESS": "1",
            "OMNI_KIT_ACCEPT_EULA": "YES",
            "HDF5_USE_FILE_LOCKING": "FALSE",
            "CUROBO_DISABLE_CUDA_LBFGS": "1",
            "PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True",
            "PYTHONPATH": "/opt/loom/src",
            "OMNIGIBSON_APPDATA_PATH": "/scratch/omnigibson/appdata",
            "HOME": "/scratch/home",
            "XDG_CACHE_HOME": "/scratch/cache/xdg",
            "MPLCONFIGDIR": "/scratch/cache/mpl",
            "TORCHINDUCTOR_CACHE_DIR": "/scratch/cache/inductor",
            "TMPDIR": "/scratch/tmp",
            "TRITON_CACHE_DIR": "/scratch/cache/triton",
            "__GL_SHADER_DISK_CACHE": "1",
            "__GL_SHADER_DISK_CACHE_SKIP_CLEANUP": "1",
            "__GL_SHADER_DISK_CACHE_PATH": "/scratch/cache/gl-shader",
        },
    )


def prepare_scratch(paths: RolloutPaths) -> None:
    base = paths.scratch_root.parent
    if not base.exists():
        base.mkdir(mode=0o700, parents=True, exist_ok=False)
    elif base.is_symlink() or not base.is_dir():
        raise BehaviorContractError("scratch root is not a real directory")
    directories = [
        paths.scratch_root,
        base / "omnigibson",
        base / "omnigibson" / "appdata",
        base / "home",
        base / "tmp",
        base / "cache",
        *(base.joinpath(*parts) for parts in _CACHE_DIRECTORY_PARTS if parts[0] == "cache"),
    ]
    for directory in directories:
        _mkdir_fresh_private(directory)


def cleanup_scratch(paths: RolloutPaths) -> None:
    base = paths.scratch_root.parent
    roots = {
        paths.scratch_root,
        base / "omnigibson",
        base / "cache",
        base / "home",
        base / "tmp",
    }
    for root in sorted(roots, key=lambda item: len(item.parts), reverse=True):
        with contextlib.suppress(FileNotFoundError):
            if root.is_symlink():
                raise BehaviorContractError("scratch cleanup encountered a symlink")
            shutil.rmtree(root)


def _mkdir_fresh_private(path: Path) -> None:
    if path.exists() or path.is_symlink():
        raise BehaviorContractError(f"scratch directory is not fresh: {path}")
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
    if stat.S_IMODE(path.stat(follow_symlinks=False).st_mode) != 0o700:
        raise BehaviorContractError("scratch directory mode is not 0700")


def _safe_relative(value: str, *, prefix: str = "payload") -> PurePosixPath:
    if not value or value.startswith("/") or "\\" in value or "\x00" in value:
        raise BehaviorContractError("path is not a safe relative POSIX path")
    path = PurePosixPath(value)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise BehaviorContractError("path contains an unsafe component")
    if not path.parts or path.parts[0] != prefix:
        raise BehaviorContractError(f"path must be below {prefix}/")
    return path


def _read_regular(path: Path, *, limit: int) -> bytes:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise BehaviorContractError(f"cannot open declared regular file: {path}") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise BehaviorContractError("declared input is not a private regular file")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(1_048_576, limit + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > limit:
                raise BehaviorContractError("declared file exceeds its byte limit")
        after = os.fstat(fd)
        if (before.st_dev, before.st_ino, before.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
        ):
            raise BehaviorContractError("declared file changed while being read")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _load_mounted_artifact(
    binding: BindingSetV1,
    root: Path,
    model: type[PipelineModel],
) -> PipelineModel:
    raw = load_canonical_document(root / "artifact.json")
    parsed = model.model_validate_json(canonical_document(raw))
    item = binding.items[0]
    if canonical_digest(parsed) != item.content_sha256:
        raise BehaviorContractError(f"{binding.binding_name} semantic content digest drift")
    return parsed


def load_mounted_inputs(request: StageRequestV1, paths: RolloutPaths) -> MountedRolloutInputs:
    bindings = validate_rollout_request(request)
    task = cast(
        BehaviorTaskInstanceArtifactV1,
        _load_mounted_artifact(
            bindings.task_instance,
            paths.artifact_root("task_instance"),
            BehaviorTaskInstanceArtifactV1,
        ),
    )
    dataset = cast(
        BehaviorDatasetSnapshotArtifactV1,
        _load_mounted_artifact(
            bindings.dataset,
            paths.artifact_root("dataset"),
            BehaviorDatasetSnapshotArtifactV1,
        ),
    )
    policy = cast(
        BehaviorPolicyCheckpointArtifactV1,
        _load_mounted_artifact(
            bindings.policy,
            paths.artifact_root("policy"),
            BehaviorPolicyCheckpointArtifactV1,
        ),
    )
    _validate_signed_identity(request, task, dataset)
    _validate_declared_bddl(task, dataset, paths.artifact_root("dataset"))
    return MountedRolloutInputs(task, dataset, policy, bindings)


def _validate_signed_identity(
    request: StageRequestV1,
    task: BehaviorTaskInstanceArtifactV1,
    dataset: BehaviorDatasetSnapshotArtifactV1,
) -> None:
    parameters = cast(BehaviorRolloutParametersV1, request.parameters)
    child = task.payload
    expected = (
        parameters.eval_instance_index,
        parameters.episode_index,
        parameters.seed,
        False,
        30,
    )
    actual = (
        child.eval_instance_index,
        child.episode_index,
        child.seed,
        parameters.record_depth,
        parameters.recording_fps,
    )
    if actual != expected:
        raise BehaviorContractError("task child and five rollout parameters disagree")
    compatibility = dataset.payload.compatibility
    if not isinstance(compatibility, DatasetCompatibilityV1):
        raise BehaviorContractError("dataset compatibility branch drift")
    rows = [
        row
        for row in compatibility.test_instance_sets
        if row.behavior_task_id == child.behavior_task_id
    ]
    if len(rows) != 1 or rows[0].task_name != child.task_name:
        raise BehaviorContractError("dataset has no unique matching signed task row")
    if parameters.eval_instance_index >= len(rows[0].engine_task_instance_ids):
        raise BehaviorContractError("eval_instance_index is outside the signed selector array")
    resolved = rows[0].engine_task_instance_ids[parameters.eval_instance_index]
    if resolved != child.engine_task_instance_id:
        raise BehaviorContractError("selector and engine_task_instance_id disagree")


def _validate_declared_bddl(
    task: BehaviorTaskInstanceArtifactV1,
    dataset: BehaviorDatasetSnapshotArtifactV1,
    dataset_root: Path,
) -> None:
    relative = f"payload/{task.payload.source_bddl_path}"
    path = _safe_relative(relative)
    allowed = (
        PurePosixPath("payload/omnigibson/behavior-1k-assets"),
        PurePosixPath("payload/omnigibson/2025-challenge-task-instances"),
    )
    if not any(path.is_relative_to(root) for root in allowed):
        raise BehaviorContractError("source_bddl_path is outside the two signed runtime roots")
    manifests = {item.relative_path: item for item in dataset.files}
    descriptor = manifests.get(relative)
    if descriptor is None:
        raise BehaviorContractError("source BDDL is not declared by the dataset manifest")
    value = _read_regular(dataset_root.joinpath(*path.parts), limit=descriptor.size_bytes)
    if len(value) != descriptor.size_bytes or digest_bytes(value) != descriptor.sha256:
        raise BehaviorContractError("source BDDL bytes disagree with the manifest")


class TransitionArgumentV1(PipelineModel):
    scope_name: str
    scene_name: str

    _scope = field_validator("scope_name")(
        lambda value: _nfc(value, label="transition scope", max_bytes=512)
    )
    _scene = field_validator("scene_name")(
        lambda value: _nfc(value, label="transition scene", max_bytes=512)
    )


class BddlTransitionV1(PipelineModel):
    step_idx: Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)]
    predicate_id: str
    predicate_name: str
    old_value: bool
    new_value: bool
    args: list[TransitionArgumentV1] | None = None
    obj_a: str | None = None
    obj_b: str | None = None

    @field_validator("predicate_id")
    @classmethod
    def predicate_id_nfc(cls, value: str) -> str:
        return _nfc(value, label="predicate_id", max_bytes=512)

    @field_validator("predicate_name")
    @classmethod
    def predicate_name_nfc(cls, value: str) -> str:
        return _nfc(value, label="predicate_name", max_bytes=128)

    @field_validator("obj_a", "obj_b")
    @classmethod
    def object_name_nfc(cls, value: str | None) -> str | None:
        return None if value is None else _nfc(value, label="predicate object", max_bytes=512)

    @model_validator(mode="after")
    def validate_union(self) -> BddlTransitionV1:
        if self.old_value == self.new_value:
            raise ValueError("transition values must differ")
        if self.predicate_name == "IsGrasping":
            if self.args is None or len(self.args) != 2 or self.obj_a is not None or self.obj_b is not None:
                raise ValueError("IsGrasping requires exactly two ordered args")
        elif self.args is not None:
            raise ValueError("goal args are dormant for non-IsGrasping predicates in v1")
        elif self.obj_a is None:
            raise ValueError("unary/binary transition requires obj_a")
        return self


class GraspHistoryV1(PipelineModel):
    step_idx: Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)]
    arm: Literal["left", "right"]
    old_value: str | None
    new_value: str | None

    @field_validator("old_value", "new_value")
    @classmethod
    def grasp_value_nfc(cls, value: str | None) -> str | None:
        return None if value is None else _nfc(value, label="grasp scene name", max_bytes=512)

    @model_validator(mode="after")
    def changed(self) -> GraspHistoryV1:
        if self.old_value is None and self.new_value is None:
            raise ValueError("grasp history requires one non-null value")
        if self.old_value == self.new_value:
            raise ValueError("grasp history values must differ")
        return self


class BddlTransitionsDocumentV1(PipelineModel):
    task_name: str
    instance_id: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    demo_id: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    total_steps: Annotated[int, Field(strict=True, ge=1, le=9_007_199_254_740_991)]
    success: bool
    transitions: Annotated[list[BddlTransitionV1], Field(max_length=MAX_TRANSITION_RECORDS)]
    grasp_history: Annotated[list[GraspHistoryV1], Field(max_length=MAX_TRANSITION_RECORDS)]

    @field_validator("task_name")
    @classmethod
    def task_name_nfc(cls, value: str) -> str:
        return _nfc(value, label="task_name", max_bytes=256)

    @model_validator(mode="after")
    def validate_order(self) -> BddlTransitionsDocumentV1:
        for values in (self.transitions, self.grasp_history):
            steps = [item.step_idx for item in values]
            if steps != sorted(steps) or any(step >= self.total_steps for step in steps):
                raise ValueError("transition/grasp steps must be nondecreasing and in bounds")
        return self


class SceneStateObjectV1(PipelineModel):
    ordinal: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    scene_name: str
    joint_position_count: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]

    _name = field_validator("scene_name")(
        lambda value: _nfc(value, label="scene_name", max_bytes=512)
    )


class SceneIdentityV1(PipelineModel):
    scope_name: str
    scene_name: str

    _scope = field_validator("scope_name")(
        lambda value: _nfc(value, label="scope_name", max_bytes=512)
    )
    _scene = field_validator("scene_name")(
        lambda value: _nfc(value, label="scene_name", max_bytes=512)
    )


class RolloutSceneProjectionV1(PipelineModel):
    schema_version: Literal["behavior.rollout-scene-projection.v1"]
    task_name: str
    instance_id: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    demo_id: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    state_objects: Annotated[list[SceneStateObjectV1], Field(min_length=1)]
    robot_scene_name: str
    inst_to_name: Annotated[list[SceneIdentityV1], Field(min_length=1)]

    _task = field_validator("task_name")(
        lambda value: _nfc(value, label="task_name", max_bytes=256)
    )
    _robot = field_validator("robot_scene_name")(
        lambda value: _nfc(value, label="robot_scene_name", max_bytes=512)
    )

    @model_validator(mode="after")
    def validate_projection(self) -> RolloutSceneProjectionV1:
        if [item.ordinal for item in self.state_objects] != list(range(len(self.state_objects))):
            raise ValueError("scene ordinals must be contiguous from zero")
        names = [item.scene_name for item in self.state_objects]
        if len(names) != len(set(names)):
            raise ValueError("state_objects scene names must be unique")
        robots = [
            item
            for item in self.state_objects
            if item.scene_name == self.robot_scene_name and item.joint_position_count == 28
        ]
        if len(robots) != 1:
            raise ValueError("projection requires one signed 28-joint robot")
        scopes = [item.scope_name for item in self.inst_to_name]
        scenes = [item.scene_name for item in self.inst_to_name]
        if scopes != sorted(scopes, key=lambda value: value.encode("utf-8")):
            raise ValueError("inst_to_name must be sorted by bytewise scope_name")
        if len(scopes) != len(set(scopes)) or len(scenes) != len(set(scenes)):
            raise ValueError("inst_to_name scope and scene names must be unique")
        return self


def project_scene(
    source: object,
    *,
    task_name: str,
    instance_id: int,
    demo_id: int,
) -> RolloutSceneProjectionV1:
    value = _deep_decode_scene(source)
    if not isinstance(value, dict):
        raise BehaviorContractError("scene source must decode to an object")
    try:
        state = value["state"]
        metadata = value["metadata"]
        registry = state["registry"]["object_registry"]
        inst_to_name = metadata["inst_to_name"]
    except (KeyError, TypeError) as exc:
        raise BehaviorContractError("scene source lacks the fixed registry identity") from exc
    if not isinstance(registry, dict) or not registry or not isinstance(inst_to_name, dict) or not inst_to_name:
        raise BehaviorContractError("scene registry and inst_to_name must be nonempty objects")

    ordered: list[tuple[str, int]] = []
    robot_candidates: list[str] = []
    for scene_name, state_value in registry.items():
        _nfc(scene_name, label="registry scene name", max_bytes=512)
        if not isinstance(state_value, dict):
            raise BehaviorContractError("object_registry entry must be an object")
        joint_pos = state_value.get("joint_pos")
        if joint_pos is not None and not isinstance(joint_pos, list):
            raise BehaviorContractError("joint_pos must be a JSON array when present")
        count = len(joint_pos) if isinstance(joint_pos, list) else 0
        ordered.append((scene_name, count))
        if count == 28:
            robot_candidates.append(scene_name)
    if len(robot_candidates) != 1:
        raise BehaviorContractError("scene source requires exactly one 28-joint registry entry")

    seen = {name for name, _ in ordered}
    identities: list[SceneIdentityV1] = []
    for scope_name, scene_name in inst_to_name.items():
        if not isinstance(scope_name, str) or not isinstance(scene_name, str):
            raise BehaviorContractError("inst_to_name must map strings to strings")
        identities.append(SceneIdentityV1(scope_name=scope_name, scene_name=scene_name))
        if scene_name not in seen:
            ordered.append((scene_name, 0))
            seen.add(scene_name)
    identities.sort(key=lambda item: item.scope_name.encode("utf-8"))
    return RolloutSceneProjectionV1(
        schema_version="behavior.rollout-scene-projection.v1",
        task_name=task_name,
        instance_id=instance_id,
        demo_id=demo_id,
        state_objects=[
            SceneStateObjectV1(
                ordinal=index,
                scene_name=scene_name,
                joint_position_count=count,
            )
            for index, (scene_name, count) in enumerate(ordered)
        ],
        robot_scene_name=robot_candidates[0],
        inst_to_name=identities,
    )


def _deep_decode_scene(source: object) -> object:
    value = source
    for _ in range(4):
        if not isinstance(value, str):
            return value
        encoded = value.encode("utf-8")
        if len(encoded) > SCENE_BYTES_LIMIT:
            raise BehaviorContractError("encoded scene source exceeds 16 MiB")
        if encoded.startswith(b"\xef\xbb\xbf"):
            raise BehaviorContractError("scene source contains a UTF-8 BOM")
        try:
            value = json.loads(
                value,
                object_pairs_hook=_reject_duplicate_pairs,
                parse_constant=_reject_nonfinite,
            )
        except json.JSONDecodeError as exc:
            raise BehaviorContractError("scene source is not valid JSON") from exc
    if isinstance(value, str):
        raise BehaviorContractError("scene source exceeds recursive decode depth")
    return value


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise BehaviorContractError(f"duplicate scene JSON key: {key}")
        value[key] = item
    return value


def _reject_nonfinite(value: str) -> None:
    raise BehaviorContractError(f"nonfinite scene JSON value is forbidden: {value}")


@dataclass(frozen=True)
class Hdf5Facts:
    step_count: int
    seed: int
    scene_source: object | None


@dataclass(frozen=True)
class VideoFacts:
    frame_count: int
    timestamps: tuple[str, ...]
    fps: int
    codec: str
    pixel_format: str
    width: int
    height: int


class Hdf5Inspector(Protocol):
    def inspect(self, path: Path) -> Hdf5Facts: ...


class VideoInspector(Protocol):
    def inspect(self, path: Path) -> VideoFacts: ...


class CompositeBuilder(Protocol):
    def build(self, left: Path, right: Path, head: Path, output: Path) -> None: ...


class RealHdf5Inspector:
    """Read the small signed HDF5 surface from the pinned GPU image."""

    def inspect(self, path: Path) -> Hdf5Facts:
        try:
            h5py = import_module("h5py")
        except ModuleNotFoundError as exc:  # pragma: no cover - GPU image boundary
            raise BehaviorContractError("Pipeline image is missing h5py") from exc
        try:
            with h5py.File(path, "r") as document:
                data = document["data"]
                demo = data["demo_0"]
                state_count = len(demo["state"])
                state_size_count = len(demo["state_size"])
                if state_count != state_size_count:
                    raise BehaviorContractError("HDF5 state/state_size lengths disagree")
                raw_seed = demo.attrs.get("seed", data.attrs.get("seed"))
                if isinstance(raw_seed, bool):
                    raise BehaviorContractError("HDF5 has no integer Pipeline seed")
                try:
                    seed = operator.index(raw_seed)
                except TypeError as exc:
                    raise BehaviorContractError("HDF5 has no integer Pipeline seed") from exc
                if not 0 <= seed <= 4_294_967_295:
                    raise BehaviorContractError("HDF5 Pipeline seed is outside uint32")
                scene_source = data.attrs.get("scene_file")
                if hasattr(scene_source, "item"):
                    scene_source = scene_source.item()
                if isinstance(scene_source, bytes):
                    scene_source = scene_source.decode("utf-8", errors="strict")
                return Hdf5Facts(state_count, seed, scene_source)
        except (KeyError, OSError, TypeError) as exc:
            raise BehaviorContractError("invalid rollout HDF5") from exc


class FfprobeVideoInspector:
    def __init__(self, executable: str = "/usr/bin/ffprobe") -> None:
        self.executable = executable

    def inspect(self, path: Path) -> VideoFacts:
        command = [
            self.executable,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,pix_fmt,width,height,r_frame_rate:frame=best_effort_timestamp_time",
            "-show_frames",
            "-of",
            "json",
            str(path),
        ]
        result = subprocess.run(
            command,
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            env={},
        )
        if result.returncode != 0:
            raise BehaviorContractError("ffprobe rejected rollout video")
        try:
            payload = json.loads(result.stdout)
            streams = payload["streams"]
            if len(streams) != 1:
                raise BehaviorContractError("rollout video must have exactly one video stream")
            stream = streams[0]
            numerator, denominator = stream["r_frame_rate"].split("/", 1)
            fps = int(numerator) / int(denominator)
            frames = payload["frames"]
            timestamps = tuple(frame["best_effort_timestamp_time"] for frame in frames)
            facts = VideoFacts(
                frame_count=len(frames),
                timestamps=timestamps,
                fps=int(fps),
                codec=stream["codec_name"],
                pixel_format=stream["pix_fmt"],
                width=int(stream["width"]),
                height=int(stream["height"]),
            )
        except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
            raise BehaviorContractError("ffprobe output is incomplete") from exc
        if fps != 30 or facts.fps != 30 or not facts.timestamps:
            raise BehaviorContractError("rollout video is not positive 30fps")
        return facts


class FfmpegCompositeBuilder:
    """Build the one canonical composite without inheriting encoder settings."""

    OUTPUT_OPTIONS = (
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "18",
        "-pix_fmt",
        "yuv420p",
        "-r",
        "30",
        "-fps_mode",
        "cfr",
        "-an",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-movflags",
        "+faststart",
        "-threads",
        "1",
    )

    def __init__(self, executable: str = "/usr/bin/ffmpeg") -> None:
        self.executable = executable

    def command(self, left: Path, right: Path, head: Path, output: Path) -> tuple[str, ...]:
        return (
            self.executable,
            "-nostdin",
            "-y",
            "-i",
            str(left),
            "-i",
            str(right),
            "-i",
            str(head),
            "-filter_complex",
            (
                "[0:v]scale=224:224[left_wrist];"
                "[1:v]scale=224:224[right_wrist];"
                "[left_wrist][right_wrist]vstack=inputs=2[left_column];"
                "[2:v]scale=448:448[head];"
                "[left_column][head]hstack=inputs=2[composite]"
            ),
            "-map",
            "[composite]",
            *self.OUTPUT_OPTIONS,
            str(output),
        )

    def build(self, left: Path, right: Path, head: Path, output: Path) -> None:
        if output.exists() or output.is_symlink():
            raise BehaviorContractError("composite output already exists")
        result = subprocess.run(
            self.command(left, right, head, output),
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env={},
        )
        if result.returncode != 0:
            raise BehaviorContractError("canonical ffmpeg composite failed")


class ChildProcess(Protocol):
    pid: int

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...


class ProcessLauncher(Protocol):
    def start(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        process_group: int | None,
    ) -> ChildProcess: ...


class SubprocessLauncher:
    def start(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        process_group: int | None,
    ) -> ChildProcess:
        return subprocess.Popen(
            list(argv),
            cwd=WORKDIR,
            env=dict(env),
            stdin=subprocess.DEVNULL,
            start_new_session=False,
            process_group=0 if process_group is None else process_group,
        )


ConnectProbe = Callable[[str, int], bool]
CancellationCheck = Callable[[], bool]
GroupSignal = Callable[[int, int], None]


def _tcp_connect(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def wait_for_vla_ready(
    process: ChildProcess,
    *,
    connect: ConnectProbe = _tcp_connect,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    for attempt in range(VLA_PROBE_ATTEMPTS):
        if process.poll() is not None:
            raise BehaviorContractError("VLA server exited before TCP readiness")
        if connect("127.0.0.1", VLA_PORT):
            return
        if attempt + 1 < VLA_PROBE_ATTEMPTS:
            sleep(VLA_PROBE_INTERVAL_SECONDS)
    raise BehaviorContractError("VLA server was not ready after 180 TCP probes")


class RolloutSupervisor:
    def __init__(
        self,
        *,
        launcher: ProcessLauncher | None = None,
        connect: ConnectProbe = _tcp_connect,
        cancelled: CancellationCheck | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        signal_group: GroupSignal = os.killpg,
    ) -> None:
        self.launcher = launcher or SubprocessLauncher()
        self.connect = connect
        self.cancelled = cancelled or (lambda: False)
        self.monotonic = monotonic
        self.sleep = sleep
        self.signal_group = signal_group

    def run(
        self,
        *,
        vla_argv: Sequence[str],
        vla_env: Mapping[str, str],
        engine_argv: Sequence[str],
        engine_env: Mapping[str, str],
    ) -> None:
        with _interrupt_cleanup_scope():
            vla = self.launcher.start(vla_argv, env=vla_env, process_group=None)
            engine: ChildProcess | None = None
            try:
                wait_for_vla_ready(vla, connect=self.connect, sleep=self.sleep)
                engine = self.launcher.start(engine_argv, env=engine_env, process_group=vla.pid)
                started = self.monotonic()
                while True:
                    return_code = engine.poll()
                    if return_code is not None:
                        if return_code != 0:
                            raise BehaviorContractError(
                                f"rollout engine exited with status {return_code}"
                            )
                        return
                    if vla.poll() is not None:
                        raise BehaviorContractError("VLA server exited while simulator was running")
                    if self.cancelled():
                        raise BehaviorInterruptedError(signal.SIGTERM)
                    if self.monotonic() - started >= ENGINE_DEADLINE_SECONDS:
                        raise BehaviorContractError(
                            "rollout engine exceeded the 8100 second deadline"
                        )
                    self.sleep(1.0)
            finally:
                self._stop_group(vla.pid, [item for item in (engine, vla) if item is not None])

    def _stop_group(self, group_id: int, processes: list[ChildProcess]) -> None:
        if all(item.poll() is not None for item in processes):
            for item in processes:
                with contextlib.suppress(Exception):
                    item.wait(timeout=0)
            return
        with contextlib.suppress(ProcessLookupError):
            self.signal_group(group_id, signal.SIGINT)
        deadline = self.monotonic() + ENGINE_KILL_GRACE_SECONDS
        while any(item.poll() is None for item in processes) and self.monotonic() < deadline:
            self.sleep(1.0)
        if any(item.poll() is None for item in processes):
            with contextlib.suppress(ProcessLookupError):
                self.signal_group(group_id, signal.SIGKILL)
        for item in processes:
            with contextlib.suppress(Exception):
                item.wait(timeout=ENGINE_KILL_GRACE_SECONDS)


@contextlib.contextmanager
def _interrupt_cleanup_scope() -> Iterator[None]:
    """Convert parent signals to a typed unwind so the owned group is reaped."""

    prior: dict[int, Any] = {}

    def interrupt(signum: int, _frame: object) -> None:
        for handled in prior:
            signal.signal(handled, signal.SIG_IGN)
        raise BehaviorInterruptedError(signum)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            prior[signum] = signal.signal(signum, interrupt)
    except ValueError:
        # Tests may invoke a supervisor in a non-main thread. Production CLI is
        # the container's main Python thread and always installs both handlers.
        prior.clear()
    try:
        yield
    finally:
        for restore_signum, handler in prior.items():
            signal.signal(restore_signum, handler)


class RolloutAdapter:
    """Callable StageAdapter with explicit runtime and inspection seams."""

    def __init__(
        self,
        *,
        runtime: RolloutRuntimeContractV1 | None = None,
        paths: RolloutPaths | None = None,
        supervisor: RolloutSupervisor | None = None,
        hdf5: Hdf5Inspector | None = None,
        video: VideoInspector | None = None,
        composite: CompositeBuilder | None = None,
    ) -> None:
        self.runtime = runtime
        self.paths = paths or RolloutPaths()
        self.supervisor = supervisor or RolloutSupervisor()
        self.hdf5 = hdf5 or RealHdf5Inspector()
        self.video = video or FfprobeVideoInspector()
        self.composite = composite or FfmpegCompositeBuilder()

    def __call__(self, request: StageRequestV1, workspace: AttemptWorkspace) -> StageResultV1:
        inputs = load_mounted_inputs(request, self.paths)
        _validate_policy_tree(inputs.policy, self.paths.artifact_root("policy"))
        runtime = self.runtime or _load_runtime_contract(self.paths.input_root)
        prepare_scratch(self.paths)
        try:
            task = inputs.task_instance.payload
            self.supervisor.run(
                vla_argv=build_vla_argv(task.behavior_task_id, task.seed),
                vla_env=build_vla_env(runtime),
                engine_argv=build_engine_argv(),
                engine_env=build_simulator_env(runtime),
            )
            return self._commit_artifact(request, inputs, workspace)
        except BaseException:
            _discard_partial_adapter_output(workspace)
            raise
        finally:
            _cleanup_engine_staging(self.paths)
            cleanup_scratch(self.paths)

    def _commit_artifact(
        self,
        request: StageRequestV1,
        inputs: MountedRolloutInputs,
        workspace: AttemptWorkspace,
    ) -> StageResultV1:
        child = inputs.task_instance.payload
        task_tag = f"task-{child.behavior_task_id:04d}"
        stem = child.demo_stem
        source_paths = {
            "rollout_hdf5": self.paths.engine_output_root
            / "payload"
            / "trajectories"
            / task_tag
            / f"{stem}.hdf5",
            "bddl_transitions": self.paths.engine_output_root
            / "payload"
            / "meta"
            / "episodes"
            / task_tag
            / f"{stem}_bddl_transitions.json",
            "rgb_head": self.paths.engine_output_root
            / "payload"
            / "videos"
            / task_tag
            / "observation.images.rgb.head"
            / f"{stem}.mp4",
            "rgb_left_wrist": self.paths.engine_output_root
            / "payload"
            / "videos"
            / task_tag
            / "observation.images.rgb.left_wrist"
            / f"{stem}.mp4",
            "rgb_right_wrist": self.paths.engine_output_root
            / "payload"
            / "videos"
            / task_tag
            / "observation.images.rgb.right_wrist"
            / f"{stem}.mp4",
        }
        byte_limit = workspace.final_output_bytes_limit
        hdf5_bytes = _read_regular(source_paths["rollout_hdf5"], limit=byte_limit)
        transition_value = load_canonical_document(
            source_paths["bddl_transitions"], max_bytes=SCENE_BYTES_LIMIT
        )
        transitions = BddlTransitionsDocumentV1.model_validate_json(
            canonical_document(transition_value)
        )
        expected_transition_identity = (
            child.task_name,
            child.engine_task_instance_id,
            child.demo_id,
        )
        if (
            transitions.task_name,
            transitions.instance_id,
            transitions.demo_id,
        ) != expected_transition_identity:
            raise BehaviorContractError("transition identity disagrees with signed task child")

        hdf5_facts = self.hdf5.inspect(source_paths["rollout_hdf5"])
        if hdf5_facts.step_count != transitions.total_steps or hdf5_facts.seed != child.seed:
            raise BehaviorContractError("HDF5 step count or seed disagrees with rollout identity")
        scene = _resolve_scene_projection(
            hdf5_facts.scene_source,
            self.paths.scratch_root / "meta" / "episodes" / task_tag / f"{stem}.json",
            task_name=child.task_name,
            instance_id=child.engine_task_instance_id,
            demo_id=child.demo_id,
        )
        scene_bytes = canonical_document(scene)
        transition_bytes = canonical_document(transitions)

        video_facts = {
            role: self.video.inspect(source_paths[role])
            for role in ("rgb_head", "rgb_left_wrist", "rgb_right_wrist")
        }
        _validate_camera_facts(video_facts, transitions.total_steps)
        composite_source = self.paths.scratch_root / "rgb_composite.mp4"
        self.composite.build(
            source_paths["rgb_left_wrist"],
            source_paths["rgb_right_wrist"],
            source_paths["rgb_head"],
            composite_source,
        )
        composite_facts = self.video.inspect(composite_source)
        if (
            composite_facts.frame_count != transitions.total_steps
            or composite_facts.fps != 30
            or composite_facts.codec != "h264"
            or composite_facts.pixel_format != "yuv420p"
            or (composite_facts.width, composite_facts.height) != (672, 448)
        ):
            raise BehaviorContractError("composite video does not match the fixed media contract")

        relative_paths = {
            "rollout_hdf5": f"payload/trajectories/{task_tag}/{stem}.hdf5",
            "bddl_transitions": f"payload/meta/episodes/{task_tag}/{stem}_bddl_transitions.json",
            "scene_metadata": f"payload/meta/episodes/{task_tag}/{stem}_scene.json",
            "rgb_head": f"payload/videos/{task_tag}/observation.images.rgb.head/{stem}.mp4",
            "rgb_left_wrist": (
                f"payload/videos/{task_tag}/observation.images.rgb.left_wrist/{stem}.mp4"
            ),
            "rgb_right_wrist": (
                f"payload/videos/{task_tag}/observation.images.rgb.right_wrist/{stem}.mp4"
            ),
            "rgb_composite": "payload/rgb_composite.mp4",
        }
        payload_bytes = {
            "rollout_hdf5": hdf5_bytes,
            "bddl_transitions": transition_bytes,
            "scene_metadata": scene_bytes,
            "rgb_head": _read_regular(source_paths["rgb_head"], limit=byte_limit),
            "rgb_left_wrist": _read_regular(source_paths["rgb_left_wrist"], limit=byte_limit),
            "rgb_right_wrist": _read_regular(source_paths["rgb_right_wrist"], limit=byte_limit),
            "rgb_composite": _read_regular(composite_source, limit=byte_limit),
        }
        if sum(map(len, payload_bytes.values())) > byte_limit:
            raise BehaviorContractError("rollout payload exceeds the final output budget")
        for role, relative_path in relative_paths.items():
            workspace.write_payload_bytes("rollout", relative_path, payload_bytes[role])

        media_types = {
            "rollout_hdf5": "application/x-hdf5",
            "bddl_transitions": "application/json",
            "scene_metadata": "application/json",
            "rgb_head": "video/mp4",
            "rgb_left_wrist": "video/mp4",
            "rgb_right_wrist": "video/mp4",
            "rgb_composite": "video/mp4",
        }
        data_descriptors = {
            role: RolloutDataFileDescriptorV1(
                role=cast(Any, role),
                relative_path=relative_paths[role],
                sha256=digest_bytes(payload_bytes[role]),
                size_bytes=len(payload_bytes[role]),
                media_type=media_types[role],
            )
            for role in ("rollout_hdf5", "bddl_transitions", "scene_metadata")
        }
        all_video_facts = {**video_facts, "rgb_composite": composite_facts}
        video_descriptors = {
            role: RolloutVideoFileDescriptorV1(
                role=cast(Any, role),
                relative_path=relative_paths[role],
                sha256=digest_bytes(payload_bytes[role]),
                size_bytes=len(payload_bytes[role]),
                media_type="video/mp4",
                frame_count=facts.frame_count,
                fps=30,
                codec="h264",
                pixel_format="yuv420p",
                width=facts.width,
                height=facts.height,
            )
            for role, facts in all_video_facts.items()
        }
        required: list[RolloutDataFileDescriptorV1 | RolloutVideoFileDescriptorV1] = [
            data_descriptors["rollout_hdf5"],
            data_descriptors["bddl_transitions"],
            data_descriptors["scene_metadata"],
            video_descriptors["rgb_head"],
            video_descriptors["rgb_left_wrist"],
            video_descriptors["rgb_right_wrist"],
            video_descriptors["rgb_composite"],
        ]
        optional, optional_bytes = _optional_predicate_catalog(
            self.paths.engine_output_root,
            task_tag=task_tag,
            demo_stem=stem,
            limit=max(0, byte_limit - sum(map(len, payload_bytes.values()))),
        )
        if optional is not None and optional_bytes is not None:
            workspace.write_payload_bytes(
                "rollout", optional.relative_path, optional_bytes
            )

        artifact = BehaviorRolloutBundleArtifactV1(
            schema_version="behavior_rollout_bundle.v1",
            payload=BehaviorRolloutBundlePayloadV1(
                task_instance_identity=child.task_instance_identity,
                loom_task_id=child.loom_task_id,
                behavior_task_id=child.behavior_task_id,
                task_name=child.task_name,
                task_checksum=child.task_checksum,
                task_bundle_digest=child.task_bundle_digest,
                source_bddl_path=child.source_bddl_path,
                eval_instance_index=child.eval_instance_index,
                engine_task_instance_id=child.engine_task_instance_id,
                episode_index=child.episode_index,
                demo_id=child.demo_id,
                demo_stem=child.demo_stem,
                seed=child.seed,
                domain_outcome=(
                    "rollout_success" if transitions.success else "rollout_failure"
                ),
                success=transitions.success,
                step_count=transitions.total_steps,
                recording_fps=30,
                dataset=_content_ref(inputs.bindings.dataset),
                policy=_content_ref(inputs.bindings.policy),
                policy_identity=_policy_identity(inputs.policy),
                runtime=RolloutRuntimeV1(
                    loom_commit_sha=request.provenance.loom_commit_sha,
                    image_digest=request.provenance.image_digest,
                    compatibility_manifest_sha256=(
                        request.provenance.compatibility_manifest_sha256
                    ),
                ),
                required_file_descriptors=required,
                optional_audit_files=[] if optional is None else [optional],
            ),
            files=sorted(
                [
                    ArtifactFileV1(
                        name=cast(Any, role),
                        relative_path=relative_path,
                        sha256=digest_bytes(payload_bytes[role]),
                        size_bytes=len(payload_bytes[role]),
                        media_type=media_types[role],
                        required=True,
                    )
                    for role, relative_path in relative_paths.items()
                ]
                + (
                    []
                    if optional is None
                    else [
                        ArtifactFileV1(
                            name="predicate_catalog",
                            relative_path=optional.relative_path,
                            sha256=optional.sha256,
                            size_bytes=optional.size_bytes,
                            media_type=optional.media_type,
                            required=False,
                        )
                    ]
                ),
                key=lambda item: item.relative_path.encode("utf-8"),
            ),
            provenance=_artifact_provenance(request),
        )
        workspace.write_artifact_json("rollout", artifact)
        return _stage_result(request, transitions)


def rollout_stage_binding() -> StageAdapterBinding:
    return StageAdapterBinding(
        adapter=RolloutAdapter(),
        output_declarations=ROLLOUT_OUTPUT_DECLARATIONS,
    )


def _load_runtime_contract(input_root: Path) -> RolloutRuntimeContractV1:
    value = load_canonical_document(input_root / "runtime-contract.json", max_bytes=1_048_576)
    return RolloutRuntimeContractV1.model_validate_json(canonical_document(value))


def _validate_policy_tree(policy: BehaviorPolicyCheckpointArtifactV1, root: Path) -> None:
    compatibility = policy.payload.compatibility
    if not isinstance(compatibility, PolicyCompatibilityV1):
        raise BehaviorContractError("policy compatibility branch drift")
    files = [item for item in policy.files if item.relative_path.startswith("payload/checkpoint/")]
    if not files or len(files) != len(policy.files):
        raise BehaviorContractError("policy manifest is not an exact checkpoint-root tree")
    tree: list[dict[str, object]] = []
    for descriptor in files:
        path = _safe_relative(descriptor.relative_path)
        value = _read_regular(root.joinpath(*path.parts), limit=descriptor.size_bytes)
        if len(value) != descriptor.size_bytes or digest_bytes(value) != descriptor.sha256:
            raise BehaviorContractError("policy checkpoint bytes disagree with its manifest")
        tree.append(
            {
                "relative_path": descriptor.relative_path.removeprefix("payload/checkpoint/"),
                "sha256": descriptor.sha256,
                "size_bytes": descriptor.size_bytes,
            }
        )
    if canonical_digest(tree, persisted=False) != compatibility.checkpoint_tree_sha256:
        raise BehaviorContractError("policy checkpoint tree digest drift")


def _resolve_scene_projection(
    hdf5_source: object | None,
    meta_path: Path,
    *,
    task_name: str,
    instance_id: int,
    demo_id: int,
) -> RolloutSceneProjectionV1:
    projections: list[RolloutSceneProjectionV1] = []
    if hdf5_source is not None:
        projections.append(
            project_scene(
                hdf5_source,
                task_name=task_name,
                instance_id=instance_id,
                demo_id=demo_id,
            )
        )
    if meta_path.exists() or meta_path.is_symlink():
        encoded = _read_regular(meta_path, limit=SCENE_BYTES_LIMIT)
        if encoded.startswith(b"\xef\xbb\xbf"):
            raise BehaviorContractError("scene meta source contains a UTF-8 BOM")
        try:
            meta = json.loads(
                encoded,
                object_pairs_hook=_reject_duplicate_pairs,
                parse_constant=_reject_nonfinite,
            )
            source = meta["scene_file"]
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise BehaviorContractError("scene meta source is invalid") from exc
        projections.append(
            project_scene(
                source,
                task_name=task_name,
                instance_id=instance_id,
                demo_id=demo_id,
            )
        )
    if not projections:
        raise BehaviorContractError("rollout has no signed scene source")
    canonical = [canonical_document(item) for item in projections]
    if any(item != canonical[0] for item in canonical[1:]):
        raise BehaviorContractError("HDF5 and sibling scene projections disagree")
    return projections[0]


def _validate_camera_facts(facts: Mapping[str, VideoFacts], step_count: int) -> None:
    values = list(facts.values())
    if len(values) != 3:
        raise BehaviorContractError("rollout requires exactly three camera videos")
    first = values[0]
    for value in values:
        if (
            value.frame_count != step_count
            or value.frame_count <= 0
            or value.timestamps != first.timestamps
            or value.fps != 30
            or value.codec != "h264"
            or value.pixel_format != "yuv420p"
            or value.width <= 0
            or value.height <= 0
        ):
            raise BehaviorContractError("camera frame/timestamp/media facts disagree")


def _content_ref(binding: BindingSetV1) -> ContentArtifactRefV1:
    item = binding.items[0]
    return ContentArtifactRefV1(
        artifact_id=item.artifact_id,
        artifact_type=binding.artifact_type,
        manifest_sha256=item.manifest_sha256,
        content_sha256=item.content_sha256,
    )


def _policy_identity(policy: BehaviorPolicyCheckpointArtifactV1) -> RolloutPolicyIdentityV1:
    value = policy.payload.compatibility
    if not isinstance(value, PolicyCompatibilityV1):
        raise BehaviorContractError("policy compatibility branch drift")
    return RolloutPolicyIdentityV1(
        architecture=value.architecture,
        checkpoint_format=value.checkpoint_format,
        checkpoint_root=value.checkpoint_root,
        checkpoint_tree_sha256=value.checkpoint_tree_sha256,
        vla_interface_version=value.vla_interface_version,
        controller_adapter_version=value.controller_adapter_version,
        model_identifier=value.model_identifier,
    )


def _artifact_provenance(request: StageRequestV1) -> PipelineArtifactProvenanceV1:
    source_artifacts = sorted(
        [
            ArtifactRefV1(
                artifact_id=item.artifact_id,
                artifact_type=binding.artifact_type,
                manifest_sha256=item.manifest_sha256,
            )
            for binding in request.inputs
            for item in binding.items
        ],
        key=lambda item: item.artifact_id.bytes,
    )
    return PipelineArtifactProvenanceV1(
        producer_kind="pipeline",
        loom_commit_sha=request.provenance.loom_commit_sha,
        pipeline_run_id=request.run_id,
        stage_run_id=request.stage_run_id,
        execution_attempt_id=request.attempt_id,
        recipe_digest=request.provenance.recipe_digest,
        execution_spec_digest=request.provenance.execution_spec_digest,
        image_digest=request.provenance.image_digest,
        compatibility_manifest_sha256=request.provenance.compatibility_manifest_sha256,
        control_binding=None,
        source_artifacts=source_artifacts,
    )


def _stage_result(
    request: StageRequestV1, transitions: BddlTransitionsDocumentV1
) -> StageResultV1:
    return StageResultV1(
        schema_version="loom.stage-result.v1",
        domain_outcome="rollout_success" if transitions.success else "rollout_failure",
        reason_code="rollout_completed",
        retry_class=RetryClass.NONE,
        inputs=[
            StageResultInputV1(
                binding_name=binding.binding_name,
                item_key=item.item_key,
                artifact_id=item.artifact_id,
                artifact_type=binding.artifact_type,
                manifest_sha256=item.manifest_sha256,
            )
            for binding in request.inputs
            for item in binding.items
        ],
        outputs=[
            StageResultOutputV1(name="rollout", artifact_type="behavior_rollout_bundle.v1")
        ],
        metrics={
            "demo_id": transitions.demo_id,
            "seed": cast(BehaviorRolloutParametersV1, request.parameters).seed,
            "step_count": transitions.total_steps,
        },
        provenance=StageResultProvenanceV1(
            pipeline_run_id=request.run_id,
            stage_run_id=request.stage_run_id,
            execution_attempt_id=request.attempt_id,
            recipe_digest=request.provenance.recipe_digest,
            execution_spec_digest=request.provenance.execution_spec_digest,
            image_digest=request.provenance.image_digest.rsplit("@", maxsplit=1)[-1],
        ),
        error=None,
    )


def _optional_predicate_catalog(
    output_root: Path,
    *,
    task_tag: str,
    demo_stem: str,
    limit: int,
) -> tuple[RolloutDataFileDescriptorV1 | None, bytes | None]:
    relative = f"payload/meta/episodes/{task_tag}/{demo_stem}_predicate_catalog.json"
    path = output_root.joinpath(*PurePosixPath(relative).parts)
    if not path.exists() and not path.is_symlink():
        return None, None
    value = load_canonical_document(path, max_bytes=limit)
    encoded = canonical_document(value)
    return (
        RolloutDataFileDescriptorV1(
            role="predicate_catalog",
            relative_path=relative,
            sha256=digest_bytes(encoded),
            size_bytes=len(encoded),
            media_type="application/json",
        ),
        encoded,
    )


def _discard_partial_adapter_output(workspace: AttemptWorkspace) -> None:
    root = workspace.partial_root
    if not root.exists():
        return
    if root.is_symlink():
        raise BehaviorContractError("partial workspace was replaced by a symlink")
    shutil.rmtree(root)


def _cleanup_engine_staging(paths: RolloutPaths) -> None:
    """Remove only the engine-owned raw tree before AttemptWorkspace validates /outputs."""

    root = paths.engine_output_root / "payload"
    if not root.exists() and not root.is_symlink():
        return
    if root.is_symlink() or not root.is_dir():
        raise BehaviorContractError("engine payload staging root is not a real directory")
    shutil.rmtree(root)
