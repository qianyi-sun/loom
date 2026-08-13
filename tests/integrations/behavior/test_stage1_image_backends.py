from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from tests.integrations.behavior.test_rollout_adapter import (
    _dataset_document,
    _policy_document,
    _request,
    _task_document,
    _write_inputs,
)

from loom.integrations.behavior.errors import BehaviorContractError
from loom.integrations.behavior.stages import rollout_backend
from loom.integrations.behavior.stages.rollout import RolloutPaths
from loom.integrations.behavior.stages.rollout_backend import (
    PINNED_OMNIGIBSON_SOURCE_COMMIT,
    OmniGibsonStage1EpisodeDriver,
)
from loom.integrations.behavior.vla import policy_backend
from loom.integrations.behavior.vla.policy_backend import (
    PINNED_B1K_SOURCE_COMMIT,
    PINNED_OPENPI_SOURCE_COMMIT,
    OpenPiStage1ServerBackend,
)


class _FakePolicyRuntime:
    version = "0.1.0"

    def __init__(self) -> None:
        self.events: list[object] = []

    def get_config(self, name: str) -> object:
        self.events.append(("config", name))
        return SimpleNamespace(name=name)

    def create_policy(
        self,
        config: object,
        checkpoint: Path,
        *,
        sample_kwargs: dict[str, int],
        default_prompt: str,
    ) -> Any:
        self.events.append(
            ("load", config, checkpoint, sample_kwargs, default_prompt)
        )
        return SimpleNamespace(_rng="default", metadata={"model": "pi_behavior"})

    def seed_policy(self, policy: Any, seed: int) -> None:
        policy._rng = ("jax-key", seed)
        self.events.append(("seed", seed, policy._rng))

    def wrap_policy(self, policy: Any, *, task_id: int) -> object:
        self.events.append(("wrap", task_id, policy._rng))
        return ("wrapped", task_id)

    def make_server(
        self,
        *,
        policy: object,
        host: str,
        port: int,
        metadata: dict[str, Any],
    ) -> object:
        self.events.append(("server", policy, host, port, metadata))
        return "server"

    def serve_forever(self, server: object) -> None:
        self.events.append(("serve", server))


def test_backend_source_identity_constants_are_exact() -> None:
    assert PINNED_OMNIGIBSON_SOURCE_COMMIT == "d9056fa1468dd277e425b0b17caea90d367d35a6"
    assert PINNED_B1K_SOURCE_COMMIT == "ca556f74a455cef7987a2be4537b5ac85cc56dd7"
    assert PINNED_OPENPI_SOURCE_COMMIT == "01177e0242a1c7e8fad2547caa0e987def614cda"


def test_vla_runtime_loader_accepts_only_the_verified_pinned_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def get_config(config_name: str) -> object:
        return SimpleNamespace(name=config_name)

    def create_trained_policy(
        train_config: object,
        checkpoint_dir: Path,
        *,
        sample_kwargs: dict[str, int],
        default_prompt: str,
    ) -> object:
        return (train_config, checkpoint_dir, sample_kwargs, default_prompt)

    def key(seed: int) -> object:
        return seed

    class B1KWrapperConfig:
        def __init__(
            self,
            actions_to_execute: int,
            actions_to_keep: int,
            execute_in_n_steps: int,
            history_len: int,
            votes_to_promote: int,
            time_threshold_inpaint: float,
            num_steps: int,
            apply_eval_tricks: bool,
        ) -> None:
            pass

    class B1KPolicyWrapper:
        def __init__(
            self,
            policy: object,
            task_id: int,
            config: object,
            checkpoint_switcher: object,
        ) -> None:
            pass

    class WebsocketPolicyServer:
        def __init__(
            self,
            policy: object,
            host: str,
            port: int,
            metadata: dict[str, Any],
        ) -> None:
            pass

    modules = {
        "b1k": SimpleNamespace(__version__="0.1.0"),
        "b1k.policies.policy_config": SimpleNamespace(
            create_trained_policy=create_trained_policy
        ),
        "b1k.shared.eval_b1k_wrapper": SimpleNamespace(
            B1KWrapperConfig=B1KWrapperConfig,
            B1KPolicyWrapper=B1KPolicyWrapper,
        ),
        "b1k.training.config": SimpleNamespace(get_config=get_config),
        "jax.random": SimpleNamespace(key=key),
        "omnigibson.learning.utils.network_utils": SimpleNamespace(
            WebsocketPolicyServer=WebsocketPolicyServer
        ),
    }
    monkeypatch.setattr(policy_backend, "import_module", modules.__getitem__)
    assert policy_backend._load_runtime().version == "0.1.0"

    def drifted_create(train_config: object, checkpoint_dir: Path) -> object:
        return (train_config, checkpoint_dir)

    modules["b1k.policies.policy_config"] = SimpleNamespace(
        create_trained_policy=drifted_create
    )
    with pytest.raises(
        BehaviorContractError,
        match="create_trained_policy: missing sample_kwargs,default_prompt",
    ):
        policy_backend._load_runtime()


def test_vla_backend_loads_exact_config_seeds_jax_and_binds_loopback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    monkeypatch.setattr(policy_backend, "_CHECKPOINT_ROOT", checkpoint)
    runtime = _FakePolicyRuntime()
    backend = OpenPiStage1ServerBackend(runtime=runtime)

    backend.load_policy(
        task_id=7,
        checkpoint=checkpoint,
        policy_config="pi_behavior_b1k_fast",
        seed=41,
    )
    backend.serve(host="127.0.0.1", port=8000)

    assert ("seed", 41, ("jax-key", 41)) in runtime.events
    assert ("wrap", 7, ("jax-key", 41)) in runtime.events
    assert (
        "server",
        ("wrapped", 7),
        "127.0.0.1",
        8000,
        {"model": "pi_behavior"},
    ) in runtime.events
    assert runtime.events[-1] == ("serve", "server")


def test_vla_backend_fails_closed_on_version_or_bind_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    monkeypatch.setattr(policy_backend, "_CHECKPOINT_ROOT", checkpoint)
    runtime = _FakePolicyRuntime()
    runtime.version = "0.2.0"
    backend = OpenPiStage1ServerBackend(runtime=runtime)
    with pytest.raises(BehaviorContractError, match=r"requires b1k 0\.1\.0"):
        backend.load_policy(
            task_id=7,
            checkpoint=checkpoint,
            policy_config="pi_behavior_b1k_fast",
            seed=41,
        )

    with pytest.raises(BehaviorContractError, match="before checkpoint load"):
        OpenPiStage1ServerBackend(runtime=_FakePolicyRuntime()).serve(
            host="127.0.0.1", port=8000
        )


class _FakeEvaluator:
    def __init__(self) -> None:
        self.events: list[object] = []
        self.env = object()
        self.metrics = [SimpleNamespace(end_callback=lambda env: self.events.append(("metric", env)))]
        self.obs: dict[str, object] = {}
        self.data_recorder = SimpleNamespace(step_count=0)

    def reset(self) -> None:
        self.events.append("reset")

    def load_task_instance(self, instance_id: int, test_hidden: bool = False) -> None:
        self.events.append(("load", instance_id, test_hidden))

    def setup_recorder(
        self,
        output_folder: str,
        demo_id: int,
        record_rgb: bool = True,
        record_depth: bool = True,
    ) -> None:
        self.events.append(
            ("recorder", output_folder, demo_id, record_rgb, record_depth)
        )

    def start_episode_recording(self) -> None:
        self.events.append("recording")

    def step(self) -> tuple[bool, bool, dict[str, object]]:
        self.data_recorder.step_count += 1
        self.events.append("step")
        return True, False, {"done": {"success": True}}

    def end_episode_recording(self, success: bool) -> bool:
        self.events.append(("end", success))
        return True

    def _finalize_episode_recording(
        self,
        instance_id: int,
        episode_idx: int,
        demo_id: int,
        success: bool = False,
    ) -> None:
        self.events.append(("finalize", instance_id, episode_idx, demo_id, success))

    def __exit__(self, *_args: object) -> None:
        self.events.append("close")


class _FakeOmniRuntime:
    version = "3.8.0"

    def __init__(self) -> None:
        self.evaluator = _FakeEvaluator()
        self.create_call: tuple[str, int, Path] | None = None

    def task_id(self, task_name: str) -> int:
        assert task_name == "placing_can"
        return 7

    def create_evaluator(
        self,
        *,
        task_name: str,
        eval_instance_index: int,
        output_root: Path,
    ) -> _FakeEvaluator:
        self.create_call = (task_name, eval_instance_index, output_root)
        output_root.mkdir(parents=True)
        return self.evaluator

    def close_evaluator(self, evaluator: _FakeEvaluator) -> None:
        evaluator.__exit__(None, None, None)

    def preview_jpeg(self, evaluator: _FakeEvaluator) -> bytes:
        assert evaluator is self.evaluator
        return b"preview-jpeg"


class _FailingPreviewRuntime(_FakeOmniRuntime):
    def preview_jpeg(self, evaluator: _FakeEvaluator) -> bytes:
        assert evaluator is self.evaluator
        raise RuntimeError("preview unavailable")


class _Preview:
    def __init__(self) -> None:
        self.frames: list[tuple[int, bytes]] = []

    def offer(self, *, step_idx: int, jpeg: bytes) -> None:
        self.frames.append((step_idx, jpeg))


def _write_upstream_output(root: Path, task: Any) -> None:
    child = task.payload
    task_tag = f"task-{child.behavior_task_id:04d}"
    stem = child.demo_stem
    episode_root = root / "success" / "2025-challenge-demos"
    hdf5_path = episode_root / "trajectories" / task_tag / f"{stem}.hdf5"
    hdf5_path.parent.mkdir(parents=True)
    hdf5_path.write_bytes(b"hdf5")

    meta = episode_root / "meta" / "episodes" / task_tag
    meta.mkdir(parents=True)
    (meta / f"{stem}_bddl_transitions.json").write_text(
        json.dumps({"success": True}), encoding="utf-8"
    )
    (meta / f"{stem}.json").write_text(
        json.dumps({"scene_file": {}}), encoding="utf-8"
    )
    for camera in ("head", "left_wrist", "right_wrist"):
        video = (
            episode_root
            / "videos"
            / task_tag
            / f"observation.images.rgb.{camera}"
            / f"{stem}.mp4"
        )
        video.parent.mkdir(parents=True)
        video.write_bytes(camera.encode())


def test_rollout_backend_loads_signed_child_runs_once_and_projects_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task_document()
    dataset = _dataset_document(b"task")
    policy = _policy_document(b"checkpoint")
    request = _request(task, dataset, policy)
    paths = RolloutPaths(
        input_root=tmp_path / "inputs",
        engine_output_root=tmp_path / "outputs",
        scratch_root=tmp_path / "scratch",
    )
    _write_inputs(paths.input_root, task, dataset, policy)
    paths.engine_output_root.mkdir()
    paths.scratch_root.mkdir()
    runtime = _FakeOmniRuntime()
    driver = OmniGibsonStage1EpisodeDriver(runtime=runtime, paths=paths)
    stamped: list[tuple[Path, int]] = []
    monkeypatch.setattr(
        rollout_backend,
        "_stamp_pipeline_seed",
        lambda path, seed: stamped.append((path, seed)),
    )

    loaded = driver.load_task_instance(request)
    driver.reset_episode(task.payload.episode_index)
    preview = _Preview()
    assert (
        driver.run_episode_with_live_preview(
            task.payload.episode_index,
            output_dir=paths.engine_output_root,
            scratch=paths.scratch_root,
            live_preview=preview,
        )
        == 0
    )
    _write_upstream_output(paths.scratch_root / "omnigibson-output", task)
    driver.close()

    assert loaded.engine_task_instance_id == task.payload.engine_task_instance_id
    assert runtime.create_call == (
        task.payload.task_name,
        task.payload.eval_instance_index,
        paths.scratch_root / "omnigibson-output",
    )
    assert ("load", task.payload.engine_task_instance_id, False) in runtime.evaluator.events
    assert runtime.evaluator.events.count("step") == 1
    assert preview.frames == [(0, b"preview-jpeg")]
    hdf5_path = (
        paths.engine_output_root
        / "payload"
        / "trajectories"
        / "task-0007"
        / f"{task.payload.demo_stem}.hdf5"
    )
    assert hdf5_path.read_bytes() == b"hdf5"
    assert stamped == [
        (
            paths.scratch_root
            / "omnigibson-output"
            / "success"
            / "2025-challenge-demos"
            / "trajectories"
            / "task-0007"
            / f"{task.payload.demo_stem}.hdf5",
            task.payload.seed,
        )
    ]
    assert (
        paths.engine_output_root
        / "payload"
        / "videos"
        / "task-0007"
        / "observation.images.rgb.head"
        / f"{task.payload.demo_stem}.mp4"
    ).read_bytes() == b"head"
    assert (
        paths.scratch_root
        / "meta"
        / "episodes"
        / "task-0007"
        / f"{task.payload.demo_stem}.json"
    ).is_file()


def test_rollout_backend_rejects_unpinned_omnigibson_before_task_load(
    tmp_path: Path,
) -> None:
    task = _task_document()
    dataset = _dataset_document(b"task")
    policy = _policy_document(b"checkpoint")
    paths = RolloutPaths(
        input_root=tmp_path / "inputs",
        engine_output_root=tmp_path / "outputs",
        scratch_root=tmp_path / "scratch",
    )
    _write_inputs(paths.input_root, task, dataset, policy)
    runtime = _FakeOmniRuntime()
    runtime.version = "3.9.0"
    driver = OmniGibsonStage1EpisodeDriver(runtime=runtime, paths=paths)
    with pytest.raises(BehaviorContractError, match=r"requires OmniGibson 3\.8\.0"):
        driver.load_task_instance(_request(task, dataset, policy))


def test_rollout_backend_preview_composition_failure_does_not_fail_episode(
    tmp_path: Path,
) -> None:
    task = _task_document()
    dataset = _dataset_document(b"task")
    policy = _policy_document(b"checkpoint")
    request = _request(task, dataset, policy)
    paths = RolloutPaths(
        input_root=tmp_path / "inputs",
        engine_output_root=tmp_path / "outputs",
        scratch_root=tmp_path / "scratch",
    )
    _write_inputs(paths.input_root, task, dataset, policy)
    paths.engine_output_root.mkdir()
    paths.scratch_root.mkdir()
    driver = OmniGibsonStage1EpisodeDriver(runtime=_FailingPreviewRuntime(), paths=paths)
    driver.load_task_instance(request)
    driver.reset_episode(task.payload.episode_index)
    assert (
        driver.run_episode_with_live_preview(
            task.payload.episode_index,
            output_dir=paths.engine_output_root,
            scratch=paths.scratch_root,
            live_preview=_Preview(),
        )
        == 0
    )
