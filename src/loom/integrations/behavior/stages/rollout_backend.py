"""Image-owned OmniGibson 3.8 backend for one signed Stage 1 episode.

This module is intentionally a wrapper, not a second simulator.  It drives the
verified ``eval_data_gen_par_save_all`` runtime API from the pinned image, then
projects its single-episode output into Loom's closed payload inventory.  API
or version drift is rejected before simulator construction.
"""

from __future__ import annotations

import inspect
import io
import shutil
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from loom.integrations.behavior.contracts import BehaviorRolloutParametersV1, StageRequestV1
from loom.integrations.behavior.errors import BehaviorContractError
from loom.integrations.behavior.stages.rollout import (
    RolloutPaths,
    load_mounted_inputs,
)
from loom.integrations.behavior.stages.rollout_engine import (
    EpisodeLivePreview,
    LoadedTaskInstance,
)

if TYPE_CHECKING:
    from loom.integrations.behavior.stages.rollout_engine import EpisodeDriver

_OMNIGIBSON_VERSION = "3.8.0"
# The image/SBOM lock must bind this restore-tree source identity.  Import-time
# version and API checks below catch runtime drift but are not a substitute for
# immutable build provenance.
PINNED_OMNIGIBSON_SOURCE_COMMIT = "d9056fa1468dd277e425b0b17caea90d367d35a6"
_VLA_HOST = "127.0.0.1"
_VLA_PORT = 8000
_PREVIEW_INTERVAL_STEPS = 30


class _Evaluator(Protocol):
    env: object
    metrics: list[object]
    obs: Mapping[str, object]
    data_recorder: object | None

    def reset(self) -> None: ...

    def load_task_instance(self, instance_id: int, test_hidden: bool = False) -> None: ...

    def setup_recorder(
        self,
        output_folder: str,
        demo_id: int,
        record_rgb: bool = True,
        record_depth: bool = True,
    ) -> None: ...

    def start_episode_recording(self) -> None: ...

    def step(self) -> tuple[bool, bool, Mapping[str, object]]: ...

    def end_episode_recording(self, success: bool) -> bool: ...

    def _finalize_episode_recording(
        self,
        instance_id: int,
        episode_idx: int,
        demo_id: int,
        success: bool = False,
    ) -> None: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> object: ...


class _OmniRuntime(Protocol):
    @property
    def version(self) -> str: ...

    def task_id(self, task_name: str) -> int: ...

    def create_evaluator(
        self,
        *,
        task_name: str,
        eval_instance_index: int,
        output_root: Path,
    ) -> _Evaluator: ...

    def close_evaluator(self, evaluator: _Evaluator) -> None: ...

    def preview_jpeg(self, evaluator: _Evaluator) -> bytes: ...


def _require_method(value: object, name: str, parameters: tuple[str, ...]) -> None:
    method = getattr(value, name, None)
    if not callable(method):
        raise BehaviorContractError(f"OmniGibson 3.8 rollout API is missing {name}")
    try:
        actual = inspect.signature(method).parameters
    except (TypeError, ValueError) as exc:
        raise BehaviorContractError(
            f"OmniGibson 3.8 rollout API has no inspectable {name}"
        ) from exc
    missing = [parameter for parameter in parameters if parameter not in actual]
    if missing:
        raise BehaviorContractError(
            f"OmniGibson 3.8 rollout API drift at {name}: missing {','.join(missing)}"
        )


@dataclass(frozen=True)
class _PinnedOmniRuntime:
    version: str
    _module: Any
    _hydra: Any
    _omega_conf: Any

    def task_id(self, task_name: str) -> int:
        mapping = cast(Mapping[str, int], self._module.TASK_NAMES_TO_INDICES)
        try:
            value = mapping[task_name]
        except KeyError as exc:
            raise BehaviorContractError(
                "signed task name is absent from the pinned OmniGibson task universe"
            ) from exc
        if isinstance(value, bool) or not isinstance(value, int):
            raise BehaviorContractError("pinned OmniGibson task id is not an integer")
        return value

    def create_evaluator(
        self,
        *,
        task_name: str,
        eval_instance_index: int,
        output_root: Path,
    ) -> _Evaluator:
        # Resolve the name before interpolating it into Hydra syntax.  This
        # means the only accepted strings come from the pinned task table.
        self.task_id(task_name)
        config_dir = Path(cast(str, self._module.__file__)).parent / "configs"
        if config_dir.is_symlink() or not config_dir.is_dir():
            raise BehaviorContractError("pinned OmniGibson config directory is unavailable")
        output_root.mkdir(mode=0o700, parents=True, exist_ok=False)
        try:
            self._module.register_omegaconf_resolvers()
            with self._hydra.initialize_config_dir(
                str(config_dir),
                version_base="1.1",
            ):
                config = self._hydra.compose(
                    "datacollect_config",
                    overrides=[
                        f"task.name={task_name}",
                        "policy=websocket",
                        f"model.host={_VLA_HOST}",
                        f"model.port={_VLA_PORT}",
                        "headless=true",
                        "write_video=false",
                        "eval_on_train_instances=false",
                        "test_hidden=false",
                        f"eval_instance_ids=[{eval_instance_index}]",
                        "+record_rgb=true",
                        "+record_depth=false",
                        "+only_successes=false",
                    ],
                )
            self._omega_conf.update(
                config,
                "output_folder",
                str(output_root),
                merge=False,
                force_add=True,
            )
            recording_path = (
                output_root
                / "_staging"
                / "2025-challenge-demos"
                / "trajectories"
                / f"task-{self.task_id(task_name):04d}"
                / "stage1.hdf5"
            )
            recording_path.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
            self._omega_conf.update(
                config,
                "recording_path",
                str(recording_path),
                merge=False,
                force_add=True,
            )
            self._omega_conf.resolve(config)
            self._module.gm.HEADLESS = True
            return cast(_Evaluator, self._module.EvaluatorWithDirectRecording(config))
        except BehaviorContractError:
            raise
        except Exception as exc:
            raise BehaviorContractError("OmniGibson 3.8 Stage 1 evaluator setup failed") from exc

    def close_evaluator(self, evaluator: _Evaluator) -> None:
        try:
            evaluator.__exit__(None, None, None)
        except BehaviorContractError:
            raise
        except Exception as exc:
            raise BehaviorContractError("OmniGibson 3.8 evaluator shutdown failed") from exc

    def preview_jpeg(self, evaluator: _Evaluator) -> bytes:
        try:
            numpy = import_module("numpy")
            pillow = import_module("PIL.Image")
            camera_names = cast(Mapping[str, str], self._module.ROBOT_CAMERA_NAMES["R1Pro"])

            def camera(role: str, size: tuple[int, int]) -> Any:
                source = evaluator.obs[f"{camera_names[role]}::rgb"]
                detach = getattr(source, "detach", None)
                if callable(detach):
                    source = detach()
                cpu = getattr(source, "cpu", None)
                if callable(cpu):
                    source = cpu()
                to_numpy = getattr(source, "numpy", None)
                if callable(to_numpy):
                    source = to_numpy()
                value = numpy.asarray(source)[..., :3]
                if value.dtype.kind == "f":
                    maximum = float(value.max(initial=0.0))
                    if maximum <= 1.0:
                        value = value * 255.0
                value = numpy.clip(value, 0, 255).astype(numpy.uint8, copy=False)
                image = pillow.fromarray(value, mode="RGB")
                return image.resize(size, resample=pillow.Resampling.BILINEAR)

            left = camera("left_wrist", (224, 224))
            right = camera("right_wrist", (224, 224))
            head = camera("head", (448, 448))
            composite = pillow.new("RGB", (672, 448))
            composite.paste(left, (0, 0))
            composite.paste(right, (0, 224))
            composite.paste(head, (224, 0))
            encoded = io.BytesIO()
            composite.save(
                encoded,
                format="JPEG",
                quality=80,
                optimize=False,
                progressive=False,
            )
            return encoded.getvalue()
        except Exception as exc:
            # The engine's live-preview wrapper isolates this optional channel.
            # Raising here closes only the preview sink, never the episode.
            raise BehaviorContractError("OmniGibson live-preview composition failed") from exc


def _load_runtime() -> _OmniRuntime:
    try:
        omnigibson = import_module("omnigibson")
        module = import_module("omnigibson.learning.eval_data_gen_par_save_all")
        hydra = import_module("hydra")
        omega_conf = import_module("omegaconf").OmegaConf
    except ImportError as exc:  # pragma: no cover - GPU image boundary
        unresolved = exc.name or "unknown"
        raise BehaviorContractError(
            f"pinned OmniGibson rollout import is unavailable: {unresolved}"
        ) from exc
    version = getattr(omnigibson, "__version__", None)
    if version != _OMNIGIBSON_VERSION:
        raise BehaviorContractError(
            f"Stage 1 requires OmniGibson {_OMNIGIBSON_VERSION}, found {version!r}"
        )
    required_symbols = (
        "EvaluatorWithDirectRecording",
        "ROBOT_CAMERA_NAMES",
        "TASK_NAMES_TO_INDICES",
        "gm",
        "register_omegaconf_resolvers",
    )
    for symbol in required_symbols:
        if not hasattr(module, symbol):
            raise BehaviorContractError(
                f"OmniGibson 3.8 rollout API is missing symbol {symbol}"
            )
    evaluator_type = module.EvaluatorWithDirectRecording
    _require_method(evaluator_type, "load_task_instance", ("instance_id", "test_hidden"))
    _require_method(
        evaluator_type,
        "setup_recorder",
        ("output_folder", "demo_id", "record_rgb", "record_depth"),
    )
    _require_method(evaluator_type, "start_episode_recording", ())
    _require_method(evaluator_type, "step", ())
    _require_method(evaluator_type, "end_episode_recording", ("success",))
    _require_method(
        evaluator_type,
        "_finalize_episode_recording",
        ("instance_id", "episode_idx", "demo_id", "success"),
    )
    return _PinnedOmniRuntime(
        version=version,
        _module=module,
        _hydra=hydra,
        _omega_conf=omega_conf,
    )


def _step_count(evaluator: _Evaluator) -> int:
    recorder = evaluator.data_recorder
    value = getattr(recorder, "step_count", None)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise BehaviorContractError("OmniGibson recorder has no positive step count")
    return value


def _metric_end(evaluator: _Evaluator) -> None:
    for metric in evaluator.metrics:
        callback = getattr(metric, "end_callback", None)
        if not callable(callback):
            raise BehaviorContractError("OmniGibson metric is missing end_callback")
        callback(evaluator.env)


def _regular_source(path: Path) -> None:
    try:
        facts = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise BehaviorContractError(f"OmniGibson did not produce {path.name}") from exc
    if not stat.S_ISREG(facts.st_mode) or facts.st_nlink != 1:
        raise BehaviorContractError("OmniGibson output is not a private regular file")


def _copy_fresh(source: Path, destination: Path) -> None:
    _regular_source(source)
    if destination.exists() or destination.is_symlink():
        raise BehaviorContractError("Loom rollout payload destination already exists")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    shutil.copyfile(source, destination, follow_symlinks=False)


def _stamp_pipeline_seed(path: Path, seed: int) -> None:
    """Add the signed seed missing from the historical recorder's HDF5 ABI."""

    try:
        h5py = import_module("h5py")
        with h5py.File(path, "r+") as value:
            data = value["data"]
            demos = list(data.keys())
            if demos != ["demo_0"]:
                raise BehaviorContractError("OmniGibson HDF5 is not exactly one episode")
            demo = data["demo_0"]
            existing = demo.attrs.get("seed")
            if existing is not None and int(existing) != seed:
                raise BehaviorContractError("OmniGibson HDF5 seed disagrees with signed seed")
            demo.attrs["seed"] = seed
    except BehaviorContractError:
        raise
    except Exception as exc:
        raise BehaviorContractError("could not bind signed seed into rollout HDF5") from exc


class OmniGibsonStage1EpisodeDriver:
    """Drive exactly one signed task child through the pinned evaluator."""

    def __init__(
        self,
        *,
        runtime: _OmniRuntime | None = None,
        paths: RolloutPaths | None = None,
    ) -> None:
        self._runtime = runtime
        self._paths = paths or RolloutPaths()
        self._evaluator: _Evaluator | None = None
        self._request: StageRequestV1 | None = None
        self._success: bool | None = None
        self._completed = False
        self._preview_closed = False
        self._upstream_root = self._paths.scratch_root / "omnigibson-output"

    def load_task_instance(self, request: StageRequestV1) -> LoadedTaskInstance:
        if self._request is not None:
            raise BehaviorContractError("Stage 1 driver may load only one task child")
        inputs = load_mounted_inputs(request, self._paths)
        parameters = request.parameters
        if not isinstance(parameters, BehaviorRolloutParametersV1):
            raise BehaviorContractError("Stage 1 driver accepts only rollout parameters")
        task = inputs.task_instance.payload
        runtime = self._runtime or _load_runtime()
        if runtime.version != _OMNIGIBSON_VERSION:
            raise BehaviorContractError(
                f"Stage 1 requires OmniGibson {_OMNIGIBSON_VERSION}, found {runtime.version!r}"
            )
        if runtime.task_id(task.task_name) != task.behavior_task_id:
            raise BehaviorContractError("signed task name and OmniGibson task id disagree")
        evaluator = runtime.create_evaluator(
            task_name=task.task_name,
            eval_instance_index=task.eval_instance_index,
            output_root=self._upstream_root,
        )
        try:
            evaluator.reset()
            evaluator.load_task_instance(task.engine_task_instance_id, test_hidden=False)
        except Exception as exc:
            runtime.close_evaluator(evaluator)
            raise BehaviorContractError("signed OmniGibson task instance load failed") from exc
        self._runtime = runtime
        self._evaluator = evaluator
        self._request = request
        return LoadedTaskInstance(
            eval_instance_index=task.eval_instance_index,
            engine_task_instance_id=task.engine_task_instance_id,
            episode_index=task.episode_index,
            seed=task.seed,
        )

    def reset_episode(self, episode_index: int) -> None:
        request, evaluator = self._loaded()
        task = load_mounted_inputs(request, self._paths).task_instance.payload
        if episode_index != task.episode_index:
            raise BehaviorContractError("requested episode index disagrees with signed task child")
        try:
            evaluator.reset()
            evaluator.setup_recorder(
                output_folder=str(self._upstream_root),
                demo_id=task.demo_id,
                record_rgb=True,
                record_depth=False,
            )
            evaluator.start_episode_recording()
        except Exception as exc:
            raise BehaviorContractError("OmniGibson Stage 1 episode reset failed") from exc

    def run_episode(self, episode_index: int, *, output_dir: Path, scratch: Path) -> int:
        return self._run(episode_index, output_dir=output_dir, scratch=scratch, live_preview=None)

    def run_episode_with_live_preview(
        self,
        episode_index: int,
        *,
        output_dir: Path,
        scratch: Path,
        live_preview: EpisodeLivePreview,
    ) -> int:
        return self._run(
            episode_index,
            output_dir=output_dir,
            scratch=scratch,
            live_preview=live_preview,
        )

    def _run(
        self,
        episode_index: int,
        *,
        output_dir: Path,
        scratch: Path,
        live_preview: EpisodeLivePreview | None,
    ) -> int:
        request, evaluator = self._loaded()
        if output_dir != self._paths.engine_output_root or scratch != self._paths.scratch_root:
            raise BehaviorContractError("Stage 1 engine output/scratch path drift")
        task = load_mounted_inputs(request, self._paths).task_instance.payload
        if episode_index != task.episode_index or self._completed:
            raise BehaviorContractError("Stage 1 episode is not the single signed episode")
        terminated = truncated = False
        info: Mapping[str, object] = {}
        try:
            while not (terminated or truncated):
                terminated, truncated, info = evaluator.step()
                step_idx = _step_count(evaluator) - 1
                if live_preview is not None and step_idx % _PREVIEW_INTERVAL_STEPS == 0:
                    self._offer_preview(
                        evaluator=evaluator,
                        live_preview=live_preview,
                        step_idx=step_idx,
                    )
            done = info.get("done")
            if not isinstance(done, Mapping) or not isinstance(done.get("success"), bool):
                raise BehaviorContractError("OmniGibson terminal info has no boolean success")
            success = cast(bool, done["success"])
            if not evaluator.end_episode_recording(success):
                raise BehaviorContractError("OmniGibson did not save the signed episode")
            evaluator._finalize_episode_recording(
                instance_id=task.engine_task_instance_id,
                episode_idx=task.episode_index,
                demo_id=task.demo_id,
                success=success,
            )
            _metric_end(evaluator)
            self._success = success
            self._completed = True
            return 0
        except BehaviorContractError:
            raise
        except Exception as exc:
            raise BehaviorContractError("OmniGibson Stage 1 episode execution failed") from exc

    def _offer_preview(
        self,
        *,
        evaluator: _Evaluator,
        live_preview: EpisodeLivePreview,
        step_idx: int,
    ) -> None:
        if self._preview_closed:
            return
        try:
            runtime = cast(_OmniRuntime, self._runtime)
            live_preview.offer(step_idx=step_idx, jpeg=runtime.preview_jpeg(evaluator))
        except Exception:
            # Preview is explicitly non-authoritative.  Composition or sink
            # failure disables only the remaining preview callbacks.
            self._preview_closed = True

    def close(self) -> None:
        evaluator = self._evaluator
        runtime = self._runtime
        if evaluator is None or runtime is None:
            return
        self._evaluator = None
        runtime.close_evaluator(evaluator)
        if self._completed:
            self._project_output()

    def _loaded(self) -> tuple[StageRequestV1, _Evaluator]:
        if self._request is None or self._evaluator is None:
            raise BehaviorContractError("Stage 1 task must be loaded before episode execution")
        return self._request, self._evaluator

    def _project_output(self) -> None:
        if self._request is None or self._success is None:
            raise BehaviorContractError("Stage 1 output cannot be projected before completion")
        task = load_mounted_inputs(self._request, self._paths).task_instance.payload
        task_tag = f"task-{task.behavior_task_id:04d}"
        stem = task.demo_stem
        outcome = "success" if self._success else "failure"
        root = self._upstream_root / outcome / "2025-challenge-demos"
        hdf5 = root / "trajectories" / task_tag / f"{stem}.hdf5"
        _regular_source(hdf5)
        _stamp_pipeline_seed(hdf5, task.seed)
        sources = {
            self._paths.engine_output_root
            / "payload"
            / "trajectories"
            / task_tag
            / f"{stem}.hdf5": hdf5,
            self._paths.engine_output_root
            / "payload"
            / "meta"
            / "episodes"
            / task_tag
            / f"{stem}_bddl_transitions.json": (
                root / "meta" / "episodes" / task_tag / f"{stem}_bddl_transitions.json"
            ),
            self._paths.scratch_root
            / "meta"
            / "episodes"
            / task_tag
            / f"{stem}.json": root / "meta" / "episodes" / task_tag / f"{stem}.json",
        }
        for camera in ("head", "left_wrist", "right_wrist"):
            sources[
                self._paths.engine_output_root
                / "payload"
                / "videos"
                / task_tag
                / f"observation.images.rgb.{camera}"
                / f"{stem}.mp4"
            ] = (
                root
                / "videos"
                / task_tag
                / f"observation.images.rgb.{camera}"
                / f"{stem}.mp4"
            )
        for destination, source in sources.items():
            _copy_fresh(source, destination)


def create_episode_driver() -> OmniGibsonStage1EpisodeDriver:
    """Production factory resolved by the Loom-owned one-episode engine."""

    return OmniGibsonStage1EpisodeDriver()


if TYPE_CHECKING:

    def _protocol_check(driver: OmniGibsonStage1EpisodeDriver) -> EpisodeDriver:
        return driver


__all__ = [
    "PINNED_OMNIGIBSON_SOURCE_COMMIT",
    "OmniGibsonStage1EpisodeDriver",
    "create_episode_driver",
]
