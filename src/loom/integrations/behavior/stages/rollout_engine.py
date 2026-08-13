"""One-episode BEHAVIOR engine entrypoint owned and supervised by Loom.

This module intentionally contains no campaign, Slurm, Hydra, or output-upload
logic.  The GPU image supplies the simulator-facing ``EpisodeDriver`` while
this code owns the signed request, seed points, and exactly-one episode loop.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, cast

from loom.integrations.behavior.canonical_json import load_canonical_document
from loom.integrations.behavior.contracts import (
    MAX_STAGE_REQUEST_BYTES,
    BehaviorRolloutParametersV1,
    BehaviorStage,
    StageRequestV1,
    validate_stage_request,
)
from loom.integrations.behavior.errors import BehaviorContractError, BehaviorExitCode
from loom.integrations.behavior.stages.pipeline_mode import SeedAuthority


@dataclass(frozen=True)
class LoadedTaskInstance:
    eval_instance_index: int
    engine_task_instance_id: int
    episode_index: int
    seed: int


class EpisodeDriver(Protocol):
    """Simulator seam implemented inside the pinned BEHAVIOR image."""

    def load_task_instance(self, request: StageRequestV1) -> LoadedTaskInstance: ...

    def reset_episode(self, episode_index: int) -> None: ...

    def run_episode(self, episode_index: int, *, output_dir: Path, scratch: Path) -> int: ...

    def close(self) -> None: ...


class EpisodeLivePreview(Protocol):
    """Non-authoritative sink called by the image-owned simulator driver.

    The driver offers only the already composed, metadata-free Stage 1 JPEG.
    The sink owns cadence, sequence allocation, validation, and atomic spool
    publication.  Implementations must contain every failure to this optional
    channel so simulator inference and final recording remain authoritative.
    """

    def offer(self, *, step_idx: int, jpeg: bytes) -> None: ...


class PreviewEpisodeDriver(EpisodeDriver, Protocol):
    """Additive image boundary for a driver that can supply live composites."""

    def run_episode_with_live_preview(
        self,
        episode_index: int,
        *,
        output_dir: Path,
        scratch: Path,
        live_preview: EpisodeLivePreview,
    ) -> int: ...


class _PreviewProducer(Protocol):
    def emit(
        self,
        *,
        sequence: int,
        step_idx: int,
        jpeg: bytes,
        monotonic_now: float,
    ) -> Any: ...


class _BestEffortLivePreview:
    """Adapt the strict producer to an exception-free simulator callback."""

    def __init__(self, producer: _PreviewProducer) -> None:
        self._producer = producer
        self._sequence = 0
        self._closed = False

    def offer(self, *, step_idx: int, jpeg: bytes) -> None:
        if self._closed:
            return
        try:
            result = self._producer.emit(
                sequence=self._sequence,
                step_idx=step_idx,
                jpeg=jpeg,
                monotonic_now=time.monotonic(),
            )
            if result.accepted:
                self._sequence += 1
        except Exception:
            # Preview is explicitly non-authoritative.  A malformed frame or
            # local spool failure permanently closes only this callback.
            self._closed = True


def best_effort_live_preview(producer: _PreviewProducer) -> EpisodeLivePreview:
    """Wrap the strict spool producer in the image driver's no-fail callback."""

    return _BestEffortLivePreview(producer)


def execute_one_episode(
    request: StageRequestV1,
    driver: EpisodeDriver,
    *,
    output_dir: Path,
    scratch: Path,
    seed_authority: SeedAuthority | None = None,
    live_preview: EpisodeLivePreview | None = None,
) -> int:
    """Run the signed episode index once, with the two fixed seed points."""

    if request.stage is not BehaviorStage.ROLLOUT or not isinstance(
        request.parameters, BehaviorRolloutParametersV1
    ):
        raise BehaviorContractError("rollout_engine accepts only rollout StageRequestV1")
    parameters = request.parameters
    authority = seed_authority or SeedAuthority(allow_same_seed_replay=True)
    try:
        authority.apply(parameters.seed)
        loaded = driver.load_task_instance(request)
        signed = (
            parameters.eval_instance_index,
            parameters.episode_index,
            parameters.seed,
        )
        actual = (loaded.eval_instance_index, loaded.episode_index, loaded.seed)
        if actual != signed:
            raise BehaviorContractError("loaded task parameters disagree with StageRequest")

        # This tuple is deliberately not ``range(1)``: episode 1 must remain 1.
        episodes = (parameters.episode_index,)
        if len(episodes) != 1:
            raise AssertionError("one request must contain exactly one episode")
        authority.apply(parameters.seed)
        driver.reset_episode(episodes[0])
        preview_run = getattr(driver, "run_episode_with_live_preview", None)
        if live_preview is not None and callable(preview_run):
            return cast(PreviewEpisodeDriver, driver).run_episode_with_live_preview(
                episodes[0],
                output_dir=output_dir,
                scratch=scratch,
                live_preview=live_preview,
            )
        return driver.run_episode(episodes[0], output_dir=output_dir, scratch=scratch)
    finally:
        driver.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m loom.integrations.behavior.stages.rollout_engine"
    )
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    return parser


def _production_driver() -> EpisodeDriver:
    """Load the image-owned simulator driver without an external source path."""

    try:
        module = import_module("loom.integrations.behavior.stages.rollout_backend")
        factory = cast(Callable[[], EpisodeDriver], module.create_episode_driver)
    except ImportError as exc:  # pragma: no cover - pinned GPU image boundary
        raise BehaviorContractError("pinned image is missing the Loom rollout backend") from exc
    return factory()


def _production_live_preview(
    request: StageRequestV1,
    *,
    scratch: Path,
) -> EpisodeLivePreview | None:
    """Build the Stage 1 producer without making preview a start dependency."""

    parameters = request.parameters
    if not isinstance(parameters, BehaviorRolloutParametersV1):
        return None
    # The fixed recording rate and signed Stage timeout provide a conservative
    # signed upper bound without trusting image/backend state.
    episode_bound = request.budget.timeout_seconds * parameters.recording_fps
    try:
        from loom_worker.pipeline_live_preview import PipelineLivePreviewProducer

        producer = PipelineLivePreviewProducer(
            preview_root=scratch.parent / "live-preview",
            episode_bound=episode_bound,
        )
    except Exception:
        return None
    return _BestEffortLivePreview(producer)


def main(
    argv: Sequence[str] | None = None,
    *,
    driver_factory: Callable[[], EpisodeDriver] = _production_driver,
) -> int:
    args = _parser().parse_args(argv)
    try:
        request = validate_stage_request(
            load_canonical_document(args.request, max_bytes=MAX_STAGE_REQUEST_BYTES)
        )
        return execute_one_episode(
            request,
            driver_factory(),
            output_dir=args.output_dir,
            scratch=args.scratch,
            live_preview=_production_live_preview(request, scratch=args.scratch),
        )
    except (BehaviorContractError, OSError, ValueError) as exc:
        print(f"contract error: {exc}")
        return int(BehaviorExitCode.CONTRACT_ERROR)


__all__ = [
    "EpisodeDriver",
    "EpisodeLivePreview",
    "LoadedTaskInstance",
    "PreviewEpisodeDriver",
    "best_effort_live_preview",
    "execute_one_episode",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
