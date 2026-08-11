"""One-episode BEHAVIOR engine entrypoint owned and supervised by Loom.

This module intentionally contains no campaign, Slurm, Hydra, or output-upload
logic.  The GPU image supplies the simulator-facing ``EpisodeDriver`` while
this code owns the signed request, seed points, and exactly-one episode loop.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast

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


def execute_one_episode(
    request: StageRequestV1,
    driver: EpisodeDriver,
    *,
    output_dir: Path,
    scratch: Path,
    seed_authority: SeedAuthority | None = None,
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
        )
    except (BehaviorContractError, OSError, ValueError) as exc:
        print(f"contract error: {exc}")
        return int(BehaviorExitCode.CONTRACT_ERROR)


if __name__ == "__main__":
    raise SystemExit(main())
