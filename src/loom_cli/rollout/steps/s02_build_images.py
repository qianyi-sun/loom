"""Step 02 — docker build the rollout-critical images (#340, #365).

Builds each of the release-critical service images:
control-plane, gateway, service, web, worker, egress-xds. Uses the
worktree created by :mod:`s01_worktree` as the docker build context so
the operator's main checkout is untouched.

The set must cover every locally-tagged image referenced by a rendered
managed Deployment; ``tests/loom_cli/rollout/steps/test_s02_build_images.py``
diffs :data:`ROLLOUT_IMAGES` against the deploy manifests to catch a
future omission (#365 was exactly this — loom-web was left out and the
web pod failed with ImagePullBackOff after cluster-up).

Pip retry resilience (#199) is baked into the Dockerfiles via
``ENV PIP_RETRIES=10 PIP_DEFAULT_TIMEOUT=60``, so this step's transient
failure tolerance comes for free.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    AUXILIARY_ROLLOUT_IMAGES,
    ROLLOUT_IMAGES,
    build_exact_images,
    inspect_exact_images,
)
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import SubprocessResult, run_captured

__all__ = ["AUXILIARY_ROLLOUT_IMAGES", "ROLLOUT_IMAGES", "BuildImagesStep", "image_tag"]


def _all_build_images() -> tuple[tuple[str, str], ...]:
    return ALL_BUILD_IMAGES


def image_tag(image_name: str, ctx: RolloutContext) -> str:
    return f"{image_name}:{ctx.image_tag}"


def _run_docker(argv: Sequence[str], cwd: Path | None) -> SubprocessResult:
    return run_captured(list(argv), cwd=cwd)


class BuildImagesStep(BaseStep):
    number = 2
    name = "build-images"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        """Ask docker whether each expected tag exists."""
        try:
            inspect_exact_images(
                _run_docker,
                image_tag=ctx.image_tag,
                resolved_sha=ctx.resolved_sha,
            )
        except ValueError:
            return VerifyOutcome.MISMATCH
        else:
            return VerifyOutcome.MATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        # Build context lives at the worktree from step 01.
        worktree = step_dir.path.parent / "01-worktree" / "src"
        if not worktree.is_dir():
            return RunResult(
                exit_code=1,
                error=(
                    f"worktree not found at {worktree}; step 01 must "
                    "succeed before step 02 can build against it."
                ),
            )
        try:
            artifact = build_exact_images(
                _run_docker,
                candidate_root=worktree,
                image_tag=ctx.image_tag,
                resolved_sha=ctx.resolved_sha,
            )
        except ValueError as exc:
            return RunResult(exit_code=1, error=str(exc))
        self.write_stdout(
            step_dir,
            "\n".join(
                f"{name}={image_id}" for name, image_id in sorted(artifact.image_digests.items())
            ),
        )
        return RunResult(
            exit_code=0,
            summary=(f"verified {len(_all_build_images())} exact images at tag {ctx.image_tag}"),
        )
