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

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import run_captured

#: Rollout-critical images. Kept as a module-level constant so a test
#: can parametrize over the same list AND diff it against the rendered
#: deploy manifests.
ROLLOUT_IMAGES: tuple[tuple[str, str], ...] = (
    ("loom-control-plane", "deploy/Dockerfile.control-plane"),
    ("loom-llm-gateway",   "deploy/Dockerfile.gateway"),
    ("loom-service",       "deploy/Dockerfile.service"),
    ("loom-web",           "deploy/Dockerfile.web"),
    ("loom-worker",        "deploy/Dockerfile.worker"),
    ("loom-egress-xds",    "deploy/Dockerfile.egress-xds"),
)


def image_tag(image_name: str, ctx: RolloutContext) -> str:
    return f"{image_name}:{ctx.image_tag}"


class BuildImagesStep(BaseStep):
    number = 2
    name = "build-images"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
        }

    def _verify_impl(
        self, ctx: RolloutContext, step_dir: StepDir,
    ) -> VerifyOutcome:
        """Ask docker whether each expected tag exists."""
        missing: list[str] = []
        for image, _ in ROLLOUT_IMAGES:
            tag = image_tag(image, ctx)
            result = run_captured(
                ["docker", "inspect", "--type=image", tag],
            )
            if result.returncode != 0:
                missing.append(tag)
        if not missing:
            return VerifyOutcome.MATCH
        return VerifyOutcome.MISMATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        # Build context lives at the worktree from step 01.
        worktree = (
            step_dir.path.parent / "01-worktree" / "src"
        )
        if not worktree.is_dir():
            return RunResult(
                exit_code=1,
                error=(
                    f"worktree not found at {worktree}; step 01 must "
                    "succeed before step 02 can build against it."
                ),
            )
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        for image, dockerfile in ROLLOUT_IMAGES:
            tag = image_tag(image, ctx)
            # If the tag already exists (recovery from a partial prior
            # run), skip re-building — the tests verify() step catches
            # this at resume, but re-running with make-safe deltas here
            # too keeps the step idempotent under partial recovery.
            inspect = run_captured(
                ["docker", "inspect", "--type=image", tag],
            )
            if inspect.returncode == 0:
                stdout_lines.append(f"# {tag} already present; skipping")
                continue

            result = run_captured(
                [
                    "docker", "build",
                    "-f", dockerfile,
                    "-t", tag,
                    ".",
                ],
                cwd=worktree,
            )
            stdout_lines.append(f"# docker build {tag}\n{result.stdout}")
            stderr_lines.append(f"# docker build {tag}\n{result.stderr}")
            if result.returncode != 0:
                step_dir.stdout_path().write_text("\n".join(stdout_lines))
                step_dir.stderr_path().write_text("\n".join(stderr_lines))
                return RunResult(
                    exit_code=result.returncode,
                    error=(
                        result.stderr.strip().splitlines()[-1]
                        if result.stderr.strip() else
                        f"docker build {tag} exited {result.returncode}"
                    ),
                )
        step_dir.stdout_path().write_text("\n".join(stdout_lines))
        step_dir.stderr_path().write_text("\n".join(stderr_lines))
        return RunResult(
            exit_code=0,
            summary=(
                f"built {len(ROLLOUT_IMAGES)} images at tag "
                f"{ctx.image_tag}"
            ),
        )
