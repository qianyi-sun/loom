"""Step 03 — load images into kind cluster containerd (#340).

Delegates to the ``loom cluster load-images`` subcommand shipped in
#96 (see PR #344). Uses ``--check-only`` first so we don't waste time
re-loading images that are already present, then loads only the
missing ones.
"""

from __future__ import annotations

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
)
from loom_cli.rollout.steps.s02_build_images import (
    ROLLOUT_IMAGES,
    image_tag,
)
from loom_cli.rollout.steps.subprocess_util import run_captured


def _loom_cluster_load_images_argv(
    ctx: RolloutContext,
    *,
    check_only: bool,
) -> list[str]:
    argv = candidate_loom_argv(
        "cluster",
        "load-images",
        "--cluster-name",
        ctx.cluster_name,
    )
    for image, _ in ROLLOUT_IMAGES:
        argv += ["--image", image_tag(image, ctx)]
    if check_only:
        argv.append("--check-only")
    return argv


class KindLoadImagesStep(BaseStep):
    number = 4
    name = "kind-load-images"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "cluster_name": ctx.cluster_name,
            "image_tag": ctx.image_tag,
            "images": [image_tag(i, ctx) for i, _ in ROLLOUT_IMAGES],
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        # Cheap: call --check-only. Zero exit → all present.
        try:
            check = run_captured(
                _loom_cluster_load_images_argv(ctx, check_only=True),
                cwd=candidate_loom_cwd(step_dir),
                env=candidate_loom_env(step_dir),
            )
        except CandidateToolingError:
            return VerifyOutcome.UNKNOWN
        if check.returncode == 0:
            return VerifyOutcome.MATCH
        if check.returncode == 1:
            return VerifyOutcome.MISMATCH
        return VerifyOutcome.UNKNOWN

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            cwd = candidate_loom_cwd(step_dir)
            env = candidate_loom_env(step_dir)
        except CandidateToolingError as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))

        # Try check-only first; skip the load if everything's already there.
        check = run_captured(
            _loom_cluster_load_images_argv(ctx, check_only=True),
            stdout_log=step_dir.artifact_path("check-only.stdout"),
            stderr_log=step_dir.artifact_path("check-only.stderr"),
            cwd=cwd,
            env=env,
        )
        if check.returncode == 0:
            step_dir.stdout_path().write_text(
                "check-only: all images already present in kind\n",
            )
            return RunResult(
                exit_code=0,
                summary="all images already loaded",
            )

        # Load. run_captured will overwrite the top-level stdout/stderr logs.
        result = run_captured(
            _loom_cluster_load_images_argv(ctx, check_only=False),
            stdout_log=step_dir.stdout_path(),
            stderr_log=step_dir.stderr_path(),
            cwd=cwd,
            env=env,
        )
        if result.returncode != 0:
            return RunResult(
                exit_code=result.returncode,
                error=(
                    result.stderr.strip().splitlines()[-1]
                    if result.stderr.strip()
                    else f"loom cluster load-images exited {result.returncode}"
                ),
            )
        return RunResult(
            exit_code=0,
            summary=f"loaded {len(ROLLOUT_IMAGES)} images into {ctx.cluster_name}",
        )
