"""Step 07 — render Kubernetes manifests (#340).

Writes ``rendered.yaml`` into the step's evidence dir; subsequent steps
(preflight, migrate, cluster-up, release-gate) can point at this file.
"""

from __future__ import annotations

from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import run_captured


def rendered_yaml_path(step_dir: StepDir) -> Path:
    return step_dir.artifact_path("rendered.yaml")


class RenderStep(BaseStep):
    number = 7
    name = "render"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "image_tag": ctx.image_tag,
        }

    def _verify_impl(
        self, ctx: RolloutContext, step_dir: StepDir,
    ) -> VerifyOutcome:
        # Rendered yaml exists and is non-empty → treat as complete.
        rendered = rendered_yaml_path(step_dir)
        if rendered.is_file() and rendered.stat().st_size > 0:
            return VerifyOutcome.MATCH
        return VerifyOutcome.MISMATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        rendered = rendered_yaml_path(step_dir)
        result = run_captured(
            [
                "loom", "cluster", "render",
                "--config", str(ctx.cluster_config_path),
            ],
            stderr_log=step_dir.stderr_path(),
        )
        if result.returncode != 0:
            step_dir.stdout_path().write_text(result.stdout)
            return RunResult(
                exit_code=result.returncode,
                error=(
                    result.stderr.strip().splitlines()[-1]
                    if result.stderr.strip() else
                    f"loom cluster render exited {result.returncode}"
                ),
            )
        rendered.write_text(result.stdout)
        step_dir.stdout_path().write_text(
            f"rendered {rendered.stat().st_size} bytes to {rendered.name}\n"
        )
        return RunResult(
            exit_code=0,
            summary=f"rendered {rendered.stat().st_size} bytes",
            artifacts={"rendered_yaml": str(rendered)},
        )
