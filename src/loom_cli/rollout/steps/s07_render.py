"""Step 07 — render Kubernetes manifests (#340).

Writes ``rendered.yaml`` into the step's evidence dir; subsequent steps
(preflight, migrate, cluster-up, release-gate) can point at this file.
"""

from __future__ import annotations

from pathlib import Path

from loom_cli.cluster_config import (
    load_cluster_config,
    validate_container_registry_publication,
)
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.manifest_readiness import (
    inspect_rendered_manifests,
    pin_rendered_manifest_images,
)
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    rollout_cluster_config,
)
from loom_cli.rollout.steps.s02_build_images import rollout_all_image_bindings
from loom_cli.rollout.steps.s04_publish_images import registry_image_digests
from loom_cli.rollout.steps.subprocess_util import run_captured


def rendered_yaml_path(step_dir: StepDir) -> Path:
    return step_dir.artifact_path("rendered.yaml")


def _registry_publication(ctx: RolloutContext) -> tuple[str, str] | None:
    return validate_container_registry_publication(load_cluster_config(ctx.cluster_config_path))


def _validate_registry_render(
    ctx: RolloutContext,
    step_dir: StepDir,
    rendered_yaml: str,
    *,
    container_registry: str,
    registry_digests: dict[str, str],
) -> None:
    _plan, image_ids = rollout_all_image_bindings(ctx, step_dir)
    inspect_rendered_manifests(
        rendered_yaml,
        image_tag=ctx.image_tag,
        namespace=ctx.namespace,
        image_digests=image_ids,
        expected_image_names=registry_digests,
        container_registry=container_registry,
        registry_digests=registry_digests,
    )


class RenderStep(BaseStep):
    number = 7
    name = "render"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        rendered = rendered_yaml_path(step_dir)
        if not rendered.is_file() or rendered.stat().st_size <= 0:
            return VerifyOutcome.MISMATCH
        try:
            publication = _registry_publication(ctx)
            if publication is not None:
                digests = registry_image_digests(ctx, step_dir)
                _validate_registry_render(
                    ctx,
                    step_dir,
                    rendered.read_text(encoding="utf-8"),
                    container_registry=publication[0],
                    registry_digests=digests,
                )
        except (OSError, RuntimeError, ValueError):
            return VerifyOutcome.MISMATCH
        return VerifyOutcome.MATCH

    def verify_done(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome | None:
        return self._verify_impl(ctx, step_dir)

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        rendered = rendered_yaml_path(step_dir)
        try:
            cwd = candidate_loom_cwd(step_dir)
            env = candidate_loom_env(step_dir)
        except CandidateToolingError as exc:
            self.write_stderr(step_dir, str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))
        result = run_captured(
            candidate_loom_argv(
                "cluster",
                "render",
                "--config",
                str(rollout_cluster_config(ctx, step_dir)),
            ),
            stderr_log=step_dir.stderr_path(),
            cwd=cwd,
            env=env,
        )
        if result.returncode != 0:
            self.write_stdout(step_dir, result.stdout)
            return RunResult(
                exit_code=result.returncode,
                error=(
                    result.stderr.strip().splitlines()[-1]
                    if result.stderr.strip()
                    else f"loom cluster render exited {result.returncode}"
                ),
            )
        rendered_yaml = result.stdout
        try:
            publication = _registry_publication(ctx)
            if publication is not None:
                digests = registry_image_digests(ctx, step_dir)
                rendered_yaml = pin_rendered_manifest_images(
                    rendered_yaml,
                    image_tag=ctx.image_tag,
                    container_registry=publication[0],
                    registry_digests=digests,
                )
                _validate_registry_render(
                    ctx,
                    step_dir,
                    rendered_yaml,
                    container_registry=publication[0],
                    registry_digests=digests,
                )
        except (OSError, RuntimeError, ValueError) as exc:
            self.write_stderr(step_dir, str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))
        rendered.write_text(rendered_yaml, encoding="utf-8")
        self.write_stdout(
            step_dir,
            f"rendered {rendered.stat().st_size} bytes to {rendered.name}\n",
        )
        return RunResult(
            exit_code=0,
            summary=f"rendered {rendered.stat().st_size} bytes",
            artifacts={"rendered_yaml": str(rendered)},
        )
