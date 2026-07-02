"""Step 09 — Alembic migration via sanctioned Job (#340).

Uses ``loom cluster render-migration`` from #332 to build a proper
migration Job manifest, applies it via kubectl, and waits for the Job's
condition=complete. Deterministic job suffix so rerunning against the
same evidence dir is idempotent (same Job name → kubectl apply reuses
if pending, or hits AlreadyExists on completed Jobs cleaning up via TTL).
"""

from __future__ import annotations

import hashlib

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.subprocess_util import run_captured


def _deterministic_job_suffix(ctx: RolloutContext) -> str:
    """Return a stable short suffix so rerunning finds the same Job.

    Uses first 8 chars of sha256 over (image_tag, resolved_sha, config).
    """
    h = hashlib.sha256(
        f"{ctx.image_tag}|{ctx.resolved_sha}|{ctx.cluster_config_sha256}".encode(),
    ).hexdigest()
    return h[:8]


class MigrateStep(BaseStep):
    number = 9
    name = "migrate"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "image_tag": ctx.image_tag,
            "namespace": ctx.namespace,
            "job_suffix": _deterministic_job_suffix(ctx),
        }

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        suffix = _deterministic_job_suffix(ctx)
        # Render the migration manifest.
        render = run_captured(
            [
                "loom", "cluster", "render-migration",
                "--image-tag", ctx.image_tag,
                "--namespace", ctx.namespace,
                "--job-suffix", suffix,
            ],
        )
        if render.returncode != 0:
            step_dir.stderr_path().write_text(render.stderr)
            return RunResult(
                exit_code=render.returncode,
                error=f"render-migration failed: {render.stderr.strip()[:200]}",
            )
        manifest = step_dir.artifact_path("migration.yaml")
        manifest.write_text(render.stdout)

        # Apply.
        apply_ = run_captured(
            ["kubectl", "-n", ctx.namespace, "apply", "-f", str(manifest)],
        )
        if apply_.returncode != 0:
            step_dir.stderr_path().write_text(apply_.stderr)
            return RunResult(
                exit_code=apply_.returncode,
                error=f"kubectl apply migration failed: {apply_.stderr.strip()[:200]}",
            )

        # Wait for the Job to complete or fail.
        job_selector = (
            f"app=loom-migration,loom.image-tag={ctx.image_tag}"
        )
        wait = run_captured([
            "kubectl", "-n", ctx.namespace,
            "wait", "--for=condition=complete",
            f"--selector={job_selector}",
            "--timeout=600s",
            "job",
        ])
        step_dir.stdout_path().write_text(
            f"# render-migration\n{render.stdout[:2000]}\n"
            f"# kubectl apply\n{apply_.stdout}\n"
            f"# kubectl wait\n{wait.stdout}\n"
        )
        if wait.returncode != 0:
            step_dir.stderr_path().write_text(wait.stderr)
            return RunResult(
                exit_code=wait.returncode,
                error=(
                    f"migration Job did not complete: "
                    f"{wait.stderr.strip()[:200]}"
                ),
            )
        return RunResult(
            exit_code=0,
            summary=f"migration Job complete (suffix={suffix})",
            artifacts={"job_selector": job_selector, "suffix": suffix},
        )
