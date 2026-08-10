"""Step 10 — cluster up + wait for convergence (#340)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.backup_limits import backup_traversal_limit_argv
from loom_cli.rollout.steps.candidate_source import (
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    rollout_cluster_config,
)
from loom_cli.rollout.steps.subcommand_step import SubcommandStep


class ClusterUpStep(SubcommandStep):
    number = 10
    name = "cluster-up"

    # Cluster up polls for convergence; give it a generous ceiling.
    timeout_sec = 900.0

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        argv = candidate_loom_argv(
            "cluster",
            "up",
            "--namespace",
            ctx.namespace,
            "--config",
            str(rollout_cluster_config(ctx, step_dir)),
            "--rendered-manifest",
            str(step_dir.path.parent / "07-render" / "rendered.yaml"),
            "--backup-manifest",
            str(ctx.backup_manifest_path),
            "--recover-sandbox-deadlines",
            "--sandbox-deadline-max-pods",
            "4",
        )
        argv.extend(backup_traversal_limit_argv(ctx))
        if ctx.request_envelope_path is not None:
            argv.extend(
                [
                    "--rollout-request-envelope",
                    str(ctx.request_envelope_path),
                ]
            )
        return argv

    def cwd(self, ctx: RolloutContext, step_dir: StepDir) -> Path:
        return candidate_loom_cwd(step_dir)

    def env(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> dict[str, str]:
        return candidate_loom_env(step_dir)
