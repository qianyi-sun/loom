"""Step 08 — cluster preflight (#340)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.candidate_source import (
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    rollout_cluster_config,
)
from loom_cli.rollout.steps.subcommand_step import SubcommandStep


class PreflightStep(SubcommandStep):
    number = 8
    name = "preflight"

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        return candidate_loom_argv(
            "cluster",
            "preflight",
            "--namespace",
            ctx.namespace,
            "--config",
            str(rollout_cluster_config(ctx, step_dir)),
            "--backup-manifest",
            str(ctx.backup_manifest_path),
        )

    def cwd(self, ctx: RolloutContext, step_dir: StepDir) -> Path:
        return candidate_loom_cwd(step_dir)

    def env(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> dict[str, str]:
        return candidate_loom_env(step_dir)
