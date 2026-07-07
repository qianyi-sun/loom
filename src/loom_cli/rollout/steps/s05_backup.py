"""Step 05 — verify protected backup manifest (#340, #363).

The rollout driver does not create the backup itself: producing the
Postgres dump, mirroring MinIO, and exporting Kubernetes secrets requires
credentials and cluster access the driver doesn't hold. The operator
follows the runbook procedure to produce a backup bundle + metadata
manifest before invoking ``loom cluster rollout --backup-manifest ...``.

This step wraps ``loom cluster backup check`` so the driver refuses to
advance past step 05 without a fresh, verified manifest for the target
environment + namespace.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.candidate_source import (
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
)
from loom_cli.rollout.steps.subcommand_step import SubcommandStep


class BackupStep(SubcommandStep):
    number = 5
    name = "backup"

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        return candidate_loom_argv(
            "cluster",
            "backup",
            "check",
            "--environment",
            ctx.environment,
            "--namespace",
            ctx.namespace,
            "--manifest",
            str(ctx.backup_manifest_path),
            "--min-remaining-hours",
            str(ctx.backup_manifest_min_remaining_hours),
        )

    def cwd(self, ctx: RolloutContext, step_dir: StepDir) -> Path:
        return candidate_loom_cwd(step_dir)

    def env(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> dict[str, str]:
        return candidate_loom_env(step_dir)
