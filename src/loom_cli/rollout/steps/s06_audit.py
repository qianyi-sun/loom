"""Step 06 — public/internal boundary audit (#340)."""

from __future__ import annotations

from collections.abc import Sequence

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.subcommand_step import SubcommandStep


class AuditStep(SubcommandStep):
    number = 6
    name = "audit"

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        return [
            "loom", "cluster", "audit",
            "--namespace", ctx.namespace,
            "--config", str(ctx.cluster_config_path),
        ]
