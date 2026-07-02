"""Step 11 — cluster up + wait for convergence (#340)."""

from __future__ import annotations

from collections.abc import Sequence

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.subcommand_step import SubcommandStep


class ClusterUpStep(SubcommandStep):
    number = 11
    name = "cluster-up"

    # Cluster up polls for convergence; give it a generous ceiling.
    timeout_sec = 900.0

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        return [
            "loom", "cluster", "up",
            "--wait",
            "--namespace", ctx.namespace,
            "--config", str(ctx.cluster_config_path),
        ]
