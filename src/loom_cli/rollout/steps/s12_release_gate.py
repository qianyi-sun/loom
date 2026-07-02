"""Step 12 — release gate (#340)."""

from __future__ import annotations

from collections.abc import Sequence

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.subcommand_step import SubcommandStep


class ReleaseGateStep(SubcommandStep):
    number = 12
    name = "release-gate"

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        # Point release-gate at the manifest rendered in step 07.
        rendered = (
            step_dir.path.parent / "07-render" / "rendered.yaml"
        )
        return [
            "loom", "cluster", "release-gate",
            "--namespace", ctx.namespace,
            "--config", str(ctx.cluster_config_path),
            "--rendered-manifest", str(rendered),
        ]
