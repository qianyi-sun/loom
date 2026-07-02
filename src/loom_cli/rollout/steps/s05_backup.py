"""Step 05 — protected backup snapshot (#340).

Wraps ``loom cluster backup`` for the target namespace. Backup output
(the bundle path + manifest) is captured in the step's evidence dir.
"""

from __future__ import annotations

from collections.abc import Sequence

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.subcommand_step import SubcommandStep


class BackupStep(SubcommandStep):
    number = 5
    name = "backup"

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        # `loom cluster backup` bundles Postgres + MinIO into the
        # protected data dir. Its own idempotence handles repeat calls.
        return [
            "loom", "cluster", "backup",
            "--namespace", ctx.namespace,
            "--label", f"pre-rollout={ctx.image_tag}",
        ]
