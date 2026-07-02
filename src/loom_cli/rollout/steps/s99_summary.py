"""Step 99 — write summary.md (#340).

Emits a human-readable overview of every prior step's result. Always
succeeds — its job is documentation, not verification.
"""

from __future__ import annotations

import json

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult


class SummaryStep(BaseStep):
    number = 99
    name = "summary"

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        rollout_dir = step_dir.path.parent
        summary_path = step_dir.artifact_path("summary.md")

        lines: list[str] = []
        lines.append(f"# Rollout summary — {ctx.image_tag}\n")
        lines.append(f"- Rollout id: `{rollout_dir.name}`")
        lines.append(f"- Target ref: `{ctx.target_ref}`")
        lines.append(f"- Resolved SHA: `{ctx.resolved_sha}`")
        lines.append(f"- Cluster: `{ctx.cluster_name}` (namespace `{ctx.namespace}`)")
        lines.append(f"- Scope: `{ctx.scope}` " + (
            "(exclude-oldlab)" if ctx.exclude_oldlab else ""
        ))
        lines.append("")
        lines.append("## Steps")
        lines.append("")
        lines.append("| # | Step | State | Summary |")
        lines.append("|---|---|---|---|")

        # Iterate directories NN-name in numeric order; read result.json.
        step_dirs = sorted(
            (p for p in rollout_dir.iterdir()
             if p.is_dir() and p.name[:2].isdigit()),
        )
        for sd in step_dirs:
            if sd == step_dir.path:
                continue  # skip self
            result_file = sd / "result.json"
            if not result_file.is_file():
                continue
            try:
                data = json.loads(result_file.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            num = data.get("number", "?")
            name = data.get("name", sd.name)
            state = data.get("state", "?")
            summary = data.get("summary", "") or data.get("error", "") or ""
            lines.append(
                f"| {num:02d} | {name} | {state} | {summary} |"
            )
        summary_path.write_text("\n".join(lines) + "\n")
        step_dir.stdout_path().write_text(
            f"wrote {summary_path.name} ({summary_path.stat().st_size} bytes)\n"
        )
        return RunResult(
            exit_code=0,
            summary=f"summary.md ({summary_path.stat().st_size} bytes)",
            artifacts={"summary_md": str(summary_path)},
        )
