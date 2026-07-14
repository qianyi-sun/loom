"""Step 99 — write summary.md (#340).

Emits a human-readable overview of every prior step's result. Always
succeeds — its job is documentation, not verification.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.operator.redaction import redact_rollout_text
from loom_cli.rollout.steps.base import BaseStep, RunResult

_MAX_RESULT_BYTES = 1024 * 1024
_MAX_INLINE_LENGTH = 1000


def _markdown_inline(value: object, *, limit: int = _MAX_INLINE_LENGTH) -> str:
    safe = redact_rollout_text(str(value), limit=limit)
    return safe.replace("\r", "\\r").replace("\n", "\\n").replace("|", "\\|").replace("`", "\\`")


def _read_result(path: Path) -> dict[str, Any] | None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_RESULT_BYTES:
            return None
        payload = bytearray()
        while len(payload) <= _MAX_RESULT_BYTES:
            chunk = os.read(fd, min(65536, _MAX_RESULT_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > _MAX_RESULT_BYTES:
            return None
        data = json.loads(bytes(payload).decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    finally:
        os.close(fd)
    return data if isinstance(data, dict) else None


class SummaryStep(BaseStep):
    number = 99
    name = "summary"

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        rollout_dir = step_dir.path.parent
        summary_path = step_dir.artifact_path("summary.md")

        lines: list[str] = []
        lines.append(f"# Rollout summary — {_markdown_inline(ctx.image_tag)}")
        lines.append("")
        lines.append(f"- Rollout id: `{_markdown_inline(rollout_dir.name)}`")
        lines.append(f"- Target ref: `{_markdown_inline(ctx.target_ref)}`")
        lines.append(f"- Resolved SHA: `{_markdown_inline(ctx.resolved_sha)}`")
        lines.append(
            f"- Cluster: `{_markdown_inline(ctx.cluster_name)}` "
            f"(namespace `{_markdown_inline(ctx.namespace)}`)"
        )
        lines.append(
            f"- Scope: `{_markdown_inline(ctx.scope)}` "
            + ("(exclude-oldlab)" if ctx.exclude_oldlab else "")
        )
        if ctx.request_id is not None:
            lines.append(f"- Request id: `{_markdown_inline(ctx.request_id)}`")
            lines.append(
                "- Initiating operator: "
                f"`{_markdown_inline(ctx.initiating_operator)}` "
                f"(uid `{_markdown_inline(ctx.initiating_uid)}`)"
            )
            lines.append(
                f"- Attempt: `{_markdown_inline(ctx.attempt_number)}` by "
                f"`{_markdown_inline(ctx.attempt_operator)}` "
                f"(uid `{_markdown_inline(ctx.attempt_uid)}`)"
            )
        lines.append("")
        lines.append("## Steps")
        lines.append("")
        lines.append("| # | Step | State | Summary |")
        lines.append("|---|---|---|---|")

        # Iterate directories NN-name in numeric order; read result.json.
        step_dirs = sorted(
            (p for p in rollout_dir.iterdir() if p.is_dir() and p.name[:2].isdigit()),
        )
        for sd in step_dirs:
            if sd == step_dir.path:
                continue  # skip self
            result_file = sd / "result.json"
            data = _read_result(result_file)
            if data is None:
                continue
            num = data.get("number", "?")
            name = data.get("name", sd.name)
            state = data.get("state", "?")
            summary = data.get("summary", "") or data.get("error", "") or ""
            rendered_num = f"{num:02d}" if type(num) is int else _markdown_inline(num)
            lines.append(
                f"| {rendered_num} | {_markdown_inline(name)} | "
                f"{_markdown_inline(state)} | {_markdown_inline(summary)} |"
            )
        summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.write_stdout(
            step_dir, f"wrote {summary_path.name} ({summary_path.stat().st_size} bytes)\n"
        )
        return RunResult(
            exit_code=0,
            summary=f"summary.md ({summary_path.stat().st_size} bytes)",
            artifacts={"summary_md": str(summary_path)},
        )
