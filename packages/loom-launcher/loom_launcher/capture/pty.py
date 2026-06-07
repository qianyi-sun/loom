"""tail_pty — best-effort PTY scrape for TUI-only agents.

Used by adapters whose agent CLI runs an interactive TUI with no
structured output mode (codex, qwen-cli, kimi-cli at time of writing).
We accept lossy capture: lines that match a known prompt pattern become
`AgentThoughtEvent`-shaped dicts; everything else is dropped. Adapters
that use this mark themselves `degraded=True` so dashboards can flag
trajectories captured this way.

For v1, this is intentionally a minimal stub — the real-world agents
that need it will define their own prompt patterns. The function is
exposed for symmetry with the other three primitives.
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator

from loom_launcher.adapter import ExecHandle, TrajectoryEventLike
from loom_launcher.capture.stdout_jsonl import _DictEvent

logger = logging.getLogger(__name__)

# ANSI escape codes we strip before pattern matching. CSI sequences cover
# cursor moves, colors, etc.; TUI agents emit them generously.
_ANSI_CSI_RE = re.compile(rb"\x1b\[[0-?]*[ -/]*[@-~]")


async def tail_pty(
    handle: ExecHandle,
    *,
    prompt_pattern: re.Pattern[bytes] | None = None,
) -> AsyncIterator[TrajectoryEventLike]:
    """Yield events for each line on `handle.stdout` that matches
    `prompt_pattern` (after ANSI strip). If `prompt_pattern` is None,
    yields ALL non-empty lines wrapped as `_DictEvent({"line": ...})`.

    Best-effort: TUI streams often produce mid-line cursor moves that
    fragment a logical message across multiple stdout chunks; we
    re-assemble at LF boundaries only.
    """
    buf = b""
    async for chunk in handle.stdout:
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            stripped = _ANSI_CSI_RE.sub(b"", line).strip()
            if not stripped:
                continue
            if prompt_pattern is None or prompt_pattern.search(stripped):
                yield _DictEvent({
                    "line": stripped.decode("utf-8", errors="replace"),
                    "kind": "tty_thought",
                })
