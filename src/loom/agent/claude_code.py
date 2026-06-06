"""ClaudeCodeAgent — in-box runtime that installs the Claude Code CLI inside
the sandbox and tails its JSONL output (spec §2.1 in-box mode).

v1 contract: the in-sandbox CLI is responsible for writing JSONL events to
`/loom/trajectory.jsonl` per the Loom-aware contract. The host runtime
downloads the file after `claude` returns and forwards each event to the
host TrajectoryWriter.

v1 limitations:
- Install step is a placeholder that creates `/loom/` and assumes the host
  image has Claude Code pre-installed. A real installer (apt/curl/etc.)
  ships with Plan 4 or later.
- We read the JSONL file once after the CLI exits; live tailing during
  execution is a v1.5 concern.
"""

from __future__ import annotations

import shlex
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Literal
from uuid import UUID

from pydantic import TypeAdapter

from loom.driver.base import Driver
from loom.errors import AgentError
from loom.models.mcp import MCPConnection
from loom.models.trajectory import TrajectoryEvent
from loom.models.types import OS, ModelSpec
from loom.trajectory.writer import TrajectoryWriter

_TRAJECTORY_PATH = PurePosixPath("/loom/trajectory.jsonl")
_event_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


@dataclass
class ClaudeCodeAgent:
    """In-box runtime tailing /loom/trajectory.jsonl."""

    team_id: str
    trial_id: UUID
    mode: Literal["out-of-box", "in-box"] = "in-box"
    name: str = "claude-code-agent"
    version: str = "1.0"
    supports_os: frozenset[OS] = field(default_factory=lambda: frozenset({"linux"}))
    model: ModelSpec | None = None

    async def setup(self, env: Driver) -> None:
        # Ensure /loom/ exists and is writable by the workload user. The
        # actual `claude` binary is assumed present in the image for v1.
        result = await env.exec(
            "mkdir -p /loom && chmod 0777 /loom",
            user="root",
        )
        if result.return_code != 0:
            raise AgentError(
                f"setup mkdir /loom failed rc={result.return_code} "
                f"stderr={result.stderr[:512]!r}",
            )

    async def run(
        self,
        *,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection],
        skills_dir: PurePosixPath | None,
        step_id: str,
    ) -> None:
        # Invoke the in-box CLI. v1 assumes the image provides a `claude`
        # binary that honours LOOM_TRAJECTORY.
        cmd = (
            "LOOM_TRAJECTORY=/loom/trajectory.jsonl claude --instruction "
            + shlex.quote(instruction)
        )
        result = await env.exec(cmd)
        if result.return_code != 0:
            raise AgentError(
                f"claude exited rc={result.return_code}; "
                f"stderr={result.stderr[:512]!r}",
            )

        # Pull the trajectory file out of the sandbox and forward each event
        # to the host writer. A missing file is legitimate — the CLI may have
        # completed without LLM activity.
        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "trajectory.jsonl"
            try:
                await env.download(_TRAJECTORY_PATH, local)
            except FileNotFoundError:
                return
            with local.open("rb") as fh:
                for raw in fh:
                    line = raw.strip()
                    if not line:
                        continue
                    event = _event_adapter.validate_json(line)
                    await trajectory.append(event)
