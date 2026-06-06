"""AgentRuntime Protocol + InBoxAgentRuntime (spec §2.1).

The Protocol has ONE method (run); setup lives on InBoxAgentRuntime only —
out-of-box agents have nothing to install. Streaming is emergent from the
architecture (Gateway-emitted llm_call events or in-box JSONL tailing), not
enforced on the Protocol.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import PurePosixPath
from typing import Literal, Protocol, runtime_checkable

from loom.driver.base import Driver
from loom.models.mcp import MCPConnection
from loom.models.types import OS, ModelSpec
from loom.trajectory.writer import TrajectoryWriter


@runtime_checkable
class AgentRuntime(Protocol):
    """One trial's worth of agent execution."""

    mode: Literal["out-of-box", "in-box"]
    name: str
    version: str
    supports_os: frozenset[OS]
    model: ModelSpec | None

    async def run(
        self,
        *,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection],
        skills_dir: PurePosixPath | None,
        step_id: str,
    ) -> None: ...


@runtime_checkable
class InBoxAgentRuntime(AgentRuntime, Protocol):
    """In-box agents additionally need installation in the sandbox."""

    async def setup(self, env: Driver) -> None: ...
