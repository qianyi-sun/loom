"""AiderAdapter contract: build_invocation + tail_log_file on chat history."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ExecHandle, ModelSpec, SandboxAccess


class _ScriptedSandbox:
    """Yields a growing file across reads — each call returns the next snapshot."""

    def __init__(self, snapshots: list[str]) -> None:
        self.snapshots = list(snapshots)
        self.idx = 0

    async def read_text(self, path: PurePosixPath) -> str:
        if self.idx < len(self.snapshots) - 1:
            self.idx += 1
        return self.snapshots[self.idx]

    async def exec_oneshot(
        self, argv: list[str], *, timeout_sec: float = 10.0,
    ) -> tuple[int, bytes]:
        return (1, b"")


def _handle_with_sandbox(
    sandbox: SandboxAccess, *, runtime_sec: float = 0.3,
) -> ExecHandle:
    async def _empty() -> AsyncIterator[bytes]:
        if False:
            yield b""

    async def _wait() -> int:
        await asyncio.sleep(runtime_sec)
        return 0

    async def _kill() -> None:
        pass

    return ExecHandle(
        pid=0, stdout=_empty(), stderr=_empty(),
        _wait=_wait, _kill=_kill, sandbox=sandbox,
    )


def test_build_invocation_argv_and_telemetry_env() -> None:
    adapter = get_adapter("aider")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="fix the bug",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == [
        "aider",
        "--yes-always",
        "--no-auto-commits",
        "--model", "openai/gpt-5",
        "--message", "fix the bug",
    ]
    assert env["AIDER_NO_TELEMETRY"] == "1"


async def test_capture_via_log_file_tail() -> None:
    adapter = get_adapter("aider")
    assert adapter is not None
    # Real aider chat-history.md format: markdown sections per turn.
    snapshots = [
        "",
        "# user\n",
        "# user\nfix the bug\n",
        "# user\nfix the bug\n# assistant\n",
        "# user\nfix the bug\n# assistant\nlooking at it...\n",
    ]
    sandbox = _ScriptedSandbox(snapshots)
    handle = _handle_with_sandbox(sandbox, runtime_sec=1.2)
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    seen = [e["line"] for e in events]
    assert "# user" in seen
    assert "fix the bug" in seen
    assert "# assistant" in seen
    assert "looking at it..." in seen
