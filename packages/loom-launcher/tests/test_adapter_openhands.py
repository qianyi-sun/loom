"""OpenHandsAdapter contract: build_invocation + poll_local_http capture."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ExecHandle, ModelSpec, SandboxAccess


class _ScriptedHttpSandbox:
    def __init__(self, steps: list[tuple[int, list[dict]]]) -> None:
        self.steps = list(steps)
        self.idx = 0
        self.calls: list[list[str]] = []

    async def read_text(self, path: PurePosixPath) -> str:
        raise FileNotFoundError(str(path))

    async def exec_oneshot(
        self, argv: list[str], *, timeout_sec: float = 10.0,
    ) -> tuple[int, bytes]:
        self.calls.append(argv)
        if self.idx >= len(self.steps):
            return (0, json.dumps([]).encode())
        rc, payload = self.steps[self.idx]
        self.idx += 1
        return (rc, json.dumps(payload).encode())


def _handle_with(sandbox: SandboxAccess, *, runtime_sec: float = 0.3) -> ExecHandle:
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


def test_build_invocation_argv() -> None:
    adapter = get_adapter("openhands")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="solve it",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == [
        "python", "-m", "openhands.server",
        "--port", "9999",
        "--workdir", "/workspace",
        "--task", "solve it",
    ]


async def test_capture_via_http_poll() -> None:
    adapter = get_adapter("openhands")
    assert adapter is not None
    sandbox = _ScriptedHttpSandbox([
        (1, []),  # not ready
        (0, [{"id": 1, "kind": "thought"}]),
        (0, [{"id": 2, "kind": "tool_use", "name": "Bash"}]),
    ])
    handle = _handle_with(sandbox, runtime_sec=0.5)
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    ids = [e["id"] for e in events]
    assert 1 in ids
    assert 2 in ids
    # Verify it hit port 9999.
    assert any("localhost:9999" in " ".join(c) for c in sandbox.calls)
