"""poll_local_http contract tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import PurePosixPath

from loom_launcher.adapter import ExecHandle, SandboxAccess
from loom_launcher.capture import poll_local_http


class _ScriptedHttpSandbox:
    """Each call to exec_oneshot bumps an internal step; returns the
    scripted (rc, payload) for that step. Used to drive the polling loop."""

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


async def test_polls_and_yields_events() -> None:
    sandbox = _ScriptedHttpSandbox([
        (1, []),                                # endpoint not ready
        (0, [{"id": 1, "kind": "thought"}]),
        (0, [{"id": 2, "kind": "tool_use"}, {"id": 3, "kind": "thought"}]),
    ])
    handle = _handle_with(sandbox, runtime_sec=0.6)
    events = [
        e.model_dump() async for e in poll_local_http(
            handle, port=9000, poll_interval_sec=0.05,
        )
    ]
    ids = [e["id"] for e in events]
    assert 1 in ids
    assert 2 in ids
    assert 3 in ids


async def test_url_includes_since_offset() -> None:
    """Subsequent polls increment the `since` query parameter so the
    server can return only new events."""
    sandbox = _ScriptedHttpSandbox([
        (0, [{"id": 1}, {"id": 2}]),
        (0, [{"id": 3}]),
    ])
    handle = _handle_with(sandbox, runtime_sec=0.3)
    _ = [
        e async for e in poll_local_http(
            handle, port=9000, poll_interval_sec=0.05,
        )
    ]
    assert any("since=0" in " ".join(c) for c in sandbox.calls)
    assert any("since=2" in " ".join(c) for c in sandbox.calls)


async def test_non_json_response_logged_and_skipped() -> None:
    """The endpoint may return text or HTML during startup; we don't
    crash on it, just skip and try again."""

    class _GarbageSandbox:
        def __init__(self) -> None:
            self.calls = 0

        async def read_text(self, path: PurePosixPath) -> str:
            raise FileNotFoundError(str(path))

        async def exec_oneshot(
            self, argv: list[str], *, timeout_sec: float = 10.0,
        ) -> tuple[int, bytes]:
            self.calls += 1
            if self.calls < 3:
                return (0, b"<html>not ready</html>")
            return (0, json.dumps([{"id": 99}]).encode())

    sandbox = _GarbageSandbox()
    handle = _handle_with(sandbox, runtime_sec=0.3)
    events = [
        e.model_dump() async for e in poll_local_http(
            handle, port=9000, poll_interval_sec=0.05,
        )
    ]
    assert any(e["id"] == 99 for e in events)


async def test_requires_sandbox(make_handle) -> None:
    handle = make_handle(stdout_chunks=[])
    import pytest
    with pytest.raises(RuntimeError, match=r"requires ExecHandle\.sandbox"):
        _ = [
            e async for e in poll_local_http(handle, port=9000)
        ]
