"""tail_log_file contract tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import PurePosixPath

from loom_launcher.adapter import ExecHandle, SandboxAccess
from loom_launcher.capture import tail_log_file


class _ScriptedSandbox:
    """Yields a growing file across reads — each call returns the next
    snapshot. Models an agent writing to a log over time."""

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
    sandbox: SandboxAccess, *, runtime_sec: float = 0.2,
) -> ExecHandle:
    async def _empty() -> AsyncIterator[bytes]:
        if False:
            yield b""

    finished = asyncio.Event()

    async def _wait() -> int:
        await asyncio.sleep(runtime_sec)
        finished.set()
        return 0

    async def _kill() -> None:
        finished.set()

    return ExecHandle(
        pid=0, stdout=_empty(), stderr=_empty(),
        _wait=_wait, _kill=_kill, sandbox=sandbox,
    )


async def test_yields_lines_as_file_grows() -> None:
    sandbox = _ScriptedSandbox([
        "",
        "line 1\n",
        "line 1\nline 2\n",
        "line 1\nline 2\nline 3\n",
    ])
    handle = _handle_with_sandbox(sandbox, runtime_sec=0.6)
    events = [
        e.model_dump() async for e in tail_log_file(
            handle, path=PurePosixPath("/tmp/log"),
            poll_interval_sec=0.1,
        )
    ]
    seen_lines = [e["line"] for e in events]
    assert "line 1" in seen_lines
    assert "line 2" in seen_lines
    assert "line 3" in seen_lines


async def test_missing_file_does_not_crash() -> None:
    """The agent may not have created the log when we first poll. We
    keep polling; missing file is a no-op."""
    # An empty-snapshots sandbox is unused below — kept structure for parity.

    # Override read_text to always raise FileNotFoundError for the first
    # half of the runtime, then return content.
    class _LateSandbox:
        def __init__(self) -> None:
            self.call = 0

        async def read_text(self, path: PurePosixPath) -> str:
            self.call += 1
            if self.call < 3:
                raise FileNotFoundError(str(path))
            return "appeared\n"

        async def exec_oneshot(
            self, argv: list[str], *, timeout_sec: float = 10.0,
        ) -> tuple[int, bytes]:
            return (1, b"")

    sb = _LateSandbox()
    handle = _handle_with_sandbox(sb, runtime_sec=0.4)
    events = [
        e.model_dump() async for e in tail_log_file(
            handle, path=PurePosixPath("/tmp/log"),
            poll_interval_sec=0.05,
        )
    ]
    assert any(e["line"] == "appeared" for e in events)


async def test_requires_sandbox(make_handle) -> None:
    handle = make_handle(stdout_chunks=[])
    import pytest
    with pytest.raises(RuntimeError, match=r"requires ExecHandle\.sandbox"):
        _ = [
            e async for e in tail_log_file(
                handle, path=PurePosixPath("/tmp/log"),
            )
        ]


async def test_line_to_event_transform() -> None:
    sandbox = _ScriptedSandbox(["", "skip\nkeep\nskip\nkeep\n"])
    handle = _handle_with_sandbox(sandbox, runtime_sec=0.2)

    def parser(line: str) -> dict | None:
        return {"line": line, "wrapped": True} if line == "keep" else None

    events = [
        e.model_dump() async for e in tail_log_file(
            handle, path=PurePosixPath("/tmp/log"),
            poll_interval_sec=0.05,
            line_to_event=parser,
        )
    ]
    assert all(e["line"] == "keep" for e in events)
    assert all(e["wrapped"] is True for e in events)
