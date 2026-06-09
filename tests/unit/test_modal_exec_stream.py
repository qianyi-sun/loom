"""make_exec_handle — adapt sync ContainerProcess to ExecHandle."""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import MagicMock


class _SyncStream:
    """Mimics modal ContainerProcess.stdout: sync-iterable str chunks."""

    def __init__(self, chunks: list[str]) -> None:
        self._chunks = chunks

    def __iter__(self):  # type: ignore[no-untyped-def]
        yield from self._chunks


class _FakeProc:
    def __init__(
        self, stdout: list[str], stderr: list[str], rc: int,
    ) -> None:
        self.stdout = _SyncStream(stdout)
        self.stderr = _SyncStream(stderr)
        self._rc = rc
        self.terminated = False

    def wait(self) -> int:
        return self._rc

    def terminate(self) -> None:
        self.terminated = True


async def _collect(it: Any) -> bytes:
    buf = b""
    async for c in it:
        buf += c
    return buf


async def test_exec_handle_streams_stdout_and_exit_code() -> None:
    from loom_drivers.modal.exec_stream import make_exec_handle

    proc = _FakeProc(["hello\n", "world\n"], [], 0)
    handle = make_exec_handle(proc)
    out = await _collect(handle.stdout)
    err = await _collect(handle.stderr)
    rc = await handle.wait()
    assert out == b"hello\nworld\n"
    assert err == b""
    assert rc == 0


async def test_exec_handle_streams_stderr() -> None:
    from loom_drivers.modal.exec_stream import make_exec_handle

    proc = _FakeProc(["ok\n"], ["boom\n"], 1)
    handle = make_exec_handle(proc)
    out = await _collect(handle.stdout)
    err = await _collect(handle.stderr)
    rc = await handle.wait()
    assert out == b"ok\n"
    assert err == b"boom\n"
    assert rc == 1


async def test_exec_handle_kill_calls_terminate() -> None:
    from loom_drivers.modal.exec_stream import make_exec_handle

    proc = _FakeProc([], [], 137)
    handle = make_exec_handle(proc)
    await handle.kill()
    assert proc.terminated is True


async def test_exec_handle_does_not_block_event_loop() -> None:
    """Drain runs in a worker thread; the loop stays responsive."""
    from loom_drivers.modal.exec_stream import make_exec_handle

    class _SlowStream:
        def __iter__(self):  # type: ignore[no-untyped-def]
            for i in range(5):
                time.sleep(0.05)
                yield f"line{i}\n"

    proc = MagicMock()
    proc.stdout = _SlowStream()
    proc.stderr = _SyncStream([])
    proc.wait.return_value = 0
    proc.terminate = MagicMock()

    handle = make_exec_handle(proc)

    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while ticks < 6:
            await asyncio.sleep(0.02)
            ticks += 1

    t = asyncio.create_task(ticker())
    out = await _collect(handle.stdout)
    await _collect(handle.stderr)
    rc = await handle.wait()
    t.cancel()
    assert out.count(b"line") == 5
    assert rc == 0
    assert ticks >= 5, f"loop blocked (ticks={ticks})"


async def test_bytes_chunks_handle_non_utf8_safely() -> None:
    """If Modal yields a str with surrogates, encode with surrogateescape
    so we don't drop bytes on the floor."""
    from loom_drivers.modal.exec_stream import make_exec_handle

    proc = _FakeProc(["a\udcfeb"], [], 0)
    handle = make_exec_handle(proc)
    out = await _collect(handle.stdout)
    assert b"a" in out and b"b" in out
