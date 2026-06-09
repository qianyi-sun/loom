"""Adapt Modal's sync ``ContainerProcess`` to Loom's async ``ExecHandle``.

Modal returns a process object with sync-iterable ``.stdout`` / ``.stderr``
that yield ``str`` chunks. Loom's ``ExecHandle`` wants ``AsyncIterator[bytes]``.
We drain the sync iterators in background threads via ``asyncio.to_thread``,
encode each chunk to bytes (surrogateescape so a malformed byte never
silently drops), and push into asyncio queues that async iterators read.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from typing import Any

from loom.driver.base import ExecHandle


def _encode(chunk: str | bytes) -> bytes:
    if isinstance(chunk, bytes):
        return chunk
    return chunk.encode("utf-8", errors="surrogateescape")


def make_exec_handle(proc: Any) -> ExecHandle:
    """Wrap a modal ``ContainerProcess`` in Loom's ``ExecHandle``.

    ``proc`` must expose ``.stdout`` (sync iterable), ``.stderr`` (sync
    iterable), ``.wait() -> int``, ``.terminate() -> None``.

    Must be called from inside a running event loop (i.e. from a
    coroutine); the drain threads dispatch chunks back into the loop
    via ``call_soon_threadsafe``.
    """
    loop = asyncio.get_running_loop()
    out_q: asyncio.Queue[bytes | None] = asyncio.Queue()
    err_q: asyncio.Queue[bytes | None] = asyncio.Queue()
    stop_flag = threading.Event()

    def _drain(src: Any, q: asyncio.Queue[bytes | None]) -> None:
        try:
            for chunk in src:
                if stop_flag.is_set():
                    break
                if chunk is None or chunk == "" or chunk == b"":
                    continue
                loop.call_soon_threadsafe(q.put_nowait, _encode(chunk))
        finally:
            loop.call_soon_threadsafe(q.put_nowait, None)

    out_task = asyncio.create_task(
        asyncio.to_thread(_drain, proc.stdout, out_q),
    )
    err_task = asyncio.create_task(
        asyncio.to_thread(_drain, proc.stderr, err_q),
    )

    async def _iter(q: asyncio.Queue[bytes | None]) -> AsyncIterator[bytes]:
        while True:
            chunk = await q.get()
            if chunk is None:
                return
            yield chunk

    async def _wait() -> int:
        await asyncio.gather(out_task, err_task, return_exceptions=True)
        return int(await asyncio.to_thread(proc.wait))

    async def _kill() -> None:
        stop_flag.set()
        try:
            await asyncio.to_thread(proc.terminate)
        except Exception:
            pass

    return ExecHandle(
        pid=0,
        stdout=_iter(out_q),
        stderr=_iter(err_q),
        _wait=_wait,
        _kill=_kill,
    )
