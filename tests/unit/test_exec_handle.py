import asyncio
from collections.abc import AsyncIterator

from loom.driver.base import ExecHandle


async def _empty() -> AsyncIterator[bytes]:
    if False:
        yield b""


async def _ok() -> int:
    return 0


async def _noop() -> None:
    pass


async def test_exec_handle_fields_present() -> None:
    handle = ExecHandle(
        pid=1234,
        stdout=_empty(),
        stderr=_empty(),
        _wait=_ok,
        _kill=_noop,
    )
    assert handle.pid == 1234


async def test_exec_handle_wait_returns_code() -> None:
    async def _w() -> int:
        return 42

    handle = ExecHandle(
        pid=1, stdout=_empty(), stderr=_empty(), _wait=_w, _kill=_noop,
    )
    assert await handle.wait() == 42


async def test_exec_handle_kill_awaitable() -> None:
    killed = asyncio.Event()

    async def _k() -> None:
        killed.set()

    handle = ExecHandle(
        pid=1, stdout=_empty(), stderr=_empty(), _wait=_ok, _kill=_k,
    )
    await handle.kill()
    assert killed.is_set()
