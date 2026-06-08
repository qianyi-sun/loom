"""Adapt sandbox.process.execute_session_command into a Loom ExecHandle.

Lifecycle:
- create_session(uuid) → execute_session_command(sid, req) returning cmd_id
- drain logs in a background task via get_session_command_logs_async
  with on_stdout/on_stderr callbacks that push into asyncio.Queues
- wait() awaits the drain task, then queries get_session_command for exit
- kill() shells out to `pkill -9 -f <argv>` via a buffered exec
- drop the session in finally
"""

from __future__ import annotations

import asyncio
import shlex
import uuid
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from typing import Any

from loom.driver.base import ExecHandle


async def open_session_stream(
    *,
    sandbox: Any,
    argv: list[str],
    env_vars: dict[str, str],
    cwd: PurePosixPath,
    user: str | int | None,
) -> ExecHandle:
    from daytona import SessionExecuteRequest

    session_id = f"loom-{uuid.uuid4().hex[:8]}"
    await sandbox.process.create_session(session_id)

    env_prefix = " ".join(
        f"{k}={shlex.quote(v)}" for k, v in env_vars.items()
    )
    cmd_str = " ".join(shlex.quote(a) for a in argv)
    shell_cmd = (
        f"cd {shlex.quote(str(cwd))}; "
        + (f"{env_prefix} " if env_prefix else "")
        + cmd_str
    )

    created = await sandbox.process.execute_session_command(
        session_id,
        SessionExecuteRequest(command=shell_cmd),
        timeout=None,
    )
    cmd_id = created.cmd_id

    stdout_q: asyncio.Queue[bytes | None] = asyncio.Queue()
    stderr_q: asyncio.Queue[bytes | None] = asyncio.Queue()

    def _on_stdout(chunk: bytes) -> None:
        if chunk:
            stdout_q.put_nowait(chunk)

    def _on_stderr(chunk: bytes) -> None:
        if chunk:
            stderr_q.put_nowait(chunk)

    async def _drain() -> None:
        try:
            await sandbox.process.get_session_command_logs_async(
                session_id, cmd_id, _on_stdout, _on_stderr,
            )
        finally:
            stdout_q.put_nowait(None)
            stderr_q.put_nowait(None)

    drain_task = asyncio.create_task(_drain())

    async def _iter(q: asyncio.Queue[bytes | None]) -> AsyncIterator[bytes]:
        while True:
            chunk = await q.get()
            if chunk is None:
                return
            yield chunk

    async def _wait() -> int:
        try:
            await drain_task
        except asyncio.CancelledError:
            pass
        info = await sandbox.process.get_session_command(session_id, cmd_id)
        rc = int(getattr(info, "exit_code", 0) or 0)
        try:
            await sandbox.process.delete_session(session_id)
        except Exception:
            pass
        return rc

    async def _kill() -> None:
        target = " ".join(argv)
        await sandbox.process.exec(
            f"pkill -9 -f {shlex.quote(target)} 2>/dev/null; true",
            timeout=10,
        )
        if not drain_task.done():
            drain_task.cancel()

    return ExecHandle(
        pid=0,
        stdout=_iter(stdout_q),
        stderr=_iter(stderr_q),
        _wait=_wait,
        _kill=_kill,
    )
