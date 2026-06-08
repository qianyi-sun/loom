import asyncio
from pathlib import PurePosixPath
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from loom_drivers.daytona.exec_stream import open_session_stream


async def test_streams_stdout_and_stderr_then_exits() -> None:
    sandbox = MagicMock()

    async def _create_session(sid: str) -> None:
        pass

    sandbox.process.create_session = _create_session

    async def _execute_session_command(sid: str, req: Any, timeout: Any = None) -> Any:
        rv = MagicMock()
        rv.cmd_id = "cmd-1"
        return rv

    sandbox.process.execute_session_command = _execute_session_command

    async def _get_logs(
        sid: str, cmd_id: str, on_stdout: Any, on_stderr: Any,
    ) -> None:
        on_stdout(b"hello\n")
        on_stderr(b"warn\n")
        on_stdout(b"world\n")

    sandbox.process.get_session_command_logs_async = _get_logs

    async def _get_cmd(sid: str, cmd_id: str) -> Any:
        rv = MagicMock()
        rv.exit_code = 0
        return rv

    sandbox.process.get_session_command = _get_cmd
    sandbox.process.delete_session = AsyncMock()

    handle = await open_session_stream(
        sandbox=sandbox,
        argv=["echo", "hi"],
        env_vars={"X": "1"},
        cwd=PurePosixPath("/work"),
        user=None,
    )
    out: list[bytes] = []
    err: list[bytes] = []
    async for c in handle.stdout:
        out.append(c)
    async for c in handle.stderr:
        err.append(c)
    assert b"".join(out) == b"hello\nworld\n"
    assert b"".join(err) == b"warn\n"
    assert await handle.wait() == 0
    sandbox.process.delete_session.assert_awaited_once()


async def test_kill_terminates_session_command() -> None:
    sandbox = MagicMock()
    sandbox.process.create_session = AsyncMock()

    async def _execute_session_command(sid: str, req: Any, timeout: Any = None) -> Any:
        rv = MagicMock()
        rv.cmd_id = "cmd-K"
        return rv

    sandbox.process.execute_session_command = _execute_session_command

    kill_called = {"yes": False}

    async def _get_logs(
        sid: str, cmd_id: str, on_stdout: Any, on_stderr: Any,
    ) -> None:
        while not kill_called["yes"]:
            await asyncio.sleep(0.01)

    sandbox.process.get_session_command_logs_async = _get_logs

    async def _get_cmd(sid: str, cmd_id: str) -> Any:
        rv = MagicMock()
        rv.exit_code = 137
        return rv

    sandbox.process.get_session_command = _get_cmd

    async def _exec(cmd: str, cwd: Any = None, env: Any = None, timeout: Any = None) -> Any:
        kill_called["yes"] = True
        rv = MagicMock()
        rv.exit_code = 0
        rv.result = ""
        return rv

    sandbox.process.exec = _exec
    sandbox.process.delete_session = AsyncMock()

    handle = await open_session_stream(
        sandbox=sandbox, argv=["sleep", "9999"],
        env_vars={}, cwd=PurePosixPath("/work"), user=None,
    )
    await handle.kill()
    rc = await handle.wait()
    assert rc == 137
