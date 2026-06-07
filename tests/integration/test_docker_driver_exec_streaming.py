"""DockerDriver.exec_streaming integration tests. Docker-gated."""

from __future__ import annotations

import asyncio
from pathlib import PurePosixPath

from loom.driver.docker import DockerDriver


async def test_docker_exec_streaming_echo() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "for i in 1 2 3; do echo line $i; sleep 0.05; done"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        out = b""
        async for chunk in handle.stdout:
            out += chunk
        rc = await handle.wait()
        assert rc == 0
        assert out == b"line 1\nline 2\nline 3\n"
    finally:
        await driver.stop()


async def test_docker_exec_streaming_env_vars_visible() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "echo $LOOM_TEST_VAR"],
            env_vars={"LOOM_TEST_VAR": "hello-from-test"},
            cwd=PurePosixPath("/workspace"),
        )
        out = b""
        async for chunk in handle.stdout:
            out += chunk
        rc = await handle.wait()
        assert rc == 0
        assert b"hello-from-test" in out
    finally:
        await driver.stop()


async def test_docker_exec_streaming_non_zero_exit() -> None:
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        handle = await driver.exec_streaming(
            ["sh", "-c", "exit 7"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        async for _ in handle.stdout:
            pass
        rc = await handle.wait()
        assert rc == 7
    finally:
        await driver.stop()


async def test_docker_exec_streaming_kill_is_callable() -> None:
    """kill() is best-effort across docker's PID namespaces (see ExecHandle
    docstring). We verify the public contract: kill() doesn't raise, and
    a process that runs naturally to completion still resolves wait()
    correctly."""
    driver = DockerDriver(image="alpine:3.20")
    await driver.start()
    try:
        # A short-lived process so wait() resolves on its own.
        handle = await driver.exec_streaming(
            ["sh", "-c", "exit 0"],
            env_vars={},
            cwd=PurePosixPath("/workspace"),
        )
        await handle.kill()  # must not raise even if the process is gone
        rc = await asyncio.wait_for(handle.wait(), timeout=5.0)
        assert rc == 0
    finally:
        await driver.stop()
