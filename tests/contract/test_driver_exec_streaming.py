import asyncio
from pathlib import PurePosixPath

from loom.driver.fake import FakeDriver, scripted_streaming_handler


async def test_fake_exec_streaming_yields_chunks() -> None:
    handler = scripted_streaming_handler(
        stdout_chunks=[b"line 1\n", b"line 2\n"],
        stderr_chunks=[b"warn\n"],
        return_code=0,
    )
    driver = FakeDriver(streaming_handler=handler)
    await driver.start()
    handle = await driver.exec_streaming(
        ["echo", "hello"],
        env_vars={"FOO": "bar"},
        cwd=PurePosixPath("/workspace"),
    )
    out = b"".join([chunk async for chunk in handle.stdout])
    err = b"".join([chunk async for chunk in handle.stderr])
    rc = await handle.wait()
    assert out == b"line 1\nline 2\n"
    assert err == b"warn\n"
    assert rc == 0
    await driver.stop()


async def test_fake_exec_streaming_kill_completes_wait() -> None:
    handler = scripted_streaming_handler(
        stdout_chunks=[],
        stderr_chunks=[],
        return_code=137,
        sleep_before_exit=10.0,
    )
    driver = FakeDriver(streaming_handler=handler)
    await driver.start()
    handle = await driver.exec_streaming(
        ["sleep", "10"], env_vars={}, cwd=PurePosixPath("/workspace"),
    )
    await handle.kill()
    rc = await asyncio.wait_for(handle.wait(), timeout=1.0)
    assert rc == 137
    await driver.stop()


async def test_fake_exec_streaming_requires_handler() -> None:
    driver = FakeDriver()  # no streaming_handler
    await driver.start()
    import pytest

    with pytest.raises(NotImplementedError):
        await driver.exec_streaming(
            ["true"], env_vars={}, cwd=PurePosixPath("/workspace"),
        )
    await driver.stop()
