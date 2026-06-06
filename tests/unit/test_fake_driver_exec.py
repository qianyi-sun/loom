import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult


@pytest.fixture
async def fake_with_table() -> FakeDriver:
    handler = command_table_handler({
        "ls /": ExecResult(return_code=0, stdout=b"bin\netc\n", stderr=b"",
                           truncated=False, duration_sec=0.01),
        "cat /missing": ExecResult(return_code=1, stdout=b"", stderr=b"No such file",
                                   truncated=False, duration_sec=0.01),
    })
    fake = FakeDriver(exec_handler=handler)
    await fake.start(options=StartOptions())
    return fake


async def test_command_table_match(fake_with_table: FakeDriver):
    r = await fake_with_table.exec("ls /")
    assert r.return_code == 0
    assert r.stdout == b"bin\netc\n"


async def test_command_table_miss_returns_default(fake_with_table: FakeDriver):
    r = await fake_with_table.exec("unmatched-cmd")
    assert r.return_code == 0
    assert r.stdout == b""


async def test_exec_truncation_enforced():
    """Spec §2.2: stdout cap at MAX_EXEC_STREAM_BYTES."""
    huge = b"a" * (12 * 1024 * 1024)  # 12 MB
    handler = command_table_handler({
        "echo big": ExecResult(return_code=0, stdout=huge, stderr=b"",
                               truncated=False, duration_sec=0.01),
    })
    fake = FakeDriver(exec_handler=handler)
    await fake.start(options=StartOptions())
    r = await fake.exec("echo big")
    assert len(r.stdout) == 10 * 1024 * 1024
    assert r.truncated is True


async def test_exec_truncation_marks_when_either_stream_clipped():
    """If only stderr exceeds the cap, truncated should still flip to True."""
    huge_err = b"e" * (11 * 1024 * 1024)
    handler = command_table_handler({
        "noisy": ExecResult(return_code=2, stdout=b"hi", stderr=huge_err,
                            truncated=False, duration_sec=0.01),
    })
    fake = FakeDriver(exec_handler=handler)
    await fake.start(options=StartOptions())
    r = await fake.exec("noisy")
    assert r.stdout == b"hi"
    assert len(r.stderr) == 10 * 1024 * 1024
    assert r.truncated is True


async def test_command_table_custom_default():
    """Caller-supplied default is returned on miss instead of empty success."""
    handler = command_table_handler(
        {},
        default=ExecResult(return_code=127, stdout=b"", stderr=b"not found",
                           truncated=False, duration_sec=0.0),
    )
    fake = FakeDriver(exec_handler=handler)
    await fake.start(options=StartOptions())
    r = await fake.exec("anything")
    assert r.return_code == 127
    assert r.stderr == b"not found"
