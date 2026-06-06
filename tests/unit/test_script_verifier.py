import json
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.verifier.script_verifier import ScriptVerifier


def _ok_handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
    return ExecResult(
        return_code=0, stdout=b"", stderr=b"",
        truncated=False, duration_sec=0.01,
    )


@pytest.fixture
async def fake_with_script_output() -> FakeDriver:
    fake = FakeDriver(exec_handler=_ok_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = json.dumps({
        "rewards": {"score": 0.85},
        "checks": [
            {"name": "linter", "passed": True, "score": 1.0},
            {"name": "format", "passed": False, "score": 0.0, "message": "bad indent"},
        ],
    }).encode()
    return fake


async def test_script_verifier_reads_output(fake_with_script_output: FakeDriver):
    v = ScriptVerifier(script_path=PurePosixPath("/tests/check.sh"))
    result = await v.verify(
        task=None,  # type: ignore[arg-type]
        env=fake_with_script_output,
        artifacts_dir=PurePosixPath("/loom/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.rewards == {"score": 0.85}
    assert len(result.checks) == 2
    assert result.checks[1].name == "format"


async def test_script_verifier_missing_output_returns_error(tmp_path: Path):
    fake = FakeDriver(exec_handler=_ok_handler)
    await fake.start(options=StartOptions())
    v = ScriptVerifier(script_path=PurePosixPath("/tests/check.sh"))
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/x"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.error is not None
    assert result.error.kind == "missing_tests"


async def test_script_verifier_invalid_json_returns_parse_error():
    fake = FakeDriver(exec_handler=_ok_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = b"not json"
    v = ScriptVerifier(script_path=PurePosixPath("/tests/check.sh"))
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/x"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.error is not None
    assert result.error.kind == "parse_failure"
