import json
from pathlib import PurePosixPath
from typing import Any

import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.verifier.structured import StructuredOutputVerifier

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "score": {"type": "number", "minimum": 0, "maximum": 1},
    },
    "required": ["answer"],
}


@pytest.fixture
async def fake() -> FakeDriver:
    f = FakeDriver()
    await f.start(options=StartOptions())
    return f


async def test_valid_artifact_passes(fake: FakeDriver):
    fake.filesystem[PurePosixPath("/loom/artifacts/output.json")] = (
        json.dumps({"answer": "42", "score": 0.9}).encode()
    )
    v = StructuredOutputVerifier(
        artifact_path=PurePosixPath("/loom/artifacts/output.json"),
        schema=SCHEMA,
    )
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/loom/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.rewards["valid"] == 1.0
    assert result.error is None


async def test_invalid_artifact_records_error(fake: FakeDriver):
    fake.filesystem[PurePosixPath("/loom/artifacts/output.json")] = (
        json.dumps({"answer": 42}).encode()
    )
    v = StructuredOutputVerifier(
        artifact_path=PurePosixPath("/loom/artifacts/output.json"),
        schema=SCHEMA,
    )
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/loom/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.rewards["valid"] == 0.0
    assert result.error is not None


async def test_missing_artifact_returns_missing_tests(fake: FakeDriver):
    v = StructuredOutputVerifier(
        artifact_path=PurePosixPath("/loom/artifacts/missing.json"),
        schema=SCHEMA,
    )
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/loom/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.error is not None
    assert result.error.kind == "missing_tests"


async def test_invalid_json_artifact_parses_to_parse_failure(fake: FakeDriver):
    fake.filesystem[PurePosixPath("/loom/artifacts/output.json")] = b"not json at all"
    v = StructuredOutputVerifier(
        artifact_path=PurePosixPath("/loom/artifacts/output.json"),
        schema=SCHEMA,
    )
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/loom/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.error is not None
    assert result.error.kind == "parse_failure"
