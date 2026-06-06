import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import pytest

from loom.agent.claude_code import ClaudeCodeAgent
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.models.trajectory import EventKind
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


def _scripted_jsonl(events: list[dict[str, Any]]) -> bytes:
    return b"\n".join(json.dumps(e).encode() for e in events) + b"\n"


@pytest.fixture
def store() -> FakeObjectStore:
    return FakeObjectStore()


@pytest.fixture
async def writer(
    tmp_path: Path, store: FakeObjectStore,
) -> AsyncGenerator[TrajectoryWriter, None]:
    w = TrajectoryWriter(
        local_path=tmp_path / "events.jsonl", store=store,
        bucket="trajectories", key=f"team/{uuid4()}/events.jsonl",
        min_part_bytes=0,
    )
    async with w:
        yield w


async def test_setup_creates_loom_dir():
    cmds_run: list[str] = []

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        cmds_run.append(cmd)
        return ExecResult(
            return_code=0, stdout=b"", stderr=b"",
            truncated=False, duration_sec=0.01,
        )

    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())

    agent = ClaudeCodeAgent(team_id="t", trial_id=uuid4())
    await agent.setup(env=driver)

    assert any("mkdir" in c and "/loom" in c for c in cmds_run)


async def test_run_tails_in_box_jsonl(writer: TrajectoryWriter):
    scripted = _scripted_jsonl([
        {
            "kind": "agent_thought",
            "emitted_at": datetime.now(UTC).isoformat(),
            "trial_id": str(uuid4()),
            "step_id": "main",
            "seq": 0,
            "content": "I'm thinking",
        },
        {
            "kind": "tool_use",
            "emitted_at": datetime.now(UTC).isoformat(),
            "trial_id": str(uuid4()),
            "step_id": "main",
            "seq": 1,
            "tool_name": "read",
            "args": {"path": "/x"},
            "result": {"content": "x"},
            "duration_sec": 0.1,
        },
    ])

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        return ExecResult(
            return_code=0, stdout=b"finished\n", stderr=b"",
            truncated=False, duration_sec=0.01,
        )

    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())
    driver.filesystem[PurePosixPath("/loom/trajectory.jsonl")] = scripted

    agent = ClaudeCodeAgent(team_id="t", trial_id=uuid4())
    await agent.setup(env=driver)
    await agent.run(
        instruction="hi", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    reader = TrajectoryReader(writer.local_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.AGENT_THOUGHT in kinds
    assert EventKind.TOOL_USE in kinds


async def test_run_tolerates_missing_jsonl(writer: TrajectoryWriter):
    """Spec: if the in-box CLI completes without writing any events (e.g., a
    trivial task), download() returns FileNotFoundError → agent.run returns
    silently rather than raising."""
    driver = FakeDriver()
    await driver.start(options=StartOptions())
    agent = ClaudeCodeAgent(team_id="t", trial_id=uuid4())
    await agent.setup(env=driver)
    # /loom/trajectory.jsonl was never created → download raises FileNotFoundError
    await agent.run(
        instruction="trivial", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )
    reader = TrajectoryReader(writer.local_path)
    assert list(reader.iter_all()) == []


async def test_malformed_jsonl_raises_agent_error(writer: TrajectoryWriter):
    """Regression for Bug 6: a malformed JSONL line from the in-box CLI used
    to raise pydantic.ValidationError → classified as INTERNAL_ERROR. Now
    wrapped as AgentError → AGENT_ERROR."""
    from loom.errors import AgentError

    driver = FakeDriver()
    await driver.start(options=StartOptions())
    driver.filesystem[PurePosixPath("/loom/trajectory.jsonl")] = b'{"not": "valid event"}\n'
    agent = ClaudeCodeAgent(team_id="t", trial_id=uuid4())
    await agent.setup(env=driver)
    with pytest.raises(AgentError, match="malformed"):
        await agent.run(
            instruction="hi", env=driver, trajectory=writer,
            mcp=[], skills_dir=None, step_id="main",
        )


def test_metadata():
    agent = ClaudeCodeAgent(team_id="t", trial_id=uuid4())
    assert agent.mode == "in-box"
    assert agent.name == "claude-code-agent"
    assert "linux" in agent.supports_os
