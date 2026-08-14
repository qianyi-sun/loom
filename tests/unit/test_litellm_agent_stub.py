from collections.abc import AsyncGenerator
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient, GatewayCallResponse
from loom.agent.litellm import LiteLLMAgent
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.trajectory import ChatMessage, EventKind
from loom.models.types import ModelSpec
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


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


def _resp(text: str, finish: str = "stop") -> GatewayCallResponse:
    return GatewayCallResponse(
        response=ChatMessage(role="assistant", content=text),
        input_tokens=10, cached_input_tokens=0, cache_write_tokens=0,
        output_tokens=5, thinking_tokens=0,
        provider_extras={}, cost_usd=0.001,
        finish_reason=finish, duration_sec=0.05, streamed=False,
        time_to_first_token_sec=None, rate_card_hash="card-2026-06",
        gateway_request_id=f"req-{uuid4()}",
    )


async def test_single_turn_emits_llm_call_event(writer: TrajectoryWriter):
    fake_gateway = FakeLLMGatewayClient(scripted=[_resp("done")])
    driver = FakeDriver()
    await driver.start(options=StartOptions())

    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway, team_id="t1", trial_id=uuid4(), max_turns=1,
    )
    await agent.run(
        instruction="hello", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    reader = TrajectoryReader(writer.local_path)
    llm_calls = list(reader.iter_kind(EventKind.LLM_CALL))
    assert len(llm_calls) == 1
    assert llm_calls[0].response.content == "done"


async def test_agent_forwards_request_params_to_gateway(writer: TrajectoryWriter):
    fake_gateway = FakeLLMGatewayClient(scripted=[_resp("done")])
    driver = FakeDriver()
    await driver.start(options=StartOptions())

    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway,
        team_id="t1",
        trial_id=uuid4(),
        max_turns=1,
        request_params={
            "temperature": 0,
            "top_p": 0.5,
            "seed": 1234,
            "messages": [{"role": "user", "content": "secret"}],
            "api_key": "sk-hidden",
            "extra_body": {"top_k": 40, "prompt": "secret"},
        },
    )
    await agent.run(
        instruction="hello", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    assert fake_gateway.calls_recorded[0].request_params == {
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "extra_body": {"top_k": 40},
    }


def test_metadata():
    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=FakeLLMGatewayClient(scripted=[]),
        team_id="t", trial_id=uuid4(),
    )
    assert agent.mode == "out-of-box"
    assert agent.name == "direct-completion"
    assert "linux" in agent.supports_os
    assert agent.model is not None


async def test_multi_turn_records_all_calls(writer: TrajectoryWriter):
    fake_gateway = FakeLLMGatewayClient(scripted=[
        _resp("partial 1", finish="length"),
        _resp("partial 2", finish="length"),
        _resp("final", finish="stop"),
    ])
    driver = FakeDriver()
    await driver.start(options=StartOptions())
    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway, team_id="t", trial_id=uuid4(), max_turns=5,
    )
    await agent.run(
        instruction="x", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )
    reader = TrajectoryReader(writer.local_path)
    assert len(list(reader.iter_kind(EventKind.LLM_CALL))) == 3


async def test_max_turns_exhausted_raises(writer: TrajectoryWriter):
    """If the gateway never returns finish_reason='stop', the agent must
    raise AgentError so the trial fails cleanly rather than spinning."""
    from loom.errors import AgentError
    fake_gateway = FakeLLMGatewayClient(scripted=[
        _resp("x", finish="length"),
        _resp("y", finish="length"),
    ])
    driver = FakeDriver()
    await driver.start(options=StartOptions())
    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway, team_id="t", trial_id=uuid4(), max_turns=2,
    )
    with pytest.raises(AgentError, match="max_turns"):
        await agent.run(
            instruction="x", env=driver, trajectory=writer,
            mcp=[], skills_dir=None, step_id="main",
        )


async def test_final_answer_artifact_strips_helper_code_block_but_keeps_answer_text(
    writer: TrajectoryWriter,
):
    """AIME-style answer artifacts must not receive helper code blocks."""
    content = """I will compute it with a quick check.

```python
print(75)
```

The final answer is \\boxed{70}.
"""
    fake_gateway = FakeLLMGatewayClient(scripted=[_resp(content)])
    driver = FakeDriver()
    await driver.start(options=StartOptions())

    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway,
        team_id="t",
        trial_id=uuid4(),
        max_turns=1,
        artifact_paths=["final_answer.txt"],
    )
    await agent.run(
        instruction="solve", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    written = driver.filesystem[PurePosixPath("/workspace/final_answer.txt")].decode()
    assert "print(75)" not in written
    assert written.endswith("The final answer is \\boxed{70}.")


async def test_harbor_answer_artifact_writes_normalized_value(
    writer: TrajectoryWriter,
):
    content = """Scratch work first.

```python
print(75)
```

The final answer is \\boxed{70}.
"""
    fake_gateway = FakeLLMGatewayClient(scripted=[_resp(content)])
    driver = FakeDriver()
    await driver.start(options=StartOptions())

    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway,
        team_id="t",
        trial_id=uuid4(),
        max_turns=1,
        artifact_paths=["answer.txt"],
    )
    await agent.run(
        instruction="solve", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    assert driver.filesystem[PurePosixPath("/workspace/answer.txt")] == b"70"


async def test_harbor_answer_artifact_strips_terminal_sentence_period(
    writer: TrajectoryWriter,
):
    fake_gateway = FakeLLMGatewayClient(
        scripted=[_resp("The final answer is 70.")],
    )
    driver = FakeDriver()
    await driver.start(options=StartOptions())

    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway,
        team_id="t",
        trial_id=uuid4(),
        max_turns=1,
        artifact_paths=["answer.txt"],
    )
    await agent.run(
        instruction="solve", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    assert driver.filesystem[PurePosixPath("/workspace/answer.txt")] == b"70"


async def test_code_artifact_keeps_fenced_code_block(writer: TrajectoryWriter):
    content = """Here is the implementation.

```python
def answer():
    return 42
```

This solves the task.
"""
    fake_gateway = FakeLLMGatewayClient(scripted=[_resp(content)])
    driver = FakeDriver()
    await driver.start(options=StartOptions())

    agent = LiteLLMAgent(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=fake_gateway,
        team_id="t",
        trial_id=uuid4(),
        max_turns=1,
        artifact_paths=["solution.py"],
    )
    await agent.run(
        instruction="solve", env=driver, trajectory=writer,
        mcp=[], skills_dir=None, step_id="main",
    )

    assert driver.filesystem[PurePosixPath("/workspace/solution.py")] == (
        b"def answer():\n    return 42"
    )


async def test_direct_completion_rejects_artifact_glob_destination(
    writer: TrajectoryWriter,
) -> None:
    fake_gateway = FakeLLMGatewayClient(scripted=[_resp("not an image")])
    driver = FakeDriver()
    await driver.start(options=StartOptions())
    agent = LiteLLMAgent(
        model=ModelSpec(provider="openai", name="gpt-4o"),
        gateway=fake_gateway,
        team_id="t",
        trial_id=uuid4(),
        max_turns=1,
        artifact_paths=["*.png"],
    )

    from loom.errors import AgentError

    with pytest.raises(AgentError, match="exact relative artifact path"):
        await agent.run(
            instruction="make a poster",
            env=driver,
            trajectory=writer,
            mcp=[],
            skills_dir=None,
            step_id="main",
        )

    assert PurePosixPath("/workspace/*.png") not in driver.filesystem


@pytest.mark.parametrize("artifact_path", ["/root/output.txt", "../output.txt", ""])
async def test_direct_completion_rejects_non_relative_artifact_destination(
    writer: TrajectoryWriter,
    artifact_path: str,
) -> None:
    driver = FakeDriver()
    await driver.start(options=StartOptions())
    agent = LiteLLMAgent(
        model=ModelSpec(provider="openai", name="gpt-4o"),
        gateway=FakeLLMGatewayClient(scripted=[_resp("answer")]),
        team_id="t",
        trial_id=uuid4(),
        max_turns=1,
        artifact_paths=[artifact_path],
    )

    from loom.errors import AgentError

    with pytest.raises(AgentError, match="exact relative artifact path"):
        await agent.run(
            instruction="answer",
            env=driver,
            trajectory=writer,
            mcp=[],
            skills_dir=None,
            step_id="main",
        )
