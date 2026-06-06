from pathlib import Path, PurePosixPath

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient, GatewayCallResponse
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom.trajectory.reader import TrajectoryReader
from loom.verifier.llm_judge import LLMJudgeVerifier


def _judge_resp(content: str) -> GatewayCallResponse:
    return GatewayCallResponse(
        response=ChatMessage(role="assistant", content=content),
        input_tokens=100, cached_input_tokens=0, cache_write_tokens=0,
        output_tokens=20, thinking_tokens=0,
        provider_extras={}, cost_usd=0.005,
        finish_reason="stop", duration_sec=1.0, streamed=False,
        time_to_first_token_sec=None, rate_card_hash="card",
        gateway_request_id="req",
    )


@pytest.fixture
def trajectory_file(tmp_path: Path) -> Path:
    f = tmp_path / "events.jsonl"
    f.write_text("")
    return f


async def test_judge_parses_structured_response(trajectory_file: Path):
    gateway = FakeLLMGatewayClient(scripted=[_judge_resp(
        '{"rewards": {"correctness": 0.8}, "confidence": 0.9, "rationale": "ok"}',
    )])
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    reader = TrajectoryReader(trajectory_file)

    v = LLMJudgeVerifier(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=gateway, team_id="t", trial_id="r",
        rubric_prompt="Grade this.",
    )
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/x"), trajectory=reader,
    )
    assert result.rewards == {"correctness": 0.8}
    assert result.confidence == 0.9
    assert result.structured is not None
    assert result.structured["rationale"] == "ok"


async def test_judge_parse_failure_records_error(trajectory_file: Path):
    gateway = FakeLLMGatewayClient(scripted=[_judge_resp("not json")])
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    reader = TrajectoryReader(trajectory_file)
    v = LLMJudgeVerifier(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=gateway, team_id="t", trial_id="r",
        rubric_prompt="Grade.",
    )
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/x"), trajectory=reader,
    )
    assert result.error is not None
    assert result.error.kind == "parse_failure"


async def test_judge_rejects_non_object_rewards(trajectory_file: Path):
    """Spec §2.4: judge must return rewards as an object; a non-object
    surfaces as parse_failure rather than silently dropping data."""
    gateway = FakeLLMGatewayClient(scripted=[_judge_resp(
        '{"rewards": "bad", "confidence": 0.5}',
    )])
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    reader = TrajectoryReader(trajectory_file)
    v = LLMJudgeVerifier(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        gateway=gateway, team_id="t", trial_id="r",
        rubric_prompt="Grade.",
    )
    result = await v.verify(
        task=None, env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/x"), trajectory=reader,
    )
    assert result.error is not None
    assert result.error.kind == "parse_failure"
