from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from loom.models.trajectory import (
    AgentThoughtEvent,
    ChatMessage,
    LLMCallEvent,
    StepEndEvent,
    StepStartEvent,
    ToolUseEvent,
    TrialEndEvent,
    TrialStartEvent,
)
from loom.models.types import ModelSpec
from loom.trajectory.atif import project_to_atif


def _ev(seq: int, **kwargs: Any) -> dict[str, Any]:
    return {
        "emitted_at": datetime.now(UTC),
        "trial_id": uuid4(),
        "step_id": kwargs.pop("step_id", "main"),
        "seq": seq,
        **kwargs,
    }


def _llm_call_event(seq: int) -> LLMCallEvent:
    return LLMCallEvent(
        **_ev(seq),
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        rate_card_hash="abc",
        system_prompt=None,
        messages=[],
        response=ChatMessage(role="assistant", content="ok"),
        finish_reason="stop",
        input_tokens=1,
        cached_input_tokens=0,
        cache_write_tokens=0,
        output_tokens=1,
        thinking_tokens=0,
        provider_extras={},
        cost_usd_snapshot=0.0,
        duration_sec=0.1,
        streamed=False,
        time_to_first_token_sec=None,
        gateway_request_id=f"req-{seq}",
        cache_keys=[],
    )


def test_openhands_reasoning_from_tool_use_fields() -> None:
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="openhands-sdk", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        _llm_call_event(2),
        ToolUseEvent(
            **_ev(3),
            tool_name="terminal",
            args={"command": "ls"},
            result={"content": "a.txt"},
            duration_sec=0.1,
            reasoning_content="model-native CoT",
        ),
        StepEndEvent(**_ev(4), summary={}),
        TrialEndEvent(**_ev(5), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="openhands-sdk", agent_version="1.0")
    assert atif.steps[0].reasoning_content == "model-native CoT"


def test_openhands_reasoning_from_think_tool_and_agent_thought() -> None:
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="openhands-sdk", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        _llm_call_event(2),
        AgentThoughtEvent(
            **_ev(3),
            content="model-native CoT",
            reasoning_content="model-native CoT",
        ),
        ToolUseEvent(
            **_ev(4),
            tool_name="think",
            args={"thought": "explicit think reasoning"},
            result={"content": "ok"},
            duration_sec=0.1,
        ),
        AgentThoughtEvent(**_ev(5), content="status: running"),
        StepEndEvent(**_ev(6), summary={}),
        TrialEndEvent(**_ev(7), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="openhands-sdk", agent_version="1.0")
    assert atif.steps[0].reasoning_content == (
        "explicit think reasoning\n---\nmodel-native CoT"
    )


def test_openhands_reasoning_excludes_status_and_result_lines() -> None:
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="openhands-sdk", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        _llm_call_event(2),
        AgentThoughtEvent(**_ev(3), content="status: working"),
        AgentThoughtEvent(**_ev(4), content="result: ok"),
        StepEndEvent(**_ev(5), summary={}),
        TrialEndEvent(**_ev(6), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="openhands-sdk", agent_version="1.0")
    assert atif.steps[0].reasoning_content is None
