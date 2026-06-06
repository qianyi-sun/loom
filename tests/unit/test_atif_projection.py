from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

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


def _llm_call_event(seq: int, step_id: str, input_t: int, output_t: int, cost: float) -> LLMCallEvent:
    return LLMCallEvent(
        **_ev(seq, step_id=step_id),
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        rate_card_hash="abc",
        system_prompt="be helpful",
        messages=[ChatMessage(role="user", content="hi")],
        response=ChatMessage(role="assistant", content="hello"),
        finish_reason="stop",
        input_tokens=input_t,
        cached_input_tokens=0,
        cache_write_tokens=0,
        output_tokens=output_t,
        thinking_tokens=0,
        provider_extras={},
        cost_usd_snapshot=cost,
        duration_sec=0.5,
        streamed=False,
        time_to_first_token_sec=None,
        gateway_request_id=f"req-{seq}",
        cache_keys=[],
    )


def test_single_llm_call_per_step():
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="oracle", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        _llm_call_event(2, "main", input_t=10, output_t=5, cost=0.001),
        StepEndEvent(**_ev(3), summary={"reward": 1.0}),
        TrialEndEvent(**_ev(4), final_state="succeeded", reward={"passed": 1.0}),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="oracle", agent_version="1.0")
    assert len(atif.steps) == 1
    step = atif.steps[0]
    assert step.llm_call_count == 1
    assert step.messages is not None
    assert step.metrics is not None
    assert step.metrics.input_tokens == 10
    assert step.metrics.cost_usd == 0.001


def test_multi_llm_call_aggregation():
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="oracle", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        _llm_call_event(2, "main", input_t=10, output_t=5, cost=0.001),
        _llm_call_event(3, "main", input_t=20, output_t=8, cost=0.002),
        _llm_call_event(4, "main", input_t=15, output_t=4, cost=0.003),
        StepEndEvent(**_ev(5), summary={}),
        TrialEndEvent(**_ev(6), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="oracle", agent_version="1.0")
    step = atif.steps[0]
    assert step.llm_call_count == 3
    assert step.metrics is not None
    assert step.metrics.input_tokens == 45
    assert step.metrics.output_tokens == 17
    assert step.metrics.cost_usd == pytest.approx(0.006)


def test_zero_llm_calls_step():
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="oracle", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="run script"),
        StepEndEvent(**_ev(2), summary={}),
        TrialEndEvent(**_ev(3), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="oracle", agent_version="1.0")
    step = atif.steps[0]
    assert step.llm_call_count == 0
    assert step.messages is None
    assert step.metrics is None
    assert step.reasoning_content is None


def test_reasoning_content_concatenated_across_thoughts():
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="oracle", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        _llm_call_event(2, "main", input_t=1, output_t=1, cost=0.0),
        AgentThoughtEvent(**_ev(3), content="thought-A"),
        AgentThoughtEvent(**_ev(4), content="thought-B"),
        StepEndEvent(**_ev(5), summary={}),
        TrialEndEvent(**_ev(6), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="oracle", agent_version="1.0")
    assert atif.steps[0].reasoning_content == "thought-A\n---\nthought-B"


def test_tool_calls_collected():
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="oracle", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        ToolUseEvent(**_ev(2), tool_name="read", args={"p": "/a"}, result={"c": "x"}, duration_sec=0.1),
        ToolUseEvent(**_ev(3), tool_name="write", args={"p": "/b"}, result={}, duration_sec=0.1),
        StepEndEvent(**_ev(4), summary={}),
        TrialEndEvent(**_ev(5), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="oracle", agent_version="1.0")
    step = atif.steps[0]
    assert step.tool_calls is not None
    assert len(step.tool_calls) == 2
    assert step.tool_calls[0]["name"] == "read"


def test_idempotency():
    events_a = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="oracle", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="x"),
        _llm_call_event(2, "main", input_t=10, output_t=5, cost=0.001),
        StepEndEvent(**_ev(3), summary={}),
        TrialEndEvent(**_ev(4), final_state="succeeded"),
    ]
    events_b = list(events_a)
    a = project_to_atif(iter(events_a), task_id="t", agent_name="oracle", agent_version="1.0")
    b = project_to_atif(iter(events_b), task_id="t", agent_name="oracle", agent_version="1.0")
    assert a.metadata == b.metadata
    assert a.steps == b.steps


def test_multi_step():
    """Two step_starts → two AtifStep entries in event order."""
    events = [
        TrialStartEvent(**_ev(0), task_id="t", agent_name="oracle", agent_mode="out-of-box"),
        StepStartEvent(**_ev(1), instruction_excerpt="p1"),
        _llm_call_event(2, "main", input_t=1, output_t=1, cost=0.0),
        StepEndEvent(**_ev(3), summary={}),
        StepStartEvent(**_ev(4, step_id="phase-2"), instruction_excerpt="p2"),
        _llm_call_event(5, "phase-2", input_t=2, output_t=2, cost=0.0),
        StepEndEvent(**_ev(6, step_id="phase-2"), summary={}),
        TrialEndEvent(**_ev(7), final_state="succeeded"),
    ]
    atif = project_to_atif(iter(events), task_id="t", agent_name="oracle", agent_version="1.0")
    assert [s.step_id for s in atif.steps] == ["main", "phase-2"]
