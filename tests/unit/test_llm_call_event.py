from datetime import UTC, datetime
from uuid import uuid4

from loom.models.trajectory import (
    ChatMessage,
    EventKind,
    LLMCallEvent,
)
from loom.models.types import ModelSpec


def _env(**overrides):
    base = {
        "emitted_at": datetime.now(UTC),
        "trial_id": uuid4(),
        "step_id": "main",
        "seq": 0,
    }
    base.update(overrides)
    return base


def test_llm_call_event_required_fields():
    e = LLMCallEvent(
        **_env(),
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        rate_card_hash="abc123",
        system_prompt="be helpful",
        messages=[ChatMessage(role="user", content="hi")],
        response=ChatMessage(role="assistant", content="hello"),
        finish_reason="stop",
        input_tokens=10,
        cached_input_tokens=0,
        cache_write_tokens=0,
        output_tokens=5,
        thinking_tokens=0,
        provider_extras={},
        cost_usd_snapshot=0.001,
        duration_sec=0.5,
        streamed=False,
        time_to_first_token_sec=None,
        gateway_request_id="req-1",
        cache_keys=[],
    )
    assert e.kind == EventKind.LLM_CALL
    assert e.input_tokens == 10


def test_llm_call_event_provider_extras_named_counters():
    """RFC R3 fix — provider_extras is dict[str, int], not opaque dict[str, Any]."""
    e = LLMCallEvent(
        **_env(),
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        rate_card_hash="abc",
        system_prompt=None,
        messages=[],
        response=ChatMessage(role="assistant", content=""),
        finish_reason="stop",
        input_tokens=0, cached_input_tokens=0, cache_write_tokens=0,
        output_tokens=0, thinking_tokens=0,
        provider_extras={"cache_creation_input_tokens": 1234},
        cost_usd_snapshot=0.0,
        duration_sec=0.1,
        streamed=False,
        time_to_first_token_sec=None,
        gateway_request_id="req",
        cache_keys=[],
    )
    assert e.provider_extras["cache_creation_input_tokens"] == 1234


def test_chat_message_with_tool_call():
    m = ChatMessage(
        role="assistant",
        content="",
        tool_calls=[{"id": "1", "type": "function", "function": {"name": "f", "arguments": "{}"}}],
    )
    assert m.role == "assistant"
    assert m.tool_calls is not None
