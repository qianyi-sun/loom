import pytest

from loom.agent.gateway_client import (
    FakeLLMGatewayClient,
    GatewayCallRequest,
    GatewayCallResponse,
)
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec


def _request(messages: list[ChatMessage]) -> GatewayCallRequest:
    return GatewayCallRequest(
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        messages=messages,
        system_prompt=None,
        tools=None,
        tool_choice=None,
        team_id="t",
        trial_id="r",
        step_id="main",
    )


def _stub_response(text: str = "x") -> GatewayCallResponse:
    return GatewayCallResponse(
        response=ChatMessage(role="assistant", content=text),
        input_tokens=0, cached_input_tokens=0, cache_write_tokens=0,
        output_tokens=0, thinking_tokens=0,
        provider_extras={}, cost_usd=0.0,
        finish_reason="stop", duration_sec=0.0, streamed=False,
        time_to_first_token_sec=None, rate_card_hash="abc",
        gateway_request_id="req",
    )


async def test_fake_returns_scripted_response():
    fake = FakeLLMGatewayClient(scripted=[_stub_response("hi")])
    r = await fake.call(_request([ChatMessage(role="user", content="hi")]))
    assert r.response.content == "hi"


async def test_fake_records_requests():
    fake = FakeLLMGatewayClient(scripted=[_stub_response()])
    await fake.call(_request([ChatMessage(role="user", content="ping")]))
    assert len(fake.calls_recorded) == 1
    assert fake.calls_recorded[0].messages[0].content == "ping"


async def test_fake_raises_when_scripted_exhausted():
    fake = FakeLLMGatewayClient(scripted=[])
    with pytest.raises(IndexError):
        await fake.call(_request([]))
