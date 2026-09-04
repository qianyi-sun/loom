from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest

from loom.agent.gateway_client import GatewayCallRequest
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom_worker.step_gateway_client import StepTokenGatewayClient


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _TokenIssuer:
    def __init__(self, clock: _Clock | None = None) -> None:
        self.clock = clock
        self.calls: list[dict[str, object]] = []

    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str:
        self.calls.append(
            {
                "team_id": team_id,
                "trial_id": trial_id,
                "step_id": step_id,
                "ttl_sec": ttl_sec,
            },
        )
        if self.clock is not None:
            self.clock.now = 200.0
        return f"attempt-token-{len(self.calls)}"


def _request(team_id: UUID, trial_id: UUID, *, step_id: str = "solve") -> GatewayCallRequest:
    return GatewayCallRequest(
        model=ModelSpec(provider="openai", name="gpt-4"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None,
        tools=None,
        tool_choice=None,
        team_id=str(team_id),
        trial_id=str(trial_id),
        step_id=step_id,
    )


def _response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        request=request,
        json={
            "choices": [{"message": {"role": "assistant", "content": "done"}}],
            "loom": {
                "input_tokens": 1,
                "cached_input_tokens": 0,
                "cache_write_tokens": 0,
                "output_tokens": 1,
                "thinking_tokens": 0,
                "provider_extras": {},
                "cost_usd": 0.0,
                "finish_reason": "stop",
                "duration_sec": 0.01,
                "streamed": False,
                "time_to_first_token_sec": None,
                "rate_card_hash": "test",
                "gateway_request_id": "request-1",
            },
        },
    )


def _legacy_client(
    *,
    issuer: _TokenIssuer,
    team_id: UUID,
    trial_id: UUID,
) -> StepTokenGatewayClient:
    return StepTokenGatewayClient(
        gateway=HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="worker-token",
        ),
        token_issuer=issuer,
        team_id=team_id,
        trial_id=trial_id,
    )


async def test_attempt_binding_reuses_one_token_and_transport() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    issuer = _TokenIssuer()
    authorizations: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers.get("authorization"))
        return _response(request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        attempt = _legacy_client(
            issuer=issuer,
            team_id=team_id,
            trial_id=trial_id,
        ).for_attempt(AttemptDeadline.after(60.0), http_client=http)
        await attempt.call(_request(team_id, trial_id))
        await attempt.call(_request(team_id, trial_id))

    assert len(issuer.calls) == 1
    assert authorizations == ["Bearer attempt-token-1", "Bearer attempt-token-1"]


async def test_legacy_unbound_client_keeps_per_call_token_behavior() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    issuer = _TokenIssuer()
    authorizations: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers.get("authorization"))
        return _response(request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        legacy = StepTokenGatewayClient(
            gateway=HttpLLMGatewayClient(
                base_url="http://gateway.test",
                token="worker-token",
                _client=http,
            ),
            token_issuer=issuer,
            team_id=team_id,
            trial_id=trial_id,
        )
        await legacy.call(_request(team_id, trial_id))
        await legacy.call(_request(team_id, trial_id))

    assert len(issuer.calls) == 2
    assert authorizations == ["Bearer attempt-token-1", "Bearer attempt-token-2"]


async def test_expired_attempt_does_not_mint_token_or_dispatch_request() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    issuer = _TokenIssuer()
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _response(request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        attempt = _legacy_client(
            issuer=issuer,
            team_id=team_id,
            trial_id=trial_id,
        ).for_attempt(AttemptDeadline.after(0.0), http_client=http)
        with pytest.raises(AttemptDeadlineExceededError):
            await attempt.call(_request(team_id, trial_id))

    assert issuer.calls == []
    assert calls == 0


async def test_deadline_crossed_during_mint_does_not_dispatch_request() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    clock = _Clock(100.0)
    issuer = _TokenIssuer(clock)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _response(request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        attempt = _legacy_client(
            issuer=issuer,
            team_id=team_id,
            trial_id=trial_id,
        ).for_attempt(AttemptDeadline.after(50.0, clock=clock), http_client=http)
        with pytest.raises(AttemptDeadlineExceededError):
            await attempt.call(_request(team_id, trial_id))

    assert len(issuer.calls) == 1
    assert calls == 0


async def test_retry_binding_gets_a_new_deadline_and_token() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    issuer = _TokenIssuer()
    authorizations: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers.get("authorization"))
        return _response(request)

    root = _legacy_client(issuer=issuer, team_id=team_id, trial_id=trial_id)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        first = root.for_attempt(AttemptDeadline.after(60.0), http_client=http)
        retry = root.for_attempt(AttemptDeadline.after(60.0), http_client=http)
        await first.call(_request(team_id, trial_id))
        await retry.call(_request(team_id, trial_id))

    assert len(issuer.calls) == 2
    assert authorizations == ["Bearer attempt-token-1", "Bearer attempt-token-2"]


async def test_attempt_owned_transport_can_be_closed_independently() -> None:
    attempt = _legacy_client(
        issuer=_TokenIssuer(),
        team_id=uuid4(),
        trial_id=uuid4(),
    ).for_attempt(AttemptDeadline.after(60.0))

    assert attempt.gateway._client is not None
    assert not attempt.gateway._client.is_closed

    await attempt.aclose()

    assert attempt.gateway._client.is_closed
