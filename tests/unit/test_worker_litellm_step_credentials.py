"""Builtin LiteLLM uses per-call step JWTs instead of the worker token."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest

from loom.agent.gateway_client import GatewayCallRequest
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.subprocess import SubprocessAgent
from loom.errors import AgentError
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom_worker.main_loop import _default_agent_factory
from loom_worker.step_gateway_client import StepTokenGatewayClient


class _RecordingTokenIssuer:
    def __init__(self) -> None:
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
        return "loom_step_scoped-test-jwt"


async def test_builtin_litellm_mints_bound_step_credential_per_call() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    issuer = _RecordingTokenIssuer()
    seen: dict[str, object] = {}

    async def _gateway(request: httpx.Request) -> httpx.Response:
        seen["authorization"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            request=request,
            json={
                "choices": [
                    {
                        "message": {"role": "assistant", "content": "done"},
                    },
                ],
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

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_gateway),
        base_url="http://gateway.test",
    ) as http:
        worker_gateway = HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="loom_worker_must-not-reach-gateway",
            _client=http,
        )
        factory = _default_agent_factory(
            team_id=team_id,
            trial_id=trial_id,
            cp_client=issuer,  # type: ignore[arg-type]
            worker_gateway_url="http://gateway.test",
        )
        agent = factory(
            task_dir=Path("/tmp"),
            gateway=worker_gateway,
            model=ModelSpec(provider="openai", name="gpt-4"),
            agent_name="litellm",
        )

        assert isinstance(agent, LiteLLMAgent)
        assert isinstance(agent.gateway, StepTokenGatewayClient)
        await agent.gateway.call(
            GatewayCallRequest(
                model=agent.model,
                messages=[ChatMessage(role="user", content="hi")],
                system_prompt=None,
                tools=None,
                tool_choice=None,
                team_id=str(team_id),
                trial_id=str(trial_id),
                step_id="solve",
            ),
        )

    assert issuer.calls == [
        {
            "team_id": team_id,
            "trial_id": trial_id,
            "step_id": "solve",
            "ttl_sec": 600,
        },
    ]
    assert seen["authorization"] == "Bearer loom_step_scoped-test-jwt"
    assert "loom_worker" not in str(seen)
    assert seen["body"] == {
        "model": "openai/gpt-4",
        "messages": [{"role": "user", "content": "hi"}],
        "loom": {
            "team_id": str(team_id),
            "trial_id": str(trial_id),
            "step_id": "solve",
        },
    }


def test_worker_factory_routes_direct_completion_name() -> None:
    factory = _default_agent_factory(
        team_id=uuid4(),
        trial_id=uuid4(),
        cp_client=_RecordingTokenIssuer(),
        worker_gateway_url="http://gateway.test",
    )

    agent = factory(
        task_dir=Path("/tmp"),
        gateway=HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="unused",
        ),
        model=ModelSpec(provider="openai", name="gpt-4"),
        agent_name="direct-completion",
    )

    assert isinstance(agent, LiteLLMAgent)


def test_subprocess_factory_applies_daytona_gateway_and_short_token_ttl() -> None:
    factory = _default_agent_factory(
        team_id=uuid4(),
        trial_id=uuid4(),
        cp_client=_RecordingTokenIssuer(),
        worker_gateway_url="http://worker-only.gateway.test",
        sandbox_gateway_url="https://gateway.example.com/openai/v1",
        step_token_ttl_sec=600,
    )

    agent = factory(
        Path("/tmp"),
        HttpLLMGatewayClient(
            base_url="http://worker-only.gateway.test",
            token="unused",
        ),
        ModelSpec(provider="openai", name="gpt-4"),
        "codex",
    )

    assert isinstance(agent, SubprocessAgent)
    assert agent.agent_gateway_url == "https://gateway.example.com/openai/v1"
    assert agent.step_token_ttl_sec == 600


async def test_builtin_litellm_rejects_identity_drift_before_minting() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    issuer = _RecordingTokenIssuer()
    gateway = StepTokenGatewayClient(
        gateway=HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="loom_worker_must-not-reach-gateway",
        ),
        token_issuer=issuer,
        team_id=team_id,
        trial_id=trial_id,
    )

    with pytest.raises(AgentError, match="does not match its trial scope"):
        await gateway.call(
            GatewayCallRequest(
                model=ModelSpec(provider="openai", name="gpt-4"),
                messages=[ChatMessage(role="user", content="hi")],
                system_prompt=None,
                tools=None,
                tool_choice=None,
                team_id=str(team_id),
                trial_id=str(uuid4()),
                step_id="solve",
            ),
        )

    assert issuer.calls == []
