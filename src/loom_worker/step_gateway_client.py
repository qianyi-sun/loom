"""Step-scoped Gateway credentials for the builtin LiteLLM agent."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from loom.agent.gateway_client import GatewayCallRequest, GatewayCallResponse
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.errors import AgentError


class _StepTokenIssuer(Protocol):
    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str: ...


@dataclass
class StepTokenGatewayClient:
    """Mint a narrowly bound bearer for every builtin model call.

    The worker service token remains confined to worker/control-plane
    operations.  The wrapped HTTP transport is reused, but its configured
    bearer is deliberately ignored.
    """

    gateway: HttpLLMGatewayClient
    token_issuer: _StepTokenIssuer
    team_id: UUID
    trial_id: UUID
    token_ttl_sec: int = 600

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        if (
            request.team_id != str(self.team_id)
            or request.trial_id != str(self.trial_id)
            or not request.step_id
        ):
            raise AgentError(
                "builtin LiteLLM gateway request does not match its trial scope",
            )

        step_token = await self.token_issuer.mint_step_token(
            team_id=self.team_id,
            trial_id=self.trial_id,
            step_id=request.step_id,
            ttl_sec=self.token_ttl_sec,
        )
        scoped_gateway = HttpLLMGatewayClient(
            base_url=self.gateway.base_url,
            token=step_token,
            timeout_sec=self.gateway.timeout_sec,
            _client=self.gateway._client,
        )
        return await scoped_gateway.call(request)
