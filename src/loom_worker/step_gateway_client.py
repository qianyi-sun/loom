"""Step-scoped Gateway credentials for the direct-completion runtime."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

import httpx

from loom.agent.gateway_client import GatewayCallRequest, GatewayCallResponse
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.attempt_deadline import AttemptDeadline
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
    """Mint narrowly bound bearers for builtin model calls.

    The worker service token remains confined to worker/control-plane
    operations.  The wrapped HTTP transport is reused, but its configured
    bearer is deliberately ignored. Legacy callers mint per call;
    ``for_attempt`` binds one bearer and transport to an absolute deadline.
    """

    gateway: HttpLLMGatewayClient
    token_issuer: _StepTokenIssuer
    team_id: UUID
    trial_id: UUID
    token_ttl_sec: int = 600
    attempt_deadline: AttemptDeadline | None = None
    _scoped_gateway: HttpLLMGatewayClient | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _bound_step_id: str | None = field(default=None, init=False, repr=False)
    _bind_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )

    def for_attempt(
        self,
        deadline: AttemptDeadline,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> StepTokenGatewayClient:
        """Bind one fresh token and closable transport to a single attempt.

        The returned client does not inherit any credential cached by this
        instance. Calling this method for a retry therefore starts a new
        credential and deadline lifecycle.
        """

        return StepTokenGatewayClient(
            gateway=self.gateway.for_attempt(deadline, client=http_client),
            token_issuer=self.token_issuer,
            team_id=self.team_id,
            trial_id=self.trial_id,
            token_ttl_sec=self.token_ttl_sec,
            attempt_deadline=deadline,
        )

    async def aclose(self) -> None:
        """Interrupt and close this attempt's transport, when it owns one."""

        await self.gateway.aclose()

    async def _attempt_gateway(self, request: GatewayCallRequest) -> HttpLLMGatewayClient:
        if self.attempt_deadline is None:
            step_token = await self._mint(request.step_id)
            return self.gateway.with_token(step_token)

        self.attempt_deadline.require_remaining()
        async with self._bind_lock:
            self.attempt_deadline.require_remaining()
            if self._scoped_gateway is None:
                step_token = await self._mint(request.step_id)
                self.attempt_deadline.require_remaining()
                self._bound_step_id = request.step_id
                self._scoped_gateway = self.gateway.with_token(step_token)
            elif request.step_id != self._bound_step_id:
                raise AgentError(
                    "attempt-bound gateway request changed its step scope",
                )
            return self._scoped_gateway

    async def _mint(self, step_id: str) -> str:
        return await self.token_issuer.mint_step_token(
            team_id=self.team_id,
            trial_id=self.trial_id,
            step_id=step_id,
            ttl_sec=self.token_ttl_sec,
        )

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        if (
            request.team_id != str(self.team_id)
            or request.trial_id != str(self.trial_id)
            or not request.step_id
        ):
            raise AgentError(
                "direct-completion gateway request does not match its trial scope",
            )

        scoped_gateway = await self._attempt_gateway(request)
        return await scoped_gateway.call(request)
