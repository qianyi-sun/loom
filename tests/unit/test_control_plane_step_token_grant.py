from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from loom_worker.control_plane_client import HttpControlPlaneClient, StepTokenGrant


async def test_attempt_step_token_sends_wall_clock_and_returns_grant() -> None:
    observed: dict[str, object] = {}
    deadline = datetime.now(UTC) + timedelta(seconds=60)

    def handler(request: httpx.Request) -> httpx.Response:
        observed["body"] = json.loads(request.content)
        return httpx.Response(
            201,
            request=request,
            json={
                "token": "loom_step_signed",
                "expires_at": (deadline + timedelta(seconds=300)).isoformat(),
                "attempt_deadline_wall_clock": deadline.isoformat(),
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://control-plane.test",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane.test",
            token="worker-token",
            _client=http,
        )
        grant = await client.mint_attempt_step_token(
            team_id=uuid4(),
            trial_id=uuid4(),
            step_id="main",
            ttl_sec=361,
            attempt_deadline_wall_clock=deadline,
        )

    assert isinstance(grant, StepTokenGrant)
    assert grant.token == "loom_step_signed"
    assert grant.expires_at == deadline + timedelta(seconds=300)
    assert grant.attempt_deadline_wall_clock == deadline
    assert observed["body"]["attempt_deadline_wall_clock"] == deadline.isoformat()  # type: ignore[index]


async def test_legacy_mint_step_token_keeps_raw_string_contract() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert b"attempt_deadline_wall_clock" not in request.content
        return httpx.Response(
            201,
            request=request,
            json={"token": "loom_step_legacy"},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://control-plane.test",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane.test",
            token="worker-token",
            _client=http,
        )
        token = await client.mint_step_token(
            team_id=uuid4(),
            trial_id=uuid4(),
            step_id="main",
            ttl_sec=60,
        )

    assert token == "loom_step_legacy"


async def test_attempt_above_token_ceiling_fails_before_http_dispatch() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500, request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://control-plane.test",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane.test",
            token="worker-token",
            _client=http,
        )
        with pytest.raises(ValueError, match="above 30000 seconds"):
            await client.mint_attempt_step_token(
                team_id=uuid4(),
                trial_id=uuid4(),
                step_id="main",
                ttl_sec=30_000,
                attempt_deadline_wall_clock=(
                    datetime.now(UTC) + timedelta(seconds=29_701)
                ),
            )

    assert calls == 0


async def test_monotonic_number_is_rejected_before_http_dispatch() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500, request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://control-plane.test",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane.test",
            token="worker-token",
            _client=http,
        )
        with pytest.raises(ValueError, match="wall-clock"):
            await client.mint_attempt_step_token(
                team_id=uuid4(),
                trial_id=uuid4(),
                step_id="main",
                ttl_sec=600,
                attempt_deadline_wall_clock=12345.0,  # type: ignore[arg-type]
            )

    assert calls == 0


def test_grant_rejects_server_expiry_without_deadline_grace() -> None:
    deadline = datetime.now(UTC)
    with pytest.raises(ValueError, match="does not cover"):
        StepTokenGrant.from_payload(
            {
                "token": "loom_step_signed",
                "expires_at": (deadline + timedelta(seconds=299)).isoformat(),
                "attempt_deadline_wall_clock": deadline.isoformat(),
            }
        )
