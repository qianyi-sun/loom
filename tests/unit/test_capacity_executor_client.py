from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest

from loom_capacity_executor.client import (
    CapacityExecutorClient,
    ExecutorTransportError,
)
from loom_capacity_executor.dry_run import DryRunExecutorBinding
from loom_capacity_manager.grant_contracts import (
    DryRunExecutorHeartbeatV1,
    DryRunReservationAcceptanceV1,
)


def _binding() -> DryRunExecutorBinding:
    return DryRunExecutorBinding(
        authority_incarnation=UUID(int=10),
        writer_epoch=4,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=11),
        pool_id="oldlab",
        pool_generation=2,
    )


async def test_executor_client_fetches_exact_checkpoint_and_accepts_over_https() -> None:
    seen: list[httpx.Request] = []
    binding = _binding()
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=UUID(int=20),
        proposal_digest="a" * 64,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "executor_row_id": str(UUID(int=12)),
                    "authority_incarnation": str(binding.authority_incarnation),
                    "writer_epoch": binding.writer_epoch,
                    "executor_id": binding.executor_id,
                    "executor_incarnation": str(binding.executor_incarnation),
                    "pool_id": binding.pool_id,
                    "pool_generation": binding.pool_generation,
                    "command_sequence": 0,
                    "journal_sequence": 0,
                    "journal_digest": "0" * 64,
                    "inventory_sequence": 0,
                    "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
                    "executable": False,
                },
            )
        assert json.loads(request.content) == acceptance.model_dump(mode="json")
        return httpx.Response(
            200,
            json={
                "tranche_id": str(acceptance.tranche_id),
                "intent_ids": [str(UUID(int=21))],
                "replayed": False,
                "executable": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    checkpoint = await client.checkpoint()
    accepted = await client.accept_reservation(acceptance)
    await http.aclose()

    assert checkpoint.journal_digest == "0" * 64
    assert accepted.tranche_id == acceptance.tranche_id
    assert [request.url.path for request in seen] == [
        "/v1/executors/oldlab/checkpoint",
        f"/v1/executors/oldlab/reservations/{acceptance.tranche_id}/accept",
    ]
    assert all(request.headers["authorization"] == "Bearer executor-secret" for request in seen)
    assert seen[1].headers["content-type"] == "application/json"


async def test_executor_client_rejects_binding_drift_bad_receipts_and_redirects() -> None:
    binding = _binding()
    calls = 0

    async def bad_receipt(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"unexpected": True})

    http = httpx.AsyncClient(transport=httpx.MockTransport(bad_receipt))
    client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    wrong = DryRunExecutorHeartbeatV1(
        authority_incarnation=binding.authority_incarnation,
        writer_epoch=binding.writer_epoch,
        executor_id="gb10-executor",
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        heartbeat_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    with pytest.raises(ExecutorTransportError, match="binding"):
        await client.heartbeat(wrong)
    assert calls == 0

    with pytest.raises(ExecutorTransportError, match="receipt"):
        await client.checkpoint()
    await http.aclose()

    async def redirected(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(307, headers={"Location": "https://evil.invalid"})

    redirected_http = httpx.AsyncClient(transport=httpx.MockTransport(redirected))
    redirected_client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=redirected_http,
    )
    with pytest.raises(ExecutorTransportError, match="status 307"):
        await redirected_client.checkpoint()
    await redirected_http.aclose()


async def test_executor_client_rejects_unsafe_origins_and_tokens() -> None:
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200)))
    with pytest.raises(ValueError, match="HTTPS origin"):
        CapacityExecutorClient(
            _binding(),
            manager_origin="http://capacity.example.test",
            bearer_token="secret",
            http_client=http,
        )
    await http.aclose()
    with pytest.raises(ValueError, match="credential"):
        CapacityExecutorClient(
            _binding(),
            manager_origin="https://capacity.example.test",
            bearer_token="bad token",
            http_client=http,
        )
    with pytest.raises(ValueError, match="credential"):
        CapacityExecutorClient(
            _binding(),
            manager_origin="https://capacity.example.test",
            bearer_token="x" * 4097,
            http_client=http,
        )
