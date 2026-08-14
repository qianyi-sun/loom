from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest

import loom_capacity_executor.client as client_module
from loom_capacity_executor.client import (
    CapacityExecutorClient,
    ExecutableCapacityExecutorClient,
    ExecutorTransportError,
)
from loom_capacity_executor.dry_run import DryRunExecutorBinding
from loom_capacity_manager.executable_contracts import ExecutableExecutorRegistrationV2
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


class _GuardedOversizeStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.read_past_limit = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        yield b" " * client_module._MAX_RECEIPT_BYTES
        yield b"x"
        self.read_past_limit = True
        raise AssertionError("client read past the configured response byte bound")


class _StreamingOversizeTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.stream = _GuardedOversizeStream()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, stream=self.stream)


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


async def test_executor_client_aborts_streaming_receipt_once_byte_bound_is_crossed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed = False

    def unexpected_parse(cls: object, payload: bytes) -> object:
        nonlocal parsed
        parsed = True
        raise AssertionError(f"oversized receipt was parsed: {len(payload)}")

    monkeypatch.setattr(
        client_module.ExecutorCheckpointReceiptV1,
        "model_validate_json",
        classmethod(unexpected_parse),
    )
    transport = _StreamingOversizeTransport()
    http = httpx.AsyncClient(transport=transport)
    client = CapacityExecutorClient(
        _binding(),
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )

    with pytest.raises(ExecutorTransportError, match="receipt exceeds"):
        await client.checkpoint()

    assert parsed is False
    assert transport.stream.read_past_limit is False
    await http.aclose()


async def test_executable_work_fetch_aborts_streaming_body_once_byte_bound_is_crossed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed = False

    def unexpected_parse(payload: bytes) -> object:
        nonlocal parsed
        parsed = True
        raise AssertionError(f"oversized work was parsed: {len(payload)}")

    monkeypatch.setattr(client_module._EXECUTABLE_WORK, "validate_json", unexpected_parse)
    transport = _StreamingOversizeTransport()
    http = httpx.AsyncClient(transport=transport)
    registration = ExecutableExecutorRegistrationV2.model_construct(
        pool_id="oldlab",
        executable=True,
    )
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )

    with pytest.raises(ExecutorTransportError, match="work exceeds"):
        await client.next_executable_work(0)

    assert parsed is False
    assert transport.stream.read_past_limit is False
    await http.aclose()
