from __future__ import annotations

import json
from uuid import UUID

import httpx
import pytest

from loom_worker.control_plane_client import HttpControlPlaneClient


@pytest.mark.asyncio
async def test_protected_worker_credential_uses_dedicated_header_only() -> None:
    observed: dict[str, object] = {}

    def respond(request: httpx.Request) -> httpx.Response:
        observed["header"] = request.headers.get(
            "X-Loom-Executor-Worker-Credential"
        )
        observed["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            request=request,
            json={"worker_id": "00000000-0000-4000-8000-000000000001"},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        base_url="http://control-plane",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane",
            token="legacy-worker-token",
            executor_worker_credential="launcher-bound-credential",
            _client=http,
        )
        await client.register(
            hostname="worker-host",
            version="0.0.1",
            capabilities=[
                {
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                }
            ],
            pool_name="oldlab",
        )

    assert observed["header"] == "launcher-bound-credential"
    assert "executor_worker_credential" not in observed["payload"]  # type: ignore[operator]


@pytest.mark.asyncio
async def test_protected_worker_credential_authenticates_claim_and_heartbeat() -> None:
    worker_id = UUID("00000000-0000-4000-8000-000000000001")
    observed: list[tuple[str, str | None]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        observed.append(
            (
                request.url.path,
                request.headers.get("X-Loom-Executor-Worker-Credential"),
            )
        )
        if request.url.path == "/trials/claim":
            return httpx.Response(204, request=request)
        return httpx.Response(200, request=request, json={"status": "ok"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        base_url="http://control-plane",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane",
            token="legacy-worker-token",
            executor_worker_credential="launcher-bound-credential",
            _client=http,
        )
        assert await client.claim(worker_id=worker_id, caps=[]) is None
        await client.heartbeat(worker_id)

    assert observed == [
        ("/trials/claim", "launcher-bound-credential"),
        (
            "/workers/00000000-0000-4000-8000-000000000001/heartbeat",
            "launcher-bound-credential",
        ),
    ]


@pytest.mark.asyncio
async def test_registration_binds_credential_to_long_lived_client_headers() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            json={"worker_id": "00000000-0000-4000-8000-000000000001"},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
        base_url="http://control-plane",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane",
            token="legacy-worker-token",
            _client=http,
        )
        await client.register(
            hostname="worker-host",
            version="0.0.1",
            capabilities=[],
            executor_worker_credential="launcher-bound-credential",
        )

        assert client.request_headers == {
            "Authorization": "Bearer legacy-worker-token",
            "X-Loom-Executor-Worker-Credential": "launcher-bound-credential",
        }
