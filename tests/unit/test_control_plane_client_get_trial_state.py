"""HttpControlPlaneClient.get_trial_state (#360)."""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from loom_worker.control_plane_client import HttpControlPlaneClient


class _FakeTransport(httpx.AsyncBaseTransport):
    def __init__(self, response_body: dict[str, str], status: int = 200) -> None:
        self.response_body = response_body
        self.status = status
        self.calls: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        return httpx.Response(self.status, json=self.response_body)


@pytest.fixture
def cp_client() -> HttpControlPlaneClient:
    return HttpControlPlaneClient(
        base_url="http://cp.local:8080",
        token="worker-token",
    )


async def test_get_trial_state_returns_string_from_body(
    cp_client: HttpControlPlaneClient,
) -> None:
    transport = _FakeTransport({"state": "cancelled", "id": str(uuid4())})
    cp_client._client = httpx.AsyncClient(  # type: ignore[assignment]
        base_url="http://cp.local:8080",
        transport=transport,
    )
    tid = uuid4()

    state = await cp_client.get_trial_state(tid)

    assert state == "cancelled"
    assert len(transport.calls) == 1
    assert transport.calls[0].url.path == f"/trials/{tid}"
    assert transport.calls[0].method == "GET"
    assert transport.calls[0].headers["authorization"] == "Bearer worker-token"

    await cp_client._client.aclose()  # type: ignore[union-attr]


async def test_get_trial_state_propagates_http_error(
    cp_client: HttpControlPlaneClient,
) -> None:
    transport = _FakeTransport({}, status=500)
    cp_client._client = httpx.AsyncClient(  # type: ignore[assignment]
        base_url="http://cp.local:8080",
        transport=transport,
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await cp_client.get_trial_state(uuid4())
    finally:
        await cp_client._client.aclose()  # type: ignore[union-attr]
