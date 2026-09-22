"""Explicit provider transport never falls back to ambient cluster credentials."""

from __future__ import annotations

import httpx
import pytest

from tests.unit.test_nebius_kubernetes import connection as connection
from tests.unit.test_nebius_kubernetes import sdk as sdk


async def test_kubernetes_auth_renews_only_at_configured_origin(connection, sdk):
    from loom.nebius_kubernetes import NebiusKubernetesCredentials
    from loom_service.environment_management.provider import ProviderBlockedError
    from loom_service.environment_management.runtime import NebiusManagementAuth

    seen = []

    def respond(request):
        seen.append(request)
        return httpx.Response(200, json={})

    credentials = NebiusKubernetesCredentials(connection)
    try:
        async with httpx.AsyncClient(
            base_url=connection.endpoint, auth=NebiusManagementAuth(connection, credentials),
            transport=httpx.MockTransport(respond), cookies={"private": "not-for-kubernetes"},
        ) as client:
            await client.get("/api/v1/namespaces")
            await client.get("/api/v1/namespaces")
            with pytest.raises(ProviderBlockedError, match="kubernetes_origin_mismatch"):
                await client.get("https://other.example/api/v1/namespaces")
        assert [request.headers["Authorization"] for request in seen] == ["Bearer ephemeral-1", "Bearer ephemeral-2"]
        assert all("cookie" not in request.headers for request in seen)
    finally:
        await credentials.close()


async def test_kubernetes_token_failure_is_scrubbed_and_never_sends_request(connection, sdk):
    from loom.nebius_kubernetes import NebiusKubernetesCredentials
    from loom_service.environment_management.provider import ProviderRetryError
    from loom_service.environment_management.runtime import NebiusManagementAuth

    def must_not_send(request):
        pytest.fail("request was sent without a fresh usable token")

    sdk.failure = True
    credentials = NebiusKubernetesCredentials(connection)
    try:
        async with httpx.AsyncClient(
            base_url=connection.endpoint, auth=NebiusManagementAuth(connection, credentials),
            transport=httpx.MockTransport(must_not_send),
        ) as client:
            with pytest.raises(ProviderRetryError, match="kubernetes_credentials_unavailable") as caught:
                await client.get("/api/v1/namespaces")
        assert "renewal unavailable" not in str(caught.value)
    finally:
        await credentials.close()
