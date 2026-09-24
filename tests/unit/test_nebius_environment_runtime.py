"""Explicit provider transport never falls back to ambient cluster credentials."""

from __future__ import annotations

from pathlib import Path

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


async def test_projected_token_rotation_and_origin_fencing(connection, tmp_path):
    from loom_service.environment_management.kubernetes_credentials import (
        ProjectedKubernetesConnection,
        ProjectedKubernetesCredentials,
    )
    from loom_service.environment_management.provider import ProviderBlockedError
    from loom_service.environment_management.runtime import NebiusManagementAuth

    first, second = tmp_path / "generation-one", tmp_path / "generation-two"
    first.write_text("projected-token-one")
    second.write_text("projected-token-two")
    first.chmod(0o440)
    second.chmod(0o440)
    token = tmp_path / "token"
    token.symlink_to(first)
    binding = ProjectedKubernetesConnection(kind="projected_service_account", endpoint=connection.endpoint,
                                            ca_file=connection.ca_file, token_file=token)
    credentials = ProjectedKubernetesCredentials(binding)
    observed = []

    def respond(request):
        observed.append((request.headers["Authorization"], request.headers.get("Cookie")))
        return httpx.Response(200)

    async with httpx.AsyncClient(base_url=binding.endpoint, auth=NebiusManagementAuth(binding, credentials),
                                 transport=httpx.MockTransport(respond), cookies={"private": "not-for-kubernetes"}) as http:
        await http.get("/api/v1/namespaces")
        token.unlink()
        token.symlink_to(second)
        await http.get("/api/v1/namespaces")
        with pytest.raises(ProviderBlockedError, match="origin_mismatch"):
            await http.get("https://foreign.example.test/api/v1/namespaces")
    await credentials.close()
    assert observed == [("Bearer projected-token-one", None), ("Bearer projected-token-two", None)]


@pytest.mark.parametrize("problem", ["missing", "world_readable", "group_write", "empty", "large", "newline", "directory"])
async def test_invalid_projected_token_fails_before_transmission(connection, tmp_path, problem):
    from loom_service.environment_management.kubernetes_credentials import (
        ProjectedKubernetesConnection,
        ProjectedKubernetesCredentials,
    )
    from loom_service.environment_management.provider import ProviderRetryError
    from loom_service.environment_management.runtime import NebiusManagementAuth

    token = tmp_path / "token"
    if problem == "directory":
        token.mkdir()
    elif problem != "missing":
        token.write_text("" if problem == "empty" else "a" * 16385 if problem == "large" else
                         "private\r\nInjected: secret" if problem == "newline" else "private-token")
        token.chmod(0o644 if problem == "world_readable" else 0o660 if problem == "group_write" else 0o440)
    binding = ProjectedKubernetesConnection(kind="projected_service_account", endpoint=connection.endpoint,
                                            ca_file=connection.ca_file, token_file=token)
    credentials = ProjectedKubernetesCredentials(binding)

    def cannot_send(request):
        pytest.fail("invalid projected token reached HTTP")

    async with httpx.AsyncClient(base_url=binding.endpoint, auth=NebiusManagementAuth(binding, credentials),
                                 transport=httpx.MockTransport(cannot_send)) as http:
        with pytest.raises(ProviderRetryError) as error:
            await http.get("/api/v1/namespaces")
    await credentials.close()
    assert str(error.value) == "kubernetes_credentials_unavailable"


async def test_rejected_projected_token_directory_does_not_leak_descriptors(connection, tmp_path):
    from loom_service.environment_management.kubernetes_credentials import (
        ProjectedKubernetesConnection,
        ProjectedKubernetesCredentials,
    )

    descriptors = Path("/proc/self/fd")
    if not descriptors.is_dir():
        pytest.skip("descriptor accounting requires Linux procfs")
    token = tmp_path / "token"
    token.mkdir()
    credentials = ProjectedKubernetesCredentials(ProjectedKubernetesConnection(
        kind="projected_service_account", endpoint=connection.endpoint,
        ca_file=connection.ca_file, token_file=token,
    ))
    before = len(list(descriptors.iterdir()))
    for _ in range(5):
        with pytest.raises(ValueError, match="projected Kubernetes credential unavailable"):
            await credentials.get_token()
    assert len(list(descriptors.iterdir())) == before


@pytest.mark.parametrize("change", ["http", "foreign_path", "both", "implicit"])
def test_projected_mode_requires_closed_explicit_credentials_shape(connection, tmp_path, change):
    from loom_service.environment_management.runtime import ProviderRuntimeSettings

    kube = {"kind": "projected_service_account", "endpoint": connection.endpoint,
            "ca_file": str(connection.ca_file), "token_file": str(tmp_path / "token")}
    if change == "http":
        kube["endpoint"] = "http://cluster.example.test"
    elif change == "foreign_path":
        kube["endpoint"] += "/foreign"
    elif change == "both":
        kube["credentials_file"] = str(connection.credentials_file)
    else:
        del kube["kind"]
    with pytest.raises(ValueError):
        ProviderRuntimeSettings(kubernetes=kube, cloud_credentials_file=connection.credentials_file)
