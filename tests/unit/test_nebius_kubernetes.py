"""Remote clients preserve target/CA and delegate renewal to the native SDK."""

import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from loom.nebius_kubernetes import (
    NebiusKubernetesConnection,
    NebiusKubernetesCredentials,
    connection_from_fields,
    create_api_client,
)
from loom_execution_actuator.config import ExecutionActuatorSettings
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_capacity_collector.kubernetes import InClusterKubernetesCapacityReader


@pytest.fixture
def connection(tmp_path: Path) -> NebiusKubernetesConnection:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-cluster-ca")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(datetime.now(UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    ca = tmp_path / "ca.crt"
    ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}")
    credentials.chmod(0o440)
    return NebiusKubernetesConnection(
        endpoint="https://regional-api.example:443", ca_file=ca, credentials_file=credentials
    )


class FakeSDK:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = False
        self.expiration = datetime.now(UTC) + timedelta(hours=1)
        self.failure = False

    def get_token_sync(self, *, timeout: float) -> Any:
        assert timeout == 30
        self.calls += 1
        if self.failure:
            raise TimeoutError("renewal unavailable")
        return SimpleNamespace(token=f"ephemeral-{self.calls}", expiration=self.expiration)

    async def get_token(self, *, timeout: float) -> Any:
        return self.get_token_sync(timeout=timeout)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> FakeSDK:
    import nebius.sdk

    instance = FakeSDK()
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: instance)
    return instance


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "https://",
        "http://regional-api.example",
        "https://u:p@host",
        "https://host/path",
        "https://host?x=1",
        "https://host#fragment",
        "https://host:bad",
        "https://ho st",
        "https://host\\evil",
    ],
)
def test_remote_origin_rejects_ambiguous_or_insecure_target(endpoint: str) -> None:
    with pytest.raises(ValueError, match="HTTPS origin"):
        connection_from_fields(endpoint, Path("ca"), Path("credential"))


@pytest.mark.parametrize(
    "present",
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, False),
        (True, False, True),
        (False, True, True),
    ],
)
def test_partial_configuration_cannot_select_local_cluster(present: tuple[bool, ...]) -> None:
    with pytest.raises(ValueError, match="set together"):
        connection_from_fields(
            "https://host" if present[0] else None,
            Path("ca") if present[1] else None,
            Path("credential") if present[2] else None,
        )
    assert connection_from_fields(None, None, None) is None


@pytest.mark.asyncio
async def test_native_hook_refresh_and_async_gateway_interface(
    connection: NebiusKubernetesConnection, sdk: FakeSDK
) -> None:
    api, credentials = create_api_client(connection)
    try:
        cfg = api.configuration
        assert cfg.host == connection.endpoint
        assert cfg.ssl_ca_cert == str(connection.ca_file)
        assert cfg.verify_ssl is True
        assert cfg.get_api_key_with_prefix("authorization") == "Bearer ephemeral-1"
        assert cfg.get_api_key_with_prefix("authorization") == "Bearer ephemeral-2"
        assert await credentials.get_token() == "ephemeral-3"
        sdk.failure = True
        with pytest.raises(TimeoutError):
            cfg.get_api_key_with_prefix("authorization")
    finally:
        api.close()
        await credentials.close()
    assert sdk.closed


@pytest.mark.asyncio
async def test_expired_native_token_never_enters_request(
    connection: NebiusKubernetesConnection, sdk: FakeSDK
) -> None:
    credentials = NebiusKubernetesCredentials(connection)
    sdk.expiration = datetime.now(UTC) - timedelta(seconds=1)
    try:
        with pytest.raises(RuntimeError, match="expired"):
            credentials.get_token_sync()
        with pytest.raises(RuntimeError, match="expired"):
            await credentials.get_token()
    finally:
        await credentials.close()


@pytest.mark.parametrize(
    "reader_type", [InClusterKubernetesJobApi, InClusterKubernetesCapacityReader]
)
@pytest.mark.asyncio
async def test_remote_clients_share_explicit_target_without_local_loader(
    connection: NebiusKubernetesConnection,
    sdk: FakeSDK,
    reader_type: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kubernetes import config

    def forbidden() -> None:
        pytest.fail("remote mode must never load local credentials")

    monkeypatch.setattr(config, "load_incluster_config", forbidden)
    reader = reader_type(connection=connection)
    try:
        core = reader._core.api_client
        other = (
            reader._batch.api_client
            if isinstance(reader, InClusterKubernetesJobApi)
            else reader._apps.api_client
        )
        assert core is other
        assert core.configuration.host == connection.endpoint
        assert core.configuration.get_api_key_with_prefix("authorization") == "Bearer ephemeral-1"
    finally:
        await reader.close()
    assert sdk.closed


@pytest.mark.parametrize(
    "reader_type", [InClusterKubernetesJobApi, InClusterKubernetesCapacityReader]
)
def test_invalid_remote_ca_fails_without_local_fallback(
    connection: NebiusKubernetesConnection, reader_type: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kubernetes import config

    monkeypatch.setattr(config, "load_incluster_config", lambda: pytest.fail("local fallback"))
    connection.ca_file.write_text("not a certificate")
    with pytest.raises(ssl.SSLError, match=r"certificate|PEM"):
        reader_type(connection=connection)


def test_world_readable_credential_rejected(connection: NebiusKubernetesConnection) -> None:
    connection.credentials_file.chmod(0o444)
    with pytest.raises(ValueError, match="private bounded"):
        NebiusKubernetesCredentials(connection, sdk_factory=FakeSDK)


def test_actuator_environment_requires_complete_remote_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "LOOM_EXECUTION_ACTUATOR_KUBERNETES_ENDPOINT", "https://regional-api.example"
    )
    with pytest.raises(ValueError, match="set together"):
        ExecutionActuatorSettings(
            db_url="postgresql://unused",
            controller_id="test",
            target_id="regional",
            namespace="execution",
        )


@pytest.mark.asyncio
async def test_remote_request_uses_bound_origin_namespace_and_fresh_authorization(
    connection: NebiusKubernetesConnection, sdk: FakeSDK, monkeypatch: pytest.MonkeyPatch
) -> None:
    from urllib3 import HTTPResponse

    reader = InClusterKubernetesJobApi(connection=connection)
    calls = []

    def get(url: str, **kwargs: Any) -> HTTPResponse:
        calls.append((url, kwargs["headers"]["authorization"]))
        return HTTPResponse(
            body=b'{"apiVersion":"v1","kind":"PodList","items":[]}',
            status=200,
            headers={"Content-Type": "application/json"},
        )

    monkeypatch.setattr(reader._core.api_client.rest_client, "GET", get)
    try:
        reader._core.list_namespaced_pod(namespace="region-two-execution")
        reader._core.list_namespaced_pod(namespace="region-two-execution")
        assert calls == [
            (
                connection.endpoint + "/api/v1/namespaces/region-two-execution/pods",
                "Bearer ephemeral-1",
            ),
            (
                connection.endpoint + "/api/v1/namespaces/region-two-execution/pods",
                "Bearer ephemeral-2",
            ),
        ]
    finally:
        await reader.close()
