"""Authenticated, bounded publication for trusted demand reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest

from loom_capacity_agent.client import (
    DemandPublishError,
    DemandReporterClient,
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    read_owner_only_bytes,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardDemandObservationV1,
    ReporterConfigurationV1,
)
from loom_capacity_agent.reporter import build_demand_snapshot
from loom_capacity_manager.contracts import canonical_digest


def _configuration() -> ReporterConfigurationV1:
    registration = AgentRegistrationV1(
        environment_id="dev-alice",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        agent_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        candidate_digest="a" * 64,
        deployment_generation=7,
        configuration_generation=11,
    )
    return ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
            AgentPoolCapabilityV1(
                capability_id="gb10-arm-none",
                pool_id="gb10",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


def _snapshot(configuration: ReporterConfigurationV1):  # type: ignore[no-untyped-def]
    observation = GuardDemandObservationV1(
        **{
            field: getattr(configuration, field)
            for field in AgentRegistrationV1.model_fields
        },
        sequence=1,
        source_observed_at="2026-08-10T12:00:00Z",
        attempts=(),
    )
    return build_demand_snapshot(observation, configuration)


@pytest.mark.asyncio
async def test_publish_uses_exact_subject_endpoint_and_verifies_receipt() -> None:
    configuration = _configuration()
    snapshot = _snapshot(configuration)
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "snapshot_id": str(uuid4()),
                "digest": canonical_digest(snapshot),
                "sequence": snapshot.sequence,
                "replayed": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal:8443",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        receipt = await client.publish(snapshot)
    finally:
        await http.aclose()

    assert receipt.sequence == 1
    assert receipt.replayed is False
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "PUT"
    assert request.url == httpx.URL(
        f"https://capacity.internal:8443/v1/reports/demand/{configuration.subject_id}"
    )
    assert request.headers["Authorization"] == "Bearer reporter-secret"
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(request.content) == snapshot.model_dump(mode="json", exclude_none=False)


@pytest.mark.asyncio
async def test_publish_rejects_binding_mismatch_before_network() -> None:
    configuration = _configuration()
    snapshot = _snapshot(configuration).model_copy(update={"sequence": 2})
    stale = snapshot.model_copy(update={"subject_incarnation": uuid4()})
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="binding"):
            await client.publish(stale)
    finally:
        await http.aclose()
    assert calls == 0


@pytest.mark.asyncio
async def test_publish_errors_are_bounded_and_never_echo_credentials() -> None:
    configuration = _configuration()
    snapshot = _snapshot(configuration)
    secret = "never-echo-this-reporter-secret"

    async def denied(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text=f"upstream accidentally echoed {secret}")

    denied_http = httpx.AsyncClient(transport=httpx.MockTransport(denied))
    denied_client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token=secret,
        http_client=denied_http,
    )
    try:
        with pytest.raises(DemandPublishError) as caught:
            await denied_client.publish(snapshot)
    finally:
        await denied_http.aclose()
    assert "503" in str(caught.value)
    assert secret not in str(caught.value)
    assert "accidentally" not in str(caught.value)

    async def wrong_receipt(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "snapshot_id": str(uuid4()),
                "digest": "b" * 64,
                "sequence": snapshot.sequence,
                "replayed": True,
            },
        )

    wrong_http = httpx.AsyncClient(transport=httpx.MockTransport(wrong_receipt))
    wrong_client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token=secret,
        http_client=wrong_http,
    )
    try:
        with pytest.raises(DemandPublishError, match="receipt"):
            await wrong_client.publish(snapshot)
    finally:
        await wrong_http.aclose()


def _owner_file(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def test_owner_only_file_reader_rejects_unsafe_paths_and_bounds(tmp_path: Path) -> None:
    safe = _owner_file(tmp_path / "safe", b"value\n")
    assert read_owner_only_bytes(safe, max_bytes=16) == b"value\n"

    broad = _owner_file(tmp_path / "broad", b"value")
    broad.chmod(0o640)
    with pytest.raises(ValueError, match="0600"):
        read_owner_only_bytes(broad)

    target = _owner_file(tmp_path / "target", b"value")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="nonsymlink"):
        read_owner_only_bytes(link)

    oversized = _owner_file(tmp_path / "oversized", b"x" * 17)
    with pytest.raises(ValueError, match="maximum"):
        read_owner_only_bytes(oversized, max_bytes=16)


def test_tls_builder_uses_verified_open_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ca = _owner_file(tmp_path / "ca.pem", b"CA DATA")
    cert = _owner_file(tmp_path / "client.pem", b"CERT DATA")
    key = _owner_file(tmp_path / "client.key", b"KEY DATA")
    calls: dict[str, Any] = {}

    class FakeContext:
        minimum_version: Any = None
        check_hostname: bool = False
        verify_mode: Any = None

        def load_verify_locations(self, *, cadata: str) -> None:
            calls["ca"] = cadata

        def load_cert_chain(self, certfile: str, keyfile: str) -> None:
            calls["certfile"] = certfile
            calls["keyfile"] = keyfile
            assert Path(certfile).read_bytes() == b"CERT DATA"
            assert Path(keyfile).read_bytes() == b"KEY DATA"

    fake = FakeContext()
    monkeypatch.setattr("ssl.create_default_context", lambda: fake)
    result = build_reporter_tls_context(
        DemandReporterTLSFiles(ca_file=ca, certificate_file=cert, private_key_file=key)
    )
    assert result is fake
    assert calls["ca"] == "CA DATA"
    assert str(calls["certfile"]).startswith("/proc/self/fd/")
    assert str(calls["keyfile"]).startswith("/proc/self/fd/")


@pytest.mark.parametrize(
    "origin",
    (
        "http://capacity.internal",
        "https://user@capacity.internal",
        "https://capacity.internal/path",
        "https://capacity.internal?query=yes",
        "https://capacity.internal/#fragment",
    ),
)
def test_client_rejects_non_origin_or_non_https_manager_urls(origin: str) -> None:
    configuration = _configuration()
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200)))
    try:
        with pytest.raises(ValueError, match="manager origin"):
            DemandReporterClient(
                configuration,
                manager_origin=origin,
                bearer_token="reporter-secret",
                http_client=http,
            )
    finally:
        # This constructor-only test does not enter an event loop; closing the
        # unconnected MockTransport client is unnecessary.
        pass
