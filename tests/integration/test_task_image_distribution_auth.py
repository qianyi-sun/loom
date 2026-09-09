"""Actual pinned Distribution authentication, isolated from any live registry."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import docker
import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from loom_task_image_authority.registry_token import (
    DistributionRegistryTokenIssuer,
    publication_repository,
)

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]

IMAGE = "registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"


@pytest.fixture(scope="module")
def token_registry(tmp_path_factory):
    client = docker.from_env()
    registry = None
    root = tmp_path_factory.mktemp("distribution-auth")
    data = root / "data"
    data.mkdir()
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    issuer = DistributionRegistryTokenIssuer(
        private_key=private_key,
        registry_origin="https://registry.example:5443",
        service="registry.example",
        issuer="loom-task-image-authority",
    )
    now = datetime.now(UTC)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "disposable-token-root")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(private_key, hashes.SHA256())
    )
    trust = root / "token-root.pem"
    trust.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    config = root / "config.yml"
    config.write_text(
        "version: 0.1\nlog:\n  level: error\nstorage:\n"
        "  filesystem:\n    rootdirectory: /var/lib/registry\n"
        "http:\n  addr: :5000\nauth:\n  token:\n"
        "    realm: https://authority.example/token\n"
        "    service: registry.example\n    issuer: loom-task-image-authority\n"
        "    rootcertbundle: /etc/registry-token-root.pem\n",
        encoding="utf-8",
    )
    try:
        # A pinned image may be fetched by Docker on a cold CI runner. Only
        # disposable test storage is mounted, never the daemon socket or keys.
        registry = client.containers.run(
            IMAGE,
            detach=True,
            user=f"{os.getuid()}:{os.getgid()}",
            volumes={
                str(data): {"bind": "/var/lib/registry", "mode": "rw"},
                str(config): {"bind": "/etc/docker/registry/config.yml", "mode": "ro"},
                str(trust): {"bind": "/etc/registry-token-root.pem", "mode": "ro"},
            },
            ports={"5000/tcp": ("127.0.0.1", None)},
            cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
            mem_limit="128m",
            nano_cpus=1_000_000_000,
            pids_limit=64,
            read_only=True,
        )
        registry.reload()
        port = registry.attrs["NetworkSettings"]["Ports"]["5000/tcp"][0]["HostPort"]
        # HTTP is loopback-only: this exercises the real token verifier, not
        # production TLS/routing, clock health, maintenance or native activation.
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=3) as http:
            deadline = time.monotonic() + 10
            while True:
                try:
                    response = http.get("/v2/")
                    if response.status_code == 401 and "Bearer" in response.headers.get("www-authenticate", ""):
                        break
                except httpx.HTTPError:
                    pass
                if time.monotonic() >= deadline:
                    pytest.fail("disposable token registry did not become ready")
                time.sleep(0.05)
            yield issuer, http, private_key
    finally:
        if registry is not None:
            registry.remove(force=True)
        client.close()


@pytest.mark.parametrize("action", ["push", "pull"])
@pytest.mark.parametrize("cpu_arch,component", [("arm64", "task"), ("x86_64", "sidecar:cache")])
def test_production_issuer_token_is_accepted_by_pinned_distribution(token_registry, action, cpu_arch, component):
    issuer, http, _private_key = token_registry
    repository = publication_repository(
        purpose="production", shadow_campaign_id=None,
        cpu_arch=cpu_arch, attempt_id=uuid4(), component=component,
    )
    now = datetime.now(UTC).replace(microsecond=0)
    issue = issuer.issue if action == "push" else issuer.issue_pull
    issued = issue(
        credential_id=uuid4(), repository=repository,
        issued_at=now, expires_at=now + timedelta(seconds=45),
    )
    headers = {"Authorization": f"Bearer {issued.token}"}
    if action == "push":
        response = http.post(f"/v2/{repository}/blobs/uploads/", headers=headers)
        expected = 202
    else:
        response = http.get(f"/v2/{repository}/manifests/sha256:{'a' * 64}", headers=headers)
        expected = 404  # Authorized, but this fresh repository has no manifest.
    # Never print the token, request, or headers when reporting auth failures.
    assert response.status_code == expected, response.text


@pytest.mark.parametrize("restriction", [
    "other_repository", "pull_cannot_push", "untrusted_key", "wrong_issuer",
    "wrong_audience", "expired", "future", "expiry_leeway",
])
def test_distribution_enforces_trust_scope_and_token_time(token_registry, restriction):
    issuer, http, private_key = token_registry
    repository = publication_repository(
        purpose="production", shadow_campaign_id=None,
        cpu_arch="arm64", attempt_id=uuid4(), component="task",
    )
    now = datetime.now(UTC).replace(microsecond=0)
    if restriction == "untrusted_key":
        issuer = DistributionRegistryTokenIssuer(
            private_key=rsa.generate_private_key(public_exponent=65537, key_size=3072),
            registry_origin=issuer.registry_origin, service=issuer.service, issuer=issuer.issuer,
        )
    issue = issuer.issue_pull if restriction == "pull_cannot_push" else issuer.issue
    if restriction in {"wrong_issuer", "wrong_audience"}:
        # Produce a genuinely signed token, modifying only its declared trust
        # identity. Never teach the fixture registry to trust caller headers.
        issuer = DistributionRegistryTokenIssuer(
            private_key=private_key,
            registry_origin=issuer.registry_origin,
            service="other-service" if restriction == "wrong_audience" else issuer.service,
            issuer="other-issuer" if restriction == "wrong_issuer" else issuer.issuer,
        )
        issue = issuer.issue
    offset = {"expired": -120, "future": 120, "expiry_leeway": -75}.get(restriction, 0)
    issued_at = now + timedelta(seconds=offset)
    issued = issue(
        credential_id=uuid4(), repository=repository,
        issued_at=issued_at, expires_at=issued_at + timedelta(seconds=45),
    )
    if restriction == "other_repository":
        repository = publication_repository(
            purpose="production", shadow_campaign_id=None,
            cpu_arch="arm64", attempt_id=uuid4(), component="task",
        )
    response = http.post(
        f"/v2/{repository}/blobs/uploads/",
        headers={"Authorization": f"Bearer {issued.token}"},
    )
    assert response.status_code == (202 if restriction == "expiry_leeway" else 401), response.text
