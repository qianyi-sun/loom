from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from loom_cli.rollout.staging_baseline_source import (
    BaselineHttpResponse,
    StagingBaselineProbeSource,
    TlsRouteEvidence,
)

ROUTE = "https://yylx.world/dev"


def _body(name: str) -> dict[str, Any]:
    if name == "health":
        return {"status": "ok"}
    if name == "whoami":
        return {
            "auth_kind": "readonly_probe",
            "credential_type": "staging_readonly_probe",
            "principal_type": "readonly_probe",
            "scopes": ["read:own"],
            "allowed_http_methods": ["GET", "HEAD"],
            "readonly_authority_version": "v1",
        }
    if name == "ready":
        return {
            "status": "ready",
            "postgres": "ready",
            "object_store": "ready",
            "resource_digest": "a" * 64,
            "blockers": [],
        }
    if name == "tasks":
        return {"items": [], "total": 0, "next_cursor": None}
    return {"items": []}


@pytest.fixture
def token_path(tmp_path: Path) -> Path:
    path = tmp_path / "readonly-token"
    path.write_text("loom_readonly_exact\n")
    path.chmod(0o600)
    return path


def test_exact_readonly_source_probes_all_fixed_paths(token_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def get(url: str, token: str) -> BaselineHttpResponse:
        calls.append((url, token))
        name = next(name for name in _body_names() if _path(name) in url)
        return BaselineHttpResponse(200, "HTTP/2", _body(name))

    source = StagingBaselineProbeSource(
        route=ROUTE,
        token_path=token_path,
        service_uid=os.getuid(),
        mutation_epoch=9,
        http_get=get,
        tls_probe=lambda _route: TlsRouteEvidence("b" * 64, "c" * 64, 443),
    )

    results = {check_id: probe() for check_id, probe in source.probes().items()}

    assert all(result.ready for result in results.values())
    assert all(result.observed_mutation_epoch == 9 for result in results.values())
    assert {url for url, _token in calls} == {
        f"{ROUTE}/api/v1/health",
        f"{ROUTE}/api/v1/auth/whoami",
        f"{ROUTE}/api/v1/agents",
        f"{ROUTE}/api/v1/models",
        f"{ROUTE}/api/v1/tasks?limit=1",
        f"{ROUTE}/api/v1/health/ready",
    }
    assert {token for _url, token in calls} == {"loom_readonly_exact"}


def test_source_reports_all_catalog_and_dependency_blockers(token_path: Path) -> None:
    def get(url: str, _token: str) -> BaselineHttpResponse:
        if url.endswith("/health/ready"):
            return BaselineHttpResponse(
                503,
                "HTTP/1.1",
                {
                    "status": "not-ready",
                    "postgres": "not-ready",
                    "object_store": "not-ready",
                    "resource_digest": "d" * 64,
                    "blockers": ["redacted"],
                },
            )
        return BaselineHttpResponse(502, "HTTP/1.1", {"detail": "never exposed"})

    source = StagingBaselineProbeSource(
        route=ROUTE,
        token_path=token_path,
        service_uid=os.getuid(),
        mutation_epoch=9,
        http_get=get,
        tls_probe=lambda _route: (_ for _ in ()).throw(OSError("private IP")),
    )
    probes = source.probes()

    assert probes["staging.health"]().blockers == {"service": "health-not-ok"}
    assert probes["staging.auth"]().blockers == {
        "principal": "readonly-authority-drift"
    }
    assert probes["staging.catalog-task"]().blockers == {
        "agents": "agents-catalog-unavailable",
        "models": "models-catalog-unavailable",
        "tasks": "tasks-catalog-unavailable",
    }
    assert probes["staging.storage-db"]().blockers == {
        "postgres": "postgres-readiness-failed",
        "object-store": "object-store-readiness-failed",
    }
    assert probes["staging.network"]().blockers == {
        "route": "dns-tls-authentication-failed"
    }


def test_source_rejects_unsafe_route_or_token_metadata(token_path: Path) -> None:
    with pytest.raises(ValueError, match="route is invalid"):
        StagingBaselineProbeSource(
            route="http://yylx.world/dev",
            token_path=token_path,
            service_uid=os.getuid(),
            mutation_epoch=1,
        )
    token_path.chmod(0o644)
    source = StagingBaselineProbeSource(
        route=ROUTE,
        token_path=token_path,
        service_uid=os.getuid(),
        mutation_epoch=1,
        http_get=lambda _url, _token: BaselineHttpResponse(200, "HTTP/2", {}),
    )
    with pytest.raises(ValueError, match="metadata is unsafe"):
        source.probes()["staging.health"]()


def _body_names() -> tuple[str, ...]:
    return ("ready", "whoami", "agents", "models", "tasks", "health")


def _path(name: str) -> str:
    return {
        "health": "/api/v1/health",
        "ready": "/api/v1/health/ready",
        "whoami": "/api/v1/auth/whoami",
        "agents": "/api/v1/agents",
        "models": "/api/v1/models",
        "tasks": "/api/v1/tasks?limit=1",
    }[name]
