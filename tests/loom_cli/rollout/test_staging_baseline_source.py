from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.readonly_database_authority import (
    ReadonlyDatabaseEvidence,
    ReadonlySmokeAuthorityEvidence,
)
from loom_cli.rollout.staging_baseline_source import (
    BaselineHttpResponse,
    CrossVersionStagingBaselineProbeSource,
    ObjectStoreBaselineEvidence,
    StagingBaselineProbeSource,
    TlsRouteEvidence,
    read_staging_capacity,
    read_staging_mutation_epoch,
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
        capacity = StagingCapacity(1, 2, 80, 90)
        return {
            "status": "ready",
            "postgres": "ready",
            "object_store": "ready",
            "resource_digest": "a" * 64,
            "environment": "staging",
            "namespace": "loom-staging",
            "mutation_epoch": 9,
            "capacity": {
                "object_count": capacity.object_count,
                "bytes_used": capacity.bytes_used,
                "disk_free_percent": capacity.disk_free_percent,
                "inode_free_percent": capacity.inode_free_percent,
                "policy_sha256": staging_capacity_policy_digest(),
                "evidence_sha256": capacity.evidence_digest,
            },
            "capacity_ready": True,
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
                    "environment": "staging",
                    "namespace": "loom-staging",
                    "mutation_epoch": 9,
                    "capacity": None,
                    "capacity_ready": False,
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
    assert probes["staging.auth"]().blockers == {"principal": "readonly-authority-drift"}
    assert probes["staging.catalog-task"]().blockers == {
        "agents": "agents-catalog-unavailable",
        "models": "models-catalog-unavailable",
        "tasks": "tasks-catalog-unavailable",
    }
    assert probes["staging.storage-db"]().blockers == {
        "postgres": "postgres-readiness-failed",
        "object-store": "object-store-readiness-failed",
        "capacity": "dependency-capacity-unready",
    }
    assert probes["staging.network"]().blockers == {"route": "dns-tls-authentication-failed"}


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


def test_mutation_epoch_uses_same_readonly_endpoint(token_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def get(url: str, token: str) -> BaselineHttpResponse:
        calls.append((url, token))
        return BaselineHttpResponse(503, "HTTP/2", _body("ready"))

    assert (
        read_staging_mutation_epoch(
            route=ROUTE,
            token_path=token_path,
            service_uid=os.getuid(),
            http_get=get,
        )
        == 9
    )
    assert calls == [(f"{ROUTE}/api/v1/health/ready", "loom_readonly_exact")]


def test_capacity_uses_same_readonly_endpoint(token_path: Path) -> None:
    capacity = read_staging_capacity(
        route=ROUTE,
        token_path=token_path,
        service_uid=os.getuid(),
        http_get=lambda _url, _token: BaselineHttpResponse(200, "HTTP/2", _body("ready")),
    )
    assert capacity == StagingCapacity(1, 2, 80, 90)


def _database_evidence() -> ReadonlyDatabaseEvidence:
    return ReadonlyDatabaseEvidence(
        schema_revision="0065",
        mutation_epoch=0,
        epoch_authority="legacy-pre-0069",
        baseline_counts={
            "agents": 2,
            "provider_models": 3,
            "tasks": 4,
            "teams": 5,
            "users": 6,
        },
        capacity=None,
        evidence_sha256="d" * 64,
    )


def _smoke_authority_evidence(
    *,
    team_exists: bool = True,
    team_active: bool = True,
    team_submissions_enabled: bool = True,
    user_exists: bool = True,
    user_active: bool = True,
    membership_present: bool = True,
    mutation_epoch: int = 0,
) -> ReadonlySmokeAuthorityEvidence:
    return ReadonlySmokeAuthorityEvidence(
        mutation_epoch=mutation_epoch,
        team_exists=team_exists,
        team_active=team_active,
        team_submissions_enabled=team_submissions_enabled,
        user_exists=user_exists,
        user_active=user_active,
        membership_present=membership_present,
        evidence_sha256="f" * 64,
    )


def test_cross_version_baseline_uses_public_health_database_and_object_health() -> None:
    source = CrossVersionStagingBaselineProbeSource(
        route=ROUTE,
        database=_database_evidence(),
        smoke_authority=_smoke_authority_evidence(),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "e" * 64),
        public_http_get=lambda url: (
            BaselineHttpResponse(200, "HTTP/2", {"status": "ok"})
            if url == f"{ROUTE}/api/v1/health"
            else (_ for _ in ()).throw(AssertionError(url))
        ),
        tls_probe=lambda _route: TlsRouteEvidence("b" * 64, "c" * 64, 443),
    )

    results = {check_id: probe() for check_id, probe in source.probes().items()}

    assert all(result.ready for result in results.values())
    assert all(result.observed_mutation_epoch == 0 for result in results.values())
    assert results["staging.auth"].readonly_principal == "loom-rollout-readonly"


def test_cross_version_auth_blocks_missing_represented_user_membership() -> None:
    source = CrossVersionStagingBaselineProbeSource(
        route=ROUTE,
        database=_database_evidence(),
        smoke_authority=_smoke_authority_evidence(membership_present=False),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "e" * 64),
        public_http_get=lambda _url: BaselineHttpResponse(200, "HTTP/2", {"status": "ok"}),
        tls_probe=lambda _route: TlsRouteEvidence("b" * 64, "c" * 64, 443),
    )

    result = source.probes()["staging.auth"]()

    assert result.blockers == {"smoke-membership": "represented-user-team-membership-missing"}


@pytest.mark.parametrize(
    ("evidence", "expected"),
    (
        (
            _smoke_authority_evidence(
                team_exists=False,
                team_active=False,
                team_submissions_enabled=False,
                membership_present=False,
            ),
            {"smoke-team": "represented-team-not-found"},
        ),
        (
            _smoke_authority_evidence(team_active=False),
            {"smoke-team": "represented-team-disabled"},
        ),
        (
            _smoke_authority_evidence(team_submissions_enabled=False),
            {"smoke-submissions": "represented-team-submissions-paused"},
        ),
        (
            _smoke_authority_evidence(
                user_exists=False,
                user_active=False,
                membership_present=False,
            ),
            {"smoke-user": "represented-user-not-found"},
        ),
        (
            _smoke_authority_evidence(user_active=False),
            {"smoke-user": "represented-user-inactive"},
        ),
        (
            _smoke_authority_evidence(mutation_epoch=1),
            {"smoke-epoch": "represented-authority-epoch-drift"},
        ),
    ),
)
def test_cross_version_auth_localizes_each_smoke_authority_blocker(
    evidence: ReadonlySmokeAuthorityEvidence,
    expected: dict[str, str],
) -> None:
    source = CrossVersionStagingBaselineProbeSource(
        route=ROUTE,
        database=_database_evidence(),
        smoke_authority=evidence,
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "e" * 64),
    )

    result = source.probes()["staging.auth"]()

    assert result.blockers == expected


def test_cross_version_baseline_aggregates_public_and_object_blockers() -> None:
    source = CrossVersionStagingBaselineProbeSource(
        route=ROUTE,
        database=_database_evidence(),
        smoke_authority=_smoke_authority_evidence(),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(False, "e" * 64),
        public_http_get=lambda _url: BaselineHttpResponse(503, "HTTP/1.1", {"status": "not-ok"}),
        tls_probe=lambda _route: (_ for _ in ()).throw(OSError("tls unavailable")),
    )
    probes = source.probes()

    assert probes["staging.health"]().blockers == {"service": "health-not-ok"}
    assert probes["staging.storage-db"]().blockers == {
        "object-store": "object-store-readiness-failed"
    }
    assert probes["staging.network"]().blockers == {"route": "dns-tls-authentication-failed"}


def test_cross_version_baseline_localizes_missing_capacity_to_storage() -> None:
    database = ReadonlyDatabaseEvidence(
        schema_revision="0070",
        mutation_epoch=9,
        epoch_authority="staging-mutation-epoch-v1",
        baseline_counts={
            "agents": 2,
            "provider_models": 3,
            "tasks": 4,
            "teams": 5,
            "users": 6,
        },
        capacity=None,
        evidence_sha256="d" * 64,
    )
    source = CrossVersionStagingBaselineProbeSource(
        route=ROUTE,
        database=database,
        smoke_authority=_smoke_authority_evidence(mutation_epoch=9),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "e" * 64),
        public_http_get=lambda _url: BaselineHttpResponse(200, "HTTP/2", {"status": "ok"}),
        tls_probe=lambda _route: TlsRouteEvidence("b" * 64, "c" * 64, 443),
    )

    results = {check_id: probe() for check_id, probe in source.probes().items()}

    assert results["staging.storage-db"].blockers == {"capacity": "dependency-capacity-unready"}
    assert all(
        result.ready for check_id, result in results.items() if check_id != "staging.storage-db"
    )


def test_cross_version_baseline_tolerates_missing_capacity_below_capacity_migration() -> None:
    # A predecessor schema mid-migration (e.g. staging at 0069 before dev-tip's
    # 0070 capacity migration): the authority does not read capacity yet, so its
    # absence must NOT fail storage-db. Regression guard for the #857-renumbering
    # threshold drift (this check read 69 while the authority read 70).
    database = ReadonlyDatabaseEvidence(
        schema_revision="0069",
        mutation_epoch=9,
        epoch_authority="staging-mutation-epoch-v1",
        baseline_counts={
            "agents": 2,
            "provider_models": 3,
            "tasks": 4,
            "teams": 5,
            "users": 6,
        },
        capacity=None,
        evidence_sha256="d" * 64,
    )
    source = CrossVersionStagingBaselineProbeSource(
        route=ROUTE,
        database=database,
        smoke_authority=_smoke_authority_evidence(mutation_epoch=9),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "e" * 64),
        public_http_get=lambda _url: BaselineHttpResponse(200, "HTTP/2", {"status": "ok"}),
        tls_probe=lambda _route: TlsRouteEvidence("b" * 64, "c" * 64, 443),
    )

    results = {check_id: probe() for check_id, probe in source.probes().items()}

    assert results["staging.storage-db"].ready
    assert all(result.ready for result in results.values())


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
