from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.operator.readonly_preflight_authority import (
    ReadonlyPreflightAuthority,
    derive_staging_route,
)
from loom_cli.rollout.staging_baseline_source import BaselineHttpResponse
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config


@dataclass(frozen=True)
class _Result:
    returncode: int
    stdout: str


def _write_authority(tmp_path: Path) -> tuple[Path, Path]:
    cluster = tmp_path / "cluster.toml"
    cluster.write_text(
        '\n'.join(
            (
                'namespace = "loom-staging"',
                'runtime_environment = "staging"',
                'ingress_host = "staging.example.test"',
                'frontend_route_path = "/dev"',
                'frontend_api_base_path = "/dev"',
            )
        )
        + "\n"
    )
    cluster.chmod(0o600)
    token = tmp_path / "readonly-token"
    token.write_text("loom_readonly_exact\n")
    token.chmod(0o600)
    return cluster, token


def _http(url: str, token: str) -> BaselineHttpResponse:
    assert token == "loom_readonly_exact"
    if url.endswith("/auth/whoami"):
        return BaselineHttpResponse(
            200,
            "HTTP/2",
            {
                "auth_kind": "readonly_probe",
                "credential_type": "staging_readonly_probe",
                "principal_type": "readonly_probe",
                "readonly_authority_version": "v1",
                "scopes": ["read:own"],
                "allowed_http_methods": ["GET", "HEAD"],
                "team_id": "00000000-0000-0000-0000-000000000001",
            },
        )
    capacity = StagingCapacity(1, 2, 80, 90)
    return BaselineHttpResponse(
        200,
        "HTTP/2",
        {
            "status": "ready",
            "postgres": "ready",
            "object_store": "ready",
            "resource_digest": "a" * 64,
            "environment": "staging",
            "namespace": "loom-staging",
            "mutation_epoch": 11,
            "capacity_ready": True,
            "capacity": {
                "object_count": capacity.object_count,
                "bytes_used": capacity.bytes_used,
                "disk_free_percent": capacity.disk_free_percent,
                "inode_free_percent": capacity.inode_free_percent,
                "policy_sha256": staging_capacity_policy_digest(),
                "evidence_sha256": capacity.evidence_digest,
            },
            "blockers": [],
        },
    )


def test_authority_derives_route_and_reuses_exact_readonly_sources(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster, token = _write_authority(tmp_path)
    object.__setattr__(config, "cluster_config_path", cluster)
    calls: list[tuple[tuple[str, ...], bytes]] = []
    payloads = iter(
        (
            {
                "status": {
                    "userInfo": {
                        "username": (
                            "system:serviceaccount:loom-staging:loom-rollout-readonly"
                        )
                    }
                }
            },
            {
                "status": {
                    "incomplete": False,
                    "resourceRules": [
                        {
                            "apiGroups": [""],
                            "resources": ["deployments", "pods", "services"],
                            "verbs": ["get", "list", "watch"],
                        },
                        {
                            "apiGroups": ["authorization.k8s.io"],
                            "resources": [
                                "selfsubjectaccessreviews",
                                "selfsubjectrulesreviews",
                            ],
                            "verbs": ["create"],
                        },
                        {
                            "apiGroups": ["authentication.k8s.io"],
                            "resources": ["selfsubjectreviews"],
                            "verbs": ["create"],
                        },
                    ],
                    "nonResourceRules": [
                        {"nonResourceURLs": ["/api", "/apis"], "verbs": ["get"]}
                    ],
                }
            },
        )
    )

    def kubernetes(argv, stdin):
        calls.append((tuple(argv), stdin))
        return _Result(0, json.dumps(next(payloads)))

    authority = ReadonlyPreflightAuthority(
        config=config,
        service_uid=os.getuid(),
        kubernetes_run=kubernetes,
        http_get=_http,
        token_path=token,
        kubeconfig_path=tmp_path / "readonly-kubeconfig",
    )

    assert derive_staging_route(config, service_uid=os.getuid()) == (
        "https://staging.example.test/dev"
    )
    assert authority.route == "https://staging.example.test/dev"
    assert authority.mutation_epoch() == 11
    assert authority.capacity() == StagingCapacity(1, 2, 80, 90)
    assert set(authority.baseline_probes(11)) == {
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
    }
    assert authority.capabilities().ready
    assert len(calls) == 2
    assert all(str(tmp_path / "readonly-kubeconfig") in argv for argv, _ in calls)


def test_route_authority_rejects_frontend_api_drift(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster, _token = _write_authority(tmp_path)
    cluster.write_text(cluster.read_text().replace('"/dev"\n', '"/wrong"\n', 1))
    object.__setattr__(config, "cluster_config_path", cluster)

    try:
        derive_staging_route(config, service_uid=os.getuid())
    except ValueError as exc:
        assert str(exc) == "staging cluster route authority is invalid"
    else:  # pragma: no cover - defensive
        raise AssertionError("route drift was accepted")


def test_authority_exposes_no_token_in_evidence(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster, token = _write_authority(tmp_path)
    object.__setattr__(config, "cluster_config_path", cluster)
    seen: list[str] = []

    def http(url: str, raw_token: str) -> BaselineHttpResponse:
        seen.append(raw_token)
        return _http(url, raw_token)

    authority = ReadonlyPreflightAuthority(
        config=config,
        service_uid=os.getuid(),
        kubernetes_run=lambda _argv, _stdin: _Result(1, ""),
        http_get=http,
        token_path=token,
        kubeconfig_path=tmp_path / "readonly-kubeconfig",
    )
    digest = hashlib.sha256(str(authority.capacity()).encode()).hexdigest()
    assert len(digest) == 64
    assert seen == ["loom_readonly_exact"]
    assert "loom_readonly_exact" not in digest
