from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.operator.readonly_preflight_authority import (
    ReadonlyPreflightAuthority,
    derive_staging_baseline_probe_route,
    derive_staging_route,
)
from loom_cli.rollout.readonly_database_authority import (
    ReadonlyDatabaseEvidence,
    ReadonlyMutationEpochEvidence,
)
from loom_cli.rollout.staging_baseline_source import (
    BaselineHttpResponse,
    ObjectStoreBaselineEvidence,
    TlsRouteEvidence,
)
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config


@dataclass(frozen=True)
class _Result:
    returncode: int
    stdout: str


def _write_authority(tmp_path: Path) -> tuple[Path, Path]:
    cluster = tmp_path / "cluster.toml"
    cluster.write_text(
        "\n".join(
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


def _database() -> ReadonlyDatabaseEvidence:
    capacity = StagingCapacity(1, 2, 80, 90)
    return ReadonlyDatabaseEvidence(
        schema_revision="0070",
        mutation_epoch=11,
        epoch_authority="staging-mutation-epoch-v1",
        baseline_counts={
            "agents": 1,
            "provider_models": 2,
            "tasks": 3,
            "teams": 4,
            "users": 5,
        },
        capacity={
            "environment": "staging",
            "namespace": "loom-staging",
            "object_count": capacity.object_count,
            "bytes_used": capacity.bytes_used,
            "disk_free_percent": capacity.disk_free_percent,
            "inode_free_percent": capacity.inode_free_percent,
            "policy_sha256": staging_capacity_policy_digest(),
            "evidence_sha256": capacity.evidence_digest,
            "source": "exact-object-inventory-v1",
            "observed_at_epoch": 1_721_390_400,
        },
        evidence_sha256="a" * 64,
    )


def _mutation_epoch() -> ReadonlyMutationEpochEvidence:
    return ReadonlyMutationEpochEvidence(
        schema_revision="0070",
        mutation_epoch=11,
        epoch_authority="staging-mutation-epoch-v1",
        evidence_sha256="d" * 64,
    )


def test_authority_derives_route_and_reuses_exact_readonly_sources(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster, _token = _write_authority(tmp_path)
    object.__setattr__(config, "cluster_config_path", cluster)
    calls: list[tuple[tuple[str, ...], bytes]] = []
    payloads = iter(
        (
            {
                "status": {
                    "userInfo": {
                        "username": ("system:serviceaccount:loom-staging:loom-rollout-readonly")
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
                    "nonResourceRules": [{"nonResourceURLs": ["/api", "/apis"], "verbs": ["get"]}],
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
        database_evidence=_database,
        mutation_epoch_evidence=_mutation_epoch,
        capacity_source=lambda: StagingCapacity(1, 2, 80, 90),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "b" * 64),
        public_http_get=lambda _url: BaselineHttpResponse(200, "HTTP/2", {"status": "ok"}),
        tls_probe=lambda _route: TlsRouteEvidence("b" * 64, "c" * 64, 443),
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


def _write_transition_authority(tmp_path: Path, *, target: str, predecessor: str) -> Path:
    cluster = tmp_path / "cluster.toml"
    lines = [
        'namespace = "loom-staging"',
        'runtime_environment = "staging"',
        'ingress_host = "staging.example.test"',
        f'frontend_route_path = "{target}"',
        f'frontend_api_base_path = "{target}"',
    ]
    if predecessor:
        lines.append(f'frontend_route_path_from = "{predecessor}"')
    cluster.write_text("\n".join(lines) + "\n")
    cluster.chmod(0o600)
    return cluster


def test_baseline_probe_route_targets_predecessor_during_transition(tmp_path: Path) -> None:
    # dev tip renames the staging route /dev -> /staging; the live env still
    # serves /dev, so the baseline probes must hit the predecessor while the
    # canonical/target route stays /staging.
    config = _config(tmp_path)
    cluster = _write_transition_authority(tmp_path, target="/staging", predecessor="/dev")
    object.__setattr__(config, "cluster_config_path", cluster)

    assert derive_staging_route(config, service_uid=os.getuid()) == (
        "https://staging.example.test/staging"
    )
    assert derive_staging_baseline_probe_route(config, service_uid=os.getuid()) == (
        "https://staging.example.test/dev"
    )


def test_baseline_probe_route_equals_target_without_transition(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster = _write_transition_authority(tmp_path, target="/staging", predecessor="")
    object.__setattr__(config, "cluster_config_path", cluster)

    target = "https://staging.example.test/staging"
    assert derive_staging_route(config, service_uid=os.getuid()) == target
    assert derive_staging_baseline_probe_route(config, service_uid=os.getuid()) == target


def test_route_authority_rejects_noop_route_transition(tmp_path: Path) -> None:
    # A declared predecessor identical to the target is a no-op that must be
    # left empty, not accepted.
    config = _config(tmp_path)
    cluster = _write_transition_authority(tmp_path, target="/staging", predecessor="/staging")
    object.__setattr__(config, "cluster_config_path", cluster)

    with pytest.raises(ValueError, match="staging cluster route authority is invalid"):
        derive_staging_baseline_probe_route(config, service_uid=os.getuid())


def test_route_authority_rejects_malformed_predecessor(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster = _write_transition_authority(tmp_path, target="/staging", predecessor="dev")
    object.__setattr__(config, "cluster_config_path", cluster)

    with pytest.raises(ValueError, match="staging cluster route authority is invalid"):
        derive_staging_baseline_probe_route(config, service_uid=os.getuid())


def test_authority_exposes_no_database_credential_in_evidence(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster, _token = _write_authority(tmp_path)
    object.__setattr__(config, "cluster_config_path", cluster)
    authority = ReadonlyPreflightAuthority(
        config=config,
        service_uid=os.getuid(),
        kubernetes_run=lambda _argv, _stdin: _Result(1, ""),
        database_evidence=_database,
        mutation_epoch_evidence=_mutation_epoch,
        capacity_source=lambda: StagingCapacity(1, 2, 80, 90),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "b" * 64),
        kubeconfig_path=tmp_path / "readonly-kubeconfig",
    )
    assert authority.capacity() == StagingCapacity(1, 2, 80, 90)
    assert "password" not in repr(authority.capacity()).lower()


def test_authority_uses_fresh_live_capacity_before_schema_0070(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster, _token = _write_authority(tmp_path)
    object.__setattr__(config, "cluster_config_path", cluster)
    legacy = ReadonlyDatabaseEvidence(
        schema_revision="0065",
        mutation_epoch=0,
        epoch_authority="legacy-pre-0069",
        baseline_counts={
            "agents": 0,
            "provider_models": 0,
            "tasks": 0,
            "teams": 0,
            "users": 0,
        },
        capacity=None,
        evidence_sha256="c" * 64,
    )
    live = StagingCapacity(12, 34, 56, 78)
    calls: list[object] = []
    authority = ReadonlyPreflightAuthority(
        config=config,
        service_uid=os.getuid(),
        kubernetes_run=lambda _argv, _stdin: _Result(1, ""),
        database_evidence=lambda: legacy,
        mutation_epoch_evidence=lambda: ReadonlyMutationEpochEvidence(
            schema_revision="0065",
            mutation_epoch=0,
            epoch_authority="legacy-pre-0069",
            evidence_sha256="d" * 64,
        ),
        capacity_source=lambda: calls.append(object()) or live,
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "b" * 64),
        kubeconfig_path=tmp_path / "readonly-kubeconfig",
    )

    assert authority.capacity() == live
    assert len(calls) == 1


def test_baseline_factory_reports_only_storage_when_capacity_is_missing(tmp_path: Path) -> None:
    config = _config(tmp_path)
    cluster, _token = _write_authority(tmp_path)
    object.__setattr__(config, "cluster_config_path", cluster)
    database = ReadonlyDatabaseEvidence(
        schema_revision="0070",
        mutation_epoch=11,
        epoch_authority="staging-mutation-epoch-v1",
        baseline_counts={
            "agents": 1,
            "provider_models": 2,
            "tasks": 3,
            "teams": 4,
            "users": 5,
        },
        capacity=None,
        evidence_sha256="e" * 64,
    )

    authority = ReadonlyPreflightAuthority(
        config=config,
        service_uid=os.getuid(),
        kubernetes_run=lambda _argv, _stdin: _Result(1, ""),
        database_evidence=lambda: database,
        mutation_epoch_evidence=_mutation_epoch,
        capacity_source=lambda: StagingCapacity(1, 2, 80, 90),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "b" * 64),
        public_http_get=lambda _url: BaselineHttpResponse(200, "HTTP/2", {"status": "ok"}),
        tls_probe=lambda _route: TlsRouteEvidence("b" * 64, "c" * 64, 443),
        kubeconfig_path=tmp_path / "readonly-kubeconfig",
    )

    probes = authority.baseline_probes(11)

    assert set(probes) == {
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
    }
    results = {check_id: execute() for check_id, execute in probes.items()}

    assert results["staging.storage-db"].blockers == {"capacity": "dependency-capacity-unready"}
    assert all(
        result.ready for check_id, result in results.items() if check_id != "staging.storage-db"
    )


def test_capability_probe_does_not_consume_capacity_or_baseline_evidence(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    cluster, _token = _write_authority(tmp_path)
    object.__setattr__(config, "cluster_config_path", cluster)
    database_calls: list[object] = []
    payloads = iter(
        (
            {
                "status": {
                    "userInfo": {
                        "username": ("system:serviceaccount:loom-staging:loom-rollout-readonly")
                    }
                }
            },
            {
                "status": {
                    "incomplete": False,
                    "resourceRules": [
                        {
                            "apiGroups": [""],
                            "resources": ["pods"],
                            "verbs": ["get", "list", "watch"],
                        }
                    ],
                    "nonResourceRules": [],
                }
            },
        )
    )

    def missing_capacity() -> ReadonlyDatabaseEvidence:
        database_calls.append(object())
        raise ValueError("readonly database capacity evidence is incomplete")

    authority = ReadonlyPreflightAuthority(
        config=config,
        service_uid=os.getuid(),
        kubernetes_run=lambda _argv, _stdin: _Result(0, json.dumps(next(payloads))),
        database_evidence=missing_capacity,
        mutation_epoch_evidence=_mutation_epoch,
        capacity_source=lambda: StagingCapacity(1, 2, 80, 90),
        object_store_probe=lambda: ObjectStoreBaselineEvidence(True, "b" * 64),
        kubeconfig_path=tmp_path / "readonly-kubeconfig",
    )

    evidence = authority.capabilities()

    assert evidence.ready
    assert database_calls == []
