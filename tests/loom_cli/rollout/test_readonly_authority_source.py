from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from loom_cli.rollout.readonly_authority_source import (
    JsonCommandRunner,
    probe_readonly_authority,
    probe_readonly_object_store_health,
)


@dataclass(frozen=True)
class _Result:
    returncode: int
    stdout: str


def _subject() -> dict[str, object]:
    return {
        "status": {
            "userInfo": {
                "username": "system:serviceaccount:loom-staging:loom-rollout-readonly",
            },
        },
    }


def _rules(*, extra: dict[str, object] | None = None) -> dict[str, object]:
    resource_rules: list[dict[str, object]] = [
        {
            "apiGroups": [""],
            "resources": ["deployments", "pods", "pods/log", "services"],
            "verbs": ["get", "list", "watch"],
        },
        {
            "apiGroups": ["authorization.k8s.io"],
            "resources": ["selfsubjectaccessreviews", "selfsubjectrulesreviews"],
            "verbs": ["create"],
        },
        {
            "apiGroups": ["authentication.k8s.io"],
            "resources": ["selfsubjectreviews"],
            "verbs": ["create"],
        },
        {
            "apiGroups": [""],
            "resources": ["pods/portforward"],
            "resourceNames": [
                "loom-minio-0",
                "loom-postgres-1",
                "loom-postgres-2",
                "loom-postgres-3",
            ],
            "verbs": ["create"],
        },
    ]
    if extra is not None:
        resource_rules.append(extra)
    return {
        "status": {
            "incomplete": False,
            "resourceRules": resource_rules,
            "nonResourceRules": [
                {
                    "nonResourceURLs": [
                        "/.well-known/openid-configuration",
                        "/api",
                        "/apis",
                        "/healthz",
                    ],
                    "verbs": ["get"],
                },
            ],
        },
    }


def _application(**overrides: object) -> bytes:
    body: dict[str, object] = {
        "auth_kind": "readonly_probe",
        "credential_type": "staging_readonly_probe",
        "principal_type": "readonly_probe",
        "readonly_authority_version": "v1",
        "scopes": ["read:own"],
        "allowed_http_methods": ["GET", "HEAD"],
        "team_id": str(UUID(int=1)),
    }
    body.update(overrides)
    return json.dumps(body).encode()


def _run_for(rules: dict[str, object]):
    calls: list[tuple[tuple[str, ...], bytes]] = []
    payloads = iter((_subject(), rules))

    def run(argv: tuple[str, ...], stdin: bytes) -> _Result:
        calls.append((tuple(argv), stdin))
        return _Result(0, json.dumps(next(payloads)))

    return run, calls


def test_server_observed_readonly_authority_accepts_exact_safe_rules() -> None:
    run, calls = _run_for(_rules())

    evidence = probe_readonly_authority(
        cast(JsonCommandRunner, run),
        kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
        namespace="loom-staging",
        application_observation=_application,
    )

    assert evidence.ready
    assert evidence.kubernetes_verbs == ("get", "list", "watch")
    assert "selfsubjectrulesreviews" not in evidence.kubernetes_resources
    assert len(calls) == 2
    assert all("--raw" in argv and argv[-2:] == ("-f", "-") for argv, _stdin in calls)


def test_server_observed_authority_accepts_select_only_database_digest() -> None:
    run, _calls = _run_for(_rules())

    evidence = probe_readonly_authority(
        cast(JsonCommandRunner, run),
        kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
        namespace="loom-staging",
        database_authority_digest="a" * 64,
    )

    assert evidence.ready
    assert evidence.http_methods == ()


def test_server_observed_authority_rejects_ambiguous_data_authority() -> None:
    run, _calls = _run_for(_rules())

    with pytest.raises(ValueError, match="authority is ambiguous"):
        probe_readonly_authority(
            cast(JsonCommandRunner, run),
            kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
            namespace="loom-staging",
            application_observation=_application,
            database_authority_digest="a" * 64,
        )


def test_object_store_probe_uses_only_exact_health_proxy() -> None:
    calls: list[tuple[tuple[str, ...], bytes]] = []

    def run(argv: tuple[str, ...], stdin: bytes) -> _Result:
        calls.append((tuple(argv), stdin))
        return _Result(0, "")

    evidence = probe_readonly_object_store_health(
        cast(JsonCommandRunner, run),
        kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
        namespace="loom-staging",
    )

    assert evidence.ready
    assert calls == [
        (
            (
                "kubectl",
                "--kubeconfig",
                "/var/lib/loom-staging-rollout/readonly-kubeconfig",
                "get",
                "--raw",
                (
                    "/api/v1/namespaces/loom-staging/services/"
                    "http:loom-minio:9000/proxy/minio/health/ready"
                ),
                "--request-timeout=10s",
            ),
            b"",
        )
    ]


@pytest.mark.parametrize(
    "extra",
    (
        {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]},
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["create"]},
        {
            "apiGroups": [""],
            "resources": ["pods/portforward"],
            "resourceNames": ["loom-postgres-1"],
            "verbs": ["create"],
        },
        {
            "apiGroups": [""],
            "resources": ["pods/portforward"],
            "resourceNames": ["loom-minio-0", "loom-postgres-0"],
            "verbs": ["get", "create"],
        },
        {"apiGroups": ["*"], "resources": ["*"], "verbs": ["get"]},
    ),
)
def test_server_observed_readonly_authority_reports_protected_capabilities(
    extra: dict[str, object],
) -> None:
    run, _calls = _run_for(_rules(extra=extra))

    evidence = probe_readonly_authority(
        cast(JsonCommandRunner, run),
        kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
        namespace="loom-staging",
        application_observation=_application,
    )

    assert not evidence.ready


def test_server_observed_readonly_authority_rejects_incomplete_review() -> None:
    rules = _rules()
    cast(dict[str, object], rules["status"])["incomplete"] = True
    run, _calls = _run_for(rules)

    with pytest.raises(ValueError, match="incomplete"):
        probe_readonly_authority(
            cast(JsonCommandRunner, run),
            kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
            namespace="loom-staging",
            application_observation=_application,
        )


@pytest.mark.parametrize(
    "url",
    (
        "/.well-known",
        "/.well-known/../secrets",
        "/.well-known//openid-configuration",
        "/.hidden/authority",
        "/../api",
    ),
)
def test_server_observed_readonly_authority_rejects_unsafe_non_resource_url(
    url: str,
) -> None:
    rules = _rules()
    status = cast(dict[str, object], rules["status"])
    status["nonResourceRules"] = [{"nonResourceURLs": [url], "verbs": ["get"]}]
    run, _calls = _run_for(rules)

    with pytest.raises(ValueError, match="non-resource"):
        probe_readonly_authority(
            cast(JsonCommandRunner, run),
            kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
            namespace="loom-staging",
            application_observation=_application,
        )


@pytest.mark.parametrize(
    "overrides",
    (
        {"principal_type": "team"},
        {"scopes": ["read:own", "submit"]},
        {"allowed_http_methods": ["GET", "POST"]},
    ),
)
def test_server_observed_readonly_authority_rejects_application_drift(
    overrides: dict[str, object],
) -> None:
    run, _calls = _run_for(_rules())

    with pytest.raises(ValueError, match="application"):
        probe_readonly_authority(
            cast(JsonCommandRunner, run),
            kubeconfig=Path("/var/lib/loom-staging-rollout/readonly-kubeconfig"),
            namespace="loom-staging",
            application_observation=lambda: _application(**overrides),
        )
