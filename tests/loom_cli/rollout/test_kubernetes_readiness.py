from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from loom_cli.rollout.kubernetes_readiness import probe_kubernetes_client


def test_kubernetes_client_probe_collects_context_and_namespace() -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(argv))
        stdout = "kind-loom-staging\n" if argv[-2:] == ["config", "current-context"] else ""
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    readiness = probe_kubernetes_client(
        run,
        kubeconfig=Path("/fixed/kubeconfig"),
        cluster_name="loom-staging",
        namespace="loom-staging",
    )

    assert readiness.ready
    assert readiness.current_context == "kind-loom-staging"
    assert readiness.namespace == "loom-staging"
    assert len(readiness.evidence_digest) == 64
    assert calls == [
        (
            "kubectl",
            "--kubeconfig",
            "/fixed/kubeconfig",
            "config",
            "current-context",
        ),
        (
            "kubectl",
            "--kubeconfig",
            "/fixed/kubeconfig",
            "get",
            "namespace",
            "loom-staging",
            "--request-timeout=10s",
        ),
    ]


def test_kubernetes_client_probe_reports_both_independent_failures_safely() -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(argv))
        if "current-context" in argv:
            return subprocess.CompletedProcess(argv, 0, "prod\nsecret=value\n", "")
        raise OSError("private namespace diagnostic")

    readiness = probe_kubernetes_client(
        run,
        kubeconfig=Path("/fixed/kubeconfig"),
        cluster_name="loom-staging",
        namespace="loom-staging",
    )

    assert readiness.ready is False
    assert readiness.current_context == "unavailable"
    assert readiness.context_ready is False
    assert readiness.namespace_ready is False
    assert len(calls) == 2
    assert "secret" not in repr(readiness)


@pytest.mark.parametrize(
    ("kubeconfig", "cluster", "namespace"),
    [
        (Path("relative"), "loom-staging", "loom-staging"),
        (Path("/fixed/../escape"), "loom-staging", "loom-staging"),
        (Path("/fixed/kubeconfig"), "INVALID_NAME", "loom-staging"),
        (Path("/fixed/kubeconfig"), "loom-staging", "../escape"),
    ],
)
def test_kubernetes_client_probe_rejects_unsafe_targets(
    kubeconfig: Path,
    cluster: str,
    namespace: str,
) -> None:
    with pytest.raises(ValueError, match="target is invalid"):
        probe_kubernetes_client(
            lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
            kubeconfig=kubeconfig,
            cluster_name=cluster,
            namespace=namespace,
        )
