"""Kind cluster recovery step contract (#206)."""

from __future__ import annotations

import json
from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s03_kind_cluster import KindClusterStep


def _backup_manifest(tmp_path: Path) -> Path:
    secrets = tmp_path / "backup" / "secrets"
    secrets.mkdir(parents=True)
    manifest = tmp_path / "backup-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "components": {
                    "k8s_secrets": {
                        "kind": "directory",
                        "path": str(secrets),
                    },
                },
            },
        ),
        encoding="utf-8",
    )
    return manifest


def test_creates_missing_kind_cluster_and_restores_namespace_secrets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[list[str]] = []
    ingressclass_gets = 0

    def fake_run(argv, **kwargs):
        nonlocal ingressclass_gets
        calls.append(list(argv))

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        if list(argv) == ["kind", "get", "clusters"]:
            Result.stdout = "other-cluster\n"
        if list(argv) == ["kubectl", "get", "namespace", "loom-staging"]:
            Result.returncode = 1
        if list(argv) == ["kubectl", "get", "ingressclass", "nginx"]:
            ingressclass_gets += 1
            Result.returncode = 1 if ingressclass_gets == 1 else 0
        if list(argv) == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "deployment",
            "ingress-nginx-controller",
        ]:
            Result.returncode = 1
        return Result()

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s03_kind_cluster.run_captured",
        fake_run,
    )
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        rollout_root=tmp_path / "loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(3, "kind-cluster")

    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert [
        "kind",
        "create",
        "cluster",
        "--name",
        "loom-staging",
        "--config",
        str(step_dir.artifact_path("kind-cluster.yaml")),
    ] in calls
    assert ["kind", "export", "kubeconfig", "--name", "loom-staging"] in calls
    assert [
        "kubectl",
        "apply",
        "-f",
        "https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.10.1/deploy/static/provider/kind/deploy.yaml",
    ] in calls
    assert [
        "kubectl",
        "-n",
        "ingress-nginx",
        "wait",
        "--for=condition=Available",
        "deployment/ingress-nginx-controller",
        "--timeout=180s",
    ] in calls
    assert ["kubectl", "create", "namespace", "loom-staging"] in calls
    assert [
        "kubectl",
        "-n",
        "loom-staging",
        "apply",
        "-f",
        str(tmp_path / "backup" / "secrets"),
    ] in calls
    rendered = step_dir.artifact_path("kind-cluster.yaml").read_text(encoding="utf-8")
    assert "hostPort: 80" in rendered
    assert "hostPort: 443" in rendered
    assert f"hostPath: {tmp_path / 'loom-staging'}" in rendered
    assert f"containerPath: {tmp_path / 'loom-staging'}" in rendered


def test_existing_kind_cluster_skips_create_but_refreshes_and_restores(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        if list(argv) == ["kind", "get", "clusters"]:
            Result.stdout = "loom-staging\n"
        if list(argv) == ["kubectl", "get", "namespace", "loom-staging"]:
            Result.returncode = 0
        return Result()

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s03_kind_cluster.run_captured",
        fake_run,
    )
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(3, "kind-cluster")

    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert not any(call[:3] == ["kind", "create", "cluster"] for call in calls)
    assert ["kind", "export", "kubeconfig", "--name", "loom-staging"] in calls
    assert [
        "kubectl",
        "-n",
        "loom-staging",
        "apply",
        "-f",
        str(tmp_path / "backup" / "secrets"),
    ] in calls


def test_verify_fails_when_ingressclass_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_run(argv, **kwargs):
        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        if list(argv) == ["kind", "get", "clusters"]:
            Result.stdout = "loom-staging\n"
        if list(argv) == ["kubectl", "get", "ingressclass", "nginx"]:
            Result.returncode = 1
        return Result()

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s03_kind_cluster.run_captured",
        fake_run,
    )
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(3, "kind-cluster")

    assert KindClusterStep().verify(ctx, step_dir).name == "MISMATCH"
