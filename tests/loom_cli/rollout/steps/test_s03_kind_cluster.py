"""Kind cluster recovery step contract (#206)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s03_kind_cluster import (
    KindClusterStep,
    _write_sanitized_secret_restore_dir,
)


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
    assert result.artifacts["secret_count"] == "0"
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
        "deploy/k8s/ingress-nginx-kind.yaml",
    ] in calls
    assert not any(str(part).startswith("https://") for call in calls for part in call)
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
    secret_apply_calls = [
        call for call in calls if call[:4] == ["kubectl", "-n", "loom-staging", "apply"]
    ]
    assert secret_apply_calls
    secret_apply = secret_apply_calls[-1]
    assert "--server-side" in secret_apply
    assert "--force-conflicts" in secret_apply
    assert "--field-manager=loom-rollout-secret-restore" in secret_apply
    assert str(tmp_path / "backup" / "secrets") not in secret_apply
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
        if list(argv) == ["kubectl", "get", "pv", "loom-staging-worker-trajectories-data"]:
            Result.returncode = 1
        if list(argv) == [
            "kubectl",
            "-n",
            "loom-staging",
            "get",
            "pvc",
            "loom-worker-trajectories",
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
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(3, "kind-cluster")

    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert not any(call[:3] == ["kind", "create", "cluster"] for call in calls)
    assert ["kind", "export", "kubeconfig", "--name", "loom-staging"] in calls
    secret_apply_calls = [
        call for call in calls if call[:4] == ["kubectl", "-n", "loom-staging", "apply"]
    ]
    assert secret_apply_calls
    assert "--server-side" in secret_apply_calls[-1]
    assert str(tmp_path / "backup" / "secrets") not in secret_apply_calls[-1]


def test_bootstraps_worker_trajectories_static_storage_before_secret_restore(
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
    storage_root = tmp_path / "loom-staging"
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        rollout_root=storage_root,
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx.cluster_config_path.write_text(
        "\n".join(
            [
                'namespace = "loom-staging"',
                'persistent_storage_backend = "static-host-path"',
                f'persistent_storage_host_path_root = "{storage_root}"',
                "",
            ],
        ),
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(3, "kind-cluster")

    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts["worker_trajectories_storage"] == "applied"
    storage_manifest = step_dir.artifact_path("worker-trajectories-storage.yaml")
    apply_storage = [
        "kubectl",
        "-n",
        "loom-staging",
        "apply",
        "-f",
        str(storage_manifest),
    ]
    assert apply_storage in calls
    secret_apply = [
        call for call in calls if call[:4] == ["kubectl", "-n", "loom-staging", "apply"]
    ][-1]
    assert calls.index(apply_storage) < calls.index(secret_apply)

    docs = list(yaml.safe_load_all(storage_manifest.read_text(encoding="utf-8")))
    assert [(doc["kind"], doc["metadata"]["name"]) for doc in docs] == [
        ("PersistentVolume", "loom-staging-worker-trajectories-data"),
        ("PersistentVolumeClaim", "loom-worker-trajectories"),
    ]
    pv, pvc = docs
    assert pv["spec"]["persistentVolumeReclaimPolicy"] == "Retain"
    assert pv["spec"]["storageClassName"] == ""
    assert pv["spec"]["hostPath"] == {
        "path": str(storage_root / "trajectories"),
        "type": "DirectoryOrCreate",
    }
    assert pv["spec"]["claimRef"] == {
        "namespace": "loom-staging",
        "name": "loom-worker-trajectories",
    }
    assert pvc["metadata"]["namespace"] == "loom-staging"
    assert pvc["spec"]["storageClassName"] == ""
    assert pvc["spec"]["volumeName"] == "loom-staging-worker-trajectories-data"
    assert pvc["spec"]["resources"]["requests"]["storage"] == "100Gi"


def test_secret_restore_sanitizer_strips_runtime_metadata(tmp_path: Path) -> None:
    source = tmp_path / "source-secrets"
    target = tmp_path / "sanitized-secrets"
    source.mkdir()
    target.mkdir()
    (source / "loom-secrets.yaml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "loom-secrets",
                    "namespace": "loom-staging",
                    "creationTimestamp": "2026-07-06T21:06:35Z",
                    "resourceVersion": "535",
                    "uid": "bfd5efcc-a308-498f-8982-40125f591290",
                    "managedFields": [{"manager": "kubectl-client-side-apply"}],
                    "annotations": {
                        "kubectl.kubernetes.io/last-applied-configuration": (
                            '{"data":{"token":"should-not-stay-in-annotation"}}'
                        ),
                        "loom.example/keep": "yes",
                    },
                },
                "type": "Opaque",
                "data": {"token": "cmVkYWN0ZWQ="},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    count = _write_sanitized_secret_restore_dir(source, target)

    assert count == 1
    sanitized = yaml.safe_load((target / "loom-secrets.yaml").read_text(encoding="utf-8"))
    metadata = sanitized["metadata"]
    assert "creationTimestamp" not in metadata
    assert "resourceVersion" not in metadata
    assert "uid" not in metadata
    assert "managedFields" not in metadata
    assert metadata["annotations"] == {"loom.example/keep": "yes"}
    assert sanitized["data"] == {"token": "cmVkYWN0ZWQ="}


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
