"""Kind cluster recovery step contract (#206)."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.driver import run_rollout
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
from loom_cli.rollout.state import RolloutState, StepState
from loom_cli.rollout.steps import s03_kind_cluster
from loom_cli.rollout.steps.base import RunResult, VerifyOutcome, step_result_dict
from loom_cli.rollout.steps.candidate_source import candidate_worktree
from loom_cli.rollout.steps.s03_kind_cluster import (
    KindClusterStep,
    _write_sanitized_secret_restore_dir,
)

_CONTROLLER_CONFIG_GET = [
    "kubectl",
    "-n",
    "ingress-nginx",
    "get",
    "configmap",
    "ingress-nginx-controller",
    "-o",
    "json",
]
_AMBIENT_INGRESS_MANIFEST = Path("deploy/k8s/ingress-nginx-kind.yaml").resolve()


def _expected_controller_config_data(
    manifest: Path = _AMBIENT_INGRESS_MANIFEST,
) -> dict[str, str]:
    documents = [
        document
        for document in yaml.safe_load_all(
            manifest.read_text(encoding="utf-8"),
        )
        if document is not None
    ]
    config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document["metadata"]["name"] == "ingress-nginx-controller"
        and document["metadata"]["namespace"] == "ingress-nginx"
    )
    return dict(config["data"])


def _controller_config_json(data: object | None = None) -> str:
    return json.dumps(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "ingress-nginx-controller",
                "namespace": "ingress-nginx",
            },
            "data": _expected_controller_config_data() if data is None else data,
        },
    )


def _prepare_candidate_step(
    ctx: RolloutContext,
    *,
    controller_config: dict[str, str] | None = None,
    include_manifest: bool = True,
) -> tuple[RolloutContext, StepDir, Path]:
    rollout_id = ctx.metadata["rollout_id"]
    ctx.rollout_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    evidence = EvidenceDirectory(ctx.rollout_root, rollout_id)
    evidence.ensure()
    step_dir = evidence.step_dir(3, "kind-cluster")
    manifest = candidate_worktree(step_dir) / s03_kind_cluster.INGRESS_NGINX_KIND_MANIFEST
    manifest.parent.mkdir(parents=True, exist_ok=True)
    if include_manifest:
        if controller_config is None:
            manifest.write_bytes(_AMBIENT_INGRESS_MANIFEST.read_bytes())
        else:
            documents = list(
                yaml.safe_load_all(
                    _AMBIENT_INGRESS_MANIFEST.read_text(encoding="utf-8"),
                ),
            )
            config = next(
                document
                for document in documents
                if isinstance(document, dict)
                and document.get("kind") == "ConfigMap"
                and document.get("metadata", {}).get("name") == "ingress-nginx-controller"
            )
            config["data"].update(controller_config)
            manifest.write_text(
                yaml.safe_dump_all(documents, sort_keys=False),
                encoding="utf-8",
            )
    else:
        (manifest.parent.parent.parent / "README.md").write_text(
            "candidate without ingress manifest\n",
            encoding="utf-8",
        )
    worktree = candidate_worktree(step_dir)
    subprocess.run(["git", "init", "--quiet"], cwd=worktree, check=True)
    subprocess.run(
        ["git", "config", "user.email", "rollout-tests@example.invalid"],
        cwd=worktree,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Rollout Tests"],
        cwd=worktree,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=worktree, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "candidate"],
        cwd=worktree,
        check=True,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    updated_ctx = replace(ctx, resolved_sha=head)
    candidate_artifact = step_dir.artifact_path(
        s03_kind_cluster.INGRESS_NGINX_CANDIDATE_ARTIFACT,
    ).resolve()
    if include_manifest:
        candidate_artifact = s03_kind_cluster._materialize_candidate_ingress_manifest(
            updated_ctx,
            step_dir,
        ).evidence_path
    return updated_ctx, step_dir, candidate_artifact


def _ingress_apply() -> list[str]:
    return ["kubectl", "apply", "-f", "-"]


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


def test_registry_profile_verifies_existing_k3s_without_kind_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_manifest = _backup_manifest(tmp_path)
    ctx = make_ctx(tmp_path, backup_manifest_path=backup_manifest)
    ctx.cluster_config_path.write_text(
        'namespace = "loom-staging"\n'
        'container_registry = "192.168.50.13:5000"\n'
        'container_registry_push = "localhost:5000"\n'
        'persistent_storage_backend = "dynamic"\n'
    )
    step_dir = StepDir(3, "kind-cluster", tmp_path / "03-kind-cluster")
    step_dir.path.mkdir()
    calls: list[tuple[str, ...]] = []

    def run(argv, **_kwargs):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        stdout = "ok\n"
        if command == ("kubectl", "config", "current-context"):
            stdout = "loom-staging\n"
        elif command == ("kubectl", "get", "nodes", "-o", "json"):
            stdout = json.dumps(
                {
                    "items": [
                        {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
                        {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
                    ]
                }
            )
        elif "jsonpath={.subsets[0].addresses[0].ip}" in command:
            stdout = "10.42.0.10"
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setattr(s03_kind_cluster, "run_captured", run)

    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts["ingress_nginx_manifest_source"] == "k3s"
    assert not any(command and command[0] == "kind" for command in calls)
    assert not any("apply" in command or "label" in command for command in calls)


def _write_kind_done_evidence(
    ctx: RolloutContext,
    step_dir: StepDir,
    candidate_manifest: Path,
) -> EvidenceDirectory:
    evidence = EvidenceDirectory(ctx.rollout_root, ctx.metadata["rollout_id"])
    evidence.write_inputs(ctx.to_inputs_dict())
    step = KindClusterStep()
    inputs_hash = step.inputs_hash(ctx)
    started_at = "2026-07-14T12:00:00Z"
    finished_at = "2026-07-14T12:00:01Z"
    state = RolloutState.new(
        rollout_id=ctx.metadata["rollout_id"],
        steps=[(step.number, step.name)],
    )
    state.mark_step_running(step.number, started_at=started_at)
    state.mark_step_done(
        step.number,
        finished_at=finished_at,
        inputs_hash=inputs_hash,
    )
    state.save(evidence.state_path())
    artifacts = {
        "cluster_name": ctx.cluster_name,
        "namespace": ctx.namespace,
        "candidate_sha": ctx.resolved_sha,
        "ingress_nginx": "installed",
        "ingress_nginx_manifest_path": str(candidate_manifest),
        "ingress_nginx_manifest_source": str(
            s03_kind_cluster.INGRESS_NGINX_KIND_MANIFEST,
        ),
        "ingress_nginx_manifest_sha256": hashlib.sha256(
            candidate_manifest.read_bytes(),
        ).hexdigest(),
        "worker_trajectories_storage": "skipped",
        "worker_trajectories_storage_manifest": str(
            step_dir.artifact_path("worker-trajectories-storage.yaml"),
        ),
        "secret_count": "0",
        "secrets_dir": str(s03_kind_cluster._backup_secrets_dir(ctx)),
    }
    evidence.write_step_result(
        step_dir,
        step_result_dict(
            step=step,
            state="done",
            inputs_hash=inputs_hash,
            started_at=started_at,
            finished_at=finished_at,
            summary="kind cluster candidate contract applied",
            artifacts=artifacts,
        ),
    )
    return evidence


def _write_kind_pending_evidence(
    ctx: RolloutContext,
    step_dir: StepDir,
    candidate_manifest: Path,
) -> EvidenceDirectory:
    evidence = _write_kind_done_evidence(ctx, step_dir, candidate_manifest)
    state = RolloutState.load(evidence.state_path())
    result = evidence.read_step_result(step_dir)
    assert result is not None
    state.reset_step_for_retry(
        KindClusterStep.number,
        started_at=str(result["started_at"]),
    )
    state.mark_step_verifying(KindClusterStep.number)
    state.save(evidence.state_path())
    result["state"] = "verifying"
    evidence.write_step_result(step_dir, result)
    return evidence


def test_creates_missing_kind_cluster_and_restores_namespace_secrets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[list[str]] = []
    ingressclass_gets = 0
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        rollout_root=tmp_path / "loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, _ = _prepare_candidate_step(ctx)

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
        if list(argv) == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            Result.stdout = "10.244.0.10"
        if list(argv) == _CONTROLLER_CONFIG_GET:
            Result.stdout = _controller_config_json()
        return Result()

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s03_kind_cluster.run_captured",
        fake_run,
    )
    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts["secret_count"] == "0"
    assert result.artifacts["ingress_nginx"] == "installed"
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
    assert _ingress_apply() in calls
    assert not any(str(part).startswith("https://") for call in calls for part in call)
    assert [
        "kubectl",
        "label",
        "node",
        "loom-staging-control-plane",
        "ingress-ready=true",
        "--overwrite",
    ] in calls
    assert [
        "kubectl",
        "-n",
        "ingress-nginx",
        "wait",
        "--for=condition=Ready",
        "pod",
        "--selector=app.kubernetes.io/component=controller,app.kubernetes.io/instance=ingress-nginx,app.kubernetes.io/name=ingress-nginx",
        "--timeout=180s",
    ] in calls
    assert [
        "kubectl",
        "-n",
        "ingress-nginx",
        "get",
        "endpoints",
        "ingress-nginx-controller-admission",
        "-o",
        "jsonpath={.subsets[0].addresses[0].ip}",
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
    assert 'node-labels: "ingress-ready=true"' in rendered
    assert "hostPort: 80" in rendered
    assert "hostPort: 443" in rendered
    assert f"hostPath: {tmp_path / 'loom-staging'}" in rendered
    assert f"containerPath: {tmp_path / 'loom-staging'}" in rendered


def test_existing_kind_cluster_reconciles_controller_refreshes_and_restores(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[list[str]] = []
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, _ = _prepare_candidate_step(ctx)

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
        if list(argv) == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            Result.stdout = "10.244.0.10"
        if list(argv) == _CONTROLLER_CONFIG_GET:
            Result.stdout = _controller_config_json()
        return Result()

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s03_kind_cluster.run_captured",
        fake_run,
    )
    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts["ingress_nginx"] == "reconciled"
    assert not any(call[:3] == ["kind", "create", "cluster"] for call in calls)
    assert _ingress_apply() in calls
    assert ["kind", "export", "kubeconfig", "--name", "loom-staging"] in calls
    assert [
        "kubectl",
        "label",
        "node",
        "loom-staging-control-plane",
        "ingress-ready=true",
        "--overwrite",
    ] in calls
    secret_apply_calls = [
        call for call in calls if call[:4] == ["kubectl", "-n", "loom-staging", "apply"]
    ]
    assert secret_apply_calls
    assert "--server-side" in secret_apply_calls[-1]
    assert str(tmp_path / "backup" / "secrets") not in secret_apply_calls[-1]


def test_existing_controller_with_missing_ingressclass_is_reconciled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    ingressclass_gets = 0
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(ctx)

    def fake_run(argv, **kwargs):
        nonlocal ingressclass_gets
        call = list(argv)
        calls.append(call)
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if call == ["kind", "get", "clusters"]:
            result.stdout = "loom-staging\n"
        elif call == ["kubectl", "get", "ingressclass", "nginx"]:
            ingressclass_gets += 1
            result.returncode = 1 if ingressclass_gets == 1 else 0
        elif call == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            result.stdout = "10.244.0.10"
        elif call == _CONTROLLER_CONFIG_GET:
            result.stdout = _controller_config_json(
                _expected_controller_config_data(candidate_manifest),
            )
        return result

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)

    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts["ingress_nginx"] == "reconciled"
    assert _ingress_apply() in calls


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
        if list(argv) == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            Result.stdout = "10.244.0.12"
        if list(argv) == _CONTROLLER_CONFIG_GET:
            Result.stdout = _controller_config_json()
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
    ctx, step_dir, _ = _prepare_candidate_step(ctx)

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


def test_ingress_controller_apply_failure_fails_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, _ = _prepare_candidate_step(ctx)
    ingress_apply = _ingress_apply()

    def fake_run(argv, **kwargs):
        call = list(argv)
        calls.append(call)
        if call == ["kind", "get", "clusters"]:
            return SimpleNamespace(returncode=0, stdout="loom-staging\n", stderr="")
        if call == ingress_apply:
            return SimpleNamespace(
                returncode=17,
                stdout="",
                stderr="controller apply rejected",
            )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)
    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code == 17
    assert result.error == "controller apply rejected"
    assert ingress_apply in calls
    assert _CONTROLLER_CONFIG_GET not in calls


@pytest.mark.parametrize("live_state", ["missing", "malformed", "stale", "extra"])
def test_post_apply_controller_config_mismatch_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_state: str,
) -> None:
    calls: list[list[str]] = []
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(ctx)
    ingress_apply = _ingress_apply()
    expected_controller_config = _expected_controller_config_data(candidate_manifest)

    def fake_run(argv, **kwargs):
        call = list(argv)
        calls.append(call)
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if call == ["kind", "get", "clusters"]:
            result.stdout = "loom-staging\n"
        elif call == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            result.stdout = "10.244.0.10"
        elif call == _CONTROLLER_CONFIG_GET:
            if live_state == "missing":
                result.returncode = 1
                result.stderr = "configmap not found"
            elif live_state == "malformed":
                result.stdout = "{not-json"
            else:
                stale = expected_controller_config.copy()
                if live_state == "extra":
                    stale["unexpected-controller-setting"] = "enabled"
                else:
                    stale["server-snippet"] = "merge_slashes on;\n"
                result.stdout = _controller_config_json(stale)
        return result

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)
    result = KindClusterStep().run(ctx, step_dir)

    assert result.exit_code != 0
    assert "ingress-nginx controller ConfigMap" in (result.error or "")
    assert ingress_apply in calls
    assert _CONTROLLER_CONFIG_GET in calls


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
    ctx, step_dir, _ = _prepare_candidate_step(ctx)

    assert KindClusterStep().verify(ctx, step_dir).name == "MISMATCH"


def test_verify_fails_when_ingress_ready_node_label_is_missing(
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
        if list(argv) == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            Result.stdout = "10.244.0.10"
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
    ctx, step_dir, _ = _prepare_candidate_step(ctx)

    assert KindClusterStep().verify(ctx, step_dir).name == "MISMATCH"
    assert [
        "kubectl",
        "get",
        "node",
        "loom-staging-control-plane",
        "-o",
        "jsonpath={.metadata.labels.ingress-ready}",
    ] in calls


def test_verify_fails_when_ingress_admission_endpoint_is_missing(
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
        if list(argv) == [
            "kubectl",
            "get",
            "node",
            "loom-staging-control-plane",
            "-o",
            "jsonpath={.metadata.labels.ingress-ready}",
        ]:
            Result.stdout = "true"
        if list(argv) == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            Result.stdout = ""
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
    ctx, step_dir, _ = _prepare_candidate_step(ctx)

    assert KindClusterStep().verify(ctx, step_dir).name == "MISMATCH"
    assert [
        "kubectl",
        "-n",
        "ingress-nginx",
        "wait",
        "--for=condition=Ready",
        "pod",
        "--selector=app.kubernetes.io/component=controller,app.kubernetes.io/instance=ingress-nginx,app.kubernetes.io/name=ingress-nginx",
        "--timeout=5s",
    ] in calls


@pytest.mark.parametrize(
    ("live_state", "expected"),
    [
        ("missing", "MISMATCH"),
        ("malformed", "MISMATCH"),
        ("stale", "MISMATCH"),
        ("extra", "MISMATCH"),
        ("exact", "MATCH"),
    ],
)
def test_verify_requires_exact_controller_config_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_state: str,
    expected: str,
) -> None:
    calls: list[list[str]] = []
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(ctx)
    expected_controller_config = _expected_controller_config_data(candidate_manifest)

    def fake_run(argv, **kwargs):
        call = list(argv)
        calls.append(call)
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if call == ["kind", "get", "clusters"]:
            result.stdout = "loom-staging\n"
        elif call == [
            "kubectl",
            "get",
            "node",
            "loom-staging-control-plane",
            "-o",
            "jsonpath={.metadata.labels.ingress-ready}",
        ]:
            result.stdout = "true"
        elif call == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            result.stdout = "10.244.0.10"
        elif call == _CONTROLLER_CONFIG_GET:
            if live_state == "missing":
                result.returncode = 1
                result.stderr = "configmap not found"
            elif live_state == "malformed":
                result.stdout = json.dumps({"data": []})
            elif live_state == "stale":
                stale = expected_controller_config.copy()
                stale["http-snippet"] = "map stale $value { default 0; }\n"
                result.stdout = _controller_config_json(stale)
            elif live_state == "extra":
                extra = expected_controller_config.copy()
                extra["unexpected-controller-setting"] = "enabled"
                result.stdout = _controller_config_json(extra)
            else:
                result.stdout = _controller_config_json(expected_controller_config)
        return result

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)
    outcome = KindClusterStep().verify(ctx, step_dir)

    assert outcome.name == expected
    assert _CONTROLLER_CONFIG_GET in calls


def test_candidate_manifest_drives_apply_readback_and_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    apply_stdin: list[str] = []
    ctx = make_ctx(
        tmp_path,
        resolved_sha="c" * 40,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    candidate_config = {
        "allow-snippet-annotations": "false",
        "http-snippet": "map $request_uri $candidate_contract { default 1; }\n",
        "server-snippet": "set $candidate_manifest_bound 1;\n",
    }
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(
        ctx,
        controller_config=candidate_config,
    )
    committed_manifest_text = candidate_manifest.read_text(encoding="utf-8")

    def fake_run(argv, **kwargs):
        call = list(argv)
        calls.append(call)
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if call == ["kind", "get", "clusters"]:
            result.stdout = "loom-staging\n"
            candidate_manifest.write_text(
                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: tampered\n",
                encoding="utf-8",
            )
        elif call == _ingress_apply():
            assert "name: tampered" in candidate_manifest.read_text(encoding="utf-8")
            apply_stdin.append(kwargs["stdin_text"])
        elif call == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            result.stdout = "10.244.0.10"
        elif call == _CONTROLLER_CONFIG_GET:
            result.stdout = _controller_config_json(candidate_config)
        return result

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)
    step = KindClusterStep()

    result = step.run(ctx, step_dir)
    assert candidate_manifest.read_text(encoding="utf-8") == committed_manifest_text
    fingerprint = step._inputs_fingerprint(ctx)

    assert result.exit_code == 0
    assert _ingress_apply() in calls
    assert apply_stdin == [committed_manifest_text]
    assert (
        result.artifacts["ingress_nginx_manifest_sha256"]
        == hashlib.sha256(
            apply_stdin[0].encode("utf-8"),
        ).hexdigest()
    )
    assert result.artifacts["candidate_sha"] == ctx.resolved_sha
    assert result.artifacts["ingress_nginx_manifest_path"] == str(candidate_manifest)
    assert result.artifacts["ingress_nginx_manifest_source"] == str(
        s03_kind_cluster.INGRESS_NGINX_KIND_MANIFEST,
    )
    assert (
        result.artifacts["ingress_nginx_manifest_sha256"]
        == hashlib.sha256(
            candidate_manifest.read_bytes(),
        ).hexdigest()
    )
    assert fingerprint["resolved_sha"] == ctx.resolved_sha
    assert (
        fingerprint["ingress_nginx_manifest_sha256"]
        == hashlib.sha256(
            candidate_manifest.read_bytes(),
        ).hexdigest()
    )
    assert (
        fingerprint["ingress_nginx_manifest_sha256"]
        != hashlib.sha256(
            _AMBIENT_INGRESS_MANIFEST.read_bytes(),
        ).hexdigest()
    )
    assert fingerprint["ingress_nginx_recovery_contract"] == (
        "node-label-admission-and-commit-bound-controller-config-v4"
    )


def test_done_step_rechecks_live_configmap_before_skip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(ctx)
    expected_controller_config = _expected_controller_config_data(candidate_manifest)
    step = KindClusterStep()
    assert step.requires_strict_live_verification()
    step_dir.result_path().write_text(
        json.dumps(
            {
                "number": step.number,
                "name": step.name,
                "state": "done",
                "inputs_hash": step.inputs_hash(ctx),
            },
        ),
        encoding="utf-8",
    )
    live_state = {"extra": False}

    def fake_run(argv, **kwargs):
        call = list(argv)
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if call == ["kind", "get", "clusters"]:
            result.stdout = "loom-staging\n"
        elif call == [
            "kubectl",
            "get",
            "node",
            "loom-staging-control-plane",
            "-o",
            "jsonpath={.metadata.labels.ingress-ready}",
        ]:
            result.stdout = "true"
        elif call == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            result.stdout = "10.244.0.10"
        elif call == _CONTROLLER_CONFIG_GET:
            data = expected_controller_config.copy()
            if live_state["extra"]:
                data["drift-after-done"] = "unexpected"
            result.stdout = _controller_config_json(data)
        return result

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)

    assert step.is_done(ctx, step_dir)
    assert step.verify_done(ctx, step_dir) is s03_kind_cluster.VerifyOutcome.MATCH
    live_state["extra"] = True
    assert step.is_done(ctx, step_dir)
    assert step.verify_done(ctx, step_dir) is s03_kind_cluster.VerifyOutcome.MISMATCH


@pytest.mark.parametrize(
    ("artifact", "tampered"),
    [
        ("candidate_sha", "SECRET_CANDIDATE_SHA"),
        ("ingress_nginx_manifest_sha256", "SECRET_MANIFEST_HASH"),
        ("ingress_nginx_manifest_path", "SECRET_MANIFEST_PATH"),
        ("ingress_nginx_manifest_source", "SECRET_MANIFEST_SOURCE"),
        ("namespace", "SECRET_NAMESPACE"),
        ("cluster_name", "SECRET_CLUSTER_NAME"),
        ("ingress_nginx", "SECRET_INGRESS_STATE"),
        ("worker_trajectories_storage", "SECRET_STORAGE_STATE"),
        (
            "worker_trajectories_storage_manifest",
            "SECRET_STORAGE_MANIFEST_PATH",
        ),
        ("secret_count", "SECRET_COUNT"),
        ("secrets_dir", "SECRET_SECRETS_DIR"),
    ],
)
def test_tampered_kind_done_artifacts_stop_before_live_verify_or_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact: str,
    tampered: str,
) -> None:
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(ctx)
    evidence = _write_kind_done_evidence(ctx, step_dir, candidate_manifest)
    result = evidence.read_step_result(step_dir)
    assert result is not None
    result["artifacts"][artifact] = tampered
    evidence.write_step_result(step_dir, result)
    resumed = KindClusterStep()
    calls: list[str] = []

    def unexpected_verify_done(ctx_arg, step_dir_arg):
        calls.append("verify_done")
        return VerifyOutcome.MATCH

    def unexpected_verify(ctx_arg, step_dir_arg):
        calls.append("verify")
        return VerifyOutcome.MATCH

    def unexpected_run(ctx_arg, step_dir_arg):
        calls.append("run")
        return RunResult(exit_code=0)

    monkeypatch.setattr(resumed, "verify_done", unexpected_verify_done)
    monkeypatch.setattr(resumed, "verify", unexpected_verify)
    monkeypatch.setattr(resumed, "run", unexpected_run)
    stream = io.StringIO()

    rc = run_rollout(ctx, [resumed], evidence, stream)

    assert rc == 2
    assert calls == []
    diagnostic = stream.getvalue()
    assert "persisted DONE result evidence" in diagnostic
    assert "canonical success and artifact contract" in diagnostic
    assert "SECRET_" not in diagnostic
    state = RolloutState.load(evidence.state_path())
    assert state.steps[0].state is StepState.DONE
    assert state.status == "done"
    assert state.driver is None


@pytest.mark.parametrize(
    ("artifact", "tampered"),
    [
        ("candidate_sha", "SECRET_PENDING_CANDIDATE_SHA"),
        ("ingress_nginx_manifest_sha256", "SECRET_PENDING_MANIFEST_HASH"),
        ("ingress_nginx_manifest_path", "SECRET_PENDING_MANIFEST_PATH"),
        ("ingress_nginx_manifest_source", "SECRET_PENDING_MANIFEST_SOURCE"),
    ],
)
def test_tampered_kind_pending_artifacts_never_finalize_after_live_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact: str,
    tampered: str,
) -> None:
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(ctx)
    evidence = _write_kind_pending_evidence(ctx, step_dir, candidate_manifest)
    result = evidence.read_step_result(step_dir)
    assert result is not None
    result["artifacts"][artifact] = tampered
    evidence.write_step_result(step_dir, result)
    resumed = KindClusterStep()
    calls: list[str] = []

    def matching_verify(ctx_arg, step_dir_arg):
        calls.append("verify")
        return VerifyOutcome.MATCH

    def unexpected_run(ctx_arg, step_dir_arg):
        calls.append("run")
        return RunResult(exit_code=0)

    monkeypatch.setattr(resumed, "verify", matching_verify)
    monkeypatch.setattr(resumed, "run", unexpected_run)
    stream = io.StringIO()

    rc = run_rollout(ctx, [resumed], evidence, stream)

    assert rc == 2
    assert calls == []
    diagnostic = stream.getvalue()
    assert "strict VERIFYING result artifacts do not match" in diagnostic
    assert "SECRET_" not in diagnostic
    state = RolloutState.load(evidence.state_path())
    assert state.steps[0].state is StepState.VERIFYING
    assert state.status == "running"
    assert state.driver is None


def test_fingerprint_rematerializes_commit_blob_and_rejects_mutable_worktree(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(
        tmp_path,
        resolved_sha="c" * 40,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, candidate_manifest = _prepare_candidate_step(ctx)
    step = KindClusterStep()
    first_hash = step.inputs_hash(ctx)
    committed_bytes = candidate_manifest.read_bytes()

    candidate_manifest.write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: changed\n",
        encoding="utf-8",
    )
    assert step.inputs_hash(ctx) == first_hash
    assert candidate_manifest.read_bytes() == committed_bytes

    worktree = candidate_worktree(step_dir)
    worktree_manifest = worktree / s03_kind_cluster.INGRESS_NGINX_KIND_MANIFEST
    worktree_manifest.write_bytes(committed_bytes + b"\n# next candidate\n")
    with pytest.raises(RuntimeError, match="candidate worktree is dirty"):
        step.inputs_hash(ctx)

    subprocess.run(["git", "add", "."], cwd=worktree, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "next candidate"],
        cwd=worktree,
        check=True,
    )
    next_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with pytest.raises(RuntimeError, match="HEAD does not match"):
        step.inputs_hash(ctx)
    assert step.inputs_hash(replace(ctx, resolved_sha=next_head)) != first_hash


def test_missing_candidate_manifest_fails_before_any_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, _ = _prepare_candidate_step(
        ctx,
        include_manifest=False,
    )

    def fake_run(argv, **kwargs):
        call = list(argv)
        calls.append(call)
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if call == ["kind", "get", "clusters"]:
            result.stdout = "loom-staging\n"
        elif call == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            result.stdout = "10.244.0.10"
        elif call == _CONTROLLER_CONFIG_GET:
            result.stdout = _controller_config_json()
        return result

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)
    step = KindClusterStep()

    with pytest.raises(RuntimeError, match="could not read candidate blob"):
        step._inputs_fingerprint(ctx)

    result = step.run(ctx, step_dir)

    assert result.exit_code == 2
    assert str(s03_kind_cluster.INGRESS_NGINX_KIND_MANIFEST) in (result.error or "")
    assert calls == []


def test_dirty_candidate_fails_fingerprint_run_and_verify_before_kubectl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, _ = _prepare_candidate_step(ctx)
    worktree_manifest = candidate_worktree(step_dir) / s03_kind_cluster.INGRESS_NGINX_KIND_MANIFEST
    worktree_manifest.write_bytes(worktree_manifest.read_bytes() + b"\n# dirty\n")
    monkeypatch.setattr(
        s03_kind_cluster,
        "run_captured",
        lambda argv, **kwargs: calls.append(list(argv)),
    )
    step = KindClusterStep()

    with pytest.raises(RuntimeError, match="candidate worktree is dirty"):
        step._inputs_fingerprint(ctx)
    result = step.run(ctx, step_dir)
    outcome = step.verify(ctx, step_dir)

    assert result.exit_code == 2
    assert "candidate worktree is dirty" in (result.error or "")
    assert outcome.name == "MISMATCH"
    assert calls == []


def test_verify_missing_candidate_manifest_fails_closed_without_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    ctx = make_ctx(
        tmp_path,
        namespace="loom-staging",
        backup_manifest_path=_backup_manifest(tmp_path),
    )
    ctx, step_dir, _ = _prepare_candidate_step(ctx, include_manifest=False)

    def fake_run(argv, **kwargs):
        call = list(argv)
        calls.append(call)
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if call == ["kind", "get", "clusters"]:
            result.stdout = "loom-staging\n"
        elif call == [
            "kubectl",
            "get",
            "node",
            "loom-staging-control-plane",
            "-o",
            "jsonpath={.metadata.labels.ingress-ready}",
        ]:
            result.stdout = "true"
        elif call == [
            "kubectl",
            "-n",
            "ingress-nginx",
            "get",
            "endpoints",
            "ingress-nginx-controller-admission",
            "-o",
            "jsonpath={.subsets[0].addresses[0].ip}",
        ]:
            result.stdout = "10.244.0.10"
        elif call == _CONTROLLER_CONFIG_GET:
            result.stdout = _controller_config_json()
        return result

    monkeypatch.setattr(s03_kind_cluster, "run_captured", fake_run)

    outcome = KindClusterStep().verify(ctx, step_dir)

    assert outcome.name == "MISMATCH"
    assert calls == []


def test_fingerprint_requires_rollout_id_without_applying(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    ctx = replace(
        make_ctx(
            tmp_path,
            namespace="loom-staging",
            backup_manifest_path=_backup_manifest(tmp_path),
        ),
        metadata={},
    )
    monkeypatch.setattr(
        s03_kind_cluster,
        "run_captured",
        lambda argv, **kwargs: calls.append(list(argv)),
    )

    with pytest.raises(RuntimeError, match="rollout_id"):
        KindClusterStep()._inputs_fingerprint(ctx)

    assert calls == []
