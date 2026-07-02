from __future__ import annotations

import json
import subprocess
from typing import Any

from loom_cli.__main__ import main
from loom_cli.cluster_release_gate import (
    ReleaseGateCheck,
    ReleaseGateReport,
    collect_release_gate_report,
    query_live_alembic_heads,
)


class _Spec:
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class _FakeAppsV1:
    def __init__(self, deployments: dict[str, Any]) -> None:
        self.deployments = deployments

    def read_namespaced_deployment(self, *, name: str, namespace: str) -> Any:
        return self.deployments[name]


class _FakeCoreV1:
    def __init__(self, pods: list[Any]) -> None:
        self.pods = pods

    def list_namespaced_pod(self, *, namespace: str) -> Any:
        return _Spec(items=self.pods)


def _deployment(
    *,
    name: str,
    image: str,
    generation: int = 7,
    observed_generation: int = 7,
    replicas: int = 1,
) -> Any:
    return _Spec(
        metadata=_Spec(name=name, generation=generation),
        spec=_Spec(
            replicas=replicas,
            selector=_Spec(match_labels={"app": name}),
            template=_Spec(
                metadata=_Spec(labels={"app": name}),
                spec=_Spec(containers=[_Spec(name="app", image=image)]),
            ),
        ),
        status=_Spec(
            observed_generation=observed_generation,
            ready_replicas=replicas,
            updated_replicas=replicas,
        ),
    )


def _ready_pod(
    *,
    name: str,
    app: str,
    image: str,
    image_id: str | None,
    status_image: str | None = None,
) -> Any:
    container_status = _Spec(name="app", image=status_image or image)
    if image_id is not None:
        container_status.image_id = image_id
    return _Spec(
        metadata=_Spec(name=name, labels={"app": app}),
        spec=_Spec(containers=[_Spec(name="app", image=image)]),
        status=_Spec(
            conditions=[_Spec(type="Ready", status="True")],
            container_statuses=[container_status],
        ),
    )


def _manifest(
    *,
    expected_digest: str = "sha256:" + "1" * 64,
    alembic_heads: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "release": {
            "environment": "public-beta",
            "git_sha": "a" * 40,
            "image_tag": "public-beta-abc123",
            "generated_at": "2026-07-01T00:00:00Z",
        },
        "cluster_config": {"sha256": "config-sha", "namespace": "loom"},
        "rendered_manifest": {
            "sha256": "rendered-sha",
            "deployment_images": {
                "loom-service": {"app": "loom-service:public-beta-abc123"},
            },
            "deployment_image_identities": {
                "loom-service": {
                    "app": {
                        "image": "loom-service:public-beta-abc123",
                        "repo_digest": f"loom-service@{expected_digest}",
                        "image_id": "sha256:" + "2" * 64,
                    },
                },
            },
        },
        "alembic": {
            "expected_heads": alembic_heads or ["0050"],
            "compatible_heads": alembic_heads or ["0050"],
        },
    }


def test_release_gate_passes_when_ready_pod_image_id_matches_expected_digest() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-abc",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert report.all_pass
    image_check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert image_check.outcome == "pass"
    assert image_check.evidence["pod"] == "loom-service-abc"
    assert image_check.evidence["generation"] == 7


def test_release_gate_fails_when_ready_pod_image_id_does_not_match_manifest() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-abc",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            image_id="docker-pullable://loom-service@sha256:" + "9" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.evidence["expected_digest"] == "sha256:" + "1" * 64
    assert check.evidence["live_image_id"].endswith("sha256:" + "9" * 64)
    assert check.evidence["identity_strategy"] == "runtime-image-id-or-repo-digest"
    assert check.evidence["runtime_identity_kind"] == "runtime"
    assert check.evidence["runtime_identity_mismatch"] is True


def test_release_gate_accepts_kind_import_runtime_identity_when_template_matches() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-kind",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "pass"
    assert check.detail == "Ready pod uses kind-imported runtime identity for release template image"
    assert check.evidence["identity_strategy"] == "kind-import-template-image"
    assert check.evidence["runtime_identity_kind"] == "kind-import"
    assert check.evidence["runtime_identity_mismatch"] is True


def test_release_gate_ignores_stale_status_image_tag_when_template_matches() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-kind",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            status_image="loom-service:public-beta-old",
            image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.evidence["live_image"] == "loom-service:public-beta-old"
    assert check.evidence["status_image_matches_template"] is False
    assert check.evidence["status_image_stale"] is True


def test_release_gate_does_not_mark_default_docker_prefix_status_image_stale() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-kind",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            status_image="docker.io/library/loom-service:public-beta-abc123",
            image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.evidence["live_image"] == "docker.io/library/loom-service:public-beta-abc123"
    assert check.evidence["status_image_matches_template"] is True
    assert check.evidence["status_image_stale"] is False


def test_release_gate_passes_zero_replica_deployment_when_template_matches() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:public-beta-abc123",
        replicas=0,
    )
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1([])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "pass"
    assert check.detail == "zero-replica Deployment template image matches release manifest"
    assert check.evidence["desired_replicas"] == 0
    assert check.evidence["identity_strategy"] == "zero-replica-template-image"


def test_release_gate_fails_zero_replica_deployment_when_template_drifts() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:old-tag",
        replicas=0,
    )
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1([])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Deployment template image does not match release manifest"
    assert check.evidence["desired_replicas"] == 0


def test_release_gate_ignores_ready_pods_not_from_deployment_template() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-old",
            app="loom-service",
            image="loom-service:old-tag",
            image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "no target-generation Ready pods found for managed Deployment"
    assert check.evidence["pod_template_image"] == "loom-service:public-beta-abc123"


def test_release_gate_fails_when_target_generation_pod_lacks_runtime_image_id() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-new",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            image_id=None,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Ready pod is missing runtime image identity"
    assert check.evidence["runtime_identity_kind"] == "missing"


def test_release_gate_rejects_stale_kind_import_pod_from_old_template() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-old",
            app="loom-service",
            image="loom-service:public-beta-old",
            image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "no target-generation Ready pods found for managed Deployment"
    assert check.evidence["pod_template_image"] == "loom-service:public-beta-abc123"


def test_release_gate_fails_when_deployment_generation_is_not_observed() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:public-beta-abc123",
            generation=8,
            observed_generation=7,
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-new",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Deployment rollout is not target-generation converged"
    assert check.evidence["generation"] == 8
    assert check.evidence["observed_generation"] == 7


def test_release_gate_fails_when_deployment_updated_replicas_are_partial() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:public-beta-abc123",
    )
    deployment.spec.replicas = 2
    deployment.status.updated_replicas = 1
    deployment.status.ready_replicas = 1
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-new",
            app="loom-service",
            image="loom-service:public-beta-abc123",
            image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
        ),
    ])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.evidence["desired_replicas"] == 2
    assert check.evidence["updated_replicas"] == 1
    assert check.evidence["ready_replicas"] == 1


def test_release_gate_fails_on_rendered_manifest_hash_drift() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=_FakeAppsV1({}),
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="different-rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(
        check for check in report.checks
        if check.name == "rendered-manifest-sha256"
    )
    assert check == ReleaseGateCheck(
        name="rendered-manifest-sha256",
        outcome="fail",
        detail="rendered manifest hash drift",
        evidence={
            "expected_sha256": "rendered-sha",
            "live_sha256": "different-rendered-sha",
        },
        remediation="rerender from the release manifest inputs before accepting rollout",
    )


def test_release_gate_fails_on_live_alembic_revision_mismatch() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(alembic_heads=["0050"]),
        apps_v1=_FakeAppsV1({}),
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0049"],
    )

    check = next(check for check in report.checks if check.name == "alembic-heads")
    assert check.outcome == "fail"
    assert check.evidence == {
        "expected_heads": ["0050"],
        "compatible_heads": ["0050"],
        "live_heads": ["0049"],
        "database_target": "env:LOOM_CP_DB_URL",
    }
    assert "LOOM_CP_DB_URL" in check.detail


def test_live_alembic_query_uses_kubectl_exec_without_leaking_db_url() -> None:
    calls: list[list[str]] = []

    def _runner(cmd: list[str]) -> tuple[int, str, str]:
        calls.append(cmd)
        return (
            0,
            json.dumps({
                "database_target": "env:LOOM_CP_DB_URL",
                "heads": ["0050"],
            }),
            "ignored stderr with postgresql://loom:secret@postgres/loom",
        )

    result = query_live_alembic_heads(
        namespace="loom",
        context="prod",
        runner=_runner,
    )

    assert result.heads == ["0050"]
    assert result.database_target == "env:LOOM_CP_DB_URL"
    assert calls[0][:5] == ["kubectl", "exec", "-n", "loom", "deploy/loom-control-plane"]
    assert "--context" in calls[0]
    assert "secret" not in json.dumps(result.evidence)


def test_live_alembic_query_timeout_returns_redacted_structured_error() -> None:
    def _runner(cmd: list[str]) -> tuple[int, str, str]:
        raise subprocess.TimeoutExpired(
            cmd=cmd,
            timeout=12,
            output="postgresql://loom:secret@postgres/loom",
            stderr="password=super-secret-token",
        )

    result = query_live_alembic_heads(
        namespace="loom",
        context="prod",
        runner=_runner,
        timeout_sec=12,
    )

    assert result.heads == []
    assert result.error == "kubectl exec timed out after 12s"
    evidence = json.dumps(result.evidence)
    assert "super-secret-token" not in evidence
    assert "postgresql://loom:secret" not in evidence
    assert "<redacted>" in evidence


def test_cluster_release_gate_cli_dry_run_reports_structured_failure(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(_manifest()), encoding="utf-8")

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        lambda **_kwargs: ReleaseGateReport(
            environment="public-beta",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="alembic-heads",
                    outcome="fail",
                    detail="live DB revision does not match env:LOOM_CP_DB_URL",
                    evidence={
                        "expected_heads": ["0050"],
                        "live_heads": ["0049"],
                    },
                    remediation="run alembic upgrade head before accepting release",
                ),
            ],
        ),
    )

    rc = main([
        "cluster",
        "release-gate",
        "--manifest",
        str(manifest_path),
        "--namespace",
        "loom",
        "--environment",
        "public-beta",
        "--dry-run",
        "--format",
        "json",
    ])

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["all_pass"] is False
    assert out["checks"][0]["name"] == "alembic-heads"
