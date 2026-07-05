from __future__ import annotations

import json
import subprocess
from typing import Any

from loom_cli.__main__ import main
from loom_cli.cluster_release_gate import (
    ReleaseGateCheck,
    ReleaseGateReport,
    collect_release_gate_report,
    format_release_gate_markdown,
    query_live_alembic_heads,
    release_gate_report_to_dict,
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
    external_workers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = {
        "schema_version": 1,
        "release": {
            "environment": "staging",
            "git_sha": "a" * 40,
            "image_tag": "staging-abc123",
            "generated_at": "2026-07-01T00:00:00Z",
        },
        "cluster_config": {"sha256": "config-sha", "namespace": "loom"},
        "rendered_manifest": {
            "sha256": "rendered-sha",
            "deployment_images": {
                "loom-service": {"app": "loom-service:staging-abc123"},
            },
            "deployment_image_identities": {
                "loom-service": {
                    "app": {
                        "image": "loom-service:staging-abc123",
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
    if external_workers is not None:
        manifest["external_workers"] = external_workers
    return manifest


def _external_workers_manifest_section() -> dict[str, Any]:
    return {
        "environment_state_file": {
            "path": "deploy/environment-state/staging.toml",
            "sha256": "state-sha",
        },
        "slurm_pools": [
            {
                "pool_name": "oldlab",
                "actuator": "slurm",
                "external_runner": True,
                "env_file": (
                    "/shared_work/qianyi/loom-worker-capacity/"
                    "staging-oldlab-worker-staging-abc123.env"
                ),
                "repo_dir": "/shared_work/qianyi/loom-remote-worker-staging-abc123",
            },
        ],
        "gb10_desired_states": [
            {
                "pool_name": "gb10-arm64",
                "image_tag": "staging-abc123",
                "env_config_version": "staging-abc123",
                "source_git_commit": "abc123ffffffffffffffffffffffffffffffffff",
            },
        ],
    }


def test_release_gate_passes_when_ready_pod_image_id_matches_expected_digest() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-abc",
            app="loom-service",
            image="loom-service:staging-abc123",
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
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-abc",
            app="loom-service",
            image="loom-service:staging-abc123",
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
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-kind",
            app="loom-service",
            image="loom-service:staging-abc123",
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


def test_release_gate_rejects_stale_status_image_on_kind_import_pod() -> None:
    """#339 regression — kind-import must not mask an old ReplicaSet pod.

    Deployment template image says `staging-abc123` (the release target),
    but the only Ready pod still has the old image in its pod spec. The pod's
    runtime image ID has the kind-import shape, so the gate must reject it
    before treating a kind-import runtime identity as acceptable.
    """
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-kind",
            app="loom-service",
            image="loom-service:staging-old",
            status_image="loom-service:staging-old",
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
    assert check.remediation is not None
    assert "wait" in check.remediation.lower()


def test_release_gate_accepts_kind_import_status_image_alias_on_target_pod() -> None:
    """kind/containerd can report another tag for the target pod's image.

    The release gate should reject old ReplicaSet pods by checking the pod spec
    against the Deployment template. Once the Ready pod's spec is the release
    template image, a kind-import runtime identity plus a different
    status.containerStatuses[].image tag can be a containerd display alias.
    """
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-kind",
            app="loom-service",
            image="loom-service:staging-abc123",
            status_image="docker.io/library/loom-service:staging-old",
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
    assert check.evidence["identity_strategy"] == "kind-import-template-image"
    assert check.evidence["status_image_stale"] is True
    assert check.evidence["status_image_matches_template"] is False
    assert check.evidence["live_image"] == "docker.io/library/loom-service:staging-old"


def test_release_gate_does_not_mark_default_docker_prefix_status_image_stale() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-kind",
            app="loom-service",
            image="loom-service:staging-abc123",
            status_image="docker.io/library/loom-service:staging-abc123",
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
    assert check.evidence["live_image"] == "docker.io/library/loom-service:staging-abc123"
    assert check.evidence["status_image_matches_template"] is True
    assert check.evidence["status_image_stale"] is False


def test_release_gate_passes_zero_replica_deployment_when_template_matches() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:staging-abc123",
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
            image="loom-service:staging-abc123",
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
    assert check.evidence["pod_template_image"] == "loom-service:staging-abc123"


def test_release_gate_fails_when_target_generation_pod_lacks_runtime_image_id() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-new",
            app="loom-service",
            image="loom-service:staging-abc123",
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
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-old",
            app="loom-service",
            image="loom-service:staging-old",
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
    assert check.evidence["pod_template_image"] == "loom-service:staging-abc123"


def test_release_gate_fails_when_deployment_generation_is_not_observed() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
            generation=8,
            observed_generation=7,
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-new",
            app="loom-service",
            image="loom-service:staging-abc123",
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
        image="loom-service:staging-abc123",
    )
    deployment.spec.replicas = 2
    deployment.status.updated_replicas = 1
    deployment.status.ready_replicas = 1
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-new",
            app="loom-service",
            image="loom-service:staging-abc123",
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


def test_release_gate_requires_environment_state_check_when_manifest_records_external_workers() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1({
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }),
        core_v1=_FakeCoreV1([
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "environment-state-convergence"
    )
    assert check.outcome == "fail"
    assert check.detail == "environment-state check artifact is required"
    assert check.evidence["expected_profile"] == "deploy/environment-state/staging.toml"
    assert check.evidence["expected_profile_sha256"] == "state-sha"


def test_release_gate_fails_when_environment_state_check_reports_drift() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1({
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }),
        core_v1=_FakeCoreV1([
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": False,
            "drift": [
                {
                    "path": (
                        "slurm_worker_jobs[production/oldlab/18186]."
                        "LOOM_REMOTE_WORKER_ENV_FILE"
                    ),
                    "desired": "staging-d46a16c",
                    "live": "staging-cb6af75",
                },
            ],
        },
        environment_state_check_path=(
            "/data/loom-staging/rollouts/20260702T055745Z-staging-d46a16c/"
            "environment-state-check-live-secrets.json"
        ),
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "environment-state-convergence"
    )
    assert check.outcome == "fail"
    assert check.detail == "live environment-state check reports drift"
    assert check.evidence["drift_count"] == 1
    assert check.evidence["drift"][0]["live"] == "staging-cb6af75"
    assert "environment-state apply/check" in (check.remediation or "")


def test_release_gate_passes_when_environment_state_check_is_clean() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1({
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }),
        core_v1=_FakeCoreV1([
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": True,
            "drift": [],
        },
        environment_state_check_path="environment-state-check-live-secrets.json",
        gb10_workers_status_artifact={
            "desired_states": [
                {
                    "environment": "staging",
                    "pool_name": "gb10-arm64",
                    "image_tag": "staging-abc123",
                    "max_concurrent": 10,
                    "env_config_version": "staging-abc123",
                    "host_intents": {"trt-gb10-1": "active"},
                },
            ],
            "nodes": [
                {
                    "environment": "staging",
                    "pool_name": "gb10-arm64",
                    "hostname": "trt-gb10-1",
                    "apply_state": "applied",
                    "current_image_tag": "staging-abc123",
                    "current_env_config_version": "staging-abc123",
                    "current_max_concurrent": 10,
                    "desired_intent": "active",
                },
            ],
        },
        gb10_workers_status_path="gb10-workers-status-staging-abc123.json",
    )

    assert report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "environment-state-convergence"
    )
    assert check.outcome == "pass"
    assert check.detail == "live environment-state check passed"
    assert check.evidence["drift_count"] == 0
    assert check.evidence["artifact"] == "environment-state-check-live-secrets.json"


def test_release_gate_evidence_includes_autoscaler_blockers() -> None:
    blockers = [
        {
            "environment": "staging",
            "pool_name": "oldlab",
            "actuator": "slurm",
            "last_decision": "blocked",
            "last_decision_reason": "no_safe_slurm_nodes",
            "last_blocked_reason": "no_safe_slurm_nodes",
            "last_blocked_details": {
                "node_exclusions": [
                    {"hostname": "oldlab-1", "reason": "insufficient_memory"},
                    {"hostname": "oldlab-2", "reason": "cpu_load_high"},
                ],
            },
            "last_error": None,
        },
    ]
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1({
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }),
        core_v1=_FakeCoreV1([
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": False,
            "drift": [],
            "autoscaler_blockers": blockers,
        },
        environment_state_check_path="environment-state-check-live-secrets.json",
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "environment-state-convergence"
    )
    assert check.outcome == "fail"
    assert check.detail == "live environment-state check reports autoscaler blockers"
    assert check.evidence["drift_count"] == 0
    assert check.evidence["autoscaler_blocker_count"] == 1
    assert check.evidence["autoscaler_blockers"] == blockers


def test_release_gate_report_includes_component_evidence_rows() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1({
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                generation=9,
                observed_generation=9,
            ),
        }),
        core_v1=_FakeCoreV1([
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": True,
            "drift": [],
        },
        environment_state_check_path="environment-state-check-live-secrets.json",
    )

    data = release_gate_report_to_dict(report)
    rows = data["component_evidence"]

    k8s_row = next(row for row in rows if row["component"] == "loom-service/app")
    assert k8s_row["surface"] == "kubernetes"
    assert k8s_row["expected_release"] == "loom-service:staging-abc123"
    assert k8s_row["live_release"] == "loom-service:staging-abc123"
    assert k8s_row["expected_digest"] == "loom-service@sha256:" + "1" * 64
    assert k8s_row["live_digest"].endswith("sha256:" + "1" * 64)
    assert k8s_row["generation"] == 9
    assert k8s_row["readiness"] == "1/1 ready"
    assert k8s_row["outcome"] == "pass"

    oldlab_row = next(row for row in rows if row["component"] == "oldlab")
    assert oldlab_row["surface"] == "external-worker"
    assert oldlab_row["expected_release"] == "deploy/environment-state/staging.toml"
    assert oldlab_row["live_release"] == "environment-state-check-live-secrets.json"
    assert oldlab_row["readiness"] == "environment-state converged"
    assert oldlab_row["outcome"] == "pass"

    gb10_row = next(row for row in rows if row["component"] == "gb10-arm64")
    assert gb10_row["surface"] == "external-worker"
    assert gb10_row["outcome"] == "pass"


def test_release_gate_markdown_formats_pasteable_component_table() -> None:
    report = ReleaseGateReport(
        environment="staging",
        namespace="loom",
        checks=[
            ReleaseGateCheck(
                name="image-identity:loom-service/app",
                outcome="pass",
                detail="Ready pod image identity matches release manifest",
                evidence={
                    "deployment": "loom-service",
                    "container": "app",
                    "expected_image": "loom-service:staging-abc123",
                    "expected_repo_digest": "loom-service@sha256:" + "1" * 64,
                    "generation": 7,
                    "observed_generation": 7,
                    "desired_replicas": 1,
                    "ready_replicas": 1,
                    "live_image": "loom-service:staging-abc123",
                    "live_image_id": "docker-pullable://loom-service@sha256:" + "1" * 64,
                    "pod": "loom-service-new",
                },
            ),
        ],
    )

    markdown = format_release_gate_markdown(report)

    assert "| Surface | Component | Expected | Live | Generation/job | Readiness | Restart/crash | Evidence | Result |" in markdown
    assert (
        "| kubernetes | loom-service/app | "
        "`loom-service:staging-abc123 / loom-service@sha256:"
        + "1" * 64
        + "` | `loom-service:staging-abc123 / docker-pullable://loom-service@sha256:"
        + "1" * 64
        + "` | `7` | 1/1 ready |  | `pod=loom-service-new` | PASS |"
    ) in markdown


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
            environment="staging",
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
        "staging",
        "--dry-run",
        "--format",
        "json",
    ])

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["all_pass"] is False
    assert out["checks"][0]["name"] == "alembic-heads"


def test_cluster_release_gate_cli_passes_environment_state_check_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(
        json.dumps(_manifest(external_workers=_external_workers_manifest_section())),
        encoding="utf-8",
    )
    environment_state_check_path = tmp_path / "environment-state-check.json"
    environment_state_check_path.write_text(
        json.dumps({
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": True,
            "drift": [],
        }),
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    def _fake_collect_release_gate_report(**kwargs: Any) -> ReleaseGateReport:
        captured.update(kwargs)
        return ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="environment-state-convergence",
                    outcome="pass",
                    detail="live environment-state check passed",
                    evidence={},
                ),
            ],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )

    rc = main([
        "cluster",
        "release-gate",
        "--manifest",
        str(manifest_path),
        "--namespace",
        "loom",
        "--environment",
        "staging",
        "--environment-state-check",
        str(environment_state_check_path),
        "--dry-run",
        "--format",
        "json",
    ])

    assert rc == 0
    assert captured["environment_state_check_artifact"]["ok"] is True
    assert captured["environment_state_check_path"] == str(environment_state_check_path.resolve())


def test_release_gate_requires_gb10_status_artifact_when_manifest_declares_gb10() -> None:
    manifest = _manifest(external_workers=_external_workers_manifest_section())
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-abc",
            app="loom-service",
            image="loom-service:staging-abc123",
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
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "gb10-worker-convergence"
    )
    assert check.outcome == "fail"
    assert check.detail == "GB10 worker status artifact is required"


def test_release_gate_fails_when_gb10_status_reports_missing_active_host() -> None:
    manifest = _manifest(external_workers=_external_workers_manifest_section())
    apps = _FakeAppsV1({
        "loom-service": _deployment(
            name="loom-service",
            image="loom-service:staging-abc123",
        ),
    })
    core = _FakeCoreV1([
        _ready_pod(
            name="loom-service-abc",
            app="loom-service",
            image="loom-service:staging-abc123",
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
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
        gb10_workers_status_artifact={
            "desired_states": [
                {
                    "environment": "staging",
                    "pool_name": "gb10-arm64",
                    "image_tag": "staging-abc123",
                    "max_concurrent": 10,
                    "env_config_version": "staging-abc123",
                    "host_intents": {"trt-gb10-14": "active"},
                },
            ],
            "nodes": [],
        },
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks
        if check.name == "gb10-worker-convergence"
    )
    assert check.outcome == "fail"
    assert "trt-gb10-14" in check.evidence["mismatches"][0]
    assert "missing active node report" in check.evidence["mismatches"][0]


def test_cluster_release_gate_cli_passes_gb10_status_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(
        json.dumps(_manifest(external_workers=_external_workers_manifest_section())),
        encoding="utf-8",
    )
    gb10_status_path = tmp_path / "gb10-workers-status.json"
    gb10_status_path.write_text(
        json.dumps({"desired_states": [], "nodes": []}),
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    def _fake_collect_release_gate_report(**kwargs: Any) -> ReleaseGateReport:
        captured.update(kwargs)
        return ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="gb10-worker-convergence",
                    outcome="pass",
                    detail="GB10 worker status matches release target",
                    evidence={},
                ),
            ],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )

    rc = main([
        "cluster",
        "release-gate",
        "--manifest",
        str(manifest_path),
        "--namespace",
        "loom",
        "--environment",
        "staging",
        "--gb10-workers-status",
        str(gb10_status_path),
        "--dry-run",
        "--format",
        "json",
    ])

    assert rc == 0
    assert captured["gb10_workers_status_artifact"] == {
        "desired_states": [],
        "nodes": [],
    }
    assert captured["gb10_workers_status_path"] == str(gb10_status_path.resolve())
