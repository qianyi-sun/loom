from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_release_gate import (
    ReleaseGateCheck,
    ReleaseGateReport,
    collect_release_gate_report,
    format_release_gate_markdown,
    query_live_alembic_heads,
    release_gate_report_to_dict,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


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
    def __init__(self, pods: list[Any], events: list[Any] | None = None) -> None:
        self.pods = pods
        self.events = events or []

    def list_namespaced_pod(self, *, namespace: str) -> Any:
        return _Spec(items=self.pods)

    def list_namespaced_event(self, *, namespace: str) -> Any:
        return _Spec(items=self.events)


def _deployment(
    *,
    name: str,
    image: str,
    generation: int = 7,
    observed_generation: int = 7,
    replicas: int = 1,
    workload_contract_env: dict[str, str] | None = None,
) -> Any:
    workload_contract_env = workload_contract_env or {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": "internal_trusted",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION": "False",
    }
    return _Spec(
        metadata=_Spec(name=name, generation=generation),
        spec=_Spec(
            replicas=replicas,
            selector=_Spec(match_labels={"app": name}),
            template=_Spec(
                metadata=_Spec(labels={"app": name}),
                spec=_Spec(
                    containers=[
                        _Spec(name="app", image=image),
                        _Spec(
                            name="loom-service",
                            image=image,
                            env=[
                                _Spec(name=key, value=value)
                                for key, value in workload_contract_env.items()
                            ],
                        ),
                    ]
                ),
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


def _pod(
    *,
    name: str,
    app: str,
    image: str,
    deletion_timestamp: str | None = None,
) -> Any:
    metadata = _Spec(name=name, labels={"app": app})
    if deletion_timestamp is not None:
        metadata.deletion_timestamp = deletion_timestamp
    return _Spec(
        metadata=metadata,
        spec=_Spec(containers=[_Spec(name="app", image=image)]),
        status=_Spec(phase="Running", container_statuses=[]),
    )


def _event(*, pod: str, reason: str, message: str) -> Any:
    return _Spec(
        involved_object=_Spec(name=pod),
        reason=reason,
        message=message,
    )


def _manifest(
    *,
    expected_digest: str = "sha256:" + "1" * 64,
    alembic_heads: list[str] | None = None,
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
        "workload_contract": {
            "workload_trust_mode": "internal_trusted",
            "taskset_transforms_enabled": False,
            "taskset_transform_network_isolated": False,
            "untrusted_workload_isolation": False,
        },
    }
    return manifest


@pytest.mark.parametrize(
    "workload_contract",
    [
        None,
        {"workload_trust_mode": "unknown"},
        {
            "workload_trust_mode": "internal_trusted",
            "taskset_transforms_enabled": True,
            "taskset_transform_network_isolated": False,
            "untrusted_workload_isolation": False,
        },
    ],
)
def test_release_gate_rejects_absent_or_invalid_manifest_workload_contract(
    workload_contract: dict[str, Any] | None,
) -> None:
    manifest = _manifest()
    if workload_contract is None:
        manifest.pop("workload_contract")
    else:
        manifest["workload_contract"] = workload_contract
    apps = _FakeAppsV1(
        {"loom-service": _deployment(name="loom-service", image="loom-service:staging-abc123")}
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert not report.all_pass


def test_release_gate_invalid_workload_contract_does_not_echo_raw_candidate_value() -> None:
    raw_mode = "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    manifest = _manifest()
    manifest["workload_contract"]["workload_trust_mode"] = raw_mode
    apps = _FakeAppsV1(
        {"loom-service": _deployment(name="loom-service", image="loom-service:staging-abc123")}
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert raw_mode not in json.dumps(check.evidence, sort_keys=True)


def test_release_gate_does_not_echo_unknown_workload_contract_field_name() -> None:
    raw_field = "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    manifest = _manifest()
    manifest["workload_contract"][raw_field] = False
    apps = _FakeAppsV1(
        {"loom-service": _deployment(name="loom-service", image="loom-service:staging-abc123")}
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert raw_field not in json.dumps(
        {"detail": check.detail, "evidence": check.evidence},
        sort_keys=True,
    )


@pytest.mark.parametrize(
    "expected_env_name",
    [
        "LOOM_SVC_WORKLOAD_TRUST_MODE",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION",
    ],
)
def test_release_gate_rejects_live_loom_service_workload_contract_mismatch(
    expected_env_name: str,
) -> None:
    live_env = {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": "internal_trusted",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION": "False",
    }
    live_env[expected_env_name] = "mismatch"
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                workload_contract_env=live_env,
            )
        }
    )

    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert (
        check.evidence["expected"][expected_env_name] != check.evidence["actual"][expected_env_name]
    )
    assert not report.all_pass


def test_release_gate_live_workload_contract_mismatch_redacts_raw_actual_value() -> None:
    raw_mode = "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    live_env = {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": raw_mode,
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION": "False",
    }
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                workload_contract_env=live_env,
            )
        }
    )

    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert raw_mode not in json.dumps(check.evidence, sort_keys=True)
    assert check.evidence["actual"]["LOOM_SVC_WORKLOAD_TRUST_MODE"] == "[REDACTED]"


def test_release_gate_rejects_missing_live_loom_service_workload_contract_env() -> None:
    live_env = {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": "internal_trusted",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
    }
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                workload_contract_env=live_env,
            )
        }
    )

    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert check.evidence["actual"]["LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION"] is None


def test_release_gate_passes_when_ready_pod_image_id_matches_expected_digest() -> None:
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
    )
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert image_check.outcome == "pass"
    assert image_check.evidence["pod"] == "loom-service-abc"
    assert image_check.evidence["generation"] == 7


def test_release_gate_fails_when_ready_pod_image_id_does_not_match_manifest() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "9" * 64,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.evidence["expected_digest"] == "sha256:" + "1" * 64
    assert check.evidence["live_image_id"].endswith("sha256:" + "9" * 64)
    assert check.evidence["identity_strategy"] == "runtime-image-id-or-repo-digest"
    assert check.evidence["runtime_identity_kind"] == "runtime"
    assert check.evidence["runtime_identity_mismatch"] is True


def test_release_gate_does_not_mark_default_docker_prefix_status_image_stale() -> None:
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
    )
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-target",
                app="loom-service",
                image="loom-service:staging-abc123",
                status_image="docker.io/library/loom-service:staging-abc123",
                image_id="containerd://sha256:" + "1" * 64,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.evidence["live_image"] == "docker.io/library/loom-service:staging-abc123"
    assert check.evidence["status_image_matches_template"] is True
    assert check.evidence["status_image_stale"] is False


def test_release_gate_passes_zero_replica_deployment_when_template_matches() -> None:
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
    )
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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Deployment template image does not match release manifest"
    assert check.evidence["desired_replicas"] == 0


def test_release_gate_ignores_ready_pods_not_from_deployment_template() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-old",
                app="loom-service",
                image="loom-service:old-tag",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "no target-generation Ready pods found for managed Deployment"
    assert check.evidence["pod_template_image"] == "loom-service:staging-abc123"


def test_release_gate_fails_when_target_generation_pod_lacks_runtime_image_id() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id=None,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Ready pod is missing runtime image identity"
    assert check.evidence["runtime_identity_kind"] == "missing"


def test_release_gate_rejects_stale_pod_from_old_template() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-old",
                app="loom-service",
                image="loom-service:staging-old",
                image_id="containerd://sha256:" + "9" * 64,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "no target-generation Ready pods found for managed Deployment"
    assert check.evidence["pod_template_image"] == "loom-service:staging-abc123"


def test_release_gate_fails_when_deployment_generation_is_not_observed() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                generation=8,
                observed_generation=7,
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
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
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.evidence["desired_replicas"] == 2
    assert check.evidence["updated_replicas"] == 1
    assert check.evidence["ready_replicas"] == 1


def test_release_gate_classifies_node_runtime_sandbox_cleanup_failure() -> None:
    """#206 regression: a target pod can be Ready while an old pod is stuck
    terminating because kubelet/containerd cannot kill its pod sandbox.

    That is a node-runtime cleanup failure. The release gate must not pass the
    image identity row just because one target-generation pod is Ready.
    """
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:staging-abc123",
    )
    deployment.status.replicas = 2
    deployment.status.updated_replicas = 1
    deployment.status.ready_replicas = 1
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
            _pod(
                name="loom-service-old",
                app="loom-service",
                image="loom-service:staging-old",
                deletion_timestamp="2026-06-30T16:44:56Z",
            ),
        ],
        events=[
            _event(
                pod="loom-service-old",
                reason="FailedKillPod",
                message=(
                    "KillPodSandboxError: rpc error: code = DeadlineExceeded "
                    "desc = context deadline exceeded"
                ),
            ),
        ],
    )

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
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "node runtime sandbox deadline blocked Deployment rollout"
    assert check.remediation is not None
    assert "--recover-sandbox-deadlines" in check.remediation
    assert check.evidence["failure_class"] == "node_runtime_sandbox_deadline"
    assert check.evidence["total_replicas"] == 2
    assert check.evidence["sandbox_deadline_diagnostics"] == [
        {
            "pod": "loom-service-old",
            "reason": "FailedKillPod",
            "operation": "kill",
            "target_generation": False,
        },
    ]


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

    check = next(check for check in report.checks if check.name == "rendered-manifest-sha256")
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


def test_release_gate_fails_when_disabled_k8s_worker_is_still_live() -> None:
    manifest = _manifest()
    manifest["cluster_config"]["k8s_worker_enabled"] = False
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
            "loom-worker": _deployment(
                name="loom-worker",
                image="loom-worker:stale",
                replicas=6,
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
            _ready_pod(
                name="loom-worker-stale",
                app="loom-worker",
                image="loom-worker:stale",
                image_id="docker-pullable://loom-worker@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom-staging",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "disabled-k8s-worker-pruned")
    assert check.outcome == "fail"
    assert check.detail == "disabled k8s worker remains live"
    assert check.evidence["deployment"] == "loom-worker"
    assert check.evidence["desired_replicas"] == 6
    assert check.evidence["ready_replicas"] == 6
    assert check.evidence["ready_pods"] == ["loom-worker-stale"]
    assert "loom cluster up" in (check.remediation or "")


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


def test_release_gate_fails_when_minio_storage_preflight_stops() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        minio_storage_preflight_artifact={
            "outcome": "stop",
            "filesystem": {
                "free_percent": 8.0,
                "free_bytes": 8 * 1024**3,
            },
            "thresholds": {
                "warn_free_percent": 25.0,
                "stop_free_percent": 15.0,
            },
            "checks": [
                {
                    "name": "minio-data-free-space",
                    "outcome": "stop",
                    "detail": "free space 8.0% is below stop threshold 15.0%",
                },
            ],
        },
        minio_storage_preflight_path="minio-storage-preflight.json",
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "minio-storage-pressure")
    assert check.outcome == "fail"
    assert check.detail == "MinIO storage preflight reports stop"
    assert check.evidence["artifact"] == "minio-storage-preflight.json"
    assert check.evidence["free_percent"] == 8.0


def test_release_gate_report_includes_component_evidence_rows() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                    generation=9,
                    observed_generation=9,
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
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

    assert (
        "| Surface | Component | Expected | Live | Generation/job | Readiness | Restart/crash | Evidence | Result |"
        in markdown
    )
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
            json.dumps(
                {
                    "database_target": "env:LOOM_CP_DB_URL",
                    "heads": ["0050"],
                }
            ),
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

    rc = main(
        [
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
        ]
    )

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["all_pass"] is False
    assert out["checks"][0]["name"] == "alembic-heads"


def test_staging_cluster_release_gate_dry_run_does_not_require_prod_credentials(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(_manifest()), encoding="utf-8")
    for name in (
        "LOOM_CANDIDATE_SHA",
        "LOOM_IMAGE_TAG",
        "LOOM_RELEASE_GATE_RUN_ID",
        "LOOM_SERVICE_API_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)

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
                    name="image-identity:loom-service/app",
                    outcome="pass",
                    detail="staging release manifest identity matched",
                    evidence={},
                ),
            ],
        ),
    )

    rc = main(
        [
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
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["environment"] == "staging"
    assert out["all_pass"] is True


def test_cluster_release_gate_cli_passes_minio_storage_preflight_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(_manifest()), encoding="utf-8")
    storage_path = tmp_path / "minio-storage-preflight.json"
    storage_path.write_text(
        json.dumps(
            {
                "outcome": "pass",
                "filesystem": {"free_percent": 42.0, "free_bytes": 42 * 1024**3},
                "thresholds": {"warn_free_percent": 25.0, "stop_free_percent": 15.0},
                "checks": [],
            }
        ),
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
                    name="minio-storage-pressure",
                    outcome="pass",
                    detail="MinIO storage preflight passed",
                    evidence={},
                ),
            ],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--namespace",
            "loom",
            "--environment",
            "staging",
            "--minio-storage-preflight",
            str(storage_path),
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert captured["minio_storage_preflight_artifact"]["outcome"] == "pass"
    assert captured["minio_storage_preflight_path"] == str(storage_path.resolve())
