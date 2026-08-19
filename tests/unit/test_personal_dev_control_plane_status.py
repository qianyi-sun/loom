from __future__ import annotations

import copy
import hashlib
import importlib
import json
import subprocess
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlan,
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    RenderedPersonalDevControlPlane,
    render_acceptance_personal_dev_control_plane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    PersonalDevAcceptanceStatus,
    observe_personal_dev_acceptance_status,
    observe_personal_dev_shadow_status,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
_MANAGED_BY = "loom-personal-dev-control-plane"
_NOW = datetime(2026, 8, 17, 21, 0, 0, tzinfo=UTC)

_CONTEXT = ("config", "current-context")
_NAMESPACES = ("get", "namespaces", "--output=json")
_RUNTIME_CLASS = (
    "get",
    "runtimeclass.node.k8s.io/loom-personal-dev-builder",
    "--output=json",
)
_NAMESPACED = (
    "get",
    (
        "deployments.apps,statefulsets.apps,jobs.batch,persistentvolumeclaims,"
        "serviceaccounts,roles.rbac.authorization.k8s.io,"
        "rolebindings.rbac.authorization.k8s.io,services,pods,"
        "ingresses.networking.k8s.io,networkpolicies.networking.k8s.io"
    ),
    "--namespace",
    "loom-dev",
    "--selector",
    f"app.kubernetes.io/managed-by={_MANAGED_BY}",
    "--output=json",
)
_CLUSTER = (
    "get",
    (
        "clusterroles.rbac.authorization.k8s.io,"
        "clusterrolebindings.rbac.authorization.k8s.io,"
        "validatingadmissionpolicies.admissionregistration.k8s.io,"
        "validatingadmissionpolicybindings.admissionregistration.k8s.io"
    ),
    "--selector",
    f"app.kubernetes.io/managed-by={_MANAGED_BY}",
    "--output=json",
)
_MANAGER = (
    "--request-timeout=10s",
    "--namespace",
    "loom-dev",
    "exec",
    "deployment/loom-capacity-manager",
    "-c",
    "manager",
    "--",
    "python",
    "-m",
    "loom_capacity_manager.health_probe",
    "--url",
    "https://127.0.0.1:8443/healthz",
    "--ca-file",
    "/var/run/loom-capacity-manager/runtime/credentials/server-ca.pem",
    "--certificate-file",
    "/var/run/loom-capacity-manager/runtime/credentials/health-certificate.pem",
    "--private-key-file",
    "/var/run/loom-capacity-manager/runtime/credentials/health-private-key.pem",
    "--server-certificate-file",
    "/var/run/loom-capacity-manager/runtime/credentials/server-certificate.pem",
    "--observe",
)
_ACCEPTANCE_MANAGER = (
    "--request-timeout=10s",
    "--namespace",
    "loom-dev",
    "exec",
    "deployment/loom-personal-dev-management",
    "-c",
    "management",
    "--",
    "python",
    "-m",
    "loom_capacity_manager.health_probe",
    "--url",
    "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status",
    "--ca-file",
    "/run/loom-personal-dev/management/files/capacity-lifecycle-ca.pem",
    "--certificate-file",
    "/run/loom-personal-dev/management/files/capacity-lifecycle-certificate.pem",
    "--private-key-file",
    "/run/loom-personal-dev/management/files/capacity-lifecycle-private-key.pem",
    "--bearer-token-file",
    "/run/loom-personal-dev/management/files/capacity-lifecycle-token",
    "--observe-identity",
)
_DEPLOYMENTS = (
    "get",
    "deployments.apps",
    "--all-namespaces",
    "--output=json",
)


def _release_value() -> dict[str, object]:
    return {
        "schema_version": 2,
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "images": {
            "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
            "personal_dev_builder": (
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
            ),
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
            ),
            "personal_dev_scanner_cache": (
                "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "a" * 64
            ),
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
        },
        "scanner": {
            "binary_platform": "linux/amd64",
            "binary_sha256": "b" * 64,
            "cache_identity_sha256": (
                "b1c136b8577f3813c62588d6930db21b0f2343b7f70278836741387c43c33761"
            ),
            "database_metadata_sha256": "c" * 64,
            "database_sha256": "d" * 64,
            "java_database_metadata_sha256": "e" * 64,
            "java_database_sha256": "f" * 64,
            "lock_sha256": "1" * 64,
            "trivy_version": "v0.70.0",
        },
        "release_evidence_sha256": "8" * 64,
    }


def _expected_render(tmp_path: Path) -> RenderedPersonalDevControlPlane:
    payload = json.dumps(
        _release_value(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    release_path = tmp_path / "trusted-release.json"
    release_path.write_bytes(payload)
    release_path.chmod(0o600)
    release = load_personal_dev_trusted_release(
        release_path,
        hashlib.sha256(payload).hexdigest(),
    )
    return render_shadow_personal_dev_control_plane(
        load_personal_dev_control_plane_profile(_PROFILE),
        release,
    )


def _identity(item: dict[str, Any]) -> tuple[str, str]:
    return item["kind"], item["metadata"]["name"]


class _FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], object]) -> None:
        self.responses = responses
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]:
        command = tuple(argv)
        self.calls.append((command, timeout_seconds))
        if command not in self.responses:
            raise AssertionError(f"unexpected command: {command!r}")
        configured = self.responses[command]
        if isinstance(configured, subprocess.CompletedProcess):
            return configured
        stdout = (
            configured
            if isinstance(configured, str)
            else json.dumps(
                configured,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return subprocess.CompletedProcess(list(argv), 0, stdout, "")


def _pod_for(item: dict[str, Any], suffix: str, *, phase: str) -> dict[str, Any]:
    template = item["spec"]["template"]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{item['metadata']['name']}-{suffix}",
            "namespace": "loom-dev",
            "labels": copy.deepcopy(template["metadata"]["labels"]),
            "annotations": copy.deepcopy(template["metadata"]["annotations"]),
        },
        "spec": copy.deepcopy(template["spec"]),
        "status": {
            "phase": phase,
            "initContainerStatuses": [],
        },
    }
    if item["kind"] == "Job":
        pod["metadata"]["labels"]["job-name"] = item["metadata"]["name"]
    return pod


def _runtime_class(
    *,
    name: str,
    handler: str,
    profile_sha256: str,
) -> dict[str, Any]:
    return {
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {
            "name": name,
            "annotations": {
                "loom.dev/runtime-profile-sha256": profile_sha256,
            },
        },
        "handler": handler,
        "scheduling": {
            "nodeSelector": {
                "kubernetes.io/arch": "amd64",
                "kubernetes.io/os": "linux",
                "loom.dev/personal-dev-runtime-profile-a": profile_sha256[:32],
                "loom.dev/personal-dev-runtime-profile-b": profile_sha256[32:],
            }
        },
    }


def _mutate_runtime_class(runtime: dict[str, Any], mutation: str) -> None:
    if mutation == "runtime-handler":
        runtime["handler"] = "runc"
    elif mutation == "runtime-profile":
        runtime["metadata"]["annotations"]["loom.dev/runtime-profile-sha256"] = "a" * 64
    elif mutation == "runtime-scheduling-missing":
        runtime.pop("scheduling")
    elif mutation == "runtime-node-selector-missing":
        runtime["scheduling"].pop("nodeSelector")
    elif mutation == "runtime-selector-key-missing":
        runtime["scheduling"]["nodeSelector"].pop("kubernetes.io/os")
    elif mutation == "runtime-selector-key-extra":
        runtime["scheduling"]["nodeSelector"]["loom.dev/extra"] = "true"
    elif mutation == "runtime-profile-half":
        runtime["scheduling"]["nodeSelector"][
            "loom.dev/personal-dev-runtime-profile-a"
        ] = "a" * 32
    elif mutation == "runtime-os":
        runtime["scheduling"]["nodeSelector"]["kubernetes.io/os"] = "windows"
    elif mutation == "runtime-architecture":
        runtime["scheduling"]["nodeSelector"]["kubernetes.io/arch"] = "arm64"
    elif mutation == "runtime-tolerations":
        runtime["scheduling"]["tolerations"] = [{"operator": "Exists"}]
    elif mutation == "runtime-overhead":
        runtime["overhead"] = {"podFixed": {"cpu": "1m"}}
    else:  # pragma: no cover - caller tables are exhaustive
        raise AssertionError(mutation)


def _healthy_fixture(
    tmp_path: Path,
) -> tuple[RenderedPersonalDevControlPlane, _FakeRunner]:
    expected = _expected_render(tmp_path)
    documents = [copy.deepcopy(item) for item in yaml.safe_load_all(expected.yaml_text)]
    namespace = next(item for item in documents if item["kind"] == "Namespace")
    cluster = [item for item in documents if "namespace" not in item["metadata"]]
    cluster.remove(namespace)
    namespaced = [item for item in documents if item["metadata"].get("namespace")]

    generated: list[dict[str, Any]] = []
    for item in namespaced:
        metadata = item["metadata"]
        metadata["generation"] = 1
        kind, name = _identity(item)
        if kind == "StatefulSet":
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 1,
                "currentReplicas": 1,
                "readyReplicas": 1,
                "updatedReplicas": 1,
                "currentRevision": "revision-1",
                "updateRevision": "revision-1",
            }
            template = item["spec"]["volumeClaimTemplates"][0]
            generated.append(
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {
                        "name": f"{template['metadata']['name']}-{name}-0",
                        "namespace": "loom-dev",
                        "labels": copy.deepcopy(template["metadata"]["labels"]),
                        "annotations": copy.deepcopy(template["metadata"]["annotations"]),
                    },
                    "spec": copy.deepcopy(template["spec"]),
                    "status": {"phase": "Bound"},
                }
            )
            generated.append(_pod_for(item, "0", phase="Running"))
        elif kind == "Deployment" and name == "loom-personal-dev-management":
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 1,
                "readyReplicas": 1,
                "availableReplicas": 1,
                "updatedReplicas": 1,
            }
            generated.append(_pod_for(item, "abcde", phase="Running"))
        elif kind == "Deployment":
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 0,
                "readyReplicas": 0,
                "availableReplicas": 0,
                "updatedReplicas": 0,
            }
        elif kind == "Job":
            item["status"] = {
                "active": 0,
                "failed": 0,
                "succeeded": 1,
                "conditions": [{"type": "Complete", "status": "True"}],
            }
            generated.append(_pod_for(item, "abcde", phase="Succeeded"))
        elif kind == "PersistentVolumeClaim":
            item["status"] = {"phase": "Bound"}

    responses: dict[tuple[str, ...], object] = {
        _CONTEXT: "reviewed-loom-dev\n",
        _NAMESPACES: {
            "apiVersion": "v1",
            "kind": "NamespaceList",
            "items": [
                namespace,
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "kube-system"},
                },
            ],
        },
        _RUNTIME_CLASS: _runtime_class(
            name=expected.runtime_class_name,
            handler=expected.runtime_handler,
            profile_sha256=expected.runtime_profile_sha256,
        ),
        _NAMESPACED: {
            "apiVersion": "v1",
            "kind": "List",
            "items": [*namespaced, *generated],
        },
        _CLUSTER: {
            "apiVersion": "v1",
            "kind": "List",
            "items": cluster,
        },
        _MANAGER: '{"executable_new_capacity_ceiling":0,"status":"ready"}\n',
    }
    return expected, _FakeRunner(responses)


def _quota_value(profile: Any) -> dict[str, int]:
    return {
        "builder_global_concurrency": profile.limits.builder_global_concurrency,
        "builder_per_owner_concurrency": profile.limits.builder_per_owner_concurrency,
        "candidate_retained_bytes": profile.limits.candidate_retained_bytes,
        "candidate_retained_count": profile.limits.candidate_retained_count,
        "global_live_instances": profile.limits.global_live_instances,
        "per_owner_aggregate_max_slots": profile.limits.per_owner_aggregate_max_slots,
        "per_owner_aggregate_min_slots": profile.limits.per_owner_aggregate_min_slots,
        "per_owner_live_instances": profile.limits.per_owner_live_instances,
        "source_max_archive_bytes": profile.limits.source_max_archive_bytes,
    }


def _acceptance_inputs(
    tmp_path: Path,
) -> tuple[RenderedPersonalDevControlPlane, PersonalDevAcceptancePlan]:
    payload = json.dumps(
        _release_value(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    release_path = tmp_path / "acceptance-trusted-release.json"
    release_path.write_bytes(payload)
    release_path.chmod(0o600)
    release = load_personal_dev_trusted_release(
        release_path,
        hashlib.sha256(payload).hexdigest(),
    )
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    shadow = render_shadow_personal_dev_control_plane(profile, release)
    protocol_sha256 = hashlib.sha256(
        json.dumps(
            dict(profile.protocol_versions),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    value = {
        "acceptance_owners": [
            {
                "team_id": "00000000-0000-0000-0000-000000000201",
                "user_id": "00000000-0000-0000-0000-000000000301",
            },
            {
                "team_id": "00000000-0000-0000-0000-000000000202",
                "user_id": "00000000-0000-0000-0000-000000000302",
            },
        ],
        "activation": {
            "key_id": "personal-dev-agent-v1",
            "public_key_sha256": "c" * 64,
        },
        "builder": {
            "protocol_map_sha256": protocol_sha256,
            "publisher_identity": profile.builder.publisher_identity,
            "registry_prefix": profile.builder.registry_prefix,
            "runtime_class_name": profile.builder.runtime_class_name,
            "runtime_handler": profile.builder.runtime_handler,
            "runtime_profile_sha256": profile.builder.runtime_profile_sha256,
            "scanner_binary_sha256": release.scanner.binary_sha256,
            "scanner_cache_identity_sha256": release.scanner.cache_identity_sha256,
            "scanner_database_sha256": release.scanner.database_sha256,
            "scanner_database_metadata_sha256": (
                release.scanner.database_metadata_sha256
            ),
            "scanner_finding_policy_sha256": "3" * 64,
            "scanner_java_database_sha256": release.scanner.java_database_sha256,
            "scanner_java_database_metadata_sha256": (
                release.scanner.java_database_metadata_sha256
            ),
            "trusted_launcher_profile_sha256": "e" * 64,
        },
        "manager": {
            "authority_incarnation": "00000000-0000-0000-0000-000000000101",
            "configuration_epoch": 7,
            "executable_new_capacity_ceiling": 0,
            "execution_epoch": 11,
            "execution_state": "prepared",
        },
        "principals": {
            "lifecycle_principal_id": "personal-dev-lifecycle",
            "reporter_principal_id": "personal-dev-reporter",
        },
        "quotas": _quota_value(profile),
        "release": {
            "images": release.canonical_value()["images"],
            "release_evidence_sha256": release.release_evidence_sha256,
            "shadow_manifest_sha256": hashlib.sha256(shadow.yaml_text.encode("utf-8")).hexdigest(),
            "trusted_release_sha256": hashlib.sha256(release.canonical_bytes()).hexdigest(),
        },
        "schema_version": 1,
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "storage": {
            "backup_restore_evidence_sha256": "b" * 64,
            "schema_head": "0102",
        },
        "window": {
            "expires_at": "2099-12-31T23:00:00Z",
            "rollback_expires_at": "2100-01-31T23:00:00Z",
            "started_at": "2026-01-01T00:00:00Z",
        },
    }
    plan_payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    plan_path = tmp_path / "acceptance-plan.json"
    plan_path.write_bytes(plan_payload)
    plan_path.chmod(0o600)
    plan = load_personal_dev_acceptance_plan(
        plan_path,
        hashlib.sha256(plan_payload).hexdigest(),
    )
    return (
        render_acceptance_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=_NOW,
        ),
        plan,
    )


def _acceptance_healthy_fixture(
    tmp_path: Path,
) -> tuple[RenderedPersonalDevControlPlane, PersonalDevAcceptancePlan, _FakeRunner]:
    expected, plan = _acceptance_inputs(tmp_path)
    documents = [copy.deepcopy(item) for item in yaml.safe_load_all(expected.yaml_text)]
    namespace = next(item for item in documents if item["kind"] == "Namespace")
    cluster = [item for item in documents if "namespace" not in item["metadata"]]
    cluster.remove(namespace)
    namespaced = [item for item in documents if item["metadata"].get("namespace")]

    generated: list[dict[str, Any]] = []
    for item in namespaced:
        metadata = item["metadata"]
        metadata["generation"] = 1
        kind, name = _identity(item)
        if kind == "StatefulSet":
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 1,
                "currentReplicas": 1,
                "readyReplicas": 1,
                "updatedReplicas": 1,
                "currentRevision": "revision-1",
                "updateRevision": "revision-1",
            }
            template = item["spec"]["volumeClaimTemplates"][0]
            generated.append(
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {
                        "name": f"{template['metadata']['name']}-{name}-0",
                        "namespace": "loom-dev",
                        "labels": copy.deepcopy(template["metadata"]["labels"]),
                        "annotations": copy.deepcopy(template["metadata"]["annotations"]),
                    },
                    "spec": copy.deepcopy(template["spec"]),
                    "status": {"phase": "Bound"},
                }
            )
            generated.append(_pod_for(item, "0", phase="Running"))
        elif kind == "Deployment":
            replicas = item["spec"]["replicas"]
            item["status"] = {
                "observedGeneration": 1,
                "replicas": replicas,
                "readyReplicas": replicas,
                "availableReplicas": replicas,
                "updatedReplicas": replicas,
            }
            if replicas:
                generated.append(_pod_for(item, "abcde", phase="Running"))
        elif kind == "Job":
            item["status"] = {
                "active": 0,
                "failed": 0,
                "succeeded": 1,
                "conditions": [{"type": "Complete", "status": "True"}],
            }
            generated.append(_pod_for(item, "abcde", phase="Succeeded"))
        elif kind == "PersistentVolumeClaim":
            item["status"] = {"phase": "Bound"}

    deployments = [item for item in namespaced if item["kind"] == "Deployment"]
    responses: dict[tuple[str, ...], object] = {
        _CONTEXT: "reviewed-loom-dev\n",
        _NAMESPACES: {
            "apiVersion": "v1",
            "kind": "NamespaceList",
            "items": [
                namespace,
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "kube-system"},
                },
            ],
        },
        _RUNTIME_CLASS: _runtime_class(
            name=plan.builder.runtime_class_name,
            handler=plan.builder.runtime_handler,
            profile_sha256=plan.builder.runtime_profile_sha256,
        ),
        _NAMESPACED: {
            "apiVersion": "v1",
            "kind": "List",
            "items": [*namespaced, *generated],
        },
        _CLUSTER: {
            "apiVersion": "v1",
            "kind": "List",
            "items": cluster,
        },
        _ACCEPTANCE_MANAGER: {
            "authority_incarnation": str(plan.manager.authority_incarnation),
            "configuration_epoch": plan.manager.configuration_epoch,
            "executable_new_capacity_ceiling": 0,
            "execution_epoch": plan.manager.execution_epoch,
            "execution_state": plan.manager.execution_state,
            "observer_principal_id": plan.principals.lifecycle_principal_id,
        },
        _DEPLOYMENTS: {
            "apiVersion": "apps/v1",
            "kind": "DeploymentList",
            "items": deployments,
        },
    }
    return expected, plan, _FakeRunner(responses)


def _items(runner: _FakeRunner, command: tuple[str, ...]) -> list[dict[str, Any]]:
    document = runner.responses[command]
    assert isinstance(document, dict)
    items = document["items"]
    assert isinstance(items, list)
    return items


def _item(
    runner: _FakeRunner,
    command: tuple[str, ...],
    kind: str,
    name: str,
) -> dict[str, Any]:
    return next(value for value in _items(runner, command) if _identity(value) == (kind, name))


def _observe(
    expected: RenderedPersonalDevControlPlane,
    runner: _FakeRunner,
):
    return observe_personal_dev_shadow_status(
        runner,
        expected=expected,
        namespace="loom-dev",
    )


def _observe_acceptance(
    expected: RenderedPersonalDevControlPlane,
    plan: PersonalDevAcceptancePlan,
    runner: _FakeRunner,
) -> PersonalDevAcceptanceStatus:
    return observe_personal_dev_acceptance_status(
        runner,
        expected=expected,
        plan=plan,
        namespace="loom-dev",
    )


def test_healthy_acceptance_returns_separate_readiness_facets_and_safe_commands(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)

    result = _observe_acceptance(expected, plan, runner)

    assert isinstance(result, PersonalDevAcceptanceStatus)
    assert result.to_dict() == {
        "acceptance_plan_sha256": plan.sha256,
        "application_ready": True,
        "blockers": [],
        "capacity_publication_ready": True,
        "components": [
            {"name": "cluster-resources", "observed": 10, "ready": True},
            {"name": "manager", "observed": 1, "ready": True},
            {"name": "namespaced-resources", "observed": 29, "ready": True},
            {"name": "namespaces", "observed": 1, "ready": True},
            {"name": "personal-workers", "observed": 0, "ready": True},
            {"name": "runtime-class", "observed": 1, "ready": True},
        ],
        "input_sha256": expected.input_sha256,
        "manager_ceiling": 0,
        "mode": "acceptance",
        "ready": True,
        "release_sha256": expected.release_sha256,
        "schema": "loom-personal-dev-control-plane-status-v1",
        "worker_available": False,
    }
    assert [call for call, _timeout in runner.calls] == [
        _CONTEXT,
        _NAMESPACES,
        _RUNTIME_CLASS,
        _NAMESPACED,
        _CLUSTER,
        _ACCEPTANCE_MANAGER,
        _DEPLOYMENTS,
    ]
    assert all(1 <= timeout <= 10 for _call, timeout in runner.calls)
    assert sum(call == _DEPLOYMENTS for call, _timeout in runner.calls) == 1
    for command, _timeout in runner.calls:
        assert "secret" not in " ".join(command).casefold()
        assert command[0] in {"config", "get", "--request-timeout=10s"}


def test_status_accepts_api_server_canonical_empty_literal_environment(
    tmp_path: Path,
) -> None:
    shadow_expected, shadow_runner = _healthy_fixture(tmp_path)
    acceptance_expected, plan, acceptance_runner = _acceptance_healthy_fixture(tmp_path)

    for runner, expected_empty_names in (
        (
            shadow_runner,
            {
                "LOOM_SVC_DEV_INSTANCE_KUBE_CONTEXT",
                "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_POLICY_SHA256",
                "LOOM_SVC_PERSONAL_DEV_TRUSTED_LAUNCHER_PROFILE_SHA256",
            },
        ),
        (acceptance_runner, {"LOOM_SVC_DEV_INSTANCE_KUBE_CONTEXT"}),
    ):
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        environment = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        removed: set[str] = set()
        for entry in environment:
            if entry.get("value") != "":
                continue
            assert set(entry) == {"name", "value"}
            removed.add(entry["name"])
            del entry["value"]
        assert removed == expected_empty_names

    shadow_status = _observe(shadow_expected, shadow_runner)
    acceptance_status = _observe_acceptance(acceptance_expected, plan, acceptance_runner)

    assert shadow_status.ready is True
    assert shadow_status.blockers == ()
    assert acceptance_status.ready is True
    assert acceptance_status.blockers == ()


def test_status_accepts_api_server_default_empty_admission_selectors(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    policies = [
        item
        for item in _items(runner, _CLUSTER)
        if item["kind"] == "ValidatingAdmissionPolicy"
    ]
    assert len(policies) == 3
    for policy in policies:
        constraints = policy["spec"]["matchConstraints"]
        constraints["namespaceSelector"] = {}
        constraints["objectSelector"] = {}

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()


def test_acceptance_status_allows_monotonic_manager_configuration_advancement(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    manager = runner.responses[_ACCEPTANCE_MANAGER]
    assert isinstance(manager, dict)
    manager["configuration_epoch"] = plan.manager.configuration_epoch + 4

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is True
    assert result.capacity_publication_ready is True
    assert "manager_binding_drift" not in result.blockers


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        ("cluster-role-aggregation", "cluster_resource_drift"),
        ("admission-binding-match-resources", "cluster_resource_drift"),
        ("admission-policy-namespace-selector", "cluster_resource_drift"),
        ("admission-policy-object-selector", "cluster_resource_drift"),
        ("default-deny-ingress", "resource_inventory_drift"),
        ("management-host-network", "resource_inventory_drift"),
        ("management-pod-host-network", "resource_inventory_drift"),
        ("postgres-node-port", "resource_inventory_drift"),
    ],
)
def test_acceptance_status_rejects_live_authority_or_exposure_widening(
    tmp_path: Path,
    mutation: str,
    blocker: str,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)

    if mutation == "cluster-role-aggregation":
        role = next(item for item in _items(runner, _CLUSTER) if item["kind"] == "ClusterRole")
        role["aggregationRule"] = {
            "clusterRoleSelectors": [{"matchLabels": {"loom.dev/aggregate": "true"}}]
        }
    elif mutation == "admission-binding-match-resources":
        binding = next(
            item
            for item in _items(runner, _CLUSTER)
            if item["kind"] == "ValidatingAdmissionPolicyBinding"
        )
        binding["spec"]["matchResources"] = {
            "namespaceSelector": {"matchLabels": {"loom.dev/skip-policy": "false"}}
        }
    elif mutation in {
        "admission-policy-namespace-selector",
        "admission-policy-object-selector",
    }:
        policy = next(
            item
            for item in _items(runner, _CLUSTER)
            if item["kind"] == "ValidatingAdmissionPolicy"
        )
        selector = (
            "namespaceSelector"
            if mutation == "admission-policy-namespace-selector"
            else "objectSelector"
        )
        policy["spec"]["matchConstraints"][selector] = {
            "matchLabels": {"loom.dev/skip-admission": "true"}
        }
    elif mutation == "default-deny-ingress":
        policy = _item(
            runner,
            _NAMESPACED,
            "NetworkPolicy",
            "loom-personal-dev-default-deny",
        )
        policy["spec"]["ingress"] = [{}]
    elif mutation == "management-host-network":
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        deployment["spec"]["template"]["spec"]["hostNetwork"] = True
    elif mutation == "management-pod-host-network":
        pod = next(
            item
            for item in _items(runner, _NAMESPACED)
            if item["kind"] == "Pod"
            and item["metadata"]["labels"].get("app") == "loom-personal-dev-management"
        )
        pod["spec"]["hostNetwork"] = True
    elif mutation == "postgres-node-port":
        service = _item(runner, _NAMESPACED, "Service", "loom-dev-postgres")
        service["spec"]["type"] = "NodePort"
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert blocker in result.blockers


def test_acceptance_queries_the_runtime_class_bound_by_the_plan(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    runtime_class_name = "custom-personal-dev-runtime"
    plan = replace(
        plan,
        builder=replace(plan.builder, runtime_class_name=runtime_class_name),
    )
    runtime_command = (
        "get",
        f"runtimeclass.node.k8s.io/{runtime_class_name}",
        "--output=json",
    )
    runner.responses[runtime_command] = _runtime_class(
        name=runtime_class_name,
        handler=plan.builder.runtime_handler,
        profile_sha256=plan.builder.runtime_profile_sha256,
    )

    result = _observe_acceptance(expected, plan, runner)

    assert "runtime_class_binding_invalid" not in result.blockers
    assert runtime_command in [command for command, _timeout in runner.calls]


def test_acceptance_permits_only_exact_owned_dynamic_namespace_families(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    namespaces = _items(runner, _NAMESPACES)
    personal_subject = "00000000-0000-0000-0000-000000000401"
    builder_subject = "00000000-0000-0000-0000-000000000402"
    namespaces.extend(
        [
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "loom-dev-alice",
                    "labels": {
                        "app.kubernetes.io/managed-by": "loom-dev-instance-controller",
                        "app.kubernetes.io/part-of": "loom",
                        "loom.dev/subject": personal_subject,
                        "pod-security.kubernetes.io/enforce": "restricted",
                    },
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": f"loom-build-{'a' * 32}-l{'b' * 16}",
                    "labels": {
                        "app.kubernetes.io/managed-by": ("loom-personal-dev-builder-controller"),
                        "app.kubernetes.io/part-of": "loom",
                        "loom.dev/subject": builder_subject,
                        "pod-security.kubernetes.io/enforce": "restricted",
                    },
                },
            },
        ]
    )
    deployments = _items(runner, _DEPLOYMENTS)
    deployments.append(
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "loom-service-g1",
                "namespace": "loom-dev-alice",
                "labels": {"app": "loom-service-g1"},
            },
            "spec": {
                "template": {
                    "metadata": {"labels": {"app": "loom-service-g1"}},
                    "spec": {
                        "containers": [
                            {
                                "name": "service",
                                "image": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
                                "env": [{"name": "LOOM_SVC_K8S_WORKER_ENABLED", "value": "false"}],
                            }
                        ]
                    },
                }
            },
        }
    )

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is True
    assert result.worker_available is False
    components = {component.name: component for component in result.components}
    assert components["namespaces"].observed == 3
    assert components["namespaces"].ready is True
    assert components["personal-workers"].observed == 0


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        ("acceptance-plan-digest", "acceptance_plan_digest_drift"),
        ("management-feature-disabled", "management_acceptance_binding_invalid"),
        ("management-readiness-path", "management_acceptance_probe_invalid"),
        ("activation-replicas", "activation_replicas_invalid"),
        ("activation-not-ready", "activation_not_ready"),
        ("runtime-handler", "runtime_class_binding_invalid"),
        ("runtime-profile", "runtime_class_binding_invalid"),
        ("runtime-scheduling-missing", "runtime_class_binding_invalid"),
        ("runtime-node-selector-missing", "runtime_class_binding_invalid"),
        ("runtime-selector-key-missing", "runtime_class_binding_invalid"),
        ("runtime-selector-key-extra", "runtime_class_binding_invalid"),
        ("runtime-profile-half", "runtime_class_binding_invalid"),
        ("runtime-os", "runtime_class_binding_invalid"),
        ("runtime-architecture", "runtime_class_binding_invalid"),
        ("runtime-tolerations", "runtime_class_binding_invalid"),
        ("runtime-overhead", "runtime_class_binding_invalid"),
        ("scanner-cache-identity", "management_acceptance_binding_invalid"),
        ("scanner-database-metadata", "management_acceptance_binding_invalid"),
        ("scanner-identity", "management_acceptance_binding_invalid"),
        ("scanner-java-database-metadata", "management_acceptance_binding_invalid"),
        ("scanner-policy", "management_acceptance_binding_invalid"),
        ("manager-authority", "manager_binding_drift"),
        ("manager-principal", "manager_binding_drift"),
        ("manager-configuration-epoch-regression", "manager_binding_drift"),
        ("manager-state", "manager_binding_drift"),
        ("manager-execution-epoch", "manager_binding_drift"),
        ("manager-ceiling", "manager_ceiling_nonzero"),
        ("window-expired", "acceptance_window_expired"),
        ("personal-name", "personal_namespace_invalid"),
        ("personal-owner", "personal_namespace_invalid"),
        ("personal-subject", "personal_namespace_invalid"),
        ("builder-name", "builder_namespace_invalid"),
        ("builder-owner", "builder_namespace_invalid"),
        ("builder-subject", "builder_namespace_invalid"),
        ("personal-worker", "unexpected_personal_worker"),
        ("manager-oversized", "manager_probe_unavailable"),
        ("deployments-wrong-kind", "deployment_inventory_invalid"),
        ("bool-returncode", "kube_context_invalid"),
    ],
)
def test_acceptance_status_matrix_fails_closed_on_exact_binding_drift(
    tmp_path: Path,
    mutation: str,
    blocker: str,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)

    if mutation == "acceptance-plan-digest":
        service = _item(
            runner,
            _NAMESPACED,
            "Service",
            "loom-personal-dev-management",
        )
        service["metadata"]["annotations"]["loom.dev/acceptance-plan-sha256"] = "a" * 64
    elif mutation in {
        "management-feature-disabled",
        "scanner-cache-identity",
        "scanner-database-metadata",
        "scanner-identity",
        "scanner-java-database-metadata",
        "scanner-policy",
    }:
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        environment = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        field = {
            "management-feature-disabled": "LOOM_SVC_DEV_INSTANCES_ENABLED",
            "scanner-cache-identity": (
                "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_IDENTITY_SHA256"
            ),
            "scanner-database-metadata": (
                "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_DATABASE_METADATA_SHA256"
            ),
            "scanner-identity": "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_IDENTITY",
            "scanner-java-database-metadata": (
                "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_JAVA_DATABASE_METADATA_SHA256"
            ),
            "scanner-policy": "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_POLICY_SHA256",
        }[mutation]
        entry = next(value for value in environment if value["name"] == field)
        entry["value"] = "false" if mutation == "management-feature-disabled" else "a" * 64
    elif mutation == "management-readiness-path":
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        deployment["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["httpGet"][
            "path"
        ] = "/api/v1/health"
    elif mutation in {"activation-replicas", "activation-not-ready"}:
        activation = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-activation-agent",
        )
        if mutation == "activation-replicas":
            activation["spec"]["replicas"] = 0
        else:
            activation["status"]["availableReplicas"] = 0
    elif mutation.startswith("runtime-"):
        runtime = runner.responses[_RUNTIME_CLASS]
        assert isinstance(runtime, dict)
        _mutate_runtime_class(runtime, mutation)
    elif mutation.startswith("manager-") and mutation != "manager-oversized":
        manager = runner.responses[_ACCEPTANCE_MANAGER]
        assert isinstance(manager, dict)
        if mutation == "manager-authority":
            manager["authority_incarnation"] = "00000000-0000-0000-0000-000000000102"
        elif mutation == "manager-principal":
            manager["observer_principal_id"] = "wrong-lifecycle"
        elif mutation == "manager-configuration-epoch-regression":
            manager["configuration_epoch"] = 6
        elif mutation == "manager-state":
            manager["execution_state"] = "drain-only"
        elif mutation == "manager-execution-epoch":
            manager["execution_epoch"] = 12
        elif mutation == "manager-ceiling":
            manager["execution_state"] = "active"
            manager["executable_new_capacity_ceiling"] = 1
        else:  # pragma: no cover - parameter table is exhaustive
            raise AssertionError(mutation)
    elif mutation == "window-expired":
        plan = replace(
            plan,
            window=replace(
                plan.window,
                started_at=datetime(2025, 1, 1, tzinfo=UTC),
                expires_at=datetime(2025, 2, 1, tzinfo=UTC),
                rollback_expires_at=datetime(2025, 3, 1, tzinfo=UTC),
            ),
        )
    elif mutation in {"personal-name", "personal-owner", "personal-subject"}:
        name = "loom-dev-shared" if mutation == "personal-name" else "loom-dev-alice"
        managed_by = (
            "wrong-controller" if mutation == "personal-owner" else "loom-dev-instance-controller"
        )
        subject = (
            "NOT-A-UUID"
            if mutation == "personal-subject"
            else "00000000-0000-0000-0000-000000000401"
        )
        _items(runner, _NAMESPACES).append(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": name,
                    "labels": {
                        "app.kubernetes.io/managed-by": managed_by,
                        "app.kubernetes.io/part-of": "loom",
                        "loom.dev/subject": subject,
                        "pod-security.kubernetes.io/enforce": "restricted",
                    },
                },
            }
        )
    elif mutation.startswith("builder-"):
        name = (
            "loom-build-attempt"
            if mutation == "builder-name"
            else f"loom-build-{'a' * 32}-l{'b' * 16}"
        )
        managed_by = (
            "wrong-controller"
            if mutation == "builder-owner"
            else "loom-personal-dev-builder-controller"
        )
        subject = (
            "00000000-0000-0000-0000-000000000000"
            if mutation == "builder-subject"
            else "00000000-0000-0000-0000-000000000402"
        )
        _items(runner, _NAMESPACES).append(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": name,
                    "labels": {
                        "app.kubernetes.io/managed-by": managed_by,
                        "app.kubernetes.io/part-of": "loom",
                        "loom.dev/subject": subject,
                        "pod-security.kubernetes.io/enforce": "restricted",
                    },
                },
            }
        )
    elif mutation == "personal-worker":
        _items(runner, _DEPLOYMENTS).append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "loom-worker",
                    "namespace": "loom-dev-alice",
                    "labels": {"app": "loom-worker"},
                },
                "spec": {
                    "template": {
                        "metadata": {"labels": {"app": "loom-worker"}},
                        "spec": {"containers": [{"name": "worker"}]},
                    }
                },
            }
        )
    elif mutation == "manager-oversized":
        runner.responses[_ACCEPTANCE_MANAGER] = "x" * (4 * 1024 * 1024 + 1)
    elif mutation == "deployments-wrong-kind":
        deployments = runner.responses[_DEPLOYMENTS]
        assert isinstance(deployments, dict)
        deployments["kind"] = "List"
    elif mutation == "bool-returncode":
        runner.responses[_CONTEXT] = subprocess.CompletedProcess(
            list(_CONTEXT),
            False,
            "reviewed-loom-dev\n",
            "",
        )
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert blocker in result.blockers
    assert result.blockers == tuple(sorted(set(result.blockers)))
    assert result.worker_available is False
    if mutation.startswith("manager-"):
        assert result.application_ready is True
        assert result.capacity_publication_ready is False
    if mutation in {
        "activation-replicas",
        "activation-not-ready",
        "management-feature-disabled",
        "management-readiness-path",
        "runtime-handler",
        "runtime-profile",
        "scanner-cache-identity",
        "scanner-database-metadata",
        "scanner-identity",
        "scanner-java-database-metadata",
        "scanner-policy",
    }:
        assert result.application_ready is False
        assert result.capacity_publication_ready is True


@pytest.mark.parametrize("mode", ["shadow", "acceptance"])
@pytest.mark.parametrize(
    ("drift", "blocker"),
    [
        ("init-image", "workload_image_drift"),
        ("init-argument", "resource_inventory_drift"),
        ("init-root-mount", "resource_inventory_drift"),
        ("generation-subpath", "resource_inventory_drift"),
        ("cache-path", "resource_inventory_drift"),
        ("fanal-limit", "resource_inventory_drift"),
        ("node-architecture", "resource_inventory_drift"),
    ],
)
def test_status_blocks_release_bound_scanner_cache_workload_drift(
    tmp_path: Path,
    mode: str,
    drift: str,
    blocker: str,
) -> None:
    if mode == "shadow":
        expected, runner = _healthy_fixture(tmp_path)
        plan = None
    else:
        expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    deployment = _item(
        runner,
        _NAMESPACED,
        "Deployment",
        "loom-personal-dev-management",
    )
    pod = deployment["spec"]["template"]["spec"]
    init = next(
        item
        for item in pod["initContainers"]
        if item["name"] == "personal-dev-scanner-cache-init"
    )
    service = next(item for item in pod["containers"] if item["name"] == "management")

    if drift == "init-image":
        init["image"] = (
            "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "9" * 64
        )
    elif drift == "init-argument":
        init["args"][-1] = "9" * 64
    elif drift == "init-root-mount":
        next(
            mount for mount in init["volumeMounts"] if mount["name"] == "scanner-cache"
        )["mountPath"] = "/tmp/scanner-cache"
    elif drift == "generation-subpath":
        next(
            mount for mount in service["volumeMounts"] if mount["name"] == "scanner-cache"
        )["subPath"] = "generations/" + "9" * 64
    elif drift == "cache-path":
        next(
            entry
            for entry in service["env"]
            if entry["name"] == "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_DIR"
        )["value"] = "/var/lib/loom-personal-dev-scanner"
    elif drift == "fanal-limit":
        next(volume for volume in pod["volumes"] if volume["name"] == "scanner-fanal")[
            "emptyDir"
        ]["sizeLimit"] = "8Gi"
    elif drift == "node-architecture":
        pod["nodeSelector"]["kubernetes.io/arch"] = "arm64"
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(drift)

    result = (
        _observe(expected, runner)
        if plan is None
        else _observe_acceptance(expected, plan, runner)
    )

    assert result.ready is False
    assert blocker in result.blockers


def test_acceptance_observer_rejects_invalid_local_inputs_before_any_call(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)

    with pytest.raises(ValueError, match="namespace"):
        observe_personal_dev_acceptance_status(
            runner,
            expected=expected,
            plan=plan,
            namespace="loom-dev-alice",
        )
    with pytest.raises(TypeError, match="expected render"):
        observe_personal_dev_acceptance_status(
            runner,
            expected=object(),  # type: ignore[arg-type]
            plan=plan,
        )
    with pytest.raises(TypeError, match="acceptance plan"):
        observe_personal_dev_acceptance_status(
            runner,
            expected=expected,
            plan=object(),  # type: ignore[arg-type]
        )

    assert runner.calls == []


def test_healthy_shadow_returns_canonical_bounded_status_and_safe_commands(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)

    result = _observe(expected, runner)

    assert result.to_dict() == {
        "blockers": [],
        "components": [
            {"name": "cluster-resources", "observed": 10, "ready": True},
            {"name": "manager", "observed": 1, "ready": True},
            {"name": "namespaced-resources", "observed": 28, "ready": True},
            {"name": "namespaces", "observed": 1, "ready": True},
            {"name": "runtime-class", "observed": 1, "ready": True},
        ],
        "input_sha256": expected.input_sha256,
        "manager_ceiling": 0,
        "mode": "shadow",
        "ready": True,
        "release_sha256": expected.release_sha256,
        "schema": "loom-personal-dev-control-plane-status-v1",
    }
    assert [call for call, _timeout in runner.calls] == [
        _CONTEXT,
        _NAMESPACES,
        _RUNTIME_CLASS,
        _NAMESPACED,
        _CLUSTER,
        _MANAGER,
    ]
    assert all(1 <= timeout <= 10 for _call, timeout in runner.calls)
    assert sum(call == _NAMESPACES for call, _timeout in runner.calls) == 1
    for command, _timeout in runner.calls:
        assert "secret" not in " ".join(command).casefold()
        assert command[0] in {"config", "get", "--request-timeout=10s"}
    assert runner.calls[-1][0] == _MANAGER


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        ("namespace-missing", "namespace_missing"),
        ("namespace-wrong-kind", "namespace_inventory_invalid"),
        ("shared-object-missing", "resource_inventory_drift"),
        ("statefulset-not-ready", "storage_not_ready"),
        ("deployment-not-ready", "management_not_ready"),
        ("migration-missing", "migration_missing"),
        ("migration-failed", "migration_failed"),
        ("migration-running", "migration_incomplete"),
        ("init-failed", "init_container_failed"),
        ("mutable-image", "workload_image_drift"),
        ("changed-image", "workload_image_drift"),
        ("render-digest-mismatch", "resource_digest_drift"),
        ("release-digest-mismatch", "resource_digest_drift"),
        ("flag-missing", "management_shadow_flags_invalid"),
        ("flag-malformed", "management_shadow_flags_invalid"),
        ("flag-true", "management_shadow_flags_invalid"),
        ("activation-nonzero", "activation_replicas_nonzero"),
        ("runtime-class-missing", "runtime_class_missing"),
        ("runtime-handler", "runtime_class_missing"),
        ("runtime-profile", "runtime_class_missing"),
        ("runtime-scheduling-missing", "runtime_class_missing"),
        ("runtime-node-selector-missing", "runtime_class_missing"),
        ("runtime-selector-key-missing", "runtime_class_missing"),
        ("runtime-selector-key-extra", "runtime_class_missing"),
        ("runtime-profile-half", "runtime_class_missing"),
        ("runtime-os", "runtime_class_missing"),
        ("runtime-architecture", "runtime_class_missing"),
        ("runtime-tolerations", "runtime_class_missing"),
        ("runtime-overhead", "runtime_class_missing"),
        ("scanner-pvc-missing", "storage_not_ready"),
        ("unexpected-personal-namespace", "unexpected_personal_namespace"),
        ("unexpected-builder-namespace", "unexpected_builder_namespace"),
        ("cluster-binding-drift", "cluster_resource_drift"),
        ("manager-unavailable", "manager_probe_unavailable"),
        ("manager-not-ready", "manager_probe_unavailable"),
        ("manager-malformed", "manager_probe_invalid"),
        ("manager-nonzero", "manager_ceiling_nonzero"),
    ],
)
def test_shadow_status_matrix_reports_stable_sorted_blockers(
    tmp_path: Path,
    mutation: str,
    blocker: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    namespaced = _items(runner, _NAMESPACED)
    namespaces = _items(runner, _NAMESPACES)

    if mutation == "namespace-missing":
        namespaces[:] = [item for item in namespaces if item["metadata"]["name"] != "loom-dev"]
    elif mutation == "namespace-wrong-kind":
        shared = next(item for item in namespaces if item["metadata"]["name"] == "loom-dev")
        shared.update({"apiVersion": "v1", "kind": "ConfigMap"})
    elif mutation == "shared-object-missing":
        namespaced.remove(_item(runner, _NAMESPACED, "Service", "loom-dev-minio"))
    elif mutation == "statefulset-not-ready":
        _item(runner, _NAMESPACED, "StatefulSet", "loom-dev-minio")["status"]["readyReplicas"] = 0
    elif mutation == "deployment-not-ready":
        _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )["status"]["availableReplicas"] = 0
    elif mutation == "migration-missing":
        namespaced[:] = [item for item in namespaced if item["kind"] != "Job"]
    elif mutation in {"migration-failed", "migration-running"}:
        migration = next(item for item in namespaced if item["kind"] == "Job")
        if mutation == "migration-failed":
            migration["status"] = {"active": 0, "failed": 1, "succeeded": 0}
        else:
            migration["status"] = {"active": 1, "failed": 0, "succeeded": 0}
    elif mutation == "init-failed":
        pod = next(item for item in namespaced if item["kind"] == "Pod")
        pod["status"]["initContainerStatuses"] = [
            {
                "name": "credentials",
                "state": {"terminated": {"exitCode": 1, "reason": "Error"}},
            }
        ]
    elif mutation in {"mutable-image", "changed-image"}:
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        deployment["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "ghcr.io/qianyi-sun/loom-service:dev"
            if mutation == "mutable-image"
            else "ghcr.io/qianyi-sun/loom-service@sha256:" + "a" * 64
        )
    elif mutation in {"render-digest-mismatch", "release-digest-mismatch"}:
        service = _item(
            runner,
            _NAMESPACED,
            "Service",
            "loom-personal-dev-management",
        )
        annotation = (
            "loom.dev/render-input-sha256"
            if mutation == "render-digest-mismatch"
            else "loom.dev/trusted-release-sha256"
        )
        service["metadata"]["annotations"][annotation] = "a" * 64
    elif mutation.startswith("flag-"):
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        environment = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        flag = next(
            item for item in environment if item["name"] == "LOOM_SVC_DEV_INSTANCES_ENABLED"
        )
        if mutation == "flag-missing":
            environment.remove(flag)
        elif mutation == "flag-malformed":
            flag["value"] = "FALSE"
        else:
            flag["value"] = "true"
    elif mutation == "activation-nonzero":
        _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-activation-agent",
        )["spec"]["replicas"] = 1
    elif mutation == "runtime-class-missing":
        runner.responses[_RUNTIME_CLASS] = subprocess.CompletedProcess(
            list(_RUNTIME_CLASS), 1, "", "not found"
        )
    elif mutation.startswith("runtime-"):
        runtime = runner.responses[_RUNTIME_CLASS]
        assert isinstance(runtime, dict)
        _mutate_runtime_class(runtime, mutation)
    elif mutation == "scanner-pvc-missing":
        namespaced.remove(
            _item(
                runner,
                _NAMESPACED,
                "PersistentVolumeClaim",
                "loom-personal-dev-scanner-cache",
            )
        )
    elif mutation == "unexpected-personal-namespace":
        namespaces.append(
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "loom-dev-alice"}}
        )
    elif mutation == "unexpected-builder-namespace":
        namespaces.append(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "loom-build-attempt"},
            }
        )
    elif mutation == "cluster-binding-drift":
        binding = _item(
            runner,
            _CLUSTER,
            "ClusterRoleBinding",
            "loom-personal-dev-management-mutation",
        )
        binding["roleRef"]["name"] = "cluster-admin"
    elif mutation == "manager-unavailable":
        runner.responses[_MANAGER] = subprocess.CompletedProcess(
            list(_MANAGER), 1, "", "probe failed"
        )
    elif mutation == "manager-not-ready":
        runner.responses[_MANAGER] = '{"executable_new_capacity_ceiling":0,"status":"not-ready"}\n'
    elif mutation == "manager-malformed":
        runner.responses[_MANAGER] = '{"status":"ready"}\n'
    elif mutation == "manager-nonzero":
        runner.responses[_MANAGER] = '{"executable_new_capacity_ceiling":1,"status":"ready"}\n'
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    first = _observe(expected, runner)
    second = _observe(expected, runner)

    assert first.ready is False
    assert blocker in first.blockers
    assert first.blockers == tuple(sorted(set(first.blockers)))
    assert second.to_dict() == first.to_dict()
    if blocker == "resource_digest_drift":
        assert first.input_sha256 is None
        assert first.release_sha256 is None
    components = {component.name: component for component in first.components}
    if mutation in {"unexpected-personal-namespace", "unexpected-builder-namespace"}:
        assert components["namespaces"].ready is False
    if mutation in {"manager-nonzero", "manager-not-ready"}:
        assert components["manager"].observed == 1


@pytest.mark.parametrize("drift", ["rogue-pod", "generated-pvc-spec"])
def test_observer_rejects_untrusted_generated_resource_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    if drift == "rogue-pod":
        rogue = copy.deepcopy(next(item for item in items if item["kind"] == "Pod"))
        rogue["metadata"]["name"] = "rogue-managed-pod"
        rogue["metadata"]["labels"]["app"] = "rogue-managed-pod"
        items.append(rogue)
    else:
        generated = next(
            item
            for item in items
            if item["kind"] == "PersistentVolumeClaim"
            and item["metadata"]["name"].startswith("data-")
        )
        generated["spec"]["resources"]["requests"]["storage"] = "999Gi"

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


def test_observer_accepts_bounded_successful_migration_history(tmp_path: Path) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job = next(item for item in items if item["kind"] == "Job")
    current_pod = next(
        item
        for item in items
        if item["kind"] == "Pod"
        and item["metadata"]["labels"].get("app") == "loom-personal-dev-migration"
    )
    historical_job = copy.deepcopy(current_job)
    historical_pod = copy.deepcopy(current_pod)
    historical_input = "a" * 64
    historical_release = "b" * 64
    historical_name = f"loom-personal-dev-migrate-{historical_input[:16]}-{historical_release[:16]}"

    historical_job["metadata"]["name"] = historical_name
    historical_pod["metadata"]["name"] = f"{historical_name}-abcde"
    historical_pod["metadata"]["labels"]["job-name"] = historical_name
    for metadata in (
        historical_job["metadata"],
        historical_job["spec"]["template"]["metadata"],
        historical_pod["metadata"],
    ):
        metadata["labels"]["loom.dev/render-input"] = historical_input[:32]
        metadata["labels"]["loom.dev/trusted-release"] = historical_release[:32]
        metadata["annotations"]["loom.dev/render-input-sha256"] = historical_input
        metadata["annotations"]["loom.dev/trusted-release-sha256"] = historical_release
    historical_pod["spec"] = copy.deepcopy(historical_job["spec"]["template"]["spec"])
    items.extend([historical_job, historical_pod])

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()
    components = {component.name: component for component in result.components}
    assert components["namespaced-resources"].observed == 30


@pytest.mark.parametrize(
    "invalid",
    [
        "duplicate",
        "unknown-shape",
        "missing-api-version",
        "deep-json",
        "oversized",
        "combined-output",
        "nonfinite",
    ],
)
def test_observer_rejects_duplicate_unknown_or_oversized_inventory(
    tmp_path: Path,
    invalid: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    if invalid == "duplicate":
        items = _items(runner, _NAMESPACED)
        items.append(copy.deepcopy(items[0]))
    elif invalid == "unknown-shape":
        document = runner.responses[_NAMESPACED]
        assert isinstance(document, dict)
        document["unexpected"] = True
    elif invalid == "missing-api-version":
        document = runner.responses[_NAMESPACED]
        assert isinstance(document, dict)
        del document["apiVersion"]
    elif invalid == "deep-json":
        runner.responses[_NAMESPACED] = (
            '{"apiVersion":"v1","items":' + "[" * 1100 + "0" + "]" * 1100 + ',"kind":"List"}'
        )
    elif invalid == "oversized":
        runner.responses[_NAMESPACED] = "x" * (4 * 1024 * 1024 + 1)
    elif invalid == "combined-output":
        document = runner.responses[_NAMESPACED]
        assert isinstance(document, dict)
        runner.responses[_NAMESPACED] = subprocess.CompletedProcess(
            list(_NAMESPACED),
            0,
            json.dumps(document, sort_keys=True, separators=(",", ":")),
            "x" * (4 * 1024 * 1024),
        )
    else:
        pod = next(item for item in _items(runner, _NAMESPACED) if item["kind"] == "Pod")
        pod["status"]["ignored"] = float("nan")

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_invalid" in result.blockers


def test_observer_rejects_invalid_namespace_and_expected_render_inputs(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)

    with pytest.raises(ValueError, match="namespace"):
        observe_personal_dev_shadow_status(
            runner,
            expected=expected,
            namespace="loom-dev-alice",
        )
    with pytest.raises(TypeError, match="expected render"):
        observe_personal_dev_shadow_status(
            runner,
            expected=object(),  # type: ignore[arg-type]
            namespace="loom-dev",
        )


def test_observer_starts_no_call_when_less_than_one_second_remains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    module = importlib.import_module("loom.personal_dev_control_plane_status")
    ticks = iter([100.0, 159.5, 160.0, 160.0, 160.0, 160.0, 160.0])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(ticks))

    result = _observe(expected, runner)

    assert runner.calls == []
    assert result.ready is False
    assert "kube_context_invalid" in result.blockers
