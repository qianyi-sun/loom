from __future__ import annotations

import copy
import hashlib
import importlib
import json
import subprocess
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlan,
    PersonalDevAcceptancePlanError,
    PersonalDevOperationalPlan,
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_operational_plan,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    RenderedPersonalDevControlPlane,
    render_acceptance_personal_dev_control_plane,
    render_operational_personal_dev_control_plane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    PersonalDevAcceptanceStatus,
    PersonalDevOperationalStatus,
    observe_personal_dev_acceptance_status,
    observe_personal_dev_operational_status,
    observe_personal_dev_shadow_status,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
_MANAGED_BY = "loom-personal-dev-control-plane"
_NOW = datetime(2026, 8, 17, 21, 0, 0, tzinfo=UTC)
_STATEFULSET_UIDS = {
    "loom-dev-postgres": "00000000-0000-0000-0000-000000000101",
    "loom-dev-minio": "00000000-0000-0000-0000-000000000102",
}
_MIGRATION_JOB_UID = "00000000-0000-0000-0000-000000000103"
_NATIVE_AGENT_INSTANCE_ID = "10000000-0000-0000-0000-000000000001"
_NATIVE_HOST_BOOT_ID = "20000000-0000-0000-0000-000000000001"
_NATIVE_AGENT_KEY_ID = "gb10-native-builder-v1"
_NATIVE_PUBLIC_KEY_SHA256 = "c" * 64
_NATIVE_RUNTIME_PROFILE_SHA256 = "d" * 64
_NATIVE_PUBLIC_STORE_ORIGIN = "https://minio.dev.yylx.world"
_NATIVE_PUBLIC_STORE_ENDPOINT_CIDRS = ("207.35.188.227/32",)
_NATIVE_AGENT_IMAGE = (
    "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "c" * 64
)

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
_NATIVE_BUILDER = (
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
    "loom_personal_dev_native_builder_probe",
)
_DEPLOYMENTS = (
    "get",
    "deployments.apps",
    "--all-namespaces",
    "--output=json",
)


def _release_value() -> dict[str, object]:
    return {
        "schema_version": 4,
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "images": {
            "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
            "loom_web": "ghcr.io/qianyi-sun/loom-web@sha256:" + "b" * 64,
            "personal_dev_builder": (
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
            ),
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
            ),
            "personal_dev_native_builder_agent": _NATIVE_AGENT_IMAGE,
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
                "35f7d0f279b656552b1eb362a0599938ff112e5103590dcfc0eece25e8326082"
            ),
            "database_metadata_sha256": "c" * 64,
            "database_sha256": "d" * 64,
            "java_database_metadata_sha256": "e" * 64,
            "java_database_sha256": "f" * 64,
            "lock_sha256": "1" * 64,
            "trivy_version": "v0.74.0",
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
            + ("\n" if command == _NATIVE_BUILDER else "")
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
        pod["metadata"]["labels"]["batch.kubernetes.io/job-name"] = item["metadata"]["name"]
        pod["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "batch/v1",
                "blockOwnerDeletion": True,
                "controller": True,
                "kind": "Job",
                "name": item["metadata"]["name"],
                "uid": item["metadata"]["uid"],
            }
        ]
    elif item["kind"] == "StatefulSet":
        pod["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "apps/v1",
                "blockOwnerDeletion": True,
                "controller": True,
                "kind": "StatefulSet",
                "name": item["metadata"]["name"],
                "uid": item["metadata"]["uid"],
            }
        ]
        pod["spec"]["volumes"] = [
            {
                "name": claim_template["metadata"]["name"],
                "persistentVolumeClaim": {
                    "claimName": (
                        f"{claim_template['metadata']['name']}-{pod['metadata']['name']}"
                    ),
                },
            }
            for claim_template in item["spec"]["volumeClaimTemplates"]
        ] + pod["spec"]["volumes"]
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
        runtime["scheduling"]["nodeSelector"]["loom.dev/personal-dev-runtime-profile-a"] = "a" * 32
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
            metadata["uid"] = _STATEFULSET_UIDS[name]
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
        elif kind == "Deployment" and name in {
            "loom-personal-dev-management",
            "loom-personal-dev-web",
        }:
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
            metadata["uid"] = _MIGRATION_JOB_UID
            item["status"] = {
                "active": 0,
                "failed": 0,
                "succeeded": 1,
                "conditions": [{"type": "Complete", "status": "True"}],
            }
            migration_pod = _pod_for(item, "abcde", phase="Succeeded")
            _materialize_migration_api_defaults(item, migration_pod)
            generated.append(migration_pod)
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
        _DEPLOYMENTS: {
            "apiVersion": "v1",
            "kind": "List",
            "items": [item for item in namespaced if item["kind"] == "Deployment"],
        },
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


def _prepared_profile() -> Any:
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    assert profile.native_builder is not None
    return replace(
        profile,
        native_builder=replace(
            profile.native_builder,
            prepared=True,
            agent_instance_id=_NATIVE_AGENT_INSTANCE_ID,
            agent_key_id=_NATIVE_AGENT_KEY_ID,
            public_key_sha256=_NATIVE_PUBLIC_KEY_SHA256,
            host_name="gx10-01c7",
            runtime_profile_sha256=_NATIVE_RUNTIME_PROFILE_SHA256,
            public_store_origin=_NATIVE_PUBLIC_STORE_ORIGIN,
            public_store_endpoint_cidrs=_NATIVE_PUBLIC_STORE_ENDPOINT_CIDRS,
        ),
    )


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
    profile = _prepared_profile()
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
            "scanner_database_metadata_sha256": (release.scanner.database_metadata_sha256),
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
        "native_builder": {
            "agent_instance_id": _NATIVE_AGENT_INSTANCE_ID,
            "agent_key_id": _NATIVE_AGENT_KEY_ID,
            "freshness_seconds": 60,
            "host_boot_id": _NATIVE_HOST_BOOT_ID,
            "host_name": "gx10-01c7",
            "max_concurrency": 2,
            "platform": "linux/arm64",
            "protocol_version": 1,
            "provider": "gb10-gvisor-docker-v1",
            "public_key_sha256": _NATIVE_PUBLIC_KEY_SHA256,
            "public_store_origin": _NATIVE_PUBLIC_STORE_ORIGIN,
            "public_store_endpoint_cidrs": list(
                _NATIVE_PUBLIC_STORE_ENDPOINT_CIDRS
            ),
            "runtime_profile_sha256": _NATIVE_RUNTIME_PROFILE_SHA256,
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
        "schema_version": 3,
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "storage": {
            "backup_restore_evidence_sha256": "b" * 64,
            "schema_head": "0128",
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


def _operational_inputs(
    tmp_path: Path,
) -> tuple[RenderedPersonalDevControlPlane, PersonalDevOperationalPlan]:
    _expected, acceptance = _acceptance_inputs(tmp_path)
    value = acceptance.canonical_value()
    value.pop("acceptance_owners")
    value.pop("window")
    value["schema_version"] = 2
    value["approval"] = {
        "acceptance_result_sha256": "4" * 64,
        "approved_at": "2026-08-17T20:00:00Z",
        "rollback_evidence_sha256": "5" * 64,
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    plan_path = tmp_path / "operational-plan.json"
    plan_path.write_bytes(payload)
    plan_path.chmod(0o600)
    plan = load_personal_dev_operational_plan(
        plan_path,
        hashlib.sha256(payload).hexdigest(),
    )
    release_path = tmp_path / "acceptance-trusted-release.json"
    release_payload = release_path.read_bytes()
    release = load_personal_dev_trusted_release(
        release_path,
        hashlib.sha256(release_payload).hexdigest(),
    )
    profile = _prepared_profile()
    return (
        render_operational_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=_NOW,
        ),
        plan,
    )


def _enabled_healthy_runner(
    expected: RenderedPersonalDevControlPlane,
    plan: PersonalDevAcceptancePlan | PersonalDevOperationalPlan,
) -> _FakeRunner:
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
            metadata["uid"] = _STATEFULSET_UIDS[name]
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
            metadata["uid"] = _MIGRATION_JOB_UID
            item["status"] = {
                "active": 0,
                "failed": 0,
                "succeeded": 1,
                "conditions": [{"type": "Complete", "status": "True"}],
            }
            migration_pod = _pod_for(item, "abcde", phase="Succeeded")
            _materialize_migration_api_defaults(item, migration_pod)
            generated.append(migration_pod)
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
        _NATIVE_BUILDER: _native_builder_status(plan),
        _DEPLOYMENTS: {
            "apiVersion": "v1",
            "kind": "List",
            "items": deployments,
        },
    }
    return _FakeRunner(responses)


def _native_builder_status(
    plan: PersonalDevAcceptancePlan | PersonalDevOperationalPlan,
) -> dict[str, object]:
    native = plan.native_builder
    assert native is not None
    agent_image = plan.release.images.personal_dev_native_builder_agent
    assert agent_image is not None
    return {
        "agent": {
            "active_grant_ids": [],
            "agent_image": agent_image,
            "agent_instance_id": str(native.agent_instance_id),
            "agent_key_id": native.agent_key_id,
            "available": True,
            "builder_image": plan.release.images.personal_dev_builder,
            "host_architecture": "aarch64",
            "host_boot_id": str(native.host_boot_id),
            "host_name": native.host_name,
            "last_seen_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace(
                "+00:00", "Z"
            ),
            "managed_grant_ids": [],
            "max_concurrency": native.max_concurrency,
            "platform": native.platform,
            "protocol_version": native.protocol_version,
            "provider": native.provider,
            "readiness_evidence_sha256": "e" * 64,
            "runtime_profile_sha256": native.runtime_profile_sha256,
            "unavailable_reason": None,
        },
        "schema": "loom-personal-dev-native-builder-agent-status-v1",
    }


def _acceptance_healthy_fixture(
    tmp_path: Path,
) -> tuple[RenderedPersonalDevControlPlane, PersonalDevAcceptancePlan, _FakeRunner]:
    expected, plan = _acceptance_inputs(tmp_path)
    return expected, plan, _enabled_healthy_runner(expected, plan)


def _operational_healthy_fixture(
    tmp_path: Path,
) -> tuple[RenderedPersonalDevControlPlane, PersonalDevOperationalPlan, _FakeRunner]:
    expected, plan = _operational_inputs(tmp_path)
    return expected, plan, _enabled_healthy_runner(expected, plan)


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


def _pod_by_app(runner: _FakeRunner, app: str) -> dict[str, Any]:
    return next(
        item
        for item in _items(runner, _NAMESPACED)
        if item["kind"] == "Pod" and item["metadata"]["labels"].get("app") == app
    )


def _native_agent(runner: _FakeRunner) -> dict[str, Any]:
    document = runner.responses[_NATIVE_BUILDER]
    assert isinstance(document, dict)
    agent = document["agent"]
    assert isinstance(agent, dict)
    return agent


def _with_statefulset_ordinals(
    expected: RenderedPersonalDevControlPlane,
    statefulset_name: str,
    *,
    replicas: int,
    start: int,
) -> RenderedPersonalDevControlPlane:
    documents = [copy.deepcopy(item) for item in yaml.safe_load_all(expected.yaml_text)]
    statefulset = next(
        item for item in documents if _identity(item) == ("StatefulSet", statefulset_name)
    )
    statefulset["spec"]["replicas"] = replicas
    statefulset["spec"]["ordinals"] = {"start": start}
    return replace(
        expected,
        yaml_text=yaml.safe_dump_all(
            documents,
            explicit_start=True,
            sort_keys=False,
        ),
    )


def _with_extra_statefulset_claim_template(
    expected: RenderedPersonalDevControlPlane,
    statefulset_name: str,
    claim_name: str,
) -> RenderedPersonalDevControlPlane:
    documents = [copy.deepcopy(item) for item in yaml.safe_load_all(expected.yaml_text)]
    statefulset = next(
        item for item in documents if _identity(item) == ("StatefulSet", statefulset_name)
    )
    claim_template = copy.deepcopy(statefulset["spec"]["volumeClaimTemplates"][0])
    claim_template["metadata"]["name"] = claim_name
    statefulset["spec"]["volumeClaimTemplates"].append(claim_template)
    return replace(
        expected,
        yaml_text=yaml.safe_dump_all(
            documents,
            explicit_start=True,
            sort_keys=False,
        ),
    )


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


def _observe_operational(
    expected: RenderedPersonalDevControlPlane,
    plan: PersonalDevOperationalPlan,
    runner: _FakeRunner,
) -> PersonalDevOperationalStatus:
    return observe_personal_dev_operational_status(
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
            {"name": "namespaced-resources", "observed": 37, "ready": True},
            {"name": "namespaces", "observed": 1, "ready": True},
            {"name": "native-builder", "observed": 1, "ready": True},
            {"name": "personal-workers", "observed": 0, "ready": True},
            {"name": "runtime-class", "observed": 1, "ready": True},
            {"name": "web", "observed": 1, "ready": True},
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
        _NATIVE_BUILDER,
        _DEPLOYMENTS,
    ]
    assert all(1 <= timeout <= 10 for _call, timeout in runner.calls)
    assert sum(call == _DEPLOYMENTS for call, _timeout in runner.calls) == 1
    for command, _timeout in runner.calls:
        assert "secret" not in " ".join(command).casefold()
        assert command[0] in {"config", "get", "--request-timeout=10s"}


def test_acceptance_status_requires_one_fresh_native_agent(tmp_path: Path) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    runner.responses[_NATIVE_BUILDER] = {
        "agent": None,
        "schema": "loom-personal-dev-native-builder-agent-status-v1",
    }

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert result.application_ready is False
    assert result.capacity_publication_ready is True
    assert result.worker_available is False
    assert result.manager_ceiling == 0
    assert "native_builder_agent_stale" in result.blockers


def test_acceptance_status_reports_management_native_provider_disabled(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    management = _item(
        runner,
        _NAMESPACED,
        "Deployment",
        "loom-personal-dev-management",
    )
    environment = management["spec"]["template"]["spec"]["containers"][0]["env"]
    enabled = next(
        entry
        for entry in environment
        if entry["name"] == "LOOM_SVC_PERSONAL_DEV_NATIVE_BUILDER_ENABLED"
    )
    enabled["value"] = "false"

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert result.application_ready is False
    assert "native_builder_disabled" in result.blockers


@pytest.mark.parametrize("offset_seconds", [-61, 300])
def test_acceptance_status_rejects_stale_or_future_native_agent(
    tmp_path: Path,
    offset_seconds: int,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    observed = datetime.now(UTC) + timedelta(seconds=offset_seconds)
    _native_agent(runner)["last_seen_at"] = observed.replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "native_builder_agent_stale" in result.blockers


@pytest.mark.parametrize(
    ("field", "value", "blocker"),
    [
        ("agent_instance_id", "10000000-0000-0000-0000-000000000002", "native_builder_identity_mismatch"),
        ("agent_key_id", "gb10-native-builder-v2", "native_builder_identity_mismatch"),
        ("host_name", "gx10-ffff", "native_builder_identity_mismatch"),
        ("host_architecture", "x86_64", "native_builder_identity_mismatch"),
        ("host_boot_id", "20000000-0000-0000-0000-000000000002", "native_builder_identity_mismatch"),
        ("provider", "wrong-provider", "native_builder_identity_mismatch"),
        ("platform", "linux/amd64", "native_builder_identity_mismatch"),
        ("protocol_version", 2, "native_builder_identity_mismatch"),
        ("agent_image", "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "f" * 64, "native_builder_inventory_drift"),
        ("builder_image", "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "f" * 64, "native_builder_inventory_drift"),
        ("runtime_profile_sha256", "f" * 64, "native_builder_inventory_drift"),
        ("max_concurrency", 1, "native_builder_inventory_drift"),
        ("readiness_evidence_sha256", "0" * 64, "native_builder_inventory_drift"),
    ],
)
def test_acceptance_status_rejects_native_identity_or_inventory_drift(
    tmp_path: Path,
    field: str,
    value: object,
    blocker: str,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    _native_agent(runner)[field] = value

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert blocker in result.blockers


def test_acceptance_status_rejects_unknown_inactive_managed_grant(tmp_path: Path) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    _native_agent(runner)["managed_grant_ids"] = [
        "30000000-0000-0000-0000-000000000001"
    ]

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "native_builder_inventory_drift" in result.blockers


def test_acceptance_status_allows_exact_active_native_grants(tmp_path: Path) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    grant_id = "30000000-0000-0000-0000-000000000001"
    agent = _native_agent(runner)
    agent["managed_grant_ids"] = [grant_id]
    agent["active_grant_ids"] = [grant_id]

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is True
    assert "native_builder_inventory_drift" not in result.blockers


@pytest.mark.parametrize(
    ("reason", "blocker"),
    [
        ("host_runtime_unavailable", "native_builder_disabled"),
        ("public_store_unavailable", "native_builder_public_store_unavailable"),
    ],
)
def test_acceptance_status_maps_native_unavailability_to_exact_blocker(
    tmp_path: Path,
    reason: str,
    blocker: str,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    agent = _native_agent(runner)
    agent["available"] = False
    agent["unavailable_reason"] = reason

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert blocker in result.blockers


@pytest.mark.parametrize("payload", ["{}", "{" + "x" * 65536])
def test_acceptance_status_rejects_malformed_or_oversized_native_probe(
    tmp_path: Path,
    payload: str,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    runner.responses[_NATIVE_BUILDER] = subprocess.CompletedProcess(
        list(_NATIVE_BUILDER),
        0,
        payload,
        "",
    )

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "native_builder_identity_mismatch" in result.blockers


def test_acceptance_status_maps_probe_inventory_exit_to_drift(tmp_path: Path) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    runner.responses[_NATIVE_BUILDER] = subprocess.CompletedProcess(
        list(_NATIVE_BUILDER),
        3,
        "",
        "personal-dev native-builder probe failed\n",
    )

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "native_builder_inventory_drift" in result.blockers


def test_acceptance_status_maps_probe_public_store_exit_to_unavailable(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    runner.responses[_NATIVE_BUILDER] = subprocess.CompletedProcess(
        list(_NATIVE_BUILDER),
        4,
        "",
        "personal-dev native-builder probe failed\n",
    )

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "native_builder_public_store_unavailable" in result.blockers


def test_acceptance_plan_v2_parses_but_cannot_render_enabled_status(
    tmp_path: Path,
) -> None:
    _expected, native_plan = _acceptance_inputs(tmp_path)
    value = native_plan.canonical_value()
    value["schema_version"] = 2
    value.pop("native_builder")
    release_path = tmp_path / "acceptance-trusted-release.json"
    release_payload = release_path.read_bytes()
    release = load_personal_dev_trusted_release(
        release_path,
        hashlib.sha256(release_payload).hexdigest(),
    )
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    path = tmp_path / "acceptance-plan-v2.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    plan = load_personal_dev_acceptance_plan(path, hashlib.sha256(payload).hexdigest())

    assert plan.schema_version == 2
    assert plan.native_builder is None

    with pytest.raises(PersonalDevAcceptancePlanError):
        render_acceptance_personal_dev_control_plane(
            _prepared_profile(),
            release,
            plan,
            now=_NOW,
        )


def test_healthy_operational_is_durable_and_zero_capacity(tmp_path: Path) -> None:
    expected, plan, runner = _operational_healthy_fixture(tmp_path)

    result = _observe_operational(expected, plan, runner)

    assert isinstance(result, PersonalDevOperationalStatus)
    assert result.ready is True
    assert result.blockers == ()
    assert result.application_ready is True
    assert result.capacity_publication_ready is True
    assert result.manager_ceiling == 0
    assert result.worker_available is False
    assert result.to_dict()["mode"] == "operational"
    assert result.to_dict()["operational_plan_sha256"] == plan.sha256
    assert [call for call, _timeout in runner.calls] == [
        _CONTEXT,
        _NAMESPACES,
        _RUNTIME_CLASS,
        _NAMESPACED,
        _CLUSTER,
        _ACCEPTANCE_MANAGER,
        _NATIVE_BUILDER,
        _DEPLOYMENTS,
    ]


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


def test_status_accepts_exact_statefulset_controller_claim_volumes(
    tmp_path: Path,
) -> None:
    shadow_expected, shadow_runner = _healthy_fixture(tmp_path)
    acceptance_expected, plan, acceptance_runner = _acceptance_healthy_fixture(tmp_path)

    shadow_status = _observe(shadow_expected, shadow_runner)
    acceptance_status = _observe_acceptance(acceptance_expected, plan, acceptance_runner)

    assert shadow_status.ready is True
    assert shadow_status.blockers == ()
    assert acceptance_status.ready is True
    assert acceptance_status.blockers == ()


def test_status_rejects_missing_statefulset_controller_claim_volumes(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    for pod in _items(runner, _NAMESPACED):
        if pod["kind"] != "Pod" or pod["metadata"]["labels"].get("app") not in {
            "loom-dev-postgres",
            "loom-dev-minio",
        }:
            continue
        pod["spec"]["volumes"] = [
            volume for volume in pod["spec"]["volumes"] if volume["name"] != "data"
        ]

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


def test_status_accepts_exact_statefulset_claim_volume_after_declared_volumes(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    postgres_pod = _pod_by_app(runner, "loom-dev-postgres")
    claim_volume = postgres_pod["spec"]["volumes"].pop(0)
    assert claim_volume["name"] == "data"
    postgres_pod["spec"]["volumes"].append(claim_volume)

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "wrong-api-version",
        "wrong-kind",
        "wrong-name",
        "wrong-uid",
        "not-controller",
        "not-blocking-deletion",
        "extra-owner",
    ],
)
def test_status_rejects_statefulset_pod_without_exact_controller_owner(
    tmp_path: Path,
    mutation: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    postgres_pod = _pod_by_app(runner, "loom-dev-postgres")
    owner_references = postgres_pod["metadata"]["ownerReferences"]
    owner = owner_references[0]

    if mutation == "missing":
        del postgres_pod["metadata"]["ownerReferences"]
    elif mutation == "wrong-api-version":
        owner["apiVersion"] = "v1"
    elif mutation == "wrong-kind":
        owner["kind"] = "Deployment"
    elif mutation == "wrong-name":
        owner["name"] = "loom-dev-minio"
    elif mutation == "wrong-uid":
        owner["uid"] = _STATEFULSET_UIDS["loom-dev-minio"]
    elif mutation == "not-controller":
        owner["controller"] = False
    elif mutation == "not-blocking-deletion":
        owner["blockOwnerDeletion"] = False
    elif mutation == "extra-owner":
        owner_references.append(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "name": "unrelated",
                "uid": "00000000-0000-0000-0000-000000000103",
            }
        )
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


def test_status_accepts_statefulset_nonzero_start_and_multiple_replicas(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    expected = _with_statefulset_ordinals(
        expected,
        "loom-dev-postgres",
        replicas=2,
        start=1,
    )
    items = _items(runner, _NAMESPACED)
    statefulset = _item(runner, _NAMESPACED, "StatefulSet", "loom-dev-postgres")
    statefulset["spec"]["replicas"] = 2
    statefulset["spec"]["ordinals"] = {"start": 1}
    for field in ("replicas", "currentReplicas", "readyReplicas", "updatedReplicas"):
        statefulset["status"][field] = 2

    first_pod = _pod_by_app(runner, "loom-dev-postgres")
    first_pod["metadata"]["name"] = "loom-dev-postgres-1"
    first_pod["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = (
        "data-loom-dev-postgres-1"
    )
    second_pod = copy.deepcopy(first_pod)
    second_pod["metadata"]["name"] = "loom-dev-postgres-2"
    second_pod["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = (
        "data-loom-dev-postgres-2"
    )

    first_claim = next(
        item
        for item in items
        if _identity(item) == ("PersistentVolumeClaim", "data-loom-dev-postgres-0")
    )
    first_claim["metadata"]["name"] = "data-loom-dev-postgres-1"
    second_claim = copy.deepcopy(first_claim)
    second_claim["metadata"]["name"] = "data-loom-dev-postgres-2"
    items.extend([second_pod, second_claim])

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()


@pytest.mark.parametrize(
    ("mutation", "expected_ready"),
    [
        ("exact", True),
        ("missing", False),
        ("duplicate", False),
        ("wrong-source", False),
    ],
)
def test_status_requires_every_exact_statefulset_claim_template_volume(
    tmp_path: Path,
    mutation: str,
    expected_ready: bool,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    expected = _with_extra_statefulset_claim_template(
        expected,
        "loom-dev-postgres",
        "cache",
    )
    items = _items(runner, _NAMESPACED)
    statefulset = _item(runner, _NAMESPACED, "StatefulSet", "loom-dev-postgres")
    claim_template = copy.deepcopy(statefulset["spec"]["volumeClaimTemplates"][0])
    claim_template["metadata"]["name"] = "cache"
    statefulset["spec"]["volumeClaimTemplates"].append(claim_template)

    postgres_pod = _pod_by_app(runner, "loom-dev-postgres")
    cache_volume = {
        "name": "cache",
        "persistentVolumeClaim": {"claimName": "cache-loom-dev-postgres-0"},
    }
    postgres_pod["spec"]["volumes"].insert(1, cache_volume)
    data_claim = next(
        item
        for item in items
        if _identity(item) == ("PersistentVolumeClaim", "data-loom-dev-postgres-0")
    )
    cache_claim = copy.deepcopy(data_claim)
    cache_claim["metadata"]["name"] = "cache-loom-dev-postgres-0"
    items.append(cache_claim)

    if mutation == "missing":
        postgres_pod["spec"]["volumes"].remove(cache_volume)
    elif mutation == "duplicate":
        postgres_pod["spec"]["volumes"].insert(2, copy.deepcopy(cache_volume))
    elif mutation == "wrong-source":
        cache_volume["persistentVolumeClaim"]["claimName"] = "cache-loom-dev-minio-0"
    elif mutation != "exact":  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    result = _observe(expected, runner)

    assert result.ready is expected_ready
    assert ("resource_inventory_drift" in result.blockers) is not expected_ready


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong-claim-name",
        "extra-claim-source-field",
        "duplicate-claim-volume",
        "unknown-volume",
        "reordered-declared-volumes",
        "unexpected-statefulset-ordinal",
        "deployment-lookalike-volume",
        "job-lookalike-volume",
    ],
)
def test_status_rejects_untrusted_statefulset_claim_volume_normalization(
    tmp_path: Path,
    mutation: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    postgres_pod = _pod_by_app(runner, "loom-dev-postgres")
    claim_volume = postgres_pod["spec"]["volumes"][0]
    assert claim_volume["name"] == "data"

    if mutation == "wrong-claim-name":
        claim_volume["persistentVolumeClaim"]["claimName"] = "data-loom-dev-minio-0"
    elif mutation == "extra-claim-source-field":
        claim_volume["persistentVolumeClaim"]["readOnly"] = False
    elif mutation == "duplicate-claim-volume":
        postgres_pod["spec"]["volumes"].insert(1, copy.deepcopy(claim_volume))
    elif mutation == "unknown-volume":
        postgres_pod["spec"]["volumes"].append({"name": "unknown", "emptyDir": {}})
    elif mutation == "reordered-declared-volumes":
        postgres_pod["spec"]["volumes"][1:] = reversed(postgres_pod["spec"]["volumes"][1:])
    elif mutation == "unexpected-statefulset-ordinal":
        postgres_pod["metadata"]["name"] = "loom-dev-postgres-1"
        claim_volume["persistentVolumeClaim"]["claimName"] = "data-loom-dev-postgres-1"
    elif mutation == "deployment-lookalike-volume":
        management_pod = _pod_by_app(runner, "loom-personal-dev-management")
        management_pod["spec"]["volumes"].insert(
            0,
            {
                "name": "data",
                "persistentVolumeClaim": {
                    "claimName": f"data-{management_pod['metadata']['name']}",
                },
            },
        )
    elif mutation == "job-lookalike-volume":
        migration_pod = _pod_by_app(runner, "loom-personal-dev-migration")
        migration_pod["spec"]["volumes"].insert(
            0,
            {
                "name": "data",
                "persistentVolumeClaim": {
                    "claimName": f"data-{migration_pod['metadata']['name']}",
                },
            },
        )
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


def test_status_accepts_api_server_default_empty_admission_selectors(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    policies = [
        item for item in _items(runner, _CLUSTER) if item["kind"] == "ValidatingAdmissionPolicy"
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
        ("management-ingress-default-backend", "resource_inventory_drift"),
        ("management-ingress-snippet", "resource_inventory_drift"),
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
            item for item in _items(runner, _CLUSTER) if item["kind"] == "ValidatingAdmissionPolicy"
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
    elif mutation == "management-ingress-default-backend":
        ingress = _item(
            runner,
            _NAMESPACED,
            "Ingress",
            "loom-personal-dev-management",
        )
        ingress["spec"]["defaultBackend"] = {
            "service": {"name": "loom-dev-postgres", "port": {"number": 5432}}
        }
    elif mutation == "management-ingress-snippet":
        ingress = _item(
            runner,
            _NAMESPACED,
            "Ingress",
            "loom-personal-dev-management",
        )
        ingress["metadata"]["annotations"]["nginx.ingress.kubernetes.io/server-snippet"] = (
            "return 418;"
        )
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


def _exact_builder_namespace() -> dict[str, object]:
    name = f"loom-build-{'a' * 32}-l{'b' * 16}"
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": {
                "app.kubernetes.io/managed-by": ("loom-personal-dev-builder-controller"),
                "app.kubernetes.io/part-of": "loom",
                "kubernetes.io/metadata.name": name,
                "loom.dev/candidate": "c" * 12,
                "loom.dev/subject": "00000000-0000-0000-0000-000000000402",
                "loom.dev/incarnation": "00000000-0000-0000-0000-000000000403",
                "loom.dev/operation": "00000000-0000-0000-0000-000000000404",
                "loom.dev/attempt": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "loom.dev/operation-epoch": "1",
                "loom.dev/build-attempt-sequence": "0",
                "loom.dev/build-lease-epoch": "1",
                "pod-security.kubernetes.io/enforce": "privileged",
                "pod-security.kubernetes.io/enforce-version": "v1.36",
                "pod-security.kubernetes.io/audit": "restricted",
                "pod-security.kubernetes.io/audit-version": "v1.36",
                "pod-security.kubernetes.io/warn": "restricted",
                "pod-security.kubernetes.io/warn-version": "v1.36",
            },
        },
    }


def test_acceptance_permits_only_exact_owned_dynamic_namespace_families(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    namespaces = _items(runner, _NAMESPACES)
    personal_subject = "00000000-0000-0000-0000-000000000401"
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
            _exact_builder_namespace(),
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
    ("label", "drifted_value"),
    [
        ("pod-security.kubernetes.io/enforce", "baseline"),
        ("pod-security.kubernetes.io/enforce-version", "latest"),
        ("pod-security.kubernetes.io/audit", "baseline"),
        ("pod-security.kubernetes.io/audit-version", "latest"),
        ("pod-security.kubernetes.io/warn", "baseline"),
        ("pod-security.kubernetes.io/warn-version", "latest"),
    ],
)
def test_acceptance_rejects_builder_namespace_pod_security_drift(
    tmp_path: Path,
    label: str,
    drifted_value: str,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    namespace = _exact_builder_namespace()
    namespace["metadata"]["labels"][label] = drifted_value  # type: ignore[index]
    _items(runner, _NAMESPACES).append(namespace)

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "builder_namespace_invalid" in result.blockers


@pytest.mark.parametrize(
    "mutation",
    [
        "annotation",
        "finalizer",
        "generated-name",
        "owner-reference",
        "extra-label",
        "attempt-mismatch",
        "injected-name-mismatch",
    ],
)
def test_acceptance_rejects_builder_namespace_metadata_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    namespace = _exact_builder_namespace()
    metadata = namespace["metadata"]
    assert isinstance(metadata, dict)
    labels = metadata["labels"]
    assert isinstance(labels, dict)
    if mutation == "annotation":
        metadata["annotations"] = {"example.invalid/drift": "true"}
    elif mutation == "finalizer":
        metadata["finalizers"] = ["example.invalid/drift"]
    elif mutation == "generated-name":
        metadata["generateName"] = "loom-build-"
    elif mutation == "owner-reference":
        metadata["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "name": "other",
                "uid": "00000000-0000-0000-0000-000000000405",
            }
        ]
    elif mutation == "extra-label":
        labels["example.invalid/drift"] = "true"
    elif mutation == "attempt-mismatch":
        labels["loom.dev/attempt"] = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    else:
        labels["kubernetes.io/metadata.name"] = "loom-build-mismatch"
    _items(runner, _NAMESPACES).append(namespace)

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "builder_namespace_invalid" in result.blockers


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
        deployments["kind"] = "DeploymentList"
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
        item for item in pod["initContainers"] if item["name"] == "personal-dev-scanner-cache-init"
    )
    service = next(item for item in pod["containers"] if item["name"] == "management")

    if drift == "init-image":
        init["image"] = "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "9" * 64
    elif drift == "init-argument":
        init["args"][-1] = "9" * 64
    elif drift == "init-root-mount":
        next(mount for mount in init["volumeMounts"] if mount["name"] == "scanner-cache")[
            "mountPath"
        ] = "/tmp/scanner-cache"
    elif drift == "generation-subpath":
        next(mount for mount in service["volumeMounts"] if mount["name"] == "scanner-cache")[
            "subPath"
        ] = "generations/" + "9" * 64
    elif drift == "cache-path":
        next(
            entry
            for entry in service["env"]
            if entry["name"] == "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_DIR"
        )["value"] = "/var/lib/loom-personal-dev-scanner"
    elif drift == "fanal-limit":
        next(volume for volume in pod["volumes"] if volume["name"] == "scanner-fanal")["emptyDir"][
            "sizeLimit"
        ] = "8Gi"
    elif drift == "node-architecture":
        pod["nodeSelector"]["kubernetes.io/arch"] = "arm64"
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(drift)

    result = (
        _observe(expected, runner) if plan is None else _observe_acceptance(expected, plan, runner)
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
            {"name": "namespaced-resources", "observed": 34, "ready": True},
            {"name": "namespaces", "observed": 1, "ready": True},
            {"name": "personal-workers", "observed": 0, "ready": True},
            {"name": "runtime-class", "observed": 1, "ready": True},
            {"name": "web", "observed": 1, "ready": True},
        ],
        "input_sha256": expected.input_sha256,
        "manager_ceiling": 0,
        "mode": "shadow",
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
        _MANAGER,
        _DEPLOYMENTS,
    ]
    assert all(1 <= timeout <= 10 for _call, timeout in runner.calls)
    assert sum(call == _NAMESPACES for call, _timeout in runner.calls) == 1
    for command, _timeout in runner.calls:
        assert "secret" not in " ".join(command).casefold()
        assert command[0] in {"config", "get", "--request-timeout=10s"}
    assert runner.calls[-2][0] == _MANAGER
    assert runner.calls[-1][0] == _DEPLOYMENTS


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        ("personal-worker", "unexpected_personal_worker"),
        ("deployments-wrong-kind", "deployment_inventory_invalid"),
        ("deployments-wrong-api-version", "deployment_inventory_invalid"),
    ],
)
def test_shadow_status_fails_closed_on_personal_worker_inventory(
    tmp_path: Path,
    mutation: str,
    blocker: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    if mutation == "personal-worker":
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
    elif mutation == "deployments-wrong-kind":
        deployments = runner.responses[_DEPLOYMENTS]
        assert isinstance(deployments, dict)
        deployments["kind"] = "DeploymentList"
    else:
        deployments = runner.responses[_DEPLOYMENTS]
        assert isinstance(deployments, dict)
        deployments["apiVersion"] = "apps/v1"

    result = _observe(expected, runner)

    assert result.ready is False
    assert blocker in result.blockers
    assert result.worker_available is False
    component = next(item for item in result.components if item.name == "personal-workers")
    assert component.ready is False


def test_status_accepts_installed_lineage_digests_on_generated_stateful_claims(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    generated_claims = [
        item
        for item in _items(runner, _NAMESPACED)
        if item["kind"] == "PersistentVolumeClaim" and item["metadata"]["name"].startswith("data-")
    ]

    assert len(generated_claims) == 2
    assert all(
        claim["metadata"]["annotations"]["loom.dev/render-input-sha256"] != expected.input_sha256
        for claim in generated_claims
    )
    assert all(
        claim["metadata"]["annotations"]["loom.dev/trusted-release-sha256"]
        != expected.release_sha256
        for claim in generated_claims
    )

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()


def test_shadow_status_rejects_plan_digest_on_generated_stateful_claim(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    generated_claim = next(
        item
        for item in _items(runner, _NAMESPACED)
        if item["kind"] == "PersistentVolumeClaim" and item["metadata"]["name"].startswith("data-")
    )
    generated_claim["metadata"]["labels"]["loom.dev/acceptance-plan-sha256"] = "a" * 32
    generated_claim["metadata"]["annotations"]["loom.dev/acceptance-plan-sha256"] = "a" * 64

    result = _observe(expected, runner)

    assert result.ready is False
    assert "acceptance_plan_digest_drift" in result.blockers


def test_acceptance_status_rejects_plan_digest_on_generated_stateful_claim(
    tmp_path: Path,
) -> None:
    expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    generated_claim = next(
        item
        for item in _items(runner, _NAMESPACED)
        if item["kind"] == "PersistentVolumeClaim" and item["metadata"]["name"].startswith("data-")
    )
    generated_claim["metadata"]["labels"]["loom.dev/acceptance-plan-sha256"] = plan.sha256[:32]
    generated_claim["metadata"]["annotations"]["loom.dev/acceptance-plan-sha256"] = plan.sha256

    result = _observe_acceptance(expected, plan, runner)

    assert result.ready is False
    assert "acceptance_plan_digest_drift" in result.blockers


def test_healthy_shadow_accepts_generic_namespace_list_from_kubectl(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    namespaces = runner.responses[_NAMESPACES]
    assert isinstance(namespaces, dict)
    namespaces["kind"] = "List"

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        ("namespace-missing", "namespace_missing"),
        ("namespace-wrong-kind", "namespace_inventory_invalid"),
        ("shared-object-missing", "resource_inventory_drift"),
        ("statefulset-not-ready", "storage_not_ready"),
        ("deployment-not-ready", "management_not_ready"),
        ("web-not-ready", "web_not_ready"),
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
    elif mutation == "web-not-ready":
        _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-web",
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


def _historical_migration_pair(
    current_job: dict[str, Any],
    current_pod: dict[str, Any],
    *,
    index: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    historical_job = copy.deepcopy(current_job)
    historical_pod = copy.deepcopy(current_pod)
    historical_input = hashlib.sha256(f"historical-input-{index}".encode()).hexdigest()
    historical_release = hashlib.sha256(f"historical-release-{index}".encode()).hexdigest()
    historical_name = f"loom-personal-dev-migrate-{historical_input[:16]}-{historical_release[:16]}"

    historical_job["metadata"]["name"] = historical_name
    historical_job["metadata"]["uid"] = f"00000000-0000-0000-0000-{index + 200:012d}"
    historical_pod["metadata"]["name"] = f"{historical_name}-abcde"
    historical_pod["metadata"]["labels"]["job-name"] = historical_name
    historical_pod["metadata"]["labels"]["batch.kubernetes.io/job-name"] = historical_name
    historical_pod["metadata"]["ownerReferences"] = [
        {
            "apiVersion": "batch/v1",
            "blockOwnerDeletion": True,
            "controller": True,
            "kind": "Job",
            "name": historical_name,
            "uid": historical_job["metadata"]["uid"],
        }
    ]
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
    _materialize_migration_api_defaults(historical_job, historical_pod)
    return historical_job, historical_pod


def _current_migration_pair(
    items: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    current_job = next(item for item in items if item["kind"] == "Job")
    current_pod = next(
        item
        for item in items
        if item["kind"] == "Pod"
        and item["metadata"]["labels"].get("app") == "loom-personal-dev-migration"
    )
    return current_job, current_pod


def _mutate_historical_migration_workload(
    job: dict[str, Any],
    pod: dict[str, Any],
    mutation: str,
) -> None:
    pod_spec = job["spec"]["template"]["spec"]
    observed_pod_defaults = {
        key: copy.deepcopy(value) for key, value in pod["spec"].items() if key not in pod_spec
    }
    container = pod_spec["containers"][0]
    environment = {entry["name"]: entry for entry in container["env"]}
    if mutation == "arbitrary-command":
        container["command"] = ["/bin/sh", "-c", "sleep 3600"]
    elif mutation == "untrusted-immutable-image":
        container["image"] = "ghcr.io/example/attacker@sha256:" + "e" * 64
    elif mutation == "sidecar":
        sidecar = copy.deepcopy(container)
        sidecar["name"] = "sidecar"
        sidecar["command"] = ["/bin/sh", "-c", "sleep 3600"]
        pod_spec["containers"].append(sidecar)
    elif mutation == "init-container":
        init_container = copy.deepcopy(container)
        init_container["name"] = "init"
        init_container["command"] = ["/bin/sh", "-c", "sleep 3600"]
        pod_spec["initContainers"] = [init_container]
    elif mutation == "secret-name":
        environment["LOOM_DB_URL"]["valueFrom"]["secretKeyRef"]["name"] = "attacker-controlled"
    elif mutation == "secret-key":
        environment["LOOM_DB_URL"]["valueFrom"]["secretKeyRef"]["key"] = "cp-db-url"
    elif mutation == "pg-timeout":
        environment["PGCONNECT_TIMEOUT"]["value"] = "30"
    elif mutation == "extra-environment":
        container["env"].append({"name": "LD_PRELOAD", "value": "/tmp/injected.so"})
    elif mutation == "privileged":
        container["securityContext"]["privileged"] = True
    elif mutation == "privilege-escalation":
        container["securityContext"]["allowPrivilegeEscalation"] = True
    elif mutation == "privilege-escalation-integer":
        container["securityContext"]["allowPrivilegeEscalation"] = 0
    elif mutation == "added-capability":
        container["securityContext"]["capabilities"]["add"] = ["NET_ADMIN"]
    elif mutation == "container-root":
        container["securityContext"]["runAsUser"] = 0
        container["securityContext"]["runAsNonRoot"] = False
    elif mutation == "container-user-float":
        container["securityContext"]["runAsUser"] = 65532.0
    elif mutation == "pod-unconfined":
        pod_spec["securityContext"]["seccompProfile"]["type"] = "Unconfined"
    elif mutation == "pod-non-root-integer":
        pod_spec["securityContext"]["runAsNonRoot"] = 1
    elif mutation == "pod-user-float":
        pod_spec["securityContext"]["runAsUser"] = 65532.0
    elif mutation == "service-account-token":
        pod_spec["automountServiceAccountToken"] = True
    elif mutation == "service-links":
        pod_spec["enableServiceLinks"] = True
    elif mutation == "management-service-account":
        pod_spec["serviceAccountName"] = "loom-personal-dev-management"
    elif mutation == "host-path":
        pod_spec["volumes"] = [{"name": "tmp", "hostPath": {"path": "/"}}]
    elif mutation == "projected-volume":
        pod_spec["volumes"] = [
            {
                "name": "tmp",
                "projected": {
                    "sources": [{"serviceAccountToken": {"path": "token"}}],
                },
            }
        ]
    elif mutation == "csi-volume":
        pod_spec["volumes"] = [
            {"name": "tmp", "csi": {"driver": "attacker.example", "readOnly": False}}
        ]
    elif mutation == "extra-volume":
        pod_spec["volumes"].append({"name": "host", "hostPath": {"path": "/"}})
    elif mutation == "wrong-volume-mount":
        container["volumeMounts"] = [{"name": "tmp", "mountPath": "/host"}]
    elif mutation == "forced-node":
        pod_spec["nodeName"] = "attacker-chosen-node"
    elif mutation == "forced-node-selector":
        pod_spec["nodeSelector"] = {"kubernetes.io/hostname": "attacker-chosen-node"}
    elif mutation == "restart-policy":
        pod_spec["restartPolicy"] = "OnFailure"
    elif mutation == "image-pull-policy":
        container["imagePullPolicy"] = "Always"
    elif mutation == "container-name":
        container["name"] = "not-migrate"
    elif mutation == "resource-envelope":
        container["resources"]["limits"]["cpu"] = "100"
    elif mutation == "job-parallelism":
        job["spec"]["parallelism"] = 2
    elif mutation == "job-backoff-bool":
        job["spec"]["backoffLimit"] = True
    elif mutation == "job-suspended":
        job["spec"]["suspend"] = True
    elif mutation == "job-ttl":
        job["spec"]["ttlSecondsAfterFinished"] = 60
    elif mutation == "template-annotation":
        job["spec"]["template"]["metadata"]["annotations"][
            "container.apparmor.security.beta.kubernetes.io/migrate"
        ] = "unconfined"
        pod["metadata"]["annotations"]["container.apparmor.security.beta.kubernetes.io/migrate"] = (
            "unconfined"
        )
    elif mutation == "missing-controller-labels":
        for key in (
            "batch.kubernetes.io/controller-uid",
            "batch.kubernetes.io/job-name",
            "controller-uid",
            "job-name",
        ):
            job["spec"]["template"]["metadata"]["labels"].pop(key)
        for key in (
            "batch.kubernetes.io/controller-uid",
            "controller-uid",
        ):
            pod["metadata"]["labels"].pop(key)
    elif mutation in {
        "pod-management-service-account",
        "pod-host-alias",
        "pod-extra-toleration",
        "pod-name",
        "pod-generate-name",
        "missing-pod-service-account",
        "missing-pod-node",
        "pod-priority-bool",
        "pod-toleration-seconds-float",
    }:
        pass
    elif mutation == "missing-job-selector":
        job["spec"].pop("selector")
    elif mutation == "missing-job-default":
        job["spec"].pop("parallelism")
    else:  # pragma: no cover - caller table is exhaustive
        raise AssertionError(mutation)
    pod["spec"] = copy.deepcopy(pod_spec)
    for key, value in observed_pod_defaults.items():
        pod["spec"].setdefault(key, value)
    if mutation == "pod-management-service-account":
        pod["spec"]["serviceAccount"] = "loom-personal-dev-management"
        pod["spec"]["serviceAccountName"] = "loom-personal-dev-management"
    elif mutation == "pod-host-alias":
        pod["spec"]["hostAliases"] = [{"ip": "127.0.0.1", "hostnames": ["postgres"]}]
    elif mutation == "pod-extra-toleration":
        pod["spec"]["tolerations"] = [{"operator": "Exists"}]
    elif mutation == "pod-name":
        pod["metadata"]["name"] = "unbound-migration-pod"
    elif mutation == "pod-generate-name":
        pod["metadata"]["generateName"] = "unbound-migration-pod-"
    elif mutation == "missing-pod-service-account":
        pod["spec"].pop("serviceAccountName")
    elif mutation == "missing-pod-node":
        pod["spec"].pop("nodeName")
    elif mutation == "pod-priority-bool":
        pod["spec"]["priority"] = False
    elif mutation == "pod-toleration-seconds-float":
        pod["spec"]["tolerations"][0]["tolerationSeconds"] = 300.0


def _materialize_migration_api_defaults(
    job: dict[str, Any],
    pod: dict[str, Any],
) -> None:
    job_name = job["metadata"]["name"]
    job_uid = job["metadata"]["uid"]
    job_spec = job["spec"]
    job_spec.update(
        {
            "completionMode": "NonIndexed",
            "completions": 1,
            "manualSelector": False,
            "parallelism": 1,
            "podReplacementPolicy": "TerminatingOrFailed",
            "selector": {
                "matchLabels": {"batch.kubernetes.io/controller-uid": job_uid},
            },
            "suspend": False,
        }
    )
    controller_labels = {
        "batch.kubernetes.io/controller-uid": job_uid,
        "batch.kubernetes.io/job-name": job_name,
        "controller-uid": job_uid,
        "job-name": job_name,
    }
    job_spec["template"]["metadata"]["labels"].update(controller_labels)
    template_spec = job_spec["template"]["spec"]
    template_spec.update(
        {
            "dnsPolicy": "ClusterFirst",
            "schedulerName": "default-scheduler",
            "terminationGracePeriodSeconds": 30,
        }
    )
    template_spec["containers"][0].update(
        {
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
        }
    )
    pod["metadata"]["labels"].update(controller_labels)
    pod["metadata"]["generateName"] = f"{job_name}-"
    pod["metadata"]["name"] = f"{job_name}-"[:58] + "abcde"
    pod["spec"] = copy.deepcopy(template_spec)
    pod["spec"].update(
        {
            "nodeName": "scheduler-selected-node",
            "preemptionPolicy": "PreemptLowerPriority",
            "priority": 0,
            "serviceAccount": "default",
            "serviceAccountName": "default",
            "tolerations": [
                {
                    "effect": "NoExecute",
                    "key": "node.kubernetes.io/not-ready",
                    "operator": "Exists",
                    "tolerationSeconds": 300,
                },
                {
                    "effect": "NoExecute",
                    "key": "node.kubernetes.io/unreachable",
                    "operator": "Exists",
                    "tolerationSeconds": 300,
                },
            ],
        }
    )


def test_observer_accepts_all_valid_retained_migration_history(tmp_path: Path) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job, current_pod = _current_migration_pair(items)
    for index in range(9):
        items.extend(_historical_migration_pair(current_job, current_pod, index=index))

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()
    components = {component.name: component for component in result.components}
    assert components["namespaced-resources"].observed == 52


@pytest.mark.parametrize("mode", ["shadow", "acceptance", "operational"])
def test_observer_accepts_valid_retained_migration_history_in_every_mode(
    tmp_path: Path,
    mode: str,
) -> None:
    if mode == "shadow":
        expected, runner = _healthy_fixture(tmp_path)
        plan = None
    elif mode == "acceptance":
        expected, plan, runner = _acceptance_healthy_fixture(tmp_path)
    elif mode == "operational":
        expected, plan, runner = _operational_healthy_fixture(tmp_path)
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mode)
    items = _items(runner, _NAMESPACED)
    current_job, current_pod = _current_migration_pair(items)
    items.extend(_historical_migration_pair(current_job, current_pod, index=0))

    if mode == "shadow":
        result = _observe(expected, runner)
    elif mode == "acceptance":
        assert isinstance(plan, PersonalDevAcceptancePlan)
        result = _observe_acceptance(expected, plan, runner)
    else:
        assert isinstance(plan, PersonalDevOperationalPlan)
        result = _observe_operational(expected, plan, runner)

    assert result.ready is True
    assert result.blockers == ()


@pytest.mark.parametrize(
    "mutation",
    [
        "arbitrary-command",
        "untrusted-immutable-image",
        "sidecar",
        "init-container",
        "secret-name",
        "secret-key",
        "pg-timeout",
        "extra-environment",
        "privileged",
        "privilege-escalation",
        "privilege-escalation-integer",
        "added-capability",
        "container-root",
        "container-user-float",
        "pod-unconfined",
        "pod-non-root-integer",
        "pod-user-float",
        "service-account-token",
        "service-links",
        "management-service-account",
        "host-path",
        "projected-volume",
        "csi-volume",
        "extra-volume",
        "wrong-volume-mount",
        "forced-node",
        "forced-node-selector",
        "restart-policy",
        "image-pull-policy",
        "container-name",
        "resource-envelope",
        "job-parallelism",
        "job-backoff-bool",
        "job-suspended",
        "job-ttl",
        "template-annotation",
        "missing-controller-labels",
        "pod-management-service-account",
        "pod-host-alias",
        "pod-extra-toleration",
        "pod-name",
        "pod-generate-name",
        "missing-job-selector",
        "missing-job-default",
        "missing-pod-service-account",
        "missing-pod-node",
        "pod-priority-bool",
        "pod-toleration-seconds-float",
    ],
)
def test_observer_rejects_retained_migration_workload_contract_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job, current_pod = _current_migration_pair(items)
    historical_job, historical_pod = _historical_migration_pair(
        current_job,
        current_pod,
        index=0,
    )
    _mutate_historical_migration_workload(
        historical_job,
        historical_pod,
        mutation,
    )
    items.extend((historical_job, historical_pod))

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


@pytest.mark.parametrize("scope", ["current", "historical"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("failed", True),
        ("failed", "0"),
        ("failed", -1),
        ("active", True),
        ("active", "0"),
        ("active", -1),
    ],
)
def test_observer_rejects_malformed_migration_status_counters(
    tmp_path: Path,
    scope: str,
    field: str,
    value: object,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job, current_pod = _current_migration_pair(items)
    if scope == "current":
        target_job = current_job
        blocker = "migration_incomplete"
    elif scope == "historical":
        target_job, historical_pod = _historical_migration_pair(
            current_job,
            current_pod,
            index=0,
        )
        items.extend((target_job, historical_pod))
        blocker = "resource_inventory_drift"
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(scope)
    target_job["status"][field] = value

    result = _observe(expected, runner)

    assert result.ready is False
    assert blocker in result.blockers


@pytest.mark.parametrize("scope", ["current", "historical"])
@pytest.mark.parametrize(
    ("target_kind", "field", "value"),
    [
        ("job", "deletionTimestamp", "2026-08-28T16:00:00Z"),
        ("job", "deletionGracePeriodSeconds", 30),
        (
            "job",
            "ownerReferences",
            [
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "name": "garbage-collection-owner",
                    "uid": "00000000-0000-0000-0000-000000000999",
                }
            ],
        ),
        ("job", "ownerReferences", {}),
        ("job", "finalizers", ["attacker.example/retain-or-delete"]),
        ("job", "finalizers", ""),
        ("pod", "deletionTimestamp", "2026-08-28T16:00:00Z"),
        ("pod", "deletionGracePeriodSeconds", 30),
        ("pod", "finalizers", ["attacker.example/retain-or-delete"]),
        ("pod", "finalizers", {}),
    ],
)
def test_observer_rejects_migration_evidence_with_destructive_lifecycle_metadata(
    tmp_path: Path,
    scope: str,
    target_kind: str,
    field: str,
    value: object,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job, current_pod = _current_migration_pair(items)
    if scope == "current":
        target_job, target_pod = current_job, current_pod
    elif scope == "historical":
        target_job, target_pod = _historical_migration_pair(
            current_job,
            current_pod,
            index=0,
        )
        items.extend((target_job, target_pod))
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(scope)
    if target_kind == "job":
        target = target_job
    elif target_kind == "pod":
        target = target_pod
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(target_kind)
    target["metadata"][field] = copy.deepcopy(value)

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


@pytest.mark.parametrize("scope", ["current", "historical"])
def test_observer_accepts_empty_migration_evidence_lifecycle_lists(
    tmp_path: Path,
    scope: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job, current_pod = _current_migration_pair(items)
    if scope == "current":
        target_job, target_pod = current_job, current_pod
    elif scope == "historical":
        target_job, target_pod = _historical_migration_pair(
            current_job,
            current_pod,
            index=0,
        )
        items.extend((target_job, target_pod))
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(scope)
    target_job["metadata"]["ownerReferences"] = []
    target_job["metadata"]["finalizers"] = []
    target_pod["metadata"]["finalizers"] = []

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()


@pytest.mark.parametrize(
    "mutation",
    [
        "job-name-label",
        "batch-job-name-label",
        "owner-name",
        "owner-uid",
        "owner-shape",
        "owner-boolean-integer",
        "phase-running",
        "phase-failed",
        "phase-pending",
    ],
)
def test_observer_rejects_current_migration_pod_pairing_or_phase_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    _current_job, current_pod = _current_migration_pair(items)
    orphan_name = "loom-personal-dev-migrate-" + "c" * 16 + "-" + "d" * 16
    labels = current_pod["metadata"]["labels"]
    owner_references = current_pod["metadata"]["ownerReferences"]
    if mutation == "job-name-label":
        labels["job-name"] = orphan_name
    elif mutation == "batch-job-name-label":
        labels["batch.kubernetes.io/job-name"] = orphan_name
    elif mutation == "owner-name":
        owner_references[0]["name"] = orphan_name
    elif mutation == "owner-uid":
        owner_references[0]["uid"] = "00000000-0000-0000-0000-000000000999"
    elif mutation == "owner-shape":
        owner_references[0]["controller"] = False
    elif mutation == "owner-boolean-integer":
        owner_references[0]["blockOwnerDeletion"] = 1
        owner_references[0]["controller"] = 1
    elif mutation.startswith("phase-"):
        current_pod["status"]["phase"] = mutation.removeprefix("phase-").title()
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


@pytest.mark.parametrize(
    "mutation",
    [
        "digest-mismatch",
        "failed",
        "running-job",
        "running-pod",
        "mutable-image",
        "pod-template-drift",
        "unpaired",
        "duplicate-pod",
        "owner-name",
        "owner-uid",
        "host-network",
        "invalid-job-api-version",
    ],
)
def test_observer_rejects_invalid_retained_migration_history(
    tmp_path: Path,
    mutation: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job, current_pod = _current_migration_pair(items)
    historical_job, historical_pod = _historical_migration_pair(
        current_job,
        current_pod,
        index=0,
    )
    if mutation == "digest-mismatch":
        historical_job["metadata"]["annotations"]["loom.dev/trusted-release-sha256"] = "f" * 64
    elif mutation == "failed":
        historical_job["status"] = {"active": 0, "failed": 1, "succeeded": 0}
    elif mutation == "running-job":
        historical_job["status"] = {"active": 1, "failed": 0, "succeeded": 0}
    elif mutation == "running-pod":
        historical_pod["status"]["phase"] = "Running"
    elif mutation == "mutable-image":
        historical_job["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "ghcr.io/qianyi-sun/loom-service:latest"
        )
        historical_pod["spec"] = copy.deepcopy(historical_job["spec"]["template"]["spec"])
    elif mutation == "pod-template-drift":
        historical_pod["spec"]["containers"][0]["image"] = (
            "ghcr.io/qianyi-sun/loom-service@sha256:" + "e" * 64
        )
    elif mutation == "owner-name":
        historical_pod["metadata"]["ownerReferences"][0]["name"] = (
            "loom-personal-dev-migrate-" + "c" * 16 + "-" + "d" * 16
        )
    elif mutation == "owner-uid":
        historical_pod["metadata"]["ownerReferences"][0]["uid"] = (
            "00000000-0000-0000-0000-000000000999"
        )
    elif mutation == "host-network":
        historical_job["spec"]["template"]["spec"]["hostNetwork"] = True
        historical_pod["spec"] = copy.deepcopy(historical_job["spec"]["template"]["spec"])
    elif mutation == "invalid-job-api-version":
        historical_job["apiVersion"] = "batch/v2"
        historical_pod["metadata"]["ownerReferences"][0]["apiVersion"] = "batch/v2"
    elif mutation == "duplicate-pod":
        pass
    elif mutation != "unpaired":  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)
    items.append(historical_job)
    if mutation != "unpaired":
        items.append(historical_pod)
    if mutation == "duplicate-pod":
        duplicate = copy.deepcopy(historical_pod)
        duplicate["metadata"]["name"] = f"{historical_job['metadata']['name']}-fghij"
        items.append(duplicate)

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


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
    ticks = iter([100.0, 159.5, 160.0, 160.0, 160.0, 160.0, 160.0, 160.0])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(ticks))

    result = _observe(expected, runner)

    assert runner.calls == []
    assert result.ready is False
    assert "kube_context_invalid" in result.blockers
