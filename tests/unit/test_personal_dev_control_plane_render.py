from __future__ import annotations

import hashlib
import json
import re
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlanError,
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_acceptance_personal_dev_control_plane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_runtime import parse_personal_dev_acceptance_runtime_binding

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
_NOW = datetime(2026, 8, 17, 21, 0, tzinfo=UTC)
_MANAGEMENT_FILES = {
    "admin-secrets.toml",
    "capacity-lifecycle-ca.pem",
    "capacity-lifecycle-certificate.pem",
    "capacity-lifecycle-private-key.pem",
    "capacity-lifecycle-token",
    "capacity-reporter-ca.pem",
    "capacity-reporter-certificate.pem",
    "capacity-reporter-private-key.pem",
    "config.json",
}


def _release_value() -> dict[str, Any]:
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


def _inputs(tmp_path: Path):
    payload = json.dumps(
        _release_value(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    release_path = tmp_path / "release.json"
    release_path.write_bytes(payload)
    release_path.chmod(0o600)
    return (
        load_personal_dev_control_plane_profile(_PROFILE),
        load_personal_dev_trusted_release(
            release_path,
            hashlib.sha256(payload).hexdigest(),
        ),
    )


def _render(tmp_path: Path):
    profile, release = _inputs(tmp_path)
    rendered = render_shadow_personal_dev_control_plane(profile, release)
    documents = [item for item in yaml.safe_load_all(rendered.yaml_text) if item]
    return profile, release, rendered, documents


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


def _acceptance_render(tmp_path: Path):
    profile, release = _inputs(tmp_path)
    shadow = render_shadow_personal_dev_control_plane(profile, release)
    release_sha256 = hashlib.sha256(release.canonical_bytes()).hexdigest()
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
            "execution_epoch": 0,
            "execution_state": "shadow",
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
            "trusted_release_sha256": release_sha256,
        },
        "schema_version": 1,
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "storage": {
            "backup_restore_evidence_sha256": "b" * 64,
            "schema_head": "0105",
        },
        "window": {
            "expires_at": "2026-08-17T23:00:00Z",
            "rollback_expires_at": "2026-08-18T23:00:00Z",
            "started_at": "2026-08-17T20:00:00Z",
        },
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    path = tmp_path / "acceptance-plan.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    plan = load_personal_dev_acceptance_plan(
        path,
        hashlib.sha256(payload).hexdigest(),
    )
    rendered = render_acceptance_personal_dev_control_plane(
        profile,
        release,
        plan,
        now=_NOW,
    )
    documents = [item for item in yaml.safe_load_all(rendered.yaml_text) if item]
    return profile, release, plan, shadow, rendered, documents


def _identity(document: dict[str, Any]) -> tuple[str, str, str]:
    metadata = document["metadata"]
    return (
        document["kind"],
        str(metadata.get("namespace", "")),
        metadata["name"],
    )


def _workload_pod_specs(documents: list[dict[str, Any]]):
    for document in documents:
        if document["kind"] in {"Deployment", "StatefulSet", "Job"}:
            yield document, document["spec"]["template"]["spec"]


def test_shadow_render_is_deterministic_complete_and_digest_bound(tmp_path: Path) -> None:
    profile, release, rendered, documents = _render(tmp_path)
    repeated = render_shadow_personal_dev_control_plane(profile, release)

    assert repeated == rendered
    assert rendered.yaml_text.endswith("\n")
    assert rendered.resource_count == len(documents)
    assert rendered.resource_count == 33
    expected_input = hashlib.sha256(
        b"loom-personal-dev-shadow-render-v1\0"
        + profile.canonical_bytes()
        + b"\0"
        + release.canonical_bytes()
    ).hexdigest()
    assert rendered.input_sha256 == expected_input
    assert rendered.release_sha256 == hashlib.sha256(release.canonical_bytes()).hexdigest()
    assert rendered.runtime_class_name == profile.builder.runtime_class_name
    assert rendered.runtime_handler == profile.builder.runtime_handler
    assert rendered.runtime_profile_sha256 == profile.builder.runtime_profile_sha256
    assert hashlib.sha256(rendered.yaml_text.encode("utf-8")).hexdigest() == (
        "85614f87502349e51827f2b9beee0adab14d95c62a096c1ca45cea80730b4fbe"
    )

    identities = {_identity(document) for document in documents}
    assert identities == {
        ("Namespace", "", "loom-dev"),
        ("ClusterRole", "", "loom-personal-dev-management-mutation"),
        ("ClusterRoleBinding", "", "loom-personal-dev-management-mutation"),
        ("ClusterRole", "", "loom-personal-dev-managed-namespace"),
        ("ClusterRole", "", "loom-personal-dev-activation-agent"),
        ("ValidatingAdmissionPolicy", "", "loom-personal-dev-management-namespaces"),
        ("ValidatingAdmissionPolicyBinding", "", "loom-personal-dev-management-namespaces"),
        ("ValidatingAdmissionPolicy", "", "loom-personal-dev-management-resources"),
        ("ValidatingAdmissionPolicyBinding", "", "loom-personal-dev-management-resources"),
        ("ValidatingAdmissionPolicy", "", "loom-personal-dev-activation-resources"),
        ("ValidatingAdmissionPolicyBinding", "", "loom-personal-dev-activation-resources"),
        ("ServiceAccount", "loom-dev", "loom-personal-dev-management"),
        ("ServiceAccount", "loom-dev", "loom-personal-dev-activation-agent"),
        ("Role", "loom-dev", "loom-personal-dev-shared-operations"),
        ("RoleBinding", "loom-dev", "loom-personal-dev-shared-operations"),
        ("Service", "loom-dev", "loom-dev-postgres-headless"),
        ("Service", "loom-dev", "loom-dev-postgres"),
        ("StatefulSet", "loom-dev", "loom-dev-postgres"),
        ("Service", "loom-dev", "loom-dev-minio"),
        ("StatefulSet", "loom-dev", "loom-dev-minio"),
        ("PersistentVolumeClaim", "loom-dev", "loom-personal-dev-scanner-cache"),
        ("Deployment", "loom-dev", "loom-personal-dev-management"),
        ("Service", "loom-dev", "loom-personal-dev-management"),
        ("Ingress", "loom-dev", "loom-personal-dev-management"),
        ("Deployment", "loom-dev", "loom-personal-dev-activation-agent"),
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-default-deny"),
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-postgres-ingress"),
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-minio-ingress"),
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-management"),
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-management-ingress"),
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-migration-egress"),
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-activation"),
        (
            "Job",
            "loom-dev",
            f"loom-personal-dev-migrate-{expected_input[:16]}-{rendered.release_sha256[:16]}",
        ),
    }
    for document in documents:
        metadata = document["metadata"]
        assert metadata["labels"]["app.kubernetes.io/managed-by"] == (
            "loom-operator"
            if document["kind"] == "Namespace"
            else "loom-personal-dev-control-plane"
        )
        assert metadata["labels"]["loom.dev/render-input"] == expected_input[:32]
        assert metadata["labels"]["loom.dev/trusted-release"] == (rendered.release_sha256[:32])
        assert metadata["annotations"]["loom.dev/render-input-sha256"] == expected_input
        assert metadata["annotations"]["loom.dev/trusted-release-sha256"] == (
            rendered.release_sha256
        )
        if document["kind"] in {"Deployment", "StatefulSet", "Job"}:
            template_metadata = document["spec"]["template"]["metadata"]
            assert template_metadata["labels"]["loom.dev/render-input"] == (expected_input[:32])
            assert template_metadata["annotations"]["loom.dev/render-input-sha256"] == (
                expected_input
            )
            assert template_metadata["annotations"]["loom.dev/trusted-release-sha256"] == (
                rendered.release_sha256
            )


def test_acceptance_render_is_deterministic_plan_bound_and_keeps_shadow_resources(
    tmp_path: Path,
) -> None:
    profile, release, plan, shadow, rendered, documents = _acceptance_render(tmp_path)
    repeated = render_acceptance_personal_dev_control_plane(
        profile,
        release,
        plan,
        now=_NOW,
    )
    shadow_documents = [item for item in yaml.safe_load_all(shadow.yaml_text) if item]

    assert repeated == rendered
    runtime_binding = parse_personal_dev_acceptance_runtime_binding(
        plan.manager_runtime_json(),
        plan.sha256,
    )
    assert runtime_binding.expected_manager.authority_incarnation == (
        plan.manager.authority_incarnation
    )
    assert runtime_binding.expected_manager.observer_principal_id == (
        plan.principals.lifecycle_principal_id
    )
    assert (
        rendered.input_sha256
        == hashlib.sha256(
            b"loom-personal-dev-acceptance-render-v1\0"
            + profile.canonical_bytes()
            + release.canonical_bytes()
            + plan.canonical_bytes()
        ).hexdigest()
    )
    assert rendered.resource_count == shadow.resource_count == 33
    assert rendered.runtime_class_name == plan.builder.runtime_class_name
    assert rendered.runtime_handler == plan.builder.runtime_handler
    assert rendered.runtime_profile_sha256 == plan.builder.runtime_profile_sha256
    assert {_identity(item) for item in documents if item["kind"] != "Job"} == {
        _identity(item) for item in shadow_documents if item["kind"] != "Job"
    }
    assert sum(item["kind"] == "Job" for item in documents) == 1
    assert {_identity(item) for item in documents if item["kind"] == "PersistentVolumeClaim"} == {
        _identity(item) for item in shadow_documents if item["kind"] == "PersistentVolumeClaim"
    }
    images = {
        container["image"]
        for _, pod in _workload_pod_specs(documents)
        for container in (*pod.get("initContainers", []), *pod["containers"])
    }
    shadow_images = {
        container["image"]
        for _, pod in _workload_pod_specs(shadow_documents)
        for container in (*pod.get("initContainers", []), *pod["containers"])
    }
    assert images == shadow_images
    for document in documents:
        metadata = document["metadata"]
        assert metadata["labels"]["loom.dev/acceptance-plan-sha256"] == plan.sha256[:32]
        assert metadata["annotations"]["loom.dev/acceptance-plan-sha256"] == plan.sha256
        if document["kind"] in {"Deployment", "StatefulSet", "Job"}:
            template = document["spec"]["template"]["metadata"]
            assert template["labels"]["loom.dev/acceptance-plan-sha256"] == plan.sha256[:32]
            assert template["annotations"]["loom.dev/acceptance-plan-sha256"] == plan.sha256


def test_acceptance_render_enables_only_personal_application_authorities(
    tmp_path: Path,
) -> None:
    _profile, _release, plan, _shadow, _rendered, documents = _acceptance_render(tmp_path)
    deployments = {
        item["metadata"]["name"]: item for item in documents if item["kind"] == "Deployment"
    }
    management = deployments["loom-personal-dev-management"]
    container = management["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item.get("value") for item in container["env"] if "value" in item}
    scanner_identity = (
        f"trivy-bin-sha256:{plan.builder.scanner_binary_sha256}:"
        f"db-sha256:{plan.builder.scanner_database_sha256}:"
        f"java-db-sha256:{plan.builder.scanner_java_database_sha256}"
    )

    assert env["LOOM_SVC_DEV_INSTANCES_ENABLED"] == "true"
    assert env["LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED"] == "true"
    assert env["LOOM_SVC_K8S_WORKER_ENABLED"] == "false"
    assert env["LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_PLAN_SHA256"] == plan.sha256
    assert env["LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_BINDING_JSON"] == (plan.manager_runtime_json())
    assert env["LOOM_SVC_PERSONAL_DEV_ACTIVATION_PUBLIC_KEY_SHA256"] == (
        plan.activation.public_key_sha256
    )
    assert env["LOOM_SVC_PERSONAL_DEV_BUILDER_RUNTIME_CLASS_NAME"] == (
        plan.builder.runtime_class_name
    )
    assert env["LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_IDENTITY"] == scanner_identity
    assert env["LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_POLICY_SHA256"] == (
        plan.builder.scanner_finding_policy_sha256
    )
    assert env["LOOM_SVC_PERSONAL_DEV_TRUSTED_LAUNCHER_PROFILE_SHA256"] == (
        plan.builder.trusted_launcher_profile_sha256
    )
    assert not any("SLURM" in name for name in env)
    assert (
        management["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["httpGet"]["path"]
        == "/api/v1/health/personal-dev-acceptance"
    )
    assert deployments["loom-personal-dev-activation-agent"]["spec"]["replicas"] == 1
    workload_names = {
        item["metadata"]["name"]
        for item in documents
        if item["kind"] in {"Deployment", "StatefulSet"}
    }
    assert "loom-worker" not in workload_names
    assert "loom-capacity-manager" not in workload_names


def test_management_prepares_and_mounts_only_the_release_bound_scanner_generation(
    tmp_path: Path,
) -> None:
    shadow_profile, shadow_release, shadow_render, shadow_documents = _render(tmp_path)
    (
        acceptance_profile,
        acceptance_release,
        acceptance_plan,
        _shadow,
        acceptance_render,
        acceptance_documents,
    ) = _acceptance_render(tmp_path)

    assert shadow_render.resource_count == acceptance_render.resource_count == 33
    for profile, release, plan, documents in (
        (shadow_profile, shadow_release, None, shadow_documents),
        (
            acceptance_profile,
            acceptance_release,
            acceptance_plan,
            acceptance_documents,
        ),
    ):
        management = next(
            item
            for item in documents
            if _identity(item)
            == ("Deployment", "loom-dev", "loom-personal-dev-management")
        )
        pod = management["spec"]["template"]["spec"]
        generation_subpath = f"generations/{release.scanner.cache_identity_sha256}"
        generation_path = f"/var/lib/loom-personal-dev-scanner/{generation_subpath}"
        init = pod["initContainers"][0]
        assert init == {
            "name": "personal-dev-scanner-cache-init",
            "image": release.images.personal_dev_scanner_cache,
            "command": ["python", "-m", "loom.personal_dev_scanner_cache_init"],
            "args": [
                "--source-root",
                "/opt/loom-personal-dev-scanner-cache/assets",
                "--destination-root",
                "/var/lib/loom-personal-dev-scanner",
                "--cache-identity-sha256",
                release.scanner.cache_identity_sha256,
                "--scanner-binary-sha256",
                release.scanner.binary_sha256,
                "--database-sha256",
                release.scanner.database_sha256,
                "--database-metadata-sha256",
                release.scanner.database_metadata_sha256,
                "--java-database-sha256",
                release.scanner.java_database_sha256,
                "--java-database-metadata-sha256",
                release.scanner.java_database_metadata_sha256,
            ],
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
                "readOnlyRootFilesystem": True,
                "runAsNonRoot": True,
                "runAsUser": 65531,
            },
            "resources": {
                "requests": {
                    "cpu": profile.resources.management.cpu_request,
                    "memory": profile.resources.management.memory_request,
                },
                "limits": {
                    "cpu": profile.resources.management.cpu_limit,
                    "memory": profile.resources.management.memory_limit,
                },
            },
            "volumeMounts": [
                {
                    "name": "scanner-cache",
                    "mountPath": "/var/lib/loom-personal-dev-scanner",
                },
                {"name": "tmp", "mountPath": "/tmp"},
            ],
        }
        assert "env" not in init
        assert pod["automountServiceAccountToken"] is False
        assert pod["nodeSelector"] == {"kubernetes.io/arch": "amd64"}

        service = next(
            container for container in pod["containers"] if container["name"] == "management"
        )
        service_env = {
            item["name"]: item["value"] for item in service["env"] if "value" in item
        }
        scanner_identity = (
            f"trivy-bin-sha256:{release.scanner.binary_sha256}:"
            f"db-sha256:{release.scanner.database_sha256}:"
            f"java-db-sha256:{release.scanner.java_database_sha256}"
        )
        assert service_env["LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_DIR"] == (
            generation_path
        )
        assert service_env[
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_IDENTITY_SHA256"
        ] == release.scanner.cache_identity_sha256
        assert service_env[
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_DATABASE_METADATA_SHA256"
        ] == release.scanner.database_metadata_sha256
        assert service_env[
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_JAVA_DATABASE_METADATA_SHA256"
        ] == release.scanner.java_database_metadata_sha256
        assert service_env["LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_IDENTITY"] == (
            scanner_identity
        )
        assert service_env["LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_POLICY_SHA256"] == (
            "" if plan is None else plan.builder.scanner_finding_policy_sha256
        )

        scanner_mounts = [
            mount for mount in service["volumeMounts"] if mount["name"] == "scanner-cache"
        ]
        assert scanner_mounts == [
            {
                "name": "scanner-cache",
                "mountPath": generation_path,
                "subPath": generation_subpath,
            }
        ]
        assert {
            "name": "scanner-fanal",
            "mountPath": f"{generation_path}/fanal",
        } in service["volumeMounts"]
        scanner_cache_root_mounts = [
            (container["name"], mount)
            for container in (*pod["initContainers"], *pod["containers"])
            for mount in container.get("volumeMounts", [])
            if mount.get("name") == "scanner-cache"
            and mount.get("mountPath") == "/var/lib/loom-personal-dev-scanner"
        ]
        assert scanner_cache_root_mounts == [
            (
                "personal-dev-scanner-cache-init",
                {
                    "name": "scanner-cache",
                    "mountPath": "/var/lib/loom-personal-dev-scanner",
                },
            )
        ]
        assert next(
            volume for volume in pod["volumes"] if volume["name"] == "scanner-fanal"
        ) == {"name": "scanner-fanal", "emptyDir": {"sizeLimit": "4Gi"}}


def test_acceptance_render_rejects_shadow_manifest_binding_drift(tmp_path: Path) -> None:
    profile, release, plan, _shadow, _rendered, _documents = _acceptance_render(tmp_path)
    drifted = replace(
        plan,
        release=replace(plan.release, shadow_manifest_sha256="0" * 64),
    )

    with pytest.raises(PersonalDevAcceptancePlanError):
        render_acceptance_personal_dev_control_plane(
            profile,
            release,
            drifted,
            now=_NOW,
        )


def test_shared_namespace_uses_cross_package_operator_ownership(tmp_path: Path) -> None:
    _, _, _, documents = _render(tmp_path)
    namespace = next(document for document in documents if document["kind"] == "Namespace")

    assert namespace["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "loom-operator"
    assert all(
        document["metadata"]["labels"]["app.kubernetes.io/managed-by"]
        == "loom-personal-dev-control-plane"
        for document in documents
        if document is not namespace
    )


def test_shadow_workloads_are_inert_immutable_and_exclude_shared_app(tmp_path: Path) -> None:
    _, release, _, documents = _render(tmp_path)
    deployments = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "Deployment"
    }
    management = deployments["loom-personal-dev-management"]
    activation = deployments["loom-personal-dev-activation-agent"]
    service = next(
        item
        for item in management["spec"]["template"]["spec"]["containers"]
        if item["name"] == "management"
    )
    service_env = {item["name"]: item.get("value") for item in service["env"] if "value" in item}
    assert service_env["LOOM_SVC_DEV_INSTANCES_ENABLED"] == "false"
    assert service_env["LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED"] == "false"
    assert service_env["LOOM_SVC_K8S_WORKER_ENABLED"] == "false"
    assert not any("SLURM" in item["name"] for item in service["env"])
    assert service_env["LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_IDENTITY"] == (
        f"trivy-bin-sha256:{release.scanner.binary_sha256}:"
        f"db-sha256:{release.scanner.database_sha256}:"
        f"java-db-sha256:{release.scanner.java_database_sha256}"
    )
    assert service_env["LOOM_SVC_PERSONAL_DEV_TRUSTED_LAUNCHER_PROFILE_SHA256"] == ""
    assert activation["spec"]["replicas"] == 0

    workload_names = {
        document["metadata"]["name"]
        for document in documents
        if document["kind"] in {"Deployment", "StatefulSet"}
    }
    forbidden = {
        "loom-control-plane",
        "loom-llm-gateway",
        "loom-web",
        "loom-worker",
        "loom-family-orchestrator",
        "loom-pipeline-orchestrator",
        "loom-capacity-manager",
    }
    assert workload_names.isdisjoint(forbidden)
    images = {
        container["image"]
        for _, pod in _workload_pod_specs(documents)
        for container in (*pod.get("initContainers", []), *pod["containers"])
    }
    assert images == {
        release.images.loom_service,
        release.images.personal_dev_activation_agent,
        release.images.personal_dev_scanner_cache,
        release.images.postgres,
        release.images.minio,
        release.images.minio_client,
    }
    assert all(re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image) for image in images)
    assert release.images.personal_dev_builder not in images


def test_minio_admin_bootstraps_base_buckets_before_readiness(tmp_path: Path) -> None:
    _, _, _, documents = _render(tmp_path)
    minio = next(
        document
        for document in documents
        if _identity(document) == ("StatefulSet", "loom-dev", "loom-dev-minio")
    )
    admin = next(
        container
        for container in minio["spec"]["template"]["spec"]["containers"]
        if container["name"] == "admin"
    )
    command = " ".join(admin["command"])
    environment = {item["name"]: item.get("value") for item in admin["env"]}

    assert "MC_HOST_local" in command
    assert environment["MC_CONFIG_DIR"] == "/tmp/mc"
    assert "@127.0.0.1:9000" in command
    assert "mc alias set" not in command
    assert "mc mb --ignore-existing local/artifacts local/trajectories" in command
    readiness = " ".join(admin["readinessProbe"]["exec"]["command"])
    assert "mc stat local/artifacts" in readiness
    assert "mc stat local/trajectories" in readiness


def test_secret_projection_and_scalar_key_boundaries_are_exact(tmp_path: Path) -> None:
    _, _, _, documents = _render(tmp_path)
    assert not any(document["kind"] == "Secret" for document in documents)
    scalar_refs = {
        env["valueFrom"]["secretKeyRef"]["key"]
        for _, pod in _workload_pod_specs(documents)
        for container in (*pod.get("initContainers", []), *pod["containers"])
        for env in container.get("env", [])
        if "secretKeyRef" in env.get("valueFrom", {})
    }
    assert scalar_refs == {
        "dev-instance-database-admin-url",
        "minio-access-key",
        "minio-secret-key",
        "postgres-database",
        "postgres-password",
        "postgres-user",
        "secret-store-master-key",
        "svc-db-url",
    }

    secret_items: dict[str, set[str]] = {}
    for _, pod in _workload_pod_specs(documents):
        for volume in pod.get("volumes", []):
            secret = volume.get("secret")
            if secret is not None:
                secret_items.setdefault(secret["secretName"], set()).update(
                    item["key"] for item in secret.get("items", [])
                )
    assert secret_items == {
        "loom-personal-dev-management": _MANAGEMENT_FILES,
        "loom-personal-dev-activation-public": {"public-key"},
        "loom-personal-dev-activation-agent": {"private-key"},
    }

    management = next(
        document
        for document in documents
        if _identity(document) == ("Deployment", "loom-dev", "loom-personal-dev-management")
    )
    init = management["spec"]["template"]["spec"]["initContainers"]
    commands = "\n".join(" ".join(item["command"]) for item in init)
    assert "loom.personal_dev_secret_init" in commands
    assert "--profile management-files" in commands
    assert "--profile activation-public" in commands
    activation = next(
        document
        for document in documents
        if _identity(document) == ("Deployment", "loom-dev", "loom-personal-dev-activation-agent")
    )
    activation_commands = "\n".join(
        " ".join(item["command"])
        for item in activation["spec"]["template"]["spec"]["initContainers"]
    )
    assert "--profile activation-private" in activation_commands
    assert "management-files" not in activation_commands
    assert "activation-public" not in activation_commands


def test_pods_are_restricted_finite_and_receive_only_explicit_api_tokens(
    tmp_path: Path,
) -> None:
    profile, _, _, documents = _render(tmp_path)
    for document, pod in _workload_pod_specs(documents):
        assert pod["automountServiceAccountToken"] is False
        assert pod["securityContext"]["runAsNonRoot"] is True
        assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
        assert pod.get("hostNetwork", False) is False
        assert pod.get("hostPID", False) is False
        assert pod.get("hostIPC", False) is False
        for container in (*pod.get("initContainers", []), *pod["containers"]):
            security = container["securityContext"]
            assert security["allowPrivilegeEscalation"] is False
            assert security["capabilities"] == {"drop": ["ALL"]}
            assert security["runAsNonRoot"] is True
            assert "requests" in container["resources"]
            assert "limits" in container["resources"]
        if document["kind"] == "Job":
            assert 1 <= document["spec"]["activeDeadlineSeconds"] <= 900
        assert all("hostPath" not in volume for volume in pod.get("volumes", []))

    deployments = [item for item in documents if item["kind"] == "Deployment"]
    for deployment in deployments:
        pod = deployment["spec"]["template"]["spec"]
        token_volume = next(
            volume for volume in pod["volumes"] if volume["name"] == "kube-api-access"
        )
        sources = token_volume["projected"]["sources"]
        token = next(
            item["serviceAccountToken"] for item in sources if "serviceAccountToken" in item
        )
        assert token == {
            "audience": "https://kubernetes.default.svc.cluster.local",
            "expirationSeconds": 600,
            "path": "token",
        }
        assert any(item.get("configMap", {}).get("name") == "kube-root-ca.crt" for item in sources)
        assert any("downwardAPI" in item for item in sources)

    policies = [item for item in documents if item["kind"] == "NetworkPolicy"]
    assert "0.0.0.0/0" not in yaml.safe_dump_all(policies)
    assert profile.network.kubernetes_api_cidr in yaml.safe_dump_all(policies)

    migration_policy = next(
        item
        for item in policies
        if item["metadata"]["name"] == "loom-personal-dev-migration-egress"
    )
    assert migration_policy["spec"]["podSelector"] == {
        "matchLabels": {"app": "loom-personal-dev-migration"}
    }
    assert migration_policy["spec"]["policyTypes"] == ["Egress"]
    migration_egress = yaml.safe_dump(migration_policy["spec"]["egress"])
    assert profile.network.dns_pod_label_value in migration_egress
    assert "loom-dev-postgres" in migration_egress
    assert "loom-dev-minio" not in migration_egress
    assert profile.network.capacity_manager_pod_label_value not in migration_egress
    assert profile.network.kubernetes_api_cidr not in migration_egress


def test_storage_ingress_separates_postgres_and_minio_callers(tmp_path: Path) -> None:
    _, _, _, documents = _render(tmp_path)
    policies = {
        item["metadata"]["name"]: item for item in documents if item["kind"] == "NetworkPolicy"
    }

    assert "loom-personal-dev-storage" not in policies
    postgres = policies["loom-personal-dev-postgres-ingress"]
    minio = policies["loom-personal-dev-minio-ingress"]
    assert postgres["spec"]["podSelector"] == {"matchLabels": {"app": "loom-dev-postgres"}}
    assert postgres["spec"]["ingress"][0]["ports"] == [{"protocol": "TCP", "port": 5432}]
    postgres_sources = yaml.safe_dump(postgres["spec"]["ingress"][0]["from"])
    assert "loom-personal-dev-management" in postgres_sources
    assert "loom-personal-dev-migration" in postgres_sources
    assert "loom-dev-instance-controller" in postgres_sources
    assert "loom-personal-dev-activation-agent" not in postgres_sources
    assert "loom-personal-dev-builder-controller" not in postgres_sources

    assert minio["spec"]["podSelector"] == {"matchLabels": {"app": "loom-dev-minio"}}
    assert minio["spec"]["ingress"][0]["ports"] == [{"protocol": "TCP", "port": 9000}]
    minio_sources = yaml.safe_dump(minio["spec"]["ingress"][0]["from"])
    assert "loom-personal-dev-management" in minio_sources
    assert "loom-personal-dev-activation-agent" in minio_sources
    assert "loom-dev-instance-controller" in minio_sources
    assert "loom-personal-dev-builder-controller" in minio_sources
    assert "loom-personal-dev-migration" not in minio_sources


def test_rbac_uses_dynamic_rolebindings_and_fail_closed_principal_policies(
    tmp_path: Path,
) -> None:
    _, _, _, documents = _render(tmp_path)
    roles = {item["metadata"]["name"]: item for item in documents if item["kind"] == "ClusterRole"}
    mutation_rules = roles["loom-personal-dev-management-mutation"]["rules"]
    assert not any(
        "secrets" in rule.get("resources", [])
        and any(verb in rule["verbs"] for verb in ("get", "list", "watch"))
        for rule in mutation_rules
    )
    managed_rules = roles["loom-personal-dev-managed-namespace"]["rules"]
    secret_rules = [rule for rule in managed_rules if "secrets" in rule.get("resources", [])]
    assert secret_rules == [
        {
            "apiGroups": [""],
            "resources": ["secrets"],
            "resourceNames": [
                "loom-admin-secret",
                "loom-capacity-agent",
                "loom-capacity-agent-credentials",
                "loom-secrets",
            ],
            "verbs": ["get"],
        }
    ]
    mutation_network_rules = [
        rule for rule in mutation_rules if rule.get("apiGroups") == ["networking.k8s.io"]
    ]
    assert mutation_network_rules == [
        {
            "apiGroups": ["networking.k8s.io"],
            "resources": ["networkpolicies"],
            "verbs": ["create", "delete", "patch", "update"],
        }
    ]
    binding = next(
        item
        for item in documents
        if _identity(item) == ("ClusterRoleBinding", "", "loom-personal-dev-management-mutation")
    )
    assert binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "loom-personal-dev-management",
            "namespace": "loom-dev",
        }
    ]
    assert not any(
        item["kind"] == "ClusterRoleBinding"
        and item["metadata"]["name"] == "loom-personal-dev-activation-agent"
        for item in documents
    )

    policies = {
        item["metadata"]["name"]: item
        for item in documents
        if item["kind"] == "ValidatingAdmissionPolicy"
    }

    def policy_expressions(name: str) -> str:
        spec = policies[name]["spec"]
        expressions = [
            item["expression"]
            for section in ("matchConditions", "validations")
            for item in spec.get(section, [])
        ]
        return "\n".join(expressions)

    namespace_policy = policy_expressions("loom-personal-dev-management-namespaces")
    resource_policy = policy_expressions("loom-personal-dev-management-resources")
    activation_policy = policy_expressions("loom-personal-dev-activation-resources")
    exact_management = "system:serviceaccount:loom-dev:loom-personal-dev-management"
    exact_activation = "system:serviceaccount:loom-dev:loom-personal-dev-activation-agent"
    assert exact_management in namespace_policy
    assert exact_management in resource_policy
    assert "startsWith('loom-dev-')" in namespace_policy
    assert "startsWith('loom-build-')" in namespace_policy
    assert (
        "pod-security.kubernetes.io/enforce'] == 'restricted'" in namespace_policy
    )
    assert "pod-security.kubernetes.io/enforce'] == 'privileged'" in namespace_policy
    assert "pod-security.kubernetes.io/enforce-version'] == 'v1.36'" in (
        namespace_policy
    )
    assert "pod-security.kubernetes.io/audit'] == 'restricted'" in namespace_policy
    assert "pod-security.kubernetes.io/audit-version'] == 'v1.36'" in (
        namespace_policy
    )
    assert "pod-security.kubernetes.io/warn'] == 'restricted'" in namespace_policy
    assert "pod-security.kubernetes.io/warn-version'] == 'v1.36'" in (
        namespace_policy
    )
    assert "matches('^loom-dev-[a-z]([-a-z0-9]{0,18}[a-z0-9])?$')" in namespace_policy
    assert "matches('^loom-build-[0-9a-f]{32}-l[0-9a-f]{16}$')" in namespace_policy
    assert (
        "matches('^loom-dev-(dev|development|staging|production|prod|local|loom|shared|default)$')"
        in namespace_policy
    )
    assert "== 'loom-dev-instance-controller'" in namespace_policy
    assert "== 'loom-personal-dev-builder-controller'" in namespace_policy
    assert (
        "in ['loom-dev-instance-controller','loom-personal-dev-builder-controller']"
        not in namespace_policy
    )
    assert "request.namespace != 'loom-dev'" in resource_policy
    assert "request.operation == 'CONNECT'" in resource_policy
    assert "request.subResource == 'exec'" in resource_policy
    assert "request.name == 'loom-dev-minio-0'" in resource_policy
    assert (
        "matches('^loom-(control-plane|llm-gateway|service|web)-g[1-9][0-9]*$')" in resource_policy
    )
    resource_rules = policies["loom-personal-dev-management-resources"]["spec"]["matchConstraints"][
        "resourceRules"
    ]
    assert resource_rules == [
        {
            "apiGroups": ["*"],
            "apiVersions": ["*"],
            "operations": ["CREATE", "UPDATE", "DELETE", "CONNECT"],
            "resources": ["*/*"],
        }
    ]
    assert "loom-personal-dev-managed-namespace" in resource_policy
    assert "loom-personal-dev-activation-agent" in resource_policy
    assert exact_activation in activation_policy
    assert "startsWith('loom-dev-')" in activation_policy
    assert "startsWith('loom-build-')" not in activation_policy
    assert "services" in activation_policy
    assert "ingresses" in activation_policy


def test_admission_requires_exact_namespace_shapes_for_every_mutation(
    tmp_path: Path,
) -> None:
    _, _, _, documents = _render(tmp_path)
    policies = {
        item["metadata"]["name"]: item
        for item in documents
        if item["kind"] == "ValidatingAdmissionPolicy"
    }

    def expressions(name: str) -> str:
        spec = policies[name]["spec"]
        return "\n".join(
            item["expression"]
            for section in ("matchConditions", "validations")
            for item in spec.get(section, [])
        )

    personal_pattern = "matches('^loom-dev-[a-z]([-a-z0-9]{0,18}[a-z0-9])?$')"
    reserved_pattern = (
        "matches('^loom-dev-(dev|development|staging|production|prod|local|loom|shared|default)$')"
    )
    builder_pattern = "matches('^loom-build-[0-9a-f]{32}-l[0-9a-f]{16}$')"
    namespace_policy = expressions("loom-personal-dev-management-namespaces")
    resource_policy = expressions("loom-personal-dev-management-resources")
    activation_policy = expressions("loom-personal-dev-activation-resources")

    for value in (namespace_policy, resource_policy, activation_policy):
        assert personal_pattern in value
        assert reserved_pattern in value
    for value in (namespace_policy, resource_policy):
        assert builder_pattern in value
    assert builder_pattern not in activation_policy
    assert (
        "metadata.labels['app.kubernetes.io/managed-by'] == 'loom-dev-instance-controller'"
        in resource_policy
    )
    assert "'loom-personal-dev-lifecycle'" in resource_policy
    assert (
        "metadata.labels['app.kubernetes.io/managed-by'] == "
        "'loom-personal-dev-builder-controller'" in resource_policy
    )


def test_management_admission_binds_resource_names_to_each_family_contract(
    tmp_path: Path,
) -> None:
    _, _, _, documents = _render(tmp_path)
    policy = next(
        item
        for item in documents
        if item["kind"] == "ValidatingAdmissionPolicy"
        and item["metadata"]["name"] == "loom-personal-dev-management-resources"
    )
    expressions = "\n".join(item["expression"] for item in policy["spec"]["validations"])

    assert "matches('^loom-(control-plane|llm-gateway|service|web)-g[1-9][0-9]*$')" in (expressions)
    assert "matches('^loom-migrate-[0-9a-f]{7}-g[1-9][0-9]*$')" in expressions
    assert "['default-deny','runtime-egress','runtime-ingress','capacity-agent-egress']" in (
        expressions
    )
    assert "matches('^build-contract-(amd64|arm64)-l[0-9a-f]{16}$')" in expressions
    assert "matches('^build-capability-(amd64|arm64)-l[0-9a-f]{16}$')" in expressions
    assert "['build-amd64','build-arm64']" in expressions
    assert "['default-deny','builder-egress']" in expressions


def test_builder_admission_binds_privileged_exception_to_exact_job_contract(
    tmp_path: Path,
) -> None:
    profile, release, _rendered, documents = _render(tmp_path)
    policy = next(
        item
        for item in documents
        if item["kind"] == "ValidatingAdmissionPolicy"
        and item["metadata"]["name"] == "loom-personal-dev-management-resources"
    )
    contract = "\n".join(
        item["expression"]
        for item in policy["spec"]["validations"]
        if item["message"].startswith("builder Job ")
    )

    for value in (
        release.images.personal_dev_builder,
        profile.builder.runtime_class_name,
        "spec.template.spec.runtimeClassName",
        "spec.template.spec.containers.size() == 1",
        "spec.template.spec.initContainers.size() == 1",
        "spec.template.spec.shareProcessNamespace == false",
        "spec.template.spec.automountServiceAccountToken == false",
        "spec.template.spec.enableServiceLinks == false",
        "spec.template.spec.hostNetwork",
        "spec.template.spec.hostPID",
        "spec.template.spec.hostIPC",
        "!has((request.operation == 'DELETE' ? oldObject : object).spec.template.spec.hostUsers)",
        "containers[0].name == 'builder'",
        "containers[0].securityContext.allowPrivilegeEscalation == false",
        "containers[0].securityContext.capabilities.drop == ['ALL']",
        "initContainers[0].name == 'buildkitd'",
        "initContainers[0].restartPolicy == 'Always'",
        "initContainers[0].command == ['/usr/local/bin/loom-personal-dev-buildkitd']",
        "initContainers[0].securityContext.allowPrivilegeEscalation == true",
        "initContainers[0].securityContext.capabilities.add == ['SETGID','SETUID']",
        "initContainers[0].securityContext.seccompProfile.type == 'Unconfined'",
        "initContainers[0].volumeMounts.size() == 3",
        "containers[0].volumeMounts.size() == 5",
        "spec.template.spec.volumes.size() == 7",
        "resources.requests['cpu'] == quantity('1')",
        "resources.limits['cpu'] == quantity('4')",
        "resources.requests['memory'] == quantity('1Gi')",
        "resources.limits['memory'] == quantity('8Gi')",
        "resources.requests['ephemeral-storage'] == quantity('4Gi')",
        "resources.limits['ephemeral-storage'] == quantity('20Gi')",
    ):
        assert value in contract
    assert "!has(" in contract
    assert "hostPath" in contract
    assert "projected" in contract
    assert "csi" in contract
    assert "attempt-capability" in contract
    assert "buildkit-state" in contract
    assert "readOnly == true" in contract


def test_management_admission_blocks_indirect_personal_secret_reads(
    tmp_path: Path,
) -> None:
    _, _, _, documents = _render(tmp_path)
    policy = next(
        item
        for item in documents
        if item["kind"] == "ValidatingAdmissionPolicy"
        and item["metadata"]["name"] == "loom-personal-dev-management-resources"
    )
    validations = policy["spec"]["validations"]
    expressions = "\n".join(item["expression"] for item in validations)
    messages = "\n".join(item["message"] for item in policy["spec"]["validations"])
    workload_expression = next(
        item["expression"]
        for item in validations
        if "builder workload cannot acquire" in item["message"]
    )
    application_secrets = "['loom-secrets','loom-admin-secret']"
    capacity_secrets = "['loom-capacity-agent']"

    assert "automountServiceAccountToken == false" in expressions
    assert "serviceAccountName == 'default'" in expressions
    for secret_names in (application_secrets, capacity_secrets):
        assert f"volume.secret.secretName in {secret_names}" in workload_expression
        assert f"source.secretRef.name in {secret_names}" in workload_expression
        assert f"variable.valueFrom.secretKeyRef.name in {secret_names}" in workload_expression
    assert "loom-capacity-agent-credentials" not in workload_expression
    assert "metadata.name == 'loom-capacity-agent'" in workload_expression
    assert "!has(volume.projected)" in expressions
    assert "!has(volume.csi)" in expressions
    assert (
        "!has((request.operation == 'DELETE' ? oldObject : object).spec.template.spec.imagePullSecrets)"
        in expressions
    )
    assert " not in " not in expressions
    assert (
        "volume.secret.secretName.matches("
        "'^build-capability-(amd64|arm64)-l[0-9a-f]{16}$')" in expressions
    )
    assert "builder workload cannot acquire API or unrelated Secret authority" in messages


def test_management_admission_binds_capacity_lifecycle_ownership_to_exact_resources(
    tmp_path: Path,
) -> None:
    _, _, _, documents = _render(tmp_path)
    policy = next(
        item
        for item in documents
        if item["kind"] == "ValidatingAdmissionPolicy"
        and item["metadata"]["name"] == "loom-personal-dev-management-resources"
    )
    ownership = next(
        item["expression"]
        for item in policy["spec"]["validations"]
        if item["message"] == "management resource lacks its namespace-family ownership"
    )

    assert "['loom-capacity-agent','loom-capacity-agent-credentials']" in ownership
    assert "metadata.name == 'loom-capacity-agent'" in ownership
    assert "metadata.name == 'capacity-agent-egress'" in ownership
    assert "'loom-personal-dev-lifecycle'" in ownership
    assert "'loom-dev-instance-controller'" in ownership


def test_activation_admission_constrains_stable_routes_to_exact_owner_and_generation(
    tmp_path: Path,
) -> None:
    profile, _, _, documents = _render(tmp_path)
    policy = next(
        item
        for item in documents
        if item["kind"] == "ValidatingAdmissionPolicy"
        and item["metadata"]["name"] == "loom-personal-dev-activation-resources"
    )
    expressions = "\n".join(item["expression"] for item in policy["spec"]["validations"])

    assert "request.namespace == 'loom-dev-' +" in expressions
    assert "metadata.labels['loom.dev/generation'].matches('^[1-9][0-9]*$')" in expressions
    assert "spec.type == 'ClusterIP'" in expressions
    assert "spec.selector.size() == 3" in expressions
    assert "metadata.name + '-g' +" in expressions
    assert "spec.ports.size() == 1" in expressions
    for port in (80, 8080, 8090, 9100):
        assert str(port) in expressions
    assert f"spec.ingressClassName == '{profile.network.ingress_class_name}'" in expressions
    assert (
        "metadata.annotations['cert-manager.io/cluster-issuer'] == "
        f"'{profile.network.ingress_cluster_issuer}'" in expressions
    )
    for value in (
        "'.dev.yylx.world'",
        "'cp-'",
        "'gw-'",
        "'loom-control-plane'",
        "'loom-llm-gateway'",
        "'loom-service'",
        "'loom-web'",
        "'loom-dev-tls'",
    ):
        assert value in expressions


def test_migration_evidence_is_retained_for_long_lived_shadow_status(
    tmp_path: Path,
) -> None:
    _, _, _, documents = _render(tmp_path)
    migration = next(document for document in documents if document["kind"] == "Job")

    assert "ttlSecondsAfterFinished" not in migration["spec"]
    container = migration["spec"]["template"]["spec"]["containers"][0]
    command = " ".join(container["command"])
    assert "alembic -c migrations/alembic.ini current" in command
    assert command.index("current") < command.index("upgrade head")
    assert 'test "$attempt" -lt 100' in command
    env = {item["name"]: item.get("value") for item in container["env"]}
    assert env["PGCONNECT_TIMEOUT"] == "3"


def test_render_contains_no_secret_values_or_physical_capacity_authority(
    tmp_path: Path,
) -> None:
    _, _, _, documents = _render(tmp_path)
    rendered = yaml.safe_dump_all(documents, sort_keys=False)
    forbidden_keys = {
        "data",
        "stringData",
    }
    assert all(document["kind"] != "Secret" for document in documents)
    assert "loom-dev-shared" not in rendered
    assert "pool_weight" not in rendered.casefold()
    assert "executable_new_capacity_ceiling: 1" not in rendered
    assert "scontrol" not in rendered.casefold()
    assert "sbatch" not in rendered.casefold()
    assert "/var/run/docker.sock" not in rendered
    assert "privileged: true" not in rendered.casefold()
    assert not any(key in document for document in documents for key in forbidden_keys)
