from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import yaml

from loom.personal_dev_control_plane_config import (
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_shadow_personal_dev_control_plane,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
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
        "schema_version": 1,
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
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
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
    assert rendered.resource_count == 32
    expected_input = hashlib.sha256(
        b"loom-personal-dev-shadow-render-v1\0"
        + profile.canonical_bytes()
        + b"\0"
        + release.canonical_bytes()
    ).hexdigest()
    assert rendered.input_sha256 == expected_input
    assert rendered.release_sha256 == hashlib.sha256(release.canonical_bytes()).hexdigest()

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
        ("NetworkPolicy", "loom-dev", "loom-personal-dev-storage"),
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
            "loom-personal-dev-control-plane"
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
    assert service_env["LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_IDENTITY"] == ""
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

    assert "MC_HOST_local" in command
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
            "resources": ["*", "*/*"],
        }
    ]
    assert "loom-personal-dev-managed-namespace" in resource_policy
    assert "loom-personal-dev-activation-agent" in resource_policy
    assert exact_activation in activation_policy
    assert "startsWith('loom-dev-')" in activation_policy
    assert "startsWith('loom-build-')" not in activation_policy
    assert "services" in activation_policy
    assert "ingresses" in activation_policy


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
