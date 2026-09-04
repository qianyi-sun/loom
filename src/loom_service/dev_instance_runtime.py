"""Validated loom-service wiring for the shared-fleet lifecycle controller."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any

from loom.dev_instance_manifest import DevInstanceManifestConfig
from loom.dev_instance_provisioner import DevInstanceProvisioner, InstanceStore
from loom.dev_instance_runtime import (
    HttpControlPlanePolicyRegistrar,
    KubectlCandidateGenerationProvisioner,
    KubectlClient,
    KubectlClusterProvisioner,
    KubectlMinioTenantProvisioner,
    KubectlSecretVault,
    PsycopgOwnerAccessBootstrap,
    PsycopgSharedFixtureSqlExecutor,
    S3BucketEnsurer,
)
from loom.personal_dev_runtime import PersonalDevPreparationRuntime, PersonalDevRuntimeConfig
from loom_service.config import LoomServiceSettings

DevInstanceProvisionerFactory = Callable[[InstanceStore], DevInstanceProvisioner]

_SENSITIVE_CONFIG_PARTS = frozenset(
    {"token", "password", "secret", "credential", "authorization", "cookie"},
)


def _load_actuator_config(raw: str, *, candidate_sha: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("dev-instance Slurm actuator config must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("dev-instance Slurm actuator config must be a JSON object")
    for key, value in parsed.items():
        normalized = str(key).lower().replace("-", "_")
        if any(part in normalized for part in _SENSITIVE_CONFIG_PARTS):
            raise RuntimeError(
                "dev-instance Slurm actuator config must reference protected files, "
                f"not embed sensitive field {key!r}",
            )
        if isinstance(value, dict) or (
            isinstance(value, list)
            and not all(isinstance(item, (str, int, float, bool)) for item in value)
        ):
            raise RuntimeError(
                "dev-instance Slurm actuator config values must be scalar or scalar arrays",
            )
    parsed["candidate_sha"] = candidate_sha
    parsed["external_runner"] = True
    return parsed


def build_dev_instance_provisioner_factory(
    settings: LoomServiceSettings,
    *,
    minio_client: Any,
) -> DevInstanceProvisionerFactory | None:
    """Return live wiring when explicitly enabled; otherwise no mutation path."""
    if not settings.dev_instances_enabled:
        return None
    if settings.dev_instance_database_admin_url is None:
        raise RuntimeError(
            "LOOM_SVC_DEV_INSTANCE_DATABASE_ADMIN_URL is required when dev instances are enabled",
        )
    kubectl_path = settings.dev_instance_kubectl_path
    if not kubectl_path.is_file() or not os.access(kubectl_path, os.X_OK):
        raise RuntimeError("configured dev-instance kubectl executable is unavailable")
    manifest_config = DevInstanceManifestConfig(
        image_tag=settings.dev_instance_image_tag,
        candidate_sha=settings.dev_instance_candidate_sha,
        deployment_generation=settings.dev_instance_deployment_generation,
        container_registry=settings.dev_instance_container_registry,
        minio_endpoint=settings.minio_endpoint,
        minio_region=settings.minio_region,
        ingress_class_name=settings.dev_instance_ingress_class_name,
        ingress_cert_manager_cluster_issuer=(
            settings.dev_instance_ingress_cert_manager_cluster_issuer
        ),
    )
    actuator_config = _load_actuator_config(
        settings.dev_instance_slurm_actuator_config_json,
        candidate_sha=settings.dev_instance_candidate_sha,
    )
    required_actuator_fields = {
        "allowed_nodes",
        "env_file",
        "repo_dir",
        "requested_cpus",
        "requested_memory_mib",
        "requested_concurrency",
        "container_cpus",
        "container_memory_mib",
        "container_pids",
        "job_pids_max",
    }
    missing = sorted(required_actuator_fields - set(actuator_config))
    if missing:
        raise RuntimeError(
            "dev-instance Slurm actuator config is missing required fields: " + ", ".join(missing),
        )
    if actuator_config["env_file"] != "/var/lib/loom-dev-workers/{environment}.env":
        raise RuntimeError(
            "dev-instance Slurm env_file must be the derived protected template "
            "/var/lib/loom-dev-workers/{environment}.env",
        )
    kubectl = KubectlClient(
        str(kubectl_path),
        context=settings.dev_instance_kube_context,
    )
    database_admin_url = str(settings.dev_instance_database_admin_url)
    sql = PsycopgSharedFixtureSqlExecutor(database_admin_url)
    buckets = S3BucketEnsurer(minio_client, region=settings.minio_region)

    def factory(store: InstanceStore) -> DevInstanceProvisioner:
        vault = KubectlSecretVault(
            kubectl=kubectl,
            database_admin_url=database_admin_url,
            manifest_config=manifest_config,
        )
        return DevInstanceProvisioner(
            store=store,
            sql=sql,
            buckets=buckets,
            vault=vault,
            object_store_tenant=KubectlMinioTenantProvisioner(
                kubectl=kubectl,
                vault=vault,
            ),
            access=PsycopgOwnerAccessBootstrap(database_admin_url),
            cluster=KubectlClusterProvisioner(
                kubectl=kubectl,
                base_manifest_config=manifest_config,
            ),
            policy=HttpControlPlanePolicyRegistrar(
                vault=vault,
                control_plane_url_template=(settings.dev_instance_control_plane_url_template),
                actuator_config=actuator_config,
                drain_timeout_seconds=(settings.dev_instance_policy_drain_timeout_sec),
            ),
            deployment_generation=settings.dev_instance_deployment_generation,
            candidate_sha=settings.dev_instance_candidate_sha,
        )

    return factory


def build_personal_dev_preparation_runtime(
    settings: LoomServiceSettings,
    *,
    minio_client: Any,
) -> PersonalDevPreparationRuntime | None:
    """Build the sole candidate-aware preparation executor when enabled."""
    if not settings.dev_instances_enabled:
        return None
    if settings.dev_instance_database_admin_url is None:
        raise RuntimeError(
            "LOOM_SVC_DEV_INSTANCE_DATABASE_ADMIN_URL is required when dev instances are enabled",
        )
    kubectl_path = settings.dev_instance_kubectl_path
    if not kubectl_path.is_file() or not os.access(kubectl_path, os.X_OK):
        raise RuntimeError("configured dev-instance kubectl executable is unavailable")
    kubectl = KubectlClient(
        str(kubectl_path),
        context=settings.dev_instance_kube_context,
    )
    database_admin_url = str(settings.dev_instance_database_admin_url)
    vault = KubectlSecretVault(
        kubectl=kubectl,
        database_admin_url=database_admin_url,
        protected_worker_runtime=True,
    )
    return PersonalDevPreparationRuntime(
        config=PersonalDevRuntimeConfig(
            minio_endpoint=settings.minio_endpoint,
            minio_region=settings.minio_region,
            ingress_class_name=settings.dev_instance_ingress_class_name,
            ingress_cert_manager_cluster_issuer=(
                settings.dev_instance_ingress_cert_manager_cluster_issuer
            ),
        ),
        sql=PsycopgSharedFixtureSqlExecutor(database_admin_url),
        buckets=S3BucketEnsurer(minio_client, region=settings.minio_region),
        vault=vault,
        object_store_tenant=KubectlMinioTenantProvisioner(
            kubectl=kubectl,
            vault=vault,
        ),
        cluster=KubectlCandidateGenerationProvisioner(kubectl=kubectl),
        access=PsycopgOwnerAccessBootstrap(database_admin_url),
    )


__all__ = [
    "DevInstanceProvisionerFactory",
    "build_dev_instance_provisioner_factory",
    "build_personal_dev_preparation_runtime",
]
