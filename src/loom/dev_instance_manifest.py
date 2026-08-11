"""Secret-free Kubernetes manifests for one external-storage dev instance."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from uuid import UUID

import yaml  # type: ignore[import-untyped]

from loom.dev_instance import DevInstanceIdentity
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS

_MANAGED_LABELS = {
    "app.kubernetes.io/managed-by": "loom-dev-instance-controller",
    "app.kubernetes.io/part-of": "loom",
}
_IMMUTABLE_IMAGE_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}@sha256:[0-9a-f]{64}",
)


@dataclass(frozen=True, slots=True)
class PersonalDevManifestBinding:
    """Durable lifecycle ownership stamped on every candidate object."""

    subject_id: UUID
    subject_incarnation: UUID
    operation_id: UUID
    attempt_id: UUID
    operation_epoch: int

    def __post_init__(self) -> None:
        if type(self.operation_epoch) is not int or self.operation_epoch <= 0:
            raise ValueError("personal-dev manifest operation epoch must be positive")


@dataclass(frozen=True, slots=True)
class DevInstanceManifestConfig:
    """Non-secret, operator-owned inputs for the per-instance workloads."""

    image_tag: str
    candidate_sha: str
    deployment_generation: int
    container_registry: str
    minio_endpoint: str
    image_references: Mapping[str, str] | None = None
    lifecycle_binding: PersonalDevManifestBinding | None = None
    minio_region: str = "us-east-1"
    ingress_class_name: str = "nginx"
    ingress_cert_manager_cluster_issuer: str = "letsencrypt-prod"
    image_pull_policy: str = "IfNotPresent"

    def __post_init__(self) -> None:
        if self.deployment_generation <= 0:
            raise ValueError("deployment_generation must be positive")
        if self.image_references is None:
            if len(self.candidate_sha) != 40 or any(
                char not in "0123456789abcdef" for char in self.candidate_sha
            ):
                raise ValueError("candidate_sha must be a full lowercase Git SHA")
            if self.candidate_sha[:7] not in self.image_tag:
                raise ValueError("image_tag must contain the candidate SHA prefix")
            if not self.container_registry or self.container_registry.endswith("/"):
                raise ValueError(
                    "container_registry must be non-empty without a trailing slash",
                )
        else:
            if len(self.candidate_sha) != 64 or any(
                char not in "0123456789abcdef" for char in self.candidate_sha
            ):
                raise ValueError("personal-dev candidate_sha must be a lowercase SHA-256 digest")
            references = dict(self.image_references)
            if set(references) != set(PERSONAL_DEV_COMPONENTS):
                raise ValueError(
                    "image references must contain the complete personal-dev component set"
                )
            if any(
                not isinstance(reference, str) or _IMMUTABLE_IMAGE_RE.fullmatch(reference) is None
                for reference in references.values()
            ):
                raise ValueError("every personal-dev image must be an immutable OCI reference")
            object.__setattr__(self, "image_references", MappingProxyType(references))
            if self.lifecycle_binding is None:
                raise ValueError("personal-dev manifests require a lifecycle binding")
        if not self.minio_endpoint.startswith(("http://", "https://")):
            raise ValueError("minio_endpoint must be an HTTP(S) URL")
        if self.image_pull_policy not in {"Always", "IfNotPresent", "Never"}:
            raise ValueError("image_pull_policy is invalid")

    def image(self, component: str) -> str:
        if self.image_references is not None:
            try:
                return self.image_references[component]
            except KeyError:
                raise ValueError("component is absent from the personal-dev image set") from None
        return f"{self.container_registry}/loom-{component}:{self.image_tag}"


def _secret_env(name: str, key: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": "loom-secrets", "key": key}},
    }


def _literal_env(name: str, value: str) -> dict[str, str]:
    return {"name": name, "value": value}


def _lifecycle_labels(config: DevInstanceManifestConfig) -> dict[str, str]:
    if config.lifecycle_binding is None:
        return {}
    binding = config.lifecycle_binding
    return {
        "loom.dev/subject": str(binding.subject_id),
        "loom.dev/incarnation": str(binding.subject_incarnation),
        "loom.dev/operation": str(binding.operation_id),
        "loom.dev/attempt": str(binding.attempt_id),
        "loom.dev/operation-epoch": str(binding.operation_epoch),
        "loom.dev/generation": str(config.deployment_generation),
    }


def _metadata(
    name: str,
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
) -> dict[str, Any]:
    return {
        "name": name,
        "namespace": identity.namespace,
        "labels": {
            **_MANAGED_LABELS,
            "loom.dev/instance": identity.name,
            "loom.dev/environment": identity.runtime_environment,
            "loom.dev/candidate": config.candidate_sha[:12],
            **_lifecycle_labels(config),
        },
    }


def _admin_volume(mount_path: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        [{"name": "loom-admin-secret", "mountPath": mount_path, "readOnly": True}],
        [
            {
                "name": "loom-admin-secret",
                "secret": {"secretName": "loom-admin-secret", "defaultMode": 0o440},
            }
        ],
    )


def _deployment(
    *,
    name: str,
    component: str,
    container_name: str,
    port: int,
    readiness_path: str,
    env: list[dict[str, Any]],
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
    admin_mount_path: str,
) -> dict[str, Any]:
    mounts, volumes = _admin_volume(admin_mount_path)
    labels = {
        "app": name,
        "loom.dev/instance": identity.name,
        "loom.dev/generation": str(config.deployment_generation),
        **_lifecycle_labels(config),
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata(name, identity, config),
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 2,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 65532,
                        "runAsGroup": 65532,
                        "fsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": container_name,
                            "image": config.image(component),
                            "imagePullPolicy": config.image_pull_policy,
                            "env": env,
                            "ports": [{"containerPort": port}],
                            "readinessProbe": {
                                "httpGet": {"path": readiness_path, "port": port},
                                "periodSeconds": 5,
                                "failureThreshold": 12,
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "100m", "memory": "192Mi"},
                                "limits": {"cpu": "1", "memory": "1Gi"},
                            },
                            "volumeMounts": mounts,
                        }
                    ],
                    "volumes": volumes,
                },
            },
        },
    }


def _service(
    name: str,
    port: int,
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
    *,
    target_port: int | None = None,
    selector_app: str | None = None,
    generation: int | None = None,
) -> dict[str, Any]:
    selector = {
        "app": selector_app or name,
        "loom.dev/instance": identity.name,
    }
    if generation is not None:
        selector["loom.dev/generation"] = str(generation)
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata(name, identity, config),
        "spec": {
            "selector": selector,
            "ports": [{"port": port, "targetPort": target_port or port}],
        },
    }


def _web_deployment(
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
    *,
    name: str = "loom-web",
) -> dict[str, Any]:
    labels = {
        "app": name,
        "loom.dev/instance": identity.name,
        "loom.dev/generation": str(config.deployment_generation),
        **_lifecycle_labels(config),
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata(name, identity, config),
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 2,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 101,
                        "runAsGroup": 101,
                        "fsGroup": 101,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "loom-web",
                            "image": config.image("web"),
                            "imagePullPolicy": config.image_pull_policy,
                            "env": [
                                _literal_env(
                                    "LOOM_FRONTEND_ENVIRONMENT",
                                    identity.runtime_environment,
                                ),
                                _literal_env(
                                    "LOOM_FRONTEND_ENVIRONMENT_LABEL",
                                    f"Personal development: {identity.name}",
                                ),
                                _literal_env("LOOM_FRONTEND_ROUTE_PATH", identity.route_path),
                                _literal_env("LOOM_FRONTEND_API_BASE", ""),
                                _literal_env(
                                    "LOOM_FRONTEND_PUBLIC_ORIGIN",
                                    f"https://{identity.route_host}",
                                ),
                            ],
                            "ports": [{"containerPort": 8080}],
                            "readinessProbe": {
                                "httpGet": {"path": "/", "port": 8080},
                                "periodSeconds": 5,
                                "failureThreshold": 12,
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "32Mi"},
                                "limits": {"cpu": "200m", "memory": "128Mi"},
                            },
                        },
                    ],
                },
            },
        },
    }


def dev_instance_manifest_documents(
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
) -> tuple[dict[str, Any], ...]:
    """Return namespace, migration, and runtime documents with no secret values."""
    personal_candidate = config.image_references is not None
    generation_suffix = f"-g{config.deployment_generation}" if personal_candidate else ""
    cp_name = f"loom-control-plane{generation_suffix}"
    gateway_name = f"loom-llm-gateway{generation_suffix}"
    service_name = f"loom-service{generation_suffix}"
    web_name = f"loom-web{generation_suffix}"
    common = [
        _literal_env("LOOM_ENV", identity.runtime_environment),
        _literal_env("LOOM_NAMESPACE", identity.namespace),
    ]
    cp_env = [
        _secret_env("LOOM_CP_DB_URL", "cp-db-url"),
        _secret_env("LOOM_CP_STEP_JWT_SIGNING_KEY", "step-jwt-signing-key"),
        _secret_env("LOOM_CP_MINIO_ACCESS_KEY", "minio-access-key"),
        _secret_env("LOOM_CP_MINIO_SECRET_KEY", "minio-secret-key"),
        _literal_env("LOOM_CP_MINIO_ENDPOINT", config.minio_endpoint),
        _literal_env("LOOM_CP_MINIO_REGION", config.minio_region),
        _literal_env("LOOM_CP_ARTIFACTS_BUCKET", identity.artifacts_bucket),
        _literal_env("LOOM_CP_TRAJECTORIES_BUCKET", identity.trajectories_bucket),
        _literal_env("LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED", "false"),
        _literal_env("LOOM_CP_ADMIN_SECRET_FILE", "/var/run/loom/admin/secrets.toml"),
        *common,
    ]
    gw_env = [
        _secret_env("LOOM_GW_DB_URL", "gw-db-url"),
        _secret_env("LOOM_GW_STEP_JWT_SIGNING_KEY", "step-jwt-signing-key"),
        _secret_env("LOOM_SECRET_STORE_MASTER_KEY", "secret-store-master-key"),
        _literal_env("LOOM_GW_ADMIN_SECRET_FILE", "/var/run/loom/admin/secrets.toml"),
        *common,
    ]
    svc_env = [
        _secret_env("LOOM_SVC_DB_URL", "svc-db-url"),
        _secret_env("LOOM_SVC_MINIO_ACCESS_KEY", "minio-access-key"),
        _secret_env("LOOM_SVC_MINIO_SECRET_KEY", "minio-secret-key"),
        _secret_env("LOOM_SECRET_STORE_MASTER_KEY", "secret-store-master-key"),
        _literal_env("LOOM_SVC_MINIO_ENDPOINT", config.minio_endpoint),
        _literal_env("LOOM_SVC_MINIO_REGION", config.minio_region),
        _literal_env("LOOM_SVC_ARTIFACTS_BUCKET", identity.artifacts_bucket),
        _literal_env("LOOM_SVC_TRAJECTORIES_BUCKET", identity.trajectories_bucket),
        _literal_env("LOOM_SVC_CONTROL_PLANE_URL", f"http://{cp_name}:8080"),
        _literal_env("LOOM_SVC_GATEWAY_URL", f"http://{gateway_name}:9100"),
        _literal_env("LOOM_SVC_K8S_WORKER_ENABLED", "false"),
        _literal_env("LOOM_SVC_ADMIN_SECRET_FILE", "/var/run/loom/admin/secrets.toml"),
        *common,
    ]
    namespace = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": identity.namespace,
            "labels": {
                **_MANAGED_LABELS,
                "loom.dev/instance": identity.name,
                "pod-security.kubernetes.io/enforce": "restricted",
                **_lifecycle_labels(config),
            },
        },
    }
    migration = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": _metadata(
            f"loom-migrate-{config.candidate_sha[:7]}-g{config.deployment_generation}",
            identity,
            config,
        ),
        "spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": 600,
            "ttlSecondsAfterFinished": 600,
            "template": {
                "metadata": {
                    "labels": {
                        "app": "loom-migration",
                        **_lifecycle_labels(config),
                    }
                },
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 65532,
                        "runAsGroup": 65532,
                        "fsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "migrate",
                            "image": config.image("control-plane"),
                            "imagePullPolicy": config.image_pull_policy,
                            "command": [
                                "alembic",
                                "-c",
                                "migrations/alembic.ini",
                                "upgrade",
                                "head",
                            ],
                            "env": [_secret_env("LOOM_DB_URL", "cp-db-url")],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "runAsNonRoot": True,
                            },
                        }
                    ],
                },
            },
        },
    }
    cp = _deployment(
        name=cp_name,
        component="control-plane",
        container_name="control-plane",
        port=8080,
        readiness_path="/healthz",
        env=cp_env,
        identity=identity,
        config=config,
        admin_mount_path="/var/run/loom/admin",
    )
    gateway = _deployment(
        name=gateway_name,
        component="llm-gateway",
        container_name="gateway",
        port=9100,
        readiness_path="/healthz",
        env=gw_env,
        identity=identity,
        config=config,
        admin_mount_path="/var/run/loom/admin",
    )
    service = _deployment(
        name=service_name,
        component="service",
        container_name="loom-service",
        port=8090,
        readiness_path="/api/v1/health",
        env=svc_env,
        identity=identity,
        config=config,
        admin_mount_path="/var/run/loom/admin",
    )
    web = _web_deployment(identity, config, name=web_name)
    ingress = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            **_metadata("loom-dev", identity, config),
            "annotations": {
                "cert-manager.io/cluster-issuer": (config.ingress_cert_manager_cluster_issuer),
                "nginx.ingress.kubernetes.io/proxy-read-timeout": "300",
            },
        },
        "spec": {
            "ingressClassName": config.ingress_class_name,
            "rules": [
                {
                    "host": identity.route_host,
                    "http": {
                        "paths": [
                            {
                                "path": "/api/v1",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "loom-service",
                                        "port": {"number": 8090},
                                    }
                                },
                            },
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "loom-web",
                                        "port": {"number": 80},
                                    }
                                },
                            },
                        ]
                    },
                },
                {
                    "host": identity.worker_control_plane_host,
                    "http": {
                        "paths": [
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "loom-control-plane",
                                        "port": {"number": 8080},
                                    }
                                },
                            }
                        ]
                    },
                },
                {
                    "host": identity.worker_gateway_host,
                    "http": {
                        "paths": [
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "loom-llm-gateway",
                                        "port": {"number": 9100},
                                    }
                                },
                            }
                        ]
                    },
                },
            ],
            "tls": [
                {
                    "hosts": [
                        identity.route_host,
                        identity.worker_control_plane_host,
                        identity.worker_gateway_host,
                    ],
                    "secretName": "loom-dev-tls",
                }
            ],
        },
    }
    preparation = (
        namespace,
        migration,
        cp,
        _service(
            cp_name,
            8080,
            identity,
            config,
            generation=(config.deployment_generation if personal_candidate else None),
        ),
        gateway,
        _service(
            gateway_name,
            9100,
            identity,
            config,
            generation=(config.deployment_generation if personal_candidate else None),
        ),
        service,
        _service(
            service_name,
            8090,
            identity,
            config,
            generation=(config.deployment_generation if personal_candidate else None),
        ),
        web,
        _service(
            web_name,
            80,
            identity,
            config,
            target_port=8080,
            generation=(config.deployment_generation if personal_candidate else None),
        ),
    )
    if not personal_candidate:
        return (*preparation, ingress)
    activation = (
        _service(
            "loom-control-plane",
            8080,
            identity,
            config,
            selector_app=cp_name,
            generation=config.deployment_generation,
        ),
        _service(
            "loom-llm-gateway",
            9100,
            identity,
            config,
            selector_app=gateway_name,
            generation=config.deployment_generation,
        ),
        _service(
            "loom-service",
            8090,
            identity,
            config,
            selector_app=service_name,
            generation=config.deployment_generation,
        ),
        _service(
            "loom-web",
            80,
            identity,
            config,
            target_port=8080,
            selector_app=web_name,
            generation=config.deployment_generation,
        ),
        ingress,
    )
    return (*preparation, *activation)


def personal_dev_preparation_manifest_documents(
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
) -> tuple[dict[str, Any], ...]:
    """Return candidate-generation objects without stable routing mutation."""
    if config.image_references is None:
        raise ValueError("personal-dev preparation requires immutable image references")
    documents = dev_instance_manifest_documents(identity, config)
    activation_count = 5
    return documents[:-activation_count]


def personal_dev_activation_manifest_documents(
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
) -> tuple[dict[str, Any], ...]:
    """Return only the protected stable-service and ingress cutover objects."""
    if config.image_references is None:
        raise ValueError("personal-dev activation requires immutable image references")
    documents = dev_instance_manifest_documents(identity, config)
    activation_count = 5
    return documents[-activation_count:]


def render_dev_instance_manifests(
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
) -> str:
    """Serialize the secret-free manifest set as stable multi-document YAML."""
    return str(
        yaml.safe_dump_all(
            dev_instance_manifest_documents(identity, config),
            sort_keys=False,
            explicit_start=True,
        )
    )


__all__ = [
    "DevInstanceManifestConfig",
    "PersonalDevManifestBinding",
    "dev_instance_manifest_documents",
    "personal_dev_activation_manifest_documents",
    "personal_dev_preparation_manifest_documents",
    "render_dev_instance_manifests",
]
