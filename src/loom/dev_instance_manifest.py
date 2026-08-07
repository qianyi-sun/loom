"""Secret-free Kubernetes manifests for one external-storage dev instance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml  # type: ignore[import-untyped]

from loom.dev_instance import DevInstanceIdentity

_MANAGED_LABELS = {
    "app.kubernetes.io/managed-by": "loom-dev-instance-controller",
    "app.kubernetes.io/part-of": "loom",
}


@dataclass(frozen=True, slots=True)
class DevInstanceManifestConfig:
    """Non-secret, operator-owned inputs for the per-instance workloads."""

    image_tag: str
    candidate_sha: str
    deployment_generation: int
    container_registry: str
    minio_endpoint: str
    minio_region: str = "us-east-1"
    ingress_class_name: str = "nginx"
    ingress_cert_manager_cluster_issuer: str = "letsencrypt-prod"
    image_pull_policy: str = "IfNotPresent"

    def __post_init__(self) -> None:
        if len(self.candidate_sha) != 40 or any(
            char not in "0123456789abcdef" for char in self.candidate_sha
        ):
            raise ValueError("candidate_sha must be a full lowercase Git SHA")
        if self.deployment_generation <= 0:
            raise ValueError("deployment_generation must be positive")
        if self.candidate_sha[:7] not in self.image_tag:
            raise ValueError("image_tag must contain the candidate SHA prefix")
        if not self.container_registry or self.container_registry.endswith("/"):
            raise ValueError("container_registry must be non-empty without a trailing slash")
        if not self.minio_endpoint.startswith(("http://", "https://")):
            raise ValueError("minio_endpoint must be an HTTP(S) URL")
        if self.image_pull_policy not in {"Always", "IfNotPresent", "Never"}:
            raise ValueError("image_pull_policy is invalid")

    def image(self, component: str) -> str:
        return f"{self.container_registry}/loom-{component}:{self.image_tag}"


def _secret_env(name: str, key: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": "loom-secrets", "key": key}},
    }


def _literal_env(name: str, value: str) -> dict[str, str]:
    return {"name": name, "value": value}


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
    labels = {"app": name, "loom.dev/instance": identity.name}
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
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata(name, identity, config),
        "spec": {
            "selector": {"app": name, "loom.dev/instance": identity.name},
            "ports": [{"port": port, "targetPort": port}],
        },
    }


def dev_instance_manifest_documents(
    identity: DevInstanceIdentity,
    config: DevInstanceManifestConfig,
) -> tuple[dict[str, Any], ...]:
    """Return namespace, migration, and runtime documents with no secret values."""
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
        _literal_env("LOOM_SVC_CONTROL_PLANE_URL", "http://loom-control-plane:8080"),
        _literal_env("LOOM_SVC_GATEWAY_URL", "http://loom-llm-gateway:9100"),
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
                "metadata": {"labels": {"app": "loom-migration"}},
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
        name="loom-control-plane",
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
        name="loom-llm-gateway",
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
        name="loom-service",
        component="service",
        container_name="loom-service",
        port=8090,
        readiness_path="/api/v1/health",
        env=svc_env,
        identity=identity,
        config=config,
        admin_mount_path="/var/run/loom/admin",
    )
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
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "loom-service",
                                        "port": {"number": 8090},
                                    }
                                },
                            }
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
    return (
        namespace,
        migration,
        cp,
        _service("loom-control-plane", 8080, identity, config),
        gateway,
        _service("loom-llm-gateway", 9100, identity, config),
        service,
        _service("loom-service", 8090, identity, config),
        ingress,
    )


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
    "dev_instance_manifest_documents",
    "render_dev_instance_manifests",
]
