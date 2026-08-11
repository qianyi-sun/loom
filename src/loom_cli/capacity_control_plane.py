"""Typed render model for the inert global capacity authority in ``loom-dev``."""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_capacity_manager.schema_startup import _capacity_head

_DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?")
_LABEL_NAME_RE = re.compile(r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?")
_OCI_DIGEST_RE = re.compile(
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
    r"(?::[A-Za-z0-9][A-Za-z0-9._-]{0,127})?@sha256:[0-9a-f]{64}"
)
_CPU_RE = re.compile(r"([1-9][0-9]*)(m)?")
_MEMORY_RE = re.compile(r"([1-9][0-9]*)(Ki|Mi|Gi)")
_MAX_CPU_MILLICORES = 64_000
_MAX_RESOURCE_MEMORY_BYTES = 1024**4
_MAX_POSTGRES_STORAGE_BYTES = 64 * 1024**4


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _is_immutable_oci_reference(value: str) -> bool:
    if _OCI_DIGEST_RE.fullmatch(value) is None or value.endswith(
        "@sha256:" + "0" * 64
    ):
        return False
    name = value.rsplit("@sha256:", 1)[0]
    return len(name) <= 255 and ("/" in name or name.count(":") <= 1)


def _validate_dns_label(value: str, *, label: str) -> str:
    if _DNS_LABEL_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a Kubernetes DNS label")
    return value


def _validate_label_key(value: str) -> str:
    parts = value.split("/")
    if len(parts) > 2 or not parts[-1] or _LABEL_NAME_RE.fullmatch(parts[-1]) is None:
        raise ValueError("Kubernetes label key is invalid")
    if len(parts) == 2 and (
        len(parts[0]) > 253
        or any(_DNS_LABEL_RE.fullmatch(segment) is None for segment in parts[0].split("."))
    ):
        raise ValueError("Kubernetes label key is invalid")
    return value


def _cpu_millicores(value: str) -> int:
    matched = _CPU_RE.fullmatch(value)
    if matched is None:
        raise ValueError("CPU resource quantity must be a positive integer or millicores")
    amount = int(matched.group(1))
    return amount if matched.group(2) else amount * 1000


def _memory_bytes(value: str) -> int:
    matched = _MEMORY_RE.fullmatch(value)
    if matched is None:
        raise ValueError("memory resource quantity must use positive Ki, Mi, or Gi")
    multipliers = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3}
    return int(matched.group(1)) * multipliers[matched.group(2)]


class ResourceEnvelope(_StrictModel):
    cpu_request: str
    memory_request: str
    cpu_limit: str
    memory_limit: str

    @model_validator(mode="after")
    def _bounded(self) -> ResourceEnvelope:
        cpu_request = _cpu_millicores(self.cpu_request)
        cpu_limit = _cpu_millicores(self.cpu_limit)
        memory_request = _memory_bytes(self.memory_request)
        memory_limit = _memory_bytes(self.memory_limit)
        if cpu_request > cpu_limit:
            raise ValueError("CPU request exceeds its limit")
        if cpu_limit > _MAX_CPU_MILLICORES:
            raise ValueError("CPU resource limit exceeds the control-plane bound")
        if memory_request > memory_limit:
            raise ValueError("memory request exceeds its limit")
        if memory_limit > _MAX_RESOURCE_MEMORY_BYTES:
            raise ValueError("memory resource limit exceeds the control-plane bound")
        return self

    def kubernetes(self) -> dict[str, dict[str, str]]:
        return {
            "requests": {"cpu": self.cpu_request, "memory": self.memory_request},
            "limits": {"cpu": self.cpu_limit, "memory": self.memory_limit},
        }


class PodSelector(_StrictModel):
    pod_label_key: str = Field(min_length=1, max_length=317)
    pod_label_value: str = Field(min_length=1, max_length=63)

    @field_validator("pod_label_key")
    @classmethod
    def _key(cls, value: str) -> str:
        return _validate_label_key(value)

    @field_validator("pod_label_value")
    @classmethod
    def _value(cls, value: str) -> str:
        if _LABEL_NAME_RE.fullmatch(value) is None:
            raise ValueError("Kubernetes label value is invalid")
        return value

    def match_labels(self) -> dict[str, str]:
        return {self.pod_label_key: self.pod_label_value}


class KubernetesEndpointSelector(PodSelector):
    namespace: str
    port: int = Field(ge=1, le=65535)

    @field_validator("namespace")
    @classmethod
    def _namespace(cls, value: str) -> str:
        return _validate_dns_label(value, label="selector namespace")


class CapacityControlPlaneProfile(_StrictModel):
    schema_version: Literal[1]
    namespace: Literal["loom-dev"]
    secret_name: str
    postgres_image: str
    postgres_storage: str
    postgres_resources: ResourceEnvelope
    migration_resources: ResourceEnvelope
    manager_resources: ResourceEnvelope
    dns: KubernetesEndpointSelector
    capacity_agent_client: PodSelector
    lifecycle_client: PodSelector
    storage_class_name: str | None = None

    @field_validator("secret_name")
    @classmethod
    def _secret_name(cls, value: str) -> str:
        return _validate_dns_label(value, label="capacity Secret name")

    @field_validator("postgres_image")
    @classmethod
    def _postgres_image(cls, value: str) -> str:
        if not _is_immutable_oci_reference(value):
            raise ValueError("capacity PostgreSQL image must be an immutable OCI reference")
        return value

    @field_validator("postgres_storage")
    @classmethod
    def _postgres_storage(cls, value: str) -> str:
        if _MEMORY_RE.fullmatch(value) is None:
            raise ValueError("capacity PostgreSQL storage must use positive Ki, Mi, or Gi")
        if _memory_bytes(value) > _MAX_POSTGRES_STORAGE_BYTES:
            raise ValueError("capacity PostgreSQL storage exceeds the control-plane bound")
        return value

    @field_validator("storage_class_name")
    @classmethod
    def _storage_class(cls, value: str | None) -> str | None:
        if value is not None and (
            len(value) > 253
            or any(_DNS_LABEL_RE.fullmatch(segment) is None for segment in value.split("."))
        ):
            raise ValueError("capacity storage class name is invalid")
        return value


def load_capacity_control_plane_profile(path: Path) -> CapacityControlPlaneProfile:
    """Load one strict non-secret infrastructure profile."""

    return CapacityControlPlaneProfile.model_validate(
        tomllib.loads(path.read_text(encoding="utf-8"))
    )


_MANAGED_LABELS = {
    "app.kubernetes.io/managed-by": "loom-capacity-control-plane",
    "app.kubernetes.io/part-of": "loom",
}
_COMPONENT_LABEL = "loom.yylx.dev/capacity-component"
_RUNTIME_ROOT = "/var/run/loom-capacity-manager"
_CREDENTIALS = f"{_RUNTIME_ROOT}/runtime/credentials"
_MANAGER_CREDENTIAL_FILES = (
    "client-ca.pem",
    "database-url",
    "health-certificate.pem",
    "health-private-key.pem",
    "ownership-public-keys.json",
    "principals.json",
    "server-ca.pem",
    "server-certificate.pem",
    "server-private-key.pem",
)


def _metadata(name: str, *, namespace: bool = True) -> dict[str, Any]:
    value: dict[str, Any] = {"name": name, "labels": dict(_MANAGED_LABELS)}
    if namespace:
        value["namespace"] = "loom-dev"
    return value


def _pod_labels(name: str, component: str) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": name,
        "app.kubernetes.io/part-of": "loom",
        _COMPONENT_LABEL: component,
    }


def _container_security(*, read_only_root: bool) -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": read_only_root,
    }


def _pod_security(user: int) -> dict[str, Any]:
    return {
        "runAsNonRoot": True,
        "runAsUser": user,
        "runAsGroup": user,
        "fsGroup": user,
        "fsGroupChangePolicy": "OnRootMismatch",
        "seccompProfile": {"type": "RuntimeDefault"},
    }


def _credential_parts(
    *,
    manager_image: str,
    secret_name: str,
    profile: Literal["manager", "migration"],
    resources: ResourceEnvelope,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    files = _MANAGER_CREDENTIAL_FILES if profile == "manager" else ("database-url",)
    init_mounts: list[dict[str, Any]] = [
        {
            "name": "projected",
            "mountPath": f"{_RUNTIME_ROOT}/projected",
            "readOnly": True,
        },
        {"name": "runtime", "mountPath": f"{_RUNTIME_ROOT}/runtime"},
    ]
    application_mounts: list[dict[str, Any]] = [
        {
            "name": "runtime",
            "mountPath": f"{_RUNTIME_ROOT}/runtime",
            "readOnly": True,
        }
    ]
    init: dict[str, Any] = {
        "name": "prepare-credentials",
        "image": manager_image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["python", "-m", "loom_capacity_manager.secret_init"],
        "args": [
            "--profile",
            profile,
            "--source",
            f"{_RUNTIME_ROOT}/projected",
            "--destination",
            _CREDENTIALS,
        ],
        "securityContext": _container_security(read_only_root=True),
        "resources": resources.kubernetes(),
        "volumeMounts": init_mounts,
    }
    volumes: list[dict[str, Any]] = [
        {
            "name": "projected",
            "secret": {
                "secretName": secret_name,
                "defaultMode": 0o440,
                "items": [{"key": filename, "path": filename} for filename in files],
            },
        },
        {
            "name": "runtime",
            "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"},
        },
    ]
    return [init], application_mounts, volumes


def _postgres_service() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata("loom-capacity-postgres"),
        "spec": {
            "clusterIP": "None",
            "selector": {"app.kubernetes.io/name": "loom-capacity-postgres"},
            "ports": [
                {
                    "name": "postgres",
                    "protocol": "TCP",
                    "port": 5432,
                    "targetPort": 5432,
                }
            ],
        },
    }


def _postgres_statefulset(profile: CapacityControlPlaneProfile) -> dict[str, Any]:
    labels = _pod_labels("loom-capacity-postgres", "database")
    secret_env = [
        {
            "name": environment,
            "valueFrom": {
                "secretKeyRef": {"name": profile.secret_name, "key": secret_key}
            },
        }
        for environment, secret_key in (
            ("POSTGRES_USER", "postgres-user"),
            ("POSTGRES_PASSWORD", "postgres-password"),
            ("POSTGRES_DB", "postgres-database"),
        )
    ]
    claim_spec: dict[str, Any] = {
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": profile.postgres_storage}},
    }
    if profile.storage_class_name is not None:
        claim_spec["storageClassName"] = profile.storage_class_name
    return {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": _metadata("loom-capacity-postgres"),
        "spec": {
            "serviceName": "loom-capacity-postgres",
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "securityContext": _pod_security(999),
                    "containers": [
                        {
                            "name": "postgres",
                            "image": profile.postgres_image,
                            "imagePullPolicy": "IfNotPresent",
                            "args": ["-c", "max_connections=200"],
                            "env": [
                                *secret_env,
                                {
                                    "name": "PGDATA",
                                    "value": "/var/lib/postgresql/data/pgdata",
                                },
                            ],
                            "ports": [{"name": "postgres", "containerPort": 5432}],
                            "readinessProbe": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-ec",
                                        'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
                                    ]
                                },
                                "periodSeconds": 5,
                                "failureThreshold": 12,
                                "timeoutSeconds": 3,
                            },
                            "livenessProbe": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-ec",
                                        'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
                                    ]
                                },
                                "periodSeconds": 10,
                                "failureThreshold": 6,
                                "timeoutSeconds": 3,
                            },
                            "securityContext": _container_security(read_only_root=True),
                            "resources": profile.postgres_resources.kubernetes(),
                            "volumeMounts": [
                                {
                                    "name": "data",
                                    "mountPath": "/var/lib/postgresql/data",
                                },
                                {"name": "run", "mountPath": "/var/run/postgresql"},
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "run", "emptyDir": {"medium": "Memory"}},
                        {"name": "tmp", "emptyDir": {"medium": "Memory"}},
                    ],
                },
            },
            "volumeClaimTemplates": [
                {"metadata": {"name": "data"}, "spec": claim_spec}
            ],
        },
    }


def _migration_job(
    profile: CapacityControlPlaneProfile,
    *,
    manager_image: str,
    authority_incarnation: UUID,
    migration_head: str,
    image_digest: str,
) -> dict[str, Any]:
    labels = _pod_labels("loom-capacity-migrate", "migration")
    init, mounts, volumes = _credential_parts(
        manager_image=manager_image,
        secret_name=profile.secret_name,
        profile="migration",
        resources=profile.migration_resources,
    )
    job_spec: dict[str, Any] = {
        "backoffLimit": 6,
        "template": {
            "metadata": {"labels": labels},
            "spec": {
                "automountServiceAccountToken": False,
                "restartPolicy": "Never",
                "securityContext": _pod_security(65532),
                "initContainers": init,
                "containers": [
                    {
                        "name": "migration",
                        "image": manager_image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["python", "-m", "loom_capacity_manager.migrate"],
                        "args": [
                            "--db-url-file",
                            f"{_CREDENTIALS}/database-url",
                            "--expected-authority-incarnation",
                            str(authority_incarnation),
                        ],
                        "securityContext": _container_security(read_only_root=True),
                        "resources": profile.migration_resources.kubernetes(),
                        "volumeMounts": mounts,
                    }
                ],
                "volumes": volumes,
            },
        },
    }
    template_identity = hashlib.sha256(
        json.dumps(
            {"migration_head": migration_head, "spec": job_spec},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    head_slug = re.sub(
        r"[^a-z0-9-]+",
        "-",
        migration_head.lower().replace("_", "-"),
    ).strip("-")
    head_prefix = head_slug[:19].rstrip("-") or "migration"
    name = (
        f"loom-capacity-migrate-{head_prefix}-"
        f"{image_digest[:10]}-{template_identity[:10]}"
    )
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": _metadata(name),
        "spec": job_spec,
    }


def _manager_service() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata("loom-capacity-manager"),
        "spec": {
            "type": "ClusterIP",
            "selector": {"app.kubernetes.io/name": "loom-capacity-manager"},
            "ports": [
                {
                    "name": "https",
                    "protocol": "TCP",
                    "port": 8443,
                    "targetPort": 8443,
                }
            ],
        },
    }


def _manager_deployment(
    profile: CapacityControlPlaneProfile,
    *,
    manager_image: str,
    authority_incarnation: UUID,
) -> dict[str, Any]:
    labels = _pod_labels("loom-capacity-manager", "manager")
    init, mounts, volumes = _credential_parts(
        manager_image=manager_image,
        secret_name=profile.secret_name,
        profile="manager",
        resources=profile.manager_resources,
    )
    health_command = [
        "python",
        "-m",
        "loom_capacity_manager.health_probe",
        "--url",
        "https://127.0.0.1:8443/healthz",
        "--ca-file",
        f"{_CREDENTIALS}/server-ca.pem",
        "--certificate-file",
        f"{_CREDENTIALS}/health-certificate.pem",
        "--private-key-file",
        f"{_CREDENTIALS}/health-private-key.pem",
        "--server-certificate-file",
        f"{_CREDENTIALS}/server-certificate.pem",
    ]
    environment = [
        {"name": name, "value": value}
        for name, value in (
            ("LOOM_CAPACITY_PRINCIPALS_FILE", f"{_CREDENTIALS}/principals.json"),
            ("LOOM_CAPACITY_DB_URL_FILE", f"{_CREDENTIALS}/database-url"),
            (
                "LOOM_CAPACITY_EXPECTED_AUTHORITY_INCARNATION",
                str(authority_incarnation),
            ),
            (
                "LOOM_CAPACITY_TLS_CERT_FILE",
                f"{_CREDENTIALS}/server-certificate.pem",
            ),
            ("LOOM_CAPACITY_TLS_KEY_FILE", f"{_CREDENTIALS}/server-private-key.pem"),
            ("LOOM_CAPACITY_TLS_CLIENT_CA_FILE", f"{_CREDENTIALS}/client-ca.pem"),
            (
                "LOOM_CAPACITY_OWNERSHIP_PUBLIC_KEYS_FILE",
                f"{_CREDENTIALS}/ownership-public-keys.json",
            ),
            ("LOOM_CAPACITY_HOST", "0.0.0.0"),
            ("LOOM_CAPACITY_PORT", "8443"),
        )
    ]
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata("loom-capacity-manager"),
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 2,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "securityContext": _pod_security(65532),
                    "initContainers": init,
                    "containers": [
                        {
                            "name": "manager",
                            "image": manager_image,
                            "imagePullPolicy": "IfNotPresent",
                            "env": environment,
                            "ports": [{"name": "https", "containerPort": 8443}],
                            "startupProbe": {
                                "exec": {"command": health_command},
                                "periodSeconds": 5,
                                "failureThreshold": 60,
                                "timeoutSeconds": 4,
                            },
                            "readinessProbe": {
                                "exec": {"command": health_command},
                                "periodSeconds": 5,
                                "failureThreshold": 3,
                                "timeoutSeconds": 4,
                            },
                            "livenessProbe": {
                                "tcpSocket": {"port": 8443},
                                "periodSeconds": 10,
                                "failureThreshold": 3,
                                "timeoutSeconds": 3,
                            },
                            "securityContext": _container_security(read_only_root=True),
                            "resources": profile.manager_resources.kubernetes(),
                            "volumeMounts": mounts,
                        }
                    ],
                    "volumes": volumes,
                },
            },
        },
    }


def _component_selector(*components: str) -> dict[str, Any]:
    return {
        "matchExpressions": [
            {"key": _COMPONENT_LABEL, "operator": "In", "values": list(components)}
        ]
    }


def _network_policies(profile: CapacityControlPlaneProfile) -> list[dict[str, Any]]:
    namespace = {
        "namespaceSelector": {
            "matchLabels": {"kubernetes.io/metadata.name": profile.namespace}
        }
    }
    return [
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-default-deny"),
            "spec": {
                "podSelector": {
                    "matchExpressions": [
                        {"key": _COMPONENT_LABEL, "operator": "Exists"}
                    ]
                },
                "policyTypes": ["Ingress", "Egress"],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-dns-egress"),
            "spec": {
                "podSelector": _component_selector("manager", "migration"),
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": profile.dns.namespace
                                    }
                                },
                                "podSelector": {
                                    "matchLabels": profile.dns.match_labels()
                                },
                            }
                        ],
                        "ports": [
                            {"protocol": "UDP", "port": profile.dns.port},
                            {"protocol": "TCP", "port": profile.dns.port},
                        ],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-database-egress"),
            "spec": {
                "podSelector": _component_selector("manager", "migration"),
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                **namespace,
                                "podSelector": {
                                    "matchLabels": {
                                        "app.kubernetes.io/name": "loom-capacity-postgres"
                                    }
                                },
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 5432}],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-postgres-ingress"),
            "spec": {
                "podSelector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": "loom-capacity-postgres"
                    }
                },
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [
                            {"podSelector": _component_selector("manager", "migration")}
                        ],
                        "ports": [{"protocol": "TCP", "port": 5432}],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-manager-ingress"),
            "spec": {
                "podSelector": {
                    "matchLabels": {"app.kubernetes.io/name": "loom-capacity-manager"}
                },
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [
                            {
                                "namespaceSelector": {},
                                "podSelector": {
                                    "matchLabels": profile.capacity_agent_client.match_labels()
                                },
                            },
                            {
                                "podSelector": {
                                    "matchLabels": profile.lifecycle_client.match_labels()
                                }
                            },
                        ],
                        "ports": [{"protocol": "TCP", "port": 8443}],
                    }
                ],
            },
        },
    ]


def render_capacity_control_plane_manifests(
    profile: CapacityControlPlaneProfile,
    *,
    manager_image: str,
    authority_incarnation: UUID,
) -> str:
    """Render one exact, cluster-internal, zero-execution authority release."""

    if not isinstance(profile, CapacityControlPlaneProfile):
        raise TypeError("capacity control-plane profile is invalid")
    if not _is_immutable_oci_reference(manager_image):
        raise ValueError("capacity manager image must be an immutable OCI reference")
    if not isinstance(authority_incarnation, UUID):
        raise TypeError("capacity authority incarnation must be a UUID")
    if authority_incarnation.int == 0:
        raise ValueError("capacity authority incarnation must be non-nil")
    image_digest = manager_image.rsplit("@sha256:", 1)[1]
    migration_head = _capacity_head()
    documents = [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                **_metadata("loom-dev", namespace=False),
                "labels": {
                    **_MANAGED_LABELS,
                    "app.kubernetes.io/managed-by": "loom-operator",
                    "pod-security.kubernetes.io/enforce": "restricted",
                    "pod-security.kubernetes.io/enforce-version": "latest",
                    "pod-security.kubernetes.io/audit": "restricted",
                    "pod-security.kubernetes.io/audit-version": "latest",
                    "pod-security.kubernetes.io/warn": "restricted",
                    "pod-security.kubernetes.io/warn-version": "latest",
                },
            },
        },
        _postgres_service(),
        _postgres_statefulset(profile),
        _migration_job(
            profile,
            manager_image=manager_image,
            authority_incarnation=authority_incarnation,
            migration_head=migration_head,
            image_digest=image_digest,
        ),
        _manager_service(),
        _manager_deployment(
            profile,
            manager_image=manager_image,
            authority_incarnation=authority_incarnation,
        ),
        *_network_policies(profile),
    ]
    return cast(
        str,
        yaml.safe_dump_all(documents, sort_keys=False, explicit_start=False),
    )


__all__ = [
    "CapacityControlPlaneProfile",
    "KubernetesEndpointSelector",
    "PodSelector",
    "ResourceEnvelope",
    "load_capacity_control_plane_profile",
    "render_capacity_control_plane_manifests",
]
