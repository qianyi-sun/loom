"""Secret-free ephemeral Kubernetes contract for personal candidate builds."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevPlatform,
)

_IMMUTABLE_IMAGE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}@sha256:[0-9a-f]{64}")
_STORAGE_RE = re.compile(r"[1-9][0-9]*(?:Mi|Gi)")
_CPU_RE = re.compile(r"[1-9][0-9]*")
_MANAGED_LABELS = {
    "app.kubernetes.io/managed-by": "loom-personal-dev-builder-controller",
    "app.kubernetes.io/part-of": "loom",
}


@dataclass(frozen=True, slots=True)
class PersonalDevBuilderManifestConfig:
    """Operator-owned sandbox image and finite resource envelope."""

    builder_image: str
    cpu_request: str = "1"
    cpu_limit: str = "4"
    memory_request: str = "1Gi"
    memory_limit: str = "8Gi"
    ephemeral_storage_request: str = "4Gi"
    ephemeral_storage_limit: str = "20Gi"
    active_deadline_seconds: int = 3600
    ttl_seconds_after_finished: int = 600
    max_artifact_bytes: int = 6 * 1024 * 1024 * 1024
    max_image_archive_bytes: int = 2 * 1024 * 1024 * 1024
    capability_secret_name: str = "build-capability"
    shared_namespace: str = "loom-dev"
    runtime_class_name: str = "loom-personal-dev-builder"
    image_pull_policy: str = "IfNotPresent"

    def __post_init__(self) -> None:
        if _IMMUTABLE_IMAGE_RE.fullmatch(self.builder_image) is None:
            raise ValueError("personal-dev builder image must be immutable")
        if any(
            _CPU_RE.fullmatch(value) is None for value in (self.cpu_request, self.cpu_limit)
        ) or any(
            _STORAGE_RE.fullmatch(value) is None
            for value in (
                self.memory_request,
                self.memory_limit,
                self.ephemeral_storage_request,
                self.ephemeral_storage_limit,
            )
        ):
            raise ValueError("personal-dev builder resource envelope is invalid")
        if (
            type(self.active_deadline_seconds) is not int
            or not 1 <= self.active_deadline_seconds <= 7200
            or type(self.ttl_seconds_after_finished) is not int
            or not 60 <= self.ttl_seconds_after_finished <= 86400
        ):
            raise ValueError("personal-dev builder time envelope is invalid")
        if (
            type(self.max_artifact_bytes) is not int
            or type(self.max_image_archive_bytes) is not int
            or self.max_artifact_bytes <= 0
            or not 0 < self.max_image_archive_bytes <= self.max_artifact_bytes
        ):
            raise ValueError("personal-dev builder artifact envelope is invalid")
        if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,38}[a-z0-9])?", self.capability_secret_name):
            raise ValueError("personal-dev builder capability secret name is invalid")
        if self.shared_namespace != "loom-dev":
            raise ValueError("personal-dev builder shared namespace must be loom-dev")
        if (
            re.fullmatch(
                r"[a-z0-9](?:[-a-z0-9.]{0,61}[a-z0-9])?",
                self.runtime_class_name,
            )
            is None
        ):
            raise ValueError("personal-dev builder runtime class is invalid")
        if self.image_pull_policy not in {"Always", "IfNotPresent", "Never"}:
            raise ValueError("personal-dev builder image pull policy is invalid")


def _attempt(registration: CandidateRegistration) -> PersonalDevCandidateBuildAttemptRecord:
    attempt = registration.build_attempt
    candidate = registration.candidate
    if (
        attempt is None
        or attempt.candidate_id != candidate.id
        or attempt.state not in {"claimed", "running"}
        or attempt.lease_epoch <= 0
        or candidate.status != "building"
    ):
        raise ValueError("personal-dev builder registration is not a live exact attempt")
    return attempt


def _labels(registration: CandidateRegistration) -> dict[str, str]:
    attempt = _attempt(registration)
    return {
        **_MANAGED_LABELS,
        "loom.dev/candidate": registration.candidate.candidate_sha[:12],
        "loom.dev/subject": str(attempt.subject_id),
        "loom.dev/incarnation": str(attempt.subject_incarnation),
        "loom.dev/operation": str(attempt.operation_id),
        "loom.dev/attempt": str(attempt.id),
        "loom.dev/operation-epoch": str(attempt.operation_epoch),
        "loom.dev/build-attempt-sequence": str(attempt.attempt_sequence),
        "loom.dev/build-lease-epoch": str(attempt.lease_epoch),
    }


def _metadata(
    name: str,
    namespace: str,
    registration: CandidateRegistration,
) -> dict[str, object]:
    return {
        "name": name,
        "namespace": namespace,
        "labels": _labels(registration),
    }


def _contract(
    registration: CandidateRegistration,
    *,
    platform: str,
    config: PersonalDevBuilderManifestConfig,
) -> str:
    candidate = registration.candidate
    attempt = _attempt(registration)
    value = {
        "archive_sha256": candidate.archive_sha256,
        "archive_size_bytes": candidate.archive_size_bytes,
        "attempt_id": str(attempt.id),
        "attempt_sequence": attempt.attempt_sequence,
        "build_contract_sha256": candidate.build_contract_sha256,
        "candidate_id": str(candidate.id),
        "candidate_sha": candidate.candidate_sha,
        "components": list(PERSONAL_DEV_COMPONENTS),
        "lease_epoch": attempt.lease_epoch,
        "operation_epoch": attempt.operation_epoch,
        "operation_id": str(attempt.operation_id),
        "platform": platform,
        "max_artifact_bytes": config.max_artifact_bytes,
        "max_image_archive_bytes": config.max_image_archive_bytes,
        "schema_version": 1,
        "scope": "personal-dev-only",
        "source_sha256": candidate.source_sha256,
        "source_commit": candidate.source_commit,
        "subject_id": str(attempt.subject_id),
        "subject_incarnation": str(attempt.subject_incarnation),
    }
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def personal_dev_builder_manifest_documents(
    registration: CandidateRegistration,
    *,
    platform: PersonalDevPlatform,
    config: PersonalDevBuilderManifestConfig,
) -> tuple[dict[str, Any], ...]:
    """Render one native build job and its bounded, default-deny authority."""
    if platform not in PERSONAL_DEV_PLATFORMS:
        raise ValueError("personal-dev builder platform is unsupported")
    attempt = _attempt(registration)
    lease_suffix = f"l{attempt.lease_epoch:016x}"
    namespace = f"loom-build-{attempt.id.hex}-{lease_suffix}"
    architecture = platform.rsplit("/", 1)[1]
    contract_name = f"build-contract-{architecture}-{lease_suffix}"
    capability_secret_name = f"{config.capability_secret_name}-{architecture}-{lease_suffix}"
    namespace_document = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                **_labels(registration),
                "pod-security.kubernetes.io/enforce": "baseline",
                "pod-security.kubernetes.io/enforce-version": "v1.36",
                "pod-security.kubernetes.io/audit": "restricted",
                "pod-security.kubernetes.io/audit-version": "v1.36",
                "pod-security.kubernetes.io/warn": "restricted",
                "pod-security.kubernetes.io/warn-version": "v1.36",
            },
        },
    }
    management_binding = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": _metadata("loom-personal-dev-management", namespace, registration),
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "loom-personal-dev-managed-namespace",
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "loom-personal-dev-management",
                "namespace": "loom-dev",
            }
        ],
    }
    quota = {
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": _metadata("builder-quota", namespace, registration),
        "spec": {
            "hard": {
                "configmaps": "2",
                "count/jobs.batch": "2",
                "pods": "2",
                "secrets": "2",
            }
        },
    }
    limit_range = {
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": _metadata("builder-limits", namespace, registration),
        "spec": {
            "limits": [
                {
                    "type": "Container",
                    "defaultRequest": {
                        "cpu": config.cpu_request,
                        "ephemeral-storage": config.ephemeral_storage_request,
                        "memory": config.memory_request,
                    },
                    "default": {
                        "cpu": config.cpu_limit,
                        "ephemeral-storage": config.ephemeral_storage_limit,
                        "memory": config.memory_limit,
                    },
                }
            ]
        },
    }
    default_deny = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": _metadata("default-deny", namespace, registration),
        "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
    }
    egress = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": _metadata("builder-egress", namespace, registration),
        "spec": {
            "podSelector": {"matchLabels": {"loom.dev/builder-role": "sandbox"}},
            "policyTypes": ["Egress"],
            "egress": [
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                            },
                            "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                        }
                    ],
                    "ports": [
                        {"protocol": "UDP", "port": 53},
                        {"protocol": "TCP", "port": 53},
                    ],
                },
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "kubernetes.io/metadata.name": config.shared_namespace
                                }
                            },
                            "podSelector": {"matchLabels": {"app": "loom-dev-minio"}},
                        }
                    ],
                    "ports": [{"protocol": "TCP", "port": 9000}],
                },
                {
                    "to": [
                        {
                            "ipBlock": {
                                "cidr": "0.0.0.0/0",
                                "except": [
                                    "0.0.0.0/8",
                                    "10.0.0.0/8",
                                    "100.64.0.0/10",
                                    "127.0.0.0/8",
                                    "169.254.0.0/16",
                                    "172.16.0.0/12",
                                    "192.0.0.0/24",
                                    "192.0.2.0/24",
                                    "192.168.0.0/16",
                                    "198.18.0.0/15",
                                    "198.51.100.0/24",
                                    "203.0.113.0/24",
                                    "224.0.0.0/4",
                                    "240.0.0.0/4",
                                ],
                            }
                        },
                        {
                            "ipBlock": {
                                "cidr": "::/0",
                                "except": [
                                    "::/128",
                                    "::1/128",
                                    "2001:db8::/32",
                                    "fc00::/7",
                                    "fe80::/10",
                                    "ff00::/8",
                                ],
                            }
                        },
                    ],
                    "ports": [
                        {"protocol": "TCP", "port": 80},
                        {"protocol": "TCP", "port": 443},
                    ],
                },
            ],
        },
    }
    config_map = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": _metadata(contract_name, namespace, registration),
        "immutable": True,
        "data": {
            "contract.json": _contract(
                registration,
                platform=platform,
                config=config,
            )
        },
    }
    pod_labels = {
        **_labels(registration),
        "loom.dev/builder-role": "sandbox",
        "loom.dev/platform": architecture,
    }
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": _metadata(f"build-{architecture}", namespace, registration),
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": config.active_deadline_seconds,
            "ttlSecondsAfterFinished": config.ttl_seconds_after_finished,
            "template": {
                "metadata": {"labels": pod_labels},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "shareProcessNamespace": False,
                    "runtimeClassName": config.runtime_class_name,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "fsGroup": 1000,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "builder",
                            "image": config.builder_image,
                            "imagePullPolicy": config.image_pull_policy,
                            "args": [
                                "build",
                                "--contract-file",
                                "/var/run/loom-builder-contract/contract.json",
                                "--capability-directory",
                                "/var/run/loom-builder-capability",
                                "--workspace",
                                "/workspace",
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                            },
                            "resources": {
                                "requests": {
                                    "cpu": config.cpu_request,
                                    "ephemeral-storage": config.ephemeral_storage_request,
                                    "memory": config.memory_request,
                                },
                                "limits": {
                                    "cpu": config.cpu_limit,
                                    "ephemeral-storage": config.ephemeral_storage_limit,
                                    "memory": config.memory_limit,
                                },
                            },
                            "volumeMounts": [
                                {
                                    "name": "contract",
                                    "mountPath": "/var/run/loom-builder-contract",
                                    "readOnly": True,
                                },
                                {
                                    "name": "attempt-capability",
                                    "mountPath": "/var/run/loom-builder-capability",
                                    "readOnly": True,
                                },
                                {"name": "workspace", "mountPath": "/workspace"},
                                {"name": "tmp-client", "mountPath": "/tmp"},
                                {
                                    "name": "buildkit-run",
                                    "mountPath": "/var/run/loom-buildkit",
                                    "readOnly": True,
                                },
                            ],
                        }
                    ],
                    "initContainers": [
                        {
                            "name": "buildkitd",
                            "image": config.builder_image,
                            "imagePullPolicy": config.image_pull_policy,
                            "restartPolicy": "Always",
                            "command": [
                                "/usr/local/bin/loom-personal-dev-buildkitd"
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": True,
                                "capabilities": {
                                    "drop": ["ALL"],
                                    "add": ["SETGID", "SETUID"],
                                },
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                                "seccompProfile": {"type": "Unconfined"},
                            },
                            "resources": {
                                "requests": {
                                    "cpu": config.cpu_request,
                                    "ephemeral-storage": (
                                        config.ephemeral_storage_request
                                    ),
                                    "memory": config.memory_request,
                                },
                                "limits": {
                                    "cpu": config.cpu_limit,
                                    "ephemeral-storage": config.ephemeral_storage_limit,
                                    "memory": config.memory_limit,
                                },
                            },
                            "startupProbe": {
                                "exec": {
                                    "command": [
                                        "/usr/bin/buildctl",
                                        "--addr",
                                        (
                                            "unix:///var/run/loom-buildkit/"
                                            "buildkitd.sock"
                                        ),
                                        "debug",
                                        "workers",
                                    ]
                                },
                                "failureThreshold": 60,
                                "periodSeconds": 2,
                                "timeoutSeconds": 1,
                            },
                            "volumeMounts": [
                                {
                                    "name": "buildkit-run",
                                    "mountPath": "/var/run/loom-buildkit",
                                },
                                {
                                    "name": "buildkit-state",
                                    "mountPath": "/var/lib/loom-buildkit",
                                },
                                {"name": "tmp-buildkit", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "contract",
                            "configMap": {"name": contract_name, "defaultMode": 0o400},
                        },
                        {
                            "name": "attempt-capability",
                            "secret": {
                                "secretName": capability_secret_name,
                                "defaultMode": 0o400,
                            },
                        },
                        {
                            "name": "workspace",
                            "emptyDir": {"sizeLimit": config.ephemeral_storage_limit},
                        },
                        {"name": "tmp-client", "emptyDir": {"sizeLimit": "1Gi"}},
                        {"name": "buildkit-run", "emptyDir": {"sizeLimit": "64Mi"}},
                        {
                            "name": "buildkit-state",
                            "emptyDir": {"sizeLimit": config.ephemeral_storage_limit},
                        },
                        {"name": "tmp-buildkit", "emptyDir": {"sizeLimit": "1Gi"}},
                    ],
                },
            },
        },
    }
    return (
        namespace_document,
        management_binding,
        quota,
        limit_range,
        default_deny,
        egress,
        config_map,
        job,
    )


__all__ = [
    "PersonalDevBuilderManifestConfig",
    "personal_dev_builder_manifest_documents",
]
