"""Pure deterministic renderer for the inert personal-dev management plane."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

import yaml  # type: ignore[import-untyped]

from loom.dev_instance import INGRESS_HOST
from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlan,
    PersonalDevControlPlaneProfile,
    PersonalDevTrustedRelease,
    ResourceEnvelope,
    validate_personal_dev_acceptance_plan,
)

_MANAGED_BY = "loom-personal-dev-control-plane"
_MANAGEMENT_PRINCIPAL = "system:serviceaccount:loom-dev:loom-personal-dev-management"
_ACTIVATION_PRINCIPAL = "system:serviceaccount:loom-dev:loom-personal-dev-activation-agent"
_PERSONAL_NAMESPACE_PATTERN = r"^loom-dev-[a-z]([-a-z0-9]{0,18}[a-z0-9])?$"
_RESERVED_PERSONAL_NAMESPACE_PATTERN = (
    r"^loom-dev-(dev|development|staging|production|prod|local|loom|shared|default)$"
)
_BUILDER_NAMESPACE_PATTERN = r"^loom-build-[0-9a-f]{32}-l[0-9a-f]{16}$"
_MANAGEMENT_FILES = (
    "admin-secrets.toml",
    "capacity-lifecycle-ca.pem",
    "capacity-lifecycle-certificate.pem",
    "capacity-lifecycle-private-key.pem",
    "capacity-lifecycle-token",
    "capacity-reporter-ca.pem",
    "capacity-reporter-certificate.pem",
    "capacity-reporter-private-key.pem",
    "config.json",
)


@dataclass(frozen=True, slots=True)
class RenderedPersonalDevControlPlane:
    yaml_text: str
    input_sha256: str
    release_sha256: str
    resource_count: int
    runtime_class_name: str
    runtime_handler: str
    runtime_profile_sha256: str


@dataclass(frozen=True, slots=True)
class _RenderContext:
    input_sha256: str
    release_sha256: str
    acceptance_plan_sha256: str | None = None

    def labels(self) -> dict[str, str]:
        value = {
            "app.kubernetes.io/managed-by": _MANAGED_BY,
            "app.kubernetes.io/part-of": "loom",
            "loom.dev/render-input": self.input_sha256[:32],
            "loom.dev/trusted-release": self.release_sha256[:32],
        }
        if self.acceptance_plan_sha256 is not None:
            value["loom.dev/acceptance-plan-sha256"] = self.acceptance_plan_sha256[:32]
        return value

    def annotations(self) -> dict[str, str]:
        value = {
            "loom.dev/render-input-sha256": self.input_sha256,
            "loom.dev/trusted-release-sha256": self.release_sha256,
        }
        if self.acceptance_plan_sha256 is not None:
            value["loom.dev/acceptance-plan-sha256"] = self.acceptance_plan_sha256
        return value


def _metadata(
    context: _RenderContext,
    name: str,
    *,
    namespace: str | None = None,
    labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "name": name,
        "labels": {**context.labels(), **(labels or {})},
        "annotations": {**context.annotations(), **(annotations or {})},
    }
    if namespace is not None:
        value["namespace"] = namespace
    return value


def _pod_metadata(
    context: _RenderContext,
    *,
    labels: dict[str, str],
) -> dict[str, Any]:
    return {
        "labels": {**context.labels(), **labels},
        "annotations": context.annotations(),
    }


def _resources(value: ResourceEnvelope) -> dict[str, dict[str, str]]:
    return {
        "requests": {"cpu": value.cpu_request, "memory": value.memory_request},
        "limits": {"cpu": value.cpu_limit, "memory": value.memory_limit},
    }


def _container_security(*, read_only: bool = True, user: int | None = None) -> dict[str, Any]:
    value: dict[str, Any] = {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": read_only,
        "runAsNonRoot": True,
    }
    if user is not None:
        value["runAsUser"] = user
    return value


def _pod_security(*, user: int, group: int) -> dict[str, Any]:
    return {
        "fsGroup": group,
        "fsGroupChangePolicy": "OnRootMismatch",
        "runAsGroup": group,
        "runAsNonRoot": True,
        "runAsUser": user,
        "seccompProfile": {"type": "RuntimeDefault"},
    }


def _secret_env(name: str, secret: str, key: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": secret, "key": key}},
    }


def _literal_env(name: str, value: str) -> dict[str, str]:
    return {"name": name, "value": value}


def _service_account_token_volume() -> dict[str, Any]:
    return {
        "name": "kube-api-access",
        "projected": {
            "defaultMode": 0o440,
            "sources": [
                {
                    "serviceAccountToken": {
                        "audience": "https://kubernetes.default.svc.cluster.local",
                        "expirationSeconds": 600,
                        "path": "token",
                    }
                },
                {
                    "configMap": {
                        "name": "kube-root-ca.crt",
                        "items": [{"key": "ca.crt", "path": "ca.crt"}],
                    }
                },
                {
                    "downwardAPI": {
                        "items": [
                            {
                                "path": "namespace",
                                "fieldRef": {
                                    "apiVersion": "v1",
                                    "fieldPath": "metadata.namespace",
                                },
                            }
                        ]
                    }
                },
            ],
        },
    }


def _service_account_token_mount() -> dict[str, Any]:
    return {
        "name": "kube-api-access",
        "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
        "readOnly": True,
    }


def _namespace(context: _RenderContext) -> dict[str, Any]:
    metadata = _metadata(context, "loom-dev")
    metadata["labels"].update(
        {
            "app.kubernetes.io/managed-by": "loom-operator",
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/enforce-version": "latest",
            "pod-security.kubernetes.io/audit": "restricted",
            "pod-security.kubernetes.io/audit-version": "latest",
            "pod-security.kubernetes.io/warn": "restricted",
            "pod-security.kubernetes.io/warn-version": "latest",
        }
    )
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": metadata}


def _personal_namespace_cel(value: str) -> str:
    return (
        f"({value}.startsWith('loom-dev-') && "
        f"{value}.matches('{_PERSONAL_NAMESPACE_PATTERN}') && "
        f"!{value}.matches('{_RESERVED_PERSONAL_NAMESPACE_PATTERN}'))"
    )


def _builder_namespace_cel(value: str) -> str:
    return f"({value}.startsWith('loom-build-') && {value}.matches('{_BUILDER_NAMESPACE_PATTERN}'))"


def _service_account(
    context: _RenderContext,
    name: str,
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": _metadata(context, name, namespace="loom-dev"),
        "automountServiceAccountToken": False,
    }


def _management_mutation_role(context: _RenderContext) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": _metadata(context, "loom-personal-dev-management-mutation"),
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["namespaces"],
                "verbs": ["create", "delete", "get", "patch", "update"],
            },
            {
                "apiGroups": [""],
                "resources": [
                    "configmaps",
                    "limitranges",
                    "resourcequotas",
                    "secrets",
                    "services",
                ],
                "verbs": ["create", "delete", "patch", "update"],
            },
            {
                "apiGroups": ["apps"],
                "resources": ["deployments"],
                "verbs": ["create", "delete", "patch", "update"],
            },
            {
                "apiGroups": ["batch"],
                "resources": ["jobs"],
                "verbs": ["create", "delete", "patch", "update"],
            },
            {
                "apiGroups": ["networking.k8s.io"],
                "resources": ["networkpolicies"],
                "verbs": ["create", "delete", "patch", "update"],
            },
            {
                "apiGroups": ["rbac.authorization.k8s.io"],
                "resources": ["rolebindings"],
                "verbs": ["create", "delete", "patch", "update"],
            },
            {
                "apiGroups": ["rbac.authorization.k8s.io"],
                "resources": ["clusterroles"],
                "resourceNames": [
                    "loom-personal-dev-activation-agent",
                    "loom-personal-dev-managed-namespace",
                ],
                "verbs": ["bind"],
            },
        ],
    }


def _management_mutation_binding(context: _RenderContext) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": _metadata(context, "loom-personal-dev-management-mutation"),
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "loom-personal-dev-management-mutation",
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "loom-personal-dev-management",
                "namespace": "loom-dev",
            }
        ],
    }


def _managed_namespace_role(context: _RenderContext) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": _metadata(context, "loom-personal-dev-managed-namespace"),
        "rules": [
            {
                "apiGroups": [""],
                "resources": [
                    "configmaps",
                    "limitranges",
                    "pods",
                    "resourcequotas",
                    "services",
                ],
                "verbs": ["get", "list", "watch"],
            },
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
            },
            {
                "apiGroups": ["apps"],
                "resources": ["deployments", "replicasets"],
                "verbs": ["get", "list", "watch"],
            },
            {
                "apiGroups": ["batch"],
                "resources": ["jobs"],
                "verbs": ["get", "list", "watch"],
            },
            {
                "apiGroups": ["networking.k8s.io"],
                "resources": ["ingresses", "networkpolicies"],
                "verbs": ["get", "list", "watch"],
            },
            {
                "apiGroups": ["rbac.authorization.k8s.io"],
                "resources": ["rolebindings"],
                "verbs": ["get"],
            },
        ],
    }


def _activation_role(context: _RenderContext) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": _metadata(context, "loom-personal-dev-activation-agent"),
        "rules": [
            {
                "apiGroups": ["apps"],
                "resources": ["deployments"],
                "verbs": ["get"],
            },
            {
                "apiGroups": [""],
                "resources": ["services"],
                "verbs": ["create", "get", "patch"],
            },
            {
                "apiGroups": ["networking.k8s.io"],
                "resources": ["ingresses"],
                "verbs": ["create", "get", "patch"],
            },
        ],
    }


def _shared_role(context: _RenderContext) -> tuple[dict[str, Any], dict[str, Any]]:
    role = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": _metadata(
            context,
            "loom-personal-dev-shared-operations",
            namespace="loom-dev",
        ),
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["pods"],
                "resourceNames": ["loom-dev-minio-0"],
                "verbs": ["get"],
            },
            {
                "apiGroups": [""],
                "resources": ["pods/exec"],
                "resourceNames": ["loom-dev-minio-0"],
                "verbs": ["create"],
            },
        ],
    }
    binding = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": _metadata(
            context,
            "loom-personal-dev-shared-operations",
            namespace="loom-dev",
        ),
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "loom-personal-dev-shared-operations",
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "loom-personal-dev-management",
                "namespace": "loom-dev",
            }
        ],
    }
    return role, binding


def _admission_binding(context: _RenderContext, name: str) -> dict[str, Any]:
    return {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": _metadata(context, name),
        "spec": {"policyName": name, "validationActions": ["Deny"]},
    }


def _management_namespace_admission(context: _RenderContext) -> tuple[dict[str, Any], ...]:
    name = "loom-personal-dev-management-namespaces"
    target = "(request.operation == 'DELETE' ? oldObject : object)"
    target_name = f"{target}.metadata.name"
    personal_namespace = _personal_namespace_cel(target_name)
    builder_namespace = _builder_namespace_cel(target_name)
    policy = {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": _metadata(context, name),
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [
                    {
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "operations": ["CREATE", "UPDATE", "DELETE"],
                        "resources": ["namespaces"],
                    }
                ]
            },
            "matchConditions": [
                {
                    "name": "exact-management-principal",
                    "expression": f"request.userInfo.username == '{_MANAGEMENT_PRINCIPAL}'",
                }
            ],
            "validations": [
                {
                    "expression": f"{personal_namespace} || {builder_namespace}",
                    "message": (
                        "management may create only exact personal or attempt-bound builder "
                        "namespaces"
                    ),
                },
                {
                    "expression": (
                        f"({personal_namespace} && "
                        f"{target}.metadata.labels["
                        "'pod-security.kubernetes.io/enforce'] == 'restricted') || "
                        f"({builder_namespace} && "
                        f"{target}.metadata.labels["
                        "'pod-security.kubernetes.io/enforce'] == 'privileged' && "
                        f"{target}.metadata.labels["
                        "'pod-security.kubernetes.io/enforce-version'] == 'v1.36' && "
                        f"{target}.metadata.labels["
                        "'pod-security.kubernetes.io/audit'] == 'restricted' && "
                        f"{target}.metadata.labels["
                        "'pod-security.kubernetes.io/audit-version'] == 'v1.36' && "
                        f"{target}.metadata.labels["
                        "'pod-security.kubernetes.io/warn'] == 'restricted' && "
                        f"{target}.metadata.labels["
                        "'pod-security.kubernetes.io/warn-version'] == 'v1.36')"
                    ),
                    "message": "managed namespace pod security differs from its family",
                },
                {
                    "expression": (
                        f"({target}.metadata.name.startsWith('loom-dev-') && "
                        f"{target}.metadata.labels['app.kubernetes.io/managed-by'] == "
                        "'loom-dev-instance-controller') || "
                        f"({target}.metadata.name.startsWith('loom-build-') && "
                        f"{target}.metadata.labels['app.kubernetes.io/managed-by'] == "
                        "'loom-personal-dev-builder-controller')"
                    ),
                    "message": "managed namespace family and authority label differ",
                },
            ],
        },
    }
    return policy, _admission_binding(context, name)


def _builder_job_admission_validations(
    *,
    target: str,
    builder_namespace: str,
    builder_image: str,
    runtime_class_name: str,
) -> tuple[dict[str, str], ...]:
    builder_job = (
        f"({builder_namespace} && request.resource.resource == 'jobs')"
    )
    template = f"{target}.spec.template"
    pod = f"{template}.spec"
    client = f"{pod}.containers[0]"
    sidecar = f"{pod}.initContainers[0]"

    def exact_resources(container: str) -> str:
        requests = f"{container}.resources.requests"
        limits = f"{container}.resources.limits"
        return (
            f"has({container}.resources) && has({requests}) && "
            f"{requests}.size() == 3 && "
            f"{requests}['cpu'] == quantity('1') && "
            f"{requests}['memory'] == quantity('1Gi') && "
            f"{requests}['ephemeral-storage'] == quantity('4Gi') && "
            f"has({limits}) && {limits}.size() == 3 && "
            f"{limits}['cpu'] == quantity('4') && "
            f"{limits}['memory'] == quantity('8Gi') && "
            f"{limits}['ephemeral-storage'] == quantity('20Gi')"
        )

    pod_boundary = (
        f"!{builder_job} || ("
        f"{target}.apiVersion == 'batch/v1' && {target}.kind == 'Job' && "
        f"{target}.spec.backoffLimit == 0 && "
        f"{target}.spec.activeDeadlineSeconds == 3600 && "
        f"{target}.spec.ttlSecondsAfterFinished == 600 && "
        f"{pod}.restartPolicy == 'Never' && "
        f"{pod}.runtimeClassName == '{runtime_class_name}' && "
        f"{pod}.automountServiceAccountToken == false && "
        f"{pod}.enableServiceLinks == false && "
        f"{pod}.shareProcessNamespace == false && "
        f"(!has({pod}.hostNetwork) || {pod}.hostNetwork == false) && "
        f"(!has({pod}.hostPID) || {pod}.hostPID == false) && "
        f"(!has({pod}.hostIPC) || {pod}.hostIPC == false) && "
        f"!has({pod}.hostUsers) && !has({pod}.nodeName) && "
        f"!has({pod}.nodeSelector) && !has({pod}.affinity) && "
        f"!has({pod}.tolerations) && "
        f"(!has({pod}.serviceAccountName) || "
        f"{pod}.serviceAccountName == 'default') && "
        f"!has({pod}.imagePullSecrets) && "
        f"has({pod}.securityContext) && "
        f"{pod}.securityContext.runAsNonRoot == true && "
        f"{pod}.securityContext.runAsUser == 1000 && "
        f"{pod}.securityContext.runAsGroup == 1000 && "
        f"{pod}.securityContext.fsGroup == 1000 && "
        f"{pod}.securityContext.seccompProfile.type == 'RuntimeDefault' && "
        f"!has({pod}.securityContext.sysctls) && "
        f"!has({pod}.securityContext.seLinuxOptions) && "
        f"(!has({template}.metadata.annotations) || "
        f"{template}.metadata.annotations.size() == 0) && "
        f"{template}.metadata.labels['loom.dev/builder-role'] == 'sandbox' && "
        f"(({target}.metadata.name == 'build-amd64' && "
        f"{template}.metadata.labels['loom.dev/platform'] == 'amd64') || "
        f"({target}.metadata.name == 'build-arm64' && "
        f"{template}.metadata.labels['loom.dev/platform'] == 'arm64')) && "
        f"{pod}.containers.size() == 1 && "
        f"{pod}.initContainers.size() == 1 && "
        f"!has({pod}.ephemeralContainers))"
    )
    client_definition_boundary = (
        f"!{builder_job} || ("
        f"{client}.name == 'builder' && {client}.image == '{builder_image}' && "
        f"{client}.imagePullPolicy == 'IfNotPresent' && "
        f"!has({client}.command) && "
        f"{client}.args == ['build','--contract-file',"
        "'/var/run/loom-builder-contract/contract.json',"
        "'--capability-directory','/var/run/loom-builder-capability',"
        "'--workspace','/workspace'] && "
        f"(!has({client}.env) || {client}.env.size() == 0) && "
        f"(!has({client}.envFrom) || {client}.envFrom.size() == 0))"
    )
    client_security_boundary = (
        f"!{builder_job} || ("
        f"has({client}.securityContext) && "
        f"{client}.securityContext.allowPrivilegeEscalation == false && "
        f"{client}.securityContext.readOnlyRootFilesystem == true && "
        f"{client}.securityContext.runAsNonRoot == true && "
        f"(!has({client}.securityContext.privileged) || "
        f"{client}.securityContext.privileged == false) && "
        f"{client}.securityContext.capabilities.drop == ['ALL'] && "
        f"!has({client}.securityContext.capabilities.add) && "
        f"!has({client}.securityContext.procMount) && "
        f"!has({client}.securityContext.seccompProfile) && "
        f"!has({client}.securityContext.appArmorProfile))"
    )
    client_resources_boundary = (
        f"!{builder_job} || ({exact_resources(client)})"
    )
    client_mount_boundary = (
        f"!{builder_job} || ("
        f"{client}.volumeMounts.size() == 5 && "
        f"{client}.volumeMounts[0].name == 'contract' && "
        f"{client}.volumeMounts[0].mountPath == '/var/run/loom-builder-contract' && "
        f"{client}.volumeMounts[0].readOnly == true && "
        f"{client}.volumeMounts[1].name == 'attempt-capability' && "
        f"{client}.volumeMounts[1].mountPath == '/var/run/loom-builder-capability' && "
        f"{client}.volumeMounts[1].readOnly == true && "
        f"{client}.volumeMounts[2].name == 'workspace' && "
        f"{client}.volumeMounts[2].mountPath == '/workspace' && "
        f"(!has({client}.volumeMounts[2].readOnly) || "
        f"{client}.volumeMounts[2].readOnly == false) && "
        f"{client}.volumeMounts[3].name == 'tmp-client' && "
        f"{client}.volumeMounts[3].mountPath == '/tmp' && "
        f"(!has({client}.volumeMounts[3].readOnly) || "
        f"{client}.volumeMounts[3].readOnly == false) && "
        f"{client}.volumeMounts[4].name == 'buildkit-run' && "
        f"{client}.volumeMounts[4].mountPath == '/var/run/loom-buildkit' && "
        f"{client}.volumeMounts[4].readOnly == true && "
        f"{client}.volumeMounts.all(mount, !has(mount.subPath) && "
        "!has(mount.subPathExpr) && !has(mount.mountPropagation)))"
    )
    sidecar_definition_boundary = (
        f"!{builder_job} || ("
        f"{sidecar}.name == 'buildkitd' && {sidecar}.image == '{builder_image}' && "
        f"{sidecar}.imagePullPolicy == 'IfNotPresent' && "
        f"{sidecar}.restartPolicy == 'Always' && "
        f"{sidecar}.command == ['/usr/local/bin/loom-personal-dev-buildkitd'] && "
        f"!has({sidecar}.args) && "
        f"(!has({sidecar}.env) || {sidecar}.env.size() == 0) && "
        f"(!has({sidecar}.envFrom) || {sidecar}.envFrom.size() == 0))"
    )
    sidecar_security_boundary = (
        f"!{builder_job} || ("
        f"has({sidecar}.securityContext) && "
        f"{sidecar}.securityContext.allowPrivilegeEscalation == true && "
        f"{sidecar}.securityContext.readOnlyRootFilesystem == true && "
        f"{sidecar}.securityContext.runAsNonRoot == true && "
        f"(!has({sidecar}.securityContext.privileged) || "
        f"{sidecar}.securityContext.privileged == false) && "
        f"{sidecar}.securityContext.capabilities.drop == ['ALL'] && "
        f"{sidecar}.securityContext.capabilities.add == ['SETGID','SETUID'] && "
        f"!has({sidecar}.securityContext.procMount) && "
        f"{sidecar}.securityContext.seccompProfile.type == 'Unconfined' && "
        f"!has({sidecar}.securityContext.appArmorProfile))"
    )
    sidecar_resources_boundary = (
        f"!{builder_job} || ({exact_resources(sidecar)})"
    )
    sidecar_probe_boundary = (
        f"!{builder_job} || ("
        f"{sidecar}.startupProbe.exec.command == ['/usr/bin/buildctl','--addr',"
        "'unix:///var/run/loom-buildkit/buildkitd.sock','debug','workers'] && "
        f"{sidecar}.startupProbe.failureThreshold == 60 && "
        f"{sidecar}.startupProbe.periodSeconds == 2 && "
        f"{sidecar}.startupProbe.timeoutSeconds == 1)"
    )
    sidecar_mount_boundary = (
        f"!{builder_job} || ("
        f"{sidecar}.volumeMounts.size() == 3 && "
        f"{sidecar}.volumeMounts[0].name == 'buildkit-run' && "
        f"{sidecar}.volumeMounts[0].mountPath == '/var/run/loom-buildkit' && "
        f"{sidecar}.volumeMounts[1].name == 'buildkit-state' && "
        f"{sidecar}.volumeMounts[1].mountPath == '/var/lib/loom-buildkit' && "
        f"{sidecar}.volumeMounts[2].name == 'tmp-buildkit' && "
        f"{sidecar}.volumeMounts[2].mountPath == '/tmp' && "
        f"{sidecar}.volumeMounts.all(mount, "
        "(!has(mount.readOnly) || mount.readOnly == false) && "
        "!has(mount.subPath) && !has(mount.subPathExpr) && "
        "!has(mount.mountPropagation)))"
    )
    volumes_boundary = (
        f"!{builder_job} || ("
        f"{pod}.volumes.size() == 7 && "
        f"{pod}.volumes.all(volume, !has(volume.hostPath) && "
        "!has(volume.projected) && !has(volume.csi)) && "
        f"{pod}.volumes[0].name == 'contract' && "
        f"{pod}.volumes[0].configMap.name.matches("
        "'^build-contract-(amd64|arm64)-l[0-9a-f]{16}$') && "
        f"{pod}.volumes[0].configMap.defaultMode == 256 && "
        f"{pod}.volumes[1].name == 'attempt-capability' && "
        f"{pod}.volumes[1].secret.secretName.matches("
        "'^build-capability-(amd64|arm64)-l[0-9a-f]{16}$') && "
        f"{pod}.volumes[1].secret.defaultMode == 256 && "
        f"{pod}.volumes[2].name == 'workspace' && "
        f"{pod}.volumes[2].emptyDir.sizeLimit == quantity('20Gi') && "
        f"{pod}.volumes[3].name == 'tmp-client' && "
        f"{pod}.volumes[3].emptyDir.sizeLimit == quantity('1Gi') && "
        f"{pod}.volumes[4].name == 'buildkit-run' && "
        f"{pod}.volumes[4].emptyDir.sizeLimit == quantity('64Mi') && "
        f"{pod}.volumes[5].name == 'buildkit-state' && "
        f"{pod}.volumes[5].emptyDir.sizeLimit == quantity('20Gi') && "
        f"{pod}.volumes[6].name == 'tmp-buildkit' && "
        f"{pod}.volumes[6].emptyDir.sizeLimit == quantity('1Gi'))"
    )
    return (
        {
            "expression": pod_boundary,
            "message": "builder Job pod boundary differs from its exact privileged exception",
        },
        {
            "expression": client_definition_boundary,
            "message": (
                "builder Job client definition differs from its exact privileged exception"
            ),
        },
        {
            "expression": client_security_boundary,
            "message": (
                "builder Job client security differs from its exact privileged exception"
            ),
        },
        {
            "expression": client_resources_boundary,
            "message": (
                "builder Job client resources differ from its exact privileged exception"
            ),
        },
        {
            "expression": client_mount_boundary,
            "message": (
                "builder Job client mounts differ from its exact privileged exception"
            ),
        },
        {
            "expression": sidecar_definition_boundary,
            "message": (
                "builder Job sidecar definition differs from its exact privileged exception"
            ),
        },
        {
            "expression": sidecar_security_boundary,
            "message": (
                "builder Job sidecar security differs from its exact privileged exception"
            ),
        },
        {
            "expression": sidecar_resources_boundary,
            "message": (
                "builder Job sidecar resources differ from its exact privileged exception"
            ),
        },
        {
            "expression": sidecar_probe_boundary,
            "message": (
                "builder Job sidecar probe differs from its exact privileged exception"
            ),
        },
        {
            "expression": sidecar_mount_boundary,
            "message": (
                "builder Job sidecar mounts differ from its exact privileged exception"
            ),
        },
        {
            "expression": volumes_boundary,
            "message": "builder Job volumes differ from its exact privileged exception",
        },
    )


def _management_resource_admission(
    context: _RenderContext,
    *,
    builder_image: str,
    runtime_class_name: str,
) -> tuple[dict[str, Any], ...]:
    name = "loom-personal-dev-management-resources"
    target = "(request.operation == 'DELETE' ? oldObject : object)"
    personal_namespace = _personal_namespace_cel("request.namespace")
    builder_namespace = _builder_namespace_cel("request.namespace")
    shared_minio_exec = (
        "(request.operation == 'CONNECT' && request.namespace == 'loom-dev' && "
        "request.resource.group == '' && request.resource.version == 'v1' && "
        "request.resource.resource == 'pods' && request.subResource == 'exec' && "
        "request.name == 'loom-dev-minio-0')"
    )
    app_resources = "['secrets','services','deployments','jobs','networkpolicies','rolebindings']"
    build_resources = (
        "['configmaps','limitranges','resourcequotas','secrets','jobs',"
        "'networkpolicies','rolebindings']"
    )
    personal_application_secret_names = "['loom-secrets','loom-admin-secret']"
    personal_capacity_secret_names = "['loom-capacity-agent']"
    personal_capacity_resource_secret_names = (
        "['loom-capacity-agent','loom-capacity-agent-credentials']"
    )
    personal_secret_names = (
        "['loom-secrets','loom-admin-secret','loom-capacity-agent',"
        "'loom-capacity-agent-credentials']"
    )
    capacity_owned_resource = (
        "((request.resource.resource == 'secrets' && "
        f"{target}.metadata.name in {personal_capacity_resource_secret_names}) || "
        "(request.resource.resource == 'deployments' && "
        f"{target}.metadata.name == 'loom-capacity-agent') || "
        "(request.resource.resource == 'networkpolicies' && "
        f"{target}.metadata.name == 'capacity-agent-egress'))"
    )
    personal_resource_names = (
        f"(request.resource.resource == 'secrets' || "
        "(request.resource.resource == 'services' && "
        f"{target}.metadata.name.matches("
        "'^loom-(control-plane|llm-gateway|service|web)-g[1-9][0-9]*$')) || "
        "(request.resource.resource == 'deployments' && "
        f"({target}.metadata.name == 'loom-capacity-agent' || "
        f"{target}.metadata.name.matches("
        "'^loom-(control-plane|llm-gateway|service|web)-g[1-9][0-9]*$'))) || "
        "(request.resource.resource == 'jobs' && "
        f"{target}.metadata.name.matches("
        "'^loom-migrate-[0-9a-f]{7}-g[1-9][0-9]*$')) || "
        "(request.resource.resource == 'networkpolicies' && "
        f"{target}.metadata.name in "
        "['default-deny','runtime-egress','runtime-ingress','capacity-agent-egress']) || "
        "request.resource.resource == 'rolebindings')"
    )
    builder_resource_names = (
        "((request.resource.resource == 'configmaps' && "
        f"{target}.metadata.name.matches("
        "'^build-contract-(amd64|arm64)-l[0-9a-f]{16}$')) || "
        "(request.resource.resource == 'limitranges' && "
        f"{target}.metadata.name == 'builder-limits') || "
        "(request.resource.resource == 'resourcequotas' && "
        f"{target}.metadata.name == 'builder-quota') || "
        "(request.resource.resource == 'secrets' && "
        f"{target}.metadata.name.matches("
        "'^build-capability-(amd64|arm64)-l[0-9a-f]{16}$')) || "
        "(request.resource.resource == 'jobs' && "
        f"{target}.metadata.name in ['build-amd64','build-arm64']) || "
        "(request.resource.resource == 'networkpolicies' && "
        f"{target}.metadata.name in ['default-deny','builder-egress']) || "
        "request.resource.resource == 'rolebindings')"
    )
    pod_spec = f"{target}.spec.template.spec"

    def container_secret_references(field: str, secret_names: str) -> str:
        containers = f"{pod_spec}.{field}"
        return (
            f"(!has({containers}) || {containers}.all(container, "
            "(!has(container.envFrom) || container.envFrom.all(source, "
            f"!has(source.secretRef) || source.secretRef.name in {secret_names})) && "
            "(!has(container.env) || container.env.all(variable, "
            "!has(variable.valueFrom) || !has(variable.valueFrom.secretKeyRef) || "
            f"variable.valueFrom.secretKeyRef.name in {secret_names}))))"
        )

    def container_without_secret_references(field: str) -> str:
        containers = f"{pod_spec}.{field}"
        return (
            f"(!has({containers}) || {containers}.all(container, "
            "(!has(container.envFrom) || container.envFrom.all(source, "
            "!has(source.secretRef))) && "
            "(!has(container.env) || container.env.all(variable, "
            "!has(variable.valueFrom) || !has(variable.valueFrom.secretKeyRef)))))"
        )

    bounded_service_account = (
        f"(!has({pod_spec}.serviceAccountName) || {pod_spec}.serviceAccountName == 'default')"
    )

    def personal_workload_secret_references(secret_names: str) -> str:
        return (
            f"({pod_spec}.automountServiceAccountToken == false && "
            f"{bounded_service_account} && "
            f"!has({pod_spec}.imagePullSecrets) && "
            f"(!has({pod_spec}.volumes) || {pod_spec}.volumes.all(volume, "
            f"(!has(volume.secret) || volume.secret.secretName in {secret_names}) && "
            "!has(volume.projected) && !has(volume.csi))) && "
            f"{container_secret_references('containers', secret_names)} && "
            f"{container_secret_references('initContainers', secret_names)})"
        )

    application_workload_secret_references = personal_workload_secret_references(
        personal_application_secret_names
    )
    capacity_workload_secret_references = personal_workload_secret_references(
        personal_capacity_secret_names
    )
    builder_workload_secret_references = (
        f"({pod_spec}.automountServiceAccountToken == false && "
        f"{bounded_service_account} && "
        f"!has({pod_spec}.imagePullSecrets) && "
        f"(!has({pod_spec}.volumes) || {pod_spec}.volumes.all(volume, "
        "(!has(volume.secret) || volume.secret.secretName.matches("
        "'^build-capability-(amd64|arm64)-l[0-9a-f]{16}$')) && "
        "!has(volume.projected) && !has(volume.csi))) && "
        f"{container_without_secret_references('containers')} && "
        f"{container_without_secret_references('initContainers')})"
    )
    policy = {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": _metadata(context, name),
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [
                    {
                        "apiGroups": ["*"],
                        "apiVersions": ["*"],
                        "operations": ["CREATE", "UPDATE", "DELETE", "CONNECT"],
                        "resources": ["*/*"],
                    }
                ]
            },
            "matchConditions": [
                {
                    "name": "exact-management-principal",
                    "expression": f"request.userInfo.username == '{_MANAGEMENT_PRINCIPAL}'",
                },
                {
                    "name": "exclude-namespace-admission",
                    "expression": (
                        "!(request.resource.group == '' && "
                        "request.resource.resource == 'namespaces')"
                    ),
                },
            ],
            "validations": [
                {
                    "expression": (f"request.namespace != 'loom-dev' || {shared_minio_exec}"),
                    "message": "management cluster authority cannot mutate shared infrastructure",
                },
                {
                    "expression": (
                        f"(({personal_namespace} && "
                        f"request.resource.resource in {app_resources}) || "
                        f"({builder_namespace} && "
                        f"request.resource.resource in {build_resources})) || "
                        f"{shared_minio_exec}"
                    ),
                    "message": "management resource is outside its namespace-family contract",
                },
                {
                    "expression": (
                        f"{shared_minio_exec} || "
                        f"({personal_namespace} && {personal_resource_names}) || "
                        f"({builder_namespace} && {builder_resource_names})"
                    ),
                    "message": "management resource name is outside its family contract",
                },
                {
                    "expression": (
                        f"{shared_minio_exec} || "
                        f"({personal_namespace} && "
                        f"(({capacity_owned_resource} && "
                        f"{target}.metadata.labels['app.kubernetes.io/managed-by'] == "
                        "'loom-personal-dev-lifecycle') || "
                        f"(!{capacity_owned_resource} && "
                        f"{target}.metadata.labels['app.kubernetes.io/managed-by'] == "
                        "'loom-dev-instance-controller'))) || "
                        f"({builder_namespace} && "
                        f"{target}.metadata.labels['app.kubernetes.io/managed-by'] == "
                        "'loom-personal-dev-builder-controller')"
                    ),
                    "message": "management resource lacks its namespace-family ownership",
                },
                {
                    "expression": (
                        f"{shared_minio_exec} || "
                        "!(request.resource.resource in ['deployments','jobs']) || "
                        f"({personal_namespace} && "
                        "((request.resource.resource == 'deployments' && "
                        f"{target}.metadata.name == 'loom-capacity-agent' && "
                        f"{capacity_workload_secret_references}) || "
                        f"({target}.metadata.name != 'loom-capacity-agent' && "
                        f"{application_workload_secret_references}))) || "
                        f"({builder_namespace} && request.resource.resource == 'jobs' && "
                        f"{builder_workload_secret_references})"
                    ),
                    "message": (
                        "personal workload can reference only fixed lifecycle Secrets; builder "
                        "workload cannot acquire API or unrelated Secret authority"
                    ),
                },
                {
                    "expression": (
                        "request.resource.resource != 'secrets' || "
                        "(request.namespace.startsWith('loom-dev-') && "
                        f"{target}.metadata.name in {personal_secret_names}) || "
                        "(request.namespace.startsWith('loom-build-') && "
                        f"{target}.metadata.name.matches("
                        "'^build-capability-(amd64|arm64)-l[0-9a-f]{16}$'))"
                    ),
                    "message": "management Secret name is outside its fixed contract",
                },
                {
                    "expression": (
                        "request.resource.resource != 'services' || "
                        f"{target}.metadata.name.matches("
                        "'^loom-(control-plane|llm-gateway|service|web)-g[1-9][0-9]*$')"
                    ),
                    "message": "management cannot mutate activation-owned stable Services",
                },
                {
                    "expression": (
                        "request.resource.resource != 'rolebindings' || "
                        f"({target}.metadata.name == 'loom-personal-dev-management' && "
                        f"{target}.roleRef.kind == 'ClusterRole' && "
                        f"{target}.roleRef.name == 'loom-personal-dev-managed-namespace' && "
                        f"{target}.subjects.size() == 1 && "
                        f"{target}.subjects[0].kind == 'ServiceAccount' && "
                        f"{target}.subjects[0].name == 'loom-personal-dev-management' && "
                        f"{target}.subjects[0].namespace == 'loom-dev') || "
                        "(request.namespace.startsWith('loom-dev-') && "
                        f"{target}.metadata.name == 'loom-personal-dev-activation-agent' && "
                        f"{target}.roleRef.kind == 'ClusterRole' && "
                        f"{target}.roleRef.name == 'loom-personal-dev-activation-agent' && "
                        f"{target}.subjects.size() == 1 && "
                        f"{target}.subjects[0].kind == 'ServiceAccount' && "
                        f"{target}.subjects[0].name == 'loom-personal-dev-activation-agent' && "
                        f"{target}.subjects[0].namespace == 'loom-dev')"
                    ),
                    "message": "management RoleBinding is outside its exact delegated roles",
                },
                *_builder_job_admission_validations(
                    target=target,
                    builder_namespace=builder_namespace,
                    builder_image=builder_image,
                    runtime_class_name=runtime_class_name,
                ),
            ],
        },
    }
    return policy, _admission_binding(context, name)


def _activation_admission(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
) -> tuple[dict[str, Any], ...]:
    name = "loom-personal-dev-activation-resources"
    target = "(request.operation == 'DELETE' ? oldObject : object)"
    personal_namespace = _personal_namespace_cel("request.namespace")
    owner = f"{target}.metadata.labels['loom.dev/instance']"
    generation = f"{target}.metadata.labels['loom.dev/generation']"
    service_port = (
        f"({target}.metadata.name == 'loom-control-plane' ? 8080 : "
        f"{target}.metadata.name == 'loom-llm-gateway' ? 9100 : "
        f"{target}.metadata.name == 'loom-service' ? 8090 : 80)"
    )
    target_port = f"({target}.metadata.name == 'loom-web' ? 8080 : {service_port})"
    service_contract = (
        "request.resource.resource != 'services' || "
        f"((!has({target}.metadata.annotations) || {target}.metadata.annotations.size() == 0) && "
        f"(!has({target}.spec.type) || {target}.spec.type == 'ClusterIP') && "
        f"(!has({target}.spec.clusterIP) || {target}.spec.clusterIP != 'None') && "
        f"(!has({target}.spec.externalName)) && "
        f"(!has({target}.spec.externalIPs) || {target}.spec.externalIPs.size() == 0) && "
        f"(!has({target}.spec.loadBalancerClass)) && "
        f"(!has({target}.spec.loadBalancerSourceRanges) || "
        f"{target}.spec.loadBalancerSourceRanges.size() == 0) && "
        f"(!has({target}.spec.publishNotReadyAddresses) || "
        f"{target}.spec.publishNotReadyAddresses == false) && "
        f"(!has({target}.spec.sessionAffinity) || {target}.spec.sessionAffinity == 'None') && "
        f"(!has({target}.spec.internalTrafficPolicy) || "
        f"{target}.spec.internalTrafficPolicy == 'Cluster') && "
        f"{target}.spec.selector.size() == 3 && "
        f"{target}.spec.selector['app'] == {target}.metadata.name + '-g' + {generation} && "
        f"{target}.spec.selector['loom.dev/instance'] == {owner} && "
        f"{target}.spec.selector['loom.dev/generation'] == {generation} && "
        f"{target}.spec.ports.size() == 1 && "
        f"{target}.spec.ports[0].port == {service_port} && "
        f"{target}.spec.ports[0].targetPort == {target_port} && "
        f"(!has({target}.spec.ports[0].protocol) || "
        f"{target}.spec.ports[0].protocol == 'TCP'))"
    )

    def ingress_path(
        rule_index: int,
        path_index: int,
        *,
        path: str,
        service: str,
        port: int,
    ) -> str:
        item = f"{target}.spec.rules[{rule_index}].http.paths[{path_index}]"
        return (
            f"({item}.path == '{path}' && {item}.pathType == 'Prefix' && "
            f"{item}.backend.service.name == '{service}' && "
            f"{item}.backend.service.port.number == {port})"
        )

    ingress_contract = (
        "request.resource.resource != 'ingresses' || "
        f"({target}.metadata.annotations.size() == 2 && "
        f"{target}.metadata.annotations['cert-manager.io/cluster-issuer'] == "
        f"'{profile.network.ingress_cluster_issuer}' && "
        f"{target}.metadata.annotations['nginx.ingress.kubernetes.io/proxy-read-timeout'] == "
        "'300' && "
        f"{target}.spec.ingressClassName == '{profile.network.ingress_class_name}' && "
        f"!has({target}.spec.defaultBackend) && "
        f"{target}.spec.rules.size() == 3 && "
        f"{target}.spec.rules[0].host == {owner} + '.dev.{INGRESS_HOST}' && "
        f"{target}.spec.rules[0].http.paths.size() == 2 && "
        f"{ingress_path(0, 0, path='/api/v1', service='loom-service', port=8090)} && "
        f"{ingress_path(0, 1, path='/', service='loom-web', port=80)} && "
        f"{target}.spec.rules[1].host == 'cp-' + {owner} + '.dev.{INGRESS_HOST}' && "
        f"{target}.spec.rules[1].http.paths.size() == 1 && "
        f"{ingress_path(1, 0, path='/', service='loom-control-plane', port=8080)} && "
        f"{target}.spec.rules[2].host == 'gw-' + {owner} + '.dev.{INGRESS_HOST}' && "
        f"{target}.spec.rules[2].http.paths.size() == 1 && "
        f"{ingress_path(2, 0, path='/', service='loom-llm-gateway', port=9100)} && "
        f"{target}.spec.tls.size() == 1 && "
        f"{target}.spec.tls[0].secretName == 'loom-dev-tls' && "
        f"{target}.spec.tls[0].hosts.size() == 3 && "
        f"{target}.spec.tls[0].hosts[0] == {owner} + '.dev.{INGRESS_HOST}' && "
        f"{target}.spec.tls[0].hosts[1] == 'cp-' + {owner} + '.dev.{INGRESS_HOST}' && "
        f"{target}.spec.tls[0].hosts[2] == 'gw-' + {owner} + '.dev.{INGRESS_HOST}')"
    )
    policy = {
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": _metadata(context, name),
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [
                    {
                        "apiGroups": ["", "networking.k8s.io"],
                        "apiVersions": ["*"],
                        "operations": ["CREATE", "UPDATE", "DELETE"],
                        "resources": ["services", "ingresses"],
                    }
                ]
            },
            "matchConditions": [
                {
                    "name": "exact-activation-principal",
                    "expression": f"request.userInfo.username == '{_ACTIVATION_PRINCIPAL}'",
                }
            ],
            "validations": [
                {
                    "expression": personal_namespace,
                    "message": "activation may mutate routes only in personal namespaces",
                },
                {
                    "expression": (
                        f"{target}.metadata.labels['app.kubernetes.io/managed-by'] == "
                        "'loom-dev-instance-controller'"
                    ),
                    "message": "activation route lacks lifecycle ownership",
                },
                {
                    "expression": (
                        "(request.resource.resource == 'services' && "
                        f"{target}.metadata.name in "
                        "['loom-control-plane','loom-llm-gateway','loom-service','loom-web']) || "
                        "(request.resource.resource == 'ingresses' && "
                        f"{target}.metadata.name == 'loom-dev')"
                    ),
                    "message": "activation resource name is outside the stable-route contract",
                },
                {
                    "expression": (
                        f"request.namespace == 'loom-dev-' + {owner} && "
                        f"{generation}.matches('^[1-9][0-9]*$')"
                    ),
                    "message": "activation route owner or generation is invalid",
                },
                {
                    "expression": service_contract,
                    "message": "activation Service differs from the exact stable-route contract",
                },
                {
                    "expression": ingress_contract,
                    "message": "activation Ingress differs from the exact stable-route contract",
                },
            ],
        },
    }
    return policy, _admission_binding(context, name)


def _service(
    context: _RenderContext,
    name: str,
    *,
    selector: dict[str, str],
    ports: list[dict[str, Any]],
    headless: bool = False,
) -> dict[str, Any]:
    spec: dict[str, Any] = {"selector": selector, "ports": ports}
    if headless:
        spec.update({"clusterIP": "None", "publishNotReadyAddresses": True})
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata(context, name, namespace="loom-dev"),
        "spec": spec,
    }


def _postgres(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
) -> tuple[dict[str, Any], ...]:
    labels = {"app": "loom-dev-postgres"}
    headless = _service(
        context,
        "loom-dev-postgres-headless",
        selector=labels,
        ports=[{"name": "postgres", "port": 5432, "targetPort": 5432}],
        headless=True,
    )
    service = _service(
        context,
        "loom-dev-postgres",
        selector=labels,
        ports=[{"name": "postgres", "port": 5432, "targetPort": 5432}],
    )
    stateful_set = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": _metadata(context, "loom-dev-postgres", namespace="loom-dev"),
        "spec": {
            "serviceName": "loom-dev-postgres-headless",
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": _pod_metadata(context, labels=labels),
                "spec": {
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "securityContext": _pod_security(user=999, group=999),
                    "containers": [
                        {
                            "name": "postgres",
                            "image": release.images.postgres,
                            "imagePullPolicy": "IfNotPresent",
                            "args": ["-c", "max_connections=400"],
                            "env": [
                                _secret_env(
                                    "POSTGRES_USER",
                                    profile.identities.management_secret,
                                    "postgres-user",
                                ),
                                _secret_env(
                                    "POSTGRES_PASSWORD",
                                    profile.identities.management_secret,
                                    "postgres-password",
                                ),
                                _secret_env(
                                    "POSTGRES_DB",
                                    profile.identities.management_secret,
                                    "postgres-database",
                                ),
                                _literal_env(
                                    "PGDATA",
                                    "/var/lib/postgresql/data/pgdata",
                                ),
                            ],
                            "ports": [{"name": "postgres", "containerPort": 5432}],
                            "readinessProbe": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-euc",
                                        'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
                                    ]
                                },
                                "periodSeconds": 5,
                                "timeoutSeconds": 3,
                                "failureThreshold": 12,
                            },
                            "resources": _resources(profile.resources.postgres),
                            "securityContext": _container_security(user=999),
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/var/lib/postgresql/data"},
                                {"name": "run", "mountPath": "/var/run/postgresql"},
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "run", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}},
                        {"name": "tmp", "emptyDir": {"sizeLimit": "128Mi"}},
                    ],
                },
            },
            "volumeClaimTemplates": [
                {
                    "metadata": _metadata(context, "data"),
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": profile.storage.storage_class_name,
                        "resources": {"requests": {"storage": profile.storage.postgres_storage}},
                    },
                }
            ],
        },
    }
    return headless, service, stateful_set


def _minio(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
) -> tuple[dict[str, Any], ...]:
    labels = {"app": "loom-dev-minio"}
    service = _service(
        context,
        "loom-dev-minio",
        selector=labels,
        ports=[
            {"name": "s3", "port": 9000, "targetPort": 9000},
            {"name": "console", "port": 9001, "targetPort": 9001},
        ],
    )
    secret_env = [
        _secret_env(
            "MINIO_ROOT_USER",
            profile.identities.management_secret,
            "minio-access-key",
        ),
        _secret_env(
            "MINIO_ROOT_PASSWORD",
            profile.identities.management_secret,
            "minio-secret-key",
        ),
    ]
    stateful_set = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": _metadata(context, "loom-dev-minio", namespace="loom-dev"),
        "spec": {
            "serviceName": "loom-dev-minio",
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": _pod_metadata(context, labels=labels),
                "spec": {
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "securityContext": _pod_security(user=1000, group=1000),
                    "containers": [
                        {
                            "name": "minio",
                            "image": release.images.minio,
                            "imagePullPolicy": "IfNotPresent",
                            "args": ["server", "/data", "--console-address", ":9001"],
                            "env": secret_env,
                            "ports": [
                                {"name": "s3", "containerPort": 9000},
                                {"name": "console", "containerPort": 9001},
                            ],
                            "readinessProbe": {
                                "httpGet": {"path": "/minio/health/ready", "port": "s3"},
                                "periodSeconds": 5,
                                "timeoutSeconds": 3,
                                "failureThreshold": 12,
                            },
                            "resources": _resources(profile.resources.minio),
                            "securityContext": _container_security(user=1000),
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/data"},
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        },
                        {
                            "name": "admin",
                            "image": release.images.minio_client,
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "/bin/sh",
                                "-euc",
                                "\n".join(
                                    (
                                        'export MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"',
                                        "attempt=0",
                                        "until mc ready local >/dev/null 2>&1; do",
                                        "  attempt=$((attempt + 1))",
                                        '  test "$attempt" -lt 150',
                                        "  sleep 2",
                                        "done",
                                        "mc mb --ignore-existing local/artifacts local/trajectories >/dev/null",
                                        "exec sleep 2147483647",
                                    )
                                ),
                            ],
                            "env": [
                                *secret_env,
                                _literal_env("MC_CONFIG_DIR", "/tmp/mc"),
                            ],
                            "readinessProbe": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-euc",
                                        (
                                            'export MC_HOST_local="http://${MINIO_ROOT_USER}:'
                                            '${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"; '
                                            "mc stat local/artifacts >/dev/null && "
                                            "mc stat local/trajectories >/dev/null"
                                        ),
                                    ]
                                },
                                "periodSeconds": 5,
                                "timeoutSeconds": 3,
                                "failureThreshold": 60,
                            },
                            "resources": {
                                "requests": {"cpu": "10m", "memory": "32Mi"},
                                "limits": {"cpu": "100m", "memory": "128Mi"},
                            },
                            "securityContext": _container_security(user=1000),
                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                        },
                    ],
                    "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "128Mi"}}],
                },
            },
            "volumeClaimTemplates": [
                {
                    "metadata": _metadata(context, "data"),
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": profile.storage.storage_class_name,
                        "resources": {"requests": {"storage": profile.storage.minio_storage}},
                    },
                }
            ],
        },
    }
    return service, stateful_set


def _scanner_cache(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": _metadata(
            context,
            profile.identities.scanner_cache_pvc,
            namespace="loom-dev",
        ),
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": profile.storage.storage_class_name,
            "resources": {"requests": {"storage": profile.storage.scanner_cache_storage}},
        },
    }


def _migration(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
) -> dict[str, Any]:
    labels = {"app": "loom-personal-dev-migration"}
    name = f"loom-personal-dev-migrate-{context.input_sha256[:16]}-{context.release_sha256[:16]}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": _metadata(context, name, namespace="loom-dev"),
        "spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": 600,
            "template": {
                "metadata": _pod_metadata(context, labels=labels),
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "securityContext": _pod_security(user=65532, group=65532),
                    "containers": [
                        {
                            "name": "migrate",
                            "image": release.images.loom_service,
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "/bin/sh",
                                "-euc",
                                "\n".join(
                                    (
                                        "attempt=0",
                                        (
                                            "until alembic -c migrations/alembic.ini current "
                                            ">/dev/null 2>&1; do"
                                        ),
                                        "  attempt=$((attempt + 1))",
                                        '  test "$attempt" -lt 100',
                                        "  sleep 2",
                                        "done",
                                        "exec alembic -c migrations/alembic.ini upgrade head",
                                    )
                                ),
                            ],
                            "env": [
                                _secret_env(
                                    "LOOM_DB_URL",
                                    profile.identities.management_secret,
                                    "svc-db-url",
                                ),
                                _literal_env("PGCONNECT_TIMEOUT", "3"),
                            ],
                            "resources": _resources(profile.resources.migration),
                            "securityContext": _container_security(user=65532),
                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                        }
                    ],
                    "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "128Mi"}}],
                },
            },
        },
    }


def _management_env(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan | None = None,
) -> list[dict[str, Any]]:
    secret = profile.identities.management_secret
    capabilities = json.dumps(
        [
            {
                "capability_id": f"{item.pool_id}-{item.architecture}-none",
                "pool_id": item.pool_id,
                "operating_system": "linux",
                "cpu_architecture": item.architecture,
                "gpu_vendor": "none",
                "network_policies": ["public"],
            }
            for item in profile.pools
        ],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    protocols = json.dumps(
        dict(profile.protocol_versions),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    acceptance_env: list[dict[str, Any]] = []
    scanner_identity = (
        f"trivy-bin-sha256:{release.scanner.binary_sha256}:"
        f"db-sha256:{release.scanner.database_sha256}:"
        f"java-db-sha256:{release.scanner.java_database_sha256}"
    )
    scanner_generation = (
        "/var/lib/loom-personal-dev-scanner/generations/"
        + release.scanner.cache_identity_sha256
    )
    scanner_policy_sha256 = ""
    launcher_profile_sha256 = ""
    runtime_class_name = profile.builder.runtime_class_name
    registry_prefix = profile.builder.registry_prefix
    publisher_identity = profile.builder.publisher_identity
    activation_key_id = "personal-dev-agent-v1"
    if plan is not None:
        scanner_policy_sha256 = plan.builder.scanner_finding_policy_sha256
        launcher_profile_sha256 = plan.builder.trusted_launcher_profile_sha256
        runtime_class_name = plan.builder.runtime_class_name
        registry_prefix = plan.builder.registry_prefix
        publisher_identity = plan.builder.publisher_identity
        activation_key_id = plan.activation.key_id
        acceptance_env = [
            _literal_env(
                "LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_BINDING_JSON",
                plan.manager_runtime_json(),
            ),
            _literal_env(
                "LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_PLAN_SHA256",
                plan.sha256,
            ),
            _literal_env(
                "LOOM_SVC_PERSONAL_DEV_ACTIVATION_PUBLIC_KEY_SHA256",
                plan.activation.public_key_sha256,
            ),
        ]
    return [
        _literal_env("LOOM_ENV", "dev"),
        _literal_env("LOOM_NAMESPACE", "loom-dev"),
        _literal_env("LOOM_SVC_BIND_HOST", "0.0.0.0"),
        _literal_env("LOOM_SVC_BIND_PORT", "8090"),
        _literal_env("LOOM_SVC_LOG_LEVEL", "info"),
        _literal_env("LOOM_SVC_PUBLIC_BASE_URL", profile.network.public_origin),
        _literal_env("LOOM_SVC_CONTROL_PLANE_URL", "http://127.0.0.1:9"),
        _literal_env("LOOM_SVC_GATEWAY_URL", "http://127.0.0.1:9"),
        _literal_env("LOOM_SVC_K8S_WORKER_ENABLED", "false"),
        _literal_env("LOOM_SVC_DEV_INSTANCES_ENABLED", "true" if plan is not None else "false"),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED",
            "true" if plan is not None else "false",
        ),
        *acceptance_env,
        _literal_env("LOOM_SVC_DEV_INSTANCE_KUBE_CONTEXT", ""),
        _literal_env("LOOM_SVC_DEV_INSTANCE_KUBECTL_PATH", "/usr/local/bin/kubectl"),
        _literal_env(
            "LOOM_SVC_DEV_INSTANCE_INGRESS_CLASS_NAME",
            profile.network.ingress_class_name,
        ),
        _literal_env(
            "LOOM_SVC_DEV_INSTANCE_INGRESS_CERT_MANAGER_CLUSTER_ISSUER",
            profile.network.ingress_cluster_issuer,
        ),
        _secret_env("LOOM_SVC_DB_URL", secret, "svc-db-url"),
        _secret_env(
            "LOOM_SVC_DEV_INSTANCE_DATABASE_ADMIN_URL",
            secret,
            "dev-instance-database-admin-url",
        ),
        _literal_env("LOOM_SVC_MINIO_ENDPOINT", "http://loom-dev-minio:9000"),
        _literal_env("LOOM_SVC_MINIO_REGION", "us-east-1"),
        _secret_env("LOOM_SVC_MINIO_ACCESS_KEY", secret, "minio-access-key"),
        _secret_env("LOOM_SVC_MINIO_SECRET_KEY", secret, "minio-secret-key"),
        _secret_env("LOOM_SECRET_STORE_MASTER_KEY", secret, "secret-store-master-key"),
        _literal_env("LOOM_SVC_ARTIFACTS_BUCKET", "artifacts"),
        _literal_env("LOOM_SVC_TRAJECTORIES_BUCKET", "trajectories"),
        _literal_env(
            "LOOM_SVC_ADMIN_SECRET_FILE",
            "/run/loom-personal-dev/management/files/admin-secrets.toml",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_ACTIVATION_PUBLIC_KEY_FILE",
            "/run/loom-personal-dev/activation-public/files/public-key",
        ),
        _literal_env("LOOM_SVC_PERSONAL_DEV_ACTIVATION_KEY_ID", activation_key_id),
        _literal_env("LOOM_SVC_PERSONAL_DEV_BUILDER_IMAGE", release.images.personal_dev_builder),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_RUNTIME_CLASS_NAME",
            runtime_class_name,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_REGISTRY_AUTH_FILE",
            "/run/loom-personal-dev/management/files/config.json",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_REGISTRY_PREFIX",
            registry_prefix,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_PUBLISHER_IDENTITY",
            publisher_identity,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_DIR",
            scanner_generation,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_IDENTITY_SHA256",
            release.scanner.cache_identity_sha256,
        ),
        _literal_env("LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_IDENTITY", scanner_identity),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_DATABASE_METADATA_SHA256",
            release.scanner.database_metadata_sha256,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_JAVA_DATABASE_METADATA_SHA256",
            release.scanner.java_database_metadata_sha256,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_POLICY_SHA256",
            scanner_policy_sha256,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_TRUSTED_LAUNCHER_PROFILE_SHA256",
            launcher_profile_sha256,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_GLOBAL_CONCURRENCY",
            str(profile.limits.builder_global_concurrency),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_BUILDER_PER_OWNER_CONCURRENCY",
            str(profile.limits.builder_per_owner_concurrency),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_SOURCE_MAX_ARCHIVE_BYTES",
            str(profile.limits.source_max_archive_bytes),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CANDIDATE_RETAINED_COUNT_LIMIT",
            str(profile.limits.candidate_retained_count),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CANDIDATE_RETAINED_BYTES_LIMIT",
            str(profile.limits.candidate_retained_bytes),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_GLOBAL_LIVE_INSTANCE_LIMIT",
            str(profile.limits.global_live_instances),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_PER_OWNER_LIVE_INSTANCE_LIMIT",
            str(profile.limits.per_owner_live_instances),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_PER_OWNER_AGGREGATE_MIN_SLOTS",
            str(profile.limits.per_owner_aggregate_min_slots),
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_PER_OWNER_AGGREGATE_MAX_SLOTS",
            str(profile.limits.per_owner_aggregate_max_slots),
        ),
        _literal_env("LOOM_SVC_PERSONAL_DEV_PROTOCOL_VERSIONS_JSON", protocols),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_AGENT_IMAGE",
            release.images.loom_service,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_MANAGER_ORIGIN",
            profile.network.capacity_manager_origin,
        ),
        _literal_env("LOOM_SVC_PERSONAL_DEV_CAPACITY_MANAGER_NAMESPACE", "loom-dev"),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_MANAGER_POD_LABEL_KEY",
            profile.network.capacity_manager_pod_label_key,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_MANAGER_POD_LABEL",
            profile.network.capacity_manager_pod_label_value,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_MANAGER_PORT",
            str(profile.network.capacity_manager_port),
        ),
        _literal_env("LOOM_SVC_PERSONAL_DEV_CAPACITY_DATABASE_NAMESPACE", "loom-dev"),
        _literal_env("LOOM_SVC_PERSONAL_DEV_CAPACITY_DATABASE_POD_LABEL_KEY", "app"),
        _literal_env("LOOM_SVC_PERSONAL_DEV_CAPACITY_DATABASE_POD_LABEL", "loom-dev-postgres"),
        _literal_env("LOOM_SVC_PERSONAL_DEV_CAPACITY_DATABASE_PORT", "5432"),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_DNS_NAMESPACE",
            profile.network.dns_namespace,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_DNS_POD_LABEL_KEY",
            profile.network.dns_pod_label_key,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_DNS_POD_LABEL",
            profile.network.dns_pod_label_value,
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_DNS_PORT",
            str(profile.network.dns_port),
        ),
        _literal_env("LOOM_SVC_PERSONAL_DEV_CAPACITY_POOL_CAPABILITIES_JSON", capabilities),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_LIFECYCLE_BEARER_TOKEN_FILE",
            "/run/loom-personal-dev/management/files/capacity-lifecycle-token",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_LIFECYCLE_CA_FILE",
            "/run/loom-personal-dev/management/files/capacity-lifecycle-ca.pem",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_LIFECYCLE_CERTIFICATE_FILE",
            "/run/loom-personal-dev/management/files/capacity-lifecycle-certificate.pem",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_LIFECYCLE_PRIVATE_KEY_FILE",
            "/run/loom-personal-dev/management/files/capacity-lifecycle-private-key.pem",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_CA_FILE",
            "/run/loom-personal-dev/management/files/capacity-reporter-ca.pem",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_CERTIFICATE_FILE",
            "/run/loom-personal-dev/management/files/capacity-reporter-certificate.pem",
        ),
        _literal_env(
            "LOOM_SVC_PERSONAL_DEV_CAPACITY_PRIVATE_KEY_FILE",
            "/run/loom-personal-dev/management/files/capacity-reporter-private-key.pem",
        ),
        _literal_env("LOOM_SVC_TEAM_REGISTRATION_OPEN", "false"),
        _literal_env("LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN", "false"),
        _literal_env("LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION", "true"),
    ]


def _credential_init(
    *,
    name: str,
    image: str,
    source_volume: str,
    source_path: str,
    destination_parent: str,
    destination: str,
    profile: str,
) -> dict[str, Any]:
    command = (
        f"install -d -m 0700 {destination_parent}; "
        f"exec python -m loom.personal_dev_secret_init --profile {profile} "
        f"--source {source_path} --destination {destination}"
    )
    return {
        "name": name,
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/sh", "-euc", command],
        "resources": {
            "requests": {"cpu": "10m", "memory": "32Mi"},
            "limits": {"cpu": "100m", "memory": "128Mi"},
        },
        "securityContext": _container_security(user=65532),
        "volumeMounts": [
            {"name": source_volume, "mountPath": source_path, "readOnly": True},
            {"name": "runtime-credentials", "mountPath": "/run/loom-personal-dev"},
        ],
    }


def _scanner_cache_init(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
) -> dict[str, Any]:
    return {
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
        "securityContext": _container_security(user=65531),
        "resources": _resources(profile.resources.management),
        "volumeMounts": [
            {
                "name": "scanner-cache",
                "mountPath": "/var/lib/loom-personal-dev-scanner",
            },
            {"name": "tmp", "mountPath": "/tmp"},
        ],
    }


def _management_deployment(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan | None = None,
) -> dict[str, Any]:
    labels = {"app": "loom-personal-dev-management"}
    projected_management = [{"key": filename, "path": filename} for filename in _MANAGEMENT_FILES]
    generation_subpath = f"generations/{release.scanner.cache_identity_sha256}"
    generation_path = f"/var/lib/loom-personal-dev-scanner/{generation_subpath}"
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata(
            context,
            profile.identities.management_service,
            namespace="loom-dev",
        ),
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 2,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": _pod_metadata(context, labels=labels),
                "spec": {
                    "serviceAccountName": profile.identities.management_service_account,
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "securityContext": _pod_security(user=65532, group=65532),
                    "nodeSelector": {"kubernetes.io/arch": "amd64"},
                    "initContainers": [
                        _scanner_cache_init(profile, release),
                        _credential_init(
                            name="management-credential-init",
                            image=release.images.loom_service,
                            source_volume="management-projected",
                            source_path="/var/run/loom-personal-dev-management-projected",
                            destination_parent="/run/loom-personal-dev/management",
                            destination="/run/loom-personal-dev/management/files",
                            profile="management-files",
                        ),
                        _credential_init(
                            name="activation-public-credential-init",
                            image=release.images.loom_service,
                            source_volume="activation-public-projected",
                            source_path="/var/run/loom-personal-dev-activation-public-projected",
                            destination_parent="/run/loom-personal-dev/activation-public",
                            destination="/run/loom-personal-dev/activation-public/files",
                            profile="activation-public",
                        ),
                    ],
                    "containers": [
                        {
                            "name": "management",
                            "image": release.images.loom_service,
                            "imagePullPolicy": "IfNotPresent",
                            "env": _management_env(profile, release, plan),
                            "ports": [{"name": "http", "containerPort": 8090}],
                            "readinessProbe": {
                                "httpGet": {
                                    "path": (
                                        "/api/v1/health/personal-dev-acceptance"
                                        if plan is not None
                                        else "/api/v1/health"
                                    ),
                                    "port": "http",
                                },
                                "periodSeconds": 5,
                                "timeoutSeconds": 3,
                                "failureThreshold": 12,
                            },
                            "resources": _resources(profile.resources.management),
                            "securityContext": _container_security(user=65532),
                            "volumeMounts": [
                                {
                                    "name": "runtime-credentials",
                                    "mountPath": "/run/loom-personal-dev",
                                    "readOnly": True,
                                },
                                {
                                    "name": "scanner-cache",
                                    "mountPath": generation_path,
                                    "subPath": generation_subpath,
                                },
                                {
                                    "name": "scanner-fanal",
                                    "mountPath": f"{generation_path}/fanal",
                                },
                                _service_account_token_mount(),
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "management-projected",
                            "secret": {
                                "secretName": profile.identities.management_secret,
                                "defaultMode": 0o440,
                                "items": projected_management,
                            },
                        },
                        {
                            "name": "activation-public-projected",
                            "secret": {
                                "secretName": profile.identities.activation_public_secret,
                                "defaultMode": 0o440,
                                "items": [{"key": "public-key", "path": "public-key"}],
                            },
                        },
                        {
                            "name": "runtime-credentials",
                            "emptyDir": {"medium": "Memory", "sizeLimit": "32Mi"},
                        },
                        {
                            "name": "scanner-cache",
                            "persistentVolumeClaim": {
                                "claimName": profile.identities.scanner_cache_pvc
                            },
                        },
                        {"name": "scanner-fanal", "emptyDir": {"sizeLimit": "4Gi"}},
                        _service_account_token_volume(),
                        {"name": "tmp", "emptyDir": {"sizeLimit": "1Gi"}},
                    ],
                },
            },
        },
    }


def _management_service_and_ingress(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
) -> tuple[dict[str, Any], ...]:
    service = _service(
        context,
        profile.identities.management_service,
        selector={"app": "loom-personal-dev-management"},
        ports=[{"name": "http", "port": 8090, "targetPort": 8090}],
    )
    host = urlsplit(profile.network.public_origin).hostname
    assert host is not None
    ingress = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": _metadata(
            context,
            profile.identities.management_ingress,
            namespace="loom-dev",
            annotations={
                "cert-manager.io/cluster-issuer": profile.network.ingress_cluster_issuer,
                "nginx.ingress.kubernetes.io/proxy-body-size": "512m",
                "nginx.ingress.kubernetes.io/proxy-read-timeout": "300",
            },
        ),
        "spec": {
            "ingressClassName": profile.network.ingress_class_name,
            "tls": [
                {
                    "hosts": [host],
                    "secretName": "loom-personal-dev-management-tls",
                }
            ],
            "rules": [
                {
                    "host": host,
                    "http": {
                        "paths": [
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": profile.identities.management_service,
                                        "port": {"number": 8090},
                                    }
                                },
                            }
                        ]
                    },
                }
            ],
        },
    }
    return service, ingress


def _activation_deployment(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan | None = None,
) -> dict[str, Any]:
    labels = {"app": "loom-personal-dev-activation-agent"}
    init = _credential_init(
        name="activation-private-credential-init",
        image=release.images.personal_dev_activation_agent,
        source_volume="activation-private-projected",
        source_path="/var/run/loom-personal-dev-activation-private-projected",
        destination_parent="/run/loom-personal-dev/activation-private",
        destination="/run/loom-personal-dev/activation-private/files",
        profile="activation-private",
    )
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata(
            context,
            "loom-personal-dev-activation-agent",
            namespace="loom-dev",
        ),
        "spec": {
            "replicas": 1 if plan is not None else profile.activation_agent_replicas,
            "revisionHistoryLimit": 2,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": _pod_metadata(context, labels=labels),
                "spec": {
                    "serviceAccountName": profile.identities.activation_service_account,
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "securityContext": _pod_security(user=65532, group=65532),
                    "initContainers": [init],
                    "containers": [
                        {
                            "name": "activation-agent",
                            "image": release.images.personal_dev_activation_agent,
                            "imagePullPolicy": "IfNotPresent",
                            "env": [
                                _literal_env(
                                    "LOOM_PERSONAL_DEV_ACTIVATION_SERVICE_URL",
                                    "http://loom-personal-dev-management:8090",
                                ),
                                _literal_env(
                                    "LOOM_PERSONAL_DEV_ACTIVATION_ALLOW_INSECURE_HTTP",
                                    "1",
                                ),
                                _literal_env(
                                    "LOOM_PERSONAL_DEV_ACTIVATION_KEY_ID",
                                    (
                                        plan.activation.key_id
                                        if plan is not None
                                        else "personal-dev-agent-v1"
                                    ),
                                ),
                                _literal_env(
                                    "LOOM_PERSONAL_DEV_ACTIVATION_PRIVATE_KEY_FILE",
                                    "/run/loom-personal-dev/activation-private/files/private-key",
                                ),
                                _literal_env(
                                    "LOOM_PERSONAL_DEV_ACTIVATION_MINIO_ENDPOINT",
                                    "http://loom-dev-minio:9000",
                                ),
                                _literal_env(
                                    "LOOM_PERSONAL_DEV_ACTIVATION_INGRESS_CLASS_NAME",
                                    profile.network.ingress_class_name,
                                ),
                                _literal_env(
                                    "LOOM_PERSONAL_DEV_ACTIVATION_INGRESS_CLUSTER_ISSUER",
                                    profile.network.ingress_cluster_issuer,
                                ),
                            ],
                            "resources": _resources(profile.resources.activation),
                            "securityContext": _container_security(user=65532),
                            "volumeMounts": [
                                {
                                    "name": "runtime-credentials",
                                    "mountPath": "/run/loom-personal-dev",
                                    "readOnly": True,
                                },
                                _service_account_token_mount(),
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "activation-private-projected",
                            "secret": {
                                "secretName": profile.identities.activation_private_secret,
                                "defaultMode": 0o440,
                                "items": [{"key": "private-key", "path": "private-key"}],
                            },
                        },
                        {
                            "name": "runtime-credentials",
                            "emptyDir": {"medium": "Memory", "sizeLimit": "2Mi"},
                        },
                        _service_account_token_volume(),
                        {"name": "tmp", "emptyDir": {"sizeLimit": "128Mi"}},
                    ],
                },
            },
        },
    }


def _network_policies(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
) -> tuple[dict[str, Any], ...]:
    def policy(name: str, spec: dict[str, Any]) -> dict[str, Any]:
        return {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata(context, name, namespace="loom-dev"),
            "spec": spec,
        }

    dns = {
        "to": [
            {
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": profile.network.dns_namespace}
                },
                "podSelector": {
                    "matchLabels": {
                        profile.network.dns_pod_label_key: (profile.network.dns_pod_label_value)
                    }
                },
            }
        ],
        "ports": [
            {"protocol": "UDP", "port": profile.network.dns_port},
            {"protocol": "TCP", "port": profile.network.dns_port},
        ],
    }
    api = {
        "to": [{"ipBlock": {"cidr": profile.network.kubernetes_api_cidr}}],
        "ports": [{"protocol": "TCP", "port": profile.network.kubernetes_api_port}],
    }
    postgres = {
        "to": [{"podSelector": {"matchLabels": {"app": "loom-dev-postgres"}}}],
        "ports": [{"protocol": "TCP", "port": 5432}],
    }
    minio = {
        "to": [{"podSelector": {"matchLabels": {"app": "loom-dev-minio"}}}],
        "ports": [{"protocol": "TCP", "port": 9000}],
    }
    manager = {
        "to": [
            {
                "podSelector": {
                    "matchLabels": {
                        profile.network.capacity_manager_pod_label_key: (
                            profile.network.capacity_manager_pod_label_value
                        )
                    }
                }
            }
        ],
        "ports": [{"protocol": "TCP", "port": profile.network.capacity_manager_port}],
    }
    default = policy(
        "loom-personal-dev-default-deny",
        {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
    )
    postgres_ingress = policy(
        "loom-personal-dev-postgres-ingress",
        {
            "podSelector": {"matchLabels": {"app": "loom-dev-postgres"}},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "podSelector": {
                                "matchExpressions": [
                                    {
                                        "key": "app",
                                        "operator": "In",
                                        "values": [
                                            "loom-personal-dev-management",
                                            "loom-personal-dev-migration",
                                        ],
                                    }
                                ]
                            }
                        },
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/managed-by": ("loom-dev-instance-controller")
                                }
                            }
                        },
                    ],
                    "ports": [{"protocol": "TCP", "port": 5432}],
                }
            ],
        },
    )
    minio_ingress = policy(
        "loom-personal-dev-minio-ingress",
        {
            "podSelector": {"matchLabels": {"app": "loom-dev-minio"}},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "podSelector": {
                                "matchExpressions": [
                                    {
                                        "key": "app",
                                        "operator": "In",
                                        "values": [
                                            "loom-personal-dev-management",
                                            "loom-personal-dev-activation-agent",
                                        ],
                                    }
                                ]
                            }
                        },
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/managed-by": ("loom-dev-instance-controller")
                                }
                            }
                        },
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/managed-by": (
                                        "loom-personal-dev-builder-controller"
                                    )
                                }
                            }
                        },
                    ],
                    "ports": [{"protocol": "TCP", "port": 9000}],
                }
            ],
        },
    )
    management = policy(
        "loom-personal-dev-management",
        {
            "podSelector": {"matchLabels": {"app": "loom-personal-dev-management"}},
            "policyTypes": ["Egress"],
            "egress": [dns, postgres, minio, manager, api],
        },
    )
    migration = policy(
        "loom-personal-dev-migration-egress",
        {
            "podSelector": {"matchLabels": {"app": "loom-personal-dev-migration"}},
            "policyTypes": ["Egress"],
            "egress": [dns, postgres],
        },
    )
    management_ingress = policy(
        "loom-personal-dev-management-ingress",
        {
            "podSelector": {"matchLabels": {"app": "loom-personal-dev-management"}},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}
                            },
                            "podSelector": {
                                "matchLabels": {"app.kubernetes.io/name": "ingress-nginx"}
                            },
                        },
                        {
                            "podSelector": {
                                "matchLabels": {"app": "loom-personal-dev-activation-agent"}
                            }
                        },
                    ],
                    "ports": [{"protocol": "TCP", "port": 8090}],
                }
            ],
        },
    )
    activation = policy(
        "loom-personal-dev-activation",
        {
            "podSelector": {"matchLabels": {"app": "loom-personal-dev-activation-agent"}},
            "policyTypes": ["Egress"],
            "egress": [
                dns,
                {
                    "to": [
                        {"podSelector": {"matchLabels": {"app": "loom-personal-dev-management"}}}
                    ],
                    "ports": [{"protocol": "TCP", "port": 8090}],
                },
                minio,
                api,
            ],
        },
    )
    return (
        default,
        postgres_ingress,
        minio_ingress,
        management,
        management_ingress,
        migration,
        activation,
    )


def _sort_key(document: dict[str, Any]) -> tuple[int, str, str, str, str]:
    metadata = document["metadata"]
    namespace = str(metadata.get("namespace", ""))
    return (
        0 if not namespace else 1,
        document["apiVersion"],
        document["kind"],
        namespace,
        metadata["name"],
    )


def _render_documents(
    context: _RenderContext,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan | None = None,
) -> RenderedPersonalDevControlPlane:
    shared_role, shared_binding = _shared_role(context)
    runtime = plan.builder if plan is not None else profile.builder
    documents = [
        _namespace(context),
        _management_mutation_role(context),
        _management_mutation_binding(context),
        _managed_namespace_role(context),
        _activation_role(context),
        *_management_namespace_admission(context),
        *_management_resource_admission(
            context,
            builder_image=release.images.personal_dev_builder,
            runtime_class_name=runtime.runtime_class_name,
        ),
        *_activation_admission(context, profile),
        _service_account(context, profile.identities.management_service_account),
        _service_account(context, profile.identities.activation_service_account),
        shared_role,
        shared_binding,
        *_postgres(context, profile, release),
        *_minio(context, profile, release),
        _scanner_cache(context, profile),
        _migration(context, profile, release),
        _management_deployment(context, profile, release, plan),
        *_management_service_and_ingress(context, profile),
        _activation_deployment(context, profile, release, plan),
        *_network_policies(context, profile),
    ]
    documents.sort(key=_sort_key)
    yaml_text = yaml.safe_dump_all(
        documents,
        explicit_start=False,
        sort_keys=False,
        default_flow_style=False,
    )
    return RenderedPersonalDevControlPlane(
        yaml_text=yaml_text,
        input_sha256=context.input_sha256,
        release_sha256=context.release_sha256,
        resource_count=len(documents),
        runtime_class_name=runtime.runtime_class_name,
        runtime_handler=runtime.runtime_handler,
        runtime_profile_sha256=runtime.runtime_profile_sha256,
    )


def render_shadow_personal_dev_control_plane(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
) -> RenderedPersonalDevControlPlane:
    """Render one immutable shadow package without reading or changing live state."""

    if not isinstance(profile, PersonalDevControlPlaneProfile):
        raise TypeError("personal-dev control-plane profile is invalid")
    if not isinstance(release, PersonalDevTrustedRelease):
        raise TypeError("personal-dev trusted release is invalid")
    if (
        profile.namespace != "loom-dev"
        or profile.dev_instances_enabled
        or profile.personal_dev_builder_enabled
        or profile.activation_agent_replicas != 0
        or profile.executable_new_capacity_ceiling != 0
        or profile.builder.prepared
    ):
        raise ValueError("personal-dev control-plane profile is not inert shadow state")
    input_sha256 = hashlib.sha256(
        b"loom-personal-dev-shadow-render-v1\0"
        + profile.canonical_bytes()
        + b"\0"
        + release.canonical_bytes()
    ).hexdigest()
    release_sha256 = hashlib.sha256(release.canonical_bytes()).hexdigest()
    context = _RenderContext(
        input_sha256=input_sha256,
        release_sha256=release_sha256,
    )
    return _render_documents(context, profile, release)


def render_acceptance_personal_dev_control_plane(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan,
    *,
    now: datetime,
) -> RenderedPersonalDevControlPlane:
    """Render the reviewed personal application acceptance without capacity."""

    if not isinstance(plan, PersonalDevAcceptancePlan):
        raise TypeError("personal-dev acceptance plan is invalid")
    shadow = render_shadow_personal_dev_control_plane(profile, release)
    validate_personal_dev_acceptance_plan(
        profile,
        release,
        hashlib.sha256(shadow.yaml_text.encode("utf-8")).hexdigest(),
        plan,
        now=now,
    )
    input_sha256 = hashlib.sha256(
        b"loom-personal-dev-acceptance-render-v1\0"
        + profile.canonical_bytes()
        + release.canonical_bytes()
        + plan.canonical_bytes()
    ).hexdigest()
    context = _RenderContext(
        input_sha256=input_sha256,
        release_sha256=hashlib.sha256(release.canonical_bytes()).hexdigest(),
        acceptance_plan_sha256=plan.sha256,
    )
    return _render_documents(context, profile, release, plan)


__all__ = [
    "RenderedPersonalDevControlPlane",
    "render_acceptance_personal_dev_control_plane",
    "render_shadow_personal_dev_control_plane",
]
