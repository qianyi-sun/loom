"""Checked-in fail-closed authority contract for isolated rehearsal."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

import yaml  # type: ignore[import-untyped]

DEFAULT_REHEARSAL_AUTHORITY_MANIFEST = Path("deploy/k8s/staging-rollout-rehearsal-authority.yaml")
REHEARSAL_PRINCIPAL = "system:serviceaccount:loom-rollout-system:loom-rollout-rehearsal"
_EXPECTED_KINDS = (
    "Namespace",
    "ServiceAccount",
    "ClusterRole",
    "ClusterRole",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyBinding",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyBinding",
    "ClusterRoleBinding",
    "Role",
    "RoleBinding",
)
_MUTATING_VERBS = frozenset({"create", "delete", "patch", "update"})
_EXPECTED_CANONICAL_SHA256 = "fb90731781b7187d0b6a1aa38f379ce83aa32070f60c77057487799427040560"
_MUTATOR_RULES = [
    {
        "apiGroups": [""],
        "resources": ["namespaces"],
        "verbs": ["create", "delete", "get", "patch", "update"],
    },
    {
        "apiGroups": [""],
        "resources": ["nodes"],
        "resourceNames": [f"trt-eai-oldlab-{index}" for index in range(1, 6)],
        "verbs": ["get"],
    },
    {
        "apiGroups": [""],
        "resources": [
            "configmaps",
            "endpoints",
            "persistentvolumeclaims",
            "pods",
            "pods/exec",
            "pods/log",
            "secrets",
            "serviceaccounts",
            "services",
        ],
        "verbs": ["create", "delete", "patch", "update"],
    },
    {
        "apiGroups": [""],
        "resources": ["pods/portforward"],
        "verbs": ["create"],
    },
    {
        "apiGroups": ["apps"],
        "resources": ["deployments", "replicasets", "statefulsets"],
        "verbs": ["create", "delete", "patch", "update"],
    },
    {
        "apiGroups": ["batch"],
        "resources": ["jobs"],
        "verbs": ["create", "delete", "patch", "update"],
    },
    {
        "apiGroups": ["networking.k8s.io"],
        "resources": ["ingresses", "networkpolicies"],
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
        "resourceNames": ["loom-rollout-rehearsal-observer"],
        "verbs": ["bind"],
    },
    {
        "apiGroups": ["authentication.k8s.io"],
        "resources": ["selfsubjectreviews"],
        "verbs": ["create"],
    },
    {
        "apiGroups": ["authorization.k8s.io"],
        "resources": ["selfsubjectaccessreviews", "selfsubjectrulesreviews"],
        "verbs": ["create"],
    },
]
_OBSERVER_RULES = [
    {
        "apiGroups": [""],
        "resources": [
            "configmaps",
            "endpoints",
            "persistentvolumeclaims",
            "pods",
            "pods/log",
            "secrets",
            "serviceaccounts",
            "services",
        ],
        "verbs": ["get", "list", "watch"],
    },
    {
        "apiGroups": ["apps"],
        "resources": ["deployments", "replicasets", "statefulsets"],
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
        "verbs": ["get", "list", "watch"],
    },
]


def rehearsal_authority_digest(
    path: Path = DEFAULT_REHEARSAL_AUTHORITY_MANIFEST,
) -> str:
    """Validate the exact least-privilege/admission composition and hash it."""
    if not path.is_file():
        raise ValueError("rehearsal authority manifest is unavailable")
    payload = path.read_bytes()
    if not payload or len(payload) > 1024 * 1024:
        raise ValueError("rehearsal authority manifest is invalid")
    try:
        documents = tuple(yaml.safe_load_all(payload))
    except yaml.YAMLError as exc:
        raise ValueError("rehearsal authority manifest is invalid") from exc
    if (
        len(documents) != len(_EXPECTED_KINDS)
        or any(not isinstance(document, dict) for document in documents)
        or tuple(document.get("kind") for document in documents) != _EXPECTED_KINDS
    ):
        raise ValueError("rehearsal authority resource set is invalid")
    (
        namespace,
        account,
        role,
        observer_role,
        namespace_policy,
        namespace_binding,
        resource_policy,
        resource_binding,
        binding,
        ingress_role,
        ingress_binding,
    ) = documents
    if (
        _name(namespace) != "loom-rollout-system"
        or _name(account) != "loom-rollout-rehearsal"
        or _namespace(account) != "loom-rollout-system"
        or account.get("automountServiceAccountToken") is not False
        or _name(role) != "loom-rollout-rehearsal"
        or _name(observer_role) != "loom-rollout-rehearsal-observer"
        or not _observer_role_is_readonly(observer_role)
        or _name(binding) != "loom-rollout-rehearsal"
        or not _binding_is_exact(binding)
        or not _ingress_role_is_exact(ingress_role)
        or not _ingress_binding_is_exact(ingress_binding)
        or not _role_is_bounded(role)
        or not _policy_is_exact(
            namespace_policy,
            name="loom-rollout-rehearsal-namespaces",
            required_fragment="metadata.name.startsWith('loom-rehearsal-')",
        )
        or not _policy_is_exact(
            resource_policy,
            name="loom-rollout-rehearsal-resources",
            required_fragment="request.namespace.startsWith('loom-rehearsal-')",
        )
        or not _policy_binding_is_exact(
            namespace_binding,
            name="loom-rollout-rehearsal-namespaces",
        )
        or not _policy_binding_is_exact(
            resource_binding,
            name="loom-rollout-rehearsal-resources",
        )
    ):
        raise ValueError("rehearsal authority contract drifted")
    canonical = json.dumps(documents, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(canonical).hexdigest()
    if digest != _EXPECTED_CANONICAL_SHA256:
        raise ValueError("rehearsal authority contract drifted")
    return digest


def _metadata(value: Mapping[str, object]) -> Mapping[str, object]:
    metadata = value.get("metadata")
    return metadata if isinstance(metadata, Mapping) else {}


def _name(value: Mapping[str, object]) -> object:
    return _metadata(value).get("name")


def _namespace(value: Mapping[str, object]) -> object:
    return _metadata(value).get("namespace")


def _binding_is_exact(value: Mapping[str, object]) -> bool:
    role_ref = value.get("roleRef")
    subjects = value.get("subjects")
    return bool(
        isinstance(role_ref, Mapping)
        and role_ref
        == {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "loom-rollout-rehearsal",
        }
        and subjects
        == [
            {
                "kind": "ServiceAccount",
                "name": "loom-rollout-rehearsal",
                "namespace": "loom-rollout-system",
            }
        ]
    )


def _ingress_role_is_exact(value: Mapping[str, object]) -> bool:
    return bool(
        _name(value) == "loom-rollout-rehearsal-ingress-observer"
        and _namespace(value) == "ingress-nginx"
        and value.get("rules")
        == [
            {
                "apiGroups": [""],
                "resources": ["services"],
                "resourceNames": ["ingress-nginx-controller"],
                "verbs": ["get"],
            }
        ]
    )


def _ingress_binding_is_exact(value: Mapping[str, object]) -> bool:
    return bool(
        _name(value) == "loom-rollout-rehearsal-ingress-observer"
        and _namespace(value) == "ingress-nginx"
        and value.get("roleRef")
        == {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "loom-rollout-rehearsal-ingress-observer",
        }
        and value.get("subjects")
        == [
            {
                "kind": "ServiceAccount",
                "name": "loom-rollout-rehearsal",
                "namespace": "loom-rollout-system",
            }
        ]
    )


def _role_is_bounded(value: Mapping[str, object]) -> bool:
    rules = value.get("rules")
    if not isinstance(rules, list) or rules != _MUTATOR_RULES:
        return False
    has_namespace_mutation = False
    has_policy_mutation = False
    for rule in rules:
        if not isinstance(rule, Mapping):
            return False
        resources = rule.get("resources")
        verbs = rule.get("verbs")
        if (
            not isinstance(resources, list)
            or not isinstance(verbs, list)
            or not resources
            or not verbs
            or "*" in resources
            or "*" in verbs
            or set(verbs) & {"list", "watch"}
            or ("get" in verbs and resources not in (["namespaces"], ["nodes"]))
        ):
            return False
        if "bind" in verbs and (
            resources != ["clusterroles"]
            or rule.get("resourceNames") != ["loom-rollout-rehearsal-observer"]
        ):
            return False
        if resources == ["nodes"] and rule.get("resourceNames") != [
            f"trt-eai-oldlab-{index}" for index in range(1, 6)
        ]:
            return False
        mutation = bool(set(verbs) & _MUTATING_VERBS)
        has_namespace_mutation = has_namespace_mutation or (
            resources == ["namespaces"] and mutation
        )
        has_policy_mutation = has_policy_mutation or (
            mutation
            and any(
                resource in {"validatingadmissionpolicies", "validatingadmissionpolicybindings"}
                for resource in resources
            )
        )
    return has_namespace_mutation and not has_policy_mutation


def _observer_role_is_readonly(value: Mapping[str, object]) -> bool:
    rules = value.get("rules")
    if not isinstance(rules, list) or rules != _OBSERVER_RULES:
        return False
    for rule in rules:
        if not isinstance(rule, Mapping):
            return False
        resources = rule.get("resources")
        verbs = rule.get("verbs")
        if (
            not isinstance(resources, list)
            or not resources
            or not isinstance(verbs, list)
            or not verbs
            or set(verbs) - {"get", "list", "watch"}
            or "*" in resources
        ):
            return False
    return True


def _policy_is_exact(
    value: Mapping[str, object],
    *,
    name: str,
    required_fragment: str,
) -> bool:
    spec = value.get("spec")
    if not isinstance(spec, Mapping) or _name(value) != name or spec.get("failurePolicy") != "Fail":
        return False
    conditions = spec.get("matchConditions")
    validations = spec.get("validations")
    if not isinstance(conditions, list) or not isinstance(validations, list):
        return False
    expressions = tuple(
        item.get("expression") for item in (*conditions, *validations) if isinstance(item, Mapping)
    )
    return bool(
        any(isinstance(item, str) and REHEARSAL_PRINCIPAL in item for item in expressions)
        and any(isinstance(item, str) and required_fragment in item for item in expressions)
        and (
            name != "loom-rollout-rehearsal-resources"
            or any(
                isinstance(item, str)
                and "loom-rollout-rehearsal-observer" in item
                and "rolebindings" in item
                for item in expressions
            )
        )
    )


def _policy_binding_is_exact(value: Mapping[str, object], *, name: str) -> bool:
    spec = value.get("spec")
    return bool(
        _name(value) == name
        and isinstance(spec, Mapping)
        and spec.get("policyName") == name
        and spec.get("validationActions") == ["Deny"]
    )


__all__ = [
    "DEFAULT_REHEARSAL_AUTHORITY_MANIFEST",
    "REHEARSAL_PRINCIPAL",
    "rehearsal_authority_digest",
]
