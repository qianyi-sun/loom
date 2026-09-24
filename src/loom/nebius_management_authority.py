"""Explicit management bootstrap authority; rendering does not install privileges.

The protected installer must qualify fail-closed admission before activating the
manager. RBAC grants no global Secret access, namespace mutation or escalation.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

INSTALLATION_LABEL = "loom.nebius/namespace-installation"
_ACCOUNT = "loom-management-provisioner"
_UUID = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


class ManagementNamespaceAuthority(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    installation_id: UUID
    namespace: str = Field(pattern=r"^loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?$", max_length=53)

    @field_validator("installation_id")
    @classmethod
    def _identity(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("management authority requires a non-nil identity")
        return value

    @property
    def name(self) -> str:
        return "loom-management-" + self.installation_id.hex


def _subject(binding: ManagementNamespaceAuthority) -> dict[str, str]:
    return {"kind": "ServiceAccount", "name": _ACCOUNT, "namespace": binding.namespace}


def namespace_binding(binding: ManagementNamespaceAuthority, namespace: str) -> dict[str, Any]:
    return {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {
        "name": binding.name, "namespace": namespace,
    }, "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                   "name": binding.name + "-resources"}, "subjects": [_subject(binding)]}


def _policy(binding: ManagementNamespaceAuthority, suffix: str, *, group: str, resource: str,
            operations: list[str], expression: str, message: str) -> list[dict[str, Any]]:
    name = binding.name + "-" + suffix
    subject = "system:serviceaccount:" + binding.namespace + ":" + _ACCOUNT
    return [{"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
             "metadata": {"name": name}, "spec": {
                 "failurePolicy": "Fail",
                 "matchConstraints": {"resourceRules": [{"apiGroups": [group], "apiVersions": ["v1"],
                                                         "operations": operations, "resources": [resource]}]},
                 "matchConditions": [{"name": "management-subject",
                                      "expression": "request.userInfo.username == " + json.dumps(subject)}],
                 "validations": [{"expression": expression, "message": message, "reason": "Forbidden"}],
             }},
            {"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
             "metadata": {"name": name}, "spec": {"policyName": name, "validationActions": ["Deny"]}}]


def render_namespace_authority(binding: ManagementNamespaceAuthority) -> list[dict[str, Any]]:
    """Admission first, bootstrap last; installer must verify enforcement too."""
    marker = json.dumps(str(binding.installation_id))
    labels = "object.metadata.labels"
    incarnation = labels + "['loom.nebius/incarnation']"
    valid_ids = " && ".join(
        f"{labels}['{key}'].matches({json.dumps(_UUID)}) && "
        f"{labels}['{key}'] != '00000000-0000-0000-0000-000000000000'"
        for key in ("loom.nebius/environment-id", "loom.nebius/incarnation")
    )
    names = (
        "(object.metadata.name in ['loom-dev', 'loom-staging', 'loom-prod'] || "
        "(object.metadata.name.matches('^loom-dev-[a-z0-9]([-a-z0-9]{0,52}[a-z0-9])?$') && "
        "!(object.metadata.name in ['loom-dev-dev', 'loom-dev-staging', 'loom-dev-prod', 'loom-dev-shared'])) || "
        f"object.metadata.name == 'loom-run-' + {incarnation}.replace('-', '') || "
        f"object.metadata.name == 'loom-run-' + {incarnation}.replace('-', '') + '-build')"
    )
    namespace_rule = (
        "has(object.metadata.labels) && "
        + " && ".join(f"{json.dumps(key)} in {labels}" for key in (
            INSTALLATION_LABEL, "pod-security.kubernetes.io/enforce", "loom.nebius/environment-id", "loom.nebius/incarnation",
        )) + f" && {labels}['{INSTALLATION_LABEL}'] == {marker} && "
        f"{labels}['pod-security.kubernetes.io/enforce'] == 'restricted' && {valid_ids} && {names}"
    )
    owned = (
        "namespaceObject != null && has(namespaceObject.metadata.labels) && "
        f"'{INSTALLATION_LABEL}' in namespaceObject.metadata.labels && "
        "'pod-security.kubernetes.io/enforce' in namespaceObject.metadata.labels && "
        f"namespaceObject.metadata.labels['{INSTALLATION_LABEL}'] == {marker} && "
        "namespaceObject.metadata.labels['pod-security.kubernetes.io/enforce'] == 'restricted'"
    )
    # Kubernetes defaults apiGroup to the empty string on ServiceAccount subjects.
    common = (
        "object.roleRef.apiGroup == 'rbac.authorization.k8s.io' && has(object.subjects) && size(object.subjects) == 1 && "
        "object.subjects[0].kind == 'ServiceAccount' && "
        "(!has(object.subjects[0].apiGroup) || object.subjects[0].apiGroup == '')"
    )
    provisioner = (
        f"object.metadata.name == {json.dumps(binding.name)} && object.roleRef.kind == 'ClusterRole' && "
        f"object.roleRef.name == {json.dumps(binding.name + '-resources')} && "
        f"object.subjects[0].namespace == {json.dumps(binding.namespace)} && object.subjects[0].name == '{_ACCOUNT}'"
    )
    observer = (
        "object.metadata.name == 'loom-execution-observer' && object.roleRef.kind == 'Role' && "
        "object.roleRef.name == 'loom-execution-observer' && "
        "object.subjects[0].namespace == request.namespace && object.subjects[0].name == 'loom-execution-actuator'"
    )
    docs = _policy(binding, "namespaces", group="", resource="namespaces", operations=["CREATE"],
                   expression=namespace_rule, message="management namespace boundary")
    docs += _policy(binding, "bindings", group="rbac.authorization.k8s.io", resource="rolebindings",
                    operations=["CREATE", "UPDATE"], expression=f"{owned} && {common} && (({provisioner}) || ({observer}))",
                    message="management namespace binding boundary")
    rules = [
        {"apiGroups": [""], "resources": ["secrets", "services", "serviceaccounts", "configmaps", "resourcequotas",
                                           "persistentvolumeclaims"], "verbs": ["get", "create"]},
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch", "delete"]},
        {"apiGroups": ["apps"], "resources": ["deployments", "statefulsets"], "verbs": ["get", "create", "patch"]},
        {"apiGroups": ["apps"], "resources": ["replicasets"], "verbs": ["get", "list"]},
        {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch", "create", "patch"]},
        {"apiGroups": ["batch"], "resources": ["cronjobs"], "verbs": ["get", "create", "patch"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses"], "verbs": ["get", "create", "patch"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["networkpolicies"], "verbs": ["get", "create"]},
        {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["roles", "rolebindings"], "verbs": ["get", "create"]},
    ]
    docs += [{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
              "metadata": {"name": binding.name + "-resources"}, "rules": rules},
             {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
              "metadata": {"name": binding.name + "-bootstrap"}, "rules": [
                  {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "create"]},
                  {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["rolebindings"], "verbs": ["get", "create"]},
                  {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["clusterroles"],
                   "resourceNames": [binding.name + "-resources"], "verbs": ["bind"]},
              ]},
             {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
              "metadata": {"name": binding.name + "-bootstrap"}, "subjects": [_subject(binding)],
              "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                          "name": binding.name + "-bootstrap"}}]
    for doc in docs:
        doc["metadata"]["labels"] = {INSTALLATION_LABEL: str(binding.installation_id)}
    return docs
