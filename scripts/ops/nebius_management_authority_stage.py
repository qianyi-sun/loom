"""Create-only, fixed management namespace authority for the protected installer.

This stages the existing authority renderer, never caller-supplied RBAC. Runtime
activation additionally requires actual-subject admission qualification.
"""
from __future__ import annotations

import copy
import ssl
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _key, _snapshot
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    _MARKER,
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_management_authority import (
    ManagementNamespaceAuthority,
    render_namespace_authority,
)
from loom.nebius_platform_render import digest

_PATHS = {
    "ValidatingAdmissionPolicy": "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies",
    "ValidatingAdmissionPolicyBinding": "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings",
    "ClusterRole": "/apis/rbac.authorization.k8s.io/v1/clusterroles",
    "ClusterRoleBinding": "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
}


def _documents(authority: ManagementNamespaceAuthority, binding: ManagementBinding) -> dict[str, dict[str, Any]]:
    if (str(authority.installation_id), authority.namespace) != (binding.installation_id, binding.namespace):
        raise ManagementStageError("management authority binding differs")
    return {_key(doc): doc for doc in render_namespace_authority(authority)}


def _defaulted(api: ManagementStageAPI, desired: dict[str, Any]) -> dict[str, Any]:
    observed = _snapshot(api.default_resource(desired))
    comparison = copy.deepcopy(observed)
    if desired["kind"] == "ValidatingAdmissionPolicy":
        constraints = comparison["spec"]["matchConstraints"]
        # These API defaults retain the renderer's all-namespace/all-object
        # matching. Any extra selector, parameter or rule narrows enforcement.
        for field, default in (("matchPolicy", "Equivalent"), ("namespaceSelector", {}), ("objectSelector", {})):
            if constraints.pop(field, default) != default:
                raise ManagementStageError("authority defaulting changed admission scope")
        for rule in constraints["resourceRules"]:
            if rule.pop("scope", "*") != "*":
                raise ManagementStageError("authority defaulting changed admission scope")
    elif desired["kind"] == "ClusterRoleBinding":
        for subject in comparison["subjects"]:
            if subject.pop("apiGroup", "") != "":
                raise ManagementStageError("authority defaulting changed subject")
    if comparison != desired:
        raise ManagementStageError("authority defaulting changed fixed grant or policy")
    return observed


class HTTPSManagementAuthorityAPI(HTTPSManagementStageAPI):
    """Explicit-trust transport, fixed cluster-scoped resource names and contents."""

    def __init__(self, *, authority: ManagementNamespaceAuthority, binding: ManagementBinding,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents = _documents(authority, binding)
        self.binding = binding
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            desired = copy.deepcopy(document)
            expected = self.documents[_key(desired)]
            annotations = desired["metadata"].get("annotations", {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or UUID(operation).int == 0:
                    raise ValueError()
            if not annotations and "annotations" not in expected["metadata"]:
                desired["metadata"].pop("annotations", None)
            if desired != expected:
                raise ValueError()
            return _PATHS[desired["kind"]]
        except Exception:
            raise ManagementStageError("resource outside fixed management authority scope") from None


def stage_management_authority(*, authority: ManagementNamespaceAuthority, binding: ManagementBinding,
                               api: ManagementStageAPI, state_dir: Path) -> dict[str, Any]:
    documents = _documents(authority, binding)
    receipt = _stage_fixed_documents(documents=documents, revision=digest(documents), phase="namespace-authority",
                                    binding=binding, api=api, state_dir=state_dir, default_document=_defaulted)
    return {**receipt, "status": "management_authority_staged"}
