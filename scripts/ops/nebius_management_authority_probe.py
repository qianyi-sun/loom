"""Non-persisting qualification of actual management service-account authority.

SelfSubjectReview prevents operator credentials/impersonation from masquerading as
the runtime identity. Reviews and dry-runs never create a probe namespace or grant.
Exact recorded policies and type-check status must also be verified by the caller.
"""
from __future__ import annotations

import copy
import json
import ssl
from typing import Any
from uuid import UUID, uuid4

from scripts.ops.nebius_management_stage import ManagementStageError
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_management_authority import ManagementNamespaceAuthority, namespace_binding


class HTTPSManagementAuthorityProbe(ManagementKubernetesTransport):
    error_type = ManagementStageError

    def __init__(self, *, authority: ManagementNamespaceAuthority, service_account_uid: str,
                 api_server: str, ssl_context: ssl.SSLContext, token: str):
        if str(UUID(service_account_uid)) != service_account_uid or UUID(service_account_uid).int == 0 or not token:
            raise ManagementStageError("invalid management authority probe identity")
        self.authority, self.service_account_uid = authority, service_account_uid
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _post(self, path: str, document: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            with self.client.stream("POST", path, json=document) as response:
                if (response.status_code not in {200, 201, 403}
                        or response.headers.get("content-encoding", "identity").lower() != "identity"):
                    raise ValueError()
                payload = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(payload) + len(chunk) > 65536:
                        raise ValueError()
                    payload.extend(chunk)
                value = json.loads(payload)
                if not isinstance(value, dict):
                    raise ValueError()
                return response.status_code, value
        except Exception:
            raise ManagementStageError("management authority probe unavailable") from None

    def _access(self, *, group: str, resource: str, verb: str, name: str | None = None) -> bool:
        attributes = {"group": group, "resource": resource, "verb": verb}
        if name is not None:
            attributes["name"] = name
        code, result = self._post("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", {
            "apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
            "spec": {"resourceAttributes": attributes},
        })
        status = result.get("status", {})
        if code not in {200, 201} or type(status.get("allowed")) is not bool or status.get("evaluationError"):
            raise ManagementStageError("management authority access review unavailable")
        return bool(status["allowed"])

    def _denied(self, path: str, document: dict[str, Any], suffix: str, message: str) -> bool:
        code, value = self._post(path + "?dryRun=All", document)
        if code in {200, 201}:
            return False  # Admission caches may not have propagated yet.
        if (value.get("kind") != "Status" or value.get("reason") != "Forbidden" or value.get("code") != 403
                or self.authority.name + "-" + suffix not in value.get("message", "")
                or message not in value.get("message", "")):
            raise ManagementStageError("management authority denied by unqualified policy")
        return True

    def qualify(self) -> bool:
        """False means admission propagation pending; unexpected authority raises."""
        try:
            authority = self.authority
            code, result = self._post("/apis/authentication.k8s.io/v1/selfsubjectreviews", {
                "apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview",
            })
            user = result.get("status", {}).get("userInfo", {})
            if (code not in {200, 201} or user.get("username") != "system:serviceaccount:" + authority.namespace + ":loom-management-provisioner"
                    or user.get("uid") != self.service_account_uid
                    or set(user.get("groups", [])) != {"system:serviceaccounts", "system:serviceaccounts:" + authority.namespace,
                                                      "system:authenticated"}):
                raise ManagementStageError("management authority authenticated subject differs")
            for group, resource, verb, name in (
                ("", "namespaces", "create", None), ("", "namespaces", "get", None),
                ("rbac.authorization.k8s.io", "rolebindings", "create", None),
                ("rbac.authorization.k8s.io", "clusterroles", "bind", authority.name + "-resources"),
            ):
                if not self._access(group=group, resource=resource, verb=verb, name=name):
                    return False
            for group, resource, verb in (
                ("", "secrets", "get"), ("", "secrets", "list"), ("", "namespaces", "patch"),
                ("", "namespaces", "delete"), ("", "persistentvolumeclaims", "delete"),
                ("rbac.authorization.k8s.io", "clusterrolebindings", "create"),
                ("rbac.authorization.k8s.io", "roles", "escalate"),
            ):
                if self._access(group=group, resource=resource, verb=verb):
                    raise ManagementStageError("management authority has unqualified global privileges")
            probe_id = uuid4()
            document: dict[str, Any] = {"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": "loom-dev-probe-" + probe_id.hex, "labels": {
                    "loom.nebius/namespace-installation": str(authority.installation_id),
                    "loom.nebius/environment-id": str(probe_id), "loom.nebius/incarnation": str(probe_id),
                    "pod-security.kubernetes.io/enforce": "restricted",
                },
            }}
            code, result = self._post("/api/v1/namespaces?dryRun=All", document)
            if code not in {200, 201} or result.get("metadata", {}).get("name") != document["metadata"]["name"]:
                raise ManagementStageError("management authority cannot admit an owned namespace")
            invalid = copy.deepcopy(document)
            invalid["metadata"]["name"] = "foreign-probe-" + probe_id.hex
            if not self._denied("/api/v1/namespaces", invalid, "namespaces", "management namespace boundary"):
                return False
            # The management namespace is deliberately not a child namespace.
            # The bootstrap RoleBinding permission must not let the manager give
            # itself or other subjects resources in that (or any foreign) scope.
            path = "/apis/rbac.authorization.k8s.io/v1/namespaces/" + authority.namespace + "/rolebindings"
            return self._denied(path, namespace_binding(authority, authority.namespace),
                                "bindings", "management namespace binding boundary")
        except ManagementStageError:
            raise
        except Exception:
            raise ManagementStageError("management authority qualification unavailable") from None
