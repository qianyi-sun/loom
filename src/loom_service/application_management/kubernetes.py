"""One-attempt application mutations; uncertain dispatches only reconcile.

Internal trusted lifecycle adapter, not an owner manifest API or a lifecycle
worker. The caller qualifies templates/material and owns installation credentials.
Effect observation is historical evidence, never readiness or access retirement.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any

import httpx

from loom_service.application_management.effects import ApplicationEffect, ApplicationEffectJournal
from loom_service.application_management.leases import ApplicationLease
from loom_service.environment_management.kubernetes_provider import _contains
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError

_RESOURCES = {
    "Namespace": "namespaces", "Secret": "secrets", "Service": "services",
    "ServiceAccount": "serviceaccounts", "Pod": "pods", "ResourceQuota": "resourcequotas",
    "Deployment": "deployments", "RoleBinding": "rolebindings",
    "Ingress": "ingresses", "NetworkPolicy": "networkpolicies",
}


class KubernetesEffectRejectedError(ProviderBlockedError):
    """This exact write was rejected; only a NEW journal key may try anew."""
    def __init__(self, status_code: int):
        super().__init__("application_kubernetes_effect_rejected")
        self.status_code = status_code


class ApplicationKubernetesProvider:
    def __init__(self, registry: ApplicationEffectJournal, http: httpx.AsyncClient):
        url = http.base_url
        if url.scheme != "https" or url.path != "/" or url.query or url.fragment or url.userinfo:
            raise ValueError("application Kubernetes endpoint must be an HTTPS origin")
        self.registry, self.http = registry, http

    async def _namespace(self, lease: ApplicationLease, kind: str, namespace: str | None) -> None:
        plan = await self.registry.frozen_plan(lease)
        name = plan["registration"]["application_namespace"]
        if kind == "Namespace" and namespace is None:
            return
        if namespace != name:
            raise ProviderBlockedError("application_namespace_identity_conflict")
        history = await self.registry.effect_history(lease)
        identities = {item.observed_uid for item in history if item.phase == "observed"
                      and item.intent.kind == "Namespace" and item.intent.name == name
                      and item.intent.action == "create"}
        if len(identities) != 1 or None in identities:
            raise ProviderBlockedError("application_namespace_identity_missing")
        planned = next((doc for docs in plan["files"].values() for doc in docs
                        if doc["kind"] == "Namespace" and doc["metadata"]["name"] == name), None)
        if planned is None:
            raise ProviderBlockedError("application_namespace_identity_missing")
        actual = await self._read("/api/v1/namespaces/" + name)
        expected = {"metadata": {"name": name, "labels": {
            "loom.nebius/application-id": str(lease.application_id),
            "loom.nebius/incarnation": str(lease.incarnation),
        }}}
        if (actual is None or actual.get("metadata", {}).get("uid") not in identities
                or actual.get("metadata", {}).get("deletionTimestamp")
                or not _contains(actual, expected) or not _contains(actual, planned)):
            raise ProviderBlockedError("application_namespace_identity_conflict")

    @staticmethod
    def _document(lease: ApplicationLease, key: str, document: dict[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(document)
        metadata = value["metadata"]
        metadata.setdefault("labels", {}).update({
            "loom.nebius/application-id": str(lease.application_id),
            "loom.nebius/incarnation": str(lease.incarnation),
        })
        metadata.setdefault("annotations", {}).update({
            "loom.nebius/operation-id": str(lease.operation_id),
            "loom.nebius/deployment-generation": str(lease.deployment_generation),
            "loom.nebius/effect-key": key,
        })
        return value

    async def _request(self, method: str, path: str, body: Any = None) -> httpx.Response:
        try:
            response = await self.http.request(method, path, json=body, follow_redirects=False, timeout=30,
                headers={"Content-Type": "application/json-patch+json"} if method == "PATCH" else {})
        except httpx.TransportError:
            raise ProviderWaitingError("application_kubernetes_unconfirmed") from None
        if response.status_code == 404 and method in {"GET", "DELETE"}:
            return response
        if method != "GET" and response.status_code in (409, 422):
            raise KubernetesEffectRejectedError(response.status_code)
        if response.status_code in (409, 422, 429) or response.status_code >= 500:
            raise ProviderWaitingError("application_kubernetes_unconfirmed")
        if response.status_code not in (200, 201, 202):
            raise ProviderBlockedError("application_kubernetes_request_rejected")
        return response

    async def _read(self, path: str) -> dict[str, Any] | None:
        response = await self._request("GET", path)
        if response.status_code == 404:
            return None
        try:
            value = response.json()
            if not isinstance(value, dict) or not isinstance(value.get("metadata"), dict):
                raise ValueError
            if any(not isinstance(value["metadata"].get(field), str)
                   or re.fullmatch(r"[A-Za-z0-9._:-]{1,256}", value["metadata"][field]) is None
                   for field in ("uid", "resourceVersion")):
                raise ValueError
            return value
        except ValueError:
            raise ProviderBlockedError("application_kubernetes_invalid_response") from None

    async def _apply(self, lease: ApplicationLease, key: str, intent: dict[str, Any],
                     body: Any, expected: dict[str, Any] | None) -> ApplicationEffect:
        # Namespace readback precedes preparation, so missing bootstrap identity
        # cannot consume the operation's one outstanding journal slot.
        await self._namespace(lease, intent["kind"], intent["namespace"])
        intent = intent | {"request_sha256": hashlib.sha256(json.dumps(
            body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()}
        effect = await self.registry.prepare_effect(lease, key, intent)
        if effect.phase == "observed":
            return effect
        if effect.phase == "rejected":
            assert effect.rejection_status is not None
            raise KubernetesEffectRejectedError(effect.rejection_status)
        parsed = effect.intent
        prefix = "/api/v1" if parsed.api_version == "v1" else "/apis/" + parsed.api_version
        if parsed.namespace is not None:
            prefix += "/namespaces/" + parsed.namespace
        collection = prefix + "/" + _RESOURCES[parsed.kind]
        path = collection + "/" + parsed.name
        if await self.registry.dispatch_effect(lease, key):
            method = {"create": "POST", "patch": "PATCH", "delete": "DELETE"}[parsed.action]
            try:
                await self._request(method, collection if method == "POST" else path, body)
            except KubernetesEffectRejectedError as exc:
                await self.registry.reject_effect(lease, key, status_code=exc.status_code)
                raise
        # A dispatched effect is NEVER resent, including an earlier timeout/404.
        actual = await self._read(path)
        if parsed.action == "delete":
            if actual is not None and actual.get("metadata", {}).get("uid") == parsed.uid:
                raise ProviderWaitingError("application_kubernetes_retirement_pending")
            uid, resource_version = parsed.uid, None
        else:
            if actual is None:
                raise ProviderWaitingError("application_kubernetes_unconfirmed")
            metadata = actual.get("metadata", {})
            uid, resource_version = metadata.get("uid"), metadata.get("resourceVersion")
            if (not isinstance(uid, str) or not uid or not isinstance(resource_version, str) or not resource_version
                    or metadata.get("deletionTimestamp") or (parsed.uid is not None and uid != parsed.uid)
                    or not _contains(actual, expected)):
                raise ProviderBlockedError("application_kubernetes_resource_identity_conflict")
        await self._namespace(lease, parsed.kind, parsed.namespace)
        assert uid is not None
        try:
            await self.registry.observe_effect(lease, key, uid=uid, resource_version=resource_version)
        except ManagementError as exc:
            if exc.code != "application_effect_observation_conflict":
                raise
            # Another reconciler may have recorded the same successful object
            # before a controller changed only its RV. Preserve that immutable
            # historical observation; never replace it with our newer readback.
            recorded = await self.registry.prepare_effect(lease, key, intent)
            if recorded.phase != "observed" or recorded.observed_uid != uid:
                raise
            return recorded
        return await self.registry.prepare_effect(lease, key, intent)

    async def create(self, lease: ApplicationLease, key: str, document: dict[str, Any]) -> ApplicationEffect:
        expected = self._document(lease, key, document)
        metadata = expected["metadata"]
        return await self._apply(lease, key, {
            "api_version": expected["apiVersion"], "kind": expected["kind"], "action": "create",
            "namespace": metadata.get("namespace"), "name": metadata["name"],
        }, expected, expected)

    async def patch_spec(self, lease: ApplicationLease, key: str, document: dict[str, Any], *,
                         uid: str, resource_version: str) -> ApplicationEffect:
        expected = self._document(lease, key, document)
        metadata = expected["metadata"]
        patches = [
            {"op": "test", "path": "/metadata/uid", "value": uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version},
            {"op": "replace", "path": "/spec", "value": expected["spec"]},
            {"op": "add", "path": "/metadata/labels", "value": metadata["labels"]},
            {"op": "add", "path": "/metadata/annotations", "value": metadata["annotations"]},
        ]
        return await self._apply(lease, key, {
            "api_version": expected["apiVersion"], "kind": expected["kind"], "action": "patch",
            "namespace": metadata.get("namespace"), "name": metadata["name"],
            "uid": uid, "resource_version": resource_version,
        }, patches, expected)

    async def delete(self, lease: ApplicationLease, key: str, *, api_version: str, kind: str,
                     namespace: str, name: str, uid: str, resource_version: str) -> ApplicationEffect:
        return await self._apply(lease, key, {
            "api_version": api_version, "kind": kind, "namespace": namespace, "name": name,
            "action": "delete", "uid": uid, "resource_version": resource_version,
        }, {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background",
            "preconditions": {"uid": uid, "resourceVersion": resource_version}}, None)
