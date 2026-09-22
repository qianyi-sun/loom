"""Retain immutable names/data while stopping only journal-proven controllers.

Deleting a controller name is unsafe: an earlier timed-out create request could
recreate it after cleanup. A stopped object keeps that name occupied. This module
does not release admission/storage or declare Pods absent; the lifecycle journal
must establish those separately after every controller has been fenced.
"""

from __future__ import annotations

import copy
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProvisioningContext
from loom_service.environment_management.steps import ProvisioningStep


class RetainedKubernetesCleanup:
    def __init__(self, kubernetes: KubernetesEnvironmentProvider):
        self.kubernetes = kubernetes

    async def inventory(self, context: ProvisioningContext, namespace: str, resource: str) -> list[dict[str, Any]]:
        if namespace not in context.namespaces or resource not in {"pods", "jobs", "replicasets"}:
            raise ProviderBlockedError("retained_inventory_scope_invalid")
        prefix = {"pods": "/api/v1", "jobs": "/apis/batch/v1", "replicasets": "/apis/apps/v1"}[resource]
        path = prefix + "/namespaces/" + namespace + "/" + resource
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        token = ""
        for _ in range(100):
            response = await self.kubernetes._request("GET", path + "?" + urlencode({"limit": 200, "continue": token}))
            if response is None or not isinstance(response.get("items"), list) or len(response["items"]) > 200:
                raise ProviderBlockedError("retained_inventory_invalid")
            if not all(isinstance(item, dict) for item in response["items"]):
                raise ProviderBlockedError("retained_inventory_invalid")
            items.extend(response["items"])
            token = response.get("metadata", {}).get("continue", "")
            if not token:
                return items
            if not isinstance(token, str) or token in seen or len(token) > 16384:
                break
            seen.add(token)
        raise ProviderBlockedError("retained_inventory_incomplete")

    async def dependent_job(self, context: ProvisioningContext, doc: dict[str, Any], *, cleanup_id: UUID) -> str:
        """Stop a CronJob child whose exact UID/spec were committed by discovery."""
        if doc.get("kind") != "Job":
            raise ProviderBlockedError("retained_dependent_kind_invalid")
        _, path = self.kubernetes._path(context, doc)
        uid = doc["metadata"]["uid"]
        if not isinstance(uid, str) or not uid:
            raise ProviderBlockedError("retained_dependent_identity_invalid")
        current = await self.kubernetes._request("GET", path)
        if current is None:
            return uid  # The final Pod inventory still owns its remaining children.
        stopped = self._stopped(doc, context, cleanup_id)
        marker = current.get("metadata", {}).get("annotations", {}).get("loom.nebius/retained-by")
        if marker is not None:
            if marker != str(cleanup_id):
                raise ProviderBlockedError("retained_operation_conflict")
            return self.kubernetes._identity(current, stopped, uid)
        self.kubernetes._identity(current, doc, uid)
        version = current["metadata"].get("resourceVersion")
        if not isinstance(version, str) or not version:
            raise ProviderBlockedError("kubernetes_resource_version_missing")
        actual_stopped = self._stopped(current, context, cleanup_id)
        current = await self.kubernetes._request("PATCH", path, body=[
            {"op": "test", "path": "/metadata/uid", "value": uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": version},
            {"op": "replace", "path": "/spec", "value": actual_stopped["spec"]},
            {"op": "add", "path": "/metadata/annotations", "value": actual_stopped["metadata"]["annotations"]},
        ])
        assert current is not None
        return self.kubernetes._identity(current, stopped, uid)

    async def terminal_pod(self, context: ProvisioningContext, doc: dict[str, Any]) -> str:
        from loom_service.environment_management.provider import ProviderWaitingError

        metadata = doc.get("metadata", {})
        namespace, name, uid = metadata.get("namespace"), metadata.get("name"), metadata.get("uid")
        if (doc.get("kind") != "Pod" or namespace not in context.namespaces
                or not isinstance(name, str) or not name or not isinstance(uid, str) or not uid):
            raise ProviderBlockedError("retained_pod_scope_invalid")
        # Validate Kubernetes name grammar via the normal namespaced path builder.
        self.kubernetes._path(context, {"apiVersion": "batch/v1", "kind": "Job", "metadata": metadata})
        path = "/api/v1/namespaces/" + namespace + "/pods/" + name
        current = await self.kubernetes._request("GET", path)
        if current is None:
            return uid
        if current.get("metadata", {}).get("deletionTimestamp"):
            if current["metadata"].get("uid") != uid:
                raise ProviderBlockedError("kubernetes_resource_identity_conflict")
            raise ProviderWaitingError("retained_pod_terminating")
        self.kubernetes._identity(current, doc, uid)
        if current.get("status", {}).get("phase") not in {"Succeeded", "Failed"}:
            raise ProviderBlockedError("retained_pod_not_terminal")
        await self.kubernetes._request("DELETE", path, body={
            "apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid},
        })
        # DELETE acceptance is not absence. The next reconciliation reads it.
        raise ProviderWaitingError("retained_pod_terminating")

    @staticmethod
    def _stopped(doc: dict[str, Any], context: ProvisioningContext, cleanup_id: UUID) -> dict[str, Any]:
        result = copy.deepcopy(doc)
        kind, spec = result["kind"], result["spec"]
        if kind in {"Deployment", "StatefulSet"}:
            if kind == "StatefulSet" and spec.get("persistentVolumeClaimRetentionPolicy") != {
                "whenDeleted": "Retain", "whenScaled": "Retain",
            }:
                raise ProviderBlockedError("retained_database_policy_invalid")
            spec["replicas"] = 0
        elif kind in {"Job", "CronJob"}:
            spec["suspend"] = True
        elif kind == "Ingress":
            # Keep valid routing objects, but send every path to a deliberately
            # nonexistent Service. No wildcard/foreign-host route is introduced.
            disabled = "loom-retained-" + context.registration["incarnation"].replace("-", "")

            def close_backend(backend: dict[str, Any]) -> None:
                if "service" not in backend or "resource" in backend:
                    raise ProviderBlockedError("retained_ingress_backend_invalid")
                backend["service"]["name"] = disabled

            if "defaultBackend" in spec:
                close_backend(spec["defaultBackend"])
            for rule in spec.get("rules", []):
                for path in rule.get("http", {}).get("paths", []):
                    close_backend(path["backend"])
        else:
            raise ProviderBlockedError("retained_resource_not_supported")
        result["metadata"].setdefault("annotations", {})["loom.nebius/retained-by"] = str(cleanup_id)
        return result

    async def stop(self, context: ProvisioningContext, step: ProvisioningStep, *, cleanup_id: UUID) -> str:
        """Context/step are the frozen CREATE intent, not the later cleanup lease."""
        kube = self.kubernetes
        original = kube._expected(context, step)
        stopped = self._stopped(original, context, cleanup_id)
        collection, path = kube._path(context, original)
        namespace = original["metadata"].get("namespace")
        namespace_uid = context.identities.get("k8s:Namespace:-:" + str(namespace))
        if namespace_uid is None:
            raise ProviderBlockedError("kubernetes_namespace_identity_missing")
        observed_namespace = await kube._request("GET", "/api/v1/namespaces/" + namespace)
        if observed_namespace is None:
            raise ProviderBlockedError("kubernetes_recorded_namespace_missing")
        kube._identity(observed_namespace, {"metadata": {"labels": {
            "loom.nebius/environment-id": str(context.lease.environment_id),
            "loom.nebius/incarnation": context.registration["incarnation"],
        }}}, namespace_uid)
        actual = await kube._request("GET", path)
        recorded = context.identities.get(step.key)
        if actual is None:
            if recorded is not None:
                raise ProviderBlockedError("kubernetes_recorded_resource_missing")
            actual = await kube._request("POST", collection, body=stopped)
        else:
            marker = actual.get("metadata", {}).get("annotations", {}).get("loom.nebius/retained-by")
            if marker is not None:
                if marker != str(cleanup_id):
                    raise ProviderBlockedError("retained_operation_conflict")
            else:
                uid = kube._identity(actual, original, recorded)
                version = actual["metadata"].get("resourceVersion")
                if not isinstance(version, str) or not version:
                    raise ProviderBlockedError("kubernetes_resource_version_missing")
                actual_stopped = self._stopped(actual, context, cleanup_id)
                actual = await kube._request("PATCH", path, body=[
                    {"op": "test", "path": "/metadata/uid", "value": uid},
                    {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                    {"op": "replace", "path": "/spec", "value": actual_stopped["spec"]},
                    {"op": "add", "path": "/metadata/annotations/loom.nebius~1retained-by", "value": str(cleanup_id)},
                ])
        assert actual is not None
        return kube._identity(actual, stopped, recorded)
