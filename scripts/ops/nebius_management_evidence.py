"""Fixed Kubernetes evidence reads and short-lived management token issuance.

The protected installer must check the recorded stage UIDs/configuration before
calling this adapter. It cannot create workloads or choose a different subject,
Job, container or namespace. Tokens and raw logs never enter returned receipts.
"""
from __future__ import annotations

import json
import re
import ssl
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_install import ManagementInstallError
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI, _canonical_quantities

from loom_service.environment_management.deployment import RenderedManagement
from loom_service.environment_management.kubernetes_provider import _contains


def _matches_backup_template(pod: dict[str, Any], template: dict[str, Any]) -> bool:
    def quantities(spec: dict[str, Any]) -> dict[str, Any]:
        normalized = _canonical_quantities({"kind": "Job", "spec": {"template": {"spec": spec}}})
        result: dict[str, Any] = normalized["spec"]["template"]["spec"]
        return result

    observed, wanted = quantities(pod), quantities(template)
    # DefaultTolerationSeconds mutates Pods, not controller templates. Qualify
    # only these additions; different grace periods or extra tolerations fail.
    wanted_tolerations = wanted.get("tolerations", [])
    tolerations = observed.get("tolerations", [])
    for key in ("not-ready", "unreachable"):
        default = {"key": "node.kubernetes.io/" + key, "operator": "Exists",
                   "effect": "NoExecute", "tolerationSeconds": 300}
        if default not in wanted_tolerations and default in tolerations:
            tolerations.remove(default)
    return _contains(observed, wanted)


class HTTPSManagementEvidenceAPI(HTTPSManagementStageAPI):
    error_type = ManagementInstallError

    def __init__(self, *, binding: ManagementBinding, rendered: RenderedManagement,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        super().__init__(binding=binding, rendered=rendered, phase="10-config-network.yaml",
                         api_server=api_server, ssl_context=ssl_context, token=token)
        self.backup = rendered.files["85-backup-verify.yaml"][0]

    def runtime_token(self, *, service_account_uid: str) -> str:
        """Mint one 10-minute token, never a persisted token Secret or retry."""
        try:
            if str(UUID(service_account_uid)) != service_account_uid or UUID(service_account_uid).int == 0:
                raise ValueError()
            path = "/api/v1/namespaces/" + self.binding.namespace + "/serviceaccounts/loom-management-provisioner"

            def account() -> dict[str, Any]:
                self.verify_identity(self.binding)
                value = self._request("GET", path)
                if (value is None or value.get("kind") != "ServiceAccount" or _uid(value) != service_account_uid
                        or value["metadata"].get("name") != "loom-management-provisioner"
                        or value["metadata"].get("namespace") != self.binding.namespace
                        or value["metadata"].get("deletionTimestamp") or value["metadata"].get("ownerReferences")
                        or value["metadata"].get("labels", {}).get("loom.nebius/management-installation") != self.binding.installation_id
                        or value.get("automountServiceAccountToken") is not False):
                    raise ValueError()
                return _snapshot(value)

            before = account()
            result = self._request("POST", path + "/token", document={
                "apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
                "spec": {"audiences": [], "expirationSeconds": 600},
            })
            if result is None or result.get("kind") != "TokenRequest" or account() != before:
                raise ValueError()
            token = result["status"]["token"]
            expires = datetime.fromisoformat(result["status"]["expirationTimestamp"].replace("Z", "+00:00"))
            remaining = (expires - datetime.now(UTC)).total_seconds()
            if (not isinstance(token, str) or not 0 < len(token) <= 16384
                    or re.fullmatch(r"[A-Za-z0-9._~+/-]+={0,2}", token) is None or not 30 < remaining <= 660):
                raise ValueError()
            return token
        except Exception:
            raise ManagementInstallError("management runtime token qualification unavailable") from None

    def backup_report(self, *, job_uid: str) -> dict[str, Any]:
        """Read only the completed recorded Job's unique, unrestarted uploader."""
        try:
            if str(UUID(job_uid)) != job_uid or UUID(job_uid).int == 0:
                raise ValueError()
            namespace = self.binding.namespace
            job_name = self.backup["metadata"]["name"]
            job_path = "/apis/batch/v1/namespaces/" + namespace + "/jobs/" + job_name
            pods_path = "/api/v1/namespaces/" + namespace + "/pods"

            def completed_job() -> dict[str, Any]:
                self.verify_identity(self.binding)
                value = self._request("GET", job_path)
                if (value is None or value.get("kind") != "Job" or _uid(value) != job_uid
                        or value["metadata"].get("name") != job_name or value["metadata"].get("namespace") != namespace
                        or value["metadata"].get("deletionTimestamp") or value["metadata"].get("ownerReferences")
                        or not _contains(_canonical_quantities(value), _canonical_quantities(self.backup))):
                    raise ValueError()
                status = value.get("status", {})
                conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
                if conditions.get("Complete") != "True" or conditions.get("Failed") == "True" or status.get("succeeded") != 1:
                    raise ValueError()
                return value

            job = completed_job()
            listing = self._request("GET", pods_path + "?" + urlencode({
                "labelSelector": "batch.kubernetes.io/controller-uid=" + job_uid, "limit": 2,
            }))
            if (listing is None or listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                    or listing.get("metadata", {}).get("continue")
                    or len(listing.get("items", [])) != 1):
                raise ValueError()
            # Typed Kubernetes lists omit item TypeMeta; individual GETs include
            # it. Inherit only absent fields from this exact verified collection,
            # retaining explicit conflicting types for rejection below.
            pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
            meta = pod["metadata"]
            _uid(pod)
            if (pod.get("apiVersion") != "v1" or pod.get("kind") != "Pod"
                    or meta.get("namespace") != namespace or meta.get("deletionTimestamp")
                    or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", meta["name"])
                    or meta.get("labels", {}).get("batch.kubernetes.io/controller-uid") != job_uid):
                raise ValueError()
            owners = meta.get("ownerReferences", [])
            if (len(owners) != 1 or any(owners[0].get(key) != value for key, value in {
                    "apiVersion": "batch/v1", "kind": "Job", "name": job_name, "uid": job_uid, "controller": True,
            }.items())):
                raise ValueError()
            # Match executable contents from the actual Job; quantity spelling may
            # be canonicalized by the API. Extra containers cannot supply evidence.
            expected = job["spec"]["template"]["spec"]
            if not _matches_backup_template(pod["spec"], expected) or pod.get("status", {}).get("phase") != "Succeeded":
                raise ValueError()
            for field, status_field in (("containers", "containerStatuses"), ("initContainers", "initContainerStatuses")):
                names = {row["name"] for row in expected.get(field, [])}
                statuses = pod["status"].get(status_field, [])
                if len(statuses) != len(names) or {row["name"] for row in statuses} != names:
                    raise ValueError()
                if any(row.get("restartCount") != 0 or row.get("state", {}).get("terminated", {}).get("exitCode") != 0
                       for row in statuses):
                    raise ValueError()
            uploader = expected["containers"][0]["name"]
            path = pods_path + "/" + meta["name"]
            query = urlencode({"container": uploader, "tailLines": 20, "limitBytes": 16384, "timestamps": "false"})
            with self.client.stream("GET", path + "/log?" + query) as response:
                if response.status_code != 200 or response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ValueError()
                payload = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(payload) + len(chunk) > 16384:
                        raise ValueError()
                    payload.extend(chunk)
            # Successful uploader emits exactly one JSON record, not free text.
            report = json.loads(payload)
            if not isinstance(report, dict) or set(report) != {"backup_key", "sha256", "bytes"}:
                raise ValueError()
            after = self._request("GET", path)
            # A Pod has its validated Job owner; the stage snapshot deliberately
            # forbids owners for directly installed objects and cannot be used.
            if after != pod or _snapshot(completed_job()) != _snapshot(job):
                raise ValueError()
            return report
        except Exception:
            raise ManagementInstallError("management backup execution evidence unavailable") from None
