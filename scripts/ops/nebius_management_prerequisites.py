"""Live read-only prerequisites for the protected management installation."""
from __future__ import annotations

import asyncio
import json
import ssl
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import UUID, uuid5

import httpx
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_dns_publication import qualify_public_routes
from scripts.ops.nebius_ingress_operation import LiveIngressAPI, qualify_dns_target
from scripts.ops.nebius_management_capacity import _count, qualify_platform_capacity
from scripts.ops.nebius_management_cloud_scope import (
    ManagementCloudScope,
    _pages,
    qualify_cloud_material,
)
from scripts.ops.nebius_management_install import ManagementInstallRequest, render_installation
from scripts.ops.nebius_management_live import backup_client
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_environment_contract import new_environment_registration
from loom.nebius_environment_render import PlatformEnvelope, render_environment
from loom_execution_capacity_collector.kubernetes import _quantity
from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.deployment import RenderedManagement


class ManagementPrerequisiteError(RuntimeError):
    """Private credential, artifact and inventory contents must not escape."""


class ManagementPrerequisiteSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    candidate_id: UUID
    cloud: ManagementCloudScope
    storage_class_uid: UUID
    storage_parameters: dict[str, str]
    storage_quota_name: str = Field(min_length=1, max_length=255)
    storage_quota_unit: str = Field(min_length=1, max_length=40)


class HTTPSManagementPrerequisites(ManagementKubernetesTransport):
    """Private installer reads only; no generic apply or operator command input."""

    error_type = ManagementPrerequisiteError

    def __init__(self, *, settings: ManagementPrerequisiteSettings, ingress: LiveIngressAPI,
                 certificate_config: dict[str, Any], ingress_state: Path, operator_cloud_credentials: Path,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.settings, self.ingress, self.certificate_config = settings, ingress, certificate_config
        self.ingress_state, self.operator_cloud_credentials = ingress_state, operator_cloud_credentials
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def foundation(self, request: ManagementInstallRequest) -> None:
        try:
            foundation = request.deployment.installation.foundation
            config = foundation.platform_config
            scope, binding = self.settings.cloud, self.ingress.binding
            if (scope.provisioning_project_id != foundation.provisioning_project_id
                    or scope.tenant_id != config["quota_parent_id"] or scope.region != config["region"]
                    or scope.provisioning_project_id == config["project_id"]
                    or binding.kube_system_uid != request.binding.kube_system_uid
                    or binding.namespace != foundation.ingress_namespace
                    or binding.child_domain != foundation.public_dns_zone
                    or binding.management_host != request.deployment.public_host
                    or self.api_server.rstrip("/") != config["kubernetes_api_server"].rstrip("/")):
                raise ValueError()
            current = self.ingress.foundation()
            live = current.platform_config
            # Cutover owns this flag; ordinary protected app publication can
            # advance independently of the initial ingress installation journal.
            config["shared_ingress_enabled"] = live["shared_ingress_enabled"] = False
            if (config != live or current.ingress_class_name != foundation.ingress_class_name
                    or current.ingress_controller_label != foundation.ingress_controller_label
                    or current.ingress_namespace != foundation.ingress_namespace
                    or current.public_dns_zone != foundation.public_dns_zone):
                raise ValueError()
        except Exception:
            raise ManagementPrerequisiteError("management foundation unqualified") from None

    def inventory(self, api: str, resource: str, kind: str) -> list[dict[str, Any]]:
        """Only fixed prerequisite collections, with complete stable pagination."""
        collections = {("v1", "nodes", "Node"), ("v1", "pods", "Pod"),
                       ("v1", "persistentvolumeclaims", "PersistentVolumeClaim"),
                       ("networking.k8s.io/v1", "ingresses", "Ingress"),
                       ("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler"),
                       *(("apps/v1", resource, kind) for resource, kind in (
                           ("deployments", "Deployment"), ("statefulsets", "StatefulSet"),
                           ("replicasets", "ReplicaSet"), ("daemonsets", "DaemonSet"))),
                       ("batch/v1", "jobs", "Job"), ("batch/v1", "cronjobs", "CronJob")}
        try:
            if (api, resource, kind) not in collections:
                raise ValueError()
            prefix = "/api/v1/" if api == "v1" else "/apis/" + api + "/"
            token, version = "", None
            seen: set[str] = set()
            result: list[dict[str, Any]] = []
            for _ in range(30):
                query = {"limit": "100", **({"continue": token} if token else {})}
                if kind == "Pod":
                    query["fieldSelector"] = "status.phase!=Succeeded,status.phase!=Failed"
                page = self._request("GET", prefix + resource + "?" + urlencode(query))
                if (page is None or page.get("apiVersion") != api or page.get("kind") != kind + "List"
                        or not isinstance(page.get("items"), list) or len(page["items"]) > 100
                        or not page.get("metadata", {}).get("resourceVersion")
                        or any(row.get("apiVersion", api) != api or row.get("kind", kind) != kind for row in page["items"])):
                    raise ValueError()
                current = page["metadata"]["resourceVersion"]
                if version is not None and current != version:
                    raise ValueError()
                version = current
                # Typed Kubernetes lists omit TypeMeta on each item. Inherit
                # only absent fields from the fixed, verified collection type;
                # an explicitly conflicting type still fails above.
                result.extend({"apiVersion": api, "kind": kind, **row} for row in page["items"])
                token = page["metadata"].get("continue", "")
                if not token:
                    return result
                if not isinstance(token, str) or token in seen:
                    raise ValueError()
                seen.add(token)
            raise ValueError()
        except Exception:
            raise ManagementPrerequisiteError("management resource inventory unqualified") from None

    def public_route(self, request: ManagementInstallRequest) -> None:
        try:
            self.foundation(request)
            host = request.deployment.public_host
            for row in self.inventory("networking.k8s.io/v1", "ingresses", "Ingress"):
                claimed = [rule.get("host", "") for rule in row.get("spec", {}).get("rules", [])]
                if any(name == host or (name.startswith("*.") and host.partition(".")[2] == name[2:]) for name in claimed):
                    if (row["metadata"].get("namespace"), row["metadata"].get("name")) != (request.binding.namespace, "loom-management"):
                        raise ValueError()
            target = qualify_dns_target(api=self.ingress, certificate_config=self.certificate_config,
                                        state_dir=self.ingress_state)
            if target["management_host"] != host or target["child_domain"] != request.deployment.installation.foundation.public_dns_zone:
                raise ValueError()
            qualify_public_routes(target)
            self.foundation(request)
        except Exception:
            raise ManagementPrerequisiteError("management public route unqualified") from None

    def preflight(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> None:
        try:
            if rendered != render_installation(request):
                raise ValueError()
            self.foundation(request)
            missing_storage = self.platform_capacity(request, rendered)
            asyncio.run(self.provider_and_publication(request, missing_storage))
            with backup_client(request) as objects:
                response = objects.head_bucket(Bucket=request.deployment.backup_bucket)
                if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                    raise ValueError()
            self.public_route(request)
        except Exception:
            raise ManagementPrerequisiteError("management installation prerequisites unqualified") from None

    def platform_capacity(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> int:
        """Check live scheduler fit and return additional persistent-disk demand."""
        installation = request.deployment.installation
        foundation, budget = installation.foundation, installation.platform_budget
        config = foundation.platform_config
        identity = uuid5(request.deployment.installation_id, "render-only-child-capacity")
        registration = new_environment_registration(foundation, environment_id=identity, incarnation=identity,
            owner_user_id=identity, owner_team_id=identity, slug="preflight")
        child = render_environment(registration, request.candidate, foundation, profile=request.profile,
                                   keyring=installation.keyring, repo_root=Path(__file__).resolve().parents[2])
        # This is the renderer's footprint, not a fixed owner roster. Reserve
        # Pod slots for every child the configured aggregate allowance can fit.
        bounds = vars(child.platform_envelope)
        if any(value <= 0 for value in bounds.values()):
            raise ValueError()
        children = min(getattr(budget, key) // value for key, value in bounds.items())
        child_slots = sum(_count(doc) for docs in child.files.values() for doc in docs
                          if doc["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"})
        nodes, pods = self.inventory("v1", "nodes", "Node"), self.inventory("v1", "pods", "Pod")
        controllers = []
        for api, resource, kind in (("apps/v1", "deployments", "Deployment"), ("apps/v1", "statefulsets", "StatefulSet"),
            ("apps/v1", "replicasets", "ReplicaSet"), ("apps/v1", "daemonsets", "DaemonSet"),
            ("batch/v1", "jobs", "Job"), ("batch/v1", "cronjobs", "CronJob")):
            controllers.extend(self.inventory(api, resource, kind))
        # Account HPA maxima rather than promising capacity from a temporary
        # replica low-water mark. This is an in-memory sizing view, never a patch.
        by_key = {(row["kind"], row["metadata"]["namespace"], row["metadata"]["name"]): row for row in controllers}
        for hpa in self.inventory("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler"):
            target = hpa["spec"]["scaleTargetRef"]
            row = by_key[(target["kind"], hpa["metadata"]["namespace"], target["name"])]
            maximum = hpa["spec"]["maxReplicas"]
            if target["kind"] not in {"Deployment", "StatefulSet", "ReplicaSet"} or type(maximum) is not int or maximum <= 0:
                raise ValueError()
            row["spec"]["replicas"] = max(row["spec"].get("replicas", 1), maximum)
        planned = [row for docs in rendered.files.values() for row in docs
                   if row["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"}]
        qualify_platform_capacity(nodes=nodes, pods=pods, controllers=controllers, planned=planned,
                                  reserve=PlatformEnvelope(**budget.model_dump()), reserve_pods=children * child_slots)
        storage_class = self._request("GET", "/apis/storage.k8s.io/v1/storageclasses/" + config["storage_class"])
        if (storage_class is None or storage_class.get("kind") != "StorageClass"
                or storage_class["metadata"].get("uid") != str(self.settings.storage_class_uid)
                or storage_class["metadata"].get("name") != config["storage_class"]
                or storage_class["metadata"].get("deletionTimestamp")
                or storage_class.get("provisioner") != "compute.csi.nebius.com"
                or storage_class.get("parameters", {}) != self.settings.storage_parameters
                or storage_class.get("volumeBindingMode") not in {"Immediate", "WaitForFirstConsumer"}):
            raise ValueError()
        claims = self.inventory("v1", "persistentvolumeclaims", "PersistentVolumeClaim")
        pending = 0
        own = False
        for claim in claims:
            if (claim["metadata"]["namespace"], claim["metadata"]["name"]) == (request.binding.namespace, "data-loom-postgres-0"):
                own = True  # Identity/adoption is separately guarded before DB creation.
            # Include pending foreign claims. They may consume the observed
            # provider headroom even when no backing disk is counted yet.
            if claim.get("status", {}).get("phase") != "Bound":
                pending += _quantity(claim["spec"]["resources"]["requests"]["storage"], kind="storage")
        return budget.storage_mib + pending + (0 if own else request.deployment.postgres_storage_gi * 1024)

    async def provider_and_publication(self, request: ManagementInstallRequest, missing_storage_mib: int) -> None:
        from nebius.api.nebius.quotas import v1
        from nebius.sdk import SDK

        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_management_publication(request=request, candidate_id=self.settings.candidate_id, http=http)
        # The observing operator SDK never becomes runtime material. Its private
        # path is installation-owned and cannot come from a personal deployment.
        before = private_state._private_read(self.operator_cloud_credentials, limit=1024 * 1024)
        sdk = SDK(credentials_file_name=str(self.operator_cloud_credentials), user_agent_prefix="loom-management-installer/1.0")
        try:
            async with asyncio.timeout(180):
                await qualify_cloud_material(sdk=sdk, scope=self.settings.cloud, material=request.material,
                                             bucket_name=request.deployment.backup_bucket)
                quotas = await _pages(v1.QuotaAllowanceServiceClient(sdk).list, v1.ListQuotaAllowancesRequest,
                                      parent_id=self.settings.cloud.tenant_id)
                rows = [row for row in quotas if row["metadata"]["name"] == self.settings.storage_quota_name
                        and row.get("spec", {}).get("region") == self.settings.cloud.region]
                if len(rows) != 1 or self.settings.storage_quota_unit not in {"byte", "bytes", "B"}:
                    raise ValueError()
                row = rows[0]
                if (row["metadata"]["parent_id"] != self.settings.cloud.tenant_id
                        or row["status"]["state"] != "STATE_ACTIVE"
                        or row["status"]["usage_state"] not in {"USAGE_STATE_USED", "USAGE_STATE_NOT_USED"}
                        or row["status"]["service"] != "compute"
                        or row["status"]["unit"] != self.settings.storage_quota_unit):
                    raise ValueError()
                limit, used = (int(row[part].get(field, 0)) for part, field in (("spec", "limit"), ("status", "usage")))
                if min(limit, used) < 0 or limit - used < missing_storage_mib * 1024**2:
                    raise ValueError()
                if private_state._private_read(self.operator_cloud_credentials, limit=1024 * 1024) != before:
                    raise ValueError()
        finally:
            await sdk.close()


async def qualify_management_publication(*, request: ManagementInstallRequest, candidate_id: UUID,
                                         http: httpx.AsyncClient) -> None:
    try:
        installation = request.deployment.installation
        catalog = GitHubCandidateCatalog(http, token=request.material["loom-management-publications"]["token"],
            publications=list(installation.publications), registry_prefix=installation.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
        selected = await catalog.resolve(candidate_id)
        if selected.candidate != request.candidate or selected.profile != request.profile:
            raise ValueError()
    except Exception:
        raise ManagementPrerequisiteError("management publication unqualified") from None
