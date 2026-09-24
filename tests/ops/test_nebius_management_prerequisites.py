"""Connected read-only installation checks reuse publication authority, not labels."""
from __future__ import annotations

import copy
import io
import json
import ssl
import zipfile
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_candidate_catalog import github_transport
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def published_request(installation, publication):
    from loom_service.environment_management.deployment import ManagementDeployment

    request, _ = installation
    reference, _, payload, keyring, candidate = publication
    deployment = request.deployment.model_dump(mode="json")
    deployment["installation"].update(publications=[{**reference, "candidate_id": str(reference["candidate_id"])}],
                                       registry_prefix=candidate["registry_prefix"], keyring=json.loads(keyring))
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        profile = json.loads(archive.read("runtime-profile.json"))
    material = copy.deepcopy(request.material)
    material["loom-management-publications"]["token"] = "test-github-secret"
    return replace(request, deployment=ManagementDeployment.model_validate(deployment), candidate=candidate,
                   profile=profile, material=material)


async def publication_check(request, publication):
    from scripts.ops.nebius_management_prerequisites import qualify_management_publication

    reference, responses, payload, _, _ = publication
    async with httpx.AsyncClient(transport=github_transport(responses, payload), trust_env=False) as client:
        await qualify_management_publication(request=request, candidate_id=reference["candidate_id"], http=client)


async def test_selected_management_bytes_require_exact_approved_publication(installation, publication):
    request = published_request(installation, publication)
    await publication_check(request, publication)


@pytest.mark.parametrize("mutation", ["unselected", "image", "profile", "failed_check", "expired"])
async def test_supplied_candidate_cannot_substitute_for_github_publication(installation, publication, mutation):
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    request = published_request(installation, publication)
    if mutation == "unselected":
        request = replace(request, deployment=request.deployment.model_copy(update={
            "installation": request.deployment.installation.model_copy(update={"publications": ()})}))
    elif mutation == "image":
        request.candidate["images"]["service"]["image_ref"] = "cr.eu-north1.nebius.cloud/other/service@sha256:" + "a" * 64
    elif mutation == "profile":
        request.profile["candidate_sha"] = "b" * 40
    elif mutation == "failed_check":
        publication[1]["commits/" + "b" * 40 + "/check-runs"]["check_runs"][0]["conclusion"] = "failure"
    else:
        publication[1]["actions/artifacts/123"]["expired"] = True
    with pytest.raises(ManagementPrerequisiteError) as error:
        await publication_check(request, publication)
    assert "test-github-secret" not in str(error.value)


@pytest.fixture
def checks(installation, cloud, tmp_path):
    from scripts.ops.nebius_management_prerequisites import (
        HTTPSManagementPrerequisites,
        ManagementPrerequisiteSettings,
    )

    request, _ = installation
    foundation = request.deployment.installation.foundation
    config = foundation.platform_config
    cloud.scope.update(provisioning_project_id=foundation.provisioning_project_id,
                       tenant_id=config["quota_parent_id"], region=config["region"])
    ingress = SimpleNamespace(binding=SimpleNamespace(kube_system_uid=request.binding.kube_system_uid,
        namespace=foundation.ingress_namespace, child_domain=foundation.public_dns_zone,
        management_host=request.deployment.public_host), foundation=lambda: foundation)
    settings = ManagementPrerequisiteSettings(candidate_id=uuid4(), cloud=cloud.scope,
        storage_class_uid=uuid4(), storage_parameters={}, storage_quota_name="compute-disks-size-nonreplicated-ssd",
        storage_quota_unit="bytes")
    client = HTTPSManagementPrerequisites(settings=settings, ingress=ingress, certificate_config={},
        ingress_state=tmp_path / "ingress", operator_cloud_credentials=tmp_path / "operator.json",
        api_server=config["kubernetes_api_server"], ssl_context=ssl.create_default_context(), token="operator-test-token")
    client.client.close()
    yield client, request
    client.client.close()


def respond_inventory(checks, pages):
    client = checks[0]
    calls = []

    def respond(request):
        assert request.method == "GET"
        calls.append(str(request.url))
        item = pages.pop(0)
        return httpx.Response(200, json=item)

    client.client = httpx.Client(base_url=client.api_server, transport=httpx.MockTransport(respond))
    return calls


def test_live_inventory_requires_complete_stable_pagination(checks):
    calls = respond_inventory(checks, [
        {"apiVersion": "v1", "kind": "NodeList", "metadata": {"resourceVersion": "7", "continue": "next"},
         "items": [{"apiVersion": "v1", "kind": "Node", "metadata": {"uid": str(uuid4())}}]},
        {"apiVersion": "v1", "kind": "NodeList", "metadata": {"resourceVersion": "7"},
         "items": [{"apiVersion": "v1", "kind": "Node", "metadata": {"uid": str(uuid4())}}]},
    ])
    assert len(checks[0].inventory("v1", "nodes", "Node")) == 2
    assert "continue=next" in calls[1]


def test_typed_api_collection_supplies_omitted_item_type_metadata(checks):
    uid = str(uuid4())
    respond_inventory(checks, [{"apiVersion": "v1", "kind": "NodeList", "metadata": {"resourceVersion": "9"},
        "items": [{"metadata": {"name": "computeinstance-test", "uid": uid}}]}])
    assert checks[0].inventory("v1", "nodes", "Node") == [{"apiVersion": "v1", "kind": "Node",
        "metadata": {"name": "computeinstance-test", "uid": uid}}]


@pytest.mark.parametrize("mutation", ["version", "repeat", "kind", "missing"])
def test_partial_or_changed_inventory_is_not_empty_capacity(checks, mutation):
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    first = {"apiVersion": "v1", "kind": "NodeList", "metadata": {"resourceVersion": "7", "continue": "next"}, "items": []}
    second = {"apiVersion": "v1", "kind": "NodeList", "metadata": {"resourceVersion": "7"}, "items": []}
    if mutation == "version":
        second["metadata"]["resourceVersion"] = "8"
    elif mutation == "repeat":
        second["metadata"]["continue"] = "next"
    elif mutation == "kind":
        second["kind"] = "PodList"
    else:
        second.pop("items")
    respond_inventory(checks, [first, second])
    with pytest.raises(ManagementPrerequisiteError):
        checks[0].inventory("v1", "nodes", "Node")


def test_management_public_route_rejects_competing_host_before_https_credentials(checks, monkeypatch):
    from scripts.ops import nebius_management_prerequisites as prerequisites

    client, request = checks
    respond_inventory(checks, [{"apiVersion": "networking.k8s.io/v1", "kind": "IngressList",
        "metadata": {"resourceVersion": "1"}, "items": [{"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
        "metadata": {"namespace": "foreign", "name": "interloper"},
        "spec": {"rules": [{"host": request.deployment.public_host}]}}]}])
    calls = []
    monkeypatch.setattr(prerequisites, "qualify_dns_target", lambda **kwargs: calls.append("public"))
    with pytest.raises(prerequisites.ManagementPrerequisiteError):
        client.public_route(request)
    assert not calls


def test_foundation_or_cloud_scope_drift_blocks_before_external_work(checks):
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    client, request = checks
    client.settings = client.settings.model_copy(update={"cloud": client.settings.cloud.model_copy(update={"tenant_id": "tenant-other"})})
    with pytest.raises(ManagementPrerequisiteError):
        client.foundation(request)


@pytest.fixture
def connected_checks(checks, installation, publication, cloud, monkeypatch):
    import boto3
    import nebius.sdk
    from botocore.hooks import HierarchicalEmitter
    from nebius.api.nebius.iam import v1, v2
    from nebius.api.nebius.quotas import v1 as quotas
    from nebius.api.nebius.storage import v1 as storage
    from scripts.ops import nebius_management_prerequisites as module

    client, _ = checks
    request = published_request(installation, publication)
    selected = client.settings.cloud.provisioning_project_id
    updated = {identity.replace("project-children", selected): (cls, json.loads(json.dumps(doc).replace("project-children", selected)))
               for identity, (cls, doc) in cloud.rows.items()}
    cloud.rows.clear()
    cloud.rows.update(updated)
    cloud.permits["group-manager"][0]["spec"]["resource_id"] = selected
    material = copy.deepcopy(cloud.material)
    material["loom-management-publications"]["token"] = "test-github-secret"
    request = replace(request, material=material)
    client.settings = client.settings.model_copy(update={"candidate_id": publication[0]["candidate_id"]})
    client.operator_cloud_credentials.write_text("synthetic-operator-input")
    client.operator_cloud_credentials.chmod(0o600)
    events = []
    class SDK:
        async def close(self):
            events.append("cloud-closed")
    def sdk(**kwargs):
        assert kwargs["credentials_file_name"] == str(client.operator_cloud_credentials)
        events.append("cloud-opened")
        return SDK()
    monkeypatch.setattr(nebius.sdk, "SDK", sdk)
    for api, name, key in [(v1, "ProjectServiceClient", "projects"), (v1, "ServiceAccountServiceClient", "accounts"),
        (v1, "GroupMembershipServiceClient", "memberships"), (v1, "AccessPermitServiceClient", "permits"),
        (v1, "AuthPublicKeyServiceClient", "public_keys"), (v2, "AccessKeyServiceClient", "access_keys"),
        (storage, "BucketServiceClient", "buckets")]:
        monkeypatch.setattr(api, name, lambda _sdk, key=key: cloud.clients[key])
    quota = {"metadata": {"id": "quota-test", "parent_id": client.settings.cloud.tenant_id,
        "name": client.settings.storage_quota_name}, "spec": {"region": client.settings.cloud.region, "limit": str(200 * 1024**3)},
        "status": {"state": "STATE_ACTIVE", "usage_state": "USAGE_STATE_USED", "service": "compute",
        "unit": "bytes", "usage": str(100 * 1024**3)}}
    async def list_quotas(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0}
        return quotas.ListQuotaAllowancesResponse.from_json(json.dumps({"items": [quota]}))
    monkeypatch.setattr(quotas, "QuotaAllowanceServiceClient", lambda _sdk: SimpleNamespace(list=list_quotas))
    real_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(
        transport=github_transport(publication[1], publication[2]), **kwargs))
    config = request.deployment.installation.foundation.platform_config
    resources = {"nodes": [{"apiVersion": "v1", "kind": "Node", "metadata": {"name": "computeinstance-test", "uid": str(uuid4()),
        "labels": {"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}},
        "spec": {"providerID": "nebius://computeinstance-test"}, "status": {"conditions": [{"type": "Ready", "status": "True"}],
        "allocatable": {"cpu": "16", "memory": "64Gi", "ephemeral-storage": "256Gi", "pods": "64"}}}]}
    kinds = {"nodes": "Node", "pods": "Pod", "deployments": "Deployment", "statefulsets": "StatefulSet", "replicasets": "ReplicaSet",
             "daemonsets": "DaemonSet", "jobs": "Job", "cronjobs": "CronJob", "persistentvolumeclaims": "PersistentVolumeClaim",
             "ingresses": "Ingress", "horizontalpodautoscalers": "HorizontalPodAutoscaler"}
    def respond(req):
        assert req.method == "GET"
        resource = req.url.path.rsplit("/", 1)[-1]
        if "/storageclasses/" in req.url.path:
            return httpx.Response(200, json={"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": {"name": config["storage_class"], "uid": str(client.settings.storage_class_uid)},
                "provisioner": "compute.csi.nebius.com", "parameters": {}, "volumeBindingMode": "WaitForFirstConsumer"})
        api = "v1" if req.url.path.startswith("/api/v1/") else "/".join(req.url.path.split("/")[2:4])
        return httpx.Response(200, json={"apiVersion": api, "kind": kinds[resource] + "List",
            "metadata": {"resourceVersion": "1"}, "items": resources.get(resource, [])})
    client.client = httpx.Client(base_url=client.api_server, transport=httpx.MockTransport(respond))
    def object_client(name, **kwargs):
        assert name == "s3" and kwargs["aws_access_key_id"] == "aws-backup"
        assert kwargs["aws_secret_access_key"] == "never-print-secret"
        def head_bucket(**kwargs):
            assert kwargs == {"Bucket": "loom-management-backup"}
            events.append("backup-read")
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}
        return SimpleNamespace(head_bucket=head_bucket, close=lambda: events.append("backup-closed"),
                               meta=SimpleNamespace(events=HierarchicalEmitter()))
    monkeypatch.setattr(boto3, "client", object_client)
    monkeypatch.setattr(module, "qualify_dns_target", lambda **kwargs: {
        "management_host": request.deployment.public_host, "child_domain": request.deployment.installation.foundation.public_dns_zone})
    monkeypatch.setattr(module, "qualify_public_routes", lambda target: events.append("public-route"))
    return client, request, resources, quota, events


def test_connected_preflight_qualifies_publication_iam_capacity_storage_and_route(connected_checks):
    from scripts.ops.nebius_management_install import render_installation

    client, request, _, _, events = connected_checks
    client.preflight(request, render_installation(request))
    assert events.count("cloud-opened") == events.count("cloud-closed") == 1
    assert "public-route" in events and "backup-read" in events and "backup-closed" in events


@pytest.mark.parametrize("shortfall", ["quota", "platform", "pending_volume"])
def test_live_capacity_shortfall_prevents_installation_qualification(connected_checks, shortfall):
    from scripts.ops.nebius_management_install import render_installation
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    client, request, resources, quota, _ = connected_checks
    if shortfall == "quota":
        quota["spec"]["limit"] = quota["status"]["usage"]
    elif shortfall == "platform":
        resources["nodes"][0]["status"]["allocatable"]["ephemeral-storage"] = "1Gi"
    else:
        resources["persistentvolumeclaims"] = [{"apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "competing", "namespace": "foreign", "uid": str(uuid4())},
            "spec": {"storageClassName": request.deployment.installation.foundation.platform_config["storage_class"],
                     "resources": {"requests": {"storage": "99Gi"}}}, "status": {"phase": "Pending"}}]
    with pytest.raises(ManagementPrerequisiteError):
        client.preflight(request, render_installation(request))
