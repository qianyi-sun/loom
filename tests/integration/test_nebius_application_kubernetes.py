"""Real journal transactions bound to controlled Kubernetes HTTP outcomes."""
from __future__ import annotations

import asyncio
import copy
import json

import httpx
import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_effects import expire, started
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class KubernetesAPI:
    def __init__(self):
        self.objects = {}
        self.mutations = []
        self.lose_response = False
        self.hide_objects = False
        self.pending_delete = False

    def handle(self, request):
        path = request.url.path
        if request.method == "GET":
            value = None if self.hide_objects else self.objects.get(path)
            return httpx.Response(200 if value else 404, json=value or {})
        body = json.loads(request.content)
        self.mutations.append((request.method, path, body))
        if request.method == "POST":
            path += "/" + body["metadata"]["name"]
            if path in self.objects:
                return httpx.Response(409, json={})
            value = copy.deepcopy(body)
            value["metadata"].update(uid=f"uid-{len(self.mutations)}", resourceVersion="1")
            self.objects[path] = value
            if self.lose_response:
                raise httpx.ReadTimeout("must-not-escape-protected-detail")
            return httpx.Response(201, json=value)
        value = self.objects.get(path)
        if value is None:
            return httpx.Response(404, json={})
        if request.method == "PATCH":
            assert request.headers["content-type"] == "application/json-patch+json"
            assert body[:2] == [
                {"op": "test", "path": "/metadata/uid", "value": value["metadata"]["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": value["metadata"]["resourceVersion"]},
            ]
            for patch in body[2:]:
                if patch["path"] == "/spec":
                    value["spec"] = patch["value"]
                elif patch["path"] == "/metadata/annotations":
                    value["metadata"]["annotations"] = patch["value"]
                elif patch["path"] == "/metadata/labels":
                    value["metadata"]["labels"] = patch["value"]
                else:
                    raise AssertionError("unexpected patch field")
            value["metadata"]["resourceVersion"] = "2"
            return httpx.Response(200, json=value)
        assert request.method == "DELETE"
        assert body == {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background",
                        "preconditions": {"uid": value["metadata"]["uid"],
                                          "resourceVersion": value["metadata"]["resourceVersion"]}}
        if not self.pending_delete:
            del self.objects[path]
        return httpx.Response(202, json={"kind": "Status", "status": "Success"})


@pytest.fixture
async def provider(applications):
    from loom_service.application_management.kubernetes import ApplicationKubernetesProvider

    registry, _, _, plan, _, lease = await started(applications)
    api = KubernetesAPI()
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        client = ApplicationKubernetesProvider(registry, http)
        yield client, api, registry, plan, lease


async def namespace_ready(provider):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    effect = await client.create(lease, "namespace", document)
    assert effect.phase == "observed" and effect.observed_uid == "uid-1"
    return client, api, registry, plan, lease


async def test_namespace_create_is_single_winner_and_observed_replay_never_resends(provider):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    original = copy.deepcopy(document)
    results = await asyncio.gather(*[client.create(lease, "namespace", document) for _ in range(3)],
                                   return_exceptions=True)
    assert all(not isinstance(value, Exception) or isinstance(value, ProviderWaitingError) for value in results)
    observed = await client.create(lease, "namespace", document)
    assert observed.phase == "observed" and observed.observed_uid == "uid-1"
    assert document == original
    assert len(api.mutations) == 1
    history = await registry.effect_history(lease)
    assert history == [observed]


async def test_uncertain_create_and_absence_never_authorize_resend(provider, applications):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    api.lose_response = True
    with pytest.raises(ProviderWaitingError) as error:
        await client.create(lease, "namespace", document)
    assert "protected-detail" not in str(error.value)
    await expire(applications[1], lease)
    stale = lease
    lease = await registry.claim(lease.operation_id)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await client.create(stale, "namespace", document)
    api.hide_objects = True
    with pytest.raises(ProviderWaitingError):
        await client.create(lease, "namespace", document)
    assert (await registry.effect_history(lease))[0].phase == "dispatched"
    assert len(api.mutations) == 1
    api.hide_objects = False
    result = await client.create(lease, "namespace", document)
    assert result.phase == "observed" and len(api.mutations) == 1


async def test_namespaced_create_requires_recorded_unchanged_namespace(provider):
    client, api, _, plan, lease = provider
    document = named(plan["prepared"], "Deployment", "loom-service")
    with pytest.raises(ProviderBlockedError, match="namespace_identity"):
        await client.create(lease, "api", document)
    assert api.mutations == []
    await namespace_ready(provider)
    api.objects["/api/v1/namespaces/loom-dev-alice"]["metadata"]["uid"] = "replacement"
    with pytest.raises(ProviderBlockedError, match="namespace_identity"):
        await client.create(lease, "api", document)
    assert len(api.mutations) == 1


async def test_credential_contents_never_enter_effect_journal(provider):
    client, api, _, plan, lease = await namespace_ready(provider)
    namespace = plan["prepared"].registration.application_namespace
    name = f"loom-application-auth-{lease.incarnation.hex}-g1"
    document = {"apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                "metadata": {"name": name, "namespace": namespace}, "data": {"key": "dGVzdC1vbmx5LW1hdGVyaWFs"}}
    created = await client.create(lease, "auth", document)
    assert created.phase == "observed"
    serialized = json.dumps(created.intent.model_dump())
    assert "dGVzdC1vbmx5LW1hdGVyaWFs" not in serialized and "data" not in created.intent.model_dump()
    assert api.objects[f"/api/v1/namespaces/{namespace}/secrets/{name}"]["data"] == document["data"]


async def test_patch_and_delete_send_exact_preconditions_and_wait_for_retirement(provider):
    client, api, registry, plan, lease = await namespace_ready(provider)
    document = named(plan["prepared"], "Deployment", "loom-service")
    created = await client.create(lease, "api", document)
    stopped = copy.deepcopy(document)
    stopped["spec"]["replicas"] = 0
    patched = await client.patch_spec(lease, "stop-api", stopped,
        uid=created.observed_uid, resource_version=created.observed_resource_version)
    assert patched.observed_uid == created.observed_uid and patched.observed_resource_version == "2"
    target = dict(api_version="apps/v1", kind="Deployment", namespace="loom-dev-alice", name="loom-service",
                  uid=patched.observed_uid, resource_version=patched.observed_resource_version)
    api.pending_delete = True
    with pytest.raises(ProviderWaitingError):
        await client.delete(lease, "delete-api", **target)
    assert (await registry.effect_history(lease))[-1].phase == "dispatched"
    path = "/apis/apps/v1/namespaces/loom-dev-alice/deployments/loom-service"
    # A replacement proves the old UID is gone, never authorizes deleting it.
    api.objects[path]["metadata"]["uid"] = "replacement-api"
    retired = await client.delete(lease, "delete-api", **target)
    assert retired.phase == "observed" and retired.observed_resource_version is None
    assert api.objects[path]["metadata"]["uid"] == "replacement-api"
    assert [method for method, _, _ in api.mutations] == ["POST", "POST", "PATCH", "DELETE"]


async def test_wrong_reconciliation_identity_is_not_adopted(provider):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    api.lose_response = True
    with pytest.raises(ProviderWaitingError):
        await client.create(lease, "namespace", document)
    api.objects["/api/v1/namespaces/loom-dev-alice"]["metadata"]["labels"]["loom.nebius/application-id"] = "foreign"
    with pytest.raises(ProviderBlockedError, match="resource_identity"):
        await client.create(lease, "namespace", document)
    assert (await registry.effect_history(lease))[0].phase == "dispatched"
    assert len(api.mutations) == 1


async def test_malformed_delete_readback_cannot_prove_old_resource_absence(provider):
    client, api, registry, plan, lease = await namespace_ready(provider)
    created = await client.create(lease, "api", named(plan["prepared"], "Deployment", "loom-service"))
    target = dict(api_version="apps/v1", kind="Deployment", namespace="loom-dev-alice", name="loom-service",
                  uid=created.observed_uid, resource_version=created.observed_resource_version)
    api.pending_delete = True
    with pytest.raises(ProviderWaitingError):
        await client.delete(lease, "delete-api", **target)
    del api.objects["/apis/apps/v1/namespaces/loom-dev-alice/deployments/loom-service"]["metadata"]["uid"]
    with pytest.raises(ProviderBlockedError):
        await client.delete(lease, "delete-api", **target)
    assert (await registry.effect_history(lease))[-1].phase == "dispatched"
    assert len(api.mutations) == 3


async def test_stale_lease_cannot_issue_a_kubernetes_write(provider, applications):
    client, api, registry, plan, lease = provider
    alice = applications[2][0]
    await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
                             action="suspend", expected_generation=1)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await client.create(lease, "namespace", named(plan["prepared"], "Namespace", "loom-dev-alice"))
    assert api.mutations == []
