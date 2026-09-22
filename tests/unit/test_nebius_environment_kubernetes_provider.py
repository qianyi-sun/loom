"""Create-only Kubernetes provider: exact intent/ownership and UID readiness."""

from __future__ import annotations

import copy
import json
from uuid import uuid4

import httpx
import pytest

from loom_service.environment_management.steps import ProvisioningStep


def context():
    from loom_service.environment_management.provider import ProvisioningContext
    from loom_service.environment_management.registry import OperationLease

    return ProvisioningContext(
        OperationLease(uuid4(), uuid4(), 1, 1, uuid4()),
        {"incarnation": str(uuid4()), "application_namespace": "loom-dev-alice",
         "execution_namespace": "loom-execution-alice", "build_namespace": "loom-execution-alice-build"},
        {}, {},
    )


def namespace():
    return ProvisioningStep("ns", "kubernetes", {
        "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "loom-dev-alice"},
    })


async def test_create_readback_recovery_is_idempotent_and_does_not_patch():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )

    stored, writes = {}, []

    def api(request):
        if request.method == "POST":
            writes.append(request.method)
            obj = json.loads(request.content)
            obj["metadata"]["uid"] = "namespace-uid"
            stored.update(obj)
            return httpx.Response(201, json=obj)
        assert request.method == "GET"
        return httpx.Response(200, json=stored) if stored else httpx.Response(404)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        ctx = context()
        assert await provider.apply(ctx, namespace()) == "namespace-uid"
        assert await provider.apply(ctx, namespace()) == "namespace-uid"
        assert writes == ["POST"]
        assert stored["metadata"]["labels"]["loom.nebius/environment-id"] == str(ctx.lease.environment_id)


@pytest.mark.parametrize("change", ["owner", "body", "deleted", "uid"])
async def test_existing_foreign_changed_deleting_or_replaced_resource_is_never_adopted(change):
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    stored = {}

    def api(request):
        if request.method == "POST":
            stored.update(json.loads(request.content))
            stored["metadata"]["uid"] = "owned-uid"
            return httpx.Response(201, json=stored)
        return httpx.Response(200, json=stored) if stored else httpx.Response(404)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        ctx = context()
        assert await provider.apply(ctx, namespace()) == "owned-uid"
        if change == "owner":
            stored["metadata"]["labels"]["loom.nebius/environment-id"] = str(uuid4())
        elif change == "body":
            stored["kind"] = "Secret"
        elif change == "deleted":
            stored["metadata"]["deletionTimestamp"] = "2026-09-22T00:00:00Z"
        else:
            ctx.identities["ns"] = "earlier-uid"
        with pytest.raises(ProviderBlockedError):
            await provider.apply(ctx, namespace())


async def test_job_ready_requires_recorded_uid_and_complete_condition():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import (
        ProviderBlockedError,
        ProviderRetryError,
    )

    ctx = context()
    ctx.identities["job"] = "owned-job"
    job = {"metadata": {"uid": "owned-job"}, "status": {}}
    step = ProvisioningStep("ready", "job_ready", {
        "namespace": "loom-dev-alice", "name": "loom-migrate", "resource_key": "job",
    })
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=copy.deepcopy(job)),
    )) as http:
        provider = KubernetesEnvironmentProvider(http)
        with pytest.raises(ProviderRetryError):
            await provider.apply(ctx, step)
        job["status"] = {"conditions": [{"type": "Complete", "status": "True"}]}
        assert await provider.apply(ctx, step) == "owned-job"
        job["metadata"]["uid"] = "replacement-job"
        with pytest.raises(ProviderBlockedError):
            await provider.apply(ctx, step)


@pytest.mark.parametrize("kind,namespace_name", [("ClusterRole", None), ("Secret", "foreign"), ("Namespace", "foreign")])
async def test_provider_rejects_out_of_scope_intent_without_network_call(kind, namespace_name):
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    def unexpected(request):
        pytest.fail("out-of-scope intent made a request")

    doc = {"apiVersion": "v1", "kind": kind, "metadata": {"name": "foreign"}}
    if kind != "Namespace":
        doc["metadata"]["namespace"] = namespace_name
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(unexpected)) as http:
        with pytest.raises(ProviderBlockedError):
            await KubernetesEnvironmentProvider(http).apply(context(), ProvisioningStep("bad", "kubernetes", doc))


async def test_namespace_replacement_blocks_creation_into_the_same_name():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    ctx = context()
    ctx.identities["k8s:Namespace:-:loom-dev-alice"] = "original-namespace"
    calls = []

    def api(request):
        calls.append(request.method)
        if request.url.path == "/api/v1/namespaces/loom-dev-alice":
            return httpx.Response(200, json={"metadata": {"uid": "foreign-replacement"}})
        if request.method == "GET":
            return httpx.Response(404)
        obj = json.loads(request.content)
        obj["metadata"]["uid"] = "wrongly-created-object"
        return httpx.Response(201, json=obj)

    doc = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"namespace": "loom-dev-alice", "name": "test"},
           "data": {"key": "value"}}
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        with pytest.raises(ProviderBlockedError):
            await KubernetesEnvironmentProvider(http).apply(ctx, ProvisioningStep("cm", "kubernetes", doc))
    assert "POST" not in calls
