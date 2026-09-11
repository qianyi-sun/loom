"""Build admission client stays pool-bound and replays exact request evidence."""

import json
from importlib import import_module

import httpx
import pytest

from loom_capacity_agent.admission import PreparedExecutableAdmissionV2
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def registration(pool="gb10"):
    return ExecutableBootstrapRegistrationV2(binding=typed_context(pool=pool).binding,
        command_sequence=2,bootstrap_registration_epoch=1,bootstrap_evidence_sha256="a"*64)


def client_for(http, request, *, origin="https://management.test"):
    module = import_module("loom_capacity_executor.build_admission_client")
    binding = request.binding
    identity = module.BuildAdmissionExecutorV1(pool_id=binding.pool_id,pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,executor_incarnation=binding.executor_incarnation)
    return module.BuildAdmissionClient(identity,origin=origin,bearer_token="admission-only-secret",http_client=http)


def receipt(request):
    digest = canonical_executable_digest(request)
    return PreparedExecutableAdmissionV2(subject_id=request.binding.subject_id,
        subject_incarnation=request.binding.subject_incarnation,intent_id=request.binding.intent_id,
        bootstrap_registration_epoch=1,bootstrap_sha256="b"*64,request_digest=digest,
        admission_digest=digest,protected_high_water=1)


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
async def test_client_replays_exact_preparation_after_lost_reply(pool):
    request = registration(pool)
    delivered = []

    async def handle(outgoing):
        delivered.append(outgoing.content)
        assert outgoing.headers["Authorization"] == "Bearer admission-only-secret"
        assert outgoing.url.path == f"/api/v1/internal/capacity-build/pools/{pool}/intents/{request.binding.intent_id}/prepare"
        assert json.loads(outgoing.content)["registration"] == request.model_dump(mode="json")
        if len(delivered)==1:
            raise httpx.ReadTimeout("lost",request=outgoing)
        return httpx.Response(200,content=canonical_executable_bytes(receipt(request)))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http,request)
        with pytest.raises(RuntimeError,match="transport"):
            await client.prepare_worker(request,bootstrap_sha256="b"*64)
        assert await client.prepare_worker(request,bootstrap_sha256="b"*64) == receipt(request)
    assert delivered[0] == delivered[1]


@pytest.mark.parametrize("boundary", ["wrong-digest", "wrong-hash", "oversized", "redirect", "reject", "pool"])
async def test_client_rejects_changed_evidence_and_transport(boundary):
    request = registration()
    outgoing_requests = []

    async def handle(outgoing):
        outgoing_requests.append(outgoing)
        result = receipt(request)
        if boundary=="wrong-digest":
            result = result.model_copy(update={"admission_digest":"c"*64})
        elif boundary=="wrong-hash":
            result = result.model_copy(update={"bootstrap_sha256":"c"*64})
        if boundary=="oversized":
            return httpx.Response(200,content=b"x"*(64*1024+1))
        if boundary=="redirect":
            return httpx.Response(307,headers={"Location":"https://foreign.test/stolen"})
        if boundary=="reject":
            return httpx.Response(409,content=b"private database diagnostic")
        return httpx.Response(200,content=canonical_executable_bytes(result))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle),follow_redirects=True) as http:
        client = client_for(http,request)
        if boundary=="pool":
            request = request.model_copy(update={"binding":request.binding.model_copy(update={"pool_id":"oldlab"})})
        with pytest.raises((RuntimeError,ValueError)) as failure:
            await client.prepare_worker(request,bootstrap_sha256="b"*64)
        assert "private database diagnostic" not in str(failure.value)
    assert len(outgoing_requests) == (0 if boundary=="pool" else 1)


@pytest.mark.parametrize("origin", ["http://management.test", "https://user:password@management.test", "https://management.test/path"])
async def test_client_requires_exact_https_origin(origin):
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValueError):
            client_for(http,registration(),origin=origin)
