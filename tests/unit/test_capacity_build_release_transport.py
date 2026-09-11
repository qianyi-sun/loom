"""Registered native release is authenticated and validates exact evidence."""

import json
from uuid import uuid4

import httpx
import pytest

from loom_capacity_agent.admission import ExecutableReleaseReceiptV2, ExecutableReleaseRequestV2
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_build_admission_client import client_for, native_registration


def release_request(pool="gb10"):
    worker = native_registration(pool)
    return ExecutableReleaseRequestV2(binding=worker.binding, operation_id=uuid4(),
        reporter_incarnation=uuid4(), bootstrap_registration_epoch=1,
        protected_registration_epoch=2, expected_claim_high_water=1, release_epoch=4)


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("boundary", ["exact", "binding", "reporter_incarnation", "bootstrap_registration_epoch",
    "protected_registration_epoch", "claim_high_water", "release_epoch", "request_digest",
    "protected_release_sha256", "noncanonical", "rejected"])
async def test_native_release_client_validates_exact_receipt(pool, boundary):
    request = release_request(pool)
    digest = canonical_executable_digest(request)
    expected = ExecutableReleaseReceiptV2(binding=request.binding, reporter_incarnation=request.reporter_incarnation,
        bootstrap_registration_epoch=1, protected_registration_epoch=2, claim_high_water=1,
        release_epoch=4, request_digest=digest, protected_release_sha256=digest, protected_high_water=7)

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/release")
        assert outgoing.headers["Authorization"] == "Bearer admission-only-secret"
        assert json.loads(outgoing.content) == {"schema_version": 1,
            "release": request.model_dump(mode="json"), "worker_credential": "w" * 43}
        result = expected
        if boundary == "binding":
            result = result.model_copy(update={"binding": request.binding.model_copy(update={"intent_id": uuid4()})})
        elif boundary == "reporter_incarnation":
            result = result.model_copy(update={boundary: uuid4()})
        elif boundary in {"request_digest", "protected_release_sha256"}:
            result = result.model_copy(update={boundary: "f" * 64})
        elif boundary in {"bootstrap_registration_epoch", "protected_registration_epoch", "claim_high_water", "release_epoch"}:
            result = result.model_copy(update={boundary: 9})
        elif boundary == "rejected":
            return httpx.Response(409, content=b"private credential database diagnostics")
        return httpx.Response(200, content=canonical_executable_bytes(result) + (b" " if boundary == "noncanonical" else b""))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, request)
        if boundary == "exact":
            assert await client.acknowledge_release(request, current_worker_credential="w" * 43) == expected
        else:
            with pytest.raises(RuntimeError) as failure:
                await client.acknowledge_release(request, current_worker_credential="w" * 43)
            assert "private credential" not in str(failure.value)


@pytest.mark.parametrize("credential", ["short", "w" * 513, "é" * 43, " " * 43])
async def test_native_release_rejects_invalid_secret_before_network(credential):
    async def unexpected(outgoing):
        pytest.fail("invalid release credential reached transport")

    request = release_request()
    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as http:
        with pytest.raises(ValueError):
            await client_for(http, request).acknowledge_release(request, current_worker_credential=credential)


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("purpose", ["personal-build-worker", "application-worker"])
async def test_release_router_preserves_purpose_and_closes_client(tmp_path, pool, purpose):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from tests.unit.test_capacity_typed_admission_routing import configured

    module, binding_request, document, path, digest = configured(tmp_path, pool, purpose)
    request = release_request(pool).model_copy(update={"binding": binding_request.binding})
    release = AsyncMock(return_value="receipt")
    close = AsyncMock()

    def selected(*args, **kwargs):
        return SimpleNamespace(acknowledge_release=release, aclose=close)

    def unexpected(*args, **kwargs):
        pytest.fail("release crossed purpose authority")

    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        application_client_factory=selected if purpose == "application-worker" else unexpected,
        build_client_factory=selected if purpose == "personal-build-worker" else unexpected)
    assert await router.acknowledge_release(request, current_worker_credential="w" * 43) == "receipt"
    release.assert_awaited_once_with(request, current_worker_credential="w" * 43)
    close.assert_awaited_once()
