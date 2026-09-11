"""Native context stays compact, purpose-routed and bound to the exact claim."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from loom_capacity_agent.build_admission import BuildClaimRequestV1, BuildSourceContextV1
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_capacity_build_admission_client import client_for, native_registration
from tests.unit.test_capacity_typed_admission_routing import configured
from tests.unit.test_native_build_source import sealed_source as sealed_source


def context_for(claim):
    return BuildSourceContextV1(claim_digest=canonical_digest(claim), request_id=claim.request_id,
        source_binding_sha256="a" * 64, platform="linux/arm64" if claim.binding.pool_id == "gb10" else "linux/amd64",
        candidate_id=uuid4(), candidate_sha="b" * 64, source_sha256="c" * 64,
        archive_sha256="d" * 64, archive_size_bytes=10240, build_contract_sha256="e" * 64,
        source_commit="f" * 40, dirty=True, attempt_id=uuid4(), attempt_sequence=2,
        lease_epoch=3, subject_id=uuid4(), subject_incarnation=uuid4(), operation_id=uuid4(),
        operation_epoch=4, lease_not_after=datetime.now(UTC) + timedelta(minutes=1))


def claim_for(binding):
    return BuildClaimRequestV1(binding=binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=uuid4(), worker_incarnation=uuid4())


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("boundary", ["exact", "claim", "request", "platform", "noncanonical", "oversize"])
async def test_native_context_client_validates_current_claim_and_bounded_reply(pool, boundary):
    claim = claim_for(native_registration(pool).binding)
    context = context_for(claim)
    changed = {"claim": {"claim_digest": "f" * 64}, "request": {"request_id": uuid4()},
        "platform": {"platform": "linux/amd64" if pool == "gb10" else "linux/arm64"}}
    async def handle(request):
        assert request.url.path.endswith("/context")
        assert b'"worker_credential":"' in request.content
        wire = canonical_bytes(context.model_copy(update=changed.get(boundary, {})))
        if boundary == "noncanonical":
            wire += b" "
        if boundary == "oversize":
            wire += b" " * (64 * 1024)
        return httpx.Response(200, content=wire)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, claim)
        if boundary == "exact":
            assert await client.read_source_context(claim, worker_credential="x" * 43) == context
        else:
            with pytest.raises((RuntimeError, ValueError)):
                await client.read_source_context(claim, worker_credential="x" * 43)


@pytest.mark.parametrize("purpose", ["application-worker", "personal-build-worker"])
@pytest.mark.parametrize("boundary", ["exact", "error", "invalid", "directory"])
async def test_native_context_router_never_uses_application_authority(tmp_path, purpose, boundary):
    module, request, document, path, digest = configured(tmp_path, "gb10", purpose)
    claim = claim_for(request.binding)
    context, events = context_for(claim), []
    async def read(incoming, **options):
        assert incoming == claim
        assert options == {"worker_credential": "x" * 43}
        if boundary == "error":
            raise ValueError("unavailable")
        return object() if boundary == "invalid" else context
    async def close():
        events.append("close")
    def factory(*args):
        events.append("open")
        return SimpleNamespace(read_source_context=read, aclose=close)
    def forbidden(*args, **kwargs):
        pytest.fail("native context reached application credentials")
    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        build_client_factory=factory, application_client_factory=forbidden)
    if boundary == "directory":
        path.write_bytes(path.read_bytes() + b" ")
    if purpose == "personal-build-worker" and boundary == "exact":
        assert await router.read_source_context(claim, worker_credential="x" * 43) == context
    else:
        with pytest.raises((ValueError, RuntimeError)):
            await router.read_source_context(claim, worker_credential="x" * 43)
    assert events == ([] if purpose == "application-worker" or boundary == "directory" else ["open", "close"])


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("boundary", ["exact", "context-claim", "context-source", "context-commit", "context-dirty",
    "contract", "limit", "source-drift", "cancel"])
async def test_claim_context_stages_real_sealed_source_without_application_records(sealed_source, pool, boundary):
    import base64
    import hashlib
    import json
    from importlib import import_module

    from loom.personal_dev_build_platform_requests import canonical_build_source

    registration, archive, workspace = sealed_source
    candidate, attempt = registration.candidate, registration.build_attempt
    claim = claim_for(native_registration(pool).binding)
    context = context_for(claim).model_copy(update={"candidate_id": candidate.id, "candidate_sha": candidate.candidate_sha,
        "source_binding_sha256": hashlib.sha256(canonical_build_source(registration)).hexdigest(),
        "source_sha256": candidate.source_sha256, "archive_sha256": candidate.archive_sha256,
        "archive_size_bytes": len(archive), "build_contract_sha256": candidate.build_contract_sha256,
        "source_commit": candidate.source_commit, "dirty": candidate.dirty, "attempt_id": attempt.id})
    context = context.model_copy(update={
        "context-claim": {"claim_digest": "f" * 64}, "context-source": {"source_sha256": "f" * 64},
        "context-commit": {"source_commit": "f" * 40}, "context-dirty": {"dirty": not candidate.dirty},
        "contract": {"build_contract_sha256": "f" * 64},
    }.get(boundary, {}))
    calls = []
    async def handle(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/context"):
            return httpx.Response(200, content=canonical_bytes(context))
        incoming = json.loads(request.content)
        offset, length = incoming["offset"], incoming["length"]
        if boundary == "cancel" and offset:
            import asyncio

            raise asyncio.CancelledError
        return httpx.Response(200, content=json.dumps({"schema_version": 1,
            "claim_digest": canonical_digest(claim), "archive_sha256": candidate.archive_sha256,
            "source_binding_sha256": "f" * 64 if boundary == "source-drift" else context.source_binding_sha256,
            "archive_size_bytes": len(archive), "offset": offset,
            "data_base64": base64.b64encode(archive[offset:offset + length]).decode("ascii")},
            sort_keys=True, separators=(",", ":")).encode("ascii"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        module = import_module("loom_capacity_executor.native_build_source")
        source = module.NativeClaimBuildSource(client=client_for(http, claim), workspace=workspace,
            max_archive_bytes=1 if boundary == "limit" else 2 * 1024 * 1024)
        async def consume():
            async with source.stage_claim(claim, worker_credential="x" * 43) as staged:
                assert boundary == "exact"
                assert staged.context == context
                assert staged.archive.read_bytes() == archive
            assert not staged.archive.exists()
        if boundary == "exact":
            await consume()
        elif boundary == "cancel":
            import asyncio

            with pytest.raises(asyncio.CancelledError):
                await consume()
        else:
            with pytest.raises((ValueError, RuntimeError)):
                await consume()
    assert not list(workspace.iterdir())
    if boundary in {"context-claim", "contract", "limit"}:
        assert len(calls) == 1
