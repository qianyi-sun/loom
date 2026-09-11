"""Allocated credentials stay in a scoped IO owner, never mapped runtime data."""

import asyncio
import hashlib
import socket
from importlib import import_module
from uuid import uuid4

import pytest

from loom_capacity_agent.build_admission import BuildArtifactV1, BuildOutcomeReceiptV1, BuildOutcomeRequestV1
from loom_capacity_agent.build_artifact_stream import BuildArtifactUploadReceiptV1
from loom_capacity_executor.native_build_source import NativeStagedBuildSource
from loom_capacity_executor.native_supervisor import NativeAuthorityPermission, NativeAuthorityRequest
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_native_build_context import context_for
from tests.unit.test_native_execution_deadline import receipt
from tests.unit.test_native_execution_permit import execution_request


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
@pytest.mark.parametrize("boundary", ["exact", "claim", "upload-receipt", "outcome-receipt", "transport"])
async def test_allocated_io_binds_exact_upload_outcome_and_scope(tmp_path, pool, boundary):
    module = import_module("loom_capacity_executor.native_allocated_io")
    claim = execution_request(pool).claim
    source = NativeStagedBuildSource(context_for(claim), tmp_path / "source.tar")
    artifact = BuildArtifactV1(archive_sha256=hashlib.sha256(b"artifact").hexdigest(), archive_size_bytes=8)
    request = BuildOutcomeRequestV1(claim=execution_request(pool).claim if boundary == "claim" else claim,
        operation_id=uuid4(), result="artifact-ready", artifact=artifact)
    calls = []

    class Client:
        async def upload_artifact(self, value, *, worker_credential, artifact, chunks):
            assert value == claim and worker_credential == "w" * 43
            calls.append("upload")
            assert b"".join([part async for part in chunks]) == b"artifact"
            if boundary == "transport":
                raise OSError("transport unavailable")
            return BuildArtifactUploadReceiptV1(claim_digest="f" * 64 if boundary == "upload-receipt" else canonical_digest(claim),
                artifact=artifact)

        async def record_outcome(self, value, *, worker_credential):
            assert value == request and worker_credential == "w" * 43
            calls.append("outcome")
            changed = value.model_copy(update={"operation_id": uuid4()}) if boundary == "outcome-receipt" else value
            return BuildOutcomeReceiptV1(request=changed, request_digest=canonical_digest(changed))

    async def chunks():
        yield b"artifact"

    async with module.scoped_native_allocated_io(claim=claim, source=source, client=Client(), worker_credential="w" * 43) as owner:
        assert owner.claim == claim and owner.source == source
        assert "w" * 43 not in repr(owner) and not hasattr(owner, "worker_credential")
        if boundary in {"upload-receipt", "transport"}:
            with pytest.raises((ValueError, OSError)):
                await owner.upload_artifact(artifact, chunks=chunks())
        else:
            uploaded = await owner.upload_artifact(artifact, chunks=chunks())
            assert uploaded.artifact == artifact
            if boundary in {"claim", "outcome-receipt"}:
                with pytest.raises(ValueError):
                    await owner.record_outcome(request)
            else:
                assert (await owner.record_outcome(request)).request == request
    assert calls == ["upload"] + (["outcome"] if boundary in {"exact", "outcome-receipt"} else [])
    with pytest.raises(RuntimeError, match="closed"):
        await owner.record_outcome(request)
    assert calls.count("upload") == 1  # No implicit write retries.


@pytest.mark.parametrize("boundary", ["exact", "foreign-source", "close-inflight", "closed"])
async def test_allocated_authority_scope_settles_inflight_operations(tmp_path, boundary):
    module = import_module("loom_capacity_executor.native_allocated_io")
    request = execution_request()
    source = NativeStagedBuildSource(context_for(request.claim).model_copy(update={
        "source_binding_sha256": request.source_binding_sha256}), tmp_path / "source.tar")
    entered, cancelled = asyncio.Event(), asyncio.Event()
    calls = []

    class Client:
        async def authorize_execution(self, value, *, worker_credential):
            calls.append(value)
            assert worker_credential == "w" * 43
            entered.set()
            if boundary == "close-inflight":
                try:
                    await asyncio.Future()
                finally:
                    cancelled.set()
            return receipt(value)

    monitor, helper = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    monitor.setblocking(False)
    task = None
    try:
        async with module.scoped_native_allocated_io(claim=request.claim, source=source,
            client=Client(), worker_credential="w" * 43) as owner:
            if boundary != "closed":
                task = asyncio.create_task(owner.serve_authority(helper))
                sent = request.model_copy(update={"source_binding_sha256": "f" * 64}) if boundary == "foreign-source" else request
                monitor.send(canonical_bytes(NativeAuthorityRequest(request=sent)))
                async with asyncio.timeout(2):
                    if boundary == "close-inflight":
                        await entered.wait()
                    else:
                        wire = await asyncio.get_running_loop().sock_recv(monitor, 65536)
                        assert b"w" * 43 not in wire
                        if boundary == "exact":
                            assert NativeAuthorityPermission.model_validate_json(wire).permit.request == request
        if boundary == "closed":
            with pytest.raises(RuntimeError, match="closed"):
                await owner.serve_authority(helper)
        else:
            assert task.done()
            if boundary == "close-inflight":
                assert task.cancelled() and cancelled.is_set()
        assert len(calls) == (1 if boundary in {"exact", "close-inflight"} else 0)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        monitor.close()
        helper.close()
