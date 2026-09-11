"""Sealed native launch connects assigned admission to verified source staging."""

import asyncio
import base64
import hashlib
import json
import os
from importlib import import_module
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pytest

from loom.personal_dev_build_platform_requests import canonical_build_source
from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_agent.build_admission import (
    BuildClaimReceiptV1,
    BuildClaimRequestV1,
    BuildSourceReadReceiptV1,
)
from loom_capacity_executor.native_worker_handoff import (
    NativeWorkerHandoffV1,
    sealed_native_worker_handoff,
)
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_capacity_build_admission_client import client_for, native_registration
from tests.unit.test_capacity_typed_admission_routing import configured
from tests.unit.test_native_build_context import context_for
from tests.unit.test_native_build_source import sealed_source as sealed_source


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("view", ["source", "io"])
@pytest.mark.parametrize("boundary", ["exact", "lost-reply", "unavailable", "retry-cancel", "job", "cgroup", "purpose",
    "receipt", "source", "cancel", "consumer"])
async def test_allocated_worker_owns_handoff_claims_exact_work_and_cleans(sealed_source, tmp_path, monkeypatch, pool, boundary, view):
    module = import_module("loom_capacity_executor.native_allocated_worker")
    registration, archive, workspace = sealed_source
    router_module, bootstrap, document, route, digest = configured(tmp_path, pool,
        "application-worker" if boundary == "purpose" else "personal-build-worker")
    worker = native_registration(pool).model_copy(update={"binding": bootstrap.binding})
    physical = PhysicalJobBindingV2(binding=worker.binding, ownership_evidence_sha256="a" * 64,
        operation_id=uuid4(), bootstrap_registration_epoch=1, slurm_job_id=worker.slurm_job_id)
    packet = NativeWorkerHandoffV1(registration=worker, physical=physical,
        executor=document.executor, admission={"path": str(route), "sha256": digest}, worker_credential="w" * 43)
    request_id = uuid4()
    calls, claims, staged_paths = [], [], []
    inherited = None
    def cgroup(path):
        assert path == Path("/proc/self/cgroup")
        with pytest.raises(OSError):
            os.fstat(inherited)
        return PurePosixPath(f"/system.slice/slurmstepd.scope/job_{'9999' if boundary == 'cgroup' else worker.slurm_job_id}/step_batch/user/task_0")
    monkeypatch.setattr(module, "_unified_cgroup_path", cgroup)
    if boundary == "retry-cancel":
        async def cancel_backoff(delay):
            assert delay == 0.2
            raise asyncio.CancelledError
        monkeypatch.setattr(module.asyncio, "sleep", cancel_backoff)

    async def handle(outgoing):
        body = json.loads(outgoing.content)
        assert body["worker_credential"] == packet.worker_credential
        calls.append(outgoing.url.path)
        if outgoing.url.path.endswith("/claim-assigned"):
            claims.append(body["claim"])
            assert "request_id" not in body["claim"]
            claim = BuildClaimRequestV1.model_validate_json(json.dumps({**body["claim"], "request_id": str(request_id)}))
            if boundary in {"unavailable", "retry-cancel"} or (boundary == "lost-reply" and len(claims) == 1):
                raise httpx.ReadError("reply lost after commit")
            if boundary == "receipt":
                claim = claim.model_copy(update={"worker_incarnation": uuid4()})
            return httpx.Response(200, content=canonical_bytes(BuildClaimReceiptV1(request=claim, request_digest=canonical_digest(claim))))
        claim = BuildClaimRequestV1.model_validate_json(json.dumps(body["claim"]))
        context = context_for(claim).model_copy(update={
            "source_binding_sha256": hashlib.sha256(canonical_build_source(registration)).hexdigest(),
            "source_sha256": registration.candidate.source_sha256,
            "archive_sha256": registration.candidate.archive_sha256, "archive_size_bytes": len(archive),
            "build_contract_sha256": registration.candidate.build_contract_sha256,
            "source_commit": registration.candidate.source_commit, "dirty": registration.candidate.dirty})
        if outgoing.url.path.endswith("/context"):
            return httpx.Response(200, content=canonical_bytes(context))
        if boundary == "cancel":
            raise asyncio.CancelledError
        offset, length = body["offset"], body["length"]
        data = archive[offset:offset + length]
        if boundary == "source":
            data = b"!" + data[1:]
        return httpx.Response(200, content=canonical_bytes(BuildSourceReadReceiptV1.model_validate({"schema_version": 1,
            "claim_digest": canonical_digest(claim), "source_binding_sha256": context.source_binding_sha256,
            "archive_sha256": context.archive_sha256, "archive_size_bytes": len(archive),
            "offset": offset, "data_base64": base64.b64encode(data).decode()})))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        def factory(path, **kwargs):
            assert path == route and kwargs == {"expected_sha256": digest, "executor": document.executor}
            return router_module.TypedAdmissionRouter(path, **kwargs,
                build_client_factory=lambda *args: client_for(http, worker),
                application_client_factory=lambda *args, **kwargs: pytest.fail("opened application authority"))
        with sealed_native_worker_handoff(packet) as descriptor:
            inherited = os.dup(descriptor)
            async def consume():
                scope_factory = module.allocated_worker_io if view == "io" else module.stage_allocated_worker_source
                async with scope_factory(inherited,
                    job_id="9999" if boundary == "job" else worker.slurm_job_id,
                    workspace=workspace, max_archive_bytes=2 * 1024 * 1024, admission_factory=factory) as staged:
                    assert boundary in {"exact", "lost-reply", "consumer"}
                    assert staged.source.archive.read_bytes() == archive
                    assert staged.claim.worker_id == worker.worker_id
                    assert staged.claim.request_id == request_id
                    assert not hasattr(staged, "worker_credential")
                    staged_paths.append(staged.source.archive)
                    if boundary == "consumer":
                        raise RuntimeError("consumer failed")
            if boundary in {"exact", "lost-reply"}:
                await consume()
            elif boundary in {"cancel", "retry-cancel"}:
                with pytest.raises(asyncio.CancelledError):
                    await consume()
            else:
                with pytest.raises((RuntimeError, ValueError)) as error:
                    await consume()
                assert packet.worker_credential not in str(error.value)
            with pytest.raises(OSError):
                os.fstat(inherited)
    assert list(workspace.iterdir()) == []
    assert all(not path.exists() for path in staged_paths)
    if boundary in {"job", "cgroup", "purpose"}:
        assert calls == []
    if boundary == "retry-cancel":
        assert len(claims) == len(calls) == 1
    if boundary in {"lost-reply", "unavailable"}:
        assert len(claims) == (2 if boundary == "lost-reply" else 3)
        assert all(claim == claims[0] for claim in claims)


async def test_allocated_claim_operation_is_stable_and_registration_scoped(tmp_path):
    module = import_module("loom_capacity_executor.native_allocated_worker")
    from tests.unit.test_native_worker_handoff import packet_for, prepared_worker

    _directory, _lease, physical, admission, credential = await prepared_worker(tmp_path)
    packet = packet_for(physical, admission.requests[0], credential, tmp_path)
    first = module.allocated_claim_request(packet.registration)
    assert first == module.allocated_claim_request(packet.registration)
    assert first.binding == packet.registration.binding
    assert first.worker_id == packet.registration.worker_id
    assert first.worker_incarnation == packet.registration.worker_incarnation
    assert first.operation_id != module.allocated_claim_request(packet.registration.model_copy(
        update={"worker_incarnation": uuid4()})).operation_id
