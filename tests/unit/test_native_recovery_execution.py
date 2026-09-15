"""Recovery-capable authority cannot silently downgrade or change final records."""

import asyncio
import socket
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from loom_capacity_agent.native_recovery_execution import (
    BuildExecutionPermitV2,
    BuildExecutionRequestV2,
)
from loom_capacity_executor.native_authority_bridge import serve_native_execution_authority
from loom_capacity_executor.native_execution_deadline import NativeExecutionDeadline
from loom_capacity_executor.native_supervisor import NativeAuthorityRequest, NativeAuthorityStop
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.unit.test_native_execution_deadline import receipt
from tests.unit.test_native_execution_permit import execution_request


@pytest.mark.parametrize("boundary", ["downgrade", "foreign", "expiry", "digest", "naive"])
def test_recovery_permit_and_deadline_reject_downgrade_or_changed_binding(boundary):
    original = execution_request()
    guard = NativeExecutionDeadline(original.claim, source_binding_sha256=original.source_binding_sha256,
        recovery_finalization_sha256="a" * 64)
    request = guard.begin_request()
    now = datetime.now(UTC)
    if boundary == "downgrade":
        permit = receipt(original.model_copy(update={"challenge": request.challenge}))
    elif boundary == "foreign":
        permit = receipt(request.model_copy(update={"recovery_finalization_sha256": "b" * 64}))
    else:
        with pytest.raises(ValueError):
            BuildExecutionPermitV2(request=request, request_digest="f" * 64 if boundary == "digest" else canonical_executable_digest(request),
                issued_at=now.replace(tzinfo=None) if boundary == "naive" else now,
                not_after=now+timedelta(seconds=11 if boundary == "expiry" else 10))
        return
    with pytest.raises(RuntimeError):
        guard.accept(permit)
    assert guard.stopped_reason == "protocol-error"


@pytest.mark.parametrize("boundary", ["downgrade", "foreign", "upgrade"])
async def test_recovery_bridge_rejects_changed_version_or_digest_before_network(boundary):
    original = execution_request()
    request = (original if boundary == "downgrade" else BuildExecutionRequestV2(claim=original.claim,
        challenge=original.challenge, source_binding_sha256=original.source_binding_sha256,
        recovery_finalization_sha256="b" * 64))

    async def forbidden(*args, **kwargs):
        pytest.fail("changed recovery identity reached network authority")

    monitor, helper = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    monitor.setblocking(False)
    task = asyncio.create_task(serve_native_execution_authority(helper, claim=original.claim,
        source_binding_sha256=original.source_binding_sha256, worker_credential="x" * 43,
        recovery_finalization_sha256=None if boundary == "upgrade" else "a" * 64,
        client=SimpleNamespace(authorize_execution=forbidden, authorize_recovery_execution=forbidden)))
    try:
        monitor.send(canonical_bytes(NativeAuthorityRequest(request=request)))
        async with asyncio.timeout(2):
            reply = await asyncio.get_running_loop().sock_recv(monitor, 65536)
            await task
        assert reply == canonical_bytes(NativeAuthorityStop(kind="renewal-failed"))
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        monitor.close()
        helper.close()


@pytest.mark.parametrize("purpose", ["personal-build-worker", "application-worker"])
@pytest.mark.parametrize("boundary", ["exact", "changed", "invalid"])
async def test_recovery_execution_uses_only_exact_build_route(tmp_path, purpose, boundary):
    from tests.unit.test_capacity_typed_admission_routing import configured

    module, physical, document, path, digest = configured(tmp_path, "oldlab", purpose)
    original = execution_request()
    claim = original.claim.model_copy(update={"binding": physical.binding})
    request = BuildExecutionRequestV2(claim=claim, challenge=original.challenge,
        source_binding_sha256=original.source_binding_sha256, recovery_finalization_sha256="a" * 64)
    calls = []

    async def authorize(incoming, **kwargs):
        assert incoming == request and kwargs == {"worker_credential": "x" * 43}
        calls.append("authorize")
        if boundary == "invalid":
            return object()
        return receipt(request.model_copy(update={"recovery_finalization_sha256": "b" * 64}) if boundary == "changed" else request)

    async def close():
        calls.append("close")

    def forbidden(*args, **kwargs):
        pytest.fail("recovery execution reached application authority")

    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        build_client_factory=lambda *args: SimpleNamespace(authorize_recovery_execution=authorize, aclose=close),
        application_client_factory=forbidden)
    if purpose == "personal-build-worker" and boundary == "exact":
        assert (await router.authorize_recovery_execution(request, worker_credential="x" * 43)).request == request
    else:
        with pytest.raises(ValueError):
            await router.authorize_recovery_execution(request, worker_credential="x" * 43)
    assert calls == ([] if purpose == "application-worker" else ["authorize", "close"])
