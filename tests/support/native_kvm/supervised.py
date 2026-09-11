"""Disposable real monitor/broker runtime; authority replies are fixture-only."""

import asyncio
import os
import socket
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loom_capacity_agent.build_admission import (
    BuildClaimRequestV1,
    BuildExecutionPermitV1,
    BuildSourceContextV1,
)
from loom_capacity_executor.native_authority_bridge import serve_native_execution_authority
from loom_capacity_executor.native_build_session import execute_native_build_session
from loom_capacity_executor.native_parent_death import bind_native_parent_death
from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_executor.native_runtime_broker import NativeBrokerReady
from loom_capacity_executor.native_runtime_cleanup import reconcile_native_runtime_cleanup
from loom_capacity_executor.native_supervisor import supervise_native_execution
from loom_capacity_manager.contracts import canonical_digest


def serve_authority(descriptor, parent_pid, expiry):
    bind_native_parent_death(parent_pid)
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    claim = BuildClaimRequestV1.model_validate_json(Path("/fixtures/claim.json").read_bytes())
    assert canonical_digest(claim) == context.claim_digest

    class FixtureClient:
        calls = 0

        async def authorize_execution(self, request, *, worker_credential):
            assert worker_credential == "x" * 43
            self.calls += 1
            if expiry and self.calls > 1:
                await asyncio.Future()  # Simulate HTTP blocked while the client runs.
            now = datetime.now(UTC)
            return BuildExecutionPermitV1(request=request, request_digest=canonical_digest(request),
                issued_at=now, not_after=now + timedelta(seconds=10))

    with socket.socket(fileno=descriptor) as channel:
        asyncio.run(serve_native_execution_authority(channel, claim=claim,
            source_binding_sha256=context.source_binding_sha256, worker_credential="x" * 43,
            client=FixtureClient()))


def supervised_build(expiry=False, native_session=False):
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    claim = BuildClaimRequestV1.model_validate_json(Path("/fixtures/claim.json").read_bytes())
    assert canonical_digest(claim) == context.claim_digest
    layout = NativeRunscLayout(Path("/runtime/runsc"), Path("/tmp/runsc-state"),
        Path("/fixtures"), context.claim_digest)
    authority, auth_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    channel, broker_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    children = []
    try:
        children.append(subprocess.Popen([sys.executable, __file__, str(auth_child.fileno()), str(os.getpid()), str(int(expiry))],
            pass_fds=(auth_child.fileno(),)))
        auth_child.close()
        if native_session:
            session = execute_native_build_session(claim=claim, context=context, layout=layout,
                workspace=Path("/tmp/native-work"), authority=authority, expected_parent_pid=os.getppid(),
                max_artifact_bytes=32 * 1024**2, max_image_archive_bytes=3 * 1024**2)
            assert session.supervision.broker_reaped and session.cleanup.confirmed, session
            if expiry:
                assert session.supervision.reason == "expired" and session.artifact is None, session
            else:
                assert session.supervision.client_succeeded and len(session.artifact.images) == 10, session
                print("native-supervised-build-completed", flush=True)
            print("native-supervised-cleanup-confirmed", flush=True)
            return
        broker = subprocess.Popen([sys.executable, "-m", "loom_capacity_executor.native_runtime_broker",
            *layout.arguments(), "--expected-parent", str(os.getpid()), "--control-fd", str(broker_child.fileno())],
            pass_fds=(broker_child.fileno(),))
        children.append(broker)
        broker_child.close()
        channel.settimeout(10)
        ready = NativeBrokerReady.model_validate_json(channel.recv(65536))
        assert (ready.pid, ready.parent_pid, ready.claim_digest) == (broker.pid, os.getpid(), context.claim_digest)
        result = supervise_native_execution(claim, source_binding_sha256=context.source_binding_sha256,
            authority=authority, broker_channel=channel, broker_process=broker)
        assert result.broker_reaped, result
        if expiry:
            assert not result.client_succeeded and result.reason == "expired", result
        else:
            assert result.client_succeeded, result
            print("native-supervised-build-completed", flush=True)
        cleanup = reconcile_native_runtime_cleanup(layout, broker_process=broker)
        assert cleanup.confirmed, cleanup
        print("native-supervised-cleanup-confirmed", flush=True)
    finally:
        for peer in (authority, auth_child, channel, broker_child):
            peer.close()
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)


if __name__ == "__main__":
    serve_authority(int(sys.argv[1]), int(sys.argv[2]), bool(int(sys.argv[3])))
