"""The supervisor owns deadlines while real helper processes can be blocked."""

import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.unit.test_native_execution_permit import execution_request

ROOT = Path(__file__).resolve().parents[2]

AUTHORITY = r"""
import json, socket, sys
from datetime import UTC, datetime, timedelta
from loom_capacity_agent.build_admission import BuildExecutionPermitV1, BuildExecutionRequestV1
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
channel = socket.socket(fileno=int(sys.argv[1]))
boundary = sys.argv[2]
count = 0
while True:
    wire = channel.recv(65537)
    if not wire:
        break
    request = BuildExecutionRequestV1.model_validate_json(json.dumps(json.loads(wire)['request']))
    count += 1
    if boundary == 'initial-silence':
        import signal
        signal.pause()
    if count > 1 and boundary in {'blocked-authority', 'blocked-broker'}:
        import signal
        signal.pause()
    if boundary == 'authority-eof':
        break
    if boundary == 'oversize':
        channel.send(b'x' * 65537)
        continue
    if boundary == 'malformed':
        channel.send(b'{')
        continue
    if boundary == 'cancelled':
        channel.send(b'{"kind":"cancelled","schema_version":1}')
        continue
    if boundary == 'wrong-challenge':
        from uuid import uuid4
        request = request.model_copy(update={'challenge':uuid4()})
    now = datetime.now(UTC)
    permit = BuildExecutionPermitV1(request=request, request_digest=canonical_digest(request),
        issued_at=now, not_after=now + timedelta(seconds=1))
    reply = b'{"kind":"permit","permit":' + canonical_bytes(permit) + b',"schema_version":1}'
    channel.send(reply + (b' ' if boundary == 'noncanonical' else b''))
    if boundary == 'duplicate':
        channel.send(reply)
"""

BROKER = r"""
import json, signal, socket, sys, time
from pathlib import Path
channel = socket.socket(fileno=int(sys.argv[1]))
boundary, trace = sys.argv[2], Path(sys.argv[3])
roles = []
while True:
    wire = channel.recv(65537)
    if not wire:
        break
    command = json.loads(wire)
    assert command['kind'] == 'start'
    assert command['deadline_boottime_ns'] > time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    role = command['role']
    roles.append(role)
    trace.write_text(json.dumps(roles))
    if boundary == 'blocked-broker' or (boundary == 'blocked-authority' and role == 'client'):
        signal.pause()
    if boundary == 'broker-eof':
        break
    kind = {'pause':'pause-ready', 'buildkit':'buildkit-ready', 'client':'client-succeeded'}[role]
    if boundary == 'wrong-order':
        kind = 'client-succeeded'
    if boundary == 'client-failed' and role == 'client':
        kind = 'client-failed'
    if boundary == 'renew' and role == 'client':
        import time
        time.sleep(1.5)
    channel.send(json.dumps({'schema_version':1,'kind':kind}, sort_keys=True, separators=(',',':')).encode())
    if role == 'client':
        signal.pause()
"""


@pytest.mark.parametrize("boundary", ["complete", "blocked-broker", "blocked-authority", "authority-eof",
    "oversize", "malformed", "cancelled", "broker-eof", "wrong-order", "client-failed", "initial-silence",
    "noncanonical", "wrong-challenge", "duplicate", "renew", "suspend-before-pause",
    "suspend-before-buildkit", "suspend-before-client", "suspend-before-completion"])
def test_supervisor_stops_exact_broker_without_blocking_on_helpers(tmp_path, monkeypatch, boundary):
    import json

    from loom_capacity_executor.native_supervisor import supervise_native_execution

    authority, auth_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    broker_channel, broker_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    env = {"PATH": "/usr/bin:/bin", "PYTHONPATH": str(ROOT / "src")}
    trace = tmp_path / "broker-roles.json"
    helper = subprocess.Popen([sys.executable, "-c", AUTHORITY, str(auth_child.fileno()), boundary],
        pass_fds=(auth_child.fileno(),), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    broker = subprocess.Popen([sys.executable, "-c", BROKER, str(broker_child.fileno()), boundary, str(trace)],
        pass_fds=(broker_child.fileno(),), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    auth_child.close()
    broker_child.close()
    request = execution_request()
    if boundary.startswith("suspend-before-"):
        from loom_capacity_executor import native_supervisor as module

        clock = time.clock_gettime_ns
        offset = [0]
        selections = [0]
        target = ["pause", "buildkit", "client", "completion"].index(boundary.removeprefix("suspend-before-")) + 1
        original_selector = module.selectors.DefaultSelector

        class SuspendSelector(original_selector):
            def select(self, timeout=None):
                events = super().select(timeout)
                if events:
                    selections[0] += 1
                    if selections[0] == target:
                        # Model resume with a ready packet after BOOTTIME has
                        # jumped past expiry. Children retain their real clocks.
                        offset[0] = 20_000_000_000
                return events

        monkeypatch.setattr(module.selectors, "DefaultSelector", SuspendSelector)
        monkeypatch.setattr(module.time, "clock_gettime_ns", lambda clock_id: clock(clock_id) + offset[0])
    started = time.monotonic()
    try:
        result = supervise_native_execution(request.claim, source_binding_sha256=request.source_binding_sha256,
            authority=authority, broker_channel=broker_channel, broker_process=broker)
        assert time.monotonic() - started < (12 if boundary == "initial-silence" else 5), "blocked helper stalled the deadline monitor"
        assert result.broker_reaped and broker.poll() is not None
        roles = json.loads(trace.read_text()) if trace.exists() else []
        if boundary in {"complete", "renew"}:
            assert result.client_succeeded and result.reason == "completed"
            assert roles == ["pause", "buildkit", "client"]
            assert broker.returncode == -signal.SIGKILL
            if boundary == "renew":
                assert time.monotonic() - started > 1.5
        else:
            assert not result.client_succeeded
            if boundary in {"blocked-broker", "blocked-authority"}:
                assert result.reason == "expired"
                assert roles == (["pause"] if boundary == "blocked-broker" else ["pause", "buildkit", "client"])
            elif boundary == "cancelled":
                assert result.reason == "cancelled" and roles == []
            elif boundary == "wrong-order":
                assert result.reason == "protocol-error" and roles == ["pause"]
            elif boundary == "initial-silence":
                assert result.reason == "expired" and roles == []
            elif boundary.startswith("suspend-before-"):
                assert result.reason == "expired"
                assert roles == ["pause", "buildkit", "client"][:target - 1]
    finally:
        authority.close()
        broker_channel.close()
        for child in (helper, broker):
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            if child.stderr is not None:
                child.stderr.close()


def test_pending_deadline_is_pollable_without_granting_execution():
    from loom_capacity_executor.native_execution_deadline import NativeExecutionDeadline

    request = execution_request()
    guard = NativeExecutionDeadline(request.claim, source_binding_sha256=request.source_binding_sha256)
    guard.begin_request()
    assert guard.poll_deadline() > time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    with pytest.raises(RuntimeError):
        guard.require_live()


def test_control_channel_rejects_descriptor_transfer_without_leaking_fd(tmp_path):
    import array

    from loom_capacity_executor.native_supervisor import _BROKER_MODELS, _receive

    receiver, sender = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    marker = tmp_path / "not-a-capability"
    marker.write_bytes(b"fixture")
    try:
        with marker.open("rb") as stream:
            before = set(Path("/proc/self/fd").iterdir())
            sender.sendmsg([b'{"kind":"pause-ready","schema_version":1}'],
                [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [stream.fileno()]))])
            with pytest.raises(ValueError):
                _receive(receiver, _BROKER_MODELS)
            assert set(Path("/proc/self/fd").iterdir()) == before
    finally:
        receiver.close()
        sender.close()


@pytest.mark.parametrize("boundary", ["initial-start", "completion"])
def test_queued_permit_then_cancellation_precedes_start_or_completion(monkeypatch, boundary):
    from loom_capacity_executor import native_supervisor as module
    from loom_capacity_manager.contracts import canonical_bytes
    from tests.unit.test_native_execution_deadline import receipt

    authority, authority_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    broker_channel, broker_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    authority_peer.settimeout(1)
    broker_peer.settimeout(1)
    broker = subprocess.Popen([sys.executable, "-c", "import signal; signal.pause()"])
    clock = [100_000_000_000]
    roles = []
    original_selector = module.selectors.DefaultSelector

    def reply(cancel=False):
        request = module.NativeAuthorityRequest.model_validate_json(authority_peer.recv(65536)).request
        authority_peer.send(canonical_bytes(module.NativeAuthorityPermission(permit=receipt(request))))
        if cancel:
            authority_peer.send(canonical_bytes(module.NativeAuthorityStop(kind="cancelled")))

    class QueuedSelector(original_selector):
        step = 0

        def select(self, timeout=None):
            if self.step == 0:
                reply(cancel=boundary == "initial-start")
            else:
                command = module.NativeBrokerStart.model_validate_json(broker_peer.recv(65536))
                roles.append(command.role)
                kind = {"pause": "pause-ready", "buildkit": "buildkit-ready", "client": "client-succeeded"}[command.role]
                if command.role == "client":
                    # The clock jump below made renewal due. Queue both replies
                    # before the selector reports simultaneous client success.
                    reply(cancel=True)
                broker_peer.send(canonical_bytes(module.NativeBrokerEvent(kind=kind)))
                if command.role == "buildkit":
                    clock[0] += 6_000_000_000
            self.step += 1
            return super().select(timeout)

    monkeypatch.setattr(module.selectors, "DefaultSelector", QueuedSelector)
    monkeypatch.setattr(module.time, "clock_gettime_ns", lambda _clock_id: clock[0])
    request = execution_request()
    try:
        result = module.supervise_native_execution(request.claim, source_binding_sha256=request.source_binding_sha256,
            authority=authority, broker_channel=broker_channel, broker_process=broker)
        assert not result.client_succeeded and result.reason == "cancelled"
        assert result.broker_reaped
        if boundary == "initial-start":
            assert roles == []
            broker_peer.setblocking(False)
            with pytest.raises(BlockingIOError):
                broker_peer.recv(65536)
        else:
            assert roles == ["pause", "buildkit", "client"]
    finally:
        for channel in (authority, authority_peer, broker_channel, broker_peer):
            channel.close()
        if broker.poll() is None:
            broker.kill()
        broker.wait(timeout=5)
