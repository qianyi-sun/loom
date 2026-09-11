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
    now = datetime.now(UTC)
    permit = BuildExecutionPermitV1(request=request, request_digest=canonical_digest(request),
        issued_at=now, not_after=now + timedelta(seconds=1))
    channel.send(b'{"kind":"permit","permit":' + canonical_bytes(permit) + b',"schema_version":1}')
"""

BROKER = r"""
import json, signal, socket, sys
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
    channel.send(json.dumps({'schema_version':1,'kind':kind}, sort_keys=True, separators=(',',':')).encode())
    if role == 'client':
        signal.pause()
"""


@pytest.mark.parametrize("boundary", ["complete", "blocked-broker", "blocked-authority", "authority-eof",
    "oversize", "malformed", "cancelled", "broker-eof", "wrong-order", "client-failed"])
def test_supervisor_stops_exact_broker_without_blocking_on_helpers(tmp_path, boundary):
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
    started = time.monotonic()
    try:
        result = supervise_native_execution(request.claim, source_binding_sha256=request.source_binding_sha256,
            authority=authority, broker_channel=broker_channel, broker_process=broker)
        assert time.monotonic() - started < 5, "blocked helper stalled the deadline monitor"
        assert result.broker_reaped and broker.poll() is not None
        roles = json.loads(trace.read_text()) if trace.exists() else []
        if boundary == "complete":
            assert result.client_succeeded and result.reason == "completed"
            assert roles == ["pause", "buildkit", "client"]
            assert broker.returncode == -signal.SIGKILL
        else:
            assert not result.client_succeeded
            if boundary in {"blocked-broker", "blocked-authority"}:
                assert result.reason == "expired"
                assert roles == (["pause"] if boundary == "blocked-broker" else ["pause", "buildkit", "client"])
            elif boundary == "cancelled":
                assert result.reason == "cancelled" and roles == []
            elif boundary == "wrong-order":
                assert result.reason == "protocol-error" and roles == ["pause"]
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
