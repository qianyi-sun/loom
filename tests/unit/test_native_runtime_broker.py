"""The separate broker never turns control input into a generic host command."""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_executor.native_supervisor import NativeBrokerEvent, NativeBrokerStart
from loom_capacity_manager.contracts import canonical_bytes

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("boundary", ["wrong-order", "malformed", "expired", "eof"])
def test_real_broker_rejects_invalid_control_without_executing_runtime(tmp_path, boundary):
    from loom_capacity_executor.native_runtime_broker import NativeBrokerReady

    marker = tmp_path / "must-not-execute"
    runtime = tmp_path / "runsc"
    runtime.write_text(f"#!/bin/sh\ntouch {marker}\nexit 1\n")
    runtime.chmod(0o700)
    layout = NativeRunscLayout(runtime, tmp_path / "state", tmp_path / "bundles", "a" * 64)
    channel, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    channel.settimeout(5)
    broker = subprocess.Popen([sys.executable, "-m", "loom_capacity_executor.native_runtime_broker",
        *layout.arguments(), "--expected-parent", str(os.getpid()), "--control-fd", str(child.fileno())],
        pass_fds=(child.fileno(),), env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(ROOT / "src")},
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    child.close()
    try:
        ready = NativeBrokerReady.model_validate_json(channel.recv(65536))
        assert (ready.pid, ready.parent_pid, ready.claim_digest) == (broker.pid, os.getpid(), layout.claim_digest)
        if boundary == "eof":
            channel.close()
        elif boundary == "malformed":
            channel.send(b'{"command":"arbitrary-host-exec"}')
        else:
            channel.send(canonical_bytes(NativeBrokerStart(role="client" if boundary == "wrong-order" else "pause",
                deadline_boottime_ns=1 if boundary == "expired" else time.clock_gettime_ns(time.CLOCK_BOOTTIME) + 1_000_000_000)))
        if boundary != "eof":
            assert NativeBrokerEvent.model_validate_json(channel.recv(65536)).kind == "failed"
        assert broker.wait(timeout=5) != 0
        assert not marker.exists()
    finally:
        channel.close()
        if broker.poll() is None:
            broker.kill()
        broker.wait(timeout=5)
        if broker.stderr is not None:
            broker.stderr.close()
