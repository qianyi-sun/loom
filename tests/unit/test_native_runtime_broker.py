"""The separate broker never turns control input into a generic host command."""

import ctypes
import json
import os
import select
import signal
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

FAKE_RUNTIME = r'''
import json, os, signal, sys
from pathlib import Path
signal.alarm(15)
root = Path(next(arg.split('=', 1)[1] for arg in sys.argv if arg.startswith('--root=')))
operation = next(arg for arg in sys.argv[1:] if not arg.startswith('--'))
identity = sys.argv[-1]
if operation == 'run':
    descriptors = []
    for fd in Path('/proc/self/fd').iterdir():
        try:
            descriptors.append(os.readlink(fd))
        except FileNotFoundError:
            pass
    (root / identity).write_text(json.dumps({'id':identity,'status':'running','pid':os.getpid(),'fds':descriptors}))
    if identity.startswith('loom-native-client-'):
        raise SystemExit(0)
    signal.pause()
elif operation == 'state':
    print((root / identity).read_text())
elif operation == 'exec':
    raise SystemExit(0)
else:
    raise SystemExit(1)
'''


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


@pytest.mark.parametrize("boundary", ["duplicate", "broker-death", "complete"])
def test_broker_lifetime_and_descriptor_isolation_with_real_exec(tmp_path, boundary):
    from loom_capacity_executor.native_runtime_broker import NativeBrokerReady

    runtime = tmp_path / "runsc"
    runtime.write_text(f"#!{sys.executable}\n" + FAKE_RUNTIME)
    runtime.chmod(0o700)
    state = tmp_path / "state"
    state.mkdir()
    layout = NativeRunscLayout(runtime, state, tmp_path / "bundles", "b" * 64)
    channel, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    channel.settimeout(5)
    broker = subprocess.Popen([sys.executable, "-m", "loom_capacity_executor.native_runtime_broker",
        *layout.arguments(), "--expected-parent", str(os.getpid()), "--control-fd", str(child.fileno())],
        pass_fds=(child.fileno(),), env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(ROOT / "src")},
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    child.close()
    libc = ctypes.CDLL(None, use_errno=True)
    libc.pidfd_open.argtypes = [ctypes.c_int, ctypes.c_uint]
    libc.pidfd_open.restype = ctypes.c_int
    libc.pidfd_send_signal.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
    libc.pidfd_send_signal.restype = ctypes.c_int
    owned = []
    try:
        NativeBrokerReady.model_validate_json(channel.recv(65536))
        for role in ("pause", "buildkit", "client"):
            channel.send(canonical_bytes(NativeBrokerStart(role=role,
                deadline_boottime_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME) + 5_000_000_000)))
            event = NativeBrokerEvent.model_validate_json(channel.recv(65536))
            assert event.kind == {"pause": "pause-ready", "buildkit": "buildkit-ready", "client": "client-succeeded"}[role]
            observed = json.loads((state / layout.identity(role)).read_text())
            assert not any(target.startswith("socket:") for target in observed["fds"]), "broker control writer survived exec"
            if role != "client":
                handle = libc.pidfd_open(observed["pid"], 0)
                assert handle >= 0
                owned.append(handle)
            if role == "pause" and boundary != "complete":
                if boundary == "duplicate":
                    channel.send(canonical_bytes(NativeBrokerStart(role="pause",
                        deadline_boottime_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME) + 5_000_000_000)))
                    assert NativeBrokerEvent.model_validate_json(channel.recv(65536)).kind == "failed"
                    assert broker.wait(timeout=5) != 0
                else:
                    broker.kill()
                    broker.wait(timeout=5)
                break
        if boundary == "complete":
            assert broker.poll() is None
            broker.kill()
            broker.wait(timeout=5)
        for handle in owned:
            assert select.select([handle], [], [], 5)[0], "runtime survived broker exit"
    finally:
        channel.close()
        if broker.poll() is None:
            broker.kill()
        broker.wait(timeout=5)
        for handle in owned:
            if not select.select([handle], [], [], 0)[0]:
                assert libc.pidfd_send_signal(handle, signal.SIGKILL, None, 0) == 0
            os.close(handle)
        if broker.stderr is not None:
            broker.stderr.close()


def test_runtime_control_output_is_bounded_during_capture(monkeypatch, tmp_path):
    from loom_capacity_executor import native_runtime_broker as module

    layout = NativeRunscLayout(tmp_path / "runsc", tmp_path / "state", tmp_path / "bundles", "a" * 64)
    monkeypatch.setattr(module, "_wrapper", lambda *_args: [sys.executable, "-c",
        "import os,signal; os.write(1, b'x'*65537); signal.pause()"])
    started = time.monotonic()
    with pytest.raises(ValueError, match="bound"):
        module._control(layout, "state", "pause")
    assert time.monotonic() - started < 2, "overflow was not stopped during capture"
