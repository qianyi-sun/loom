"""Native launch helpers fail closed if their exact trusted parent disappears."""

import ctypes
import os
import select
import signal
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_parent_death_binding_rejects_an_already_replaced_parent():
    from loom_capacity_executor.native_parent_death import bind_native_parent_death

    with pytest.raises(ValueError):
        bind_native_parent_death(1)
    with pytest.raises(RuntimeError, match="parent"):
        bind_native_parent_death(os.getppid() + 100000)


@pytest.mark.parametrize("boundary", ["set-error", "read-error", "wrong-signal", "changed-after", "unavailable"])
def test_parent_death_setup_errors_never_succeed(monkeypatch, boundary):
    from types import SimpleNamespace

    from loom_capacity_executor import native_parent_death as module

    calls = []
    class Prctl:
        def __call__(self, operation, argument, *_unused):
            calls.append(operation)
            if operation == 1:
                return -1 if boundary == "set-error" else 0
            module.ctypes.c_int.from_address(argument).value = (
                signal.SIGTERM if boundary == "wrong-signal" else signal.SIGKILL)
            return -1 if boundary == "read-error" else 0
    def load(*args, **kwargs):
        if boundary == "unavailable":
            raise OSError("unavailable")
        return SimpleNamespace(prctl=Prctl())
    monkeypatch.setattr(module.ctypes, "CDLL", load)
    parents = iter([3456, 1 if boundary == "changed-after" else 3456])
    monkeypatch.setattr(module.os, "getppid", lambda: next(parents))
    with pytest.raises(RuntimeError, match="parent"):
        module.bind_native_parent_death(3456)
    assert calls == ([] if boundary == "unavailable" else [1] if boundary == "set-error" else [1, 2])


@pytest.mark.parametrize("boundary", ["fork", "exec", "cleanup-control"])
def test_kernel_parent_death_stops_child_even_when_it_is_blocked(boundary):
    # The locked Python build lacks os.pidfd_open although the host libc/kernel
    # provide it. Keep exact process handles for failure cleanup; never send a
    # late signal to an unchecked numeric PID.
    libc = ctypes.CDLL(None, use_errno=True)
    libc.pidfd_open.argtypes = [ctypes.c_int, ctypes.c_uint]
    libc.pidfd_open.restype = ctypes.c_int
    libc.pidfd_send_signal.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
    libc.pidfd_send_signal.restype = ctypes.c_int
    check_fd = libc.pidfd_open(os.getpid(), 0)
    assert check_fd >= 0, "parent-death probe requires kernel pidfd support"
    os.close(check_fd)
    read_end, write_end = os.pipe()
    program = """
import os, signal, sys
from loom_capacity_executor.native_parent_death import bind_native_parent_death
ready, boundary = int(sys.argv[1]), sys.argv[2]
parent = os.getpid()
child = os.fork()
if child == 0:
    signal.alarm(15)  # Final safety bound even if the harness itself fails.
    if boundary != 'cleanup-control':
        bind_native_parent_death(parent)
    if boundary == 'exec':
        os.set_inheritable(ready, True)
        os.execv(sys.executable, [sys.executable, '-c',
            'import os, signal, sys; os.write(int(sys.argv[1]), str(os.getpid()).encode()); signal.pause()',
            str(ready)])
    os.write(ready, str(os.getpid()).encode()+b'\\n')
    signal.pause()
else:
    os.close(ready)
    signal.pause()
"""
    parent = subprocess.Popen([sys.executable, "-c", program, str(write_end), boundary],
        pass_fds=(write_end,), env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(ROOT / "src")},
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    os.close(write_end)
    child_pidfd = None
    try:
        assert select.select([read_end], [], [], 10)[0], "child did not bind its parent"
        raw_pid = os.read(read_end, 64)
        if not raw_pid:
            parent.kill()
            pytest.fail(parent.communicate(timeout=5)[1].decode())
        assert int(raw_pid) > 1
        opened_fd = libc.pidfd_open(int(raw_pid), 0)
        assert opened_fd >= 0, "cannot retain exact child identity"
        child_pidfd = opened_fd
        # Only the bound child retains this writer, including across exec.
        # EOF proves its blocked lifetime ended without PID-reuse ambiguity.
        assert not select.select([read_end], [], [], 0)[0]
        parent.kill()
        assert parent.wait(timeout=5) == -signal.SIGKILL
        if boundary == "cleanup-control":
            assert not select.select([child_pidfd], [], [], 0)[0]
            return  # finally must kill the deliberately unbound blocked child.
        assert select.select([read_end], [], [], 5)[0], "orphan child survived parent death"
        assert os.read(read_end, 1) == b""
    finally:
        if parent.poll() is None:
            parent.kill()
        parent.wait(timeout=5)
        if child_pidfd is not None:
            try:
                if not select.select([child_pidfd], [], [], 0)[0]:
                    assert libc.pidfd_send_signal(child_pidfd, signal.SIGKILL, None, 0) == 0
                assert select.select([child_pidfd], [], [], 5)[0], "test child cleanup did not finish"
            finally:
                os.close(child_pidfd)
        os.close(read_end)
        if parent.stderr is not None:
            parent.stderr.close()
