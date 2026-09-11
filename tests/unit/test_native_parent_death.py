"""Native launch helpers fail closed if their exact trusted parent disappears."""

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


@pytest.mark.parametrize("boundary", ["fork", "exec"])
def test_kernel_parent_death_stops_child_even_when_it_is_blocked(boundary):
    read_end, write_end = os.pipe()
    program = """
import os, signal, sys
from loom_capacity_executor.native_parent_death import bind_native_parent_death
ready, boundary = int(sys.argv[1]), sys.argv[2]
parent = os.getpid()
child = os.fork()
if child == 0:
    bind_native_parent_death(parent)
    if boundary == 'exec':
        os.set_inheritable(ready, True)
        os.execv(sys.executable, [sys.executable, '-c',
            'import os, signal, sys; os.write(int(sys.argv[1]), str(os.getpid()).encode()+b"\\n"); signal.pause()',
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
    child_pid = None
    child_pidfd = None
    try:
        assert select.select([read_end], [], [], 10)[0], "child did not bind its parent"
        raw_pid = os.read(read_end, 64)
        assert raw_pid, parent.communicate(timeout=5)[1].decode()
        child_pid = int(raw_pid)
        child_pidfd = os.pidfd_open(child_pid)
        assert not select.select([child_pidfd], [], [], 0)[0]
        parent.kill()
        assert parent.wait(timeout=5) == -signal.SIGKILL
        assert select.select([child_pidfd], [], [], 5)[0], "orphan child survived parent death"
    finally:
        if parent.poll() is None:
            parent.kill()
        parent.wait(timeout=5)
        if child_pidfd is not None:
            if not select.select([child_pidfd], [], [], 0)[0]:
                signal.pidfd_send_signal(child_pidfd, signal.SIGKILL)
            os.close(child_pidfd)
        os.close(read_end)
        if parent.stderr is not None:
            parent.stderr.close()
