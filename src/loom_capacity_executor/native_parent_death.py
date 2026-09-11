"""Kernel lifetime binding for one trusted native helper, not containment proof."""

from __future__ import annotations

import ctypes
import os
import signal


def bind_native_parent_death(expected_parent_pid: int) -> None:
    """Fail closed unless this process remains bound to its original parent.

    Call from the helper itself, never subprocess preexec_fn in a timer loop.
    Check before and after prctl to cover a parent that died before binding.
    Every fork link must bind separately; fork resets this setting, and set-ID
    exec/credential changes can clear it. The installed rootless runtime chain
    therefore requires its own empirical conformance test. This does not reap
    processes, kill a cgroup, prove physical release, or authorize a command.
    """
    if type(expected_parent_pid) is not int or expected_parent_pid <= 1:
        raise ValueError("native helper parent identity is invalid")
    if os.getppid() != expected_parent_pid:
        raise RuntimeError("native helper parent changed before lifetime binding")
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        prctl = libc.prctl
        prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
        prctl.restype = ctypes.c_int
        if prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
            raise RuntimeError("native helper parent-death binding failed")
        configured = ctypes.c_int()
        if prctl(2, ctypes.addressof(configured), 0, 0, 0) != 0 or configured.value != signal.SIGKILL:
            raise RuntimeError("native helper parent-death binding readback failed")
    except (AttributeError, OSError):
        raise RuntimeError("native helper parent-death binding is unavailable") from None
    if os.getppid() != expected_parent_pid:
        raise RuntimeError("native helper parent changed during lifetime binding")
