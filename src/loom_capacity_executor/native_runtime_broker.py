"""One fixed runtime broker; spawn and control IO never block the supervisor.

Run only as the trusted supervisor's precreated child. Material and allocation
verification are caller prerequisites, not established by this module. Kernel
binding stops the attached runtime chain on broker death; installed rootless
containment and exact cleanup still require independent conformance.
"""

from __future__ import annotations

import json
import os
import select
import socket
import subprocess
import sys
import time
from typing import Annotated, Literal

from pydantic import Field

from loom_capacity_executor.native_parent_death import bind_native_parent_death
from loom_capacity_executor.native_runsc import NativeRunscLayout, layout_parser
from loom_capacity_executor.native_supervisor import (
    NativeBrokerEvent,
    NativeBrokerStart,
    _configure,
    _receive,
    _send,
)
from loom_capacity_manager.contracts import Digest, StrictV1Model


class NativeBrokerReady(StrictV1Model):
    kind: Literal["broker-ready"] = "broker-ready"
    pid: Annotated[int, Field(gt=1)]
    parent_pid: Annotated[int, Field(gt=1)]
    claim_digest: Digest


def _wrapper(layout: NativeRunscLayout, operation: str, role: str, deadline: int = 0) -> list[str]:
    return [sys.executable, "-m", "loom_capacity_executor.native_runsc", *layout.arguments(),
        "--expected-parent", str(os.getpid()), "--operation", operation, "--role", role,
        "--deadline", str(deadline)]


def _check_children(children: dict[str, subprocess.Popen[bytes]]) -> None:
    for role in ("pause", "buildkit"):
        if role in children and children[role].poll() is not None:
            raise RuntimeError("native runtime parent exited")


def _control(layout: NativeRunscLayout, operation: str, role: str) -> subprocess.CompletedProcess[bytes]:
    # Only the broker may block here. Every wrapper binds before exec; no IPC
    # writer or worker credential descriptor is inherited by runtime commands.
    command = _wrapper(layout, operation, role)
    child = subprocess.Popen(command, close_fds=True,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    output = bytearray()
    until = time.clock_gettime_ns(time.CLOCK_BOOTTIME) + 5_000_000_000
    try:
        assert child.stdout is not None
        descriptor = child.stdout.fileno()
        os.set_blocking(descriptor, False)
        while True:
            remaining = (until - time.clock_gettime_ns(time.CLOCK_BOOTTIME)) / 1_000_000_000
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, 5)
            if not select.select([descriptor], [], [], min(0.05, remaining))[0]:
                continue
            part = os.read(descriptor, min(16384, 65537 - len(output)))
            if not part:
                child.wait(timeout=max(0, (until - time.clock_gettime_ns(time.CLOCK_BOOTTIME)) / 1_000_000_000))
                return subprocess.CompletedProcess(command, child.returncode, bytes(output))
            output.extend(part)
            if len(output) > 65536:
                raise ValueError("native runtime control output exceeds its bound")
    finally:
        try:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
        finally:
            if child.stdout is not None:
                child.stdout.close()


def _wait_ready(layout: NativeRunscLayout, role: str, channel: socket.socket,
    children: dict[str, subprocess.Popen[bytes]],
) -> None:
    until = time.clock_gettime_ns(time.CLOCK_BOOTTIME) + 20_000_000_000
    while time.clock_gettime_ns(time.CLOCK_BOOTTIME) < until:
        _check_children(children)
        if select.select([channel], [], [], 0)[0]:
            # No second start is allowed until readiness has been acknowledged.
            raise ValueError("native broker received premature control or EOF")
        result = _control(layout, "state", role)
        if result.returncode == 0:
            state = json.loads(result.stdout)
            if not isinstance(state, dict) or state.get("id") != layout.identity(role):
                raise ValueError("native runtime state identity changed")
            if state.get("status") == "running":
                if role == "pause" or _control(layout, "ready", role).returncode == 0:
                    _check_children(children)
                    return
        # Readability interrupts readiness polling if the supervisor disappears.
        if select.select([channel], [], [], 0.05)[0]:
            raise ValueError("native broker received premature control or EOF")
    raise RuntimeError("native runtime did not become ready")


def run_native_runtime_broker(layout: NativeRunscLayout, *, channel: socket.socket,
    expected_parent_pid: int,
) -> int:
    """Single-use role sequence; the supervisor owns successful broker teardown."""
    children: dict[str, subprocess.Popen[bytes]] = {}
    try:
        bind_native_parent_death(expected_parent_pid)
        layout.__post_init__()
        _configure(channel)
        _send(channel, NativeBrokerReady(pid=os.getpid(), parent_pid=expected_parent_pid,
            claim_digest=layout.claim_digest))
        for role in ("pause", "buildkit", "client"):
            while not select.select([channel], [], [], 0.05)[0]:
                _check_children(children)
            _check_children(children)
            message = _receive(channel, {"start": NativeBrokerStart})
            if not isinstance(message, NativeBrokerStart) or message.role != role:
                raise ValueError("native broker start sequence changed")
            if time.clock_gettime_ns(time.CLOCK_BOOTTIME) >= message.deadline_boottime_ns:
                raise RuntimeError("native broker received an expired start")
            children[role] = subprocess.Popen(_wrapper(layout, "start", role, message.deadline_boottime_ns),
                close_fds=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=None)
            if role != "client":
                _wait_ready(layout, role, channel, children)
                _send(channel, NativeBrokerEvent(kind="pause-ready" if role == "pause" else "buildkit-ready"))
        while children["client"].poll() is None:
            _check_children(children)
            if select.select([channel], [], [], 0.05)[0]:
                raise ValueError("native broker received repeated control or EOF")
        _check_children(children)
        _send(channel, NativeBrokerEvent(kind="client-succeeded" if children["client"].returncode == 0 else "client-failed"))
        # Remain owned/alive until the supervisor stops us. Exiting immediately
        # could race its client-success read with a broker-death observation.
        while not select.select([channel], [], [], 0.05)[0]:
            _check_children(children)
        raise ValueError("native broker control closed after client completion")
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired):
        try:
            _send(channel, NativeBrokerEvent(kind="failed"))
        except (OSError, ValueError):
            pass
        return 1
    finally:
        # Parent SIGKILL bypasses finally: per-command kernel binding remains
        # mandatory. This normal-error path reaps only this broker's children.
        for child in reversed(tuple(children.values())):
            try:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                pass  # Never emit success or physical release from cleanup.


def main() -> int:
    parser = layout_parser()
    parser.add_argument("--control-fd", type=int, required=True)
    args = parser.parse_args()
    if args.control_fd < 3:
        raise ValueError("native broker control descriptor is invalid")
    with socket.socket(fileno=args.control_fd) as channel:
        return run_native_runtime_broker(
            NativeRunscLayout(args.runsc, args.state_root, args.bundle_root, args.claim_digest),
            channel=channel, expected_parent_pid=args.expected_parent)


if __name__ == "__main__":
    raise SystemExit(main())
