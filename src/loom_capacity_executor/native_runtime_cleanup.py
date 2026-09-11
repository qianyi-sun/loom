"""Reconcile one stopped broker's runtime; never physical capacity-release proof."""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass

from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_executor.native_runtime_broker import _control


@dataclass(frozen=True, slots=True)
class NativeRuntimeCleanupResult:
    """Local runtime state only; manager terminal/release remains mandatory."""

    confirmed: bool
    reason: str


def _states(layout: NativeRunscLayout) -> dict[str, str]:
    result = _control(layout, "list", "pause")
    if result.returncode != 0:
        raise ValueError("native runtime inventory is unavailable")
    document = json.loads(result.stdout)
    if document is None:  # Pinned runsc serializes its empty list as null.
        document = []
    if not isinstance(document, list) or len(document) > 3:
        raise ValueError("native runtime inventory is invalid")
    expected = {layout.identity(role) for role in ("pause", "buildkit", "client")}
    states: dict[str, str] = {}
    for entry in document:
        if (not isinstance(entry, dict) or not isinstance(entry.get("id"), str)
            or entry["id"] not in expected or entry["id"] in states
            or not isinstance(entry.get("status"), str)
            or entry["status"] not in {"creating", "created", "running", "stopped", "paused"}):
            raise ValueError("native runtime inventory ownership or state changed")
        states[entry["id"]] = entry["status"]
    return states


def reconcile_native_runtime_cleanup(layout: NativeRunscLayout, *,
    broker_process: subprocess.Popen[bytes],
) -> NativeRuntimeCleanupResult:
    """Confirm stopped/absent state before single exact deletes and final readback.

    Call outside the live permission monitor, under the same preverified material
    and one-shot workspace. Its broker must be dead/reaped and kernel binding
    must prevent any late start. Runtime readback gets a ten-second observation
    window; individual control/delete commands retain their bounded IO timeouts.
    Unknown inventory, failed reads/deletes or remaining state retain uncertainty.
    No mutation retry, recursive filesystem deletion or release acknowledgment.
    """
    if broker_process.poll() is None:
        return NativeRuntimeCleanupResult(False, "broker-live")
    try:
        broker_process.wait(timeout=0)
        layout.__post_init__()
        until = time.clock_gettime_ns(time.CLOCK_BOOTTIME) + 10_000_000_000
        while True:
            states = _states(layout)
            if all(value == "stopped" for value in states.values()):
                break
            if time.clock_gettime_ns(time.CLOCK_BOOTTIME) >= until:
                return NativeRuntimeCleanupResult(False, "runtime-still-live")
            time.sleep(0.05)
        for role in ("client", "buildkit", "pause"):
            if _control(layout, "delete", role).returncode != 0:
                return NativeRuntimeCleanupResult(False, "delete-failed")
        if _states(layout):
            return NativeRuntimeCleanupResult(False, "runtime-state-remains")
        return NativeRuntimeCleanupResult(True, "runtime-empty")
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired):
        return NativeRuntimeCleanupResult(False, "cleanup-uncertain")
