"""Runtime cleanup never substitutes failed reads for absence or releases capacity."""

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_capacity_executor.native_runsc import NativeRunscLayout


@pytest.mark.parametrize("boundary", ["complete", "already-absent", "broker-live", "read-failed",
    "unknown-id", "still-running", "delete-failed", "leftover"])
def test_exact_cleanup_requires_reaped_broker_and_confirmed_terminal_runtime(monkeypatch, boundary):
    from loom_capacity_executor import native_runtime_cleanup as module

    layout = NativeRunscLayout(Path("/runtime/runsc"), Path("/private/state"), Path("/private/bundles"), "a" * 64)
    events = []
    clock = [1_000_000_000]
    def control(_layout, operation, role):
        events.append((operation, role))
        if operation == "delete":
            return subprocess.CompletedProcess([], 1 if boundary == "delete-failed" else 0, b"")
        if boundary == "read-failed":
            return subprocess.CompletedProcess([], 1, b"")
        if boundary == "unknown-id":
            observed = [{"id": "foreign", "status": "stopped"}]
        elif boundary == "still-running":
            clock[0] += 11_000_000_000
            observed = [{"id": layout.identity("pause"), "status": "running"}]
        elif boundary == "already-absent" or (len(events) > 1 and boundary != "leftover"):
            observed = []
        else:
            observed = [{"id": layout.identity(role), "status": "stopped"} for role in ("pause", "buildkit", "client")]
        return subprocess.CompletedProcess([], 0, json.dumps(observed).encode())
    monkeypatch.setattr(module, "_control", control)
    monkeypatch.setattr(module.time, "clock_gettime_ns", lambda _clock_id: clock[0])
    broker = SimpleNamespace(poll=lambda: None if boundary == "broker-live" else -9, wait=lambda **_kwargs: -9)
    result = module.reconcile_native_runtime_cleanup(layout, broker_process=broker)
    assert result.confirmed is (boundary in {"complete", "already-absent"})
    if boundary == "broker-live":
        assert events == []
    elif boundary in {"read-failed", "unknown-id", "still-running"}:
        assert all(operation == "list" for operation, _ in events)
    elif boundary == "delete-failed":
        assert [role for operation, role in events if operation == "delete"] == ["client"]
    else:
        assert [role for operation, role in events if operation == "delete"] == ["client", "buildkit", "pause"]
