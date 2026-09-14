"""Disposable Kubernetes readiness is schedulable and reports only safe states."""

import json
from types import SimpleNamespace

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError
from tests.integration import test_personal_dev_storage_running_pods as fixture


def test_disposable_storage_keeps_memory_and_inode_guards_with_absolute_disk_headroom():
    args = fixture._RUNNING_SERVER_ARGS
    values = [item.removeprefix("--kubelet-arg=eviction-hard=") for item in args if item.startswith("--kubelet-arg=eviction-hard=")]
    assert len(values) == 1
    assert set(values[0].split(",")) == {"memory.available<100Mi", "nodefs.available<1Gi", "imagefs.available<1Gi",
        "nodefs.inodesFree<5%", "imagefs.inodesFree<5%"}


async def test_ready_node_with_disk_pressure_is_not_a_schedulable_fixture(monkeypatch):
    calls = []

    async def run(argv, **kwargs):
        calls.append(argv)
        conditions = [{"type": name, "status": "True" if name == "Ready" or (name == "DiskPressure" and len(calls) == 1) else "False"}
            for name in ("Ready", "DiskPressure", "MemoryPressure", "PIDPressure")]
        return SimpleNamespace(stdout=json.dumps({"items": [{"status": {"conditions": conditions}}]}))

    async def tick(_delay):
        return None

    monkeypatch.setattr(fixture.asyncio, "sleep", tick)
    client = SimpleNamespace(runner=SimpleNamespace(run=run), _argv=lambda *args: ("kubectl", *args))
    await fixture._wait_ready_nodes(client, timeout_seconds=1)
    assert len(calls) == 2


async def test_readiness_timeout_never_reports_untrusted_error_text():
    async def run(*args, **kwargs):
        raise DevInstanceRuntimeError("private-credential-sentinel")

    client = SimpleNamespace(runner=SimpleNamespace(run=run), _argv=lambda *args: ("kubectl", *args))
    with pytest.raises(TimeoutError) as raised:
        await fixture._wait_ready_nodes(client, timeout_seconds=0.01)
    notes = str(getattr(raised.value, "__notes__", []))
    assert "read-failed" in notes and "private-credential-sentinel" not in notes
