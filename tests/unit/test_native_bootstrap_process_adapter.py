"""Actual child processes exercise fixed argv, bounded IO and cleanup ownership.

Interpreter provenance and the admission-backed receiver are substituted here;
the installed-image and receiver entrypoint suites exercise those boundaries.
"""

import asyncio
import os
import sys
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2
from loom_capacity_executor.trusted_launcher import (
    TrustedCandidateExecutableV2,
    _create_candidate_snapshot_descriptor,
    _seal_candidate_snapshot,
    _write_all,
)
from tests.unit.test_native_bootstrap_delivery import delivery as delivery
from tests.unit.test_native_bootstrap_delivery import objects


@pytest.fixture
def process_fixture(delivery, monkeypatch, request):
    module = import_module("loom_capacity_executor.native_bootstrap_process_adapter")
    storage, payload, _receiver = objects(delivery)
    expected = storage.expected_native_delivery_receipt(payload)
    descriptor = _create_candidate_snapshot_descriptor()
    _write_all(descriptor, Path(sys.executable).resolve(strict=True).read_bytes())
    os.fchmod(descriptor, 0o500)
    _seal_candidate_snapshot(descriptor)
    request.addfinalizer(lambda: os.close(descriptor))
    monkeypatch.setattr(module, "_assert_private_process", lambda: None)
    monkeypatch.setattr(module, "_open_verified_candidate", lambda identity: os.dup(descriptor))
    policy = module.NativeBootstrapReceiverProcessPolicy(
        interpreter=TrustedCandidateExecutableV2(path=sys.executable, sha256="a" * 64, owner_uid=os.geteuid(), mode=0o555),
        configuration=SlurmFileIdentityV2(path="/operator/pinned/receiver.json", sha256="b" * 64, owner_uid=os.geteuid()),
        timeout_seconds=3.0, cleanup_wait_seconds=0.2)
    state = SimpleNamespace(mode="success", processes=[], calls=[], created=asyncio.Event(),
        release=asyncio.Event(), delay_handoff=False, descriptors=[])
    spawn = asyncio.create_subprocess_exec

    async def execute(*argv, **kwargs):
        state.calls.append(argv)
        assert argv[:5] == (sys.executable, "-I", "-B", "-m", "loom_capacity_executor.native_bootstrap_receiver")
        assert argv[5:] == ("--configuration", policy.configuration.path, "--configuration-sha256", "b" * 64,
            "--configuration-owner-uid", str(os.geteuid()), "--operation", argv[-1])
        assert argv[-1] in {"deliver", "status"}
        assert kwargs["env"] == {} and kwargs["start_new_session"] is True
        assert kwargs["stdin"] == kwargs["stdout"] == kwargs["stderr"] == asyncio.subprocess.PIPE
        assert kwargs["executable"] == f"/proc/self/fd/{kwargs['pass_fds'][0]}"
        state.descriptors.extend(kwargs["pass_fds"])
        assert payload.decode() not in repr(argv) + repr(kwargs)
        receipt = storage._canonical(expected) + b"\n"
        if state.mode == "hang":
            body = "import sys, time; sys.stdin.buffer.read(); time.sleep(60)"
        elif state.mode == "overflow":
            body = "import sys; sys.stdin.buffer.read(); sys.stdout.write('private-output' * 100000); sys.stdout.flush()"
        elif state.mode == "stderr":
            body = f"import sys; sys.stdin.buffer.read(); sys.stderr.write('private-error'); sys.stdout.buffer.write({receipt!r})"
        elif state.mode == "unknown":
            body = "import sys; sys.stdin.buffer.read(); sys.stdout.buffer.write(b'null\\n')"
        else:
            body = f"import sys; assert sys.stdin.buffer.read(); sys.stdout.buffer.write({receipt!r})"
        process = await spawn(sys.executable, "-I", "-B", "-c", body, **kwargs)
        state.processes.append(process)
        state.created.set()
        if state.delay_handoff:
            await state.release.wait()
        return process

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", execute)
    return SimpleNamespace(module=module, policy=policy, storage=storage, payload=payload,
        expected=expected, state=state, physical=delivery.physical)


async def test_fixed_child_success_reaps_and_closes_pinned_descriptor(process_fixture):
    value = process_fixture
    adapter = value.module.NativeBootstrapProcessAdapter(value.policy)
    try:
        assert await adapter.receive(value.payload) == value.expected
        assert all(process.returncode == 0 for process in value.state.processes)
        assert adapter.active_operations == 0
    finally:
        await adapter.aclose()
    for descriptor in value.state.descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("mode", ("hang", "overflow", "stderr", "unknown"))
async def test_failed_child_never_returns_success_or_secret_details(process_fixture, mode):
    value = process_fixture
    value.state.mode = mode
    adapter = value.module.NativeBootstrapProcessAdapter(value.policy)
    try:
        with pytest.raises(ValueError, match="unavailable or refused") as caught:
            await asyncio.wait_for(adapter.receive(value.payload), 5)
        assert "private-output" not in str(caught.value) and "private-error" not in str(caught.value)
    finally:
        await asyncio.wait_for(adapter.aclose(), 5)
    assert all(process.returncode is not None for process in value.state.processes)
    assert adapter.active_operations == 0


async def test_status_unknown_is_not_delivery_success(process_fixture):
    value = process_fixture
    value.state.mode = "unknown"
    query = value.storage.encode_native_delivery_query(value.physical, value.expected)
    adapter = value.module.NativeBootstrapProcessAdapter(value.policy)
    try:
        assert await adapter.observe_receipt(query) is None
    finally:
        await adapter.aclose()


async def test_cancellation_during_spawn_handoff_retains_cleanup_until_child_reaped(process_fixture):
    value = process_fixture
    value.state.mode, value.state.delay_handoff = "hang", True
    adapter = value.module.NativeBootstrapProcessAdapter(value.policy)
    task = asyncio.create_task(adapter.receive(value.payload))
    try:
        await asyncio.wait_for(value.state.created.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert adapter.active_operations > 0
        assert value.state.processes[0].returncode is None
        value.state.release.set()
        await asyncio.wait_for(adapter.aclose(), 5)
        assert value.state.processes[0].returncode is not None
        assert adapter.active_operations == 0
    finally:
        value.state.release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await adapter.aclose()


async def test_closed_adapter_never_starts_another_child(process_fixture):
    value = process_fixture
    adapter = value.module.NativeBootstrapProcessAdapter(value.policy)
    await adapter.aclose()
    with pytest.raises(ValueError):
        await adapter.receive(value.payload)
    assert value.state.calls == []
