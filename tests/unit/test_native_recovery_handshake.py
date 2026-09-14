"""Mapped material must wait for exact committed recovery acknowledgment."""

import asyncio
import hashlib
import os
import socket
from importlib import import_module

import pytest

from loom_capacity_agent.native_recovery_publication import (
    NativeRecoveryPublicationV1,
    NativeRecoveryReceiptV1,
)
from loom_capacity_executor.native_identity_mapping import NativeIdentityRange, NativeMappedIdentity
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_native_material_launch import material_spec
from tests.unit.test_native_recovery_contracts import observation


def recovery_spec(tmp_path):
    runtime, previous, path, _digest = material_spec(tmp_path)
    _contracts, final = observation()
    metadata = tmp_path.stat()
    locator = final.preparation.locator.model_copy(update={"physical": final.preparation.locator.physical.model_copy(update={"binding": previous.claim.binding}),
        "worker_id": previous.claim.worker_id, "worker_incarnation": previous.claim.worker_incarnation,
        "directory": str(tmp_path), "device": metadata.st_dev, "inode": metadata.st_ino})
    preparation = final.preparation.model_copy(update={"locator": locator, "node_id": previous.claim.binding.node_ids[0]})
    document = previous.model_dump(mode="json")
    document.update(schema_version=3, recovery_preparation=preparation.model_dump(mode="json"))
    import json

    spec = runtime.NativeRootlessSpecV3.model_validate_json(json.dumps(document))
    from loom_capacity_manager.executable_contracts import canonical_executable_bytes

    path.chmod(0o600)
    path.write_bytes(canonical_executable_bytes(spec))
    path.chmod(0o400)
    return runtime, spec, path, hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("boundary", ["exact", "publication-fails", "changed-receipt", "directory", "mapping", "directory-after", "cancel", "owner", "owner-after"])
async def test_mapped_handshake_waits_for_commit_without_credentials(tmp_path, monkeypatch, boundary):
    module = import_module("loom_capacity_executor.native_recovery_handshake")
    runtime, spec, path, digest = recovery_spec(tmp_path)
    assert runtime.read_native_rootless_spec(path, expected_sha256=digest) == spec
    prepared = spec.recovery_preparation
    mappings = NativeMappedIdentity((NativeIdentityRange(0, prepared.original_uid, 1), NativeIdentityRange(1, 100000, 65536)),
        (NativeIdentityRange(0, prepared.original_gid, 1), NativeIdentityRange(1, 200000, 65536)))
    if boundary == "mapping":
        mappings = NativeMappedIdentity((NativeIdentityRange(0, prepared.original_uid + 1, 1),), mappings.gid_ranges)
    monkeypatch.setattr(module, "observe_native_mapped_identity", lambda: mappings)
    real_fstat = os.fstat
    ownership_changed = [boundary == "owner"]

    def observed_fstat(descriptor):
        metadata = real_fstat(descriptor)
        if ownership_changed[0] and metadata.st_ino == prepared.locator.inode and metadata.st_dev == prepared.locator.device:
            fields = list(metadata)
            fields[4] = metadata.st_uid + 1
            return os.stat_result(fields)
        return metadata

    monkeypatch.setattr(module.os, "fstat", observed_fstat)
    if boundary == "directory":
        spec = spec.model_copy(update={"recovery_preparation": prepared.model_copy(update={"locator": prepared.locator.model_copy(update={"inode": 1})})})
    reached, release = asyncio.Event(), asyncio.Event()
    requests = []

    async def publish(request):
        requests.append(request)
        assert request.claim == spec.claim and request.record.preparation == prepared
        reached.set()
        await release.wait()
        if boundary == "publication-fails":
            raise OSError("unavailable")
        receipt = NativeRecoveryReceiptV1(request=request, request_digest=canonical_digest(request))
        return receipt.model_copy(update={"request_digest": "f" * 64}) if boundary == "changed-receipt" else receipt

    outer, mapped = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        mapping_task = asyncio.create_task(asyncio.to_thread(module.acknowledge_mapped_recovery, mapped, spec=spec, runtime_spec_sha256=digest))
        if boundary in {"directory", "mapping", "owner"}:
            with pytest.raises((ValueError, RuntimeError)):
                await asyncio.wait_for(mapping_task, 3)
            assert requests == []
            return
        publication_task = asyncio.create_task(module.commit_mapped_recovery(outer, claim=spec.claim,
            preparation=prepared, runtime_spec_sha256=digest, publish=publish))
        await asyncio.wait_for(reached.wait(), 3)
        assert not mapping_task.done(), "mapped runtime advanced before commit acknowledgment"
        if boundary == "directory-after":
            from pathlib import Path

            attempt = Path(prepared.locator.directory)
            attempt.rename(attempt.with_name(attempt.name + "-retained"))
            attempt.mkdir(mode=0o700)
        if boundary == "cancel":
            publication_task.cancel()
        if boundary == "owner-after":
            ownership_changed[0] = True
        release.set()
        if boundary in {"exact", "directory-after", "owner-after"}:
            result = await asyncio.wait_for(publication_task, 3)
            if boundary in {"directory-after", "owner-after"}:
                with pytest.raises(ValueError, match="changed during publication"):
                    await asyncio.wait_for(mapping_task, 3)
            else:
                assert await asyncio.wait_for(mapping_task, 3) == result == canonical_digest(requests[0])
            assert isinstance(requests[0], NativeRecoveryPublicationV1)
        else:
            with pytest.raises((ValueError, OSError, asyncio.CancelledError)):
                await asyncio.wait_for(publication_task, 3)
            outer.close()
            with pytest.raises((ValueError, RuntimeError)):
                await asyncio.wait_for(mapping_task, 3)
    finally:
        release.set()
        outer.close()
        mapped.close()


@pytest.mark.parametrize("phase", ["preparation", "finalization"])
async def test_recovery_write_ambiguity_never_reopens_scoped_attempt(tmp_path, monkeypatch, phase):
    from loom_capacity_executor.native_allocated_io import scoped_native_allocated_io
    from loom_capacity_executor.native_build_source import NativeStagedBuildSource

    module = import_module("loom_capacity_executor.native_allocated_io")
    _runtime, spec, _path, digest = recovery_spec(tmp_path)
    calls = []

    class Client:
        async def publish_recovery(self, request, **kwargs):
            calls.append("preparation")
            if phase == "preparation":
                raise OSError("lost reply")
            return NativeRecoveryReceiptV1(request=request, request_digest=canonical_digest(request))

    async def finalize(*args, **kwargs):
        calls.append("finalization")
        raise OSError("lost reply")

    monkeypatch.setattr(module, "commit_mapped_recovery", finalize)
    async with scoped_native_allocated_io(claim=spec.claim, source=NativeStagedBuildSource(spec.context, tmp_path / "source"),
        client=Client(), worker_credential="w" * 43) as owner:
        if phase == "finalization":
            await owner.prepare_recovery(spec.recovery_preparation)

        async def invoke():
            if phase == "preparation":
                await owner.prepare_recovery(spec.recovery_preparation)
            else:
                await owner.finalize_recovery(object(), preparation=spec.recovery_preparation, runtime_spec_sha256=digest)

        with pytest.raises(OSError):
            await invoke()
        with pytest.raises(ValueError):
            await invoke()
        assert calls.count(phase) == 1


@pytest.mark.parametrize("failed", [False, True])
def test_mapped_v3_acknowledges_before_material_and_binds_session(tmp_path, monkeypatch, failed):
    from types import SimpleNamespace

    module, spec, path, digest = recovery_spec(tmp_path)
    events = []
    authority, peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    artifact, artifact_peer = socket.socketpair()

    def acknowledge(channel, **kwargs):
        assert events == ["parent"] and channel is authority
        assert kwargs == {"spec": spec, "runtime_spec_sha256": digest}
        events.append("committed")
        if failed:
            raise ValueError("publication failed")
        return "e" * 64

    def prepare(observed):
        assert observed == spec and events == ["parent", "committed"]
        events.append("material")

    def execute(**kwargs):
        assert kwargs["recovery_finalization_sha256"] == "e" * 64
        assert events == ["parent", "committed", "material", "scratch"]
        events.append("execute")
        return SimpleNamespace(artifact=None, supervision=SimpleNamespace(
            client_succeeded=False, broker_reaped=True), cleanup=SimpleNamespace(confirmed=True))

    monkeypatch.setenv("LISTEN_PID", str(os.getpid()))
    monkeypatch.setenv("LISTEN_FDS", "2")
    monkeypatch.setattr(module, "bind_native_rootless_parent", lambda *args, **kwargs: events.append("parent") or 321)
    monkeypatch.setattr(module, "_activation_channels", lambda: (authority, artifact))
    monkeypatch.setattr(module, "acknowledge_mapped_recovery", acknowledge)
    monkeypatch.setattr(module, "prepare_native_rootless_material", prepare)
    monkeypatch.setattr(module, "capture_native_mapped_scratch", lambda spec: events.append("scratch") or object())
    monkeypatch.setattr(module, "execute_native_build_session", execute)
    monkeypatch.setattr(module, "clean_native_mapped_scratch", lambda spec: events.append("clean"))
    try:
        if failed:
            with pytest.raises(ValueError, match="publication failed"):
                module.run_native_mapped_runtime(path, expected_sha256=digest, expected_rootless_pid=123)
            assert events == ["parent", "committed"]
        else:
            module.run_native_mapped_runtime(path, expected_sha256=digest, expected_rootless_pid=123)
            assert events == ["parent", "committed", "material", "scratch", "execute", "clean"]
    finally:
        for channel in (authority, peer, artifact, artifact_peer):
            channel.close()


def test_mapped_scratch_accepts_exact_v3_and_revalidates_copies(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_mapped_scratch")
    _runtime, spec, _path, _digest = recovery_spec(tmp_path)
    from pathlib import Path

    Path(spec.workspace).mkdir(mode=0o700, exist_ok=True)
    monkeypatch.setattr(module, "_require_mapped_root", lambda: None)
    assert module.capture_native_mapped_scratch(spec).attempt.inode == tmp_path.stat().st_ino
    from types import SimpleNamespace

    # python -m creates the runtime class in __main__; importing the canonical
    # module again produces a distinct class. Validation must use the bytes.
    separate_runtime_class = SimpleNamespace(model_dump_json=spec.model_dump_json, workspace=spec.workspace)
    assert module.capture_native_mapped_scratch(separate_runtime_class).attempt.inode == tmp_path.stat().st_ino
    bad = spec.model_copy(update={"workspace": str(tmp_path / "elsewhere/work")})
    with pytest.raises(ValueError):
        module.capture_native_mapped_scratch(bad)


@pytest.mark.parametrize("boundary", ["exact", "uncommitted", "receipt", "foreign", "failure"])
async def test_allocated_io_requires_own_committed_preparation_before_finalization(tmp_path, monkeypatch, boundary):
    from loom_capacity_executor.native_allocated_io import scoped_native_allocated_io
    from loom_capacity_executor.native_build_source import NativeStagedBuildSource

    _runtime, spec, _path, digest = recovery_spec(tmp_path)
    calls = []
    module = import_module("loom_capacity_executor.native_allocated_io")
    handshake = import_module("loom_capacity_executor.native_recovery_handshake")
    _contracts, final = observation()
    final = final.model_copy(update={"preparation": spec.recovery_preparation, "runtime_spec_sha256": digest})
    publication = NativeRecoveryPublicationV1(claim=spec.claim, record=final)

    class Client:
        async def publish_recovery(self, request, *, worker_credential):
            assert worker_credential == "w" * 43
            calls.append(request)
            if boundary == "failure":
                raise OSError("commit reply unavailable")
            receipt = NativeRecoveryReceiptV1(request=request, request_digest=canonical_digest(request))
            return receipt.model_copy(update={"request_digest": "f" * 64}) if boundary == "receipt" else receipt

    async def serve(channel, **kwargs):
        assert kwargs["recovery_finalization_sha256"] == canonical_digest(publication)
        assert len(calls) == 2

    monkeypatch.setattr(module, "serve_native_execution_authority", serve)
    outer, mapped = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        async with scoped_native_allocated_io(claim=spec.claim, source=NativeStagedBuildSource(spec.context, tmp_path / "source"),
            client=Client(), worker_credential="w" * 43) as owner:
            if boundary in {"receipt", "failure"}:
                with pytest.raises((ValueError, OSError)):
                    await owner.prepare_recovery(spec.recovery_preparation)
                with pytest.raises(ValueError):
                    owner.require_recovery_preparation(spec.recovery_preparation)
                assert len(calls) == 1
                return
            if boundary != "uncommitted":
                await owner.prepare_recovery(spec.recovery_preparation)
            if boundary == "foreign":
                preparation = spec.recovery_preparation.model_copy(update={"node_configuration_sha256": "f" * 64})
            else:
                preparation = spec.recovery_preparation
            if boundary in {"uncommitted", "foreign"}:
                with pytest.raises(ValueError):
                    await owner.finalize_recovery(outer, preparation=preparation, runtime_spec_sha256=digest)
                assert len(calls) == (1 if boundary == "foreign" else 0)
                return
            from loom_capacity_executor.native_supervisor import _send

            _send(mapped, handshake.NativeRecoveryFinalize(request=publication))
            result = await owner.finalize_recovery(outer, preparation=preparation, runtime_spec_sha256=digest)
            assert result == canonical_digest(publication)
            assert handshake.NativeRecoveryAcknowledgment.model_validate_json(mapped.recv(65536)).request_digest == result
            await owner.serve_authority(outer, recovery_finalization_sha256=result)
            assert calls[0].record == preparation and calls[1] == publication
        with pytest.raises(RuntimeError, match="closed"):
            await owner.prepare_recovery(preparation)
    finally:
        outer.close()
        mapped.close()


@pytest.mark.parametrize("boundary", ["exact", "uncommitted", "final-fails"])
async def test_outer_v3_serializes_finalization_before_authority(tmp_path, monkeypatch, boundary):
    from types import SimpleNamespace

    from loom_capacity_agent.build_admission import BuildOutcomeReceiptV1

    module = import_module("loom_capacity_executor.native_outer_build")
    _runtime, spec, path, digest = recovery_spec(tmp_path)
    events = []

    class Owner:
        claim = spec.claim
        source = SimpleNamespace(context=spec.context)

        def require_recovery_preparation(self, preparation):
            assert preparation == spec.recovery_preparation
            events.append("preparation")
            if boundary == "uncommitted":
                raise ValueError("uncommitted")

        async def finalize_recovery(self, channel, **kwargs):
            assert events == ["preparation", "input", "spawn"]
            assert kwargs == {"preparation": spec.recovery_preparation, "runtime_spec_sha256": digest}
            events.append("final")
            if boundary == "final-fails":
                raise ValueError("final failed")
            return "e" * 64

        async def serve_authority(self, channel, **kwargs):
            assert "final" in events and kwargs == {"recovery_finalization_sha256": "e" * 64}
            events.append("authority")
            await asyncio.Future()

        async def record_outcome(self, request):
            assert "authority" in events
            return BuildOutcomeReceiptV1(request=request, request_digest=canonical_digest(request))

    async def prepare(*args, **kwargs):
        events.append("input")

    async def spawn(*args):
        events.append("spawn")
        return object()

    async def result(*args):
        return SimpleNamespace(broker_reaped=True, cleanup_confirmed=True, client_succeeded=False, artifact=None)

    async def receive(*args):
        return None

    async def stop(*args):
        events.append("stop")

    monkeypatch.setattr(module, "prepare_native_runtime_input", prepare)
    monkeypatch.setattr(module, "_spawn", spawn)
    monkeypatch.setattr(module, "_result", result)
    monkeypatch.setattr(module, "_receive", receive)
    monkeypatch.setattr(module, "_stop_process", stop)
    if boundary == "exact":
        await module.run_native_outer_build(Owner(), spec_path=path, expected_sha256=digest,
            artifact_workspace=tmp_path / "artifacts", timeout_seconds=3)
        assert events == ["preparation", "input", "spawn", "final", "authority", "stop"]
    else:
        with pytest.raises(ValueError):
            await module.run_native_outer_build(Owner(), spec_path=path, expected_sha256=digest,
                artifact_workspace=tmp_path / "artifacts", timeout_seconds=3)
        assert events == (["preparation"] if boundary == "uncommitted" else ["preparation", "input", "spawn", "final", "stop"])
