"""Mapped material must wait for exact committed recovery acknowledgment."""

import asyncio
import hashlib
import socket
from importlib import import_module

import pytest

from loom_capacity_agent.native_recovery_publication import NativeRecoveryPublicationV1, NativeRecoveryReceiptV1
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


@pytest.mark.parametrize("boundary", ["exact", "publication-fails", "changed-receipt", "directory", "mapping"])
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
        if boundary in {"directory", "mapping"}:
            with pytest.raises((ValueError, RuntimeError)):
                await asyncio.wait_for(mapping_task, 3)
            assert requests == []
            return
        publication_task = asyncio.create_task(module.commit_mapped_recovery(outer, claim=spec.claim,
            preparation=prepared, runtime_spec_sha256=digest, publish=publish))
        await asyncio.wait_for(reached.wait(), 3)
        assert not mapping_task.done(), "mapped runtime advanced before commit acknowledgment"
        release.set()
        if boundary == "exact":
            result = await asyncio.wait_for(publication_task, 3)
            assert await asyncio.wait_for(mapping_task, 3) == result == canonical_digest(requests[0])
            assert isinstance(requests[0], NativeRecoveryPublicationV1)
        else:
            with pytest.raises((ValueError, OSError)):
                await asyncio.wait_for(publication_task, 3)
            outer.close()
            with pytest.raises((ValueError, RuntimeError)):
                await asyncio.wait_for(mapping_task, 3)
    finally:
        release.set()
        outer.close()
        mapped.close()
