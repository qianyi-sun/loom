"""Native worker identity crosses exec only through a bounded sealed handoff."""

import hashlib
import os
from datetime import timedelta
from importlib import import_module

import pytest

from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffStore, consume_bootstrap_handoff
from tests.unit.test_capacity_executor_bootstrap_handoff import _NOW, _Admission, _physical
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


async def prepared_worker(tmp_path):
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    lease = BootstrapHandoffStore(directory).prepare(binding, bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5), trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256)
    admission, physical = _Admission(), _physical(binding)
    credential = await consume_bootstrap_handoff(directory, lease.reference, physical, admission, now=lambda: _NOW)
    return directory, lease, physical, admission, credential


async def test_native_launch_claim_returns_exact_registered_worker_once(tmp_path):
    module = import_module("loom_capacity_executor.bootstrap_handoff")
    directory, lease, physical, admission, credential = await prepared_worker(tmp_path)
    launched = module.claim_bootstrap_handoff_worker(directory, lease.reference, physical, admission, now=lambda: _NOW)
    assert launched.registration == admission.requests[0]
    assert launched.physical == physical
    assert launched.worker_credential == credential
    assert credential not in repr(launched)
    assert not (directory / lease.reference).with_suffix(".credential").exists()
    with pytest.raises(RuntimeError, match="already"):
        module.claim_bootstrap_handoff_worker(directory, lease.reference, physical, admission, now=lambda: _NOW)


def packet_for(physical, registration, credential, tmp_path):
    module = import_module("loom_capacity_executor.native_worker_handoff")
    binding = physical.binding
    return module.NativeWorkerHandoffV1(registration=registration, physical=physical,
        worker_credential=credential, admission={"path": str(tmp_path / "admission.json"), "sha256": "f" * 64},
        executor={"pool_id": binding.pool_id, "pool_generation": binding.pool_generation,
            "executor_id": binding.executor_id, "executor_incarnation": str(binding.executor_incarnation)})


@pytest.mark.parametrize("boundary", ["exact", "credential", "physical", "executor"])
async def test_native_packet_validates_identity_and_sealed_transfer(tmp_path, boundary):
    from loom_capacity_manager.contracts import canonical_bytes

    _directory, _lease, physical, admission, credential = await prepared_worker(tmp_path)
    packet = packet_for(physical, admission.requests[0], credential, tmp_path)
    module = import_module("loom_capacity_executor.native_worker_handoff")
    changed = {}
    if boundary == "credential":
        changed["worker_credential"] = "x" * 43
    elif boundary == "physical":
        changed["physical"] = physical.model_copy(update={"slurm_job_id": "999"})
    elif boundary == "executor":
        changed["executor"] = packet.executor.model_copy(update={"pool_generation": packet.executor.pool_generation + 1})
    packet = packet.model_copy(update=changed)
    if boundary != "exact":
        with pytest.raises(ValueError):
            with module.sealed_native_worker_handoff(packet):
                pytest.fail("invalid handoff became inheritable")
        return
    assert credential not in repr(packet)
    with module.sealed_native_worker_handoff(packet) as descriptor:
        assert os.get_inheritable(descriptor)
        with pytest.raises(OSError):
            os.write(descriptor, b"!")
        assert os.pread(descriptor, 65537, 0) == canonical_bytes(packet)
        received_fd = os.dup(descriptor)
        assert module.consume_native_worker_handoff(received_fd) == packet
        with pytest.raises(OSError):
            os.fstat(received_fd)
    with pytest.raises(OSError):
        os.fstat(descriptor)
    assert packet.registration.worker_credential_sha256 == hashlib.sha256(credential.encode("ascii")).hexdigest()
