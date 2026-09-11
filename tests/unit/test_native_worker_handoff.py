"""Native worker identity crosses exec only through a bounded sealed handoff."""

import hashlib
import os
from datetime import timedelta
from importlib import import_module

import pytest

from loom_capacity_executor.bootstrap_handoff import (
    BootstrapHandoffStore,
    consume_bootstrap_handoff,
)
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
            "executor_id": binding.executor_id, "executor_incarnation": binding.executor_incarnation})


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


@pytest.mark.parametrize("boundary", ["unsealed", "mode", "empty", "oversize", "noncanonical", "invalid-secret"])
async def test_native_handoff_rejection_closes_fd_without_disclosing_input(tmp_path, boundary):
    import json

    from loom_capacity_executor.trusted_launcher import (
        _create_candidate_snapshot_descriptor,
        _seal_candidate_snapshot,
        _write_all,
    )
    from loom_capacity_manager.contracts import canonical_bytes

    _directory, _lease, physical, admission, credential = await prepared_worker(tmp_path)
    packet = packet_for(physical, admission.requests[0], credential, tmp_path)
    module = import_module("loom_capacity_executor.native_worker_handoff")
    wire = canonical_bytes(packet)
    if boundary == "empty":
        wire = b""
    elif boundary == "oversize":
        wire = b"x" * 65537
    elif boundary == "noncanonical":
        wire += b" "
    elif boundary == "invalid-secret":
        payload = json.loads(wire)
        payload["worker_credential"] = "private-invalid-secret\n" * 3
        wire = json.dumps(payload).encode()
    descriptor = _create_candidate_snapshot_descriptor()
    os.fchmod(descriptor, 0o644 if boundary == "mode" else 0o600)
    _write_all(descriptor, wire)
    if boundary != "unsealed":
        _seal_candidate_snapshot(descriptor)
    with pytest.raises((ValueError, OSError)) as failure:
        module.consume_native_worker_handoff(descriptor)
    assert "private-invalid-secret" not in str(failure.value)
    assert credential not in str(failure.value)
    with pytest.raises(OSError):
        os.fstat(descriptor)


@pytest.mark.parametrize("boundary", ["purpose", "exec-error", "exec-return"])
async def test_native_exec_boundary_preserves_authority_and_closes_handoff(tmp_path, boundary):
    from loom_capacity_executor import trusted_launcher as launcher
    from loom_capacity_executor.native_worker_handoff import NATIVE_WORKER_HANDOFF_ENV

    directory, lease, physical, admission, credential = await prepared_worker(tmp_path)
    packet = packet_for(physical, admission.requests[0], credential, tmp_path)
    config = launcher.NativeTrustedLauncherConfigV3(handoff_directory=str(directory),
        admission_directory=packet.admission.path, admission_directory_sha256=packet.admission.sha256,
        executor=packet.executor, candidate_executable={"path": "/opt/loom/native-worker", "sha256": "a" * 64,
            "owner_uid": os.geteuid(), "mode": 0o555},
        candidate_image_digest="registry.example/native@sha256:" + "a" * 64,
        candidate_argv=("/opt/loom/native-worker",))
    admission.purpose = lambda binding: "application-worker" if boundary == "purpose" else "personal-build-worker"
    descriptors = []
    def execute(file, argv, environment):
        assert boundary != "purpose"
        assert credential not in repr(environment)
        descriptors.append(int(environment[NATIVE_WORKER_HANDOFF_ENV]))
        if boundary == "exec-error":
            raise OSError("exec failed")
    with pytest.raises((RuntimeError, OSError)):
        await launcher.exec_bootstrap_handoff_candidate(directory, lease.reference, physical, admission,
            candidate_argv=config.candidate_argv, native_config=config, now=lambda: _NOW,
            environment={"LOOM_SECRET": "not-inherited"}, execvpe=execute)
    if boundary == "purpose":
        assert descriptors == []
        assert (directory / lease.reference).with_suffix(".credential").exists()
    else:
        assert not (directory / lease.reference).with_suffix(".credential").exists()
        assert (directory / lease.reference).with_suffix(".launched").exists()
        with pytest.raises(OSError):
            os.fstat(descriptors[0])
        with pytest.raises(RuntimeError, match="already"):
            await launcher.exec_bootstrap_handoff_candidate(directory, lease.reference, physical, admission,
                candidate_argv=config.candidate_argv, native_config=config, now=lambda: _NOW, execvpe=execute)


async def test_native_sealed_handoff_survives_real_exec_and_is_consumed(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    from loom_capacity_executor.native_worker_handoff import sealed_native_worker_handoff
    from loom_capacity_manager.contracts import canonical_digest

    _directory, _lease, physical, admission, credential = await prepared_worker(tmp_path)
    packet = packet_for(physical, admission.requests[0], credential, tmp_path)
    program = """
import os, sys
sys.path.insert(0, sys.argv[2])
from loom_capacity_executor.native_worker_handoff import consume_native_worker_handoff
from loom_capacity_manager.contracts import canonical_digest
descriptor = int(sys.argv[1])
packet = consume_native_worker_handoff(descriptor)
try:
    os.fstat(descriptor)
except OSError:
    pass
else:
    raise AssertionError('consumed descriptor remains open')
print(canonical_digest(packet))
"""
    with sealed_native_worker_handoff(packet) as descriptor:
        # Do not use pass_fds: it would force inheritance and hide a missing
        # production set_inheritable call. A real exec must respect FD_CLOEXEC.
        result = subprocess.run([sys.executable, "-c", program, str(descriptor),
            str(Path(__file__).resolve().parents[2] / "src")], close_fds=False,
            env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == canonical_digest(packet)
    assert credential not in result.stdout + result.stderr
