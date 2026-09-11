"""The installed entrypoint composes existing authority without leaking handoffs."""

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom_capacity_agent.build_admission import BuildOutcomeReceiptV1, BuildOutcomeRequestV1
from loom_capacity_executor.native_worker_handoff import sealed_native_worker_handoff
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_native_sandbox_consumer import bound_context
from tests.unit.test_native_worker_handoff import packet_for, prepared_worker
from tests.unit.test_personal_dev_builder import _registration


@pytest.mark.parametrize("boundary", ["exact", "config", "release", "binding", "scope", "claim", "runtime", "cancel", "replay"])
async def test_installed_worker_closes_handoff_before_checks_and_retains_recovery_identity(tmp_path, monkeypatch, boundary):
    module = import_module("loom_capacity_executor.native_installed_worker")
    _directory, _lease, physical, admission, credential = await prepared_worker(tmp_path)
    packet = packet_for(physical, admission.requests[0], credential, tmp_path)
    scratch = tmp_path / "scratch"
    scratch.mkdir(mode=0o700)
    config = module.NativeInstalledWorkerConfigV1(release_manifest="/protected/release.json",
        release_manifest_sha256="a" * 64, tooling_source_sha="b" * 40,
        trusted_fleet_release_sha256=physical.binding.execution.trusted_fleet_release_sha256,
        platform="linux/amd64", scratch_root=str(scratch), max_source_archive_bytes=1024**2,
        max_artifact_bytes=2 * 1024**2, max_image_archive_bytes=1024**2,
        max_unpacked_bytes=32 * 1024**2, max_rootfs_entries=1000, tmp_bytes=1024**2,
        buildkit_state_bytes=32 * 1024**2, timeout_seconds=60)
    claim = module.allocated_claim_request(packet.registration).model_dump()
    from loom_capacity_agent.build_admission import BuildClaimRequestV1
    claim = BuildClaimRequestV1.model_validate({**claim, "request_id": uuid4()})
    context = bound_context(_registration(), "oldlab").model_copy(update={
        "claim_digest": canonical_digest(claim), "request_id": claim.request_id})
    receipt = BuildOutcomeReceiptV1(request=BuildOutcomeRequestV1(claim=claim,
        operation_id=uuid4(), result="failed", artifact=None), request_digest="a" * 64)
    calls = []
    inherited = None

    def checked(name):
        with pytest.raises(OSError):
            os.fstat(inherited)
        calls.append(name)
        if boundary == name:
            raise ValueError("fixture " + name)

    def load(*args, **kwargs):
        checked("config")
        return config

    monkeypatch.setattr(module, "read_installed_worker_config", load)
    profile = Path(__file__).parents[2] / "deploy/personal-dev-builder/client-seccomp-v1.json"
    seccomp = profile.read_bytes()
    manifest = SimpleNamespace(rootfs="/protected/rootfs.tar", seccomp="/protected/seccomp.json",
        runsc_root="/protected/gvisor", files=[SimpleNamespace(path="/protected/rootfs.tar",
            sha256="c" * 64, size_bytes=512)])

    def verify(*args, **kwargs):
        checked("release")
        assert kwargs["expected_source_sha"] == config.tooling_source_sha
        return SimpleNamespace(manifest=manifest, client_seccomp=seccomp)

    monkeypatch.setattr(module, "verify_native_installed_release", verify)
    monkeypatch.setattr(module, "_bind_running_installation", lambda *args: checked("binding"))
    monkeypatch.setattr(module, "validate_native_worker_scope", lambda *args, **kwargs: checked("scope"))

    @asynccontextmanager
    async def io(observed_packet, **kwargs):
        checked("claim")
        assert observed_packet == packet
        assert kwargs["job_id"] == physical.slurm_job_id
        assert kwargs["max_archive_bytes"] == 1024**2
        attempt = kwargs["workspace"].parent
        identity = module.NativeInstalledAttemptV1.model_validate_json((attempt / "recovery.json").read_bytes())
        assert identity.physical == physical
        assert identity.release_manifest_sha256 == config.release_manifest_sha256
        assert (identity.device, identity.inode) == (attempt.stat().st_dev, attempt.stat().st_ino)
        assert credential not in (attempt / "recovery.json").read_text()
        assert (attempt / "recovery.json").stat().st_mode & 0o777 == 0o400
        try:
            yield SimpleNamespace(claim=claim, source=SimpleNamespace(context=context))
        finally:
            calls.append("io-exit")

    monkeypatch.setattr(module, "allocated_native_packet_io", io)

    async def outer(owner, *, spec_path, expected_sha256, artifact_workspace, timeout_seconds):
        checked("runtime")
        spec = module.read_native_rootless_spec(spec_path, expected_sha256=expected_sha256)
        assert spec.claim == claim and spec.context == context
        assert spec.material.archive == manifest.rootfs and spec.material.archive_size_bytes == 512
        assert spec.material.client_seccomp_sha256 == hashlib.sha256(seccomp).hexdigest()
        assert spec.runsc == "/protected/gvisor/runsc"
        assert artifact_workspace.name == "artifacts" and timeout_seconds == 60
        if boundary == "cancel":
            raise asyncio.CancelledError
        return receipt

    monkeypatch.setattr(module, "run_native_outer_build", outer)
    with sealed_native_worker_handoff(packet) as descriptor:
        inherited = os.dup(descriptor)
        kwargs = dict(config_path=tmp_path / "config.json", config_sha256="d" * 64,
            job_id=physical.slurm_job_id)
        if boundary in {"exact", "replay"}:
            assert await module.execute_installed_native_worker(inherited, **kwargs) == receipt
        elif boundary == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await module.execute_installed_native_worker(inherited, **kwargs)
        else:
            with pytest.raises(ValueError):
                await module.execute_installed_native_worker(inherited, **kwargs)
        with pytest.raises(OSError):
            os.fstat(inherited)
        if boundary == "replay":
            calls.clear()
            inherited = os.dup(descriptor)
            with pytest.raises(FileExistsError):
                await module.execute_installed_native_worker(inherited, **kwargs)
            assert "claim" not in calls
            return
    if boundary in {"config", "release", "binding", "scope"}:
        assert "claim" not in calls and list(scratch.iterdir()) == []
    else:
        assert len(list(scratch.iterdir())) == 1, "retain exact scratch for allocation recovery"
    if boundary in {"exact", "runtime", "cancel"}:
        assert calls[-1] == "io-exit"


@pytest.mark.parametrize("fault", ["not-isolated", "interpreter", "rootlesskit", "import-tree", "unlisted-module"])
def test_running_process_must_match_verified_installation(monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_installed_worker")
    manifest = SimpleNamespace(python="/protected/python/bin/python3", rootlesskit="/usr/bin/rootlesskit",
        python_root="/protected/python", files=[SimpleNamespace(path=module.__file__)])
    monkeypatch.setattr(module.sys, "flags", SimpleNamespace(isolated=0 if fault == "not-isolated" else 1))
    monkeypatch.setattr(module.sys, "executable", "/wrong/python" if fault == "interpreter" else manifest.python)
    monkeypatch.setattr(module.sys, "path", ["/outside" if fault == "import-tree" else "/protected/python/lib"])
    if fault == "rootlesskit":
        manifest.rootlesskit = "/wrong/rootlesskit"
    if fault == "unlisted-module":
        manifest.files = []
    with pytest.raises(ValueError):
        module._bind_running_installation(SimpleNamespace(manifest=manifest))
