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
from tests.unit.test_native_installed_release import release as release
from tests.unit.test_native_sandbox_consumer import bound_context
from tests.unit.test_native_worker_handoff import packet_for, prepared_worker
from tests.unit.test_personal_dev_builder import _registration


@pytest.fixture
def protected_helper_host(monkeypatch):
    """Model an installed host, not GitHub's intentionally runner-owned helpers.

    Keep real descriptor/path traversal and production permission checks. Each
    negative case overrides the specific metadata it intends to reject.
    """
    import stat

    original = os.fstat

    def metadata(fd):
        value = original(fd)
        if not stat.S_ISDIR(value.st_mode):
            return value
        fields = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
        fields["st_uid"] = fields["st_gid"] = 0
        fields["st_mode"] &= ~0o7022
        return SimpleNamespace(**fields)

    monkeypatch.setattr(os, "fstat", metadata)


@pytest.mark.parametrize(("recovery", "boundary"), [(recovery, boundary) for recovery in (False, True)
    for boundary in ("exact", "config", "release", "binding", "scope", "claim", "runtime", "cancel", "replay")]
    + [(True, boundary) for boundary in ("admission", "host", "capture", "publication", "admitted-config", "admitted-release", "lost-preparation-ack")])
async def test_installed_worker_closes_handoff_before_checks_and_retains_recovery_identity(tmp_path, monkeypatch, boundary, recovery):
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
    if recovery:
        from loom_capacity_build_guard.installation_store import _identity
        config = module.NativeInstalledWorkerConfigV2.model_validate({**config.model_dump(), "schema_version": 2,
            "host_identity_path": "/protected/host.json"})
    claim = module.allocated_claim_request(packet.registration).model_dump()
    from loom_capacity_agent.build_admission import BuildClaimRequestV1
    claim = BuildClaimRequestV1.model_validate({**claim, "request_id": uuid4()})
    context = bound_context(_registration(), "oldlab").model_copy(update={
        "claim_digest": canonical_digest(claim), "request_id": claim.request_id})
    receipt = BuildOutcomeReceiptV1(request=BuildOutcomeRequestV1(claim=claim,
        operation_id=uuid4(), result="failed", artifact=None), request_digest="a" * 64)
    calls = []
    inherited = None
    handoff_identity = None

    def checked(name):
        try:
            observed = os.fstat(inherited)
        except OSError:
            pass
        else:
            # A closed descriptor number may already be reused by anchored
            # scratch directories. It must not still identify the handoff.
            assert (observed.st_dev, observed.st_ino) != handoff_identity
            assert not os.get_inheritable(inherited)
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

    if recovery:
        from loom_capacity_agent.native_recovery_publication import (
            NativeRecoveryAdmissionV1,
            NativeRecoveryHostIdentityV1,
            NativeRecoveryProfileV1,
            NativeRecoveryReceiptV1,
        )
        from tests.unit.test_native_recovery_contracts import observation

        host = NativeRecoveryHostIdentityV1(node_id=physical.binding.node_ids[0], boot_id=uuid4(), original_uid=24850,
            original_gid=24851, cgroup_namespace_device=4, cgroup_namespace_inode=100)
        profile = NativeRecoveryProfileV1(installation_id=_identity(claim.binding.subject_id, claim.binding.subject_incarnation,
            claim.binding.deployment_generation), pool_id="oldlab", launch_profile_sha256="e" * 64,
            worker_config_sha256="f" * 64 if boundary == "admitted-config" else "d" * 64,
            release_manifest_sha256="f" * 64 if boundary == "admitted-release" else config.release_manifest_sha256)
        monkeypatch.setattr(module, "read_native_recovery_boot_id", lambda: host.boot_id)

        def read_host(path, *, expected_sha256):
            assert str(path) == config.host_identity_path and expected_sha256 == canonical_digest(host)
            checked("host")
            return host

        def capture(locator, **kwargs):
            checked("capture")
            assert locator.config_sha256 == "d" * 64
            assert kwargs == {"launch_profile_sha256": profile.launch_profile_sha256,
                "node_configuration_sha256": canonical_digest(host), "host_identity": host}
            _contracts, final = observation()
            return final.preparation.model_copy(update={"locator": locator, "node_id": host.node_id,
                "boot_id": host.boot_id, "launch_profile_sha256": profile.launch_profile_sha256,
                "node_configuration_sha256": canonical_digest(host)})

        monkeypatch.setattr(module, "read_native_recovery_host_identity", read_host)
        monkeypatch.setattr(module, "capture_native_recovery_preparation", capture)

        class Client:
            async def read_recovery_admission(self, request, *, worker_credential):
                checked("admission")
                assert request.claim == claim and request.boot_id == host.boot_id
                assert worker_credential == credential
                return NativeRecoveryAdmissionV1(request=request, profile=profile, host=host)

            async def publish_recovery(self, request, *, worker_credential):
                checked("publication")
                assert worker_credential == credential and request.claim == claim
                result = NativeRecoveryReceiptV1(request=request, request_digest=canonical_digest(request))
                return result.model_copy(update={"request_digest": "f" * 64}) if boundary == "lost-preparation-ack" else result

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
            if recovery:
                from loom_capacity_executor.native_allocated_io import scoped_native_allocated_io
                from loom_capacity_executor.native_build_source import NativeStagedBuildSource

                async with scoped_native_allocated_io(claim=claim,
                    source=NativeStagedBuildSource(context, attempt / "source/archive"), client=Client(), worker_credential=credential) as owner:
                    yield owner
            else:
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
        if recovery:
            assert spec.schema_version == 3
            owner.require_recovery_preparation(spec.recovery_preparation)
            assert calls.index("admission") < calls.index("host") < calls.index("capture") < calls.index("publication") < calls.index("runtime")
        if boundary == "cancel":
            raise asyncio.CancelledError
        return receipt

    monkeypatch.setattr(module, "run_native_outer_build", outer)
    with sealed_native_worker_handoff(packet) as descriptor:
        inherited = os.dup(descriptor)
        metadata = os.fstat(inherited)
        handoff_identity = metadata.st_dev, metadata.st_ino
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
    if boundary in {"admission", "host", "capture", "publication", "admitted-config", "admitted-release", "lost-preparation-ack"}:
        assert "runtime" not in calls and calls[-1] == "io-exit"
        assert not list(scratch.glob("*/work/runtime-spec.json"))


@pytest.mark.parametrize("fault", ["exact", "not-isolated", "interpreter", "rootlesskit", "import-tree", "traversal-import", "unlisted-module"])
def test_running_process_must_match_verified_installation(monkeypatch, protected_helper_host, fault):
    module = import_module("loom_capacity_executor.native_installed_worker")
    manifest = SimpleNamespace(python="/protected/python/bin/python3", rootlesskit="/usr/bin/rootlesskit",
        python_root="/protected/python", files=[SimpleNamespace(path=module.__file__)], platform="linux/amd64")
    monkeypatch.setattr(module.os, "uname", lambda: SimpleNamespace(machine="x86_64"))
    monkeypatch.setattr(module.sys, "flags", SimpleNamespace(isolated=0 if fault == "not-isolated" else 1))
    monkeypatch.setattr(module.sys, "executable", "/wrong/python" if fault == "interpreter" else manifest.python)
    monkeypatch.setattr(module.sys, "path", ["/outside" if fault == "import-tree" else "/protected/python/lib"])
    if fault == "traversal-import":
        monkeypatch.setattr(module.sys, "path", ["/protected/python/../../outside"])
    if fault == "rootlesskit":
        manifest.rootlesskit = "/wrong/rootlesskit"
    if fault == "unlisted-module":
        manifest.files = []
    if fault == "exact":
        module._bind_running_installation(SimpleNamespace(manifest=manifest))
    else:
        with pytest.raises(ValueError):
            module._bind_running_installation(SimpleNamespace(manifest=manifest))


@pytest.mark.parametrize("fault", ["exact", "digest", "mode", "symlink", "unknown", "boolean-bound", "noncanonical", "float-version"])
@pytest.mark.parametrize("version", [1, 2])
def test_config_reader_requires_protected_canonical_bytes(release, monkeypatch, fault, version):
    import json

    module = import_module("loom_capacity_executor.native_installed_worker")
    _release_module, root, _check = release
    monkeypatch.setattr(module, "_require_original_identity", lambda: None)
    document = dict(schema_version=1, release_manifest="/protected/release.json", release_manifest_sha256="a" * 64,
        tooling_source_sha="b" * 40, trusted_fleet_release_sha256="c" * 64, platform="linux/amd64",
        scratch_root="/private/scratch", max_source_archive_bytes=1024**2, max_artifact_bytes=1024**2,
        max_image_archive_bytes=1024**2, max_unpacked_bytes=1024**2, max_rootfs_entries=100,
        tmp_bytes=1024**2, buildkit_state_bytes=1024**2, timeout_seconds=60)
    if version == 2:
        document.update(schema_version=2, host_identity_path="/protected/host.json")
    if fault == "float-version":
        document["schema_version"] = float(version)
    if fault == "unknown":
        document["command"] = ["arbitrary"]
    if fault == "boolean-bound":
        document["max_rootfs_entries"] = True
    wire = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
    if fault == "noncanonical":
        wire += b"\n"
    path = root / "worker.json"
    path.write_bytes(wire)
    path.chmod(0o644 if fault == "mode" else 0o444)
    if fault == "symlink":
        path.rename(root / "real-worker.json")
        path.symlink_to(root / "real-worker.json")
    digest = "f" * 64 if fault == "digest" else hashlib.sha256(wire).hexdigest()
    if fault == "exact":
        result = module.read_installed_worker_config(path, expected_sha256=digest)
        assert canonical_bytes(result) == wire
    else:
        with pytest.raises((ValueError, OSError)):
            module.read_installed_worker_config(path, expected_sha256=digest)


async def test_invalid_cli_still_consumes_inherited_handoff(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_installed_worker")
    _directory, _lease, physical, admission, credential = await prepared_worker(tmp_path)
    packet = packet_for(physical, admission.requests[0], credential, tmp_path)
    with sealed_native_worker_handoff(packet) as descriptor:
        inherited = os.dup(descriptor)
        monkeypatch.setenv(module.NATIVE_WORKER_HANDOFF_ENV, str(inherited))
        monkeypatch.setattr(module.sys, "argv", ["native-worker", "--invalid-option"])
        with pytest.raises(SystemExit):
            module.main()
        with pytest.raises(OSError):
            os.fstat(inherited)
        assert module.NATIVE_WORKER_HANDOFF_ENV not in os.environ


@pytest.mark.parametrize("field,value", [("st_uid", 1000), ("st_gid", 1000), ("st_mode", 0o040777)])
def test_fixed_helper_search_rejects_owner_writable_directory(monkeypatch, protected_helper_host, field, value):
    module = import_module("loom_capacity_executor.native_installed_worker")
    real = os.fstat

    def writable(fd):
        original = real(fd)
        if os.readlink(f"/proc/self/fd/{fd}") != "/usr/local/bin":
            return original
        fields = {name: getattr(original, name) for name in dir(original) if name.startswith("st_")}
        fields[field] = value
        return SimpleNamespace(**fields)

    monkeypatch.setattr(module.os, "fstat", writable)
    with pytest.raises(ValueError, match="protected"):
        module._verify_host_lookup_paths()


@pytest.mark.parametrize("alias", ["/bin", "/sbin"])
@pytest.mark.parametrize("fault", ["exact", "target", "owner", "replaced"])
def test_fixed_helper_alias_must_remain_protected(monkeypatch, protected_helper_host, alias, fault):
    import stat

    module = import_module("loom_capacity_executor.native_installed_worker")
    real_lstat, real_readlink = Path.lstat, os.readlink
    observations = 0

    def lstat(path, *args, **kwargs):
        nonlocal observations
        if str(path) != alias:
            return real_lstat(path, *args, **kwargs)
        observations += 1
        return SimpleNamespace(st_mode=stat.S_IFLNK | 0o777,
            st_uid=1000 if fault == "owner" else 0, st_gid=0, st_dev=1,
            st_ino=2 if fault == "replaced" and observations > 1 else 1)

    def readlink(path, *args, **kwargs):
        if str(path) == alias:
            return "/owner/bin" if fault == "target" else "usr" + alias
        return real_readlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(module.os, "readlink", readlink)
    if fault == "exact":
        module._verify_host_lookup_paths()
        assert observations == 2
    else:
        with pytest.raises(ValueError, match="alias"):
            module._verify_host_lookup_paths()
