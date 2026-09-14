"""Capture real directory identity; never trust a locator's asserted host facts."""

import os
from contextlib import ExitStack
from importlib import import_module
from pathlib import Path
from uuid import uuid4

import pytest

from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_native_installed_release import release as release
from tests.unit.test_native_recovery_contracts import observation


@pytest.fixture
def host(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_recovery_observation")
    _, final = observation()
    attempt = tmp_path / "attempt"
    attempt.mkdir(mode=0o700)
    root = tmp_path / "cgroup"
    process = "/system.slice/slurmstepd.scope/job_101/step_batch/task_0"
    (root / process.lstrip("/")).mkdir(parents=True)
    metadata = attempt.stat()
    locator = final.preparation.locator.model_copy(update={"directory": str(attempt),
        "device": metadata.st_dev, "inode": metadata.st_ino})
    boot = uuid4()
    kernel = {"/proc/self/cgroup": f"0::{process}\n",
        "/proc/sys/kernel/random/boot_id": f"{boot}\n"}
    monkeypatch.setattr(module, "_CGROUP_ROOT", root)
    monkeypatch.setattr(module, "_require_original_identity", lambda: None)
    monkeypatch.setattr(module, "_read_kernel_text", lambda path, bound: kernel[str(path)])
    monkeypatch.setattr(module, "_require_cgroup_mount", lambda descriptor: module._mount_id(descriptor))
    # The authority scanner has separate real-kernel coverage. This fixture
    # isolates original-UID observation with deliberately user-owned temp dirs.
    monkeypatch.setattr(module, "require_native_cgroup_authority", lambda *args, **kwargs: None, raising=False)
    namespace = os.stat("/proc/self/ns/cgroup")
    host_identity = module.NativeRecoveryHostIdentityV1(node_id=locator.physical.binding.node_ids[0],
        boot_id=boot, original_uid=os.getuid(), original_gid=os.getgid(),
        cgroup_namespace_device=namespace.st_dev, cgroup_namespace_inode=namespace.st_ino)
    kwargs = dict(launch_profile_sha256="c" * 64, node_configuration_sha256=canonical_digest(host_identity),
        host_identity=host_identity)
    return module, locator, kwargs, kernel, root, boot


def test_capture_records_actual_job_root_not_step_or_asserted_identity(host):
    module, locator, kwargs, _, root, boot = host
    captured = module.capture_native_recovery_preparation(locator, **kwargs)
    job = root / "system.slice/slurmstepd.scope/job_101"
    assert captured.locator == locator
    assert captured.cgroup_path == "/system.slice/slurmstepd.scope/job_101"
    assert (captured.cgroup_device, captured.cgroup_inode) == (job.stat().st_dev, job.stat().st_ino)
    assert captured.cgroup_mount_id > 0 and captured.boot_id == boot
    assert (captured.original_uid, captured.original_gid) == (os.getuid(), os.getgid())
    assert captured.launch_profile_sha256 == kwargs["launch_profile_sha256"]
    assert list(Path(locator.directory).iterdir()) == [], "observation grants no publication or cleanup"


def test_capture_refuses_delegated_job_before_publication(host, monkeypatch):
    module, locator, kwargs, _, _, _ = host
    def delegated(*args, **kwargs):
        raise ValueError("delegated job controls")
    monkeypatch.setattr(module, "require_native_cgroup_authority", delegated)
    with pytest.raises(ValueError, match="delegated"):
        module.capture_native_recovery_preparation(locator, **kwargs)


def test_capture_rechecks_nondelegation_before_returning_facts(host, monkeypatch):
    module, locator, kwargs, _, _, _ = host
    calls = []
    def changed(*args, **kwargs):
        calls.append(kwargs["relative"])
        if len(calls) == 2:
            raise ValueError("delegation changed")
    monkeypatch.setattr(module, "require_native_cgroup_authority", changed)
    with pytest.raises(ValueError, match="delegation changed"):
        module.capture_native_recovery_preparation(locator, **kwargs)
    assert calls == [Path("system.slice/slurmstepd.scope/job_101")] * 2


@pytest.mark.parametrize("fault", ["foreign-job", "nested-job", "no-step", "no-slurm", "traversal",
    "noncanonical", "extra-row", "boot", "node", "locator", "private", "symlink"])
def test_capture_rejects_foreign_or_unsafe_host_scope(host, fault):
    module, locator, kwargs, kernel, root, _ = host
    process = "/system.slice/slurmstepd.scope/job_101/step_batch/task_0"
    if fault == "foreign-job":
        process = process.replace("job_101", "job_999")
    elif fault == "nested-job":
        process = process.replace("step_batch", "job_999/step_batch")
    elif fault == "no-step":
        process = "/system.slice/slurmstepd.scope/job_101"
    elif fault == "no-slurm":
        process = "/foreign/job_101/step_batch"
    elif fault == "traversal":
        process = process.replace("step_batch", "../step_batch")
    elif fault == "noncanonical":
        process = process.replace("step_batch", "/step_batch")
    elif fault == "boot":
        kernel["/proc/sys/kernel/random/boot_id"] = "not-a-boot-id\n"
    elif fault == "node":
        kwargs["host_identity"] = kwargs["host_identity"].model_copy(update={"node_id": "foreign-node"})
        kwargs["node_configuration_sha256"] = canonical_digest(kwargs["host_identity"])
    elif fault == "locator":
        locator = locator.model_copy(update={"inode": locator.inode + 1})
    elif fault == "private":
        Path(locator.directory).chmod(0o750)
    elif fault == "symlink":
        job = root / "system.slice/slurmstepd.scope/job_101"
        job.rename(job.with_name("real-job"))
        job.symlink_to(job.with_name("real-job"), target_is_directory=True)
    kernel["/proc/self/cgroup"] = f"0::{process}\n" + ("0::/foreign\n" if fault == "extra-row" else "")
    with pytest.raises((ValueError, OSError)):
        module.capture_native_recovery_preparation(locator, **kwargs)


@pytest.mark.parametrize("fault", ["move-process", "replace-job", "replace-attempt", "boot-change", "mount-change"])
def test_capture_rechecks_kernel_and_opened_directory_identity(host, monkeypatch, fault):
    module, locator, kwargs, kernel, root, _ = host
    initial = dict(kernel)
    reads = {}
    original_mount = module._mount_id
    mounts = 0

    def read(path, bound):
        name = str(path)
        reads[name] = reads.get(name, 0) + 1
        if name == "/proc/self/cgroup" and reads[name] == 2:
            if fault == "move-process":
                return "0::/system.slice/slurmstepd.scope/job_999/step_batch\n"
            if fault in {"replace-job", "replace-attempt"}:
                target = (root / "system.slice/slurmstepd.scope/job_101"
                    if fault == "replace-job" else Path(locator.directory))
                target.rename(target.with_name(target.name + "-old"))
                target.mkdir(mode=0o700)
        if name == "/proc/sys/kernel/random/boot_id" and reads[name] == 2 and fault == "boot-change":
            return f"{uuid4()}\n"
        return initial[name]

    def mount(fd):
        nonlocal mounts
        mounts += 1
        return original_mount(fd) + (1 if fault == "mount-change" and mounts >= 3 else 0)

    monkeypatch.setattr(module, "_read_kernel_text", read)
    monkeypatch.setattr(module, "_mount_id", mount)
    with pytest.raises((ValueError, OSError)):
        module.capture_native_recovery_preparation(locator, **kwargs)


@pytest.mark.parametrize("fault", ["exact", "filesystem", "root", "mountpoint", "duplicate", "missing"])
def test_mount_observation_requires_exact_full_cgroup2_mount(tmp_path, monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_recovery_observation")
    row = "42 21 0:29 / /sys/fs/cgroup rw,nosuid,nodev,noexec - cgroup2 cgroup rw\n"
    if fault == "filesystem":
        row = row.replace("- cgroup2", "- tmpfs")
    elif fault == "root":
        row = row.replace("0:29 / ", "0:29 /delegated ")
    elif fault == "mountpoint":
        row = row.replace("/sys/fs/cgroup", "/other")
    elif fault == "duplicate":
        row += row
    elif fault == "missing":
        row = ""
    monkeypatch.setattr(module, "_read_kernel_text", lambda path, bound: row)
    monkeypatch.setattr(module, "_mount_id", lambda fd: 42)
    with ExitStack() as stack:
        fd = module._open_directory(tmp_path, stack)
        if fault == "exact":
            assert module._require_cgroup_mount(fd) == 42
        else:
            with pytest.raises(ValueError, match="mount"):
                module._require_cgroup_mount(fd)


@pytest.mark.parametrize("fault", ["exact", "overflow", "symlink", "fifo", "directory", "non-ascii"])
def test_kernel_reads_are_bounded_nofollow_and_nonblocking(tmp_path, fault):
    module = import_module("loom_capacity_executor.native_recovery_observation")
    path = tmp_path / "kernel-file"
    if fault == "fifo":
        os.mkfifo(path)
    elif fault == "directory":
        path.mkdir()
    else:
        path.write_bytes(b"\xff" if fault == "non-ascii" else b"abcde" if fault == "overflow" else b"abcd")
        if fault == "symlink":
            target = path.with_name("original")
            path.rename(target)
            path.symlink_to(target)
    if fault == "exact":
        assert module._read_kernel_text(path, 4) == "abcd"
    else:
        with pytest.raises((ValueError, OSError)):
            module._read_kernel_text(path, 4)


@pytest.mark.parametrize("changed", [False, True])
def test_cgroup_namespace_must_match_protected_installer_identity(monkeypatch, changed):
    from types import SimpleNamespace

    module = import_module("loom_capacity_executor.native_recovery_observation")
    original_stat = module.os.stat

    def metadata(path, *args, **kwargs):
        assert path != "/proc/1/ns/cgroup", "unprivileged workers cannot inspect root PID1 namespaces"
        if path == "/proc/self/ns/cgroup":
            return SimpleNamespace(st_dev=4, st_ino=100 + int(changed))
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(module.os, "stat", metadata)
    host_identity = module.NativeRecoveryHostIdentityV1(node_id="node-a", boot_id=uuid4(),
        original_uid=1000, original_gid=1000, cgroup_namespace_device=4, cgroup_namespace_inode=100)
    if changed:
        with pytest.raises(ValueError, match="namespace"):
            module._require_host_cgroup_namespace(host_identity)
    else:
        module._require_host_cgroup_namespace(host_identity)


@pytest.mark.parametrize("field", ["boot_id", "original_uid", "original_gid", "cgroup_namespace_inode", "digest"])
def test_capture_rejects_stale_or_foreign_installer_identity(host, field):
    module, locator, kwargs, _, _, _ = host
    if field == "digest":
        kwargs["node_configuration_sha256"] = "f" * 64
    else:
        original = kwargs["host_identity"]
        value = uuid4() if field == "boot_id" else getattr(original, field) + 1
        kwargs["host_identity"] = original.model_copy(update={field: value})
        kwargs["node_configuration_sha256"] = canonical_digest(kwargs["host_identity"])
    with pytest.raises(ValueError):
        module.capture_native_recovery_preparation(locator, **kwargs)


@pytest.mark.parametrize("fault", ["exact", "digest", "writable", "symlink", "noncanonical", "extra"])
def test_host_identity_reader_requires_exact_protected_installer_file(release, monkeypatch, fault):
    import hashlib
    import json

    module = import_module("loom_capacity_executor.native_recovery_observation")
    _, root, _ = release
    monkeypatch.setattr(module, "_require_original_identity", lambda: None)
    identity = module.NativeRecoveryHostIdentityV1(node_id="node-a", boot_id=uuid4(),
        original_uid=1000, original_gid=1000, cgroup_namespace_device=4, cgroup_namespace_inode=100)
    wire = canonical_bytes(identity)
    if fault == "noncanonical":
        wire += b"\n"
    elif fault == "extra":
        document = json.loads(wire)
        document["approved"] = True
        wire = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    path = root / "node-identity.json"
    path.write_bytes(wire)
    path.chmod(0o644 if fault == "writable" else 0o444)
    if fault == "symlink":
        original = path.with_name("original.json")
        path.rename(original)
        path.symlink_to(original)
    digest = "f" * 64 if fault == "digest" else hashlib.sha256(wire).hexdigest()
    if fault == "exact":
        assert module.read_native_recovery_host_identity(path, expected_sha256=digest) == identity
    else:
        with pytest.raises((ValueError, OSError)):
            module.read_native_recovery_host_identity(path, expected_sha256=digest)
