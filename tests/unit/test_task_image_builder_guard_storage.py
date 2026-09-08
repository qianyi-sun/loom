from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from loom_task_image_builder_guard import storage as storage_module
from loom_task_image_builder_guard.config import GuardConfig
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import StorageConfig
from loom_task_image_builder_guard.storage import (
    FS_XFLAG_PROJINHERIT,
    JobStorage,
    ProjectQuotaStorage,
    QuotaRecord,
)

GRANT = UUID("11111111-1111-4111-8111-111111111111")
OTHER_GRANT = UUID("22222222-2222-4222-8222-222222222222")
PROJECT_ID = 300_993
BYTE_LIMIT = 100 * 1024**3
INODE_LIMIT = 1_000_000


class _FakeSyscalls:
    def __init__(self) -> None:
        self.projects: dict[int, tuple[int, int]] = {}
        self.quota = QuotaRecord(0, 0, 0, 0)
        self.set_project_calls: list[tuple[int, int]] = []
        self.set_quota_calls: list[tuple[int, int, int]] = []
        self.project_readback: tuple[int, int] | None = None
        self.quota_readback: QuotaRecord | None = None
        self.clear_usage_when_missing: Path | None = None
        self.deleted_inode_descriptor: int | None = None
        self.fail_set_project = False
        self.fail_set_quota = False

    def get_project(self, descriptor: int) -> tuple[int, int]:
        if self.project_readback is not None:
            return self.project_readback
        return self.projects.get(os.fstat(descriptor).st_ino, (0, 0))

    def set_project(self, descriptor: int, project_id: int) -> None:
        if self.fail_set_project:
            raise GuardError("storage_project_write_failed")
        inode = os.fstat(descriptor).st_ino
        self.set_project_calls.append((inode, project_id))
        self.projects[inode] = (project_id, FS_XFLAG_PROJINHERIT)
        self.quota = replace(
            self.quota,
            used_inodes=self.quota.used_inodes + 1,
        )

    def get_quota(self, device_path: Path, project_id: int) -> QuotaRecord:
        del device_path
        assert project_id == PROJECT_ID
        if (
            self.clear_usage_when_missing is not None
            and not self.clear_usage_when_missing.exists()
        ):
            descriptor_open = False
            if self.deleted_inode_descriptor is not None:
                try:
                    os.fstat(self.deleted_inode_descriptor)
                except OSError:
                    pass
                else:
                    descriptor_open = True
            if not descriptor_open:
                self.quota = replace(self.quota, used_bytes=0, used_inodes=0)
        if self.quota_readback is not None and self.set_quota_calls:
            return self.quota_readback
        return self.quota

    def set_quota(
        self,
        device_path: Path,
        project_id: int,
        *,
        byte_limit: int,
        inode_limit: int,
    ) -> None:
        del device_path
        if self.fail_set_quota:
            raise GuardError("storage_quota_write_failed")
        assert project_id == PROJECT_ID
        self.set_quota_calls.append((project_id, byte_limit, inode_limit))
        self.quota = replace(
            self.quota,
            byte_hard_limit=byte_limit,
            inode_hard_limit=inode_limit,
        )


def _device(path: Path) -> str:
    value = path.stat().st_dev
    return f"{os.major(value)}:{os.minor(value)}"


def _mountinfo(path: Path, root: Path, *, options: str = "rw,prjquota", device: str | None = None) -> Path:
    mountinfo = path / "mountinfo"
    mountinfo.write_text(
        f"41 30 {device or _device(root)} / {root} {options} - ext4 /dev/test {options}\n",
        encoding="ascii",
    )
    return mountinfo


def _storage(
    tmp_path: Path,
    *,
    config: StorageConfig | None = None,
    syscalls: _FakeSyscalls | None = None,
    options: str = "rw,prjquota",
    mount_device: str | None = None,
) -> tuple[ProjectQuotaStorage, _FakeSyscalls, Path]:
    root = tmp_path / "storage"
    root.mkdir(mode=0o700)
    (root / "jobs").mkdir(mode=0o700)
    device = mount_device or _device(root)
    selected = config or StorageConfig(
        root,
        device,
        PROJECT_ID,
        BYTE_LIMIT,
        INODE_LIMIT,
    )
    fake = syscalls or _FakeSyscalls()
    manager = ProjectQuotaStorage(
        selected,
        uid=os.geteuid(),
        gid=os.getegid(),
        syscalls=fake,
        mountinfo_path=_mountinfo(tmp_path, root, options=options, device=device),
    )
    return manager, fake, root


def _prepared(
    tmp_path: Path,
) -> tuple[ProjectQuotaStorage, _FakeSyscalls, Path, JobStorage]:
    manager, fake, root = _storage(tmp_path)
    job = manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)
    return manager, fake, root, job


def _guard_document(tmp_path: Path) -> dict[str, object]:
    digest = "1" * 64
    return {
        "schema": "loom.task-image-builder-node-guard-config/v1",
        "cluster_id": "oldlab",
        "cpu_arch": "x86_64",
        "node_name": "trt-eai-oldlab-3",
        "identity": {
            "uid": 993,
            "gid": 980,
            "forbidden_supplementary_gids": [0],
            "supervisor_path": "/usr/local/libexec/loom-task-builder-supervisor",
            "supervisor_sha256": digest,
        },
        "protocol": {
            "socket_path": "/run/loom-task-image-builder-guard/guard.sock",
            "socket_mode": 0o660,
            "socket_gid": 980,
            "max_packet_bytes": 4096,
            "max_pending_peers": 16,
            "requests_per_second": 32,
            "ack_timeout_seconds": 5,
        },
        "authority": {
            "base_url": "https://authority.invalid:8445",
            "connect_ip": "192.0.2.10",
            "ca_path": "/etc/loom/ca",
            "cert_path": "/etc/loom/cert",
            "key_path": "/etc/loom/key",
            "bearer_path": "/etc/loom/bearer",
            "timeout_seconds": 10,
            "max_response_bytes": 65536,
        },
        "commands": {
            name: {"path": f"/usr/bin/{name}", "sha256": digest}
            for name in ("scontrol", "sacct", "bpftool")
        },
        "slurm": {
            "cluster_name": "trt-oldlab",
            "request_sha256": digest,
            "account": "loom-task-builder",
            "partition": "loom-task-builder",
            "qos": "loom-task-image-builder-rootless-oldlab",
            "feature": "loom_rootless_buildkit",
            "cpus": 8,
            "memory_mib": 32768,
            "wall_time": "02:00:00",
        },
        "containment": {
            "cgroup_root": "/sys/fs/cgroup",
            "bpffs_root": "/sys/fs/bpf/loom-task-image-builder",
            "ledger_root": "/var/lib/loom-task-image-builder-guard/ledger",
            "bpf_object_path": "/opt/loom/guard.o",
            "network_policy_path": "/etc/loom/network.json",
            "device_program_tags": ["0123456789abcdef"],
            "pids_max": 4096,
            "io_limits": [
                {"device": "8:1", "rbps": 1, "wbps": 1, "riops": 1, "wiops": 1}
            ],
            "containment_policy_sha256": digest,
            "resource_profile_sha256": digest,
            "bpf_program_sha256": digest,
            "bpf_map_schema_sha256": digest,
        },
        "storage": {
            "root": "/var/lib/loom-task-builder",
            "mount_device": "259:1",
            "project_id": PROJECT_ID,
            "byte_limit": BYTE_LIMIT,
            "inode_limit": INODE_LIMIT,
        },
        "service": {
            "attestation_interval_seconds": 15,
            "attestation_lifetime_seconds": 60,
            "max_ledger_entries": 128,
        },
    }


def test_config_requires_exact_project_quota_policy(tmp_path: Path) -> None:
    path = tmp_path / "guard.json"
    document = _guard_document(tmp_path)
    path.write_text(json.dumps(document), encoding="ascii")
    path.chmod(0o600)

    config = GuardConfig.from_file(path)

    assert config.storage == StorageConfig(
        root=Path("/var/lib/loom-task-builder"),
        mount_device="259:1",
        project_id=PROJECT_ID,
        byte_limit=BYTE_LIMIT,
        inode_limit=INODE_LIMIT,
    )

    for field, invalid in (
        ("root", "/"),
        ("mount_device", "dev/sda"),
        ("project_id", 0),
        ("byte_limit", 1023),
        ("inode_limit", 0),
    ):
        changed = _guard_document(tmp_path)
        changed["storage"][field] = invalid  # type: ignore[index]
        path.write_text(json.dumps(changed), encoding="ascii")
        with pytest.raises(GuardError) as caught:
            GuardConfig.from_file(path)
        assert caught.value.code.startswith("config_")


def test_prepare_creates_only_exact_quota_directory_and_returns_stable_proof(
    tmp_path: Path,
) -> None:
    manager, fake, root = _storage(tmp_path)

    job = manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)
    try:
        metadata = os.fstat(job.descriptor)
        assert job.path == root / "jobs" / str(GRANT)
        assert stat.S_IMODE(metadata.st_mode) == 0o700
        assert (metadata.st_uid, metadata.st_gid) == (os.geteuid(), os.getegid())
        assert (job.device, job.inode, job.project_id) == (
            metadata.st_dev,
            metadata.st_ino,
            PROJECT_ID,
        )
        assert job.byte_limit == BYTE_LIMIT
        assert job.inode_limit == INODE_LIMIT
        expected = {
            "schema_version": 1,
            "path": str(job.path),
            "device": job.device,
            "inode": job.inode,
            "project_id": PROJECT_ID,
            "byte_limit": BYTE_LIMIT,
            "inode_limit": INODE_LIMIT,
        }
        assert job.document() == expected | {
            "quota_sha256": hashlib.sha256(
                json.dumps(expected, sort_keys=True, separators=(",", ":")).encode("ascii")
            ).hexdigest()
        }
        assert fake.set_project_calls == [(metadata.st_ino, PROJECT_ID)]
        assert fake.set_quota_calls == [(PROJECT_ID, BYTE_LIMIT, INODE_LIMIT)]
        assert list((root / "jobs").iterdir()) == [job.path]
    finally:
        job.close()


@pytest.mark.parametrize("escape", ["symlink_root", "symlink_jobs", "root_escape"])
def test_prepare_refuses_symlink_or_root_escape(tmp_path: Path, escape: str) -> None:
    manager, _fake, root = _storage(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    if escape == "symlink_root":
        real = tmp_path / "real-storage"
        root.rename(real)
        root.symlink_to(real, target_is_directory=True)
    elif escape == "symlink_jobs":
        (root / "jobs").rmdir()
        (root / "jobs").symlink_to(outside, target_is_directory=True)
    with pytest.raises(GuardError) as caught:
        if escape == "root_escape":
            manager = ProjectQuotaStorage(
                replace(manager.config, root=root / ".." / "storage"),
                uid=os.geteuid(),
                gid=os.getegid(),
                syscalls=_FakeSyscalls(),
                mountinfo_path=tmp_path / "mountinfo",
            )
        manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)

    assert caught.value.code == "storage_root_invalid"
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ("device", "storage_mount_invalid"),
        ("owner", "storage_root_invalid"),
        ("mode", "storage_root_invalid"),
        ("quota", "storage_mount_invalid"),
    ],
)
def test_prepare_requires_exact_mount_device_owner_mode_and_project_quota(
    tmp_path: Path,
    mutation: str,
    code: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _fake, root = _storage(tmp_path)
    if mutation == "device":
        manager = ProjectQuotaStorage(
            replace(manager.config, mount_device="0:1"),
            uid=os.geteuid(),
            gid=os.getegid(),
            syscalls=_FakeSyscalls(),
            mountinfo_path=tmp_path / "mountinfo",
        )
    elif mutation == "owner":
        monkeypatch.setattr(storage_module.os, "geteuid", lambda: os.getuid() + 1)
    elif mutation == "mode":
        root.chmod(0o755)
    else:
        (tmp_path / "mountinfo").write_text(
            f"41 30 {_device(root)} / {root} rw - ext4 /dev/test rw\n",
            encoding="ascii",
        )

    with pytest.raises(GuardError) as caught:
        manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)

    assert caught.value.code == code


def test_prepare_refuses_reuse_nonempty_directory_or_existing_quota_usage(
    tmp_path: Path,
) -> None:
    manager, fake, root = _storage(tmp_path)
    reused = root / "jobs" / str(OTHER_GRANT)
    reused.mkdir()
    with pytest.raises(GuardError) as caught:
        manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)
    assert caught.value.code == "storage_reused"
    reused.rmdir()

    fake.quota = QuotaRecord(BYTE_LIMIT, INODE_LIMIT, 4096, 1)
    with pytest.raises(GuardError) as caught:
        manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)
    assert caught.value.code == "storage_quota_in_use"


@pytest.mark.parametrize(
    ("project_readback", "quota_readback", "code"),
    [
        ((PROJECT_ID + 1, FS_XFLAG_PROJINHERIT), None, "storage_project_mismatch"),
        ((PROJECT_ID, 0), None, "storage_project_mismatch"),
        (None, QuotaRecord(BYTE_LIMIT - 1024, INODE_LIMIT, 0, 0), "storage_quota_mismatch"),
        (None, QuotaRecord(BYTE_LIMIT, INODE_LIMIT - 1, 0, 0), "storage_quota_mismatch"),
    ],
)
def test_prepare_refuses_project_or_quota_readback_drift(
    tmp_path: Path,
    project_readback: tuple[int, int] | None,
    quota_readback: QuotaRecord | None,
    code: str,
) -> None:
    fake = _FakeSyscalls()
    fake.project_readback = project_readback
    fake.quota_readback = quota_readback
    manager, _fake, _root = _storage(tmp_path, syscalls=fake)

    with pytest.raises(GuardError) as caught:
        manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)

    assert caught.value.code == code


@pytest.mark.parametrize("failure", ["project", "quota"])
def test_prepare_refuses_partial_kernel_write(tmp_path: Path, failure: str) -> None:
    fake = _FakeSyscalls()
    if failure == "project":
        fake.fail_set_project = True
    else:
        fake.fail_set_quota = True
    manager, _fake, _root = _storage(tmp_path, syscalls=fake)

    with pytest.raises(GuardError) as caught:
        manager.prepare(GRANT, byte_limit=BYTE_LIMIT, inode_limit=INODE_LIMIT)

    assert caught.value.code == f"storage_{failure}_write_failed"


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ("quota_bytes", "storage_cleanup_ambiguous"),
        ("quota_inodes", "storage_cleanup_ambiguous"),
        ("mount", "storage_cleanup_ambiguous"),
        ("entry", "storage_cleanup_ambiguous"),
        ("symlink", "storage_cleanup_ambiguous"),
        ("inode", "storage_cleanup_ambiguous"),
        ("device", "storage_cleanup_ambiguous"),
    ],
)
def test_cleanup_preserves_evidence_on_nonempty_mount_symlink_or_identity_drift(
    tmp_path: Path,
    mutation: str,
    code: str,
) -> None:
    manager, fake, root, job = _prepared(tmp_path)
    fake.quota = QuotaRecord(BYTE_LIMIT, INODE_LIMIT, 0, 1)
    if mutation == "quota_bytes":
        fake.quota = replace(fake.quota, used_bytes=4096)
    elif mutation == "quota_inodes":
        fake.quota = replace(fake.quota, used_inodes=2)
    elif mutation == "mount":
        with (tmp_path / "mountinfo").open("a", encoding="ascii") as stream:
            stream.write(
                f"42 41 {_device(root)} / {job.path} rw - ext4 /dev/test rw,prjquota\n"
            )
    elif mutation == "entry":
        (job.path / "residue").write_text("residue", encoding="ascii")
    elif mutation == "symlink":
        (job.path / "link").symlink_to(tmp_path / "outside")
    elif mutation == "inode":
        original = job.path.with_name("original")
        job.path.rename(original)
        job.path.mkdir()
    else:
        job = replace(job, device=job.device + 1)

    with pytest.raises(GuardError) as caught:
        manager.cleanup(job)

    assert caught.value.code == code
    assert (root / "jobs").exists()
    assert fake.set_quota_calls[-1:] != [(PROJECT_ID, 0, 0)]
    job.close()


def test_cleanup_deletes_exact_empty_root_before_clearing_verified_zero_quota(
    tmp_path: Path,
) -> None:
    manager, fake, root, job = _prepared(tmp_path)
    fake.quota = QuotaRecord(BYTE_LIMIT, INODE_LIMIT, 0, 1)
    fake.clear_usage_when_missing = job.path
    fake.deleted_inode_descriptor = job.descriptor
    observed: list[str] = []
    real_set_quota = fake.set_quota

    def set_quota(
        device_path: Path,
        project_id: int,
        *,
        byte_limit: int,
        inode_limit: int,
    ) -> None:
        assert not job.path.exists()
        observed.append("quota_clear")
        fake.quota = replace(fake.quota, used_inodes=0)
        real_set_quota(
            device_path,
            project_id,
            byte_limit=byte_limit,
            inode_limit=inode_limit,
        )

    fake.set_quota = set_quota  # type: ignore[method-assign]

    manager.cleanup(job)

    assert observed == ["quota_clear"]
    assert not job.path.exists()
    assert list((root / "jobs").iterdir()) == []
    assert fake.quota == QuotaRecord(0, 0, 0, 0)
    with pytest.raises(OSError):
        os.fstat(job.descriptor)


def test_recover_reopens_only_the_exact_durable_storage_identity(tmp_path: Path) -> None:
    manager, fake, _root, job = _prepared(tmp_path)
    document = job.document()
    job.close()

    recovered = manager.recover(document)
    try:
        assert recovered.document() == document
        assert os.fstat(recovered.descriptor).st_ino == document["inode"]
    finally:
        recovered.close()

    for field, invalid in (
        ("path", str(tmp_path / "outside")),
        ("inode", int(document["inode"]) + 1),
        ("project_id", PROJECT_ID + 1),
        ("quota_sha256", "2" * 64),
    ):
        with pytest.raises(GuardError) as caught:
            manager.recover(document | {field: invalid})
        assert caught.value.code == "storage_recovery_ambiguous"

    fake.project_readback = (PROJECT_ID + 1, FS_XFLAG_PROJINHERIT)
    with pytest.raises(GuardError) as caught:
        manager.recover(document)
    assert caught.value.code == "storage_recovery_ambiguous"

    fake.project_readback = None
    fake.quota = replace(fake.quota, used_inodes=0)
    with pytest.raises(GuardError) as caught:
        manager.recover(document)
    assert caught.value.code == "storage_recovery_ambiguous"


def test_live_validation_anchors_the_retained_descriptor_to_the_durable_path(
    tmp_path: Path,
) -> None:
    manager, fake, _root, job = _prepared(tmp_path)
    fake.quota = QuotaRecord(BYTE_LIMIT, INODE_LIMIT, 0, 1)

    manager.assert_live(job)
    displaced = job.path.with_name("displaced")
    job.path.rename(displaced)
    job.path.mkdir(mode=0o700)

    with pytest.raises(GuardError) as caught:
        manager.assert_live(job)

    assert caught.value.code == "storage_live_ambiguous"
    job.close()


def test_cleanup_resume_accepts_only_the_proven_absent_root_and_clears_quota(
    tmp_path: Path,
) -> None:
    manager, fake, root, job = _prepared(tmp_path)
    document = job.document()
    fake.quota = QuotaRecord(BYTE_LIMIT, INODE_LIMIT, 0, 1)
    job.close()
    job.path.rmdir()
    fake.quota = QuotaRecord(BYTE_LIMIT, INODE_LIMIT, 0, 0)

    manager.resume_cleanup(document)
    manager.resume_cleanup(document)

    assert list((root / "jobs").iterdir()) == []
    assert fake.quota == QuotaRecord(0, 0, 0, 0)
    assert fake.set_quota_calls[-1] == (PROJECT_ID, 0, 0)
