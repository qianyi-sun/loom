"""Descriptor-relative disposable project-quota job storage."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import json
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Protocol
from uuid import UUID

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import StorageConfig

FS_IOC_FSGETXATTR = 0x801C581F
FS_IOC_FSSETXATTR = 0x401C5820
FS_XFLAG_PROJINHERIT = 0x00000200
_PRJQUOTA = 2
_Q_GETQUOTA = 0x800007
_Q_SETQUOTA = 0x800008
_QIF_BLIMITS = 1
_QIF_ILIMITS = 4
_QUOTA_BLOCK_BYTES = 1024
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


class _Fsxattr(ctypes.Structure):
    _fields_ = (
        ("fsx_xflags", ctypes.c_uint32),
        ("fsx_extsize", ctypes.c_uint32),
        ("fsx_nextents", ctypes.c_uint32),
        ("fsx_projid", ctypes.c_uint32),
        ("fsx_cowextsize", ctypes.c_uint32),
        ("fsx_pad", ctypes.c_ubyte * 8),
    )


class _IfDqblk(ctypes.Structure):
    _fields_ = (
        ("dqb_bhardlimit", ctypes.c_uint64),
        ("dqb_bsoftlimit", ctypes.c_uint64),
        ("dqb_curspace", ctypes.c_uint64),
        ("dqb_ihardlimit", ctypes.c_uint64),
        ("dqb_isoftlimit", ctypes.c_uint64),
        ("dqb_curinodes", ctypes.c_uint64),
        ("dqb_btime", ctypes.c_uint64),
        ("dqb_itime", ctypes.c_uint64),
        ("dqb_valid", ctypes.c_uint32),
    )


@dataclass(frozen=True, slots=True)
class QuotaRecord:
    byte_hard_limit: int
    inode_hard_limit: int
    used_bytes: int
    used_inodes: int

    def is_unused(self) -> bool:
        return self.used_bytes == 0 and self.used_inodes == 0

    def is_clear(self) -> bool:
        return self == QuotaRecord(0, 0, 0, 0)


class ProjectQuotaSyscalls(Protocol):
    def get_project(self, descriptor: int) -> tuple[int, int]: ...

    def set_project(self, descriptor: int, project_id: int) -> None: ...

    def get_quota(self, device_path: Path, project_id: int) -> QuotaRecord: ...

    def set_quota(
        self,
        device_path: Path,
        project_id: int,
        *,
        byte_limit: int,
        inode_limit: int,
    ) -> None: ...


def _qcmd(command: int) -> int:
    return (command << 8) | _PRJQUOTA


def _quotactl(
    command: int,
    path: Path,
    project_id: int,
    value: _IfDqblk,
) -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    call = libc.quotactl
    call.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_void_p]
    call.restype = ctypes.c_int
    result = int(
        call(
            command,
            os.fsencode(path),
            project_id,
            ctypes.cast(ctypes.pointer(value), ctypes.c_void_p),
        )
    )
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result


class LinuxProjectQuotaSyscalls:
    """Small fixed syscall surface for project IDs and generic Linux quotas."""

    @staticmethod
    def _get_fsxattr(descriptor: int) -> _Fsxattr:
        payload = bytearray(ctypes.sizeof(_Fsxattr))
        try:
            result = fcntl.ioctl(descriptor, FS_IOC_FSGETXATTR, payload, True)
        except OSError as exc:
            raise GuardError("storage_project_read_failed") from exc
        if result != 0:
            raise GuardError("storage_project_read_failed")
        return _Fsxattr.from_buffer_copy(payload)

    def get_project(self, descriptor: int) -> tuple[int, int]:
        value = self._get_fsxattr(descriptor)
        return int(value.fsx_projid), int(value.fsx_xflags)

    def set_project(self, descriptor: int, project_id: int) -> None:
        value = self._get_fsxattr(descriptor)
        value.fsx_projid = project_id
        value.fsx_xflags |= FS_XFLAG_PROJINHERIT
        payload = bytearray(bytes(value))
        try:
            result = fcntl.ioctl(descriptor, FS_IOC_FSSETXATTR, payload, True)
        except OSError as exc:
            raise GuardError("storage_project_write_failed") from exc
        if result != 0:
            raise GuardError("storage_project_write_failed")

    @staticmethod
    def get_quota(device_path: Path, project_id: int) -> QuotaRecord:
        value = _IfDqblk()
        try:
            result = _quotactl(_qcmd(_Q_GETQUOTA), device_path, project_id, value)
        except OSError as exc:
            raise GuardError("storage_quota_read_failed") from exc
        if result != 0:
            raise GuardError("storage_quota_read_failed")
        return QuotaRecord(
            int(value.dqb_bhardlimit) * _QUOTA_BLOCK_BYTES,
            int(value.dqb_ihardlimit),
            int(value.dqb_curspace),
            int(value.dqb_curinodes),
        )

    @staticmethod
    def set_quota(
        device_path: Path,
        project_id: int,
        *,
        byte_limit: int,
        inode_limit: int,
    ) -> None:
        if byte_limit % _QUOTA_BLOCK_BYTES != 0:
            raise GuardError("storage_quota_write_failed")
        value = _IfDqblk()
        value.dqb_bhardlimit = byte_limit // _QUOTA_BLOCK_BYTES
        value.dqb_bsoftlimit = value.dqb_bhardlimit
        value.dqb_ihardlimit = inode_limit
        value.dqb_isoftlimit = inode_limit
        value.dqb_valid = _QIF_BLIMITS | _QIF_ILIMITS
        try:
            result = _quotactl(_qcmd(_Q_SETQUOTA), device_path, project_id, value)
        except OSError as exc:
            raise GuardError("storage_quota_write_failed") from exc
        if result != 0:
            raise GuardError("storage_quota_write_failed")


def _directory_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
    )


def _canonical_path(path: Path) -> bool:
    value = path.as_posix()
    pure = PurePosixPath(value)
    return (
        pure.is_absolute()
        and pure != PurePosixPath("/")
        and not value.startswith("//")
        and not value.endswith("/")
        and all(part not in {"", ".", ".."} for part in value.split("/")[1:])
        and pure.as_posix() == value
    )


def _unescape_mount(value: str) -> str:
    for encoded, decoded in (
        ("\\040", " "),
        ("\\011", "\t"),
        ("\\012", "\n"),
        ("\\134", "\\"),
    ):
        value = value.replace(encoded, decoded)
    return value


@dataclass(frozen=True, slots=True)
class _MountRecord:
    device: str
    target: Path
    filesystem: str
    options: frozenset[str]


def _read_mounts(path: Path) -> tuple[_MountRecord, ...]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise GuardError("storage_mount_invalid") from exc
    if not payload or len(payload) > 4 * 1024 * 1024 or b"\x00" in payload:
        raise GuardError("storage_mount_invalid")
    records: list[_MountRecord] = []
    try:
        for raw_line in payload.decode("ascii").splitlines():
            left, right = raw_line.split(" - ", 1)
            before = left.split(" ")
            after = right.split(" ")
            if len(before) < 6 or len(after) < 3:
                raise ValueError
            records.append(
                _MountRecord(
                    before[2],
                    Path(_unescape_mount(before[4])),
                    after[0],
                    frozenset(before[5].split(","))
                    | frozenset(after[2].split(",")),
                )
            )
    except (UnicodeDecodeError, ValueError):
        raise GuardError("storage_mount_invalid") from None
    return tuple(records)


@dataclass(slots=True)
class JobStorage:
    path: Path
    descriptor: int
    device: int
    inode: int
    project_id: int
    byte_limit: int
    inode_limit: int
    quota_sha256: str
    _closed: bool = field(default=False, init=False, repr=False, compare=False)

    def document(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "path": str(self.path),
            "device": self.device,
            "inode": self.inode,
            "project_id": self.project_id,
            "byte_limit": self.byte_limit,
            "inode_limit": self.inode_limit,
            "quota_sha256": self.quota_sha256,
        }

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            os.close(self.descriptor)
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise


class ProjectQuotaStorage:
    """Own one exact disposable project directory beneath a fixed mount."""

    def __init__(
        self,
        config: StorageConfig,
        *,
        uid: int,
        gid: int,
        syscalls: ProjectQuotaSyscalls | None = None,
        mountinfo_path: Path = Path("/proc/self/mountinfo"),
    ) -> None:
        if (
            not isinstance(config, StorageConfig)
            or not _canonical_path(config.root)
            or type(uid) is not int
            or uid < 0
            or type(gid) is not int
            or gid < 0
        ):
            raise GuardError("storage_root_invalid")
        self.config = config
        self._uid = uid
        self._gid = gid
        self._syscalls = syscalls or LinuxProjectQuotaSyscalls()
        self._mountinfo_path = mountinfo_path

    def _mount(self) -> _MountRecord:
        records = tuple(
            record
            for record in _read_mounts(self._mountinfo_path)
            if record.target == self.config.root
        )
        if (
            len(records) != 1
            or records[0].device != self.config.mount_device
            or records[0].filesystem not in {"ext4", "xfs"}
            or not records[0].options.intersection({"prjquota", "pquota"})
        ):
            raise GuardError("storage_mount_invalid")
        return records[0]

    def _open_roots(self) -> tuple[int, int]:
        self._mount()
        root_fd: int | None = None
        jobs_fd: int | None = None
        try:
            root_before = os.lstat(self.config.root)
            root_fd = os.open(self.config.root, _DIRECTORY_FLAGS)
            root_after = os.fstat(root_fd)
            expected_device = os.makedev(
                *(int(part) for part in self.config.mount_device.split(":"))
            )
            if (
                not stat.S_ISDIR(root_after.st_mode)
                or _directory_identity(root_before) != _directory_identity(root_after)
                or root_after.st_dev != expected_device
                or (root_after.st_uid, root_after.st_gid) != (os.geteuid(), os.getegid())
                or stat.S_IMODE(root_after.st_mode) != 0o700
            ):
                raise GuardError("storage_root_invalid")
            jobs_before = os.stat("jobs", dir_fd=root_fd, follow_symlinks=False)
            jobs_fd = os.open("jobs", _DIRECTORY_FLAGS, dir_fd=root_fd)
            jobs_after = os.fstat(jobs_fd)
            if (
                not stat.S_ISDIR(jobs_after.st_mode)
                or _directory_identity(jobs_before) != _directory_identity(jobs_after)
                or jobs_after.st_dev != root_after.st_dev
                or (jobs_after.st_uid, jobs_after.st_gid) != (os.geteuid(), os.getegid())
                or stat.S_IMODE(jobs_after.st_mode) != 0o700
            ):
                raise GuardError("storage_root_invalid")
            result = root_fd, jobs_fd
            root_fd = None
            jobs_fd = None
            return result
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("storage_root_invalid") from exc
        finally:
            for descriptor in (jobs_fd, root_fd):
                if descriptor is not None:
                    os.close(descriptor)

    @staticmethod
    def _grant_name(grant_id: UUID) -> str:
        if not isinstance(grant_id, UUID) or grant_id.int == 0:
            raise GuardError("storage_grant_invalid")
        return str(grant_id)

    @staticmethod
    def _limits(byte_limit: int, inode_limit: int) -> None:
        if (
            type(byte_limit) is not int
            or byte_limit < 1024
            or byte_limit % 1024 != 0
            or type(inode_limit) is not int
            or inode_limit <= 0
        ):
            raise GuardError("storage_limit_invalid")

    def prepare(self, grant_id: UUID, *, byte_limit: int, inode_limit: int) -> JobStorage:
        name = self._grant_name(grant_id)
        self._limits(byte_limit, inode_limit)
        root_fd, jobs_fd = self._open_roots()
        job_fd: int | None = None
        try:
            if os.listdir(jobs_fd):
                raise GuardError("storage_reused")
            current_quota = self._syscalls.get_quota(self.config.root, self.config.project_id)
            if not current_quota.is_clear():
                raise GuardError("storage_quota_in_use")
            os.mkdir(name, mode=0o700, dir_fd=jobs_fd)
            job_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=jobs_fd)
            created = os.fstat(job_fd)
            if (
                not stat.S_ISDIR(created.st_mode)
                or created.st_dev != os.fstat(jobs_fd).st_dev
                or stat.S_IMODE(created.st_mode) != 0o700
            ):
                raise GuardError("storage_job_invalid")
            self._syscalls.set_project(job_fd, self.config.project_id)
            project_id, flags = self._syscalls.get_project(job_fd)
            if project_id != self.config.project_id or not flags & FS_XFLAG_PROJINHERIT:
                raise GuardError("storage_project_mismatch")
            self._syscalls.set_quota(
                self.config.root,
                self.config.project_id,
                byte_limit=byte_limit,
                inode_limit=inode_limit,
            )
            quota = self._syscalls.get_quota(self.config.root, self.config.project_id)
            if quota != QuotaRecord(byte_limit, inode_limit, 0, 1):
                raise GuardError("storage_quota_mismatch")
            os.fchown(job_fd, self._uid, self._gid)
            os.fchmod(job_fd, 0o700)
            final = os.fstat(job_fd)
            path_entry = os.stat(name, dir_fd=jobs_fd, follow_symlinks=False)
            if (
                _directory_identity(final) != _directory_identity(path_entry)
                or final.st_dev != created.st_dev
                or final.st_ino != created.st_ino
                or (final.st_uid, final.st_gid) != (self._uid, self._gid)
                or stat.S_IMODE(final.st_mode) != 0o700
            ):
                raise GuardError("storage_job_invalid")
            proof = {
                "schema_version": 1,
                "path": str(self.config.root / "jobs" / name),
                "device": final.st_dev,
                "inode": final.st_ino,
                "project_id": self.config.project_id,
                "byte_limit": byte_limit,
                "inode_limit": inode_limit,
            }
            digest = hashlib.sha256(
                json.dumps(proof, sort_keys=True, separators=(",", ":")).encode("ascii")
            ).hexdigest()
            result = JobStorage(
                path=self.config.root / "jobs" / name,
                descriptor=job_fd,
                device=final.st_dev,
                inode=final.st_ino,
                project_id=self.config.project_id,
                byte_limit=byte_limit,
                inode_limit=inode_limit,
                quota_sha256=digest,
            )
            job_fd = None
            return result
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("storage_prepare_failed") from exc
        finally:
            if job_fd is not None:
                os.close(job_fd)
            os.close(jobs_fd)
            os.close(root_fd)

    def _document_identity(
        self,
        document: dict[str, object],
        *,
        code: str,
    ) -> tuple[Path, str, str]:
        expected_keys = {
            "schema_version",
            "path",
            "device",
            "inode",
            "project_id",
            "byte_limit",
            "inode_limit",
            "quota_sha256",
        }
        if not isinstance(document, dict) or set(document) != expected_keys:
            raise GuardError("storage_recovery_ambiguous")
        path_value = document.get("path")
        try:
            path = Path(path_value) if isinstance(path_value, str) else Path()
            name = self._grant_name_from_path(path)
        except (GuardError, TypeError, ValueError):
            raise GuardError(code) from None
        scalar_values = (
            document.get("device"),
            document.get("inode"),
            document.get("project_id"),
            document.get("byte_limit"),
            document.get("inode_limit"),
        )
        proof = dict(document)
        quota_sha256 = proof.pop("quota_sha256", None)
        try:
            proof_sha256 = hashlib.sha256(
                json.dumps(proof, sort_keys=True, separators=(",", ":")).encode(
                    "ascii"
                )
            ).hexdigest()
        except (TypeError, ValueError, UnicodeEncodeError):
            raise GuardError(code) from None
        if (
            document.get("schema_version") != 1
            or path != self.config.root / "jobs" / name
            or any(type(value) is not int or value <= 0 for value in scalar_values)
            or document.get("project_id") != self.config.project_id
            or document.get("byte_limit") != self.config.byte_limit
            or document.get("inode_limit") != self.config.inode_limit
            or quota_sha256 != proof_sha256
        ):
            raise GuardError(code)
        return path, name, proof_sha256

    def recover(self, document: dict[str, object]) -> JobStorage:
        """Reopen the one durable job directory without weakening its identity."""

        path, name, proof_sha256 = self._document_identity(
            document,
            code="storage_recovery_ambiguous",
        )
        root_fd: int | None = None
        jobs_fd: int | None = None
        job_fd: int | None = None
        try:
            root_fd, jobs_fd = self._open_roots()
            if os.listdir(jobs_fd) != [name]:
                raise GuardError("storage_recovery_ambiguous")
            before = os.stat(name, dir_fd=jobs_fd, follow_symlinks=False)
            job_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=jobs_fd)
            opened = os.fstat(job_fd)
            project_id, flags = self._syscalls.get_project(job_fd)
            quota = self._syscalls.get_quota(self.config.root, self.config.project_id)
            if (
                not stat.S_ISDIR(opened.st_mode)
                or _directory_identity(before) != _directory_identity(opened)
                or (opened.st_dev, opened.st_ino)
                != (document["device"], document["inode"])
                or (opened.st_uid, opened.st_gid) != (self._uid, self._gid)
                or stat.S_IMODE(opened.st_mode) != 0o700
                or project_id != self.config.project_id
                or not flags & FS_XFLAG_PROJINHERIT
                or quota.byte_hard_limit != self.config.byte_limit
                or quota.inode_hard_limit != self.config.inode_limit
                or not 0 <= quota.used_bytes <= self.config.byte_limit
                or not 1 <= quota.used_inodes <= self.config.inode_limit
                or self._has_mount_at_or_below(path)
            ):
                raise GuardError("storage_recovery_ambiguous")
            result = JobStorage(
                path=path,
                descriptor=job_fd,
                device=opened.st_dev,
                inode=opened.st_ino,
                project_id=self.config.project_id,
                byte_limit=self.config.byte_limit,
                inode_limit=self.config.inode_limit,
                quota_sha256=proof_sha256,
            )
            job_fd = None
            return result
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("storage_recovery_ambiguous") from exc
        finally:
            for descriptor in (job_fd, jobs_fd, root_fd):
                if descriptor is not None:
                    os.close(descriptor)

    def assert_live(self, job: JobStorage) -> None:
        """Revalidate the retained capability against its durable path and quota."""

        if not isinstance(job, JobStorage):
            raise GuardError("storage_live_ambiguous")
        try:
            path, name, proof_sha256 = self._document_identity(
                job.document(),
                code="storage_live_ambiguous",
            )
            root_fd, jobs_fd = self._open_roots()
        except GuardError:
            raise GuardError("storage_live_ambiguous") from None
        try:
            if os.listdir(jobs_fd) != [name]:
                raise GuardError("storage_live_ambiguous")
            before = os.stat(name, dir_fd=jobs_fd, follow_symlinks=False)
            opened = os.fstat(job.descriptor)
            project_id, flags = self._syscalls.get_project(job.descriptor)
            quota = self._syscalls.get_quota(self.config.root, self.config.project_id)
            if (
                path != job.path
                or proof_sha256 != job.quota_sha256
                or not stat.S_ISDIR(opened.st_mode)
                or _directory_identity(before) != _directory_identity(opened)
                or (opened.st_dev, opened.st_ino) != (job.device, job.inode)
                or (opened.st_uid, opened.st_gid) != (self._uid, self._gid)
                or stat.S_IMODE(opened.st_mode) != 0o700
                or project_id != job.project_id
                or not flags & FS_XFLAG_PROJINHERIT
                or quota.byte_hard_limit != job.byte_limit
                or quota.inode_hard_limit != job.inode_limit
                or not 0 <= quota.used_bytes <= job.byte_limit
                or not 1 <= quota.used_inodes <= job.inode_limit
                or self._has_mount_at_or_below(job.path)
            ):
                raise GuardError("storage_live_ambiguous")
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("storage_live_ambiguous") from exc
        finally:
            os.close(jobs_fd)
            os.close(root_fd)

    def resume_cleanup(
        self,
        document: dict[str, object],
        *,
        retained: JobStorage | None = None,
    ) -> None:
        """Resume cleanup after its durable write-ahead marker."""

        path, name, _proof_sha256 = self._document_identity(
            document,
            code="storage_cleanup_ambiguous",
        )
        root_fd: int | None = None
        jobs_fd: int | None = None
        try:
            root_fd, jobs_fd = self._open_roots()
            names = os.listdir(jobs_fd)
            if names == [name]:
                job = retained
                if job is None:
                    os.close(jobs_fd)
                    jobs_fd = None
                    os.close(root_fd)
                    root_fd = None
                    job = self.recover(document)
                elif job.document() != document:
                    raise GuardError("storage_cleanup_ambiguous")
                self.assert_live(job)
                self.cleanup(job)
                return
            if names or path.exists() or self._has_mount_at_or_below(path):
                raise GuardError("storage_cleanup_ambiguous")
            quota = self._syscalls.get_quota(self.config.root, self.config.project_id)
            exact_zero_usage = quota == QuotaRecord(
                self.config.byte_limit,
                self.config.inode_limit,
                0,
                0,
            )
            if quota.is_clear():
                return
            if not exact_zero_usage:
                raise GuardError("storage_cleanup_ambiguous")
            self._syscalls.set_quota(
                self.config.root,
                self.config.project_id,
                byte_limit=0,
                inode_limit=0,
            )
            if not self._syscalls.get_quota(
                self.config.root,
                self.config.project_id,
            ).is_clear():
                raise GuardError("storage_cleanup_ambiguous")
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("storage_cleanup_ambiguous") from exc
        finally:
            for descriptor in (jobs_fd, root_fd):
                if descriptor is not None:
                    os.close(descriptor)

    def cleanup(self, job: JobStorage) -> None:
        if (
            not isinstance(job, JobStorage)
            or job.path != self.config.root / "jobs" / self._grant_name_from_path(job.path)
            or job.project_id != self.config.project_id
        ):
            raise GuardError("storage_cleanup_ambiguous")
        root_fd: int | None = None
        jobs_fd: int | None = None
        try:
            root_fd, jobs_fd = self._open_roots()
            current = os.fstat(job.descriptor)
            name = job.path.name
            path_entry = os.stat(name, dir_fd=jobs_fd, follow_symlinks=False)
            if (
                not stat.S_ISDIR(current.st_mode)
                or _directory_identity(current) != _directory_identity(path_entry)
                or (current.st_dev, current.st_ino) != (job.device, job.inode)
                or (current.st_uid, current.st_gid) != (self._uid, self._gid)
                or stat.S_IMODE(current.st_mode) != 0o700
                or os.listdir(job.descriptor)
                or self._has_mount_at_or_below(job.path)
            ):
                raise GuardError("storage_cleanup_ambiguous")
            project_id, flags = self._syscalls.get_project(job.descriptor)
            if project_id != job.project_id or not flags & FS_XFLAG_PROJINHERIT:
                raise GuardError("storage_cleanup_ambiguous")
            quota = self._syscalls.get_quota(self.config.root, job.project_id)
            if quota != QuotaRecord(job.byte_limit, job.inode_limit, 0, 1):
                raise GuardError("storage_cleanup_ambiguous")
            job.close()
            os.rmdir(name, dir_fd=jobs_fd)
            if self._syscalls.get_quota(self.config.root, job.project_id) != QuotaRecord(
                job.byte_limit, job.inode_limit, 0, 0
            ):
                raise GuardError("storage_cleanup_ambiguous")
            self._syscalls.set_quota(
                self.config.root,
                job.project_id,
                byte_limit=0,
                inode_limit=0,
            )
            if not self._syscalls.get_quota(
                self.config.root, job.project_id
            ).is_clear():
                raise GuardError("storage_cleanup_ambiguous")
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("storage_cleanup_ambiguous") from exc
        finally:
            for descriptor in (jobs_fd, root_fd):
                if descriptor is not None:
                    os.close(descriptor)

    @staticmethod
    def _grant_name_from_path(path: Path) -> str:
        try:
            grant_id = UUID(path.name)
        except (ValueError, AttributeError):
            raise GuardError("storage_cleanup_ambiguous") from None
        if grant_id.int == 0 or str(grant_id) != path.name:
            raise GuardError("storage_cleanup_ambiguous")
        return path.name

    def _has_mount_at_or_below(self, path: Path) -> bool:
        for record in _read_mounts(self._mountinfo_path):
            if record.target == path or path in record.target.parents:
                return True
        return False


__all__ = [
    "FS_IOC_FSGETXATTR",
    "FS_IOC_FSSETXATTR",
    "FS_XFLAG_PROJINHERIT",
    "JobStorage",
    "LinuxProjectQuotaSyscalls",
    "ProjectQuotaStorage",
    "ProjectQuotaSyscalls",
    "QuotaRecord",
]
