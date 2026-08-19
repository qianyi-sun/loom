#!/usr/bin/env python3
"""Install and verify the exact personal-development gVisor runtime."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
import platform
import re
import secrets
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, NoReturn, Protocol, cast, runtime_checkable

from scripts.ops.personal_dev_builder_runtime_profile import (
    RuntimeArchiveMember,
    RuntimeProfile,
    RuntimeProfileError,
    load_runtime_profile,
)

_ACTIVE_CONTAINERD_CONFIG = Path("/var/lib/rancher/k3s/agent/etc/containerd/config.toml")
_K3S_DATA_ROOT = Path("/var/lib/rancher/k3s")
_MODULES_PATH = Path("/proc/modules")
_MIN_FREE_BYTES = 20 * 1024**3
_MAX_ARCHIVE_BYTES = 1024 * 1024**3
_MAX_COMMAND_OUTPUT = 64 * 1024
_MAX_CONFIG_BYTES = 4 * 1024 * 1024
_COMMAND_TIMEOUT_SECONDS = 30
_SYSTEMCTL = "/usr/bin/systemctl"
_TEST = "/usr/bin/test"
_DF = "/usr/bin/df"
_K3S = "/usr/local/bin/k3s"
_ROOT_ENV = {
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100


class PersonalDevBuilderRuntimeInstallError(RuntimeError):
    """The runtime installation could not converge without weakening safety."""

    def __init__(self, code: str) -> None:
        safe_code = (
            code if isinstance(code, str) and _ERROR_CODE.fullmatch(code) else "internal_error"
        )
        self.code = safe_code
        super().__init__(safe_code)


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@runtime_checkable
class Runner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult: ...


class SubprocessRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        if not argv or any(not isinstance(value, str) or not value for value in argv):
            raise PersonalDevBuilderRuntimeInstallError("command_invalid")
        try:
            completed = subprocess.run(
                list(argv),
                capture_output=True,
                text=True,
                check=False,
                env=env or _ROOT_ENV,
                timeout=_COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise PersonalDevBuilderRuntimeInstallError("command_timeout") from exc
        if (
            len(completed.stdout.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
            or len(completed.stderr.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
        ):
            raise PersonalDevBuilderRuntimeInstallError("command_output_invalid")
        result = CommandResult(completed.returncode, completed.stdout, completed.stderr)
        if check and result.returncode != 0:
            raise PersonalDevBuilderRuntimeInstallError("command_failed")
        return result


@dataclass(frozen=True, slots=True)
class InstallContext:
    """Map host-absolute paths and commands into direct or test-root execution."""

    root: Path = Path("/")
    command_prefix: tuple[str, ...] = ()
    authority_uid: int = 0
    authority_gid: int = 0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.root, Path)
            or not self.root.is_absolute()
            or ".." in self.root.parts
            or self.authority_uid < 0
            or self.authority_gid < 0
            or any(not isinstance(value, str) or not value for value in self.command_prefix)
        ):
            raise PersonalDevBuilderRuntimeInstallError("context_invalid")

    def path(self, absolute: Path) -> Path:
        if not isinstance(absolute, Path) or not absolute.is_absolute() or ".." in absolute.parts:
            raise PersonalDevBuilderRuntimeInstallError("path_invalid")
        if self.root == Path("/"):
            return absolute
        return self.root.joinpath(*absolute.parts[1:])

    def argv(self, *argv: str) -> tuple[str, ...]:
        if not argv or any(not isinstance(value, str) or not value for value in argv):
            raise PersonalDevBuilderRuntimeInstallError("command_invalid")
        return (*self.command_prefix, *argv)


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_remove_stage(path: Path) -> None:
    metadata = os.lstat(path)
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise PersonalDevBuilderRuntimeInstallError("staging_cleanup_invalid")
    path.chmod(0o700)
    with os.scandir(path) as entries:
        for entry in entries:
            child = Path(entry.path)
            child_metadata = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(child_metadata.st_mode):
                _safe_remove_stage(child)
            elif stat.S_ISREG(child_metadata.st_mode) or stat.S_ISLNK(child_metadata.st_mode):
                child.unlink()
            else:
                raise PersonalDevBuilderRuntimeInstallError("staging_cleanup_invalid")
    path.rmdir()


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise PersonalDevBuilderRuntimeInstallError("atomic_publish_unavailable")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error in {errno.EEXIST, errno.ENOTEMPTY}:
            raise PersonalDevBuilderRuntimeInstallError("destination_changed")
        raise PersonalDevBuilderRuntimeInstallError("atomic_publish_failed")


class _HashingReader:
    def __init__(self, source: BinaryIO) -> None:
        self.source = source
        self.digest = hashlib.sha512()

    def read(self, size: int = -1) -> bytes:
        payload = self.source.read(size)
        self.digest.update(payload)
        return payload


def _archive_name(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name or "\\" in name:
        raise PersonalDevBuilderRuntimeInstallError("archive_member_invalid")
    path = PurePosixPath(name)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or str(path) != name
    ):
        raise PersonalDevBuilderRuntimeInstallError("archive_member_invalid")
    return name


def _write_member(
    bundle: tarfile.TarFile,
    member: tarfile.TarInfo,
    destination: Path,
    expected: RuntimeArchiveMember,
    *,
    uid: int,
    gid: int,
) -> None:
    source = bundle.extractfile(member)
    if source is None:
        raise PersonalDevBuilderRuntimeInstallError("archive_member_invalid")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(destination, flags, 0o600)
    except OSError as exc:
        source.close()
        raise PersonalDevBuilderRuntimeInstallError("archive_destination_invalid") from exc
    digest = hashlib.sha256()
    remaining = expected.size
    try:
        with os.fdopen(descriptor, "wb") as output:
            while remaining:
                chunk = source.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise PersonalDevBuilderRuntimeInstallError("archive_member_invalid")
                output.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            if source.read(1):
                raise PersonalDevBuilderRuntimeInstallError("archive_member_invalid")
            os.fchmod(output.fileno(), expected.install_mode)
            os.fchown(output.fileno(), uid, gid)
            output.flush()
            os.fsync(output.fileno())
    finally:
        source.close()
    if digest.hexdigest() != expected.sha256:
        raise PersonalDevBuilderRuntimeInstallError("archive_member_invalid")


def _extract_verified_archive(
    archive: Path,
    destination: Path,
    members: Mapping[str, RuntimeArchiveMember],
    *,
    archive_sha512: str,
    uid: int,
    gid: int,
) -> None:
    if not archive.is_absolute() or ".." in archive.parts:
        raise PersonalDevBuilderRuntimeInstallError("archive_invalid")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(archive, flags)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_ARCHIVE_BYTES
            or stat.S_IMODE(before.st_mode) != 0o600
        ):
            raise PersonalDevBuilderRuntimeInstallError("archive_invalid")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            reader = _HashingReader(source)
            seen: set[str] = set()
            try:
                with tarfile.open(fileobj=cast(BinaryIO, reader), mode="r|bz2") as bundle:
                    for member in bundle:
                        name = _archive_name(member.name)
                        expected = members.get(name)
                        if (
                            expected is None
                            or name in seen
                            or not member.isreg()
                            or member.size != expected.size
                            or stat.S_IMODE(member.mode) != expected.archive_mode
                        ):
                            raise PersonalDevBuilderRuntimeInstallError("archive_member_invalid")
                        seen.add(name)
                        target = destination.joinpath(*PurePosixPath(name).parts)
                        _write_member(
                            bundle,
                            member,
                            target,
                            expected,
                            uid=uid,
                            gid=gid,
                        )
                while reader.read(1024 * 1024):
                    pass
            except (OSError, tarfile.TarError, EOFError) as exc:
                raise PersonalDevBuilderRuntimeInstallError("archive_invalid") from exc
            if seen != set(members) or reader.digest.hexdigest() != archive_sha512:
                raise PersonalDevBuilderRuntimeInstallError("archive_invalid")
        after = os.fstat(descriptor)
        if _identity(before) != _identity(after):
            raise PersonalDevBuilderRuntimeInstallError("archive_changed")
    except OSError as exc:
        raise PersonalDevBuilderRuntimeInstallError("archive_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_regular(
    path: Path,
    *,
    maximum: int,
    uid: int | None = None,
    gid: int | None = None,
    mode: int | None = None,
) -> bytes:
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 <= before.st_size <= maximum
            or (uid is not None and before.st_uid != uid)
            or (gid is not None and before.st_gid != gid)
            or (mode is not None and stat.S_IMODE(before.st_mode) != mode)
        ):
            raise PersonalDevBuilderRuntimeInstallError("managed_file_invalid")
        payload = bytearray()
        while chunk := os.read(descriptor, min(64 * 1024, before.st_size + 1 - len(payload))):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) != before.st_size or _identity(before) != _identity(after):
            raise PersonalDevBuilderRuntimeInstallError("managed_file_invalid")
        return bytes(payload)
    except OSError as exc:
        raise PersonalDevBuilderRuntimeInstallError("managed_file_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_virtual_regular(path: Path, *, maximum: int, error_code: str) -> bytes:
    """Read a bounded procfs-style file whose reported size may be zero."""
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise PersonalDevBuilderRuntimeInstallError(error_code)
        payload = bytearray()
        while chunk := os.read(descriptor, min(64 * 1024, maximum + 1 - len(payload))):
            payload.extend(chunk)
            if len(payload) > maximum:
                raise PersonalDevBuilderRuntimeInstallError(error_code)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_gid,
            before.st_nlink,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
        ):
            raise PersonalDevBuilderRuntimeInstallError(error_code)
        return bytes(payload)
    except OSError as exc:
        raise PersonalDevBuilderRuntimeInstallError(error_code) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


class PersonalDevBuilderRuntimeInstaller:
    def __init__(
        self,
        *,
        profile: RuntimeProfile,
        context: InstallContext,
        runner: Runner,
        machine: str | None = None,
        effective_uid: int | None = None,
    ) -> None:
        if not isinstance(profile, RuntimeProfile) or not isinstance(context, InstallContext):
            raise PersonalDevBuilderRuntimeInstallError("context_invalid")
        self.profile = profile
        self.context = context
        self.runner = runner
        self.machine = platform.machine() if machine is None else machine
        self.effective_uid = os.geteuid() if effective_uid is None else effective_uid

    def _path(self, absolute: Path) -> Path:
        return self.context.path(absolute)

    def _run(self, *argv: str, check: bool = True) -> CommandResult:
        result = self.runner.run(
            self.context.argv(*argv),
            check=check,
            env=_ROOT_ENV,
        )
        if (
            not isinstance(result, CommandResult)
            or len(result.stdout.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
            or len(result.stderr.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
        ):
            raise PersonalDevBuilderRuntimeInstallError("command_output_invalid")
        return result

    def _managed_roots(self) -> tuple[Path, ...]:
        return (
            self._path(self.profile.release_root),
            self._path(self.profile.profile_path),
            self._path(self.profile.runsc_config_path),
            self._path(self.profile.k3s_template_path),
            self._path(self.profile.shim_link_path),
        )

    def _all_absent(self) -> bool:
        return all(not path.exists() and not path.is_symlink() for path in self._managed_roots())

    def _assert_existing_parent_chain(self, absolute: Path) -> None:
        mapped = self._path(absolute)
        current = self.context.root
        relative = mapped.relative_to(self.context.root)
        for part in relative.parts:
            current /= part
            if not current.exists() and not current.is_symlink():
                return
            metadata = os.lstat(current)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.context.authority_uid
                or metadata.st_gid != self.context.authority_gid
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise PersonalDevBuilderRuntimeInstallError("parent_directory_invalid")

    def _ensure_directory(self, absolute: Path) -> Path:
        mapped = self._path(absolute)
        current = self.context.root
        relative = mapped.relative_to(self.context.root)
        for part in relative.parts:
            parent = current
            current /= part
            if current.exists() or current.is_symlink():
                metadata = os.lstat(current)
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or stat.S_ISLNK(metadata.st_mode)
                    or metadata.st_uid != self.context.authority_uid
                    or metadata.st_gid != self.context.authority_gid
                    or stat.S_IMODE(metadata.st_mode) & 0o022
                ):
                    raise PersonalDevBuilderRuntimeInstallError("parent_directory_invalid")
                continue
            current.mkdir(mode=0o755)
            os.chown(current, self.context.authority_uid, self.context.authority_gid)
            _fsync_directory(parent)
        return mapped

    def _verify_host(self) -> None:
        try:
            context_root = os.lstat(self.context.root)
        except OSError as exc:
            raise PersonalDevBuilderRuntimeInstallError("parent_directory_invalid") from exc
        if (
            not stat.S_ISDIR(context_root.st_mode)
            or stat.S_ISLNK(context_root.st_mode)
            or context_root.st_uid != self.context.authority_uid
            or context_root.st_gid != self.context.authority_gid
            or stat.S_IMODE(context_root.st_mode) & 0o022
        ):
            raise PersonalDevBuilderRuntimeInstallError("parent_directory_invalid")
        if self.context.root == Path("/") and self.effective_uid != 0:
            raise PersonalDevBuilderRuntimeInstallError("authority_invalid")
        machine = self.machine.strip().lower()
        if machine not in {"x86_64", "amd64"} or self.profile.architecture != "amd64":
            raise PersonalDevBuilderRuntimeInstallError("architecture_invalid")

        device = self._run(
            _TEST,
            "-c",
            str(self._path(self.profile.device_path)),
            check=False,
        )
        if device.returncode != 0 or device.stdout or device.stderr:
            raise PersonalDevBuilderRuntimeInstallError("kvm_device_invalid")

        modules = _read_virtual_regular(
            self._path(_MODULES_PATH),
            maximum=64 * 1024,
            error_code="kvm_modules_invalid",
        )
        try:
            loaded = {
                line.split(maxsplit=1)[0] for line in modules.decode("ascii").splitlines() if line
            }
        except UnicodeDecodeError as exc:
            raise PersonalDevBuilderRuntimeInstallError("kvm_modules_invalid") from exc
        if not set(self.profile.modules).issubset(loaded):
            raise PersonalDevBuilderRuntimeInstallError("kvm_modules_invalid")

        disk = self._run(
            _DF,
            "-B1",
            "--output=avail",
            str(self._path(_K3S_DATA_ROOT)),
            check=False,
        )
        try:
            disk_lines = disk.stdout.splitlines()
            available = int(disk_lines[1])
        except (IndexError, TypeError, ValueError) as exc:
            raise PersonalDevBuilderRuntimeInstallError("disk_capacity_invalid") from exc
        if (
            disk.returncode != 0
            or disk.stderr
            or len(disk_lines) != 2
            or disk_lines[0].strip() != "Avail"
            or available < _MIN_FREE_BYTES
        ):
            raise PersonalDevBuilderRuntimeInstallError("disk_capacity_invalid")

        agent = self._run(_SYSTEMCTL, "is-active", self.profile.k3s_service, check=False)
        server = self._run(_SYSTEMCTL, "is-active", "k3s", check=False)
        if (
            agent.returncode != 0
            or agent.stdout != "active\n"
            or agent.stderr
            or server.returncode == 0
            or server.stdout != "inactive\n"
            or server.stderr
        ):
            raise PersonalDevBuilderRuntimeInstallError("service_state_invalid")

        process = self._run(
            _SYSTEMCTL,
            "show",
            self.profile.k3s_service,
            "--property=MainPID",
            "--value",
            check=False,
        )
        if (
            process.returncode != 0
            or process.stderr
            or re.fullmatch(r"[1-9][0-9]{0,6}\n", process.stdout) is None
        ):
            raise PersonalDevBuilderRuntimeInstallError("service_path_invalid")
        try:
            main_pid = int(process.stdout)
        except ValueError as exc:  # pragma: no cover - regex above is authoritative
            raise PersonalDevBuilderRuntimeInstallError("service_path_invalid") from exc
        environment = _read_virtual_regular(
            self._path(Path("/proc") / str(main_pid) / "environ"),
            maximum=1024 * 1024,
            error_code="service_path_invalid",
        )
        fields = environment[:-1].split(b"\0") if environment.endswith(b"\0") else []
        paths = [field[5:] for field in fields if field.startswith(b"PATH=")]
        expected_path = os.fsencode(self.profile.shim_link_path.parent)
        if len(paths) != 1 or expected_path not in paths[0].split(b":"):
            raise PersonalDevBuilderRuntimeInstallError("service_path_invalid")
        process_after = self._run(
            _SYSTEMCTL,
            "show",
            self.profile.k3s_service,
            "--property=MainPID",
            "--value",
            check=False,
        )
        if process_after != process:
            raise PersonalDevBuilderRuntimeInstallError("service_path_invalid")

        k3s = self._run(_K3S, "--version", check=False)
        k3s_lines = k3s.stdout.splitlines()
        if (
            k3s.returncode != 0
            or k3s.stderr
            or len(k3s_lines) != 2
            or re.fullmatch(
                rf"k3s version {re.escape(self.profile.k3s_version)} "
                r"\([0-9A-Za-z._+-]{1,64}\)",
                k3s_lines[0],
            )
            is None
            or re.fullmatch(r"go version go1[.][0-9]+(?:[.][0-9]+)?", k3s_lines[1]) is None
        ):
            raise PersonalDevBuilderRuntimeInstallError("k3s_version_invalid")
        containerd = self._run(_K3S, "ctr", "version", check=False)
        versions = re.findall(r"^\s*Version:\s+(\S+)\s*$", containerd.stdout, re.MULTILINE)
        if (
            containerd.returncode != 0
            or containerd.stderr
            or versions != [self.profile.containerd_version, self.profile.containerd_version]
        ):
            raise PersonalDevBuilderRuntimeInstallError("containerd_version_invalid")

    def _verify_destinations(self) -> None:
        for absolute in (
            self.profile.release_root.parent,
            self.profile.profile_path.parent,
            self.profile.runsc_config_path.parent,
            self.profile.k3s_template_path.parent,
            self.profile.shim_link_path.parent,
        ):
            self._assert_existing_parent_chain(absolute)
        if self._all_absent():
            return
        self._verify_installed()

    def _verify_directory(self, path: Path, *, mode: int) -> None:
        try:
            metadata = os.lstat(path)
        except OSError as exc:
            raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.context.authority_uid
            or metadata.st_gid != self.context.authority_gid
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid")

    def _verify_file(self, absolute: Path, payload: bytes, *, mode: int) -> None:
        actual = _read_regular(
            self._path(absolute),
            maximum=max(len(payload), 1),
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
            mode=mode,
        )
        if actual != payload:
            raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid")

    def _verify_installed(self) -> None:
        release = self._path(self.profile.release_root)
        self._verify_directory(release, mode=0o555)
        expected_paths = {Path(name) for name in self.profile.members}
        expected_directories = {path.parent for path in expected_paths if path.parent != Path(".")}
        actual_files: set[Path] = set()
        actual_directories: set[Path] = set()
        for root, directories, files in os.walk(release, followlinks=False):
            root_path = Path(root)
            for name in directories:
                child = root_path / name
                relative = child.relative_to(release)
                metadata = os.lstat(child)
                if stat.S_ISLNK(metadata.st_mode):
                    raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid")
                actual_directories.add(relative)
            for name in files:
                actual_files.add((root_path / name).relative_to(release))
        if actual_files != expected_paths or actual_directories != expected_directories:
            raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid")
        for relative in expected_directories:
            self._verify_directory(release / relative, mode=0o555)
        for name, member in self.profile.members.items():
            payload = _read_regular(
                release / name,
                maximum=member.size,
                uid=self.context.authority_uid,
                gid=self.context.authority_gid,
                mode=member.install_mode,
            )
            if len(payload) != member.size or hashlib.sha256(payload).hexdigest() != member.sha256:
                raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid")
        self._verify_file(self.profile.profile_path, self.profile.payload, mode=0o444)
        self._verify_file(self.profile.runsc_config_path, self.profile.runsc_toml, mode=0o444)
        self._verify_file(self.profile.k3s_template_path, self.profile.k3s_template, mode=0o444)
        link = self._path(self.profile.shim_link_path)
        try:
            metadata = os.lstat(link)
        except OSError as exc:
            raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid") from exc
        if (
            not stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.context.authority_uid
            or metadata.st_gid != self.context.authority_gid
            or metadata.st_nlink != 1
            or os.readlink(link) != str(self.profile.shim_path)
        ):
            raise PersonalDevBuilderRuntimeInstallError("staged_state_invalid")

    def _archive_stage(self, archive: Path, parent: Path | None = None) -> Path:
        base = parent
        if base is None:
            temporary = Path(tempfile.mkdtemp(prefix="loom-gvisor-verify-"))
        else:
            temporary = Path(tempfile.mkdtemp(prefix=".gvisor-stage-", dir=base))
        os.chown(temporary, self.context.authority_uid, self.context.authority_gid)
        try:
            _extract_verified_archive(
                archive,
                temporary,
                self.profile.members,
                archive_sha512=self.profile.archive_sha512,
                uid=self.context.authority_uid,
                gid=self.context.authority_gid,
            )
            return temporary
        except BaseException:
            if temporary.exists() and not temporary.is_symlink():
                _safe_remove_stage(temporary)
            raise

    def _receipt(self, operation: str, *, state: str | None = None) -> dict[str, str]:
        receipt = {
            "operation": operation,
            "profile_sha256": self.profile.sha256,
            "release": self.profile.version,
        }
        if state is not None:
            receipt["state"] = state
        return receipt

    def preflight(self, archive: Path) -> dict[str, str]:
        self._verify_host()
        self._verify_destinations()
        stage = self._archive_stage(archive)
        _safe_remove_stage(stage)
        return {
            "archive_sha512": self.profile.archive_sha512,
            **self._receipt("preflight"),
        }

    def _freeze_stage(self, stage: Path) -> None:
        directories = [stage]
        with os.scandir(stage) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    directories.append(Path(entry.path))
        for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
            os.chown(directory, self.context.authority_uid, self.context.authority_gid)
            directory.chmod(0o555)
            _fsync_directory(directory)

    def _publish_file(self, absolute: Path, payload: bytes) -> None:
        parent = self._ensure_directory(absolute.parent)
        destination = self._path(absolute)
        descriptor, name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=parent)
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o444)
            os.fchown(descriptor, self.context.authority_uid, self.context.authority_gid)
            with os.fdopen(descriptor, "wb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            os.link(temporary, destination, follow_symlinks=False)
            temporary.unlink()
            _fsync_directory(parent)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            if temporary.exists() and not temporary.is_symlink():
                temporary.unlink()
            raise

    def _publish_shim(self) -> None:
        parent = self._ensure_directory(self.profile.shim_link_path.parent)
        destination = self._path(self.profile.shim_link_path)
        temporary: Path | None = None
        for _ in range(16):
            candidate = parent / f".{destination.name}.{secrets.token_hex(8)}.tmp"
            try:
                os.symlink(str(self.profile.shim_path), candidate)
            except FileExistsError:
                continue
            temporary = candidate
            break
        if temporary is None:
            raise PersonalDevBuilderRuntimeInstallError("shim_publish_failed")
        try:
            os.lchown(temporary, self.context.authority_uid, self.context.authority_gid)
            os.link(temporary, destination, follow_symlinks=False)
            temporary.unlink()
            _fsync_directory(parent)
        except BaseException:
            if temporary.is_symlink():
                temporary.unlink()
            raise

    def install(self, archive: Path) -> dict[str, str]:
        self.preflight(archive)
        if not self._all_absent():
            self._verify_installed()
            return self._receipt("install", state="staged")
        release_parent = self._ensure_directory(self.profile.release_root.parent)
        stage = self._archive_stage(archive, release_parent)
        published = False
        try:
            self._freeze_stage(stage)
            _rename_noreplace(stage, self._path(self.profile.release_root))
            published = True
            _fsync_directory(release_parent)
        finally:
            if not published and stage.exists() and not stage.is_symlink():
                _safe_remove_stage(stage)
        self._publish_file(self.profile.profile_path, self.profile.payload)
        self._publish_file(self.profile.runsc_config_path, self.profile.runsc_toml)
        self._publish_file(self.profile.k3s_template_path, self.profile.k3s_template)
        self._publish_shim()
        self._verify_installed()
        return self._receipt("install", state="staged")

    def verify_staged(self) -> dict[str, str]:
        self._verify_installed()
        return self._receipt("verify-staged", state="staged")

    def verify_active(self) -> dict[str, str]:
        self._verify_installed()
        self._verify_host()
        payload = _read_regular(
            self._path(_ACTIVE_CONTAINERD_CONFIG),
            maximum=_MAX_CONFIG_BYTES,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
        )
        try:
            document = tomllib.loads(payload.decode("utf-8"))
            runtime = document["plugins"]["io.containerd.cri.v1.runtime"]["containerd"]["runtimes"][
                self.profile.handler
            ]
        except (KeyError, TypeError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise PersonalDevBuilderRuntimeInstallError("active_runtime_invalid") from exc
        expected = {
            "runtime_type": self.profile.runtime_type,
            "options": {
                "TypeUrl": "io.containerd.runsc.v1.options",
                "ConfigPath": str(self.profile.runsc_config_path),
            },
        }
        if runtime != expected:
            raise PersonalDevBuilderRuntimeInstallError("active_runtime_invalid")
        version = self._run(str(self.profile.runsc_path), "--version", check=False)
        version_lines = version.stdout.splitlines()
        if (
            version.returncode != 0
            or version.stderr
            or version_lines[:1] != [f"runsc version {self.profile.version}"]
            or len(version_lines) != 2
            or re.fullmatch(r"spec: 1[.]1[.]0(?:-rc[.][0-9]+)?", version_lines[1]) is None
        ):
            raise PersonalDevBuilderRuntimeInstallError("runsc_version_invalid")
        return self._receipt("verify-active", state="active")

    def _unlink(self, absolute: Path) -> None:
        path = self._path(absolute)
        path.unlink()
        _fsync_directory(path.parent)

    def remove(self) -> dict[str, str]:
        self._verify_installed()
        self._unlink(self.profile.shim_link_path)
        self._unlink(self.profile.k3s_template_path)
        self._unlink(self.profile.runsc_config_path)
        self._unlink(self.profile.profile_path)
        release = self._path(self.profile.release_root)
        release.chmod(0o700)
        for name in self.profile.members:
            parent = (release / name).parent
            if parent != release:
                parent.chmod(0o700)
        for name in sorted(
            self.profile.members, key=lambda item: len(Path(item).parts), reverse=True
        ):
            path = release / name
            path.unlink()
            _fsync_directory(path.parent)
        directories = sorted(
            {
                (release / name).parent
                for name in self.profile.members
                if (release / name).parent != release
            },
            key=lambda item: len(item.parts),
            reverse=True,
        )
        for directory in directories:
            directory.rmdir()
            _fsync_directory(directory.parent)
        release.rmdir()
        _fsync_directory(release.parent)
        for directory in (
            self._path(self.profile.release_root.parent),
            self._path(self.profile.release_root.parent.parent),
            self._path(self.profile.profile_path.parent),
        ):
            try:
                directory.rmdir()
            except OSError as exc:
                if exc.errno not in {errno.ENOTEMPTY, errno.ENOENT}:
                    raise PersonalDevBuilderRuntimeInstallError("remove_failed") from exc
            else:
                _fsync_directory(directory.parent)
        return self._receipt("remove", state="absent")


class _InstallerOperations(Protocol):
    def preflight(self, archive: Path) -> Mapping[str, object]: ...
    def install(self, archive: Path) -> Mapping[str, object]: ...
    def verify_staged(self) -> Mapping[str, object]: ...
    def verify_active(self) -> Mapping[str, object]: ...
    def remove(self) -> Mapping[str, object]: ...


InstallerFactory = Callable[[RuntimeProfile], _InstallerOperations]


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise PersonalDevBuilderRuntimeInstallError("arguments_invalid")


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(add_help=True)
    parser.add_argument(
        "operation",
        choices=("preflight", "install", "verify-staged", "verify-active", "remove"),
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--archive", type=Path)
    return parser


def _default_installer(profile: RuntimeProfile) -> _InstallerOperations:
    return PersonalDevBuilderRuntimeInstaller(
        profile=profile,
        context=InstallContext(),
        runner=SubprocessRunner(),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    installer_factory: InstallerFactory = _default_installer,
) -> int:
    try:
        arguments = _parser().parse_args(argv)
        needs_archive = arguments.operation in {"preflight", "install"}
        if needs_archive != (arguments.archive is not None):
            raise PersonalDevBuilderRuntimeInstallError("arguments_invalid")
        profile = load_runtime_profile(arguments.profile)
        installer = installer_factory(profile)
        if arguments.operation == "preflight":
            receipt = installer.preflight(arguments.archive)
        elif arguments.operation == "install":
            receipt = installer.install(arguments.archive)
        elif arguments.operation == "verify-staged":
            receipt = installer.verify_staged()
        elif arguments.operation == "verify-active":
            receipt = installer.verify_active()
        else:
            receipt = installer.remove()
        encoded = json.dumps(
            receipt,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        if len(encoded) > 1024:
            raise PersonalDevBuilderRuntimeInstallError("receipt_invalid")
        sys.stdout.write(encoded + "\n")
        return 0
    except PersonalDevBuilderRuntimeInstallError as exc:
        sys.stderr.write(f"error:{exc.code}\n")
        return 2 if exc.code == "arguments_invalid" else 1
    except RuntimeProfileError:
        sys.stderr.write("error:profile_invalid\n")
        return 1
    except Exception:
        sys.stderr.write("error:internal_error\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CommandResult",
    "InstallContext",
    "PersonalDevBuilderRuntimeInstallError",
    "PersonalDevBuilderRuntimeInstaller",
    "Runner",
    "SubprocessRunner",
    "main",
]
