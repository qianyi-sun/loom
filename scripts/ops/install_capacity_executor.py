#!/usr/bin/env python3
"""Install one digest-pinned capacity executor release without activating it."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import platform
import re
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, TYPE_CHECKING, Protocol

from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    PreparedControllerEvidence,
    PreparedControllerRequest,
)
from loom_cli.rollout.operator.protected_controller_discovery import (
    ControllerDiscoveryEvidence,
    ControllerDiscoveryRequest,
    controller_job_visibility_evidence_sha256,
)
from loom_cli.rollout.operator.protected_controller_prerequisite_component import (
    ControllerDirectoryEvidence,
    ControllerPrerequisiteEvidence,
    ControllerPrerequisiteRequest,
    capacity_executor_image_digest,
    controller_local_authority_sha256,
)
from loom_cli.rollout.operator.protected_pool_credential_transport import (
    FixedLocalPoolCredentialTransport,
    PoolExecutionCredentialEvidence,
    PoolExecutionCredentialPayload,
)

if TYPE_CHECKING:
    from scripts.ops.capacity_executor_release import (
        CapacityExecutorReleaseError,
        verify_release,
    )
else:
    try:
        from scripts.ops.capacity_executor_release import (
            CapacityExecutorReleaseError,
            verify_release,
        )
    except ModuleNotFoundError:  # Installed helper is colocated with the verifier.
        from capacity_executor_release import CapacityExecutorReleaseError, verify_release

_MAX_ARCHIVE_MEMBERS = 4096
_MAX_ARCHIVE_FILE_BYTES = 1024 * 1024 * 1024
_MAX_ARCHIVE_BYTES = 4 * 1024 * 1024 * 1024
_SOURCE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_CONTAINER_ID_RE = re.compile(r"^[0-9a-f]{12,64}$")
_SERVICE_USER = "loom_capacity_executor"
_SERVICE_GROUP = _SERVICE_USER
_SERVICE_HOME = Path("/var/lib/loom-capacity-executor")
_SERVICE_SHELL = "/usr/sbin/nologin"
_RELEASES_ROOT = Path("/opt/loom-capacity-executor-releases")
_CURRENT_RELEASE = Path("/opt/loom-capacity-executor")
_CONFIG_ROOT = Path("/etc/loom-capacity-executor")
_RUNTIME_ROOT = Path("/run/loom-capacity-executor")
_UNIT_ROOT = Path("/etc/systemd/system")
_TMPFILES_ROOT = Path("/etc/tmpfiles.d")
_TMPFILES_RELEASE_NAME = "loom-capacity-executor.tmpfiles"
_TMPFILES_DESTINATION_NAME = "loom-capacity-executor.conf"
_TMPFILES_PAYLOAD = (
    b"d /run/loom-capacity-executor 0700 loom_capacity_executor loom_capacity_executor -\n"
)
_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)
_QUIESCENT_ACTIVE_STATES = frozenset({"inactive"})
_QUIESCENT_UNIT_FILE_STATES = frozenset(
    {"disabled", "masked", "masked-runtime", "not-found", "static"}
)
_ROOT_ENV = {
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}
_DOCKER = "/usr/bin/docker"
_GETENT = "/usr/bin/getent"
_ID = "/usr/bin/id"
_IP = "/usr/sbin/ip"
_GROUPADD = "/usr/sbin/groupadd"
_USERADD = "/usr/sbin/useradd"
_USERMOD = "/usr/sbin/usermod"
_RUNUSER = "/usr/sbin/runuser"
_PYTHON = "/usr/bin/python3.12"
_SYSTEMD_ANALYZE = "/usr/bin/systemd-analyze"
_SYSTEMCTL = "/usr/bin/systemctl"
_SYSTEMD_TMPFILES = "/usr/bin/systemd-tmpfiles"
_SLURM_CONF = Path("/etc/slurm/slurm.conf")
_MAX_AUTHORITY_FILE_BYTES = 256 * 1024 * 1024
_MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024
_MAX_PREREQUISITE_REQUEST_BYTES = 2 * 1024 * 1024
_MAX_DISCOVERY_REQUEST_BYTES = 256 * 1024
_MAX_CREDENTIAL_REQUEST_BYTES = 8 * 1024 * 1024
_MAX_PREPARED_REQUEST_BYTES = 4 * 1024 * 1024
_PREPARED_OPERATIONS = frozenset(
    {
        "observe-prepared",
        "converge-prepared-files",
        "enable-prepared-timer",
        "run-prepared-tick",
        "disable-prepared-timer",
    }
)
_CONTROLLER_CREDENTIAL_HOSTS = {
    "gb10": "gx10-01c7",
    "oldlab": "TRT-EAI-OLDLAB-1",
}
_CONTROLLER_ARCHITECTURES = {"gb10": "arm64", "oldlab": "amd64"}
_CONTROLLER_CLUSTERS = {"gb10": "trt-gb10", "oldlab": "trt-oldlab"}
_CONTROLLER_TARGET_NODES = {
    "gb10": tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
    "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
}
_SLURM_EXECUTABLES = {
    name: Path(f"/usr/bin/{name}")
    for name in ("sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue")
}
_MANAGER_ROUTE_TARGET = "192.168.50.103"


class CapacityExecutorInstallError(RuntimeError):
    """The controller installation could not converge without weakening safety."""


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


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
        completed = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            check=False,
            env=env or _ROOT_ENV,
        )
        result = CommandResult(completed.returncode, completed.stdout, completed.stderr)
        if check and result.returncode != 0:
            raise CapacityExecutorInstallError(f"command failed safely: {Path(argv[0]).name}")
        return result


@dataclass(frozen=True, slots=True)
class InstallContext:
    """Map host-absolute paths and commands into direct or /host execution."""

    root: Path = Path("/")
    command_prefix: tuple[str, ...] = ()
    authority_uid: int = 0
    authority_gid: int = 0

    def __post_init__(self) -> None:
        if (
            not self.root.is_absolute()
            or ".." in self.root.parts
            or self.authority_uid < 0
            or self.authority_gid < 0
        ):
            raise CapacityExecutorInstallError("installer host context is invalid")

    def path(self, absolute: Path) -> Path:
        if not absolute.is_absolute() or ".." in absolute.parts:
            raise CapacityExecutorInstallError("installer path must be absolute and normalized")
        if self.root == Path("/"):
            return absolute
        return self.root.joinpath(*absolute.parts[1:])

    def argv(self, *argv: str) -> tuple[str, ...]:
        if not argv or any(not isinstance(value, str) or not value for value in argv):
            raise CapacityExecutorInstallError("installer command is invalid")
        return (*self.command_prefix, *argv)


@dataclass(frozen=True, slots=True)
class InstallResult:
    image: str
    source_sha: str
    architecture: str
    release_root: Path


@dataclass(frozen=True, slots=True)
class _PreparedLocalAuthority:
    uid: int
    gid: int
    release_root: Path
    unit_active_state: dict[str, str]
    unit_file_state: dict[str, str]


Extractor = Callable[[str, Path, Runner, InstallContext], None]


def _validate_image_reference(image: str) -> str:
    try:
        return capacity_executor_image_digest(image)
    except ValueError as exc:
        raise CapacityExecutorInstallError(
            "executor image must be an exact digest reference"
        ) from exc


def _archive_path(name: str) -> PurePosixPath | None:
    if not isinstance(name, str) or not name or "\x00" in name or "\\" in name:
        raise CapacityExecutorInstallError("release archive member path is invalid")
    while name.startswith("./"):
        name = name[2:]
    if name in {"", "."}:
        return None
    path = PurePosixPath(name.rstrip("/"))
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise CapacityExecutorInstallError("release archive member path is unsafe")
    return path


def _write_archive_file(bundle: tarfile.TarFile, member: tarfile.TarInfo, path: Path) -> None:
    if member.size < 0 or member.size > _MAX_ARCHIVE_FILE_BYTES:
        raise CapacityExecutorInstallError("release archive file size is unsafe")
    source = bundle.extractfile(member)
    if source is None:
        raise CapacityExecutorInstallError("release archive regular file is unavailable")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise CapacityExecutorInstallError("release archive file destination is unsafe") from exc
    remaining = member.size
    try:
        with os.fdopen(descriptor, "wb") as output:
            while remaining:
                chunk = source.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise CapacityExecutorInstallError("release archive file is truncated")
                output.write(chunk)
                remaining -= len(chunk)
            if source.read(1):
                raise CapacityExecutorInstallError("release archive file exceeds its declared size")
            output.flush()
            os.fsync(output.fileno())
    finally:
        source.close()
    path.chmod(0o444)


def _extract_release_tar(stream: IO[bytes], destination: Path) -> None:
    """Extract only canonical read-only directories and regular files."""

    if not isinstance(destination, Path) or not destination.is_absolute():
        raise CapacityExecutorInstallError("release archive destination must be absolute")
    if destination.is_symlink() or not destination.is_dir():
        raise CapacityExecutorInstallError("release archive destination must be a safe directory")
    seen: set[PurePosixPath] = set()
    directories: set[Path] = set()
    total_size = 0
    try:
        with tarfile.open(fileobj=stream, mode="r|*") as bundle:
            for index, member in enumerate(bundle, start=1):
                if index > _MAX_ARCHIVE_MEMBERS:
                    raise CapacityExecutorInstallError("release archive contains too many members")
                relative = _archive_path(member.name)
                if relative is None:
                    if not member.isdir():
                        raise CapacityExecutorInstallError("release archive root member is invalid")
                    continue
                if relative in seen:
                    raise CapacityExecutorInstallError(
                        "release archive contains a duplicate member"
                    )
                seen.add(relative)
                target = destination.joinpath(*relative.parts)
                if member.isdir():
                    if stat.S_IMODE(member.mode) != 0o555:
                        raise CapacityExecutorInstallError(
                            "release archive directory mode is invalid"
                        )
                    if target.exists() or target.is_symlink():
                        if target.is_symlink() or not target.is_dir():
                            raise CapacityExecutorInstallError(
                                "release archive directory destination is unsafe"
                            )
                    else:
                        target.mkdir(mode=0o700)
                    directories.add(target)
                    continue
                if not member.isreg() or stat.S_IMODE(member.mode) != 0o444:
                    raise CapacityExecutorInstallError(
                        "release archive contains a non-regular or writable member"
                    )
                total_size += member.size
                if total_size > _MAX_ARCHIVE_BYTES:
                    raise CapacityExecutorInstallError("release archive exceeds its byte bound")
                missing: list[Path] = []
                parent = target.parent
                while parent != destination and not parent.exists():
                    missing.append(parent)
                    parent = parent.parent
                if parent.is_symlink() or not parent.is_dir():
                    raise CapacityExecutorInstallError("release archive parent is unsafe")
                for directory in reversed(missing):
                    directory.mkdir(mode=0o700)
                    directories.add(directory)
                _write_archive_file(bundle, member, target)
    except (tarfile.TarError, OSError) as exc:
        raise CapacityExecutorInstallError("release archive extraction failed safely") from exc
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        directory.chmod(0o555)


def _extract_image_release(
    image: str,
    destination: Path,
    runner: Runner,
    context: InstallContext,
) -> None:
    created = runner.run(context.argv(_DOCKER, "create", image, "/bin/true"))
    container_id = created.stdout.strip()
    if _CONTAINER_ID_RE.fullmatch(container_id) is None:
        raise CapacityExecutorInstallError("executor artifact container identity is invalid")
    command = context.argv(
        _DOCKER,
        "cp",
        f"{container_id}:/opt/loom-capacity-executor-release/.",
        "-",
    )
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_ROOT_ENV,
        )
        if process.stdout is None or process.stderr is None:
            raise CapacityExecutorInstallError("executor artifact stream is unavailable")
        _extract_release_tar(process.stdout, destination)
        stderr = process.stderr.read(64 * 1024 + 1)
        returncode = process.wait()
        if returncode != 0 or len(stderr) > 64 * 1024:
            raise CapacityExecutorInstallError("executor artifact extraction failed safely")
    except BaseException:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        raise
    finally:
        runner.run(context.argv(_DOCKER, "rm", container_id), check=False)


def _architecture(machine: str) -> str:
    value = machine.strip().lower() if isinstance(machine, str) else ""
    if value in {"x86_64", "amd64"}:
        return "amd64"
    if value in {"aarch64", "arm64"}:
        return "arm64"
    raise CapacityExecutorInstallError("controller architecture is unsupported")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_remove_tree(path: Path) -> None:
    metadata = os.lstat(path)
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise CapacityExecutorInstallError("incomplete release cleanup target is unsafe")
    path.chmod(0o700)
    with os.scandir(path) as entries:
        for entry in entries:
            child = Path(entry.path)
            child_metadata = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(child_metadata.st_mode):
                _safe_remove_tree(child)
            else:
                child.unlink()
    path.rmdir()


def _metadata_identity(value: os.stat_result) -> tuple[int, ...]:
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


class ControllerInstaller:
    def __init__(
        self,
        *,
        context: InstallContext,
        runner: Runner,
        extractor: Extractor = _extract_image_release,
        machine: str | None = None,
        hostname: str | None = None,
        effective_uid: int | None = None,
    ) -> None:
        self.context = context
        self.runner = runner
        self.extractor = extractor
        self.machine = platform.machine() if machine is None else machine
        self.hostname = socket.gethostname().split(".", 1)[0] if hostname is None else hostname
        self.effective_uid = os.geteuid() if effective_uid is None else effective_uid

    def _run(
        self,
        *argv: str,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        return self.runner.run(
            self.context.argv(*argv),
            check=check,
            env=env,
        )

    def _path(self, absolute: Path) -> Path:
        return self.context.path(absolute)

    def _run_as_service(self, *argv: str) -> CommandResult:
        return self._run(_RUNUSER, "--user", _SERVICE_USER, "--", *argv)

    def _assert_quiescent(self) -> None:
        for unit in _UNITS:
            active = self._run(_SYSTEMCTL, "is-active", unit, check=False)
            enabled = self._run(_SYSTEMCTL, "is-enabled", unit, check=False)
            active_state = active.stdout.strip()
            enabled_state = enabled.stdout.strip()
            if (
                active_state not in _QUIESCENT_ACTIVE_STATES
                or enabled_state not in _QUIESCENT_UNIT_FILE_STATES
            ):
                raise CapacityExecutorInstallError(
                    "existing capacity executor units are active or enabled"
                )

    def _assert_current_destination_safe(self) -> str | None:
        current = self._path(_CURRENT_RELEASE)
        if not current.exists() and not current.is_symlink():
            return None
        metadata = os.lstat(current)
        if not stat.S_ISLNK(metadata.st_mode):
            raise CapacityExecutorInstallError("current executor release path is not a symlink")
        target = os.readlink(current)
        target_path = Path(target)
        if (
            not target_path.is_absolute()
            or target_path.parent != _RELEASES_ROOT
            or ".." in target_path.parts
        ):
            raise CapacityExecutorInstallError("current executor release symlink is foreign")
        mapped = self._path(target_path)
        if mapped.is_symlink() or not mapped.is_dir():
            raise CapacityExecutorInstallError("current executor release target is unsafe")
        return target

    def _inspect_image(
        self,
        image: str,
        *,
        source_sha: str,
        architecture: str,
        pull: bool = True,
    ) -> None:
        if pull:
            self._run(_DOCKER, "pull", "--quiet", image)
        inspected = self._run(_DOCKER, "image", "inspect", image)
        try:
            payload = json.loads(inspected.stdout)
        except (TypeError, ValueError) as exc:
            raise CapacityExecutorInstallError("executor OCI inspection is invalid") from exc
        if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
            raise CapacityExecutorInstallError("executor OCI inspection is invalid")
        image_data = payload[0]
        config = image_data.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        repo_digests = image_data.get("RepoDigests")
        if image_data.get("Os") != "linux" or image_data.get("Architecture") != architecture:
            raise CapacityExecutorInstallError("executor OCI architecture differs from controller")
        if (
            not isinstance(labels, dict)
            or labels.get("org.opencontainers.image.revision") != source_sha
        ):
            raise CapacityExecutorInstallError("executor OCI revision differs from expected source")
        if not isinstance(repo_digests, list) or image not in repo_digests:
            raise CapacityExecutorInstallError("executor OCI digest identity is unavailable")

    def _ensure_authority_tree(self, absolute: Path) -> None:
        if not absolute.is_absolute() or ".." in absolute.parts:
            raise CapacityExecutorInstallError("installer authority path is invalid")
        current = self.context.root
        root_metadata = os.lstat(current)
        if (
            stat.S_ISLNK(root_metadata.st_mode)
            or not stat.S_ISDIR(root_metadata.st_mode)
            or root_metadata.st_uid != self.context.authority_uid
            or root_metadata.st_gid != self.context.authority_gid
            or stat.S_IMODE(root_metadata.st_mode) & 0o022
        ):
            raise CapacityExecutorInstallError("installer authority root is unsafe")
        for part in absolute.parts[1:]:
            current /= part
            if current.exists() or current.is_symlink():
                metadata = os.lstat(current)
                if (
                    stat.S_ISLNK(metadata.st_mode)
                    or not stat.S_ISDIR(metadata.st_mode)
                    or metadata.st_uid != self.context.authority_uid
                    or metadata.st_gid != self.context.authority_gid
                    or stat.S_IMODE(metadata.st_mode) & 0o022
                ):
                    raise CapacityExecutorInstallError(
                        f"installer authority directory is unsafe: {absolute}"
                    )
                continue
            parent = current.parent
            current.mkdir(mode=0o755)
            current.chmod(0o755)
            os.chown(current, self.context.authority_uid, self.context.authority_gid)
            _fsync_directory(parent)

    def _ensure_directory(self, absolute: Path, *, mode: int, uid: int, gid: int) -> None:
        path = self._path(absolute)
        if path.exists() or path.is_symlink():
            metadata = os.lstat(path)
            if (
                stat.S_ISLNK(metadata.st_mode)
                or not stat.S_ISDIR(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != mode
                or metadata.st_uid != uid
                or metadata.st_gid != gid
            ):
                raise CapacityExecutorInstallError(f"installer directory is unsafe: {absolute}")
            return
        self._ensure_authority_tree(absolute.parent)
        parent = path.parent
        path.mkdir(mode=mode)
        os.chown(path, uid, gid)
        metadata = os.lstat(path)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_uid != uid
            or metadata.st_gid != gid
        ):
            raise CapacityExecutorInstallError(f"installer directory did not converge: {absolute}")
        _fsync_directory(parent)

    def _service_ids(self, *, create: bool = True) -> tuple[int, int]:
        group = self._run(_GETENT, "group", _SERVICE_GROUP, check=False)
        passwd = self._run(_GETENT, "passwd", _SERVICE_USER, check=False)
        if not create and (group.returncode != 0 or passwd.returncode != 0):
            raise CapacityExecutorInstallError("executor service identity authority is unavailable")
        if group.returncode != 0:
            if passwd.returncode == 0:
                raise CapacityExecutorInstallError("executor service group is unavailable")
            self._run(_GROUPADD, "--system", _SERVICE_GROUP)
        if passwd.returncode != 0:
            self._run(
                _USERADD,
                "--system",
                "--gid",
                _SERVICE_GROUP,
                "--home-dir",
                str(_SERVICE_HOME),
                "--no-create-home",
                "--shell",
                _SERVICE_SHELL,
                _SERVICE_USER,
            )
        rollout_group_fields: list[str] | None = None
        if self.hostname == _CONTROLLER_CREDENTIAL_HOSTS["oldlab"]:
            rollout_group = self._run(_GETENT, "group", "loom-rollout", check=False)
            if rollout_group.returncode != 0:
                raise CapacityExecutorInstallError(
                    "executor service identity authority is unavailable"
                )
            rollout_group_fields = rollout_group.stdout.strip().split(":")
            if (
                len(rollout_group_fields) != 4
                or rollout_group_fields[0] != "loom-rollout"
                or not rollout_group_fields[2].isdigit()
            ):
                raise CapacityExecutorInstallError("executor service identity authority is unsafe")
            members = rollout_group_fields[3].split(",") if rollout_group_fields[3] else []
            if len(members) != len(set(members)) or any(not member for member in members):
                raise CapacityExecutorInstallError("executor service identity authority is unsafe")
            if _SERVICE_USER not in members:
                if not create:
                    raise CapacityExecutorInstallError(
                        "executor service identity authority is unavailable"
                    )
                self._run(
                    _USERMOD,
                    "--append",
                    "--groups",
                    "loom-rollout",
                    _SERVICE_USER,
                )
                rollout_group = self._run(_GETENT, "group", "loom-rollout")
                rollout_group_fields = rollout_group.stdout.strip().split(":")
                members = (
                    rollout_group_fields[3].split(",")
                    if len(rollout_group_fields) == 4 and rollout_group_fields[3]
                    else []
                )
                if members.count(_SERVICE_USER) != 1:
                    raise CapacityExecutorInstallError(
                        "executor service identity authority did not converge"
                    )
        group = self._run(_GETENT, "group", _SERVICE_GROUP)
        passwd = self._run(_GETENT, "passwd", _SERVICE_USER)
        uid_result = self._run(_ID, "-u", _SERVICE_USER)
        gid_result = self._run(_ID, "-g", _SERVICE_USER)
        supplementary_gid_result = self._run(_ID, "-G", _SERVICE_USER)
        group_fields = group.stdout.strip().split(":")
        passwd_fields = passwd.stdout.strip().split(":")
        supplementary_gid_fields = supplementary_gid_result.stdout.strip().split()
        if (
            len(group_fields) != 4
            or group_fields[0] != _SERVICE_GROUP
            or not group_fields[2].isdigit()
            or bool(group_fields[3])
            or len(passwd_fields) != 7
            or passwd_fields[0] != _SERVICE_USER
            or not passwd_fields[2].isdigit()
            or not passwd_fields[3].isdigit()
            or passwd_fields[5] != str(_SERVICE_HOME)
            or passwd_fields[6] != _SERVICE_SHELL
            or not uid_result.stdout.strip().isdigit()
            or not gid_result.stdout.strip().isdigit()
            or not supplementary_gid_fields
            or any(not value.isdigit() for value in supplementary_gid_fields)
        ):
            raise CapacityExecutorInstallError("executor service identity is unsafe")
        uids = {int(passwd_fields[2]), int(uid_result.stdout.strip())}
        gids = {
            int(group_fields[2]),
            int(passwd_fields[3]),
            int(gid_result.stdout.strip()),
        }
        expected_gids = set(gids)
        if rollout_group_fields is not None:
            expected_gids.add(int(rollout_group_fields[2]))
        if (
            len(uids) != 1
            or len(gids) != 1
            or 0 in uids
            or 0 in gids
            or {int(value) for value in supplementary_gid_fields} != expected_gids
        ):
            raise CapacityExecutorInstallError("executor service identity is inconsistent")
        return uids.pop(), gids.pop()

    def _bounded_stdout(self, result: CommandResult, *, label: str) -> str:
        output = result.stdout
        if (
            not isinstance(output, str)
            or len(output.encode("utf-8")) > _MAX_COMMAND_OUTPUT_BYTES
            or "\x00" in output
        ):
            raise CapacityExecutorInstallError(f"{label} output is invalid")
        return output

    def _job_visibility_evidence(
        self,
        *,
        pool_id: str,
        partition_fields: dict[str, str],
    ) -> str:
        association_fields: tuple[str, ...] = ()
        if pool_id == "gb10":
            association_output = self._bounded_stdout(
                self._run_as_service(
                    str(_SLURM_EXECUTABLES["sacctmgr"]),
                    "--noheader",
                    "--parsable2",
                    "show",
                    "association",
                    "where",
                    "Cluster=trt-gb10",
                    "Account=loom-staging",
                    f"User={_SERVICE_USER}",
                    "format=Cluster,Account,User,Partition,QOS,DefaultQOS",
                ),
                label="Slurm admission",
            )
            lines = [line.removesuffix("|") for line in association_output.splitlines() if line]
            if len(lines) != 1:
                raise CapacityExecutorInstallError("controller discovery Slurm admission drifted")
            association_fields = tuple(lines[0].split("|"))
        try:
            return controller_job_visibility_evidence_sha256(
                pool_id=pool_id,
                partition_fields=partition_fields,
                association_fields=association_fields,
            )
        except ValueError as exc:
            raise CapacityExecutorInstallError(
                "controller discovery Slurm admission drifted"
            ) from exc

    def _authority_file_sha256(
        self,
        absolute: Path,
        *,
        executable: bool,
    ) -> str:
        path = self._path(absolute)
        try:
            descriptor = os.open(
                path,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise CapacityExecutorInstallError("controller authority file is unavailable") from exc
        try:
            before = os.fstat(descriptor)
            mode = stat.S_IMODE(before.st_mode)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_uid != self.context.authority_uid
                or before.st_gid != self.context.authority_gid
                or mode & 0o022
                or (executable and not mode & 0o111)
                or not 0 < before.st_size <= _MAX_AUTHORITY_FILE_BYTES
            ):
                raise CapacityExecutorInstallError("controller authority file metadata is unsafe")
            digest = hashlib.sha256()
            remaining = before.st_size
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    raise CapacityExecutorInstallError(
                        "controller authority file changed while reading"
                    )
                digest.update(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
            if _metadata_identity(before) != _metadata_identity(after):
                raise CapacityExecutorInstallError(
                    "controller authority file changed while reading"
                )
            return digest.hexdigest()
        finally:
            os.close(descriptor)

    def _local_authority(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> tuple[int, int, dict[str, str], dict[str, str]]:
        if (
            not isinstance(request, ControllerPrerequisiteRequest)
            or self.hostname != request.binding.controller_host
            or _architecture(self.machine) != request.architecture
        ):
            raise CapacityExecutorInstallError("controller prerequisite host authority drifted")
        uid, gid = self._service_ids(create=False)
        if uid != request.binding.local_uid:
            raise CapacityExecutorInstallError("controller prerequisite service authority drifted")
        executable_paths = request.binding.slurm_executables.model_dump(mode="python")
        executable_sha256 = {
            name: self._authority_file_sha256(Path(path), executable=True)
            for name, path in executable_paths.items()
        }
        configuration_sha256 = {
            "slurm.conf": self._authority_file_sha256(_SLURM_CONF, executable=False)
        }
        cluster_output = self._bounded_stdout(
            self._run_as_service(str(executable_paths["scontrol"]), "show", "config"),
            label="Slurm configuration",
        )
        clusters = [
            line.split("=", 1)[1].strip()
            for line in cluster_output.splitlines()
            if "=" in line and line.split("=", 1)[0].strip() == "ClusterName"
        ]
        partition_output = self._bounded_stdout(
            self._run_as_service(
                str(executable_paths["scontrol"]),
                "show",
                "partition",
                request.binding.partition,
                "-o",
            ),
            label="Slurm partition",
        )
        partition_lines = [line for line in partition_output.splitlines() if line]
        fields: dict[str, str] = {}
        if len(partition_lines) == 1:
            for token in partition_lines[0].split():
                if "=" in token:
                    key, value = token.split("=", 1)
                    if key in fields:
                        raise CapacityExecutorInstallError(
                            "controller prerequisite Slurm authority drifted"
                        )
                    fields[key] = value
        nodes_expression = fields.get("Nodes")
        if (
            clusters != [request.binding.slurm_cluster]
            or fields.get("PartitionName") != request.binding.partition
            or not nodes_expression
        ):
            raise CapacityExecutorInstallError("controller prerequisite Slurm authority drifted")
        nodes_output = self._bounded_stdout(
            self._run_as_service(
                str(executable_paths["scontrol"]),
                "show",
                "hostnames",
                nodes_expression,
            ),
            label="Slurm node inventory",
        )
        nodes = tuple(line.strip() for line in nodes_output.splitlines() if line.strip())
        if len(nodes) != len(set(nodes)) or set(nodes) != set(request.target_nodes):
            raise CapacityExecutorInstallError("controller prerequisite Slurm authority drifted")
        job_visibility_evidence_sha256 = self._job_visibility_evidence(
            pool_id=request.pool_id,
            partition_fields=fields,
        )
        if (
            job_visibility_evidence_sha256
            != request.binding.inventory.job_visibility_evidence_sha256
        ):
            raise CapacityExecutorInstallError(
                "controller prerequisite Slurm admission authority drifted"
            )
        local_authority = controller_local_authority_sha256(
            pool_id=request.pool_id,
            architecture=request.architecture,
            controller_hostname=self.hostname,
            service_uid=uid,
            service_gid=gid,
            slurm_cluster=request.binding.slurm_cluster,
            partition=request.binding.partition,
            target_nodes=request.target_nodes,
            executable_sha256=executable_sha256,
            configuration_sha256=configuration_sha256,
            job_visibility_evidence_sha256=job_visibility_evidence_sha256,
        )
        if local_authority != request.binding.local_authority_sha256:
            raise CapacityExecutorInstallError("controller prerequisite local authority drifted")
        return uid, gid, executable_sha256, configuration_sha256

    def discover_controller(
        self,
        request: ControllerDiscoveryRequest,
    ) -> ControllerDiscoveryEvidence:
        """Capture only stable, non-secret controller-local facts."""

        if self.effective_uid != 0:
            raise CapacityExecutorInstallError("controller discovery requires root")
        if not isinstance(request, ControllerDiscoveryRequest):
            raise CapacityExecutorInstallError("controller discovery request is invalid")
        expected_host = _CONTROLLER_CREDENTIAL_HOSTS.get(request.pool_id)
        expected_architecture = _CONTROLLER_ARCHITECTURES.get(request.pool_id)
        expected_cluster = _CONTROLLER_CLUSTERS.get(request.pool_id)
        expected_nodes = _CONTROLLER_TARGET_NODES.get(request.pool_id)
        if (
            self.hostname != expected_host
            or _architecture(self.machine) != expected_architecture
            or expected_cluster is None
            or expected_nodes is None
        ):
            raise CapacityExecutorInstallError("controller discovery host authority drifted")
        uid, gid = self._service_ids(create=False)
        executable_sha256 = {
            name: self._authority_file_sha256(path, executable=True)
            for name, path in _SLURM_EXECUTABLES.items()
        }
        configuration_sha256 = {
            "slurm.conf": self._authority_file_sha256(_SLURM_CONF, executable=False)
        }
        cluster_output = self._bounded_stdout(
            self._run_as_service(str(_SLURM_EXECUTABLES["scontrol"]), "show", "config"),
            label="Slurm configuration",
        )
        clusters = [
            line.split("=", 1)[1].strip()
            for line in cluster_output.splitlines()
            if "=" in line and line.split("=", 1)[0].strip() == "ClusterName"
        ]
        partition_output = self._bounded_stdout(
            self._run_as_service(
                str(_SLURM_EXECUTABLES["scontrol"]),
                "show",
                "partition",
                "loom-staging",
                "-o",
            ),
            label="Slurm partition",
        )
        partition_lines = [line for line in partition_output.splitlines() if line]
        partition_fields: dict[str, str] = {}
        if len(partition_lines) == 1:
            for token in partition_lines[0].split():
                if "=" in token:
                    key, value = token.split("=", 1)
                    if key in partition_fields:
                        raise CapacityExecutorInstallError(
                            "controller discovery Slurm authority drifted"
                        )
                    partition_fields[key] = value
        nodes_expression = partition_fields.get("Nodes")
        if (
            clusters != [expected_cluster]
            or partition_fields.get("PartitionName") != "loom-staging"
            or not nodes_expression
        ):
            raise CapacityExecutorInstallError("controller discovery Slurm authority drifted")
        nodes_output = self._bounded_stdout(
            self._run_as_service(
                str(_SLURM_EXECUTABLES["scontrol"]),
                "show",
                "hostnames",
                nodes_expression,
            ),
            label="Slurm node inventory",
        )
        nodes = tuple(line.strip() for line in nodes_output.splitlines() if line.strip())
        if len(nodes) != len(set(nodes)) or set(nodes) != set(expected_nodes):
            raise CapacityExecutorInstallError("controller discovery Slurm authority drifted")
        job_visibility_evidence_sha256 = self._job_visibility_evidence(
            pool_id=request.pool_id,
            partition_fields=partition_fields,
        )
        version_output = self._bounded_stdout(
            self._run_as_service(str(_SLURM_EXECUTABLES["scontrol"]), "--version"),
            label="Slurm version",
        ).strip()
        matched_version = re.fullmatch(r"slurm-wlm ([0-9]+)\.([0-9]+)\.([0-9]+)", version_output)
        if matched_version is None:
            raise CapacityExecutorInstallError("controller discovery Slurm version drifted")
        slurm_version = (
            int(matched_version.group(1)),
            int(matched_version.group(2)),
            int(matched_version.group(3)),
        )
        metadata_output = self._bounded_stdout(
            self._run_as_service(
                str(_SLURM_EXECUTABLES["scontrol"]),
                "show",
                "nodes",
                nodes_expression,
                "--json",
            ),
            label="Slurm metadata",
        )
        try:
            metadata = json.loads(metadata_output)
            meta = metadata["meta"]
            slurm = meta["slurm"]
            version = slurm["version"]
            plugin = meta["plugin"]
            raw_metadata_nodes = metadata["nodes"]
            observed_version = tuple(int(version[name]) for name in ("major", "minor", "micro"))
            data_parser = plugin["data_parser"]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CapacityExecutorInstallError(
                "controller discovery Slurm metadata drifted"
            ) from exc
        if (
            not isinstance(metadata, dict)
            or not isinstance(meta, dict)
            or not isinstance(slurm, dict)
            or not isinstance(version, dict)
            or not isinstance(plugin, dict)
            or not isinstance(raw_metadata_nodes, list)
            or slurm.get("cluster") != expected_cluster
            or observed_version != slurm_version
            or not isinstance(data_parser, str)
        ):
            raise CapacityExecutorInstallError("controller discovery Slurm metadata drifted")
        metadata_nodes = tuple(
            item.get("name") if isinstance(item, dict) else None for item in raw_metadata_nodes
        )
        if (
            any(not isinstance(node, str) for node in metadata_nodes)
            or len(metadata_nodes) != len(set(metadata_nodes))
            or set(metadata_nodes) != set(expected_nodes)
        ):
            raise CapacityExecutorInstallError("controller discovery Slurm metadata drifted")
        route_output = self._bounded_stdout(
            self._run_as_service(_IP, "-json", "route", "get", _MANAGER_ROUTE_TARGET),
            label="manager route",
        )
        try:
            routes = json.loads(route_output)
            route = routes[0]
            route_source = ipaddress.ip_address(route["prefsrc"])
        except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CapacityExecutorInstallError(
                "controller discovery manager route drifted"
            ) from exc
        if (
            not isinstance(routes, list)
            or len(routes) != 1
            or not isinstance(route, dict)
            or route.get("dst") != _MANAGER_ROUTE_TARGET
            or not isinstance(route_source, ipaddress.IPv4Address)
            or not route_source.is_private
        ):
            raise CapacityExecutorInstallError("controller discovery manager route drifted")
        local_authority = controller_local_authority_sha256(
            pool_id=request.pool_id,
            architecture=expected_architecture,
            controller_hostname=self.hostname,
            service_uid=uid,
            service_gid=gid,
            slurm_cluster=expected_cluster,
            partition="loom-staging",
            target_nodes=expected_nodes,
            executable_sha256=executable_sha256,
            configuration_sha256=configuration_sha256,
            job_visibility_evidence_sha256=job_visibility_evidence_sha256,
        )
        try:
            return ControllerDiscoveryEvidence(
                schema_version=1,
                pool_id=request.pool_id,
                transport_authority_sha256=request.transport_authority_sha256,
                controller_hostname=self.hostname,
                architecture=expected_architecture,
                service_user=_SERVICE_USER,
                service_uid=uid,
                service_gid=gid,
                slurm_cluster=expected_cluster,
                partition="loom-staging",
                target_nodes=expected_nodes,
                slurm_version=slurm_version,
                data_parser=data_parser,
                query_principal=_SERVICE_USER,
                manager_client_cidr=f"{route_source}/32",
                executable_sha256=executable_sha256,
                configuration_sha256=configuration_sha256,
                job_visibility_evidence_sha256=job_visibility_evidence_sha256,
                local_authority_sha256=local_authority,
            )
        except ValueError as exc:
            raise CapacityExecutorInstallError("controller discovery evidence is invalid") from exc

    def _release_root(self, *, source_sha: str, architecture: str, digest: str) -> Path:
        return _RELEASES_ROOT / f"{source_sha}-{architecture}-{digest}"

    def _verify_runtime_files(self, release_root: Path) -> None:
        root = self._path(release_root)
        root_metadata = os.lstat(root)
        if (
            not stat.S_ISDIR(root_metadata.st_mode)
            or root_metadata.st_uid != self.context.authority_uid
            or root_metadata.st_gid != self.context.authority_gid
            or stat.S_IMODE(root_metadata.st_mode) != 0o555
        ):
            raise CapacityExecutorInstallError("installed executor runtime authority is unsafe")
        allowed_symlink = root / "venv/lib64"
        for path in root.rglob("*"):
            metadata = os.lstat(path)
            if metadata.st_uid != self.context.authority_uid or (
                metadata.st_gid != self.context.authority_gid
            ):
                raise CapacityExecutorInstallError("installed executor runtime authority is unsafe")
            if stat.S_ISLNK(metadata.st_mode):
                if path != allowed_symlink or os.readlink(path) != "lib":
                    raise CapacityExecutorInstallError(
                        "installed executor runtime authority is unsafe"
                    )
            elif stat.S_ISDIR(metadata.st_mode):
                if stat.S_IMODE(metadata.st_mode) != 0o555:
                    raise CapacityExecutorInstallError(
                        "installed executor runtime authority is unsafe"
                    )
            elif stat.S_ISREG(metadata.st_mode):
                if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) not in {
                    0o444,
                    0o555,
                }:
                    raise CapacityExecutorInstallError(
                        "installed executor runtime authority is unsafe"
                    )
            else:
                raise CapacityExecutorInstallError("installed executor runtime authority is unsafe")
        for relative in (
            Path("venv/bin/python"),
            Path("venv/bin/loom-capacity-trusted-launcher"),
        ):
            path = root / relative
            if path.is_symlink() or not path.is_file():
                raise CapacityExecutorInstallError("installed executor runtime is incomplete")
            metadata = os.lstat(path)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.context.authority_uid
                or metadata.st_gid != self.context.authority_gid
                or stat.S_IMODE(metadata.st_mode) & 0o022
                or not (stat.S_IMODE(metadata.st_mode) & 0o111)
            ):
                raise CapacityExecutorInstallError("installed executor runtime authority is unsafe")

    def _offline_install(self, release_root: Path) -> None:
        mapped = self._path(release_root)
        requirements = mapped / "payload/requirements.lock"
        wheelhouse = mapped / "payload/wheelhouse"
        if requirements.is_symlink() or not requirements.is_file():
            raise CapacityExecutorInstallError("executor requirements lock is unavailable")
        wheels = sorted(wheelhouse.glob("loom-*.whl"))
        if len(wheels) != 1 or wheels[0].is_symlink() or not wheels[0].is_file():
            raise CapacityExecutorInstallError("executor project wheel is unavailable")
        venv = release_root / "venv"
        python = venv / "bin/python"
        self._run(_PYTHON, "-m", "venv", "--copies", str(venv))
        pip_env = {
            **_ROOT_ENV,
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            "PIP_NO_INDEX": "1",
        }
        self._run(
            str(python),
            "-m",
            "pip",
            "install",
            "--quiet",
            "--require-hashes",
            "--find-links",
            str(release_root / "payload/wheelhouse"),
            "--requirement",
            str(release_root / "payload/requirements.lock"),
            env=pip_env,
        )
        self._run(
            str(python),
            "-m",
            "pip",
            "install",
            "--quiet",
            "--no-deps",
            str(release_root / "payload/wheelhouse" / wheels[0].name),
            env=pip_env,
        )

    def _probe_runtime(self, release_root: Path) -> None:
        python = release_root / "venv/bin/python"
        launcher = release_root / "venv/bin/loom-capacity-trusted-launcher"
        self._run(
            str(python),
            "-I",
            "-B",
            "-c",
            (
                "import loom, loom_capacity_agent, loom_capacity_executor, "
                "loom_capacity_guard, loom_capacity_manager, "
                "loom_capacity_pool_controller, loom_capacity_pool_executor"
            ),
        )
        self._run(
            str(python),
            "-I",
            "-B",
            "-m",
            "loom_capacity_pool_controller",
            "--help",
        )
        self._run(str(launcher), "--help")

    def _freeze_release(self, release_root: Path) -> None:
        root = self._path(release_root)
        allowed_symlink = root / "venv/lib64"
        for path in sorted(root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
            metadata = os.lstat(path)
            if stat.S_ISLNK(metadata.st_mode):
                if path != allowed_symlink or os.readlink(path) != "lib":
                    raise CapacityExecutorInstallError(
                        "installed executor runtime contains a symlink"
                    )
                os.chown(
                    path,
                    self.context.authority_uid,
                    self.context.authority_gid,
                    follow_symlinks=False,
                )
                continue
            os.chown(path, self.context.authority_uid, self.context.authority_gid)
            if stat.S_ISDIR(metadata.st_mode):
                path.chmod(0o555)
            elif stat.S_ISREG(metadata.st_mode):
                path.chmod(0o555 if stat.S_IMODE(metadata.st_mode) & 0o111 else 0o444)
            else:
                raise CapacityExecutorInstallError("installed executor runtime entry is unsafe")
        os.chown(root, self.context.authority_uid, self.context.authority_gid)
        root.chmod(0o555)
        _fsync_directory(root.parent)

    def _prepare_release(
        self,
        *,
        image: str,
        source_sha: str,
        architecture: str,
        digest: str,
        current_target: str | None,
    ) -> Path:
        self._ensure_directory(
            _RELEASES_ROOT,
            mode=0o755,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
        )
        release_root = self._release_root(
            source_sha=source_sha,
            architecture=architecture,
            digest=digest,
        )
        root = self._path(release_root)
        marker = root / ".installing"
        if root.exists() or root.is_symlink():
            if root.is_symlink() or not root.is_dir():
                raise CapacityExecutorInstallError("versioned executor release path is unsafe")
            if marker.exists() and current_target != str(release_root):
                _safe_remove_tree(root)
            else:
                try:
                    verify_release(
                        root,
                        expected_source_sha=source_sha,
                        expected_architecture=architecture,
                    )
                except CapacityExecutorReleaseError as exc:
                    raise CapacityExecutorInstallError(
                        "existing versioned executor release is invalid"
                    ) from exc
                self._verify_runtime_files(release_root)
                self._probe_runtime(release_root)
                return release_root
        root.mkdir(mode=0o700)
        os.chown(root, self.context.authority_uid, self.context.authority_gid)
        marker.write_text(f"{image}\n{source_sha}\n{architecture}\n", encoding="utf-8")
        marker.chmod(0o600)
        os.chown(marker, self.context.authority_uid, self.context.authority_gid)
        created = True
        try:
            self.extractor(image, root, self.runner, self.context)
            verify_release(
                root,
                expected_source_sha=source_sha,
                expected_architecture=architecture,
            )
            self._offline_install(release_root)
            marker.unlink()
            self._freeze_release(release_root)
            verify_release(
                root,
                expected_source_sha=source_sha,
                expected_architecture=architecture,
            )
            self._verify_runtime_files(release_root)
            self._probe_runtime(release_root)
            return release_root
        except CapacityExecutorReleaseError as exc:
            if created and root.exists() and not root.is_symlink():
                _safe_remove_tree(root)
            raise CapacityExecutorInstallError(
                "executor release verification failed safely"
            ) from exc
        except BaseException:
            if created and root.exists() and not root.is_symlink():
                _safe_remove_tree(root)
            raise

    def _atomic_write(self, absolute: Path, payload: bytes, *, mode: int) -> None:
        path = self._path(absolute)
        self._ensure_authority_tree(absolute.parent)
        parent = path.parent
        if path.exists() or path.is_symlink():
            metadata = os.lstat(path)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise CapacityExecutorInstallError("installer destination is unsafe")
            if metadata.st_nlink != 1:
                raise CapacityExecutorInstallError("installer destination link count is unsafe")
            if (
                metadata.st_uid == self.context.authority_uid
                and metadata.st_gid == self.context.authority_gid
                and stat.S_IMODE(metadata.st_mode) == mode
                and path.read_bytes() == payload
            ):
                return
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, mode)
            os.fchown(descriptor, self.context.authority_uid, self.context.authority_gid)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            _fsync_directory(parent)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temporary.unlink(missing_ok=True)
            raise

    def _install_assets(self, release_root: Path, *, uid: int, gid: int) -> None:
        root = self._path(release_root)
        unit_source = root / "payload/units"
        observed = {
            path.name for path in unit_source.iterdir() if path.is_file() and not path.is_symlink()
        }
        if observed != set(_UNITS):
            raise CapacityExecutorInstallError("executor release unit set is incomplete")
        self._ensure_directory(
            _UNIT_ROOT,
            mode=0o755,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
        )
        self._ensure_directory(
            _TMPFILES_ROOT,
            mode=0o755,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
        )
        for unit in _UNITS:
            source = unit_source / unit
            if source.is_symlink() or not source.is_file():
                raise CapacityExecutorInstallError("executor release unit is unsafe")
            self._atomic_write(_UNIT_ROOT / unit, source.read_bytes(), mode=0o644)
        self._run(
            _SYSTEMD_ANALYZE,
            "verify",
            *(str(_UNIT_ROOT / unit) for unit in _UNITS),
        )
        tmpfiles_source = root / "payload/tmpfiles" / _TMPFILES_RELEASE_NAME
        if (
            tmpfiles_source.is_symlink()
            or not tmpfiles_source.is_file()
            or tmpfiles_source.read_bytes() != _TMPFILES_PAYLOAD
        ):
            raise CapacityExecutorInstallError("executor release tmpfiles policy is invalid")
        tmpfiles_destination = _TMPFILES_ROOT / _TMPFILES_DESTINATION_NAME
        self._atomic_write(tmpfiles_destination, _TMPFILES_PAYLOAD, mode=0o644)
        self._ensure_directory(_CONFIG_ROOT, mode=0o700, uid=uid, gid=gid)
        self._ensure_directory(_SERVICE_HOME, mode=0o700, uid=uid, gid=gid)
        self._run(_SYSTEMD_TMPFILES, "--create", str(tmpfiles_destination))
        runtime = self._path(_RUNTIME_ROOT)
        metadata = os.lstat(runtime)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != uid
            or metadata.st_gid != gid
        ):
            raise CapacityExecutorInstallError("executor runtime directory is unsafe")

    def _publish(self, release_root: Path) -> None:
        current = self._path(_CURRENT_RELEASE)
        self._ensure_authority_tree(_CURRENT_RELEASE.parent)
        parent = current.parent
        if parent.is_symlink() or not parent.is_dir():
            raise CapacityExecutorInstallError("executor release publication parent is unsafe")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".loom-capacity-executor.",
            dir=parent,
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        temporary.unlink()
        try:
            os.symlink(str(release_root), temporary)
            os.replace(temporary, current)
            _fsync_directory(parent)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        if not current.is_symlink() or os.readlink(current) != str(release_root):
            raise CapacityExecutorInstallError("executor release publication did not converge")

    def _private_directory_evidence(
        self,
        absolute: Path,
        *,
        uid: int,
        gid: int,
    ) -> ControllerDirectoryEvidence:
        path = self._path(absolute)
        metadata = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != uid
            or metadata.st_gid != gid
        ):
            raise CapacityExecutorInstallError("controller prerequisite directory is unsafe")
        return ControllerDirectoryEvidence(path=str(absolute), mode=0o700, uid=uid, gid=gid)

    def _pool_credential_transport(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> FixedLocalPoolCredentialTransport:
        if (
            self.effective_uid != 0
            or not isinstance(payload, PoolExecutionCredentialPayload)
            or self.hostname != _CONTROLLER_CREDENTIAL_HOSTS.get(payload.pool_id)
        ):
            raise CapacityExecutorInstallError("controller credential authority is invalid")
        self._assert_quiescent()
        uid, gid = self._service_ids(create=False)
        self._private_directory_evidence(_RUNTIME_ROOT, uid=uid, gid=gid)
        return FixedLocalPoolCredentialTransport(
            pool_id=payload.pool_id,
            target_directory=self._path(_RUNTIME_ROOT / payload.pool_id),
            service_uid=uid,
            service_gid=gid,
        )

    def observe_credential(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence | None:
        transport = self._pool_credential_transport(payload)
        try:
            evidence = transport.observe(payload)
        except (OSError, RuntimeError, ValueError) as exc:
            raise CapacityExecutorInstallError(
                "controller credential observation failed safely"
            ) from exc
        self._assert_quiescent()
        return evidence

    def publish_credential(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence:
        transport = self._pool_credential_transport(payload)
        try:
            evidence = transport.publish(payload)
        except (OSError, RuntimeError, ValueError) as exc:
            raise CapacityExecutorInstallError(
                "controller credential publication failed safely"
            ) from exc
        self._assert_quiescent()
        return evidence

    def _prepared_prerequisite(
        self,
        request: PreparedControllerRequest,
    ) -> _PreparedLocalAuthority:
        if (
            self.effective_uid != 0
            or not isinstance(request, PreparedControllerRequest)
            or request.pool_id != request.prerequisite.pool_id
            or request.transport_authority_sha256 != request.prerequisite.transport_authority_sha256
        ):
            raise CapacityExecutorInstallError("prepared controller request is invalid")
        prerequisite = request.prerequisite
        digest = _validate_image_reference(prerequisite.image)
        uid, gid, _executables, _configuration = self._local_authority(prerequisite)
        expected_release = self._release_root(
            source_sha=prerequisite.source_sha,
            architecture=prerequisite.architecture,
            digest=digest,
        )
        if self._assert_current_destination_safe() != str(expected_release):
            raise CapacityExecutorInstallError(
                "controller prerequisite changed before prepared operation"
            )
        self._inspect_image(
            prerequisite.image,
            source_sha=prerequisite.source_sha,
            architecture=prerequisite.architecture,
            pull=False,
        )
        release = self._path(expected_release)
        try:
            verify_release(
                release,
                expected_source_sha=prerequisite.source_sha,
                expected_architecture=prerequisite.architecture,
            )
        except CapacityExecutorReleaseError as exc:
            raise CapacityExecutorInstallError(
                "controller prerequisite changed before prepared operation"
            ) from exc
        self._verify_runtime_files(expected_release)
        self._probe_runtime(expected_release)
        for path in (
            _CONFIG_ROOT,
            _RUNTIME_ROOT,
            _RUNTIME_ROOT / request.pool_id,
            _SERVICE_HOME,
            Path(prerequisite.binding.state_directory),
        ):
            self._private_directory_evidence(path, uid=uid, gid=gid)
        try:
            installed_input = self._read_private_input(
                Path(prerequisite.prerequisite_input_path),
                uid=uid,
                gid=gid,
            )
        except FileNotFoundError as exc:
            raise CapacityExecutorInstallError(
                "controller prerequisite changed before prepared operation"
            ) from exc
        if installed_input != _canonical_json_bytes(prerequisite.prerequisite_input_value()):
            raise CapacityExecutorInstallError(
                "controller prerequisite changed before prepared operation"
            )
        _unit_sha256, active_states, file_states = self._prepared_unit_evidence(expected_release)
        return _PreparedLocalAuthority(
            uid=uid,
            gid=gid,
            release_root=expected_release,
            unit_active_state=active_states,
            unit_file_state=file_states,
        )

    def observe_prepared(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence | None:
        authority = self._prepared_prerequisite(request)
        installed: dict[str, bytes] = {}
        try:
            for absolute in request.files:
                installed[absolute] = self._read_private_input(
                    Path(absolute),
                    uid=authority.uid,
                    gid=authority.gid,
                )
        except FileNotFoundError:
            return None
        if installed != dict(request.files):
            return None
        tick_digest = self._prepared_tick_evidence(
            request,
            uid=authority.uid,
            gid=authority.gid,
        )
        timer = "loom-capacity-pool-executor-prepared.timer"
        timer_active = (
            authority.unit_active_state[timer],
            authority.unit_file_state[timer],
        ) == ("active", "enabled")
        return PreparedControllerEvidence(
            schema_version=1,
            pool_id=request.pool_id,
            transport_authority_sha256=request.transport_authority_sha256,
            request_sha256=request.request_sha256,
            file_sha256={
                path: hashlib.sha256(payload).hexdigest() for path, payload in installed.items()
            },
            unit_active_state=authority.unit_active_state,
            unit_file_state=authority.unit_file_state,
            successful_tick=timer_active and tick_digest is not None,
            tick_evidence_sha256=tick_digest if timer_active else None,
        )

    def converge_prepared_files(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence:
        authority = self._prepared_prerequisite(request)
        timer = "loom-capacity-pool-executor-prepared.timer"
        if (
            authority.unit_active_state[timer],
            authority.unit_file_state[timer],
        ) != ("inactive", "disabled"):
            raise CapacityExecutorInstallError(
                "prepared controller timer must be disabled before file convergence"
            )
        for absolute, payload in request.files.items():
            self._write_private_input(
                Path(absolute),
                payload,
                uid=authority.uid,
                gid=authority.gid,
            )
        evidence = self.observe_prepared(request)
        if evidence is None:
            raise CapacityExecutorInstallError("prepared controller files did not converge")
        return evidence

    def enable_prepared_timer(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence:
        evidence = self.observe_prepared(request)
        if evidence is None:
            raise CapacityExecutorInstallError("prepared controller files are not exact")
        timer = "loom-capacity-pool-executor-prepared.timer"
        state = (
            evidence.unit_active_state[timer],
            evidence.unit_file_state[timer],
        )
        if state == ("inactive", "disabled"):
            self._prepared_prerequisite(request)
            self._run(_SYSTEMCTL, "enable", "--now", timer)
            evidence = self.observe_prepared(request)
        if evidence is None or (
            evidence.unit_active_state[timer],
            evidence.unit_file_state[timer],
        ) != ("active", "enabled"):
            raise CapacityExecutorInstallError("prepared controller timer did not converge")
        return evidence

    def run_prepared_tick(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence:
        evidence = self.observe_prepared(request)
        timer = "loom-capacity-pool-executor-prepared.timer"
        if evidence is None or (
            evidence.unit_active_state[timer],
            evidence.unit_file_state[timer],
        ) != ("active", "enabled"):
            raise CapacityExecutorInstallError("prepared controller timer is not exact")
        self._prepared_prerequisite(request)
        self._run(_SYSTEMCTL, "start", "loom-capacity-pool-executor-prepared.service")
        authority = self._prepared_prerequisite(request)
        receipt = self._prepared_tick_receipt(request)
        path = self._prepared_tick_path(request)
        try:
            current = self._read_private_input(
                path,
                uid=authority.uid,
                gid=authority.gid,
            )
        except FileNotFoundError:
            current = None
        if current is not None and current != receipt:
            raise CapacityExecutorInstallError("prepared controller tick evidence drifted")
        if current is None:
            self._write_private_input(
                path,
                receipt,
                uid=authority.uid,
                gid=authority.gid,
            )
        evidence = self.observe_prepared(request)
        if evidence is None or not evidence.successful_tick:
            raise CapacityExecutorInstallError("prepared controller tick did not converge")
        return evidence

    def disable_prepared_timer(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence | None:
        self._prepared_prerequisite(request)
        timer = "loom-capacity-pool-executor-prepared.timer"
        self._run(_SYSTEMCTL, "disable", "--now", timer)
        self._run(_SYSTEMCTL, "stop", "loom-capacity-pool-executor-prepared.service")
        authority = self._prepared_prerequisite(request)
        if (
            authority.unit_active_state[timer],
            authority.unit_file_state[timer],
        ) != ("inactive", "disabled"):
            raise CapacityExecutorInstallError("prepared controller timer disable did not converge")
        installed: dict[str, bytes] = {}
        missing = 0
        for absolute in request.files:
            try:
                installed[absolute] = self._read_private_input(
                    Path(absolute),
                    uid=authority.uid,
                    gid=authority.gid,
                )
            except FileNotFoundError:
                missing += 1
        if missing:
            if missing != len(request.files):
                raise CapacityExecutorInstallError(
                    "prepared controller timer disable did not converge"
                )
            return None
        if installed != dict(request.files):
            raise CapacityExecutorInstallError("prepared controller timer disable did not converge")
        evidence = self.observe_prepared(request)
        if evidence is None or (
            evidence.unit_active_state[timer],
            evidence.unit_file_state[timer],
        ) != ("inactive", "disabled"):
            raise CapacityExecutorInstallError("prepared controller timer disable did not converge")
        return evidence

    @staticmethod
    def _prepared_tick_receipt(request: PreparedControllerRequest) -> bytes:
        return _canonical_json_bytes(
            {
                "execution": request.execution.model_dump(mode="json", exclude_none=False),
                "file_sha256": {
                    path: hashlib.sha256(payload).hexdigest()
                    for path, payload in request.files.items()
                },
                "pool_id": request.pool_id,
                "profile_sha256": request.profile_sha256,
                "request_sha256": request.request_sha256,
                "schema_version": 1,
            }
        )

    @staticmethod
    def _prepared_tick_path(request: PreparedControllerRequest) -> Path:
        return Path(request.prerequisite.binding.state_directory) / (
            f".prepared-tick-{request.request_sha256}.json"
        )

    def _prepared_tick_evidence(
        self,
        request: PreparedControllerRequest,
        *,
        uid: int,
        gid: int,
    ) -> str | None:
        expected = self._prepared_tick_receipt(request)
        try:
            current = self._read_private_input(
                self._prepared_tick_path(request),
                uid=uid,
                gid=gid,
            )
        except FileNotFoundError:
            return None
        if current != expected:
            raise CapacityExecutorInstallError("prepared controller tick evidence drifted")
        return hashlib.sha256(current).hexdigest()

    def _ensure_private_child_directory(
        self,
        absolute: Path,
        *,
        uid: int,
        gid: int,
    ) -> None:
        parent_absolute = absolute.parent
        self._private_directory_evidence(parent_absolute, uid=uid, gid=gid)
        path = self._path(absolute)
        if path.exists() or path.is_symlink():
            self._private_directory_evidence(absolute, uid=uid, gid=gid)
            return
        parent = path.parent
        try:
            path.mkdir(mode=0o700)
            path.chmod(0o700)
            os.chown(path, uid, gid)
        except FileExistsError:
            pass
        self._private_directory_evidence(absolute, uid=uid, gid=gid)
        _fsync_directory(parent)

    def _read_private_input(self, absolute: Path, *, uid: int, gid: int) -> bytes:
        path = self._path(absolute)
        try:
            descriptor = os.open(
                path,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            if isinstance(exc, FileNotFoundError):
                raise
            raise CapacityExecutorInstallError(
                "controller prerequisite input is unavailable"
            ) from exc
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_uid != uid
                or before.st_gid != gid
                or before.st_nlink != 1
                or not 0 < before.st_size <= _MAX_COMMAND_OUTPUT_BYTES
            ):
                raise CapacityExecutorInstallError(
                    "controller prerequisite input metadata is unsafe"
                )
            chunks: list[bytes] = []
            remaining = before.st_size
            while remaining:
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    raise CapacityExecutorInstallError(
                        "controller prerequisite input changed while reading"
                    )
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
            if _metadata_identity(before) != _metadata_identity(after):
                raise CapacityExecutorInstallError(
                    "controller prerequisite input changed while reading"
                )
            return b"".join(chunks)
        finally:
            os.close(descriptor)

    def _write_private_input(
        self,
        absolute: Path,
        payload: bytes,
        *,
        uid: int,
        gid: int,
    ) -> None:
        self._private_directory_evidence(absolute.parent, uid=uid, gid=gid)
        path = self._path(absolute)
        try:
            current = self._read_private_input(absolute, uid=uid, gid=gid)
        except FileNotFoundError:
            current = None
        if current == payload:
            return
        parent = path.parent
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            os.fchown(descriptor, uid, gid)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            _fsync_directory(parent)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temporary.unlink(missing_ok=True)
            raise
        if self._read_private_input(absolute, uid=uid, gid=gid) != payload:
            raise CapacityExecutorInstallError("controller prerequisite input did not converge")

    def _unit_evidence(
        self,
        release_root: Path,
        *,
        allow_prepared_timer: bool,
    ) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
        active_states: dict[str, str] = {}
        file_states: dict[str, str] = {}
        unit_sha256: dict[str, str] = {}
        release_units = self._path(release_root) / "payload/units"
        for unit in _UNITS:
            active = self._bounded_stdout(
                self._run(_SYSTEMCTL, "is-active", unit, check=False),
                label="systemd active state",
            ).strip()
            enabled = self._bounded_stdout(
                self._run(_SYSTEMCTL, "is-enabled", unit, check=False),
                label="systemd unit-file state",
            ).strip()
            expected_file_state = "disabled" if unit.endswith(".timer") else "static"
            prepared_timer = unit == "loom-capacity-pool-executor-prepared.timer"
            allowed_state = (
                {("inactive", "disabled"), ("active", "enabled")}
                if allow_prepared_timer and prepared_timer
                else {("inactive", expected_file_state)}
            )
            if (active, enabled) not in allowed_state:
                raise CapacityExecutorInstallError(
                    "controller prerequisite units are not exactly inert"
                )
            installed = self._path(_UNIT_ROOT / unit)
            source = release_units / unit
            installed_metadata = os.lstat(installed)
            if (
                not stat.S_ISREG(installed_metadata.st_mode)
                or installed_metadata.st_nlink != 1
                or installed_metadata.st_uid != self.context.authority_uid
                or installed_metadata.st_gid != self.context.authority_gid
                or stat.S_IMODE(installed_metadata.st_mode) != 0o644
                or source.read_bytes() != installed.read_bytes()
            ):
                raise CapacityExecutorInstallError("controller prerequisite unit authority drifted")
            active_states[unit] = active
            file_states[unit] = enabled
            unit_sha256[unit] = self._authority_file_sha256(
                _UNIT_ROOT / unit,
                executable=False,
            )
        return unit_sha256, active_states, file_states

    def _exact_unit_evidence(
        self,
        release_root: Path,
    ) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
        return self._unit_evidence(release_root, allow_prepared_timer=False)

    def _prepared_unit_evidence(
        self,
        release_root: Path,
    ) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
        return self._unit_evidence(release_root, allow_prepared_timer=True)

    def observe_prerequisite(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence | None:
        if self.effective_uid != 0:
            raise CapacityExecutorInstallError("capacity executor observation requires root")
        digest = _validate_image_reference(request.image)
        uid, gid, executable_sha256, configuration_sha256 = self._local_authority(request)
        self._assert_quiescent()
        expected_release = self._release_root(
            source_sha=request.source_sha,
            architecture=request.architecture,
            digest=digest,
        )
        current_target = self._assert_current_destination_safe()
        if current_target is None or current_target != str(expected_release):
            return None
        self._inspect_image(
            request.image,
            source_sha=request.source_sha,
            architecture=request.architecture,
            pull=False,
        )
        release = self._path(expected_release)
        try:
            verify_release(
                release,
                expected_source_sha=request.source_sha,
                expected_architecture=request.architecture,
            )
        except CapacityExecutorReleaseError as exc:
            raise CapacityExecutorInstallError(
                "controller prerequisite release authority drifted"
            ) from exc
        self._verify_runtime_files(expected_release)
        self._probe_runtime(expected_release)
        unit_sha256, active_states, file_states = self._exact_unit_evidence(expected_release)
        directory_paths = (
            _CONFIG_ROOT,
            _RUNTIME_ROOT,
            _RUNTIME_ROOT / request.pool_id,
            _SERVICE_HOME,
            Path(request.binding.state_directory),
        )
        try:
            directories = {
                str(path): self._private_directory_evidence(path, uid=uid, gid=gid)
                for path in directory_paths
            }
            installed_input = self._read_private_input(
                Path(request.prerequisite_input_path),
                uid=uid,
                gid=gid,
            )
        except FileNotFoundError:
            return None
        expected_input = _canonical_json_bytes(request.prerequisite_input_value())
        if installed_input != expected_input:
            return None
        release_manifest_sha256 = self._authority_file_sha256(
            expected_release / "release-manifest.json",
            executable=False,
        )
        return ControllerPrerequisiteEvidence(
            schema_version=1,
            pool_id=request.pool_id,
            controller_hostname=self.hostname,
            transport_authority_sha256=request.transport_authority_sha256,
            image=request.image,
            source_sha=request.source_sha,
            architecture=request.architecture,
            release_root=str(expected_release),
            release_manifest_sha256=release_manifest_sha256,
            service_user=request.service_user,
            service_uid=uid,
            service_gid=gid,
            slurm_cluster=request.binding.slurm_cluster,
            partition=request.binding.partition,
            target_nodes=request.target_nodes,
            executable_sha256=executable_sha256,
            configuration_sha256=configuration_sha256,
            job_visibility_evidence_sha256=(
                request.binding.inventory.job_visibility_evidence_sha256
            ),
            directories=directories,
            unit_sha256=unit_sha256,
            unit_active_state=active_states,
            unit_file_state=file_states,
            prerequisite_input_path=request.prerequisite_input_path,
            prerequisite_input_sha256=request.prerequisite_input_sha256,
            credential_metadata_sha256=request.credential_metadata_sha256,
            controller_authority_sha256=request.binding.controller_authority_sha256,
            local_authority_sha256=request.binding.local_authority_sha256,
        )

    def converge_prerequisite(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence:
        uid, gid, _executables, _configuration = self._local_authority(request)
        result = self.install(image=request.image, source_sha=request.source_sha)
        if result.architecture != request.architecture:
            raise CapacityExecutorInstallError("controller prerequisite release authority drifted")
        self._ensure_private_child_directory(
            _RUNTIME_ROOT / request.pool_id,
            uid=uid,
            gid=gid,
        )
        self._ensure_private_child_directory(
            Path(request.binding.state_directory),
            uid=uid,
            gid=gid,
        )
        self._write_private_input(
            Path(request.prerequisite_input_path),
            _canonical_json_bytes(request.prerequisite_input_value()),
            uid=uid,
            gid=gid,
        )
        evidence = self.observe_prerequisite(request)
        if evidence is None:
            raise CapacityExecutorInstallError("controller prerequisite convergence was incomplete")
        return evidence

    def install(self, *, image: str, source_sha: str) -> InstallResult:
        if self.effective_uid != 0:
            raise CapacityExecutorInstallError("capacity executor installation requires root")
        digest = _validate_image_reference(image)
        if not isinstance(source_sha, str) or _SOURCE_SHA_RE.fullmatch(source_sha) is None:
            raise CapacityExecutorInstallError("expected executor source SHA is invalid")
        architecture = _architecture(self.machine)
        self._assert_quiescent()
        current_target = self._assert_current_destination_safe()
        self._inspect_image(image, source_sha=source_sha, architecture=architecture)
        release_root = self._prepare_release(
            image=image,
            source_sha=source_sha,
            architecture=architecture,
            digest=digest,
            current_target=current_target,
        )
        self._assert_quiescent()
        uid, gid = self._service_ids()
        self._assert_quiescent()
        self._publish(release_root)
        self._install_assets(release_root, uid=uid, gid=gid)
        self._run(_SYSTEMCTL, "daemon-reload")
        self._assert_quiescent()
        return InstallResult(
            image=image,
            source_sha=source_sha,
            architecture=architecture,
            release_root=release_root,
        )


def _validate_host_root(root: Path) -> None:
    if root.is_symlink() or not root.is_dir():
        raise CapacityExecutorInstallError("installer host root is unsafe")
    metadata = os.lstat(root)
    if metadata.st_uid != 0 or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise CapacityExecutorInstallError("installer host root authority is unsafe")
    process_root = root / "proc/1/root"
    try:
        process_metadata = os.stat(process_root)
        root_metadata = os.stat(root)
        if (process_metadata.st_dev, process_metadata.st_ino) != (
            root_metadata.st_dev,
            root_metadata.st_ino,
        ):
            raise CapacityExecutorInstallError("installer is not bound to the host PID namespace")
    except OSError as exc:
        raise CapacityExecutorInstallError("installer host PID namespace is unavailable") from exc


def _controller_prerequisite_operation(
    installer: ControllerInstaller,
    operation: str,
    payload: bytes,
) -> bytes:
    if operation not in {"observe-prerequisite", "converge-prerequisite"}:
        raise CapacityExecutorInstallError("controller prerequisite operation is invalid")
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_PREREQUISITE_REQUEST_BYTES:
        raise CapacityExecutorInstallError("controller prerequisite request bytes are invalid")
    try:
        request = ControllerPrerequisiteRequest.from_bytes(payload)
    except ValueError as exc:
        raise CapacityExecutorInstallError("controller prerequisite request is invalid") from exc
    if operation == "observe-prerequisite":
        evidence = installer.observe_prerequisite(request)
        return b"null\n" if evidence is None else evidence.to_bytes()
    return installer.converge_prerequisite(request).to_bytes()


def _controller_discovery_operation(
    installer: ControllerInstaller,
    payload: bytes,
) -> bytes:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_DISCOVERY_REQUEST_BYTES:
        raise CapacityExecutorInstallError("controller discovery request bytes are invalid")
    try:
        request = ControllerDiscoveryRequest.from_bytes(payload)
    except ValueError as exc:
        raise CapacityExecutorInstallError("controller discovery request is invalid") from exc
    return installer.discover_controller(request).to_bytes()


def _pool_credential_operation(
    installer: ControllerInstaller,
    operation: str,
    payload: bytes,
) -> bytes:
    if operation not in {"observe-credential", "publish-credential"}:
        raise CapacityExecutorInstallError("controller credential operation is invalid")
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_CREDENTIAL_REQUEST_BYTES:
        raise CapacityExecutorInstallError("controller credential request bytes are invalid")
    try:
        request = PoolExecutionCredentialPayload.from_bytes(payload)
    except ValueError as exc:
        raise CapacityExecutorInstallError("controller credential request is invalid") from exc
    if operation == "observe-credential":
        evidence = installer.observe_credential(request)
        return b"null\n" if evidence is None else evidence.to_bytes()
    return installer.publish_credential(request).to_bytes()


def _prepared_controller_operation(
    installer: ControllerInstaller,
    operation: str,
    payload: bytes,
) -> bytes:
    if operation not in _PREPARED_OPERATIONS:
        raise CapacityExecutorInstallError("prepared controller operation is invalid")
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_PREPARED_REQUEST_BYTES:
        raise CapacityExecutorInstallError("prepared controller request bytes are invalid")
    try:
        request = PreparedControllerRequest.from_bytes(payload)
    except ValueError as exc:
        raise CapacityExecutorInstallError("prepared controller request is invalid") from exc
    if operation == "observe-prepared":
        evidence = installer.observe_prepared(request)
        return b"null\n" if evidence is None else evidence.to_bytes()
    handlers = {
        "converge-prepared-files": installer.converge_prepared_files,
        "enable-prepared-timer": installer.enable_prepared_timer,
        "run-prepared-tick": installer.run_prepared_tick,
        "disable-prepared-timer": installer.disable_prepared_timer,
    }
    evidence = handlers[operation](request)
    return b"null\n" if evidence is None else evidence.to_bytes()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operation",
        choices=(
            "install",
            "discover-controller",
            "observe-prerequisite",
            "converge-prerequisite",
            "observe-credential",
            "publish-credential",
            "observe-prepared",
            "converge-prepared-files",
            "enable-prepared-timer",
            "run-prepared-tick",
            "disable-prepared-timer",
        ),
        default="install",
    )
    parser.add_argument("--image")
    parser.add_argument("--source-sha")
    parser.add_argument("--host-root", type=Path, default=Path("/"))
    args = parser.parse_args(argv)
    try:
        _validate_host_root(args.host_root)
        command_prefix = (
            () if args.host_root == Path("/") else ("/usr/sbin/chroot", str(args.host_root))
        )
        installer = ControllerInstaller(
            context=InstallContext(root=args.host_root, command_prefix=command_prefix),
            runner=SubprocessRunner(),
        )
        if args.operation == "install":
            if args.image is None or args.source_sha is None:
                raise CapacityExecutorInstallError(
                    "capacity executor image and source SHA are required"
                )
            result = installer.install(image=args.image, source_sha=args.source_sha)
        else:
            if args.image is not None or args.source_sha is not None:
                raise CapacityExecutorInstallError(
                    "controller prerequisite operation has unexpected arguments"
                )
            if args.operation == "discover-controller":
                payload = sys.stdin.buffer.read(_MAX_DISCOVERY_REQUEST_BYTES + 1)
                response = _controller_discovery_operation(installer, payload)
            elif args.operation in {"observe-prerequisite", "converge-prerequisite"}:
                payload = sys.stdin.buffer.read(_MAX_PREREQUISITE_REQUEST_BYTES + 1)
                response = _controller_prerequisite_operation(installer, args.operation, payload)
            elif args.operation in _PREPARED_OPERATIONS:
                payload = sys.stdin.buffer.read(_MAX_PREPARED_REQUEST_BYTES + 1)
                response = _prepared_controller_operation(installer, args.operation, payload)
            else:
                payload = sys.stdin.buffer.read(_MAX_CREDENTIAL_REQUEST_BYTES + 1)
                response = _pool_credential_operation(installer, args.operation, payload)
    except CapacityExecutorInstallError as exc:
        parser.error(str(exc))
    if args.operation != "install":
        sys.stdout.buffer.write(response)
        return 0
    print(
        json.dumps(
            {
                "architecture": result.architecture,
                "image": result.image,
                "release_root": str(result.release_root),
                "source_sha": result.source_sha,
                "status": "installed-inert",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
