#!/usr/bin/env python3
"""Install one digest-pinned capacity executor release without activating it."""

from __future__ import annotations

import argparse
import io
import json
import os
import platform
import re
import stat
import subprocess
import tarfile
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol

try:
    from scripts.ops.capacity_executor_release import (
        CapacityExecutorReleaseError,
        verify_release,
    )
except ModuleNotFoundError:  # Installed helper is colocated with the verifier.
    from capacity_executor_release import (  # type: ignore[no-redef]
        CapacityExecutorReleaseError,
        verify_release,
    )

_IMAGE_RE = re.compile(r"^ghcr[.]io/qianyi-sun/loom-capacity-executor@sha256:([0-9a-f]{64})$")
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
_GROUPADD = "/usr/sbin/groupadd"
_USERADD = "/usr/sbin/useradd"
_PYTHON = "/usr/bin/python3.12"
_SYSTEMD_ANALYZE = "/usr/bin/systemd-analyze"
_SYSTEMCTL = "/usr/bin/systemctl"
_SYSTEMD_TMPFILES = "/usr/bin/systemd-tmpfiles"


class CapacityExecutorInstallError(RuntimeError):
    """The controller installation could not converge without weakening safety."""


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


Extractor = Callable[[str, Path, Runner, InstallContext], None]


def _validate_image_reference(image: str) -> str:
    if not isinstance(image, str):
        raise CapacityExecutorInstallError("executor image must be an exact digest reference")
    match = _IMAGE_RE.fullmatch(image)
    if match is None:
        raise CapacityExecutorInstallError("executor image must be an exact digest reference")
    return match.group(1)


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


def _extract_release_tar(stream: BinaryIO | io.BytesIO, destination: Path) -> None:
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


class ControllerInstaller:
    def __init__(
        self,
        *,
        context: InstallContext,
        runner: Runner,
        extractor: Extractor = _extract_image_release,
        machine: str | None = None,
        effective_uid: int | None = None,
    ) -> None:
        self.context = context
        self.runner = runner
        self.extractor = extractor
        self.machine = platform.machine() if machine is None else machine
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

    def _inspect_image(self, image: str, *, source_sha: str, architecture: str) -> None:
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

    def _service_ids(self) -> tuple[int, int]:
        group = self._run(_GETENT, "group", _SERVICE_GROUP, check=False)
        passwd = self._run(_GETENT, "passwd", _SERVICE_USER, check=False)
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
        if (
            len(uids) != 1
            or len(gids) != 1
            or 0 in uids
            or 0 in gids
            or {int(value) for value in supplementary_gid_fields} != gids
        ):
            raise CapacityExecutorInstallError("executor service identity is inconsistent")
        return uids.pop(), gids.pop()

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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--host-root", type=Path, default=Path("/"))
    args = parser.parse_args(argv)
    try:
        _validate_host_root(args.host_root)
        command_prefix = (
            () if args.host_root == Path("/") else ("/usr/sbin/chroot", str(args.host_root))
        )
        result = ControllerInstaller(
            context=InstallContext(root=args.host_root, command_prefix=command_prefix),
            runner=SubprocessRunner(),
        ).install(image=args.image, source_sha=args.source_sha)
    except CapacityExecutorInstallError as exc:
        parser.error(str(exc))
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
