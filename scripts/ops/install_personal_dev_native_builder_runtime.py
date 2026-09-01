#!/usr/bin/env python3
"""Install and verify the inert native personal-development builder runtime."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import ipaddress
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, NoReturn, Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from scripts.ops.personal_dev_native_builder_runtime_profile import (
    NativeBuilderRuntimeArchiveMember,
    NativeBuilderRuntimeProfile,
    NativeBuilderRuntimeProfileError,
    load_native_builder_runtime_profile,
)

_MAX_ARCHIVE_BYTES = 1024**3
_MAX_CA_BYTES = 256 * 1024
_MAX_COMMAND_OUTPUT = 64 * 1024
_COMMAND_TIMEOUT_SECONDS = 30
_ROOT_ENV = {
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_IMAGE_REFERENCE = re.compile(
    r"^[a-z0-9][a-z0-9.-]*(?::[1-9][0-9]{0,4})?/"
    r"[a-z0-9]+(?:[._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}$"
)
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_AGENT_STAGE_MANIFEST = Path("/etc/loom/personal-dev-native-builder/agent-stage-v1.json")
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100


class PersonalDevNativeBuilderRuntimeInstallError(RuntimeError):
    """The native runtime could not converge without weakening safety."""

    def __init__(self, code: str) -> None:
        safe_code = (
            code if isinstance(code, str) and _ERROR_CODE.fullmatch(code) else "internal_error"
        )
        self.code = safe_code
        super().__init__(safe_code)


@dataclass(frozen=True, slots=True)
class NativeBuilderCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@runtime_checkable
class NativeBuilderRunner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> NativeBuilderCommandResult: ...


class NativeBuilderSubprocessRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> NativeBuilderCommandResult:
        if not argv or any(not isinstance(value, str) or not value for value in argv):
            raise PersonalDevNativeBuilderRuntimeInstallError("command_invalid")
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
            raise PersonalDevNativeBuilderRuntimeInstallError("command_timeout") from exc
        if (
            len(completed.stdout.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
            or len(completed.stderr.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("command_output_invalid")
        result = NativeBuilderCommandResult(
            completed.returncode,
            completed.stdout,
            completed.stderr,
        )
        if check and result.returncode != 0:
            raise PersonalDevNativeBuilderRuntimeInstallError("command_failed")
        return result


@dataclass(frozen=True, slots=True)
class NativeBuilderInstallContext:
    """Map host-absolute paths into the real host or a controlled test root."""

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
            raise PersonalDevNativeBuilderRuntimeInstallError("context_invalid")

    def path(self, absolute: Path) -> Path:
        if not isinstance(absolute, Path) or not absolute.is_absolute() or ".." in absolute.parts:
            raise PersonalDevNativeBuilderRuntimeInstallError("path_invalid")
        if self.root == Path("/"):
            return absolute
        return self.root.joinpath(*absolute.parts[1:])

    def argv(self, *argv: str) -> tuple[str, ...]:
        if not argv or any(not isinstance(value, str) or not value for value in argv):
            raise PersonalDevNativeBuilderRuntimeInstallError("command_invalid")
        return (*self.command_prefix, *argv)


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


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
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_remove_stage(path: Path) -> None:
    metadata = os.lstat(path)
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise PersonalDevNativeBuilderRuntimeInstallError("staging_cleanup_invalid")
    path.chmod(0o700)
    with os.scandir(path) as entries:
        for entry in entries:
            child = Path(entry.path)
            child_metadata = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(child_metadata.st_mode):
                _safe_remove_stage(child)
            elif stat.S_ISREG(child_metadata.st_mode):
                child.unlink()
            else:
                raise PersonalDevNativeBuilderRuntimeInstallError("staging_cleanup_invalid")
    path.rmdir()


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise PersonalDevNativeBuilderRuntimeInstallError("atomic_publish_unavailable")
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
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise PersonalDevNativeBuilderRuntimeInstallError("destination_changed")
    raise PersonalDevNativeBuilderRuntimeInstallError("atomic_publish_failed")


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
        raise PersonalDevNativeBuilderRuntimeInstallError("archive_member_invalid")
    path = PurePosixPath(name)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or str(path) != name
    ):
        raise PersonalDevNativeBuilderRuntimeInstallError("archive_member_invalid")
    return name


def _archive_directories(
    members: Mapping[str, NativeBuilderRuntimeArchiveMember],
) -> set[str]:
    directories: set[str] = set()
    for name in members:
        parent = PurePosixPath(name).parent
        while parent != PurePosixPath("."):
            directories.add(str(parent))
            parent = parent.parent
    return directories


def _write_member(
    bundle: tarfile.TarFile,
    member: tarfile.TarInfo,
    destination: Path,
    expected: NativeBuilderRuntimeArchiveMember,
    *,
    uid: int,
    gid: int,
) -> None:
    source = bundle.extractfile(member)
    if source is None:
        raise PersonalDevNativeBuilderRuntimeInstallError("archive_member_invalid")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        digest = hashlib.sha256()
        remaining = expected.size
        with os.fdopen(descriptor, "wb") as output:
            descriptor = None
            while remaining:
                chunk = source.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise PersonalDevNativeBuilderRuntimeInstallError("archive_member_invalid")
                output.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            if source.read(1):
                raise PersonalDevNativeBuilderRuntimeInstallError("archive_member_invalid")
            os.fchmod(output.fileno(), expected.install_mode)
            os.fchown(output.fileno(), uid, gid)
            output.flush()
            os.fsync(output.fileno())
        if digest.hexdigest() != expected.sha256:
            raise PersonalDevNativeBuilderRuntimeInstallError("archive_member_invalid")
    except OSError as exc:
        raise PersonalDevNativeBuilderRuntimeInstallError("archive_destination_invalid") from exc
    finally:
        source.close()
        if descriptor is not None:
            os.close(descriptor)


def _extract_verified_archive(
    archive: Path,
    destination: Path,
    members: Mapping[str, NativeBuilderRuntimeArchiveMember],
    *,
    archive_sha512: str,
    uid: int,
    gid: int,
) -> None:
    if not isinstance(archive, Path) or not archive.is_absolute() or ".." in archive.parts:
        raise PersonalDevNativeBuilderRuntimeInstallError("archive_invalid")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            archive,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != uid
            or before.st_gid != gid
            or not 0 < before.st_size <= _MAX_ARCHIVE_BYTES
            or stat.S_IMODE(before.st_mode) != 0o600
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("archive_invalid")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            reader = _HashingReader(source)
            expected_directories = _archive_directories(members)
            seen_files: set[str] = set()
            seen_directories: set[str] = set()
            try:
                with tarfile.open(
                    fileobj=cast(BinaryIO, reader),
                    mode="r|bz2",
                ) as bundle:
                    for member in bundle:
                        name = _archive_name(member.name)
                        if member.isdir():
                            if (
                                name not in expected_directories
                                or name in seen_directories
                                or member.size != 0
                                or stat.S_IMODE(member.mode) != 0o755
                            ):
                                raise PersonalDevNativeBuilderRuntimeInstallError(
                                    "archive_member_invalid"
                                )
                            seen_directories.add(name)
                            continue
                        expected = members.get(name)
                        if (
                            expected is None
                            or name in seen_files
                            or not member.isreg()
                            or member.size != expected.size
                            or stat.S_IMODE(member.mode) != expected.archive_mode
                        ):
                            raise PersonalDevNativeBuilderRuntimeInstallError(
                                "archive_member_invalid"
                            )
                        seen_files.add(name)
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
            except PersonalDevNativeBuilderRuntimeInstallError:
                raise
            except (OSError, tarfile.TarError, EOFError) as exc:
                raise PersonalDevNativeBuilderRuntimeInstallError("archive_invalid") from exc
            if (
                seen_files != set(members)
                or seen_directories != expected_directories
                or reader.digest.hexdigest() != archive_sha512
            ):
                raise PersonalDevNativeBuilderRuntimeInstallError("archive_invalid")
        after = os.fstat(descriptor)
        if _identity(before) != _identity(after):
            raise PersonalDevNativeBuilderRuntimeInstallError("archive_changed")
    except PersonalDevNativeBuilderRuntimeInstallError:
        raise
    except OSError as exc:
        raise PersonalDevNativeBuilderRuntimeInstallError("archive_invalid") from exc
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
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 <= before.st_size <= maximum
            or (uid is not None and before.st_uid != uid)
            or (gid is not None and before.st_gid != gid)
            or (mode is not None and stat.S_IMODE(before.st_mode) != mode)
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("managed_file_invalid")
        payload = bytearray()
        while chunk := os.read(
            descriptor,
            min(64 * 1024, before.st_size + 1 - len(payload)),
        ):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) != before.st_size or _identity(before) != _identity(after):
            raise PersonalDevNativeBuilderRuntimeInstallError("managed_file_invalid")
        return bytes(payload)
    except PersonalDevNativeBuilderRuntimeInstallError:
        raise
    except OSError as exc:
        raise PersonalDevNativeBuilderRuntimeInstallError("managed_file_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _normalized_nftables(payload: str) -> tuple[str, ...]:
    if not isinstance(payload, str) or "\x00" in payload:
        raise PersonalDevNativeBuilderRuntimeInstallError("nftables_state_invalid")
    lines: list[str] = []
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(
            r"\bcounter packets [0-9]+ bytes [0-9]+\b",
            "counter",
            line,
        )
        lines.append(" ".join(line.split()))
    return tuple(lines)


_ED25519_FIELD = 2**255 - 19
_ED25519_D = (-121665 * pow(121666, _ED25519_FIELD - 2, _ED25519_FIELD)) % _ED25519_FIELD
_ED25519_BASE = (
    15112221349535400772501151409588531511454012693041857206046113283949847762202,
    46316835694926478169428394003475163141307993866256225615783033603165251855960,
    1,
    46827403850823179245072216630277197565144205554125654976674165829533817101731,
)


def _ed25519_add(
    left: tuple[int, int, int, int],
    right: tuple[int, int, int, int],
) -> tuple[int, int, int, int]:
    x1, y1, z1, t1 = left
    x2, y2, z2, t2 = right
    field = _ED25519_FIELD
    a = (y1 - x1) * (y2 - x2) % field
    b = (y1 + x1) * (y2 + x2) % field
    c = 2 * _ED25519_D * t1 * t2 % field
    d = 2 * z1 * z2 % field
    e = b - a
    f = d - c
    g = d + c
    h = b + a
    return (e * f % field, g * h % field, f * g % field, e * h % field)


def _derive_ed25519_public_key(private_seed: bytes) -> bytes:
    """Derive the RFC 8032 compressed public key using only sealed stdlib."""
    if not isinstance(private_seed, bytes) or len(private_seed) != 32:
        raise PersonalDevNativeBuilderRuntimeInstallError("public_key_invalid")
    expanded = hashlib.sha512(private_seed).digest()
    scalar_bytes = bytearray(expanded[:32])
    scalar_bytes[0] &= 248
    scalar_bytes[31] &= 63
    scalar_bytes[31] |= 64
    scalar = int.from_bytes(scalar_bytes, "little")
    result = (0, 1, 1, 0)
    addend = _ED25519_BASE
    while scalar:
        if scalar & 1:
            result = _ed25519_add(result, addend)
        addend = _ed25519_add(addend, addend)
        scalar >>= 1
    x, y, z, _ = result
    inverse_z = pow(z, _ED25519_FIELD - 2, _ED25519_FIELD)
    affine_x = x * inverse_z % _ED25519_FIELD
    affine_y = y * inverse_z % _ED25519_FIELD
    encoded = affine_y | ((affine_x & 1) << 255)
    return encoded.to_bytes(32, "little")


def _public_key_sha256(private_seed: bytes) -> str:
    return hashlib.sha256(_derive_ed25519_public_key(private_seed)).hexdigest()


def _is_ca_bundle(payload: bytes) -> bool:
    try:
        certificates = x509.load_pem_x509_certificates(payload)
        if (
            not certificates
            or b"".join(
                certificate.public_bytes(serialization.Encoding.PEM) for certificate in certificates
            )
            != payload
        ):
            return False
        return all(
            certificate.extensions.get_extension_for_class(x509.BasicConstraints).value.ca
            for certificate in certificates
        )
    except (ValueError, x509.ExtensionNotFound):
        return False


class PersonalDevNativeBuilderRuntimeInstaller:
    def __init__(
        self,
        *,
        profile: NativeBuilderRuntimeProfile,
        context: NativeBuilderInstallContext,
        runner: NativeBuilderRunner,
        effective_uid: int | None = None,
    ) -> None:
        if not isinstance(profile, NativeBuilderRuntimeProfile) or not isinstance(
            context,
            NativeBuilderInstallContext,
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("context_invalid")
        self.profile = profile
        self.context = context
        self.runner = runner
        self.effective_uid = os.geteuid() if effective_uid is None else effective_uid

    @property
    def _agent_uid(self) -> int:
        if self.context.root == Path("/"):
            return self.profile.agent_uid
        return self.context.authority_uid

    @property
    def _agent_gid(self) -> int:
        if self.context.root == Path("/"):
            return self.profile.agent_gid
        return self.context.authority_gid

    @property
    def _socket_gid(self) -> int:
        if self.context.root == Path("/"):
            return self.profile.socket_gid
        return self.context.authority_gid

    def _path(self, absolute: Path) -> Path:
        return self.context.path(absolute)

    def _run(
        self,
        *argv: str,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> NativeBuilderCommandResult:
        result = self.runner.run(
            self.context.argv(*argv),
            check=check,
            env=_ROOT_ENV if env is None else env,
        )
        if (
            not isinstance(result, NativeBuilderCommandResult)
            or len(result.stdout.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
            or len(result.stderr.encode("utf-8", errors="replace")) > _MAX_COMMAND_OUTPUT
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("command_output_invalid")
        return result

    def _verify_context(self) -> None:
        try:
            metadata = os.lstat(self.context.root)
        except OSError as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("parent_directory_invalid") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.context.authority_uid
            or metadata.st_gid != self.context.authority_gid
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("parent_directory_invalid")
        if self.context.root == Path("/") and (
            self.effective_uid != 0
            or self.context.authority_uid != 0
            or self.context.authority_gid != 0
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("authority_invalid")

    def _assert_existing_parent_chain(self, absolute: Path) -> None:
        mapped = self._path(absolute)
        current = self.context.root
        for part in mapped.relative_to(self.context.root).parts:
            current /= part
            if not current.exists() and not current.is_symlink():
                return
            metadata = os.lstat(current)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.context.authority_uid
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise PersonalDevNativeBuilderRuntimeInstallError("parent_directory_invalid")

    def _ensure_directory(self, absolute: Path) -> Path:
        mapped = self._path(absolute)
        current = self.context.root
        for part in mapped.relative_to(self.context.root).parts:
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
                    raise PersonalDevNativeBuilderRuntimeInstallError("parent_directory_invalid")
                continue
            current.mkdir(mode=0o755)
            os.chown(
                current,
                self.context.authority_uid,
                self.context.authority_gid,
            )
            _fsync_directory(parent)
        return mapped

    def _service_active(self, unit: str) -> bool:
        result = self._run("/usr/bin/systemctl", "is-active", unit, check=False)
        if result.stderr:
            raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")
        if result.returncode == 0 and result.stdout == "active\n":
            return True
        if result.returncode in {3, 4} and result.stdout in {
            "inactive\n",
            "unknown\n",
        }:
            return False
        raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")

    def _verify_services_inactive(self) -> None:
        if self._service_active(self.profile.dockerd_service_path.name) or self._service_active(
            self.profile.agent_service_path.name
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")

    def _verify_nft_table_absent(self) -> None:
        result = self._run(
            "/usr/sbin/nft",
            "list",
            "tables",
            check=False,
        )
        target = f"table inet {self.profile.nft_table}"
        if (
            result.returncode != 0
            or result.stderr
            or target in (line.strip() for line in result.stdout.splitlines())
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("nftables_state_invalid")

    def _systemctl(self, action: str, unit: str | None = None) -> None:
        arguments = ["/usr/bin/systemctl", action]
        if unit is not None:
            arguments.append(unit)
        result = self._run(*arguments, check=False)
        if result.returncode != 0 or result.stdout or result.stderr:
            raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")

    def _json_result(
        self,
        result: NativeBuilderCommandResult,
        *,
        error: str,
    ) -> object:
        if result.returncode != 0 or result.stderr or not result.stdout.endswith("\n"):
            raise PersonalDevNativeBuilderRuntimeInstallError(error)
        try:
            return json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError(error) from exc

    def _verify_identity_inventory(self, *, require_present: bool = False) -> None:
        lookups = (
            ("passwd", self.profile.agent_name),
            ("passwd", str(self.profile.agent_uid)),
            ("group", self.profile.agent_name),
            ("group", str(self.profile.agent_gid)),
            ("group", self.profile.socket_group),
            ("group", str(self.profile.socket_gid)),
        )
        rows: list[tuple[str, str] | None] = []
        for database, key in lookups:
            result = self._run(
                "/usr/bin/getent",
                database,
                key,
                check=False,
            )
            if result.returncode == 2 and not result.stdout and not result.stderr:
                rows.append(None)
            elif result.returncode == 0 and not result.stderr and result.stdout.count("\n") == 1:
                rows.append((database, result.stdout.rstrip("\n")))
            else:
                raise PersonalDevNativeBuilderRuntimeInstallError("identity_conflict")
        if all(row is None for row in rows) and not require_present:
            return
        if any(row is None for row in rows):
            raise PersonalDevNativeBuilderRuntimeInstallError("identity_conflict")
        passwd = (
            f"{self.profile.agent_name}:x:{self.profile.agent_uid}:"
            f"{self.profile.agent_gid}:Loom native builder agent:"
            "/nonexistent:/usr/sbin/nologin"
        )
        agent_group = f"{self.profile.agent_name}:x:{self.profile.agent_gid}:"
        socket_group = (
            f"{self.profile.socket_group}:x:{self.profile.socket_gid}:{self.profile.agent_name}"
        )
        expected = (
            ("passwd", passwd),
            ("passwd", passwd),
            ("group", agent_group),
            ("group", agent_group),
            ("group", socket_group),
            ("group", socket_group),
        )
        if tuple(cast(tuple[str, str], row) for row in rows) != expected:
            raise PersonalDevNativeBuilderRuntimeInstallError("identity_conflict")

    def _verify_host(self) -> None:
        self._verify_context()
        hostname = self._run("/bin/hostname", "--fqdn", check=False)
        architecture = self._run("/usr/bin/uname", "-m", check=False)
        if hostname != NativeBuilderCommandResult(
            0, self.profile.host_name + "\n"
        ) or architecture != NativeBuilderCommandResult(0, self.profile.architecture + "\n"):
            raise PersonalDevNativeBuilderRuntimeInstallError("host_identity_invalid")
        kvm = self._run(
            "/usr/bin/test",
            "-c",
            str(self._path(self.profile.device_path)),
            check=False,
        )
        if kvm != NativeBuilderCommandResult(0):
            raise PersonalDevNativeBuilderRuntimeInstallError("kvm_device_invalid")
        controllers_path = self._path(Path("/sys/fs/cgroup/cgroup.controllers"))
        cgroup = self._run(
            "/usr/bin/test",
            "-f",
            str(controllers_path),
            check=False,
        )
        if cgroup != NativeBuilderCommandResult(0):
            raise PersonalDevNativeBuilderRuntimeInstallError("cgroup_v2_invalid")
        controllers = _read_regular(controllers_path, maximum=4096)
        try:
            controller_names = set(controllers.decode("ascii").split())
        except UnicodeDecodeError as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("cgroup_v2_invalid") from exc
        if not {"cpu", "memory", "pids"}.issubset(controller_names):
            raise PersonalDevNativeBuilderRuntimeInstallError("cgroup_v2_invalid")

        version = self._json_result(
            self._run(
                "/usr/bin/docker",
                "version",
                "--format",
                (
                    '{"api":"{{.Server.APIVersion}}","arch":"{{.Server.Arch}}",'
                    '"os":"{{.Server.Os}}","version":"{{.Server.Version}}"}'
                ),
                check=False,
            ),
            error="docker_identity_invalid",
        )
        if version != {
            "api": self.profile.docker_api_version,
            "arch": "arm64",
            "os": "linux",
            "version": self.profile.docker_version,
        }:
            raise PersonalDevNativeBuilderRuntimeInstallError("docker_identity_invalid")
        info = self._json_result(
            self._run(
                "/usr/bin/docker",
                "info",
                "--format",
                ('{"cgroup_driver":"{{.CgroupDriver}}","storage_driver":"{{.Driver}}"}'),
                check=False,
            ),
            error="docker_identity_invalid",
        )
        if info != {
            "cgroup_driver": self.profile.docker_cgroup_driver,
            "storage_driver": self.profile.docker_storage_driver,
        }:
            raise PersonalDevNativeBuilderRuntimeInstallError("docker_identity_invalid")

        cpu = self._run("/usr/bin/nproc", "--all", check=False)
        memory = self._run(
            "/usr/bin/awk",
            '/^MemTotal:/ {printf "%.0f\\n", $2 * 1024}',
            "/proc/meminfo",
            check=False,
        )
        disk = self._run(
            "/usr/bin/df",
            "-B1",
            "--output=avail",
            str(self._path(Path("/var/lib"))),
            check=False,
        )
        try:
            cpus = int(cpu.stdout)
            memory_bytes = int(memory.stdout)
            disk_lines = disk.stdout.splitlines()
            disk_bytes = int(disk_lines[1])
        except (IndexError, TypeError, ValueError) as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("host_capacity_invalid") from exc
        if (
            cpu.returncode != 0
            or cpu.stderr
            or cpu.stdout != f"{cpus}\n"
            or memory.returncode != 0
            or memory.stderr
            or memory.stdout != f"{memory_bytes}\n"
            or disk.returncode != 0
            or disk.stderr
            or disk_lines != ["Avail", str(disk_bytes)]
            or cpus < self.profile.minimum_cpus
            or memory_bytes < self.profile.minimum_memory_bytes
            or disk_bytes < self.profile.minimum_disk_free_bytes
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("host_capacity_invalid")

        route_result = self._json_result(
            self._run(
                "/usr/sbin/ip",
                "-j",
                "-4",
                "route",
                "show",
                "table",
                "all",
                "type",
                "unicast",
                check=False,
            ),
            error="address_pool_conflict",
        )
        if not isinstance(route_result, list):
            raise PersonalDevNativeBuilderRuntimeInstallError("address_pool_conflict")
        provider_network = ipaddress.ip_network(self.profile.address_pool)
        for route in route_result:
            if not isinstance(route, dict) or not isinstance(route.get("dst"), str):
                raise PersonalDevNativeBuilderRuntimeInstallError("address_pool_conflict")
            destination = route["dst"]
            if destination == "default":
                continue
            try:
                network = ipaddress.ip_network(destination, strict=False)
            except ValueError as exc:
                raise PersonalDevNativeBuilderRuntimeInstallError("address_pool_conflict") from exc
            if provider_network.overlaps(network):
                raise PersonalDevNativeBuilderRuntimeInstallError("address_pool_conflict")
        self._verify_identity_inventory()
        self._verify_services_inactive()
        self._verify_nft_table_absent()
        self._validate_generated_inputs()

    def _validate_generated_inputs(self) -> None:
        with tempfile.TemporaryDirectory(prefix="loom-native-runtime-validate-") as raw:
            root = Path(raw)
            dockerd = root / "dockerd.json"
            nftables = root / "provider-network.nft"
            daemon = root / self.profile.dockerd_service_path.name
            agent = root / self.profile.agent_service_path.name
            slice_unit = root / self.profile.slice_unit_path.name
            sysusers = root / self.profile.sysusers_path.name
            dockerd.write_bytes(self.profile.dockerd_json)
            nftables.write_bytes(self.profile.nftables)
            daemon.write_bytes(self.profile.dockerd_service)
            agent.write_bytes(
                self._render_agent_unit(
                    agent_image="example.invalid/loom-agent@sha256:" + "a" * 64,
                    builder_image="example.invalid/loom-builder@sha256:" + "b" * 64,
                    service_url="https://loom.example.invalid",
                    agent_instance_id="00000000-0000-0000-0000-000000000001",
                    key_id="validation-v1",
                )
            )
            slice_unit.write_bytes(self.profile.slice_unit)
            sysusers.write_bytes(self.profile.sysusers)
            for target in (
                "network-online.target",
                "sysinit.target",
                "basic.target",
                "shutdown.target",
                "slices.target",
            ):
                (root / target).write_bytes(
                    b"[Unit]\nDescription=Native runtime validation target\n"
                )
            commands = (
                ("/usr/bin/dockerd", "--validate", f"--config-file={dockerd}"),
                ("/usr/sbin/nft", "--check", "--file", str(nftables)),
                (
                    "/usr/bin/systemd-analyze",
                    "verify",
                    str(daemon),
                    str(agent),
                    str(slice_unit),
                ),
                ("/usr/bin/systemd-sysusers", "--dry-run", str(sysusers)),
            )
            for command in commands:
                command_env = _ROOT_ENV
                if Path(command[0]).name == "systemd-analyze":
                    command_env = {
                        **_ROOT_ENV,
                        "SYSTEMD_UNIT_PATH": str(root),
                    }
                result = self._run(*command, check=False, env=command_env)
                permits_stdout = Path(command[0]).name == "systemd-sysusers"
                if (
                    result.returncode != 0
                    or (result.stdout and not permits_stdout)
                    or result.stderr
                ):
                    raise PersonalDevNativeBuilderRuntimeInstallError("generated_input_invalid")

    def _static_files(self) -> Mapping[Path, bytes]:
        return {
            self.profile.profile_path: self.profile.payload,
            self.profile.runsc_config_path: self.profile.runsc_toml,
            self.profile.dockerd_config_path: self.profile.dockerd_json,
            self.profile.nftables_path: self.profile.nftables,
            self.profile.dockerd_service_path: self.profile.dockerd_service,
            self.profile.slice_unit_path: self.profile.slice_unit,
            self.profile.sysusers_path: self.profile.sysusers,
            self.profile.agent_service_template_path: (self.profile.agent_service_template),
        }

    def _verify_file(
        self,
        absolute: Path,
        payload: bytes,
        *,
        mode: int,
        uid: int | None = None,
        gid: int | None = None,
    ) -> None:
        actual = _read_regular(
            self._path(absolute),
            maximum=max(len(payload), 1),
            uid=self.context.authority_uid if uid is None else uid,
            gid=self.context.authority_gid if gid is None else gid,
            mode=mode,
        )
        if actual != payload:
            raise PersonalDevNativeBuilderRuntimeInstallError("staged_state_invalid")

    def _verify_directory(
        self,
        path: Path,
        *,
        mode: int,
        uid: int | None = None,
        gid: int | None = None,
    ) -> None:
        try:
            metadata = os.lstat(path)
        except OSError as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("staged_state_invalid") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != (self.context.authority_uid if uid is None else uid)
            or metadata.st_gid != (self.context.authority_gid if gid is None else gid)
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("staged_state_invalid")

    def _ensure_managed_directory(
        self,
        absolute: Path,
        *,
        mode: int,
        uid: int,
        gid: int,
    ) -> None:
        parent = self._ensure_directory(absolute.parent)
        path = self._path(absolute)
        if path.exists() or path.is_symlink():
            self._verify_directory(path, mode=mode, uid=uid, gid=gid)
            return
        try:
            path.mkdir(mode=0o700)
            os.chown(path, uid, gid)
            path.chmod(mode)
            _fsync_directory(path)
            _fsync_directory(parent)
        except OSError as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("runtime_directory_invalid") from exc

    def _verify_runtime_directories(self) -> None:
        self._verify_directory(
            self._path(self.profile.data_root),
            mode=0o750,
        )
        self._verify_directory(
            self._path(self.profile.exec_root),
            mode=0o750,
            gid=self._socket_gid,
        )
        self._verify_directory(
            self._path(self.profile.agent_state_path),
            mode=0o700,
            uid=self._agent_uid,
            gid=self._agent_gid,
        )

    def _verify_release(self) -> None:
        release = self._path(self.profile.release_root)
        self._verify_directory(release, mode=0o555)
        expected_files = {Path(name) for name in self.profile.members}
        expected_directories = {path.parent for path in expected_files if path.parent != Path(".")}
        actual_files: set[Path] = set()
        actual_directories: set[Path] = set()
        for root, directories, files in os.walk(release, followlinks=False):
            root_path = Path(root)
            for name in directories:
                child = root_path / name
                metadata = os.lstat(child)
                if stat.S_ISLNK(metadata.st_mode):
                    raise PersonalDevNativeBuilderRuntimeInstallError("staged_state_invalid")
                actual_directories.add(child.relative_to(release))
            for name in files:
                actual_files.add((root_path / name).relative_to(release))
        if actual_files != expected_files or actual_directories != expected_directories:
            raise PersonalDevNativeBuilderRuntimeInstallError("staged_state_invalid")
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
                raise PersonalDevNativeBuilderRuntimeInstallError("staged_state_invalid")

    def _agent_paths(self) -> tuple[Path, ...]:
        return (
            self.profile.agent_service_path,
            self.profile.private_key_path,
            self.profile.ca_file_path,
            _AGENT_STAGE_MANIFEST,
        )

    def _read_agent_manifest(self) -> dict[str, str]:
        payload = _read_regular(
            self._path(_AGENT_STAGE_MANIFEST),
            maximum=16 * 1024,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
            mode=0o444,
        )
        try:
            value = json.loads(payload.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_stage_invalid") from exc
        keys = {
            "agent_image",
            "agent_instance_id",
            "builder_image",
            "key_id",
            "profile_sha256",
            "public_key_sha256",
            "schema",
            "service_url",
            "unit_sha256",
        }
        if (
            not isinstance(value, dict)
            or set(value) != keys
            or any(not isinstance(item, str) for item in value.values())
            or payload != _canonical_json(value)
            or value["schema"] != "loom.personal-dev-native-builder-agent-stage.v1"
            or value["profile_sha256"] != self.profile.sha256
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_stage_invalid")
        return cast(dict[str, str], value)

    def _verify_agent_staged(self) -> None:
        manifest = self._read_agent_manifest()
        self._validate_agent_bindings(
            agent_image=manifest["agent_image"],
            builder_image=manifest["builder_image"],
            service_url=manifest["service_url"],
            agent_instance_id=manifest["agent_instance_id"],
            key_id=manifest["key_id"],
        )
        key = _read_regular(
            self._path(self.profile.private_key_path),
            maximum=32,
            uid=self._agent_uid,
            gid=self._agent_gid,
            mode=self.profile.private_key_mode,
        )
        ca = _read_regular(
            self._path(self.profile.ca_file_path),
            maximum=_MAX_CA_BYTES,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
            mode=0o444,
        )
        unit = self._render_agent_unit(
            agent_image=manifest["agent_image"],
            builder_image=manifest["builder_image"],
            service_url=manifest["service_url"],
            agent_instance_id=manifest["agent_instance_id"],
            key_id=manifest["key_id"],
        )
        self._verify_file(self.profile.agent_service_path, unit, mode=0o444)
        if (
            len(key) != 32
            or _public_key_sha256(key) != manifest["public_key_sha256"]
            or not _is_ca_bundle(ca)
            or hashlib.sha256(unit).hexdigest() != manifest["unit_sha256"]
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_stage_invalid")

    def _verify_installed(self) -> None:
        self._verify_context()
        self._verify_release()
        for absolute, payload in self._static_files().items():
            self._verify_file(absolute, payload, mode=0o444)
        self._verify_runtime_directories()
        agent_states = [
            self._path(path).exists() or self._path(path).is_symlink()
            for path in self._agent_paths()
        ]
        if any(agent_states):
            if not all(agent_states):
                raise PersonalDevNativeBuilderRuntimeInstallError("agent_stage_invalid")
            self._verify_agent_staged()

    def _verify_existing_destinations(self) -> None:
        destinations = (
            self.profile.release_root,
            *self._static_files().keys(),
            *self._agent_paths(),
        )
        for destination in destinations:
            self._assert_existing_parent_chain(destination.parent)
        release = self._path(self.profile.release_root)
        if release.exists() or release.is_symlink():
            self._verify_release()
        for absolute, payload in self._static_files().items():
            path = self._path(absolute)
            if path.exists() or path.is_symlink():
                self._verify_file(absolute, payload, mode=0o444)
        agent_states = [
            self._path(path).exists() or self._path(path).is_symlink()
            for path in self._agent_paths()
        ]
        if any(agent_states):
            if not all(agent_states):
                raise PersonalDevNativeBuilderRuntimeInstallError("agent_stage_invalid")
            self._verify_agent_staged()

    def _archive_stage(self, archive: Path, parent: Path | None = None) -> Path:
        if parent is None:
            temporary = Path(tempfile.mkdtemp(prefix="loom-native-gvisor-verify-"))
        else:
            temporary = Path(tempfile.mkdtemp(prefix=".gvisor-stage-", dir=parent))
        os.chown(
            temporary,
            self.context.authority_uid,
            self.context.authority_gid,
        )
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

    def _freeze_stage(self, stage: Path) -> None:
        directories = [Path(root) for root, _, _ in os.walk(stage)]
        for directory in sorted(
            directories,
            key=lambda item: len(item.parts),
            reverse=True,
        ):
            os.chown(
                directory,
                self.context.authority_uid,
                self.context.authority_gid,
            )
            directory.chmod(0o555)
            _fsync_directory(directory)

    def _publish_file(
        self,
        absolute: Path,
        payload: bytes,
        *,
        mode: int,
        uid: int | None = None,
        gid: int | None = None,
    ) -> None:
        owner_uid = self.context.authority_uid if uid is None else uid
        owner_gid = self.context.authority_gid if gid is None else gid
        destination = self._path(absolute)
        if destination.exists() or destination.is_symlink():
            self._verify_file(
                absolute,
                payload,
                mode=mode,
                uid=owner_uid,
                gid=owner_gid,
            )
            return
        parent = self._ensure_directory(absolute.parent)
        descriptor, raw_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            dir=parent,
        )
        temporary = Path(raw_name)
        descriptor_open = True
        try:
            os.fchmod(descriptor, mode)
            os.fchown(descriptor, owner_uid, owner_gid)
            with os.fdopen(descriptor, "wb") as output:
                descriptor_open = False
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            try:
                os.link(temporary, destination, follow_symlinks=False)
            except FileExistsError:
                self._verify_file(
                    absolute,
                    payload,
                    mode=mode,
                    uid=owner_uid,
                    gid=owner_gid,
                )
            temporary.unlink()
            _fsync_directory(parent)
        except BaseException:
            if descriptor_open:
                os.close(descriptor)
            if temporary.exists() and not temporary.is_symlink():
                temporary.unlink()
            raise

    def _receipt(
        self,
        operation: str,
        *,
        state: str | None = None,
    ) -> dict[str, str]:
        result = {
            "operation": operation,
            "profile_sha256": self.profile.sha256,
            "release": self.profile.version,
        }
        if state is not None:
            result["state"] = state
        return result

    def preflight(self, archive: Path) -> dict[str, str]:
        self._verify_host()
        self._verify_existing_destinations()
        stage = self._archive_stage(archive)
        _safe_remove_stage(stage)
        return {
            "archive_sha512": self.profile.archive_sha512,
            **self._receipt("preflight"),
        }

    def install(self, archive: Path) -> dict[str, str]:
        self.preflight(archive)
        release = self._path(self.profile.release_root)
        if not release.exists() and not release.is_symlink():
            release_parent = self._ensure_directory(self.profile.release_root.parent)
            stage = self._archive_stage(archive, release_parent)
            published = False
            try:
                self._freeze_stage(stage)
                _rename_noreplace(stage, release)
                published = True
                _fsync_directory(release_parent)
            finally:
                if not published and stage.exists() and not stage.is_symlink():
                    _safe_remove_stage(stage)
        for absolute, payload in self._static_files().items():
            self._publish_file(absolute, payload, mode=0o444)
        self._systemctl("daemon-reload")
        sysusers = self._run(
            "/usr/bin/systemd-sysusers",
            str(self._path(self.profile.sysusers_path)),
            check=False,
        )
        if sysusers.returncode != 0 or sysusers.stderr:
            raise PersonalDevNativeBuilderRuntimeInstallError("identity_install_invalid")
        if self.context.root == Path("/"):
            self._verify_identity_inventory(require_present=True)
        self._ensure_managed_directory(
            self.profile.data_root,
            mode=0o750,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
        )
        self._ensure_managed_directory(
            self.profile.exec_root,
            mode=0o750,
            uid=self.context.authority_uid,
            gid=self._socket_gid,
        )
        self._ensure_managed_directory(
            self.profile.agent_state_path,
            mode=0o700,
            uid=self._agent_uid,
            gid=self._agent_gid,
        )
        self._verify_installed()
        return self._receipt("install", state="staged")

    def _validate_agent_bindings(
        self,
        *,
        agent_image: str,
        builder_image: str,
        service_url: str,
        agent_instance_id: str,
        key_id: str,
    ) -> None:
        if (
            _IMAGE_REFERENCE.fullmatch(agent_image) is None
            or _IMAGE_REFERENCE.fullmatch(builder_image) is None
            or _KEY_ID.fullmatch(key_id) is None
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_binding_invalid")
        try:
            instance = uuid.UUID(agent_instance_id)
        except (AttributeError, TypeError, ValueError) as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_binding_invalid") from exc
        parsed = urlsplit(service_url)
        if (
            str(instance) != agent_instance_id
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or parsed.geturl() != service_url
            or any(character.isspace() for character in service_url)
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_binding_invalid")
        try:
            port = parsed.port
            parsed.hostname.encode("ascii")
        except (UnicodeEncodeError, ValueError) as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_binding_invalid") from exc
        if port is not None and not 1 <= port <= 65535:
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_binding_invalid")

    def _render_agent_unit(
        self,
        *,
        agent_image: str,
        builder_image: str,
        service_url: str,
        agent_instance_id: str,
        key_id: str,
    ) -> bytes:
        replacements = {
            b"@@AGENT_IMAGE@@": agent_image.encode("ascii"),
            b"@@BUILDER_IMAGE@@": builder_image.encode("ascii"),
            b"@@SERVICE_URL@@": service_url.encode("ascii"),
            b"@@AGENT_INSTANCE_ID@@": agent_instance_id.encode("ascii"),
            b"@@KEY_ID@@": key_id.encode("ascii"),
            b"@@RUNTIME_PROFILE_SHA256@@": self.profile.sha256.encode("ascii"),
        }
        payload = self.profile.agent_service_template
        for marker, value in replacements.items():
            if marker not in payload:
                raise PersonalDevNativeBuilderRuntimeInstallError("agent_template_invalid")
            payload = payload.replace(marker, value)
        if b"@@" in payload or not payload.endswith(b"\n"):
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_template_invalid")
        return payload

    def _stage_agent(
        self,
        *,
        agent_image: str,
        builder_image: str,
        service_url: str,
        agent_instance_id: str,
        key_id: str,
        private_key: Path,
        ca_file: Path,
        expected_public_key_sha256: str | None,
    ) -> dict[str, str]:
        self._verify_installed()
        self._verify_services_inactive()
        self._validate_agent_bindings(
            agent_image=agent_image,
            builder_image=builder_image,
            service_url=service_url,
            agent_instance_id=agent_instance_id,
            key_id=key_id,
        )
        key = _read_regular(
            private_key,
            maximum=32,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
            mode=self.profile.private_key_mode,
        )
        ca = _read_regular(
            ca_file,
            maximum=_MAX_CA_BYTES,
            uid=self.context.authority_uid,
            gid=self.context.authority_gid,
            mode=0o444,
        )
        if len(key) != 32 or not _is_ca_bundle(ca):
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_material_invalid")
        public_key_sha256 = _public_key_sha256(key)
        if expected_public_key_sha256 is not None and (
            _SHA256.fullmatch(expected_public_key_sha256) is None
            or public_key_sha256 != expected_public_key_sha256
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("public_key_invalid")
        unit = self._render_agent_unit(
            agent_image=agent_image,
            builder_image=builder_image,
            service_url=service_url,
            agent_instance_id=agent_instance_id,
            key_id=key_id,
        )
        manifest = _canonical_json(
            {
                "agent_image": agent_image,
                "agent_instance_id": agent_instance_id,
                "builder_image": builder_image,
                "key_id": key_id,
                "profile_sha256": self.profile.sha256,
                "public_key_sha256": public_key_sha256,
                "schema": "loom.personal-dev-native-builder-agent-stage.v1",
                "service_url": service_url,
                "unit_sha256": hashlib.sha256(unit).hexdigest(),
            }
        )
        self._publish_file(
            self.profile.private_key_path,
            key,
            mode=self.profile.private_key_mode,
            uid=self._agent_uid,
            gid=self._agent_gid,
        )
        self._publish_file(self.profile.ca_file_path, ca, mode=0o444)
        self._publish_file(self.profile.agent_service_path, unit, mode=0o444)
        self._publish_file(_AGENT_STAGE_MANIFEST, manifest, mode=0o444)
        self._systemctl("daemon-reload")
        self._verify_agent_staged()
        return self._receipt("stage-agent", state="staged")

    def stage_agent(
        self,
        *,
        agent_image: str,
        builder_image: str,
        service_url: str,
        agent_instance_id: str,
        key_id: str,
        private_key: Path,
        ca_file: Path,
    ) -> dict[str, str]:
        return self._stage_agent(
            agent_image=agent_image,
            builder_image=builder_image,
            service_url=service_url,
            agent_instance_id=agent_instance_id,
            key_id=key_id,
            private_key=private_key,
            ca_file=ca_file,
            expected_public_key_sha256=None,
        )

    def stage_agent_authorized(
        self,
        *,
        agent_image: str,
        builder_image: str,
        service_url: str,
        agent_instance_id: str,
        key_id: str,
        private_key: Path,
        ca_file: Path,
        expected_public_key_sha256: str,
    ) -> dict[str, str]:
        """Stage material only when its raw Ed25519 public identity is approved."""
        return self._stage_agent(
            agent_image=agent_image,
            builder_image=builder_image,
            service_url=service_url,
            agent_instance_id=agent_instance_id,
            key_id=key_id,
            private_key=private_key,
            ca_file=ca_file,
            expected_public_key_sha256=expected_public_key_sha256,
        )

    def discard_agent_stage(self) -> None:
        """Remove only exact inactive agent-stage files after broker rollback."""
        self._verify_services_inactive()
        specifications = (
            (
                _AGENT_STAGE_MANIFEST,
                16 * 1024,
                self.context.authority_uid,
                self.context.authority_gid,
                0o444,
            ),
            (
                self.profile.agent_service_path,
                64 * 1024,
                self.context.authority_uid,
                self.context.authority_gid,
                0o444,
            ),
            (
                self.profile.ca_file_path,
                _MAX_CA_BYTES,
                self.context.authority_uid,
                self.context.authority_gid,
                0o444,
            ),
            (
                self.profile.private_key_path,
                32,
                self._agent_uid,
                self._agent_gid,
                self.profile.private_key_mode,
            ),
        )
        first_failure: BaseException | None = None
        removed = False
        for absolute, maximum, uid, gid, mode in specifications:
            path = self._path(absolute)
            try:
                os.lstat(path)
            except FileNotFoundError:
                continue
            except BaseException as exc:
                if first_failure is None:
                    first_failure = exc
                continue
            try:
                _read_regular(
                    path,
                    maximum=maximum,
                    uid=uid,
                    gid=gid,
                    mode=mode,
                )
            except BaseException as exc:
                if first_failure is None:
                    first_failure = exc
                continue
            try:
                removed = True
                self._unlink(absolute)
            except BaseException as exc:
                if first_failure is None:
                    first_failure = exc
        if removed:
            try:
                self._systemctl("daemon-reload")
            except BaseException as exc:
                if first_failure is None:
                    first_failure = exc
        if first_failure is not None:
            if isinstance(
                first_failure,
                PersonalDevNativeBuilderRuntimeInstallError,
            ):
                raise first_failure
            raise PersonalDevNativeBuilderRuntimeInstallError(
                "agent_stage_invalid"
            ) from first_failure

    def verify_staged(self) -> dict[str, str]:
        self._verify_installed()
        self._verify_services_inactive()
        self._verify_nft_table_absent()
        return self._receipt("verify-staged", state="staged")

    def _dedicated_inventory(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        endpoint = f"unix://{self.profile.docker_socket}"
        containers = self._run(
            "/usr/bin/docker",
            "-H",
            endpoint,
            "ps",
            "--all",
            "--quiet",
            check=False,
        )
        networks = self._run(
            "/usr/bin/docker",
            "-H",
            endpoint,
            "network",
            "ls",
            "--quiet",
            "--filter",
            "type=custom",
            check=False,
        )
        if (
            containers.returncode != 0
            or containers.stderr
            or networks.returncode != 0
            or networks.stderr
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("dedicated_daemon_invalid")
        return (
            tuple(value for value in containers.stdout.splitlines() if value),
            tuple(value for value in networks.stdout.splitlines() if value),
        )

    def verify_active(self) -> dict[str, str]:
        self._verify_installed()
        if not all(
            self._path(path).exists() and not self._path(path).is_symlink()
            for path in self._agent_paths()
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("agent_stage_invalid")
        if not all(
            (
                self._service_active(self.profile.dockerd_service_path.name),
                self._service_active(self.profile.agent_service_path.name),
            )
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")
        socket_path = self._path(self.profile.docker_socket)
        try:
            socket_metadata = os.lstat(socket_path)
        except OSError as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("dedicated_socket_invalid") from exc
        socket_kind_valid = stat.S_ISSOCK(socket_metadata.st_mode)
        if self.context.root != Path("/"):
            socket_kind_valid = socket_kind_valid or stat.S_ISREG(socket_metadata.st_mode)
        if (
            not socket_kind_valid
            or stat.S_ISLNK(socket_metadata.st_mode)
            or socket_metadata.st_uid != self.context.authority_uid
            or socket_metadata.st_gid != self._socket_gid
            or stat.S_IMODE(socket_metadata.st_mode) != self.profile.socket_mode
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("dedicated_socket_invalid")
        endpoint = f"unix://{self.profile.docker_socket}"
        daemon_info = self._json_result(
            self._run(
                "/usr/bin/docker",
                "-H",
                endpoint,
                "info",
                "--format",
                (
                    '{"cgroup_driver":"{{.CgroupDriver}}",'
                    '"default_runtime":"{{.DefaultRuntime}}",'
                    '"driver":"{{.Driver}}",'
                    '"server_version":"{{.ServerVersion}}"}'
                ),
                check=False,
            ),
            error="dedicated_daemon_invalid",
        )
        if daemon_info != {
            "cgroup_driver": self.profile.docker_cgroup_driver,
            "default_runtime": self.profile.handler,
            "driver": self.profile.docker_storage_driver,
            "server_version": self.profile.docker_version,
        }:
            raise PersonalDevNativeBuilderRuntimeInstallError("dedicated_daemon_invalid")
        version = self._run(str(self.profile.runsc_path), "--version", check=False)
        if version != NativeBuilderCommandResult(
            0,
            f"runsc version {self.profile.version}\nspec: {self.profile.runsc_spec_version}\n",
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("runsc_version_invalid")
        nft_check = self._run(
            "/usr/sbin/nft",
            "--check",
            "--file",
            str(self._path(self.profile.nftables_path)),
            check=False,
        )
        nft_live = self._run(
            "/usr/sbin/nft",
            "list",
            "table",
            "inet",
            self.profile.nft_table,
            check=False,
        )
        if (
            nft_check.returncode != 0
            or nft_check.stdout
            or nft_check.stderr
            or nft_live.returncode != 0
            or nft_live.stderr
            or _normalized_nftables(nft_live.stdout)
            != _normalized_nftables(self.profile.nftables.decode("ascii"))
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("nftables_state_invalid")
        containers, networks = self._dedicated_inventory()
        if containers or networks:
            raise PersonalDevNativeBuilderRuntimeInstallError("dedicated_daemon_busy")
        return self._receipt("verify-active", state="active")

    def _unlink(self, absolute: Path) -> None:
        path = self._path(absolute)
        path.unlink()
        _fsync_directory(path.parent)

    def _remove_release(self) -> None:
        release = self._path(self.profile.release_root)
        release.chmod(0o700)
        directories = {
            (release / name).parent
            for name in self.profile.members
            if (release / name).parent != release
        }
        for directory in directories:
            directory.chmod(0o700)
        for name in sorted(
            self.profile.members,
            key=lambda item: len(Path(item).parts),
            reverse=True,
        ):
            path = release / name
            path.unlink()
            _fsync_directory(path.parent)
        for directory in sorted(
            directories,
            key=lambda item: len(item.parts),
            reverse=True,
        ):
            directory.rmdir()
            _fsync_directory(directory.parent)
        release.rmdir()
        _fsync_directory(release.parent)

    def _remove_empty_directory(self, absolute: Path) -> None:
        directory = self._path(absolute)
        try:
            directory.rmdir()
        except OSError as exc:
            if exc.errno not in {errno.ENOENT, errno.ENOTEMPTY}:
                raise PersonalDevNativeBuilderRuntimeInstallError("remove_failed") from exc
        else:
            _fsync_directory(directory.parent)

    def _removed_installation_is_exact(self) -> bool:
        managed_artifacts = (
            self.profile.release_root,
            *self._static_files().keys(),
            *self._agent_paths(),
        )
        if any(
            self._path(absolute).exists() or self._path(absolute).is_symlink()
            for absolute in managed_artifacts
        ):
            return False
        self._verify_context()
        if self._service_active(self.profile.agent_service_path.name) or self._service_active(
            self.profile.dockerd_service_path.name
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")
        self._verify_nft_table_absent()
        if self.context.root == Path("/"):
            self._verify_identity_inventory(require_present=True)
        for absolute in (
            self.profile.config_root,
            self.profile.agent_state_path,
            self.profile.exec_root,
        ):
            path = self._path(absolute)
            if path.exists() or path.is_symlink():
                raise PersonalDevNativeBuilderRuntimeInstallError("removed_state_invalid")
        data_root = self._path(self.profile.data_root)
        if not data_root.exists() and not data_root.is_symlink():
            return True
        self._verify_directory(data_root, mode=0o750)
        try:
            children = tuple(data_root.iterdir())
        except OSError as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("removed_state_invalid") from exc
        docker_root = data_root / "docker"
        if children != (docker_root,):
            raise PersonalDevNativeBuilderRuntimeInstallError("removed_state_invalid")
        try:
            metadata = os.lstat(docker_root)
        except OSError as exc:
            raise PersonalDevNativeBuilderRuntimeInstallError("removed_state_invalid") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.context.authority_uid
            or metadata.st_gid != self.context.authority_gid
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("removed_state_invalid")
        return True

    def _removed_receipt(self) -> dict[str, str]:
        receipt = self._receipt("remove", state="managed-files-absent")
        receipt["retained"] = "dedicated-image-cache-and-system-identities"
        return receipt

    def remove(self) -> dict[str, str]:
        if self._removed_installation_is_exact():
            return self._removed_receipt()
        self._verify_installed()
        agent_active = self._service_active(self.profile.agent_service_path.name)
        dockerd_active = self._service_active(self.profile.dockerd_service_path.name)
        if agent_active:
            raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")
        started_for_inventory = False
        if not dockerd_active:
            self._systemctl("daemon-reload")
            self._systemctl("start", self.profile.dockerd_service_path.name)
            if not self._service_active(self.profile.dockerd_service_path.name):
                raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")
            started_for_inventory = True
        try:
            containers, networks = self._dedicated_inventory()
        except BaseException:
            if started_for_inventory:
                self._systemctl("stop", self.profile.dockerd_service_path.name)
            raise
        if containers or networks:
            if started_for_inventory:
                self._systemctl("stop", self.profile.dockerd_service_path.name)
            raise PersonalDevNativeBuilderRuntimeInstallError("dedicated_daemon_busy")
        self._systemctl("stop", self.profile.dockerd_service_path.name)
        if self._service_active(self.profile.dockerd_service_path.name):
            raise PersonalDevNativeBuilderRuntimeInstallError("service_state_invalid")
        for absolute in self._agent_paths():
            path = self._path(absolute)
            if path.exists() or path.is_symlink():
                self._unlink(absolute)
        for absolute in reversed(tuple(self._static_files())):
            self._unlink(absolute)
        self._remove_release()
        for absolute in (
            self.profile.config_root,
            self.profile.agent_state_path,
            self.profile.exec_root,
            self.profile.data_root,
            self.profile.release_root.parent,
            self.profile.release_root.parent.parent,
        ):
            self._remove_empty_directory(absolute)
        self._systemctl("daemon-reload")
        return self._removed_receipt()


class _InstallerOperations(Protocol):
    def preflight(self, archive: Path) -> Mapping[str, object]: ...
    def install(self, archive: Path) -> Mapping[str, object]: ...
    def stage_agent(
        self,
        *,
        agent_image: str,
        builder_image: str,
        service_url: str,
        agent_instance_id: str,
        key_id: str,
        private_key: Path,
        ca_file: Path,
    ) -> Mapping[str, object]: ...
    def verify_staged(self) -> Mapping[str, object]: ...
    def verify_active(self) -> Mapping[str, object]: ...
    def remove(self) -> Mapping[str, object]: ...


InstallerFactory = Callable[[NativeBuilderRuntimeProfile], _InstallerOperations]


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise PersonalDevNativeBuilderRuntimeInstallError("arguments_invalid")


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(add_help=True)
    parser.add_argument(
        "operation",
        choices=(
            "preflight",
            "install",
            "stage-agent",
            "verify-staged",
            "verify-active",
            "remove",
        ),
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--agent-image")
    parser.add_argument("--builder-image")
    parser.add_argument("--service-url")
    parser.add_argument("--agent-instance-id")
    parser.add_argument("--key-id")
    parser.add_argument("--private-key", type=Path)
    parser.add_argument("--ca-file", type=Path)
    return parser


def _default_installer(
    profile: NativeBuilderRuntimeProfile,
) -> _InstallerOperations:
    return PersonalDevNativeBuilderRuntimeInstaller(
        profile=profile,
        context=NativeBuilderInstallContext(),
        runner=NativeBuilderSubprocessRunner(),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    installer_factory: InstallerFactory = _default_installer,
) -> int:
    try:
        arguments = _parser().parse_args(argv)
        needs_archive = arguments.operation in {"preflight", "install"}
        stage_fields = (
            "agent_image",
            "builder_image",
            "service_url",
            "agent_instance_id",
            "key_id",
            "private_key",
            "ca_file",
        )
        has_stage_fields = [getattr(arguments, name) is not None for name in stage_fields]
        if (
            needs_archive != (arguments.archive is not None)
            or (arguments.operation == "stage-agent") != all(has_stage_fields)
            or (arguments.operation != "stage-agent" and any(has_stage_fields))
        ):
            raise PersonalDevNativeBuilderRuntimeInstallError("arguments_invalid")
        profile = load_native_builder_runtime_profile(arguments.profile)
        installer = installer_factory(profile)
        if arguments.operation == "preflight":
            receipt = installer.preflight(arguments.archive)
        elif arguments.operation == "install":
            receipt = installer.install(arguments.archive)
        elif arguments.operation == "stage-agent":
            receipt = installer.stage_agent(
                agent_image=arguments.agent_image,
                builder_image=arguments.builder_image,
                service_url=arguments.service_url,
                agent_instance_id=arguments.agent_instance_id,
                key_id=arguments.key_id,
                private_key=arguments.private_key,
                ca_file=arguments.ca_file,
            )
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
            raise PersonalDevNativeBuilderRuntimeInstallError("receipt_invalid")
        sys.stdout.write(encoded + "\n")
        return 0
    except PersonalDevNativeBuilderRuntimeInstallError as exc:
        sys.stderr.write(f"error:{exc.code}\n")
        return 2 if exc.code == "arguments_invalid" else 1
    except NativeBuilderRuntimeProfileError:
        sys.stderr.write("error:profile_invalid\n")
        return 1
    except Exception:
        sys.stderr.write("error:internal_error\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "NativeBuilderCommandResult",
    "NativeBuilderInstallContext",
    "NativeBuilderRunner",
    "NativeBuilderSubprocessRunner",
    "PersonalDevNativeBuilderRuntimeInstallError",
    "PersonalDevNativeBuilderRuntimeInstaller",
    "main",
]
