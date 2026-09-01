"""Fixed controller-local publication of the external-supervisor credential."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import os
import re
import secrets
import stat
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

_CREDENTIAL_PATH = Path("/var/lib/loom-staging-rollout/external-supervisor.kubeconfig")
_SOURCE_KUBECONFIG = Path("/var/lib/loom-staging-rollout/kubeconfig")
_PUBLISHER_RELATIVE = Path("deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh")
_EXECUTION_HOSTS = frozenset({"gx10-01c7", "TRT-EAI-OLDLAB-1"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_CREDENTIAL_BYTES = 1024 * 1024
_MAX_COMMAND_OUTPUT = 1024 * 1024
_COMMAND_TIMEOUT_SECONDS = 60.0


class CommandResult(Protocol):
    returncode: int
    stdout: bytes
    stderr: bytes


CredentialCommandRunner = Callable[[Sequence[str], Mapping[str, str], float], CommandResult]


class ProtectedExternalSupervisorCredentialTransport(Protocol):
    def observe(self) -> ExternalSupervisorCredentialEvidence | None: ...

    def publish(self) -> ExternalSupervisorCredentialEvidence: ...


@dataclass(frozen=True, slots=True)
class ExternalSupervisorCredentialEvidence:
    """Non-secret proof for one fixed controller-local runtime credential."""

    execution_host: str
    kubeconfig_sha256: str
    uid: int
    gid: int
    mode: int
    size: int
    database_secret_readable: bool
    witness_config_map_readable: bool
    pods_exec_denied: bool

    def __post_init__(self) -> None:
        if (
            self.execution_host not in _EXECUTION_HOSTS
            or _SHA256_RE.fullmatch(self.kubeconfig_sha256) is None
            or self.uid < 0
            or self.gid < 0
            or self.mode != 0o600
            or not 1 <= self.size <= _MAX_CREDENTIAL_BYTES
            or self.database_secret_readable is not True
            or self.witness_config_map_readable is not True
            or self.pods_exec_denied is not True
        ):
            raise ValueError("external supervisor credential evidence is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "database_secret_readable": self.database_secret_readable,
            "execution_host": self.execution_host,
            "gid": self.gid,
            "kubeconfig_sha256": self.kubeconfig_sha256,
            "mode": self.mode,
            "pods_exec_denied": self.pods_exec_denied,
            "size": self.size,
            "uid": self.uid,
            "witness_config_map_readable": self.witness_config_map_readable,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ExternalSupervisorCredentialEvidence:
        expected = {
            "database_secret_readable",
            "execution_host",
            "gid",
            "kubeconfig_sha256",
            "mode",
            "pods_exec_denied",
            "size",
            "uid",
            "witness_config_map_readable",
        }
        if set(value) != expected:
            raise ValueError("external supervisor credential evidence is invalid")
        fields = dict(value)
        if (
            type(fields["execution_host"]) is not str
            or type(fields["kubeconfig_sha256"]) is not str
            or type(fields["uid"]) is not int
            or type(fields["gid"]) is not int
            or type(fields["mode"]) is not int
            or type(fields["size"]) is not int
            or type(fields["database_secret_readable"]) is not bool
            or type(fields["witness_config_map_readable"]) is not bool
            or type(fields["pods_exec_denied"]) is not bool
        ):
            raise ValueError("external supervisor credential evidence is invalid")
        return cls(**fields)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class _CredentialMetadata:
    device: int
    inode: int
    mode: int
    uid: int
    gid: int
    link_count: int
    size: int
    mtime_ns: int
    ctime_ns: int
    sha256: str


def _subprocess_run(
    argv: Sequence[str],
    environment: Mapping[str, str],
    timeout_seconds: float,
) -> CommandResult:
    result = subprocess.run(
        tuple(argv),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout_seconds,
        env=dict(environment),
    )
    return subprocess.CompletedProcess(
        args=tuple(argv),
        returncode=result.returncode,
        stdout=b"",
        stderr=b"",
    )


@dataclass(frozen=True, slots=True)
class FixedLocalExternalSupervisorCredentialTransport:
    """Publish only one candidate script to one fixed controller-local path."""

    candidate_root: Path
    execution_host: str
    service_uid: int
    service_gid: int
    source_kubeconfig: Path = _SOURCE_KUBECONFIG
    output_kubeconfig: Path = _CREDENTIAL_PATH
    promote_existing_source: bool = False
    run: CredentialCommandRunner = _subprocess_run

    def __post_init__(self) -> None:
        publisher = self.candidate_root / _PUBLISHER_RELATIVE
        if (
            not self.candidate_root.is_absolute()
            or ".." in self.candidate_root.parts
            or self.execution_host not in _EXECUTION_HOSTS
            or self.service_uid < 0
            or self.service_gid < 0
            or not self.source_kubeconfig.is_absolute()
            or ".." in self.source_kubeconfig.parts
            or not self.output_kubeconfig.is_absolute()
            or ".." in self.output_kubeconfig.parts
            or self.source_kubeconfig == self.output_kubeconfig
            or type(self.promote_existing_source) is not bool
            or (self.promote_existing_source and self.execution_host != "gx10-01c7")
            or not callable(self.run)
            or not self.output_kubeconfig.parent.exists()
            or not publisher.exists()
            or publisher.is_symlink()
            or not publisher.is_file()
            or stat.S_IMODE(publisher.stat().st_mode) & 0o022
        ):
            raise ValueError("external supervisor credential transport is invalid")

    @property
    def publisher(self) -> Path:
        return self.candidate_root / _PUBLISHER_RELATIVE

    def observe(self) -> ExternalSupervisorCredentialEvidence | None:
        try:
            before = self._metadata(self.output_kubeconfig)
        except FileNotFoundError:
            return None
        self._run_checked(
            (str(self.publisher), "--check", str(self.output_kubeconfig)),
            environment=self._environment(include_source=False),
            failure="external supervisor credential check failed safely",
        )
        after = self._metadata(self.output_kubeconfig)
        if after != before:
            raise RuntimeError("external supervisor credential changed during check")
        return self._evidence(after)

    def publish(self) -> ExternalSupervisorCredentialEvidence:
        current = self.observe()
        if current is not None:
            return current
        if self.promote_existing_source:
            self._promote_existing_source()
        else:
            self._run_checked(
                (str(self.publisher), str(self.output_kubeconfig)),
                environment=self._environment(include_source=True),
                failure="external supervisor credential publication failed safely",
            )
        published = self.observe()
        if published is None:
            raise RuntimeError("external supervisor credential publication failed safely")
        return published

    def _promote_existing_source(self) -> None:
        before = self._metadata(self.source_kubeconfig)
        self._run_checked(
            (str(self.publisher), "--check", str(self.source_kubeconfig)),
            environment=self._environment(include_source=False),
            failure="external supervisor credential publication failed safely",
        )
        after, payload = self._read_credential(
            self.source_kubeconfig,
            include_payload=True,
        )
        if after != before or payload is None:
            raise RuntimeError("external supervisor credential publication failed safely")
        temporary_name = f".{self.output_kubeconfig.name}.publish.{secrets.token_hex(16)}"
        temporary = self.output_kubeconfig.with_name(temporary_name)
        directory = -1
        descriptor = -1
        try:
            directory = os.open(
                self.output_kubeconfig.parent,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory,
            )
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise RuntimeError("external supervisor credential publication failed safely")
                offset += written
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            staged = self._metadata(temporary)
            if staged.sha256 != after.sha256 or staged.size != after.size:
                raise RuntimeError("external supervisor credential publication failed safely")
            _rename_noreplace(
                directory,
                temporary_name,
                self.output_kubeconfig.name,
            )
            os.fsync(directory)
        except (OSError, ValueError) as exc:
            raise RuntimeError("external supervisor credential publication failed safely") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if directory >= 0:
                try:
                    os.unlink(temporary_name, dir_fd=directory)
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
                os.close(directory)

    def _metadata(self, path: Path) -> _CredentialMetadata:
        metadata, _payload = self._read_credential(path, include_payload=False)
        return metadata

    def _read_credential(
        self,
        path: Path,
        *,
        include_payload: bool,
    ) -> tuple[_CredentialMetadata, bytes | None]:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            metadata = os.fstat(descriptor)
            mode = stat.S_IMODE(metadata.st_mode)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or metadata.st_gid != self.service_gid
                or mode != 0o600
                or metadata.st_nlink != 1
                or not 1 <= metadata.st_size <= _MAX_CREDENTIAL_BYTES
            ):
                raise ValueError("external supervisor credential metadata is unsafe")
            digest = hashlib.sha256()
            chunks: list[bytes] = []
            remaining = metadata.st_size
            while remaining:
                payload = os.read(descriptor, min(65536, remaining))
                if not payload:
                    raise ValueError("external supervisor credential metadata is unsafe")
                digest.update(payload)
                if include_payload:
                    chunks.append(payload)
                remaining -= len(payload)
            stable = _CredentialMetadata(
                device=metadata.st_dev,
                inode=metadata.st_ino,
                mode=metadata.st_mode,
                uid=metadata.st_uid,
                gid=metadata.st_gid,
                link_count=metadata.st_nlink,
                size=metadata.st_size,
                mtime_ns=metadata.st_mtime_ns,
                ctime_ns=metadata.st_ctime_ns,
                sha256=digest.hexdigest(),
            )
            after = os.fstat(descriptor)
            if _stable_stat(after, sha256=stable.sha256) != stable:
                raise RuntimeError("external supervisor credential changed during read")
            return stable, b"".join(chunks) if include_payload else None
        except OSError as exc:
            raise ValueError("external supervisor credential metadata is unsafe") from exc
        finally:
            os.close(descriptor)

    def _evidence(
        self,
        metadata: _CredentialMetadata,
    ) -> ExternalSupervisorCredentialEvidence:
        return ExternalSupervisorCredentialEvidence(
            execution_host=self.execution_host,
            kubeconfig_sha256=metadata.sha256,
            uid=metadata.uid,
            gid=metadata.gid,
            mode=stat.S_IMODE(metadata.mode),
            size=metadata.size,
            database_secret_readable=True,
            witness_config_map_readable=True,
            pods_exec_denied=True,
        )

    def _environment(self, *, include_source: bool) -> dict[str, str]:
        environment = {
            "HOME": str(self.source_kubeconfig.parent),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "LOGNAME": "loom-rollout",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "USER": "loom-rollout",
        }
        if include_source:
            environment["KUBECONFIG"] = str(self.source_kubeconfig)
        return environment

    def _run_checked(
        self,
        argv: Sequence[str],
        *,
        environment: Mapping[str, str],
        failure: str,
    ) -> None:
        result = self.run(tuple(argv), dict(environment), _COMMAND_TIMEOUT_SECONDS)
        if (
            result.returncode != 0
            or len(result.stdout) > _MAX_COMMAND_OUTPUT
            or len(result.stderr) > _MAX_COMMAND_OUTPUT
        ):
            raise RuntimeError(failure)


def _stable_stat(metadata: os.stat_result, *, sha256: str) -> _CredentialMetadata:
    """Return identity/security/change fields while intentionally excluding atime."""

    return _CredentialMetadata(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        mode=metadata.st_mode,
        uid=metadata.st_uid,
        gid=metadata.st_gid,
        link_count=metadata.st_nlink,
        size=metadata.st_size,
        mtime_ns=metadata.st_mtime_ns,
        ctime_ns=metadata.st_ctime_ns,
        sha256=sha256,
    )


def _rename_noreplace(directory_fd: int, source: str, destination: str) -> None:
    library = ctypes.CDLL(None, use_errno=True)
    try:
        rename = library.renameat2
    except AttributeError as exc:
        raise OSError(errno.ENOSYS, "atomic credential publication is unavailable") from exc
    rename.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    rename.restype = ctypes.c_int
    if (
        rename(
            directory_fd,
            os.fsencode(source),
            directory_fd,
            os.fsencode(destination),
            1,
        )
        == 0
    ):
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, "credential destination exists")
    raise OSError(error_number, "atomic credential publication failed")


__all__ = [
    "ExternalSupervisorCredentialEvidence",
    "FixedLocalExternalSupervisorCredentialTransport",
    "ProtectedExternalSupervisorCredentialTransport",
]
