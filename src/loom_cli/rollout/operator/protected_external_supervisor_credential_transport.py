"""Fixed controller-local publication of the external-supervisor credential."""

from __future__ import annotations

import hashlib
import os
import re
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
            before = self._metadata()
        except FileNotFoundError:
            return None
        self._run_checked(
            (str(self.publisher), "--check", str(self.output_kubeconfig)),
            environment=self._environment(include_source=False),
            failure="external supervisor credential check failed safely",
        )
        after = self._metadata()
        if after != before:
            raise RuntimeError("external supervisor credential changed during check")
        return self._evidence(after)

    def publish(self) -> ExternalSupervisorCredentialEvidence:
        current = self.observe()
        if current is not None:
            return current
        self._run_checked(
            (str(self.publisher), str(self.output_kubeconfig)),
            environment=self._environment(include_source=True),
            failure="external supervisor credential publication failed safely",
        )
        published = self.observe()
        if published is None:
            raise RuntimeError("external supervisor credential publication failed safely")
        return published

    def _metadata(self) -> tuple[int, int, int, int, str]:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.output_kubeconfig, flags)
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
            remaining = metadata.st_size
            while remaining:
                payload = os.read(descriptor, min(65536, remaining))
                if not payload:
                    raise ValueError("external supervisor credential metadata is unsafe")
                digest.update(payload)
                remaining -= len(payload)
            if os.fstat(descriptor) != metadata:
                raise RuntimeError("external supervisor credential changed during read")
            return metadata.st_uid, metadata.st_gid, mode, metadata.st_size, digest.hexdigest()
        except OSError as exc:
            raise ValueError("external supervisor credential metadata is unsafe") from exc
        finally:
            os.close(descriptor)

    def _evidence(
        self,
        metadata: tuple[int, int, int, int, str],
    ) -> ExternalSupervisorCredentialEvidence:
        uid, gid, mode, size, digest = metadata
        return ExternalSupervisorCredentialEvidence(
            execution_host=self.execution_host,
            kubeconfig_sha256=digest,
            uid=uid,
            gid=gid,
            mode=mode,
            size=size,
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


__all__ = [
    "ExternalSupervisorCredentialEvidence",
    "FixedLocalExternalSupervisorCredentialTransport",
    "ProtectedExternalSupervisorCredentialTransport",
]
