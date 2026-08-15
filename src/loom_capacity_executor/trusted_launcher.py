"""Trusted executable worker launcher process boundary."""

from __future__ import annotations

import argparse
import asyncio
import errno
import hashlib
import os
import posixpath
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_executor.bootstrap_handoff import (
    BootstrapHandoffError,
    claim_bootstrap_handoff_launch,
    consume_bootstrap_handoff,
    resolve_bootstrap_handoff_physical_binding,
)
from loom_capacity_executor.runtime import RoutedExecutableAdmissionClient
from loom_capacity_executor.slurm_contracts import SlurmExecutableIdentityV2, SlurmFileIdentityV2
from loom_capacity_manager.executable_contracts import StrictV2Model

WORKER_CREDENTIAL_ENV = "LOOM_EXECUTOR_WORKER_CREDENTIAL"
_MAX_TRUSTED_CONFIG_BYTES = 64 * 1024
_IMAGE_DIGEST_PATTERN = (
    r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
)

_Execvpe = Callable[[str, tuple[str, ...], Mapping[str, str]], Any]
_AdmissionFactory = Callable[..., object]


class TrustedCandidateExecutableV2(StrictV2Model):
    """Candidate executable identity verified before scoped credential handoff."""

    path: Annotated[str, Field(min_length=1, max_length=4096)]
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    owner_uid: Annotated[int, Field(ge=0, le=(1 << 31) - 1)]
    mode: Annotated[int, Field(ge=0, le=0o7777)]

    @field_validator("path")
    @classmethod
    def _absolute_path(cls, value: str) -> str:
        components = value.split("/")[1:]
        if (
            "\0" in value
            or not Path(value).is_absolute()
            or value == "/"
            or value.startswith("//")
            or value.endswith("/")
            or any(component in {"", ".", ".."} for component in components)
            or posixpath.normpath(value) != value
        ):
            raise ValueError("trusted launcher candidate path must be canonical and absolute")
        return value

    @field_validator("mode")
    @classmethod
    def _safe_executable_mode(cls, value: int) -> int:
        if value & 0o7000 or not (value & stat.S_IXUSR) or value & 0o022:
            raise ValueError(
                "trusted launcher candidate mode must be owner-executable and unwritable"
            )
        return value


class TrustedLauncherConfigV2(StrictV2Model):
    """Pinned trusted-wrapper config loaded by the shipped launcher process."""

    handoff_directory: Annotated[str, Field(min_length=1, max_length=4096)]
    admission_directory: Annotated[str, Field(min_length=1, max_length=4096)]
    admission_directory_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    candidate_executable: TrustedCandidateExecutableV2
    candidate_image_digest: Annotated[str, Field(max_length=512, pattern=_IMAGE_DIGEST_PATTERN)]
    candidate_argv: Annotated[tuple[str, ...], Field(min_length=1, max_length=128)]

    @field_validator("handoff_directory", "admission_directory")
    @classmethod
    def _absolute_directory(cls, value: str) -> str:
        path = Path(value)
        if "\0" in value or not path.is_absolute() or path == Path("/") or ".." in path.parts:
            raise ValueError("trusted launcher directory must be an absolute path")
        return value

    @field_validator("candidate_argv")
    @classmethod
    def _candidate(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _candidate_argv(value)

    @model_validator(mode="after")
    def _candidate_binding(self) -> TrustedLauncherConfigV2:
        if self.candidate_argv[0] != self.candidate_executable.path:
            raise ValueError("trusted launcher candidate argv differs from executable identity")
        return self


def _candidate_argv(value: tuple[str, ...]) -> tuple[str, ...]:
    if (
        not value
        or not value[0].startswith("/")
        or any(not argument or "\0" in argument for argument in value)
    ):
        raise BootstrapHandoffError(
            "trusted launcher candidate argv must use a non-empty absolute executable"
        )
    return value


async def exec_bootstrap_handoff_candidate(
    directory: Path,
    reference: str,
    physical: PhysicalJobBindingV2,
    admission: object,
    *,
    candidate_argv: tuple[str, ...],
    candidate_exec_file: str | None = None,
    now: Callable[[], datetime],
    environment: Mapping[str, str] | None = None,
    execvpe: _Execvpe = os.execvpe,
) -> None:
    """Exchange the bootstrap handoff, claim the exec boundary, and exec candidate code."""

    argv = _candidate_argv(tuple(candidate_argv))
    await consume_bootstrap_handoff(directory, reference, physical, admission, now=now)
    worker_credential = claim_bootstrap_handoff_launch(
        directory,
        reference,
        physical,
        admission,
        now=now,
    )
    next_environment = dict(os.environ if environment is None else environment)
    next_environment[WORKER_CREDENTIAL_ENV] = worker_credential
    execvpe(candidate_exec_file or argv[0], argv, next_environment)
    raise BootstrapHandoffError("trusted launcher candidate exec returned")


def _split_launcher_argv(argv: Sequence[str]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    try:
        separator = tuple(argv).index("--")
    except ValueError as exc:
        raise BootstrapHandoffError("trusted launcher candidate argv separator is missing") from exc
    launcher_argv = tuple(argv[:separator])
    candidate_argv = tuple(argv[separator + 1 :])
    _candidate_argv(candidate_argv)
    return launcher_argv, candidate_argv


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        allow_abbrev=False,
    )
    parser.add_argument("--launcher-sha256", required=True)
    parser.add_argument("--launcher-owner-uid", required=True)
    parser.add_argument("--launcher-config", required=True)
    parser.add_argument("--launcher-config-sha256", required=True)
    parser.add_argument("--launcher-config-owner-uid", required=True)
    parser.add_argument("--operation-id", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--ownership-token", required=True)
    parser.add_argument("--bootstrap-handoff", required=True)
    return parser


def _sha256(value: str, *, label: str) -> str:
    if len(value) != 64 or any(item not in "0123456789abcdef" for item in value):
        raise BootstrapHandoffError(f"trusted launcher {label} is not a SHA-256 digest")
    return value


def _owner_uid(value: str, *, label: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise BootstrapHandoffError(f"trusted launcher {label} owner uid is invalid") from exc
    if parsed < 0 or parsed > (1 << 31) - 1:
        raise BootstrapHandoffError(f"trusted launcher {label} owner uid is invalid")
    return parsed


def _read_verified_file(
    identity: SlurmExecutableIdentityV2 | SlurmFileIdentityV2,
    *,
    label: str,
    executable: bool,
) -> bytes:
    path = Path(identity.path)
    expected_sha256 = identity.sha256
    expected_owner = identity.owner_uid
    try:
        before = path.lstat()
    except OSError as exc:
        raise BootstrapHandoffError(f"trusted launcher {label} is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != expected_owner
        or (executable and not (before.st_mode & stat.S_IXUSR))
        or (not executable and stat.S_IMODE(before.st_mode) != 0o600)
    ):
        raise BootstrapHandoffError(f"trusted launcher {label} identity is invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise BootstrapHandoffError(f"trusted launcher {label} must be a nonsymlink") from exc
        raise BootstrapHandoffError(f"trusted launcher {label} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != expected_owner
            or (executable and not (opened.st_mode & stat.S_IXUSR))
            or (not executable and stat.S_IMODE(opened.st_mode) != 0o600)
        ):
            raise BootstrapHandoffError(f"trusted launcher {label} changed while opening")
        maximum = _MAX_TRUSTED_CONFIG_BYTES if not executable else 16 * 1024 * 1024
        payload = os.read(descriptor, maximum + 1)
    finally:
        os.close(descriptor)
    if not payload or hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise BootstrapHandoffError(f"trusted launcher {label} digest changed")
    return payload


def _open_verified_candidate(identity: TrustedCandidateExecutableV2) -> int:
    path = Path(identity.path)
    try:
        before = path.lstat()
    except OSError as exc:
        raise BootstrapHandoffError("trusted launcher candidate executable is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != identity.owner_uid
        or stat.S_IMODE(before.st_mode) != identity.mode
        or not (before.st_mode & stat.S_IXUSR)
    ):
        raise BootstrapHandoffError("trusted launcher candidate executable identity is invalid")
    if before.st_uid == os.geteuid() and stat.S_IMODE(before.st_mode) & stat.S_IWUSR:
        raise BootstrapHandoffError(
            "trusted launcher candidate executable mode is writable by current uid"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise BootstrapHandoffError(
                "trusted launcher candidate executable must be a nonsymlink"
            ) from exc
        raise BootstrapHandoffError("trusted launcher candidate executable is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != identity.owner_uid
            or stat.S_IMODE(opened.st_mode) != identity.mode
            or not (opened.st_mode & stat.S_IXUSR)
        ):
            raise BootstrapHandoffError(
                "trusted launcher candidate executable changed while opening"
            )
        if opened.st_uid == os.geteuid() and stat.S_IMODE(opened.st_mode) & stat.S_IWUSR:
            raise BootstrapHandoffError(
                "trusted launcher candidate executable mode is writable by current uid"
            )
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        if digest.hexdigest() != identity.sha256:
            raise BootstrapHandoffError("trusted launcher candidate executable digest changed")
        os.lseek(descriptor, 0, os.SEEK_SET)
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _load_trusted_config(identity: SlurmFileIdentityV2) -> TrustedLauncherConfigV2:
    payload = _read_verified_file(identity, label="config", executable=False)
    if len(payload) > _MAX_TRUSTED_CONFIG_BYTES:
        raise BootstrapHandoffError("trusted launcher config exceeds its byte bound")
    try:
        return TrustedLauncherConfigV2.model_validate_json(payload)
    except ValueError as exc:
        raise BootstrapHandoffError("trusted launcher config is invalid") from exc


async def run_trusted_launcher(
    argv: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    now: Callable[[], datetime],
    admission_factory: _AdmissionFactory = RoutedExecutableAdmissionClient,
    execvpe: _Execvpe = os.execvpe,
) -> None:
    """Run the shipped trusted-wrapper argv/env boundary before candidate exec."""

    return await run_trusted_launcher_process(
        ("trusted-launcher", *argv),
        environment=environment,
        now=now,
        admission_factory=admission_factory,
        execvpe=execvpe,
        verify_launcher=False,
    )


async def run_trusted_launcher_process(
    process_argv: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    now: Callable[[], datetime],
    admission_factory: _AdmissionFactory = RoutedExecutableAdmissionClient,
    execvpe: _Execvpe = os.execvpe,
    verify_launcher: bool = True,
) -> None:
    """Run the shipped trusted-wrapper process argv/env boundary before candidate exec."""

    if len(process_argv) < 2:
        raise BootstrapHandoffError("trusted launcher argv is empty")
    launcher_path = process_argv[0]
    args = _parser().parse_args(list(process_argv[1:]))
    launcher_identity = SlurmExecutableIdentityV2(
        path=launcher_path,
        sha256=_sha256(args.launcher_sha256, label="binary digest"),
        owner_uid=_owner_uid(args.launcher_owner_uid, label="binary"),
    )
    if verify_launcher:
        _read_verified_file(launcher_identity, label="binary", executable=True)
    config_identity = SlurmFileIdentityV2(
        path=args.launcher_config,
        sha256=_sha256(args.launcher_config_sha256, label="config digest"),
        owner_uid=_owner_uid(args.launcher_config_owner_uid, label="config"),
    )
    config = _load_trusted_config(config_identity)
    release_sha256 = _sha256(args.release_sha256, label="release digest")
    if args.image_digest != config.candidate_image_digest:
        raise BootstrapHandoffError("trusted launcher image digest differs from config")
    candidate_descriptor = _open_verified_candidate(config.candidate_executable)
    runtime_environment = os.environ if environment is None else environment
    try:
        slurm_job_id = runtime_environment.get("SLURM_JOB_ID")
        if not isinstance(slurm_job_id, str) or not slurm_job_id:
            raise BootstrapHandoffError("trusted launcher SLURM_JOB_ID is unavailable")
        try:
            operation_id = UUID(args.operation_id)
        except (TypeError, ValueError) as exc:
            raise BootstrapHandoffError("trusted launcher operation id is invalid") from exc
        admission = admission_factory(
            Path(config.admission_directory),
            expected_directory_sha256=config.admission_directory_sha256,
        )
        physical = resolve_bootstrap_handoff_physical_binding(
            Path(config.handoff_directory),
            args.bootstrap_handoff,
            operation_id=operation_id,
            slurm_job_id=slurm_job_id,
            ownership_token=args.ownership_token,
            trusted_launcher_release_sha256=release_sha256,
            now=now,
        )
        await exec_bootstrap_handoff_candidate(
            Path(config.handoff_directory),
            args.bootstrap_handoff,
            physical,
            admission,
            candidate_argv=config.candidate_argv,
            candidate_exec_file=f"/proc/self/fd/{candidate_descriptor}",
            now=now,
            environment=runtime_environment,
            execvpe=execvpe,
        )
    finally:
        os.close(candidate_descriptor)


def main(argv: Sequence[str] | None = None) -> int:
    asyncio.run(
        run_trusted_launcher_process(
            tuple(sys.argv if argv is None else argv),
            now=lambda: datetime.now().astimezone(),
        )
    )
    return 0


__all__ = [
    "WORKER_CREDENTIAL_ENV",
    "TrustedCandidateExecutableV2",
    "TrustedLauncherConfigV2",
    "exec_bootstrap_handoff_candidate",
    "main",
    "run_trusted_launcher",
    "run_trusted_launcher_process",
]


if __name__ == "__main__":
    raise SystemExit(main())
