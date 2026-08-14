"""One-time bootstrap capability handoff for trusted executable workers."""

from __future__ import annotations

import errno
import hashlib
import os
import secrets
import stat
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Protocol, cast
from uuid import UUID, uuid4, uuid5

from pydantic import Field, field_validator

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2, PhysicalJobBindingV2
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    StrictV2Model,
    canonical_executable_bytes,
)

_MAX_HANDOFF_BYTES = 64 * 1024
_OPERATION_NAMESPACE = UUID("cb359b0c-a844-4bc5-9592-a4c35e344f3d")


class BootstrapHandoffError(RuntimeError):
    """A bootstrap handoff could not be created, validated, or consumed safely."""


class _AdmissionRegistrationClient(Protocol):
    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str,
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class BootstrapHandoffLease:
    reference: str
    bootstrap_sha256: str


class BootstrapHandoffRecordV2(StrictV2Model):
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    capability: Annotated[str, Field(min_length=43, max_length=512)]
    capability_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    expires_at: datetime
    trusted_launcher_release_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    protected_admission_route_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]

    @field_validator("expires_at")
    @classmethod
    def _expires_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("handoff expiry must be timezone-aware")
        return value.astimezone(UTC)


class BootstrapHandoffClaimV2(StrictV2Model):
    record: BootstrapHandoffRecordV2
    physical: PhysicalJobBindingV2
    worker_registration: ExecutableWorkerRegistrationV2
    worker_credential: Annotated[str, Field(min_length=43, max_length=512)]


class BootstrapHandoffCredentialV2(StrictV2Model):
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    capability_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    expires_at: datetime
    trusted_launcher_release_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    protected_admission_route_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    physical: PhysicalJobBindingV2
    worker_registration: ExecutableWorkerRegistrationV2
    worker_credential: Annotated[str, Field(min_length=43, max_length=512)]

    @field_validator("expires_at")
    @classmethod
    def _expires_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("handoff credential expiry must be timezone-aware")
        return value.astimezone(UTC)


def _private_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise BootstrapHandoffError("handoff directory is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise BootstrapHandoffError(
            "handoff directory must be a current-UID-owned 0700 nonsymlink directory"
        )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _open_private_regular(path: Path) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise BootstrapHandoffError("handoff record is unavailable or already used") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        raise BootstrapHandoffError("handoff record must be a current-UID-owned 0600 nonsymlink")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise BootstrapHandoffError("handoff record must be a nonsymlink") from exc
        raise BootstrapHandoffError("handoff record is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise BootstrapHandoffError("handoff record changed while opening")
        payload = os.read(descriptor, _MAX_HANDOFF_BYTES + 1)
    finally:
        os.close(descriptor)
    if not payload or len(payload) > _MAX_HANDOFF_BYTES:
        raise BootstrapHandoffError("handoff record exceeds its byte bound")
    return payload


def _reference(binding: ExecutableIntentBindingV2) -> str:
    digest = hashlib.sha256(canonical_executable_bytes(binding)).hexdigest()
    return f"{digest}.json"


def _record_path(directory: Path, reference: str) -> Path:
    if (
        not reference.endswith(".json")
        or "/" in reference
        or "\\" in reference
        or ".." in reference
        or len(reference) != 69
        or any(item not in "0123456789abcdef" for item in reference[:64])
    ):
        raise BootstrapHandoffError("handoff reference is invalid")
    return directory / reference


def _publish_private_new(path: Path, payload: bytes) -> bool:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(temporary, flags, 0o600)
    linked = False
    try:
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        try:
            os.link(temporary, path)
            linked = True
        except FileExistsError:
            return False
        return True
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        _fsync_directory(path.parent)
        if linked:
            _fsync_directory(path.parent)


def _unlink_private_if_present(path: Path, *, label: str) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise BootstrapHandoffError(f"{label} is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise BootstrapHandoffError(f"{label} must be a current-UID-owned 0600 nonsymlink")
    path.unlink()
    return True


class BootstrapHandoffStore:
    """Create and retain one clear bootstrap capability in an owner-only directory."""

    def __init__(self, directory: Path) -> None:
        _private_directory(directory)
        self.directory = directory

    def reference_for(self, binding: ExecutableIntentBindingV2) -> str:
        return _reference(binding)

    def prepare(
        self,
        binding: ExecutableIntentBindingV2,
        *,
        bootstrap_registration_epoch: int,
        expires_at: datetime,
        trusted_launcher_release_sha256: str,
        protected_admission_route_sha256: str,
    ) -> BootstrapHandoffLease:
        reference = self.reference_for(binding)
        path = _record_path(self.directory, reference)
        if path.exists():
            record = self._load(path)
            self._assert_record(
                record,
                binding=binding,
                bootstrap_registration_epoch=bootstrap_registration_epoch,
                trusted_launcher_release_sha256=trusted_launcher_release_sha256,
                protected_admission_route_sha256=protected_admission_route_sha256,
                now=None,
            )
            return BootstrapHandoffLease(
                reference=reference,
                bootstrap_sha256=record.capability_sha256,
            )
        capability = secrets.token_urlsafe(32)
        record = BootstrapHandoffRecordV2(
            binding=binding,
            bootstrap_registration_epoch=bootstrap_registration_epoch,
            capability=capability,
            capability_sha256=hashlib.sha256(capability.encode("ascii")).hexdigest(),
            expires_at=expires_at,
            trusted_launcher_release_sha256=trusted_launcher_release_sha256,
            protected_admission_route_sha256=protected_admission_route_sha256,
        )
        if not _publish_private_new(path, canonical_executable_bytes(record)):
            record = self._load(path)
            self._assert_record(
                record,
                binding=binding,
                bootstrap_registration_epoch=bootstrap_registration_epoch,
                trusted_launcher_release_sha256=trusted_launcher_release_sha256,
                protected_admission_route_sha256=protected_admission_route_sha256,
                now=None,
            )
        return BootstrapHandoffLease(reference=reference, bootstrap_sha256=record.capability_sha256)

    def _load(self, path: Path) -> BootstrapHandoffRecordV2:
        try:
            return BootstrapHandoffRecordV2.model_validate_json(_open_private_regular(path))
        except ValueError as exc:
            raise BootstrapHandoffError("handoff record is invalid") from exc

    @staticmethod
    def _assert_record(
        record: BootstrapHandoffRecordV2,
        *,
        binding: ExecutableIntentBindingV2,
        bootstrap_registration_epoch: int,
        trusted_launcher_release_sha256: str,
        protected_admission_route_sha256: str,
        now: datetime | None,
    ) -> None:
        if record.protected_admission_route_sha256 != protected_admission_route_sha256:
            raise BootstrapHandoffError("handoff admission route changed")
        if (
            record.binding != binding
            or record.bootstrap_registration_epoch != bootstrap_registration_epoch
            or record.trusted_launcher_release_sha256 != trusted_launcher_release_sha256
            or hashlib.sha256(record.capability.encode("ascii")).hexdigest()
            != record.capability_sha256
        ):
            raise BootstrapHandoffError("handoff record binding changed")
        if now is not None and record.expires_at <= now:
            raise BootstrapHandoffError("handoff record expired")


def _load_claim(path: Path) -> BootstrapHandoffClaimV2:
    try:
        return BootstrapHandoffClaimV2.model_validate_json(_open_private_regular(path))
    except ValueError as exc:
        raise BootstrapHandoffError("handoff claim is invalid") from exc


def _load_credential(path: Path) -> BootstrapHandoffCredentialV2:
    try:
        return BootstrapHandoffCredentialV2.model_validate_json(_open_private_regular(path))
    except ValueError as exc:
        raise BootstrapHandoffError("handoff credential is invalid") from exc


def _claim_for(
    record: BootstrapHandoffRecordV2, physical: PhysicalJobBindingV2
) -> BootstrapHandoffClaimV2:
    worker_credential = secrets.token_urlsafe(32)
    registration = ExecutableWorkerRegistrationV2(
        operation_id=uuid5(_OPERATION_NAMESPACE, f"worker-register:{physical.binding.intent_id}"),
        binding=physical.binding,
        bootstrap_registration_epoch=physical.bootstrap_registration_epoch,
        protected_registration_epoch=physical.bootstrap_registration_epoch + 1,
        slurm_job_id=physical.slurm_job_id,
        worker_id=uuid4(),
        worker_incarnation=uuid4(),
        worker_credential_sha256=hashlib.sha256(worker_credential.encode("ascii")).hexdigest(),
    )
    return BootstrapHandoffClaimV2(
        record=record,
        physical=physical,
        worker_registration=registration,
        worker_credential=worker_credential,
    )


def _credential_for(claim: BootstrapHandoffClaimV2) -> BootstrapHandoffCredentialV2:
    return BootstrapHandoffCredentialV2(
        binding=claim.record.binding,
        bootstrap_registration_epoch=claim.record.bootstrap_registration_epoch,
        capability_sha256=claim.record.capability_sha256,
        expires_at=claim.record.expires_at,
        trusted_launcher_release_sha256=claim.record.trusted_launcher_release_sha256,
        protected_admission_route_sha256=claim.record.protected_admission_route_sha256,
        physical=claim.physical,
        worker_registration=claim.worker_registration,
        worker_credential=claim.worker_credential,
    )


def _assert_claim(
    claim: BootstrapHandoffClaimV2,
    *,
    physical: PhysicalJobBindingV2,
    protected_admission_route_sha256: str,
    now: datetime,
) -> None:
    BootstrapHandoffStore._assert_record(
        claim.record,
        binding=physical.binding,
        bootstrap_registration_epoch=physical.bootstrap_registration_epoch,
        trusted_launcher_release_sha256=physical.binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=protected_admission_route_sha256,
        now=now,
    )
    registration = claim.worker_registration
    if (
        claim.physical != physical
        or registration.binding != physical.binding
        or registration.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
        or registration.protected_registration_epoch != physical.bootstrap_registration_epoch + 1
        or registration.slurm_job_id != physical.slurm_job_id
        or registration.worker_credential_sha256
        != hashlib.sha256(claim.worker_credential.encode("ascii")).hexdigest()
    ):
        raise BootstrapHandoffError("handoff claim binding changed")


def _assert_credential(
    credential: BootstrapHandoffCredentialV2,
    *,
    physical: PhysicalJobBindingV2,
    protected_admission_route_sha256: str,
    now: datetime,
) -> None:
    if (
        credential.binding != physical.binding
        or credential.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
        or credential.trusted_launcher_release_sha256
        != physical.binding.execution.trusted_fleet_release_sha256
        or credential.protected_admission_route_sha256 != protected_admission_route_sha256
        or credential.physical != physical
    ):
        raise BootstrapHandoffError("handoff credential binding changed")
    if credential.expires_at <= now:
        raise BootstrapHandoffError("handoff credential expired")
    registration = credential.worker_registration
    if (
        registration.binding != physical.binding
        or registration.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
        or registration.protected_registration_epoch != physical.bootstrap_registration_epoch + 1
        or registration.slurm_job_id != physical.slurm_job_id
        or registration.worker_credential_sha256
        != hashlib.sha256(credential.worker_credential.encode("ascii")).hexdigest()
    ):
        raise BootstrapHandoffError("handoff credential registration changed")


def _route_sha256(admission: object, binding: ExecutableIntentBindingV2) -> str:
    route = getattr(admission, "bootstrap_handoff_route_sha256", None)
    if not callable(route):
        raise BootstrapHandoffError("handoff admission route binding is unavailable")
    value = route(binding)
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(item not in "0123456789abcdef" for item in value)
    ):
        raise BootstrapHandoffError("handoff admission route binding is invalid")
    return value


async def consume_bootstrap_handoff(
    directory: Path,
    reference: str,
    physical: PhysicalJobBindingV2,
    admission: object,
    *,
    now: Callable[[], datetime],
) -> str:
    """Trusted-wrapper side exchange of one bootstrap capability for worker credential."""

    _private_directory(directory)
    path = _record_path(directory, reference)
    used = path.with_suffix(".used")
    credential_path = path.with_suffix(".credential")
    current = now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise BootstrapHandoffError("handoff consumer time must be timezone-aware")
    current = current.astimezone(UTC)
    protected_admission_route_sha256 = _route_sha256(admission, physical.binding)
    if credential_path.exists() or credential_path.is_symlink():
        credential = _load_credential(credential_path)
        _assert_credential(
            credential,
            physical=physical,
            protected_admission_route_sha256=protected_admission_route_sha256,
            now=current,
        )
        changed = _unlink_private_if_present(path, label="handoff record")
        changed = _unlink_private_if_present(used, label="handoff claim") or changed
        if changed:
            _fsync_directory(directory)
        return credential.worker_credential
    if used.exists() or used.is_symlink():
        claim = _load_claim(used)
    else:
        try:
            record = BootstrapHandoffStore(directory)._load(path)
            BootstrapHandoffStore._assert_record(
                record,
                binding=physical.binding,
                bootstrap_registration_epoch=physical.bootstrap_registration_epoch,
                trusted_launcher_release_sha256=(
                    physical.binding.execution.trusted_fleet_release_sha256
                ),
                protected_admission_route_sha256=protected_admission_route_sha256,
                now=current,
            )
            claim = _claim_for(record, physical)
            if not _publish_private_new(used, canonical_executable_bytes(claim)):
                claim = _load_claim(used)
            else:
                _unlink_private_if_present(path, label="handoff record")
                _fsync_directory(directory)
        except BootstrapHandoffError as original_exc:
            try:
                claim = _load_claim(path)
            except BootstrapHandoffError:
                raise original_exc from None
    _assert_claim(
        claim,
        physical=physical,
        protected_admission_route_sha256=protected_admission_route_sha256,
        now=current,
    )
    register_worker = getattr(admission, "register_worker", None)
    if not callable(register_worker):
        raise BootstrapHandoffError("handoff worker registration is unavailable")
    await cast(_AdmissionRegistrationClient, admission).register_worker(
        claim.worker_registration,
        bootstrap_capability=claim.record.capability,
    )
    credential = _credential_for(claim)
    if not _publish_private_new(credential_path, canonical_executable_bytes(credential)):
        credential = _load_credential(credential_path)
    _assert_credential(
        credential,
        physical=physical,
        protected_admission_route_sha256=protected_admission_route_sha256,
        now=current,
    )
    changed = _unlink_private_if_present(used, label="handoff claim")
    changed = _unlink_private_if_present(path, label="handoff record") or changed
    if changed:
        _fsync_directory(directory)
    return credential.worker_credential


__all__ = [
    "BootstrapHandoffClaimV2",
    "BootstrapHandoffCredentialV2",
    "BootstrapHandoffError",
    "BootstrapHandoffLease",
    "BootstrapHandoffRecordV2",
    "BootstrapHandoffStore",
    "consume_bootstrap_handoff",
]
