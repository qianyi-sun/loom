"""One-time bootstrap capability handoff for trusted executable workers."""

from __future__ import annotations

import base64
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


class BootstrapHandoffOwnershipV2(StrictV2Model):
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    ownership_evidence_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    expires_at: datetime
    trusted_launcher_release_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]

    @field_validator("expires_at")
    @classmethod
    def _expires_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("handoff ownership expiry must be timezone-aware")
        return value.astimezone(UTC)


class BootstrapHandoffLaunchV2(StrictV2Model):
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    capability_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    expires_at: datetime
    trusted_launcher_release_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    protected_admission_route_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    physical: PhysicalJobBindingV2
    worker_registration: ExecutableWorkerRegistrationV2
    worker_credential_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    launched_at: datetime

    @field_validator("expires_at", "launched_at")
    @classmethod
    def _aware_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("handoff launch timestamp must be timezone-aware")
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


def _normalize_expiry(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise BootstrapHandoffError("handoff expiry must be timezone-aware")
    return value.astimezone(UTC)


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
        expected_expires_at = _normalize_expiry(expires_at)
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
                expected_expires_at=expected_expires_at,
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
            expires_at=expected_expires_at,
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
                expected_expires_at=expected_expires_at,
                now=None,
            )
        return BootstrapHandoffLease(reference=reference, bootstrap_sha256=record.capability_sha256)

    def revoke_prepared(
        self,
        binding: ExecutableIntentBindingV2,
        *,
        bootstrap_registration_epoch: int,
    ) -> bool:
        """Remove only an unconsumed handoff after protected revocation commits."""

        reference = self.reference_for(binding)
        path = _record_path(self.directory, reference)
        for suffix in (".used", ".credential", ".ownership", ".launched"):
            sidecar = path.with_suffix(suffix)
            if sidecar.exists() or sidecar.is_symlink():
                raise BootstrapHandoffError(
                    "prepared handoff has physical or consumed local evidence"
                )
        if not path.exists() and not path.is_symlink():
            return False
        record = self._load(path)
        if (
            record.binding != binding
            or record.bootstrap_registration_epoch != bootstrap_registration_epoch
        ):
            raise BootstrapHandoffError("prepared handoff revocation binding changed")
        removed = _unlink_private_if_present(path, label="handoff record")
        if removed:
            _fsync_directory(self.directory)
        return removed

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
        expected_expires_at: datetime | None,
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
        if expected_expires_at is not None and record.expires_at != expected_expires_at:
            raise BootstrapHandoffError("handoff record expiry changed")
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


def _load_ownership(path: Path) -> BootstrapHandoffOwnershipV2:
    try:
        return BootstrapHandoffOwnershipV2.model_validate_json(_open_private_regular(path))
    except ValueError as exc:
        raise BootstrapHandoffError("handoff ownership binding is invalid") from exc


def _load_launch(path: Path) -> BootstrapHandoffLaunchV2:
    try:
        return BootstrapHandoffLaunchV2.model_validate_json(_open_private_regular(path))
    except ValueError as exc:
        raise BootstrapHandoffError("handoff launch marker is invalid") from exc


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


def _launch_for(
    credential: BootstrapHandoffCredentialV2, *, launched_at: datetime
) -> BootstrapHandoffLaunchV2:
    return BootstrapHandoffLaunchV2(
        binding=credential.binding,
        bootstrap_registration_epoch=credential.bootstrap_registration_epoch,
        capability_sha256=credential.capability_sha256,
        expires_at=credential.expires_at,
        trusted_launcher_release_sha256=credential.trusted_launcher_release_sha256,
        protected_admission_route_sha256=credential.protected_admission_route_sha256,
        physical=credential.physical,
        worker_registration=credential.worker_registration,
        worker_credential_sha256=hashlib.sha256(
            credential.worker_credential.encode("ascii")
        ).hexdigest(),
        launched_at=launched_at,
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
        expected_expires_at=None,
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


def _assert_launch(
    launch: BootstrapHandoffLaunchV2,
    *,
    physical: PhysicalJobBindingV2,
    protected_admission_route_sha256: str,
    now: datetime,
) -> None:
    if (
        launch.binding != physical.binding
        or launch.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
        or launch.trusted_launcher_release_sha256
        != physical.binding.execution.trusted_fleet_release_sha256
        or launch.protected_admission_route_sha256 != protected_admission_route_sha256
        or launch.physical != physical
    ):
        raise BootstrapHandoffError("handoff launch binding changed")
    if launch.expires_at <= now:
        raise BootstrapHandoffError("handoff launch expired")
    registration = launch.worker_registration
    if (
        registration.binding != physical.binding
        or registration.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
        or registration.protected_registration_epoch != physical.bootstrap_registration_epoch + 1
        or registration.slurm_job_id != physical.slurm_job_id
        or registration.worker_credential_sha256 != launch.worker_credential_sha256
    ):
        raise BootstrapHandoffError("handoff launch registration changed")


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


def _ownership_digest_from_token(token: str) -> str:
    try:
        decoded = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
    except (ValueError, TypeError) as exc:
        raise BootstrapHandoffError("handoff ownership token is invalid") from exc
    if len(decoded) != 32:
        raise BootstrapHandoffError("handoff ownership token digest is invalid")
    return decoded.hex()


def bind_bootstrap_handoff_ownership(
    directory: Path,
    reference: str,
    binding: ExecutableIntentBindingV2,
    *,
    bootstrap_registration_epoch: int,
    ownership_evidence_sha256: str,
    trusted_launcher_release_sha256: str,
    now: Callable[[], datetime],
) -> None:
    """Durably bind the expected signed ownership evidence before Slurm launch."""

    _private_directory(directory)
    path = _record_path(directory, reference)
    current = now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise BootstrapHandoffError("handoff ownership time must be timezone-aware")
    current = current.astimezone(UTC)
    record = BootstrapHandoffStore(directory)._load(path)
    BootstrapHandoffStore._assert_record(
        record,
        binding=binding,
        bootstrap_registration_epoch=bootstrap_registration_epoch,
        trusted_launcher_release_sha256=trusted_launcher_release_sha256,
        protected_admission_route_sha256=record.protected_admission_route_sha256,
        expected_expires_at=None,
        now=current,
    )
    ownership = BootstrapHandoffOwnershipV2(
        binding=binding,
        bootstrap_registration_epoch=bootstrap_registration_epoch,
        ownership_evidence_sha256=ownership_evidence_sha256,
        expires_at=record.expires_at,
        trusted_launcher_release_sha256=trusted_launcher_release_sha256,
    )
    ownership_path = path.with_suffix(".ownership")
    if not _publish_private_new(ownership_path, canonical_executable_bytes(ownership)):
        retained = _load_ownership(ownership_path)
        if retained != ownership:
            raise BootstrapHandoffError("handoff ownership binding changed")


def _physical_binding_for(
    *,
    binding: ExecutableIntentBindingV2,
    bootstrap_registration_epoch: int,
    slurm_job_id: str,
    ownership_token: str,
) -> PhysicalJobBindingV2:
    return PhysicalJobBindingV2(
        operation_id=uuid5(_OPERATION_NAMESPACE, f"physical-bind:{binding.intent_id}"),
        binding=binding,
        bootstrap_registration_epoch=bootstrap_registration_epoch,
        slurm_job_id=slurm_job_id,
        ownership_evidence_sha256=_ownership_digest_from_token(ownership_token),
    )


def resolve_bootstrap_handoff_physical_binding(
    directory: Path,
    reference: str,
    *,
    operation_id: UUID,
    slurm_job_id: str,
    ownership_token: str,
    trusted_launcher_release_sha256: str,
    now: Callable[[], datetime],
) -> PhysicalJobBindingV2:
    """Derive the protected physical binding from Slurm argv/env plus handoff state."""

    _private_directory(directory)
    path = _record_path(directory, reference)
    current = now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise BootstrapHandoffError("handoff physical binding time must be timezone-aware")
    current = current.astimezone(UTC)
    launched = path.with_suffix(".launched")
    credential_path = path.with_suffix(".credential")
    used = path.with_suffix(".used")
    ownership_path = path.with_suffix(".ownership")
    retained_physical: PhysicalJobBindingV2 | None = None
    if launched.exists() or launched.is_symlink():
        launch = _load_launch(launched)
        binding = launch.binding
        bootstrap_registration_epoch = launch.bootstrap_registration_epoch
        expires_at = launch.expires_at
        retained_physical = launch.physical
        release = launch.trusted_launcher_release_sha256
    elif credential_path.exists() or credential_path.is_symlink():
        credential = _load_credential(credential_path)
        binding = credential.binding
        bootstrap_registration_epoch = credential.bootstrap_registration_epoch
        expires_at = credential.expires_at
        retained_physical = credential.physical
        release = credential.trusted_launcher_release_sha256
    elif used.exists() or used.is_symlink():
        claim = _load_claim(used)
        binding = claim.record.binding
        bootstrap_registration_epoch = claim.record.bootstrap_registration_epoch
        expires_at = claim.record.expires_at
        retained_physical = claim.physical
        release = claim.record.trusted_launcher_release_sha256
    else:
        record = BootstrapHandoffStore(directory)._load(path)
        binding = record.binding
        bootstrap_registration_epoch = record.bootstrap_registration_epoch
        expires_at = record.expires_at
        release = record.trusted_launcher_release_sha256
    if operation_id != binding.intent_id:
        raise BootstrapHandoffError("handoff operation id differs from launch binding")
    if release != trusted_launcher_release_sha256:
        raise BootstrapHandoffError("handoff trusted launcher release changed")
    if expires_at <= current:
        raise BootstrapHandoffError("handoff record expired")
    physical = _physical_binding_for(
        binding=binding,
        bootstrap_registration_epoch=bootstrap_registration_epoch,
        slurm_job_id=slurm_job_id,
        ownership_token=ownership_token,
    )
    if retained_physical is None:
        ownership = _load_ownership(ownership_path)
        if (
            ownership.binding != binding
            or ownership.bootstrap_registration_epoch != bootstrap_registration_epoch
            or ownership.trusted_launcher_release_sha256 != trusted_launcher_release_sha256
            or ownership.expires_at <= current
            or ownership.ownership_evidence_sha256 != physical.ownership_evidence_sha256
        ):
            raise BootstrapHandoffError("handoff ownership binding changed")
    if retained_physical is not None and retained_physical != physical:
        raise BootstrapHandoffError("handoff physical binding changed")
    return physical


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
    launched = path.with_suffix(".launched")
    current = now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise BootstrapHandoffError("handoff consumer time must be timezone-aware")
    current = current.astimezone(UTC)
    protected_admission_route_sha256 = _route_sha256(admission, physical.binding)
    if launched.exists() or launched.is_symlink():
        launch = _load_launch(launched)
        _assert_launch(
            launch,
            physical=physical,
            protected_admission_route_sha256=protected_admission_route_sha256,
            now=current,
        )
        raise BootstrapHandoffError("handoff already launched")
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
                expected_expires_at=None,
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


def claim_bootstrap_handoff_launch(
    directory: Path,
    reference: str,
    physical: PhysicalJobBindingV2,
    admission: object,
    *,
    now: Callable[[], datetime],
) -> str:
    """Atomically claim the candidate-exec boundary for a recovered credential."""

    _private_directory(directory)
    path = _record_path(directory, reference)
    used = path.with_suffix(".used")
    credential_path = path.with_suffix(".credential")
    launched = path.with_suffix(".launched")
    current = now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise BootstrapHandoffError("handoff launch time must be timezone-aware")
    current = current.astimezone(UTC)
    protected_admission_route_sha256 = _route_sha256(admission, physical.binding)
    if launched.exists() or launched.is_symlink():
        launch = _load_launch(launched)
        _assert_launch(
            launch,
            physical=physical,
            protected_admission_route_sha256=protected_admission_route_sha256,
            now=current,
        )
        raise BootstrapHandoffError("handoff already launched")
    try:
        credential = _load_credential(credential_path)
    except BootstrapHandoffError as exc:
        raise BootstrapHandoffError(
            "handoff credential is unavailable or already launched"
        ) from exc
    _assert_credential(
        credential,
        physical=physical,
        protected_admission_route_sha256=protected_admission_route_sha256,
        now=current,
    )
    launch = _launch_for(credential, launched_at=current)
    if not _publish_private_new(launched, canonical_executable_bytes(launch)):
        launch = _load_launch(launched)
        _assert_launch(
            launch,
            physical=physical,
            protected_admission_route_sha256=protected_admission_route_sha256,
            now=current,
        )
        raise BootstrapHandoffError("handoff already launched")
    changed = _unlink_private_if_present(credential_path, label="handoff credential")
    changed = _unlink_private_if_present(used, label="handoff claim") or changed
    changed = _unlink_private_if_present(path, label="handoff record") or changed
    changed = (
        _unlink_private_if_present(path.with_suffix(".ownership"), label="handoff ownership")
        or changed
    )
    if changed:
        _fsync_directory(directory)
    return credential.worker_credential


__all__ = [
    "BootstrapHandoffClaimV2",
    "BootstrapHandoffCredentialV2",
    "BootstrapHandoffError",
    "BootstrapHandoffLaunchV2",
    "BootstrapHandoffLease",
    "BootstrapHandoffOwnershipV2",
    "BootstrapHandoffRecordV2",
    "BootstrapHandoffStore",
    "bind_bootstrap_handoff_ownership",
    "claim_bootstrap_handoff_launch",
    "consume_bootstrap_handoff",
    "resolve_bootstrap_handoff_physical_binding",
]
