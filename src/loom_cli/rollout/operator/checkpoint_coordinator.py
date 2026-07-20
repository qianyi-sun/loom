"""Detached critical-checkpoint, restore, lease, and rotation coordinator."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from .backup import VerifiedBackup
from .backup_job import PreflightBackupJobEnvelope
from .backup_lease import BackupLease
from .backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRetirementRecord,
    BackupRotationResult,
    BackupRotationState,
    acknowledge_retirement,
    begin_candidate,
    fail_candidate,
    promote_candidate,
    record_manifest_verified,
    record_restore_verified,
)
from .checkpoint_lease import (
    CriticalCheckpointEvidence,
    RestoreVerificationEvidence,
    build_restore_verified_lease,
)
from .model import PreflightRequest
from .worker import VerifiedBackupJob


class CriticalBackupCreator(Protocol):
    def create(
        self,
        request: PreflightRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup: ...


class CheckpointStore(Protocol):
    def read_backup_rotation(self) -> BackupRotationState: ...

    def replace_backup_rotation(
        self,
        state: BackupRotationState,
        *,
        expected_generation: int,
    ) -> object: ...

    def publish_backup_lease(self, lease: BackupLease) -> object: ...


class CheckpointCoordinatorError(RuntimeError):
    """Normalized detached-checkpoint orchestration failure."""


@dataclass(slots=True)
class DetachedCheckpointCoordinator:
    """Create and rotate one restore-verified critical checkpoint.

    Long backup and restore I/O happens inside the detached worker and never
    under the broker launch lock. Every durable transition is independently
    compare-and-swap published through the request store.
    """

    creator: CriticalBackupCreator
    store: CheckpointStore
    inspect_checkpoint: Callable[[VerifiedBackup, PreflightRequest], CriticalCheckpointEvidence]
    verify_restore: Callable[
        [CriticalCheckpointEvidence, PreflightRequest, Callable[[], bool]],
        RestoreVerificationEvidence,
    ]
    publish_attestation: Callable[
        [CriticalCheckpointEvidence, BackupLease, PreflightRequest],
        str,
    ]
    now: Callable[[], datetime]
    lease_ttl: timedelta
    referenced_payload_ids: Callable[[], frozenset[str]] = frozenset
    retire_payload: Callable[[BackupRetirementRecord], None] | None = None
    activate_payload: Callable[[BackupPayloadRecord], None] | None = None

    def __post_init__(self) -> None:
        if self.lease_ttl <= timedelta(0):
            raise ValueError("checkpoint lease TTL must be positive")

    def _publish_rotation(
        self,
        current: BackupRotationState,
        result: BackupRotationResult,
    ) -> BackupRotationState:
        self.store.replace_backup_rotation(
            result.state,
            expected_generation=current.generation,
        )
        return result.state

    def _fail_reserved_candidate(self, payload_id: str, failure_code: str) -> None:
        current = self.store.read_backup_rotation()
        candidate = current.candidate
        if candidate is None or candidate.payload_id != payload_id:
            return
        result = fail_candidate(
            current,
            payload_id=payload_id,
            failure_code=failure_code,
            referenced_payload_ids=self.referenced_payload_ids(),
        )
        self._publish_rotation(current, result)
        self._drain_retirements(strict=False)

    def _drain_retirements(self, *, strict: bool) -> None:
        """Idempotently delete and acknowledge exact persisted retirements."""
        state = self.store.read_backup_rotation()
        referenced = self.referenced_payload_ids()
        for retirement in state.retirements:
            if retirement.payload_id in referenced or self.retire_payload is None:
                continue
            try:
                self.retire_payload(retirement)
                current = self.store.read_backup_rotation()
                acknowledged = acknowledge_retirement(
                    current,
                    payload_id=retirement.payload_id,
                )
                self._publish_rotation(current, acknowledged)
            except Exception as exc:
                if strict:
                    raise CheckpointCoordinatorError(
                        "backup payload retirement did not complete"
                    ) from exc
                continue
        remaining = self.store.read_backup_rotation().retirements
        if strict and remaining:
            raise CheckpointCoordinatorError("backup payload retirement is still referenced")

    @staticmethod
    def _validate_binding(
        request: PreflightRequest,
        envelope: PreflightBackupJobEnvelope,
    ) -> None:
        if (
            request.request_id != envelope.request_id
            or request.candidate.resolved_sha != envelope.candidate_sha
            or request.candidate_tree != envelope.candidate_tree
            or request.preflight_assessment_sha256 != envelope.preflight_assessment_sha256
            or request.preflight_registry_sha256 != envelope.preflight_registry_sha256
            or request.preflight_coverage_sha256 != envelope.preflight_coverage_sha256
            or request.mutation_epoch != envelope.mutation_epoch
            or request.environment != envelope.environment
            or request.namespace != envelope.namespace
            or request.status != "pending"
        ):
            raise CheckpointCoordinatorError("backup job binding drifted")

    def __call__(
        self,
        request: PreflightRequest,
        envelope: PreflightBackupJobEnvelope,
        cancelled: Callable[[], bool],
    ) -> VerifiedBackupJob:
        self._validate_binding(request, envelope)
        if cancelled():
            raise CheckpointCoordinatorError("backup cancelled before reservation")
        state = self.store.read_backup_rotation()
        candidate = state.candidate
        if candidate is None:
            self._drain_retirements(strict=False)
            state = self.store.read_backup_rotation()
            reservation = begin_candidate(
                state,
                payload_id=envelope.payload_id,
                request_id=request.request_id,
                bundle_name=envelope.bundle_name,
                created_at=envelope.created_at,
            )
            self._publish_rotation(state, reservation)
        elif (
            candidate.payload_id != envelope.payload_id
            or candidate.request_id != request.request_id
            or candidate.bundle_name != envelope.bundle_name
            or candidate.created_at != envelope.created_at
            or candidate.phase is not BackupPayloadPhase.CREATING
        ):
            raise CheckpointCoordinatorError("backup candidate reservation drifted")
        try:
            if cancelled():
                raise CheckpointCoordinatorError("backup cancelled before checkpoint")
            backup = self.creator.create(request, created_at=envelope.created_at)
            if backup.manifest_path.parent.name != envelope.bundle_name:
                raise CheckpointCoordinatorError("backup bundle identity drifted")
            checkpoint = self.inspect_checkpoint(backup, request)
            if (
                checkpoint.request_id != request.request_id
                or checkpoint.environment != request.environment
                or checkpoint.namespace != request.namespace
                or checkpoint.mutation_epoch != request.mutation_epoch
            ):
                raise CheckpointCoordinatorError("checkpoint identity drifted")
            current = self.store.read_backup_rotation()
            manifested = record_manifest_verified(
                current,
                payload_id=envelope.payload_id,
                manifest_sha256=checkpoint.manifest_sha256,
            )
            self._publish_rotation(current, manifested)
            if cancelled():
                raise CheckpointCoordinatorError("backup cancelled before restore rehearsal")
            restore = self.verify_restore(checkpoint, request, cancelled)
            if cancelled():
                raise CheckpointCoordinatorError("backup cancelled after restore rehearsal")
            lease = build_restore_verified_lease(
                checkpoint,
                restore,
                expires_at=self.now() + self.lease_ttl,
            )
            self.store.publish_backup_lease(lease)
            attestation_digest = self.publish_attestation(checkpoint, lease, request)
            if len(attestation_digest) != 64 or any(
                character not in "0123456789abcdef" for character in attestation_digest
            ):
                raise CheckpointCoordinatorError("preflight attestation digest is invalid")
            current = self.store.read_backup_rotation()
            restored = record_restore_verified(
                current,
                payload_id=envelope.payload_id,
                lease=lease,
            )
            self._publish_rotation(current, restored)
            current = self.store.read_backup_rotation()
            promoted = promote_candidate(
                current,
                payload_id=envelope.payload_id,
                referenced_payload_ids=self.referenced_payload_ids(),
            )
            self._publish_rotation(current, promoted)
            active = self.store.read_backup_rotation().active
            if active is None:
                raise CheckpointCoordinatorError("promoted backup payload is unavailable")
            if self.activate_payload is not None:
                self.activate_payload(active)
            self._drain_retirements(strict=True)
            return VerifiedBackupJob(
                manifest_path=checkpoint.manifest_path,
                manifest_sha256=checkpoint.manifest_sha256,
                lease_digest=lease.evidence_digest,
                preflight_attestation_sha256=attestation_digest,
            )
        except BaseException:
            self._fail_reserved_candidate(envelope.payload_id, "checkpoint_or_restore_failed")
            raise


__all__ = [
    "CheckpointCoordinatorError",
    "CriticalBackupCreator",
    "DetachedCheckpointCoordinator",
]
