"""Compose detached checkpoint work with the restart-safe preflight authority."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from loom_cli.rollout.preflight_orchestrator import CandidatePreflightOrchestrator
from loom_cli.rollout.preflight_pipeline import PreflightAssessment

from .backup import VerifiedBackup
from .backup_job import PreflightBackupJobEnvelope
from .checkpoint_coordinator import (
    CheckpointStore,
    CriticalBackupCreator,
    DetachedCheckpointCoordinator,
)
from .checkpoint_lease import CriticalCheckpointEvidence
from .model import PreflightRequest
from .rehearsal_attestor import RehearsalLeaseAttestor, RehearsalStore

if TYPE_CHECKING:
    from .worker import VerifiedBackupJob


@dataclass(slots=True)
class DetachedPreflightBackupRunner:
    """Worker callback that owns checkpoint, rehearsal, lease and attestation.

    The attestor is rebuilt independently for restore verification and final
    attestation publication. Its only shared state is immutable request-store
    evidence, so a worker restart cannot inherit an unrecorded in-memory plan.
    """

    creator: CriticalBackupCreator
    store: CheckpointStore
    rehearsal_store: RehearsalStore
    load_assessment: Callable[[str], PreflightAssessment]
    orchestrator: CandidatePreflightOrchestrator
    inspect_checkpoint: Callable[
        [VerifiedBackup, PreflightRequest],
        CriticalCheckpointEvidence,
    ]
    now: Callable[[], datetime]
    lease_ttl: timedelta
    referenced_payload_ids: Callable[[], frozenset[str]] = frozenset
    retire_payload: Callable[[str], None] | None = None

    def __post_init__(self) -> None:
        if self.lease_ttl <= timedelta(0):
            raise ValueError("detached preflight lease TTL must be positive")

    def __call__(
        self,
        request: PreflightRequest,
        envelope: PreflightBackupJobEnvelope,
        cancelled: Callable[[], bool],
    ) -> VerifiedBackupJob:
        assessment = self.load_assessment(request.request_id)
        if (
            not assessment.passed
            or assessment.assessment_digest != request.preflight_assessment_sha256
            or assessment.registry_digest != request.preflight_registry_sha256
            or assessment.coverage_digest != request.preflight_coverage_sha256
        ):
            raise ValueError("persisted preflight assessment drifts from request")

        def attestor(checkpoint: CriticalCheckpointEvidence) -> RehearsalLeaseAttestor:
            return self.orchestrator.build_rehearsal_attestor(
                request=request,
                checkpoint=checkpoint,
                assessment=assessment,
                rehearsal_store=self.rehearsal_store,
            )

        coordinator = DetachedCheckpointCoordinator(
            creator=self.creator,
            store=self.store,
            inspect_checkpoint=self.inspect_checkpoint,
            verify_restore=lambda checkpoint, found, stop: attestor(checkpoint).verify_restore(
                checkpoint, found, stop
            ),
            publish_attestation=lambda checkpoint, lease, found: attestor(
                checkpoint
            ).publish_attestation(checkpoint, lease, found),
            now=self.now,
            lease_ttl=self.lease_ttl,
            referenced_payload_ids=self.referenced_payload_ids,
            retire_payload=self.retire_payload,
        )
        return coordinator(request, envelope, cancelled)


__all__ = ["DetachedPreflightBackupRunner"]
