"""Persist Tier 3 rehearsal, derive restore proof, and issue its attestation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from loom_cli.rollout.preflight_bindings import derive_attestation_bindings
from loom_cli.rollout.preflight_contract import CheckContext, PreflightAttestation
from loom_cli.rollout.preflight_pipeline import (
    PreflightAssessment,
    PreflightPipeline,
    PreflightRehearsal,
)
from loom_cli.rollout.rehearsal_restore_evidence import build_restore_verification_evidence

from .backup_lease import BackupLease
from .checkpoint_lease import CriticalCheckpointEvidence, RestoreVerificationEvidence
from .model import PreflightRequest


class RehearsalStore(Protocol):
    def publish_preflight_rehearsal(
        self,
        request_id: str,
        rehearsal: PreflightRehearsal,
    ) -> object: ...

    def read_preflight_rehearsal(self, request_id: str) -> PreflightRehearsal: ...


@dataclass(slots=True)
class RehearsalLeaseAttestor:
    """The two callbacks consumed by :class:`DetachedCheckpointCoordinator`."""

    pipeline: PreflightPipeline
    context: CheckContext
    assessment: PreflightAssessment
    store: RehearsalStore
    now: Callable[[], datetime]

    def _validate_request(
        self,
        checkpoint: CriticalCheckpointEvidence,
        request: PreflightRequest,
    ) -> None:
        if (
            checkpoint.request_id != request.request_id
            or checkpoint.environment != request.environment
            or checkpoint.namespace != request.namespace
            or checkpoint.mutation_epoch != request.mutation_epoch
            or self.context.bindings.get("candidate.sha") != request.candidate.resolved_sha
            or self.context.bindings.get("checkpoint.evidence.sha256") != checkpoint.evidence_digest
        ):
            raise ValueError("rehearsal request binding drifted")

    def verify_restore(
        self,
        checkpoint: CriticalCheckpointEvidence,
        request: PreflightRequest,
        cancelled: Callable[[], bool],
    ) -> RestoreVerificationEvidence:
        self._validate_request(checkpoint, request)
        if cancelled():
            raise ValueError("rehearsal cancelled before execution")
        rehearsal = self.pipeline.rehearse(
            context=self.context,
            assessment=self.assessment,
        )
        if not rehearsal.passed:
            codes = ",".join(sorted(blocker.failure_code for blocker in rehearsal.blockers))
            raise ValueError(f"isolated rehearsal blocked: {codes}")
        if cancelled():
            raise ValueError("rehearsal cancelled before publication")
        self.store.publish_preflight_rehearsal(request.request_id, rehearsal)
        return build_restore_verification_evidence(
            checkpoint,
            rehearsal,
            context=self.context,
            verified_at=self.now(),
        )

    def publish_attestation(
        self,
        checkpoint: CriticalCheckpointEvidence,
        lease: BackupLease,
        request: PreflightRequest,
    ) -> str:
        self._validate_request(checkpoint, request)
        if (
            lease.source_request_id != request.request_id
            or lease.manifest_sha256 != checkpoint.manifest_sha256
            or lease.mutation_epoch != checkpoint.mutation_epoch
            or lease.object_inventory_root != checkpoint.object_inventory_root
        ):
            raise ValueError("attestation lease binding drifted")
        rehearsal = self.store.read_preflight_rehearsal(request.request_id)
        bindings = derive_attestation_bindings(
            self.context,
            rehearsal.executions,
            backup_lease=lease,
        )
        attestation: PreflightAttestation = self.pipeline.attest(
            rehearsal=rehearsal,
            bindings=bindings,
        )
        return attestation.attestation_digest


__all__ = ["RehearsalLeaseAttestor", "RehearsalStore"]
