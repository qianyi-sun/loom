"""Restart-safe orchestration for one exact candidate preflight runtime."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from loom_cli.rollout.operator.checkpoint_lease import CriticalCheckpointEvidence
from loom_cli.rollout.operator.model import CandidateBinding, PreflightRequest
from loom_cli.rollout.operator.rehearsal_attestor import (
    RehearsalLeaseAttestor,
    RehearsalStore,
)
from loom_cli.rollout.preflight_artifact_reference import PreflightArtifactReference
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_authority import CandidatePreflightAuthorizer
from loom_cli.rollout.preflight_pipeline import PreflightAssessment
from loom_cli.rollout.preflight_runtime import CandidatePreflightRuntime

RuntimeFactory = Callable[
    [CandidateBinding, int, PreflightArtifactReference | None],
    CandidatePreflightRuntime,
]


@dataclass(frozen=True, slots=True)
class CandidatePreflightOrchestrator:
    """Rebuild the same registered-check authority in broker and worker.

    The broker process uses :meth:`assess` before it publishes a request. A
    detached worker later calls :meth:`build_rehearsal_attestor` using only the
    immutable request, checkpoint and persisted assessment. No process-local
    pending plan is therefore part of rollout authority.
    """

    runtime_factory: RuntimeFactory
    store: PreflightAttestationStore
    now: Callable[[], datetime]
    max_concurrency: int = 8

    def __post_init__(self) -> None:
        if self.max_concurrency <= 0:
            raise ValueError("preflight max concurrency must be positive")

    def assess(self, candidate: CandidateBinding, mutation_epoch: int) -> PreflightAssessment:
        """Execute Tier 0-2 for one exact candidate and mutation epoch."""
        runtime = self._runtime(candidate, mutation_epoch, None)
        return self._authorizer(runtime).assess(candidate)

    def build_rehearsal_attestor(
        self,
        *,
        request: PreflightRequest,
        checkpoint: CriticalCheckpointEvidence,
        assessment: PreflightAssessment,
        rehearsal_store: RehearsalStore,
    ) -> RehearsalLeaseAttestor:
        """Rebuild Tier 3 after an immutable critical checkpoint exists."""
        if (
            request.status != "pending"
            or request.candidate_tree != request.candidate.resolved_tree
            or checkpoint.request_id != request.request_id
            or checkpoint.mutation_epoch != request.mutation_epoch
            or checkpoint.environment != request.environment
            or checkpoint.namespace != request.namespace
        ):
            raise ValueError("checkpoint rehearsal request identity drifted")
        runtime = self._runtime(
            request.candidate,
            request.mutation_epoch,
            PreflightArtifactReference.from_assessment(assessment),
        )
        return self._authorizer(runtime).build_rehearsal_attestor(
            candidate=request.candidate,
            checkpoint=checkpoint,
            assessment=assessment,
            rehearsal_store=rehearsal_store,
        )

    def _runtime(
        self,
        candidate: CandidateBinding,
        mutation_epoch: int,
        artifact_reference: PreflightArtifactReference | None,
    ) -> CandidatePreflightRuntime:
        if type(mutation_epoch) is not int or mutation_epoch < 0:
            raise ValueError("preflight mutation epoch is invalid")
        runtime = self.runtime_factory(candidate, mutation_epoch, artifact_reference)
        if (
            runtime.candidate != candidate
            or runtime.bindings.get("staging.mutation-epoch") != mutation_epoch
        ):
            raise ValueError("preflight runtime factory changed exact authority")
        return runtime

    def _authorizer(
        self,
        runtime: CandidatePreflightRuntime,
    ) -> CandidatePreflightAuthorizer:
        return CandidatePreflightAuthorizer(
            planner=runtime.prebackup_plan,
            checkpoint_planner=runtime.checkpoint_plan,
            store=self.store,
            now=self.now,
            max_concurrency=self.max_concurrency,
        )


__all__ = ["CandidatePreflightOrchestrator", "RuntimeFactory"]
