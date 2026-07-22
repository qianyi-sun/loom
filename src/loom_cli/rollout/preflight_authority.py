"""Candidate-bound authority that assembles and executes deep preflight."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from loom_cli.rollout.operator.checkpoint_lease import CriticalCheckpointEvidence
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.operator.rehearsal_attestor import (
    RehearsalLeaseAttestor,
    RehearsalStore,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import AttestationBindings, CheckContext
from loom_cli.rollout.preflight_pipeline import (
    PreflightAssessment,
    PreflightPipeline,
)
from loom_cli.rollout.preflight_registry import PreflightRegistry


@dataclass(frozen=True, slots=True)
class CandidatePreflightPlan:
    candidate: CandidateBinding
    registry: PreflightRegistry
    context: CheckContext
    current_bindings: AttestationBindings | None = None
    reusable_attestation_digest: str | None = None

    def __post_init__(self) -> None:
        if self.registry.through_tier != 3:
            raise ValueError("candidate preflight plan must implement tiers 0 through 3")
        expected_base = self.candidate.approved_base_sha or "none"
        expected = {
            "candidate.sha": self.candidate.resolved_sha,
            "candidate.source-mode": self.candidate.source_mode,
            "candidate.base.sha": expected_base,
        }
        if any(self.context.bindings.get(name) != value for name, value in expected.items()):
            raise ValueError("candidate preflight plan context drifts from bound candidate")
        if self.current_bindings is not None and (
            self.current_bindings.candidate_sha != self.candidate.resolved_sha
            or (
                self.candidate.resolved_tree is not None
                and self.current_bindings.candidate_tree != self.candidate.resolved_tree
            )
        ):
            raise ValueError("reusable preflight bindings drift from bound candidate")
        if self.reusable_attestation_digest is not None and self.current_bindings is None:
            raise ValueError("reusable attestation requires current drift bindings")


PreflightPlanner = Callable[[CandidateBinding], CandidatePreflightPlan]
CheckpointPreflightPlanner = Callable[
    [CandidateBinding, CriticalCheckpointEvidence],
    CandidatePreflightPlan,
]


class CandidatePreflightAuthorizer:
    """Split pre-backup assessment from checkpoint-bound rehearsal authority."""

    def __init__(
        self,
        *,
        planner: PreflightPlanner,
        checkpoint_planner: CheckpointPreflightPlanner,
        store: PreflightAttestationStore,
        now: Callable[[], datetime],
        max_concurrency: int = 8,
    ) -> None:
        self._planner = planner
        self._checkpoint_planner = checkpoint_planner
        self._store = store
        self._now = now
        self._max_concurrency = max_concurrency

    def assess(self, candidate: CandidateBinding) -> PreflightAssessment:
        """Run the exact Tier 0-2 plan without retaining process-local state."""
        plan = self._planner(candidate)
        if plan.candidate != candidate:
            raise ValueError("preflight planner changed the immutable candidate")
        pipeline = self._pipeline(plan)
        return pipeline.assess(context=plan.context)

    def build_rehearsal_attestor(
        self,
        *,
        candidate: CandidateBinding,
        checkpoint: CriticalCheckpointEvidence,
        assessment: PreflightAssessment,
        rehearsal_store: RehearsalStore,
    ) -> RehearsalLeaseAttestor:
        """Rebuild the exact plan after checkpoint publication, without pending RAM."""
        if not assessment.passed:
            raise ValueError("checkpoint rehearsal requires a passing assessment")
        plan = self._checkpoint_planner(candidate, checkpoint)
        if plan.candidate != candidate:
            raise ValueError("checkpoint planner changed the immutable candidate")
        if (
            plan.registry.registry_digest != assessment.registry_digest
            or plan.registry.coverage_digest != assessment.coverage_digest
            or plan.context.bindings.get("checkpoint.evidence.sha256") != checkpoint.evidence_digest
            or plan.context.bindings.get("staging.mutation-epoch") != checkpoint.mutation_epoch
        ):
            raise ValueError("checkpoint plan drifts from pre-backup authority")
        return RehearsalLeaseAttestor(
            pipeline=self._pipeline(plan),
            context=plan.context,
            assessment=assessment,
            store=rehearsal_store,
            now=self._now,
        )

    def _pipeline(self, plan: CandidatePreflightPlan) -> PreflightPipeline:
        return PreflightPipeline(
            registry=plan.registry,
            store=self._store,
            max_concurrency=self._max_concurrency,
            now=self._now,
        )


__all__ = [
    "CandidatePreflightAuthorizer",
    "CandidatePreflightPlan",
    "CheckpointPreflightPlanner",
    "PreflightPlanner",
]
