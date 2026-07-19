"""Candidate-bound authority that assembles and executes deep preflight."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from threading import Lock

from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_bindings import derive_attestation_bindings
from loom_cli.rollout.preflight_contract import AttestationBindings, CheckContext
from loom_cli.rollout.preflight_pipeline import (
    PreflightAssessment,
    PreflightPipeline,
    PreflightPipelineResult,
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


class CandidatePreflightAuthorizer:
    """Create an attestation before the broker may create a rollout request."""

    def __init__(
        self,
        *,
        planner: PreflightPlanner,
        store: PreflightAttestationStore,
        now: Callable[[], datetime],
        max_concurrency: int = 8,
    ) -> None:
        self._planner = planner
        self._store = store
        self._now = now
        self._max_concurrency = max_concurrency
        self._pending: tuple[CandidatePreflightPlan, PreflightAssessment] | None = None
        self._lock = Lock()

    def assess(self, candidate: CandidateBinding) -> PreflightAssessment:
        """Run and retain the exact pre-backup plan for one candidate."""
        plan = self._planner(candidate)
        if plan.candidate != candidate:
            raise ValueError("preflight planner changed the immutable candidate")
        pipeline = self._pipeline(plan)
        assessment = pipeline.assess(context=plan.context)
        with self._lock:
            if self._pending is not None:
                raise ValueError("another pre-backup assessment is already pending")
            self._pending = (plan, assessment)
        return assessment

    def __call__(self, candidate: CandidateBinding) -> PreflightPipelineResult:
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending is None:
            raise ValueError("pre-backup assessment is unavailable")
        plan, assessment = pending
        if plan.candidate != candidate:
            raise ValueError("pre-backup assessment candidate drifted")
        pipeline = self._pipeline(plan)
        return pipeline.authorize(
            context=plan.context,
            bindings=plan.current_bindings,
            binding_factory=lambda executions: derive_attestation_bindings(
                plan.context,
                executions,
            ),
            reusable_attestation_digest=plan.reusable_attestation_digest,
            assessment=assessment,
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
    "PreflightPlanner",
]
