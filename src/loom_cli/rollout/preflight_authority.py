"""Candidate-bound authority that assembles and executes deep preflight."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_bindings import derive_attestation_bindings
from loom_cli.rollout.preflight_contract import AttestationBindings, CheckContext
from loom_cli.rollout.preflight_pipeline import PreflightPipeline, PreflightPipelineResult
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

    def __call__(self, candidate: CandidateBinding) -> PreflightPipelineResult:
        plan = self._planner(candidate)
        if plan.candidate != candidate:
            raise ValueError("preflight planner changed the immutable candidate")
        pipeline = PreflightPipeline(
            registry=plan.registry,
            store=self._store,
            max_concurrency=self._max_concurrency,
            now=self._now,
        )
        return pipeline.authorize(
            context=plan.context,
            bindings=plan.current_bindings,
            binding_factory=lambda executions: derive_attestation_bindings(
                plan.context,
                executions,
            ),
            reusable_attestation_digest=plan.reusable_attestation_digest,
        )


__all__ = [
    "CandidatePreflightAuthorizer",
    "CandidatePreflightPlan",
    "PreflightPlanner",
]
