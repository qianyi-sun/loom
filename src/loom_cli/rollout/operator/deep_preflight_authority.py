"""One production composition boundary for broker and detached preflight."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_orchestrator import CandidatePreflightOrchestrator
from loom_cli.rollout.preflight_pipeline import PreflightAssessment
from loom_cli.rollout.preflight_runtime import CandidatePreflightRuntime
from loom_cli.rollout.preflight_runtime_sources import PreflightRuntimeSources


class RuntimePurpose(StrEnum):
    ADMISSION = "admission"
    DETACHED_REHEARSAL = "detached-rehearsal"


RuntimeSourcesFactory = Callable[
    [CandidateBinding, int, RuntimePurpose],
    PreflightRuntimeSources,
]


@dataclass(frozen=True, slots=True)
class DeepPreflightAuthority:
    """Rebuild one registry from explicit sources in both process boundaries."""

    sources_factory: RuntimeSourcesFactory
    attestation_store: PreflightAttestationStore
    read_mutation_epoch: Callable[[], int]
    now: Callable[[], datetime]
    max_concurrency: int = 8

    def assess(self, candidate: CandidateBinding, mutation_epoch: int) -> PreflightAssessment:
        return self.admission_orchestrator().assess(candidate, mutation_epoch)

    def admission_orchestrator(self) -> CandidatePreflightOrchestrator:
        return self._orchestrator(RuntimePurpose.ADMISSION)

    def detached_orchestrator(self) -> CandidatePreflightOrchestrator:
        return self._orchestrator(RuntimePurpose.DETACHED_REHEARSAL)

    def current_mutation_epoch(self) -> int:
        epoch = self.read_mutation_epoch()
        if type(epoch) is not int or epoch < 0:
            raise ValueError("staging mutation epoch authority is invalid")
        return epoch

    def _orchestrator(self, purpose: RuntimePurpose) -> CandidatePreflightOrchestrator:
        def runtime_factory(
            candidate: CandidateBinding,
            mutation_epoch: int,
        ) -> CandidatePreflightRuntime:
            sources = self.sources_factory(candidate, mutation_epoch, purpose)
            if sources.candidate != candidate:
                raise ValueError("deep preflight source candidate drifted")
            loaded = sources.loaded_artifacts
            if purpose is RuntimePurpose.DETACHED_REHEARSAL:
                if (
                    loaded is None
                    or loaded.publication.candidate_sha != candidate.resolved_sha
                    or loaded.publication.candidate_tree != candidate.resolved_tree
                    or loaded.publication.mutation_epoch != mutation_epoch
                ):
                    raise ValueError("detached preflight lacks exact immutable outputs")
            return sources.build(mutation_epoch=mutation_epoch)

        return CandidatePreflightOrchestrator(
            runtime_factory=runtime_factory,
            store=self.attestation_store,
            now=self.now,
            max_concurrency=self.max_concurrency,
        )


__all__ = [
    "DeepPreflightAuthority",
    "RuntimePurpose",
    "RuntimeSourcesFactory",
]
