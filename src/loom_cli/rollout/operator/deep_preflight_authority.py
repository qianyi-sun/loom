"""One production composition boundary for broker and detached preflight."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from loom_cli.rollout.final_attestation_admission import (
    FinalAttestationAdmission,
    validate_final_attestation,
    validate_post_apply_resume_attestation,
)
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_artifact_reference import PreflightArtifactReference
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_orchestrator import CandidatePreflightOrchestrator
from loom_cli.rollout.preflight_pipeline import PreflightAssessment
from loom_cli.rollout.preflight_runtime import CandidatePreflightRuntime
from loom_cli.rollout.preflight_runtime_sources import PreflightRuntimeSources


class RuntimePurpose(StrEnum):
    ADMISSION = "admission"
    DETACHED_REHEARSAL = "detached-rehearsal"


RuntimeSourcesFactory = Callable[
    [CandidateBinding, int, RuntimePurpose, PreflightArtifactReference | None],
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
    post_apply_attested_dependencies: frozenset[str] = frozenset()

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

    def admit_final(
        self,
        candidate: CandidateBinding,
        *,
        attestation_digest: str,
        expected_registry_digest: str,
        expected_coverage_digest: str,
    ) -> FinalAttestationAdmission:
        """Reload and recheck exact Tier 0 authority before protected apply."""
        attestation = self.attestation_store.read(attestation_digest)
        if (
            attestation.registry_digest != expected_registry_digest
            or attestation.coverage_digest != expected_coverage_digest
        ):
            raise ValueError("final admission envelope authority drifted")
        mutation_epoch = self.current_mutation_epoch()
        sources = self.sources_factory(
            candidate,
            mutation_epoch,
            RuntimePurpose.ADMISSION,
            None,
        )
        if sources.candidate != candidate:
            raise ValueError("final admission source candidate drifted")
        runtime = sources.build(mutation_epoch=mutation_epoch)
        plan = runtime.prebackup_plan(candidate)
        return validate_final_attestation(
            attestation=attestation,
            candidate=candidate,
            plan=plan,
            current_mutation_epoch=mutation_epoch,
            now=self.now(),
            max_concurrency=self.max_concurrency,
        )

    def admit_post_apply_resume(
        self,
        candidate: CandidateBinding,
        *,
        prior_admission: FinalAttestationAdmission,
        attestation_digest: str,
        expected_registry_digest: str,
        expected_coverage_digest: str,
    ) -> FinalAttestationAdmission:
        """Recheck current authority before resuming an already applied chain."""
        attestation = self.attestation_store.read(attestation_digest)
        if (
            prior_admission.attestation != attestation
            or attestation.registry_digest != expected_registry_digest
            or attestation.coverage_digest != expected_coverage_digest
        ):
            raise ValueError("post-apply resume envelope authority drifted")
        mutation_epoch = self.current_mutation_epoch()
        sources = self.sources_factory(
            candidate,
            mutation_epoch,
            RuntimePurpose.ADMISSION,
            None,
        )
        if sources.candidate != candidate:
            raise ValueError("post-apply resume source candidate drifted")
        plan = sources.build(mutation_epoch=mutation_epoch).prebackup_plan(candidate)
        return validate_post_apply_resume_attestation(
            prior_admission=prior_admission,
            candidate=candidate,
            plan=plan,
            current_mutation_epoch=mutation_epoch,
            now=self.now(),
            max_concurrency=self.max_concurrency,
            attested_dependencies=self.post_apply_attested_dependencies,
        )

    def _orchestrator(self, purpose: RuntimePurpose) -> CandidatePreflightOrchestrator:
        def runtime_factory(
            candidate: CandidateBinding,
            mutation_epoch: int,
            artifact_reference: PreflightArtifactReference | None,
        ) -> CandidatePreflightRuntime:
            if (purpose is RuntimePurpose.ADMISSION) != (artifact_reference is None):
                raise ValueError("preflight artifact reference does not match runtime purpose")
            sources = self.sources_factory(
                candidate,
                mutation_epoch,
                purpose,
                artifact_reference,
            )
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
