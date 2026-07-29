"""One production composition boundary for broker and detached preflight."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from threading import Lock

from loom_cli.rollout.final_attestation_admission import (
    FinalAttestationAdmission,
    validate_final_attestation,
)
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
AdmissionPreparer = Callable[[CandidateBinding], None]

_DEFAULT_PREPARATION_TTL = timedelta(minutes=5)


@dataclass(slots=True)
class AdmissionPreparationLifecycle:
    """Keep one bounded exact-candidate infrastructure preparation lease.

    The callback is the only mutation boundary in the deep-preflight graph.
    Source construction and assessment merely require a fresh lease; they
    never attempt to manufacture the infrastructure they inspect.
    """

    prepare: AdmissionPreparer
    now: Callable[[], datetime]
    ttl: timedelta = _DEFAULT_PREPARATION_TTL
    _candidate_identity: tuple[str, str] | None = None
    _prepared_at: datetime | None = None
    _lock: Lock | None = None

    def __post_init__(self) -> None:
        if (
            not callable(self.prepare)
            or not callable(self.now)
            or not timedelta(0) < self.ttl <= timedelta(minutes=30)
        ):
            raise ValueError("admission preparation lifecycle is invalid")
        self._lock = Lock()

    @staticmethod
    def _identity(candidate: CandidateBinding) -> tuple[str, str]:
        if candidate.resolved_tree is None:
            raise ValueError("admission preparation candidate tree is unavailable")
        return candidate.resolved_sha, candidate.resolved_tree

    def prepare_admission(self, candidate: CandidateBinding) -> None:
        """Converge once for the exact candidate, refreshing an expired lease."""
        identity = self._identity(candidate)
        lock = self._lock
        if lock is None:  # pragma: no cover - dataclass initialization invariant
            raise ValueError("admission preparation lifecycle is unavailable")
        with lock:
            observed_at = self._timestamp()
            if self._is_fresh(identity, observed_at):
                return
            # Publish freshness only after the producer returns successfully.
            # A failed producer therefore cannot leave a usable lease behind.
            self.prepare(candidate)
            completed_at = self._timestamp()
            self._candidate_identity = identity
            self._prepared_at = completed_at

    def require_fresh(self, candidate: CandidateBinding) -> None:
        """Fail closed unless prepare_admission completed recently."""
        identity = self._identity(candidate)
        if not self._is_fresh(identity, self._timestamp()):
            raise ValueError("admission infrastructure is not freshly prepared")

    def _timestamp(self) -> datetime:
        value = self.now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("admission preparation clock must be timezone-aware")
        return value

    def _is_fresh(self, identity: tuple[str, str], observed_at: datetime) -> bool:
        prepared_at = self._prepared_at
        return bool(
            self._candidate_identity == identity
            and prepared_at is not None
            and timedelta(0) <= observed_at - prepared_at <= self.ttl
        )


@dataclass(frozen=True, slots=True)
class DeepPreflightAuthority:
    """Rebuild one registry from explicit sources in both process boundaries."""

    sources_factory: RuntimeSourcesFactory
    attestation_store: PreflightAttestationStore
    read_mutation_epoch: Callable[[], int]
    now: Callable[[], datetime]
    max_concurrency: int = 8
    admission_preparation: AdmissionPreparationLifecycle | None = None

    def assess(self, candidate: CandidateBinding, mutation_epoch: int) -> PreflightAssessment:
        return self.admission_orchestrator().assess(candidate, mutation_epoch)

    def prepare_admission(self, candidate: CandidateBinding) -> None:
        """Enter the one explicit infrastructure mutation surface."""
        if self.admission_preparation is not None:
            self.admission_preparation.prepare_admission(candidate)

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
        # The worker is a distinct process from the broker. Refresh (or establish)
        # its bounded lease before collecting final read-only evidence.
        self.prepare_admission(candidate)
        self._require_prepared(candidate)
        attestation = self.attestation_store.read(attestation_digest)
        if (
            attestation.registry_digest != expected_registry_digest
            or attestation.coverage_digest != expected_coverage_digest
        ):
            raise ValueError("final admission envelope authority drifted")
        mutation_epoch = self.current_mutation_epoch()
        sources = self.sources_factory(candidate, mutation_epoch, RuntimePurpose.ADMISSION)
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

    def _require_prepared(self, candidate: CandidateBinding) -> None:
        if self.admission_preparation is not None:
            self.admission_preparation.require_fresh(candidate)

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
    "AdmissionPreparationLifecycle",
    "DeepPreflightAuthority",
    "RuntimePurpose",
    "RuntimeSourcesFactory",
]
