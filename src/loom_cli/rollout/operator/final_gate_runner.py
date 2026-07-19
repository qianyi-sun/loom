"""Service-owned execution of one attested protected final-gate chain."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from loom_cli.rollout.attested_final_gate import AttestedFinalGateAuthority
from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmission
from loom_cli.rollout.final_gate_readiness import FinalGateAction
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import CheckExecution, PreflightAttestation

from .final_gate_store import FinalGateExecutionStore
from .model import DriverEnvelope

FinalGateActionsFactory = Callable[
    [DriverEnvelope, PreflightAttestation, int, FinalAttestationAdmission],
    Mapping[str, FinalGateAction],
]


@dataclass(frozen=True, slots=True)
class FinalGateRunner:
    """Resume only immutable successful checks and never repeat protected apply."""

    attestation_store: PreflightAttestationStore
    actions_factory: FinalGateActionsFactory
    read_mutation_epoch: Callable[[], int]
    now: Callable[[], datetime]
    state_root: Path
    service_uid: int
    max_concurrency: int = 4

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
            or not 1 <= self.max_concurrency <= 16
        ):
            raise ValueError("final gate runner authority is invalid")

    def __call__(
        self,
        envelope: DriverEnvelope,
        admission: FinalAttestationAdmission,
    ) -> int:
        attestation = self.attestation_store.read(envelope.preflight_attestation_sha256)
        if (
            admission.attestation != attestation
            or attestation.attestation_digest != envelope.preflight_attestation_sha256
            or attestation.registry_digest != envelope.preflight_registry_sha256
            or attestation.coverage_digest != envelope.preflight_coverage_sha256
            or attestation.bindings.candidate_sha != envelope.resolved_sha
            or attestation.bindings.candidate_tree != envelope.resolved_tree
            or attestation.bindings.environment != envelope.environment
            or attestation.bindings.namespace != envelope.namespace
        ):
            raise ValueError("final gate envelope drifts from attestation")
        mutation_epoch = self.read_mutation_epoch()
        if (
            type(mutation_epoch) is not int
            or mutation_epoch < 0
            or mutation_epoch != attestation.bindings.staging_mutation_epoch
        ):
            raise ValueError("final gate mutation epoch drifted before apply")
        journal = FinalGateExecutionStore(
            self.state_root,
            request_id=envelope.request_id,
            attempt_number=envelope.attempt_number,
            service_uid=self.service_uid,
        )
        prior = journal.read_all()
        authority = AttestedFinalGateAuthority(
            attestation=attestation,
            actions=self.actions_factory(envelope, attestation, mutation_epoch, admission),
            candidate_sha=envelope.resolved_sha,
            mutation_epoch=mutation_epoch,
            now=self.now(),
            max_concurrency=self.max_concurrency,
        )

        def publish(execution: CheckExecution) -> None:
            journal.publish(execution)

        report = authority.execute(
            now=self.now(),
            prior_executions=prior,
            on_execution=publish,
        )
        return 0 if report.passed else 1


__all__ = ["FinalGateActionsFactory", "FinalGateRunner"]
