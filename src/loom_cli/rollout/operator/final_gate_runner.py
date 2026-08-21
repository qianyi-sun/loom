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
from .protected_apply_recovery import find_advanced_epoch_attempt

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
        starting_epoch = attestation.bindings.staging_mutation_epoch
        if (
            type(mutation_epoch) is not int
            or mutation_epoch < 0
            or mutation_epoch not in {starting_epoch, starting_epoch + 1}
        ):
            raise ValueError("final gate mutation epoch drifted before apply")
        journal = FinalGateExecutionStore(
            self.state_root,
            request_id=envelope.request_id,
            attempt_number=envelope.attempt_number,
            service_uid=self.service_uid,
        )
        newest = {
            check_id: execution
            for check_id, execution in journal.read_all().items()
            if execution.passed
        }
        if envelope.resume:
            for attempt_number in range(envelope.attempt_number - 1, 0, -1):
                earlier = FinalGateExecutionStore(
                    self.state_root,
                    request_id=envelope.request_id,
                    attempt_number=attempt_number,
                    service_uid=self.service_uid,
                ).read_all()
                for check_id, execution in earlier.items():
                    if not execution.passed:
                        continue
                    newest.setdefault(check_id, execution)
        protected_apply_recorded = "final.protected-apply" in newest
        protected_apply_recovery = False
        if envelope.resume and not protected_apply_recorded:
            protected_apply_recovery = (
                find_advanced_epoch_attempt(
                    self.state_root,
                    request_id=envelope.request_id,
                    through_attempt=envelope.attempt_number - 1,
                    candidate_sha=envelope.resolved_sha,
                    attestation_digest=envelope.preflight_attestation_sha256,
                    starting_mutation_epoch=starting_epoch,
                    service_uid=self.service_uid,
                )
                is not None
            )
        protected_apply_completed = protected_apply_recorded or protected_apply_recovery
        if mutation_epoch == starting_epoch + 1 and not protected_apply_completed:
            raise ValueError("final gate mutation epoch drifted before apply")
        if admission.post_apply_resume != protected_apply_completed:
            raise ValueError("final gate post-apply admission is incomplete")
        execution_time = self.now()
        authority = AttestedFinalGateAuthority(
            attestation=attestation,
            actions=self.actions_factory(envelope, attestation, starting_epoch, admission),
            candidate_sha=envelope.resolved_sha,
            mutation_epoch=starting_epoch,
            now=execution_time,
            post_apply_resume=admission.post_apply_resume,
            protected_apply_recovery=protected_apply_recovery,
            max_concurrency=self.max_concurrency,
        )
        durable: frozenset[str] = frozenset()
        if envelope.resume or protected_apply_recorded:
            prior = authority.select_resume_evidence(newest, now=execution_time)
            if protected_apply_recorded and "final.protected-apply" not in prior:
                raise ValueError("durable protected apply evidence drifted")
            if "final.protected-apply" in prior:
                durable = frozenset({"final.protected-apply"})
        else:
            prior = newest
        protected_apply_passed = "final.protected-apply" in prior
        if mutation_epoch == starting_epoch + 1:
            if not (protected_apply_passed or protected_apply_recovery):
                raise ValueError("final gate mutation epoch drifted before apply")
        elif protected_apply_passed or protected_apply_recovery:
            raise ValueError("final gate protected apply epoch did not advance")
        for execution in prior.values():
            journal.publish(execution)

        def publish(execution: CheckExecution) -> None:
            journal.publish(execution)

        report = authority.execute(
            now=execution_time,
            prior_executions=prior,
            durable_prior_executions=durable,
            on_execution=publish,
        )
        return 0 if report.passed else 1


__all__ = ["FinalGateActionsFactory", "FinalGateRunner"]
