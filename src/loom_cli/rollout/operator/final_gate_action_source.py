"""Resolve exact preflight outputs into seven fixed installed final actions."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.final_attestation_admission import (
    FinalAttestationAdmission,
    validate_post_apply_attestation_drift,
)
from loom_cli.rollout.final_gate_command_runner import (
    FINAL_GATE_HELPER_PATH,
    CommandRunner,
    InstalledFinalGateStepRunner,
)
from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    FinalGateAction,
    FinalGateResult,
)
from loom_cli.rollout.preflight_artifact_store import (
    PreflightArtifactPublication,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
from loom_cli.rollout.preflight_contract import CheckOperation, PreflightAttestation
from loom_cli.rollout.preflight_pipeline import PreflightRehearsal

from .backup_lease import BackupLease
from .final_gate_plan import FinalGatePlan, FinalGatePlanStore
from .final_gate_store import FinalGateExecutionStore
from .model import CandidateBinding, DriverEnvelope
from .protected_apply_baseline import ProtectedApplyBaseline
from .protected_apply_recovery import find_advanced_epoch_attempt


class FinalGateRehearsalStore(Protocol):
    def read_preflight_rehearsal(self, request_id: str) -> PreflightRehearsal: ...

    def read_backup_lease(self, digest: str) -> BackupLease: ...


@dataclass(frozen=True, slots=True)
class FinalGateActionSource:
    """Build the complete Tier 4 action map from immutable request evidence."""

    request_store: FinalGateRehearsalStore
    artifact_store: PreflightArtifactStore
    state_root: Path
    service_uid: int
    run: CommandRunner
    read_mutation_epoch: Callable[[], int]
    now: Callable[[], datetime]
    post_apply_plan_factory: Callable[[CandidateBinding, int], CandidatePreflightPlan]
    executable: Path = FINAL_GATE_HELPER_PATH
    executable_owner_uid: int = 0
    post_apply_drift_attempts: int = 13
    post_apply_drift_retry_interval_seconds: float = 5.0
    sleep: Callable[[float], None] = time.sleep

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
            or self.executable_owner_uid < 0
            or not callable(self.read_mutation_epoch)
            or not callable(self.now)
            or not callable(self.post_apply_plan_factory)
            or not 1 <= self.post_apply_drift_attempts <= 61
            or not 0 < self.post_apply_drift_retry_interval_seconds <= 60
            or not callable(self.sleep)
        ):
            raise ValueError("final gate action source authority is invalid")

    def __call__(
        self,
        envelope: DriverEnvelope,
        attestation: PreflightAttestation,
        mutation_epoch: int,
        admission: FinalAttestationAdmission,
    ) -> Mapping[str, FinalGateAction]:
        if (
            mutation_epoch != attestation.bindings.staging_mutation_epoch
            or admission.attestation != attestation
        ):
            raise ValueError("final gate action source mutation epoch drifted")
        rehearsal = self.request_store.read_preflight_rehearsal(envelope.request_id)
        self._validate_rehearsal(rehearsal, attestation)
        publication = self._publication(rehearsal, attestation)
        systemd_evidence = self._execution_evidence(
            rehearsal,
            attestation,
            check_id="systemd.render",
        )
        predecessor_evidence = self._execution_evidence(
            rehearsal,
            attestation,
            check_id="external-supervisor.predecessor",
        )
        lease = self.request_store.read_backup_lease(attestation.bindings.backup_lease_digest)
        baseline = ProtectedApplyBaseline.from_executions(
            attestation,
            admission.tier2_executions,
        )
        plan = FinalGatePlan.build(
            envelope,
            attestation,
            publication,
            lease,
            baseline,
            systemd_evidence,
            predecessor_evidence,
        )
        protected_apply_plan = (
            self._validate_resume_plan(envelope, plan) if envelope.resume else None
        )
        plan_path = FinalGatePlanStore(
            self.state_root,
            request_id=envelope.request_id,
            attempt_number=envelope.attempt_number,
            service_uid=self.service_uid,
        ).publish(plan)
        runner = InstalledFinalGateStepRunner(
            service_uid=self.service_uid,
            plan_path=plan_path,
            plan_digest=plan.plan_digest,
            run=self.run,
            executable=self.executable,
            executable_owner_uid=self.executable_owner_uid,
        )
        convergence_runner = runner
        if protected_apply_plan is not None:
            protected_apply_store = FinalGatePlanStore(
                self.state_root,
                request_id=envelope.request_id,
                attempt_number=protected_apply_plan.attempt_number,
                service_uid=self.service_uid,
            )
            convergence_runner = InstalledFinalGateStepRunner(
                service_uid=self.service_uid,
                plan_path=protected_apply_store.path,
                plan_digest=protected_apply_plan.plan_digest,
                run=self.run,
                executable=self.executable,
                executable_owner_uid=self.executable_owner_uid,
            )

        def action(check_id: str) -> FinalGateAction:
            selected_runner = (
                convergence_runner
                if check_id in {"final.protected-apply", "final.convergence"}
                else runner
            )

            def execute(operation: CheckOperation) -> FinalGateResult:
                return selected_runner(
                    check_id,
                    operation,
                    candidate_sha=envelope.resolved_sha,
                    attestation_digest=attestation.attestation_digest,
                    mutation_epoch=mutation_epoch,
                )

            return execute

        actions = {check_id: action(check_id) for check_id in FINAL_CHECK_IDS}

        def verify_post_apply_drift(operation: CheckOperation) -> FinalGateResult:
            if operation is not CheckOperation.VERIFY or admission.preflight_plan is None:
                raise ValueError("post-apply drift action is unavailable")
            for attempt in range(1, self.post_apply_drift_attempts + 1):
                current_mutation_epoch = self.read_mutation_epoch()
                if type(current_mutation_epoch) is not int or current_mutation_epoch < 0:
                    raise ValueError("post-apply mutation epoch authority is invalid")
                try:
                    post_apply_plan = self.post_apply_plan_factory(
                        admission.preflight_plan.candidate,
                        current_mutation_epoch,
                    )
                    evidence = validate_post_apply_attestation_drift(
                        admission=admission,
                        plan=post_apply_plan,
                        current_mutation_epoch=current_mutation_epoch,
                        now=self.now(),
                    )
                # This validation is entirely read-only and blocks every later
                # final gate.  Cross-host read-after-write windows can surface
                # through more than one validator branch, so re-observe any
                # bounded validation mismatch and still fail closed at expiry.
                except ValueError:
                    if attempt >= self.post_apply_drift_attempts:
                        raise
                    self.sleep(self.post_apply_drift_retry_interval_seconds)
                    continue
                break
            return FinalGateResult(
                check_id="final.drift",
                operation=operation,
                candidate_sha=envelope.resolved_sha,
                attestation_digest=attestation.attestation_digest,
                observed_epoch=evidence.observed_mutation_epoch,
                evidence_digest=evidence.evidence_digest,
                protected_mutation=False,
                blockers={},
            )

        actions["final.drift"] = verify_post_apply_drift
        return actions

    def _validate_resume_plan(
        self,
        envelope: DriverEnvelope,
        plan: FinalGatePlan,
    ) -> FinalGatePlan | None:
        predecessor: FinalGatePlan | None = None
        for attempt_number in range(1, envelope.attempt_number):
            executions = FinalGateExecutionStore(
                self.state_root,
                request_id=envelope.request_id,
                attempt_number=attempt_number,
                service_uid=self.service_uid,
            ).read_all()
            protected_apply = executions.get("final.protected-apply")
            if protected_apply is None or not protected_apply.passed:
                continue
            predecessor = FinalGatePlanStore(
                self.state_root,
                request_id=envelope.request_id,
                attempt_number=attempt_number,
                service_uid=self.service_uid,
            ).read()
            break
        if predecessor is None:
            recovery_attempt = find_advanced_epoch_attempt(
                self.state_root,
                request_id=envelope.request_id,
                through_attempt=envelope.attempt_number - 1,
                candidate_sha=plan.candidate_sha,
                attestation_digest=plan.attestation_digest,
                starting_mutation_epoch=plan.starting_mutation_epoch,
                service_uid=self.service_uid,
            )
            if recovery_attempt is not None:
                predecessor = FinalGatePlanStore(
                    self.state_root,
                    request_id=envelope.request_id,
                    attempt_number=recovery_attempt,
                    service_uid=self.service_uid,
                ).read()
        if predecessor is None:
            return None

        def stable_payload(value: FinalGatePlan) -> dict[str, object]:
            payload = value.to_dict()
            for field in ("attempt_number", "request_envelope_sha256", "plan_digest"):
                payload.pop(field)
            return payload

        if stable_payload(predecessor) != stable_payload(plan):
            raise ValueError("final gate resume plan binding drifted")
        return predecessor

    def _publication(
        self,
        rehearsal: PreflightRehearsal,
        attestation: PreflightAttestation,
    ) -> PreflightArtifactPublication:
        execution_evidence = self._execution_evidence(
            rehearsal,
            attestation,
            check_id="artifacts.publish",
        )
        digest = execution_evidence.get("bundle-digest")
        if not isinstance(digest, str):
            raise ValueError("final gate artifact bundle identity is missing")
        publication = self.artifact_store.read(digest)
        expected_evidence = {
            "bundle-digest": publication.bundle_digest,
            "image-artifact-digest": publication.image_artifact_sha256,
            "manifest-artifact-digest": publication.manifest_artifact_sha256,
            "rendered-manifest-digest": publication.rendered_manifest_sha256,
            "migration-manifest-digest": publication.migration_manifest_sha256,
            "migration-artifact-digest": publication.migration_manifest_artifact_sha256,
            "production-defaults-digest": publication.production_defaults_sha256,
        }
        if dict(execution_evidence) != expected_evidence:
            raise ValueError("final gate artifact publication drifted from evidence")
        return publication

    @staticmethod
    def _validate_rehearsal(
        rehearsal: PreflightRehearsal,
        attestation: PreflightAttestation,
    ) -> None:
        if (
            not rehearsal.passed
            or rehearsal.registry_digest != attestation.registry_digest
            or rehearsal.coverage_digest != attestation.coverage_digest
        ):
            raise ValueError("final gate rehearsal authority drifted")

    @staticmethod
    def _execution_evidence(
        rehearsal: PreflightRehearsal,
        attestation: PreflightAttestation,
        *,
        check_id: str,
    ) -> Mapping[str, object]:
        matches = tuple(
            execution for execution in rehearsal.executions if execution.check_id == check_id
        )
        if len(matches) != 1:
            raise ValueError(f"final gate {check_id} evidence is missing or ambiguous")
        execution = matches[0]
        if (
            not execution.passed
            or attestation.check_implementation_digests.get(execution.check_id)
            != execution.implementation_digest
            or attestation.evidence_hashes.get(execution.check_id) != execution.evidence_hash
        ):
            raise ValueError(f"final gate {check_id} evidence drifted from attestation")
        return dict(execution.evidence)


__all__ = ["FinalGateActionSource", "FinalGateRehearsalStore"]
