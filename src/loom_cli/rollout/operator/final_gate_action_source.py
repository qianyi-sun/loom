"""Resolve exact preflight outputs into six fixed installed final actions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

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
from loom_cli.rollout.preflight_contract import CheckOperation, PreflightAttestation
from loom_cli.rollout.preflight_pipeline import PreflightRehearsal

from .backup_lease import BackupLease
from .final_gate_plan import FinalGatePlan, FinalGatePlanStore
from .model import DriverEnvelope
from .protected_apply_baseline import ProtectedApplyBaseline


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
    executable: Path = FINAL_GATE_HELPER_PATH
    executable_owner_uid: int = 0

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
            or self.executable_owner_uid < 0
        ):
            raise ValueError("final gate action source authority is invalid")

    def __call__(
        self,
        envelope: DriverEnvelope,
        attestation: PreflightAttestation,
        mutation_epoch: int,
    ) -> Mapping[str, FinalGateAction]:
        if mutation_epoch != attestation.bindings.staging_mutation_epoch:
            raise ValueError("final gate action source mutation epoch drifted")
        publication = self._publication(envelope, attestation)
        lease = self.request_store.read_backup_lease(attestation.bindings.backup_lease_digest)
        rehearsal = self.request_store.read_preflight_rehearsal(envelope.request_id)
        baseline = ProtectedApplyBaseline.from_executions(attestation, rehearsal.executions)
        plan = FinalGatePlan.build(envelope, attestation, publication, lease, baseline)
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

        def action(check_id: str) -> FinalGateAction:
            def execute(operation: CheckOperation) -> FinalGateResult:
                return runner(
                    check_id,
                    operation,
                    candidate_sha=envelope.resolved_sha,
                    attestation_digest=attestation.attestation_digest,
                    mutation_epoch=mutation_epoch,
                )

            return execute

        return {check_id: action(check_id) for check_id in FINAL_CHECK_IDS}

    def _publication(
        self,
        envelope: DriverEnvelope,
        attestation: PreflightAttestation,
    ) -> PreflightArtifactPublication:
        rehearsal = self.request_store.read_preflight_rehearsal(envelope.request_id)
        if (
            not rehearsal.passed
            or rehearsal.registry_digest != attestation.registry_digest
            or rehearsal.coverage_digest != attestation.coverage_digest
        ):
            raise ValueError("final gate rehearsal authority drifted")
        matches = tuple(
            execution
            for execution in rehearsal.executions
            if execution.check_id == "artifacts.publish"
        )
        if len(matches) != 1:
            raise ValueError("final gate artifact evidence is missing or ambiguous")
        execution = matches[0]
        if (
            not execution.passed
            or attestation.check_implementation_digests.get(execution.check_id)
            != execution.implementation_digest
            or attestation.evidence_hashes.get(execution.check_id) != execution.evidence_hash
        ):
            raise ValueError("final gate artifact evidence drifted from attestation")
        digest = execution.evidence.get("bundle-digest")
        if not isinstance(digest, str):
            raise ValueError("final gate artifact bundle identity is missing")
        publication = self.artifact_store.read(digest)
        expected_evidence = {
            "bundle-digest": publication.bundle_digest,
            "image-artifact-digest": publication.image_artifact_sha256,
            "manifest-artifact-digest": publication.manifest_artifact_sha256,
            "rendered-manifest-digest": publication.rendered_manifest_sha256,
        }
        if dict(execution.evidence) != expected_evidence:
            raise ValueError("final gate artifact publication drifted from evidence")
        return publication


__all__ = ["FinalGateActionSource", "FinalGateRehearsalStore"]
