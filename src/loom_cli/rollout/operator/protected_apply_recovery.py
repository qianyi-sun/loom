"""Recover the narrow crash window after a protected epoch advance."""

from __future__ import annotations

from pathlib import Path

from .final_gate_plan import FinalGatePlanStore
from .model import validate_safe_identifier
from .protected_apply_journal import ProtectedApplyJournal
from .protected_capacity_manager_configuration_compensation import (
    CapacityManagerConfigurationCompensationStore,
)
from .protected_execution_preparation_journal import (
    ExecutionPreparationOperationJournal,
    ExecutionPreparationRecoveryState,
)


def find_advanced_epoch_attempt(
    state_root: Path,
    *,
    request_id: str,
    through_attempt: int,
    candidate_sha: str,
    attestation_digest: str,
    starting_mutation_epoch: int,
    service_uid: int,
) -> int | None:
    """Find the newest exact plan whose component journal advanced one epoch."""
    validate_safe_identifier(request_id, "request_id")
    if (
        not state_root.is_absolute()
        or ".." in state_root.parts
        or type(through_attempt) is not int
        or through_attempt < 0
        or not candidate_sha
        or len(attestation_digest) != 64
        or type(starting_mutation_epoch) is not int
        or starting_mutation_epoch < 0
        or service_uid < 0
    ):
        raise ValueError("protected apply recovery authority is invalid")
    for attempt_number in range(through_attempt, 0, -1):
        store = FinalGatePlanStore(
            state_root,
            request_id=request_id,
            attempt_number=attempt_number,
            service_uid=service_uid,
        )
        try:
            plan = store.read()
        except FileNotFoundError:
            continue
        journal = ProtectedApplyJournal(
            state_root,
            request_id=request_id,
            attempt_number=attempt_number,
            service_uid=service_uid,
        )
        if not journal.has_advanced_epoch_terminal(plan):
            continue
        if (
            plan.candidate_sha != candidate_sha
            or plan.attestation_digest != attestation_digest
            or plan.starting_mutation_epoch != starting_mutation_epoch
        ):
            raise ValueError("protected apply recovery plan binding drifted")
        if (
            plan.manager_configuration_epoch is not None
            and plan.manager_configuration_digest is not None
            and CapacityManagerConfigurationCompensationStore(
                state_root / "protected-capacity" / "capacity-manager-configuration-compensations",
                service_uid=service_uid,
            ).find_record_for_plan(
                request_id=request_id,
                attempt_number=attempt_number,
                plan_digest=plan.plan_digest,
                predecessor_configuration_epoch=plan.manager_configuration_epoch,
                predecessor_configuration_digest=plan.manager_configuration_digest,
                backup_lease_digest=plan.backup_lease_digest,
            )
            is not None
        ):
            return None
        if plan.schema_version == 7:
            artifact_sha256 = plan.execution_prerequisite_artifact_sha256
            if not isinstance(artifact_sha256, str):
                raise ValueError("protected apply recovery execution binding is unavailable")
            preparation_state = ExecutionPreparationOperationJournal(
                state_root,
                request_id=request_id,
                attempt_number=attempt_number,
                service_uid=service_uid,
            ).recovery_state(
                plan,
                artifact_sha256=artifact_sha256,
            )
            if preparation_state is ExecutionPreparationRecoveryState.COMPENSATED:
                return None
        return attempt_number
    return None


__all__ = ["find_advanced_epoch_attempt"]
