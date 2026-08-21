"""Recover the narrow crash window after a protected epoch advance."""

from __future__ import annotations

from pathlib import Path

from .final_gate_plan import FinalGatePlanStore
from .model import validate_safe_identifier
from .protected_apply_journal import ProtectedApplyJournal


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
        return attempt_number
    return None


__all__ = ["find_advanced_epoch_attempt"]
