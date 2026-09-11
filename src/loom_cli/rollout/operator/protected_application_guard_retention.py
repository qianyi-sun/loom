"""Original-guard retention handshake subordinate to an application component.

A durable request alone cannot win a race with guard shutdown. The original
supervised guard must acknowledge it before the component may mutate SQL. Only
that component's exact terminal permits ordinary guard release again. This is
process-lifetime coordination, not application or administrator-window admission.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .protected_apply_journal import ComponentIntent, ProtectedApplyJournal
    from .staging_mutation_guard import MutationGuardEvidence

_REQUEST = "application-guard-retention.json"
_ACK = "application-guard-retention-ack.json"
_COMPONENT = "application-ownership-handoff"


def _request_root(state_root: Path, request_id: str) -> Path:
    from .model import validate_safe_identifier

    validate_safe_identifier(request_id, "request_id")
    if not state_root.is_absolute() or ".." in state_root.parts:
        raise ValueError("application guard state path is invalid")
    return state_root / "requests" / request_id


def _read(path: Path, uid: int) -> dict[str, object]:
    from .protected_apply_journal import _read_service_component_record

    return _read_service_component_record(path, service_uid=uid, filename=path.name)


def _sync(path: Path) -> None:
    for selected, flags in ((path, 0), (path.parent, os.O_DIRECTORY)):
        descriptor = os.open(selected, os.O_RDONLY | os.O_NOFOLLOW | flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _journal_context(
    journal: ProtectedApplyJournal,
    *,
    intent: ComponentIntent,
    guard: MutationGuardEvidence,
    starting_epoch: int,
) -> tuple[Path, dict[str, object]]:
    if (
        intent.component_id != _COMPONENT
        or guard.state != "ready"
        or guard.request_id != intent.request_id
        or guard.mutation_epoch != starting_epoch
    ):
        raise RuntimeError("application guard retention binding is invalid")
    root = journal.attempt_root.parent.parent
    record: dict[str, object] = {
        "schema_version": 1,
        "intent": intent.to_dict(),
        "guard": guard.to_dict(),
        "terminal_epoch": starting_epoch + 1,
    }
    return root, record


@dataclass(frozen=True, slots=True)
class _PendingRetention:
    intent: ComponentIntent
    guard: MutationGuardEvidence
    acknowledged: bool


def application_guard_is_retained(
    state_root: Path,
    *,
    request_id: str,
    service_uid: int,
    guard: MutationGuardEvidence | None = None,
    acknowledge: bool = False,
    require_record: bool = False,
) -> bool:
    return (
        _read_pending_retention(
            state_root,
            request_id=request_id,
            service_uid=service_uid,
            guard=guard,
            acknowledge=acknowledge,
            require_record=require_record,
        )
        is not None
    )


def _read_pending_retention(
    state_root: Path,
    *,
    request_id: str,
    service_uid: int,
    guard: MutationGuardEvidence | None = None,
    acknowledge: bool = False,
    require_record: bool = False,
) -> _PendingRetention | None:
    """Read pending retention; only the exact running guard may acknowledge it.

    Missing retention means normal guard semantics. Missing/malformed records
    after acknowledgement refuse release. A component terminal is the existing
    protected journal's completion authority, not a caller-selected ready flag.
    """
    from .protected_apply_journal import (
        ComponentIntent,
        ComponentTerminal,
        ProtectedApplyJournal,
        _require_directory,
    )
    from .staging_mutation_guard import MutationGuardEvidence

    root = _request_root(state_root, request_id)
    try:
        _require_directory(root, uid=service_uid)
    except FileNotFoundError:
        if require_record:
            raise RuntimeError("application guard retention request disappeared") from None
        return None
    try:
        record = _read(root / _REQUEST, service_uid)
    except FileNotFoundError:
        if require_record or os.path.lexists(root / _ACK):
            raise RuntimeError("application guard acknowledged retention disappeared") from None
        return None
    for directory in (state_root, state_root / "requests"):
        _require_directory(directory, uid=service_uid)
    if (
        set(record) != {"schema_version", "intent", "guard", "terminal_epoch"}
        or type(record["schema_version"]) is not int
        or record["schema_version"] != 1
        or not isinstance(record["intent"], dict)
        or not isinstance(record["guard"], dict)
        or type(record["terminal_epoch"]) is not int
    ):
        raise RuntimeError("application guard retention record is invalid")
    intent = ComponentIntent.from_dict(record["intent"])
    original = MutationGuardEvidence.from_dict(record["guard"])
    if (
        intent.component_id != _COMPONENT
        or intent.request_id != request_id
        or original.request_id != request_id
        or original.state != "ready"
        or record["terminal_epoch"] != original.mutation_epoch + 1
        or (guard is not None and guard != original)
    ):
        raise RuntimeError("application guard original identity changed")
    journal = ProtectedApplyJournal(
        state_root,
        request_id=request_id,
        attempt_number=intent.attempt_number,
        service_uid=service_uid,
    )
    component_root = journal.root / f"{intent.ordinal:02d}-{_COMPONENT}"
    for directory in (
        journal.attempt_root.parent,
        journal.attempt_root,
        journal.root,
        component_root,
    ):
        _require_directory(directory, uid=service_uid)
    if ComponentIntent.from_dict(_read(component_root / "intent.json", service_uid)) != intent:
        raise RuntimeError("application guard component intent changed")
    expected_ack = {
        "schema_version": 1,
        "intent_digest": intent.intent_digest,
        "guard_evidence_digest": original.evidence_digest,
    }
    try:
        ack = _read(root / _ACK, service_uid)
    except FileNotFoundError:
        ack = None
    try:
        terminal = ComponentTerminal.from_dict(_read(component_root / "terminal.json", service_uid))
    except FileNotFoundError:
        terminal = None
    if ack is not None and ack != expected_ack:
        raise RuntimeError("application guard acknowledgement changed")
    if terminal is not None:
        if (
            ack is None
            or terminal.intent_digest != intent.intent_digest
            or terminal.component_id != _COMPONENT
            or terminal.observed_epoch != record["terminal_epoch"]
        ):
            raise RuntimeError("application guard terminal binding is invalid")
        _sync(component_root / "terminal.json")
        return None
    if acknowledge:
        if guard != original or original.guard_pid != os.getpid():
            raise RuntimeError("only the original guard may acknowledge retention")
        # Flush the producer's request before committing the guard's promise.
        _sync(root / _REQUEST)
        journal._publish_or_match(root / _ACK, expected_ack)
        if _read(root / _ACK, service_uid) != expected_ack:
            raise RuntimeError("application guard acknowledgement readback changed")
        _sync(root / _ACK)
    return _PendingRetention(intent, original, acknowledge or ack is not None)


def retained_application_guard_for_resume(
    state_root: Path,
    *,
    request_id: str,
    service_uid: int,
    recovery_attempt: int | None,
    candidate_sha: str,
    candidate_tree: str,
    attestation_digest: str,
    starting_mutation_epoch: int,
) -> MutationGuardEvidence | None:
    """Select original guard only for the exact acknowledged, advanced plan.

    This reads journal authority; the caller must separately assert the original
    supervised process is ready and verify the live database is still at +1.
    Missing original processes are never permission to acquire a successor.
    """
    from .final_gate_plan import FinalGatePlanStore
    from .protected_apply_journal import ProtectedApplyJournal

    pending = _read_pending_retention(
        state_root,
        request_id=request_id,
        service_uid=service_uid,
    )
    if pending is None:
        return None
    if (
        type(recovery_attempt) is not int
        or pending.intent.attempt_number != recovery_attempt
        or not pending.acknowledged
    ):
        raise ValueError("application guard resume has no exact acknowledged recovery")
    plan = FinalGatePlanStore(
        state_root,
        request_id=request_id,
        attempt_number=recovery_attempt,
        service_uid=service_uid,
    ).read()
    original = pending.guard
    if (
        plan.plan_digest != pending.intent.plan_digest
        or plan.candidate_sha != candidate_sha
        or plan.candidate_tree != candidate_tree
        or plan.attestation_digest != attestation_digest
        or plan.starting_mutation_epoch != starting_mutation_epoch
        or original.candidate_sha != candidate_sha
        or original.candidate_tree != candidate_tree
        or original.mutation_epoch != starting_mutation_epoch
        or not ProtectedApplyJournal(
            state_root,
            request_id=request_id,
            attempt_number=recovery_attempt,
            service_uid=service_uid,
        ).has_advanced_epoch_terminal(plan)
    ):
        raise ValueError("application guard resume plan binding drifted")
    return original
