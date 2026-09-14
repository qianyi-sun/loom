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
_MIGRATION_COMPONENT = "database-migration"
_CAPACITY_COMPONENT = "staging-capacity-database"
_COMPONENTS = (_COMPONENT, _MIGRATION_COMPONENT, _CAPACITY_COMPONENT)


def _retention_names(component_id: str) -> tuple[str, str]:
    if component_id == _COMPONENT:
        return _REQUEST, _ACK
    if component_id == _MIGRATION_COMPONENT:
        return "application-migration-guard-retention.json", "application-migration-guard-retention-ack.json"
    if component_id == _CAPACITY_COMPONENT:
        return "application-capacity-guard-retention.json", "application-capacity-guard-retention-ack.json"
    raise ValueError("application guard retention component is invalid")


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
        intent.component_id not in _COMPONENTS
        or guard.state != "ready"
        or guard.request_id != intent.request_id
        or guard.mutation_epoch not in ({starting_epoch} if intent.component_id == _COMPONENT
                                       else {starting_epoch, starting_epoch + 1})
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
    observed_components: set[str] | None = None,
    component_id: str | None = None,
) -> bool:
    return (
        _read_pending_retention(
            state_root,
            request_id=request_id,
            service_uid=service_uid,
            guard=guard,
            acknowledge=acknowledge,
            require_record=require_record,
            observed_components=observed_components,
            component_id=component_id,
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
    observed_components: set[str] | None = None,
    component_id: str | None = None,
) -> _PendingRetention | None:
    """Observe all operation records before acknowledging a single pending owner.

    The supervised guard remembers every observed component, so deleting a later
    migration's request and ACK cannot be hidden by an earlier completed handoff.
    Component-specific readers require only their own completion record.
    """
    selected = _COMPONENTS if component_id is None else (component_id,)
    if observed_components is not None and not observed_components <= set(_COMPONENTS):
        raise ValueError("application guard retention history is invalid")
    root = _request_root(state_root, request_id)
    pending = []
    found = False
    for name in selected:
        request_name, _ = _retention_names(name)
        present = os.path.lexists(root / request_name)
        item = _read_component_retention(state_root, request_id=request_id, service_uid=service_uid,
            guard=guard, component_id=name,
            require_record=((component_id is not None and require_record)
                            or (observed_components is not None and name in observed_components)))
        if os.path.lexists(root / request_name) != present:
            raise RuntimeError("application guard retention history changed during observation")
        if present:
            found = True
            if observed_components is not None:
                observed_components.add(name)
        if item is not None:
            pending.append(item)
    if require_record and not found:
        raise RuntimeError("application guard acknowledged retention disappeared")
    if len(pending) > 1:
        raise RuntimeError("application guard has overlapping pending operations")
    if not pending:
        return None
    item = pending[0]
    if acknowledge:
        acknowledged = _read_component_retention(state_root, request_id=request_id, service_uid=service_uid,
            guard=guard, component_id=item.intent.component_id, acknowledge=True, require_record=True)
        if acknowledged is None:
            return None
        if acknowledged.intent != item.intent or acknowledged.guard != item.guard:
            raise RuntimeError("application guard pending operation changed during acknowledgement")
        return acknowledged
    return item


def _read_component_retention(
    state_root: Path,
    *,
    request_id: str,
    service_uid: int,
    component_id: str,
    guard: MutationGuardEvidence | None = None,
    acknowledge: bool = False,
    require_record: bool = False,
) -> _PendingRetention | None:
    """Read pending retention; only the exact running guard may acknowledge it.

    Missing retention means normal guard semantics. Missing/malformed records
    after acknowledgement refuse release. A component terminal is the existing
    protected journal's completion authority, not a caller-selected ready flag.
    After that terminal, a ready successor guard for the same request/candidate
    at the advanced epoch is no longer constrained by this finished retention.
    Pending handoffs still require the exact original guard.
    """
    from .protected_apply_journal import (
        ComponentIntent,
        ComponentTerminal,
        ProtectedApplyJournal,
        _require_directory,
    )
    from .staging_mutation_guard import MutationGuardEvidence

    request_name, ack_name = _retention_names(component_id)
    root = _request_root(state_root, request_id)
    try:
        _require_directory(root, uid=service_uid)
    except FileNotFoundError:
        if require_record:
            raise RuntimeError("application guard retention request disappeared") from None
        return None
    try:
        record = _read(root / request_name, service_uid)
    except FileNotFoundError:
        if require_record or os.path.lexists(root / ack_name):
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
        intent.component_id != component_id
        or intent.request_id != request_id
        or original.request_id != request_id
        or original.state != "ready"
        or record["terminal_epoch"] not in ({original.mutation_epoch + 1} if component_id == _COMPONENT
                                           else {original.mutation_epoch, original.mutation_epoch + 1})
    ):
        raise RuntimeError("application guard original identity changed")
    journal = ProtectedApplyJournal(
        state_root,
        request_id=request_id,
        attempt_number=intent.attempt_number,
        service_uid=service_uid,
    )
    component_root = journal.root / f"{intent.ordinal:02d}-{component_id}"
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
        ack = _read(root / ack_name, service_uid)
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
            or terminal.component_id != component_id
            or terminal.observed_epoch != record["terminal_epoch"]
        ):
            raise RuntimeError("application guard terminal binding is invalid")
        if guard is not None and guard != original and (
            guard.request_id != original.request_id
            or guard.candidate_sha != original.candidate_sha
            or guard.candidate_tree != original.candidate_tree
            or guard.mutation_epoch != record["terminal_epoch"]
            or guard.state != "ready"
        ):
            raise RuntimeError("application guard successor identity changed")
        _sync(component_root / "terminal.json")
        return None
    if guard is not None and guard != original:
        raise RuntimeError("application guard original identity changed")
    if acknowledge:
        if guard != original or original.guard_pid != os.getpid():
            raise RuntimeError("only the original guard may acknowledge retention")
        # Flush the producer's request before committing the guard's promise.
        _sync(root / request_name)
        journal._publish_or_match(root / ack_name, expected_ack)
        if _read(root / ack_name, service_uid) != expected_ack:
            raise RuntimeError("application guard acknowledgement readback changed")
        _sync(root / ack_name)
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
        or original.mutation_epoch not in ({starting_mutation_epoch} if pending.intent.component_id == _COMPONENT
                                           else {starting_mutation_epoch, starting_mutation_epoch + 1})
        or not ProtectedApplyJournal(
            state_root,
            request_id=request_id,
            attempt_number=recovery_attempt,
            service_uid=service_uid,
        ).has_advanced_epoch_terminal(plan)
    ):
        raise ValueError("application guard resume plan binding drifted")
    return original
