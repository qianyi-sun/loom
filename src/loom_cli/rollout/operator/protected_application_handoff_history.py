"""Bounded private selection of a completed original ownership operation.

Historical records establish origin only. A later request must independently
observe the enduring SQL effect under its own guard, and persist its own component
terminal before migrations may consume this origin. No old record is rewritten.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from .final_gate_plan import FinalGatePlan
from .model import validate_safe_identifier
from .protected_application_completed import historical_handoff_evidence
from .protected_apply_journal import (
    ApplicationRecoveryView,
    ComponentIntent,
    ComponentTerminal,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    _require_directory,
)

HISTORICAL_HANDOFF_IMPLEMENTATION = hashlib.sha256(b"loom-completed-owner-observation-v1").hexdigest()
_COMPONENT = "application-ownership-handoff"


class HandoffComponentBuilder(Protocol):
    def __call__(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int) -> ProtectedApplyComponent: ...


@dataclass(frozen=True, slots=True)
class CompletedApplicationHandoffOrigin:
    plan: FinalGatePlan
    journal: ProtectedApplyJournal
    component: ProtectedApplyComponent
    view: ApplicationRecoveryView = field(repr=False)
    terminal: ComponentTerminal

    def read(self) -> tuple[ApplicationRecoveryView, ComponentTerminal]:
        terminal = self.journal.read_application_handoff_terminal(self.plan, self.component, ordinal=2)
        view = self.journal.read_application_recovery_view(self.plan, self.component, ordinal=2)
        if terminal != self.terminal or view != self.view:
            raise RuntimeError("historical handoff original completion changed")
        return self.view, self.terminal


    def admitted_for(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
                     component: ProtectedApplyComponent) -> tuple[ApplicationRecoveryView, ComponentTerminal]:
        original = self.read()
        if (journal.request_id != plan.request_id or journal.attempt_number != plan.attempt_number
                or plan.request_id == self.plan.request_id or plan.starting_mutation_epoch < self.terminal.observed_epoch
                or component.component_id != _COMPONENT or component.implementation_digest != HISTORICAL_HANDOFF_IMPLEMENTATION
                or component.input_fingerprint != historical_handoff_evidence(plan, self.terminal)):
            raise RuntimeError("historical handoff current observer terminal binding changed")
        root = journal.root / f"02-{_COMPONENT}"
        try:
            for directory in (journal.attempt_root, journal.root, root):
                _require_directory(directory, uid=journal.service_uid)
            intent = ComponentIntent.from_dict(journal._read(root / "intent.json"))
            terminal = ComponentTerminal.from_dict(journal._read(root / "terminal.json"))
        except (FileNotFoundError, ValueError) as exc:
            raise RuntimeError("historical handoff current observer terminal is absent or invalid") from exc
        if (intent != ComponentIntent.build(plan, component, 2) or terminal.intent_digest != intent.intent_digest
                or terminal.component_id != _COMPONENT or terminal.applied
                or terminal.observed_epoch != plan.starting_mutation_epoch + 1
                or terminal.evidence_digest != historical_handoff_evidence(plan, self.terminal)):
            raise RuntimeError("historical handoff current observer terminal changed")
        journal._sync_application_recovery(root, "intent.json")
        journal._sync_application_recovery(root, "terminal.json")
        if self.read() != original:
            raise RuntimeError("historical handoff source changed during current terminal admission")
        return original


def _directories(path: Path, limit: int, uid: int) -> tuple[Path, ...]:
    _require_directory(path, uid=uid)
    paths: list[Path] = []
    with os.scandir(path) as entries:
        for entry in entries:
            if len(paths) >= limit or not entry.is_dir(follow_symlinks=False):
                raise RuntimeError("historical handoff directory inventory is unsafe or unbounded")
            directory = Path(entry.path)
            _require_directory(directory, uid=uid)
            paths.append(directory)
    return tuple(sorted(paths))


def select_completed_handoff(
    plan: FinalGatePlan, *, state_root: Path, service_uid: int,
    build_component: HandoffComponentBuilder,
) -> CompletedApplicationHandoffOrigin | None:
    if (not state_root.is_absolute() or ".." in state_root.parts or plan.environment != "staging"
            or plan.namespace != "loom-staging" or type(service_uid) is not int or service_uid < 0):
        raise ValueError("historical handoff selection scope is invalid")
    try:
        _require_directory(state_root, uid=service_uid)
        requests = _directories(state_root / "requests", 512, service_uid)
    except FileNotFoundError:
        return None
    origins = []
    examined = 0
    plan_bytes = 0
    for request in requests:
        validate_safe_identifier(request.name, "historical handoff request")
        if request.name == plan.request_id:
            continue
        try:
            attempts = _directories(request / "attempts", 32, service_uid)
        except FileNotFoundError:
            continue
        for attempt in attempts:
            examined += 1
            if (examined > 2048 or not attempt.name.isascii() or not attempt.name.isdecimal()
                    or int(attempt.name) < 1 or str(int(attempt.name)) != attempt.name):
                raise RuntimeError("historical handoff attempt inventory is unsafe or unbounded")
            journal = ProtectedApplyJournal(state_root, request_id=request.name,
                attempt_number=int(attempt.name), service_uid=service_uid)
            root = journal.root / f"02-{_COMPONENT}"
            if not os.path.lexists(root / "terminal.json"):
                continue
            _require_directory(journal.root, uid=service_uid)
            _require_directory(root, uid=service_uid)
            plan_path = attempt / "final-gate-plan.json"
            plan_bytes += plan_path.stat(follow_symlinks=False).st_size
            if plan_bytes > 32 * 1024 * 1024:
                raise RuntimeError("historical handoff plan inventory is unbounded")
            original = FinalGatePlan.from_dict(journal._read(plan_path))
            if original.request_id != request.name or original.attempt_number != int(attempt.name):
                raise RuntimeError("historical handoff original plan location changed")
            if (original.environment != plan.environment or original.namespace != plan.namespace
                    or original.checkpoint_schema_version != 3
                    or original.starting_mutation_epoch + 1 > plan.starting_mutation_epoch):
                continue
            intent = ComponentIntent.from_dict(journal._read(root / "intent.json"))
            if intent.implementation_digest == HISTORICAL_HANDOFF_IMPLEMENTATION:
                # An observation is not a second ownership operation. The real
                # origin must still exist and pass its full original phase checks.
                continue
            component = build_component(original, journal=journal, ordinal=2)
            if component.component_id != _COMPONENT or intent != ComponentIntent.build(original, component, 2):
                raise RuntimeError("historical handoff original implementation changed")
            terminal = journal.read_application_handoff_terminal(original, component, ordinal=2)
            view = journal.read_application_recovery_view(original, component, ordinal=2)
            if (terminal is None or view is None or view.admission is None or view.intent != intent
                    or terminal.intent_digest != intent.intent_digest
                    or terminal.observed_epoch != original.starting_mutation_epoch + 1):
                raise RuntimeError("historical handoff terminal has no original completed operation")
            origins.append(CompletedApplicationHandoffOrigin(original, journal, component, view, terminal))
    if not origins:
        return None
    origins.sort(key=lambda source: source.terminal.observed_epoch, reverse=True)
    selected = origins[0]
    assert selected.view.admission is not None
    if any(source.view.admission is None or source.view.admission.target != selected.view.admission.target
            or (source is not selected and source.terminal.observed_epoch == selected.terminal.observed_epoch)
            for source in origins):
        raise RuntimeError("historical handoff origins are contradictory or ambiguous")
    selected.read()
    return selected
