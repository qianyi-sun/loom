"""Pure state-machine transitions for family runs (#672).

The CP calls these at finalize; PR-2's orchestrator service reuses the
same functions. Keeping them pure makes both callers testable without a
DB.
"""

from __future__ import annotations

from dataclasses import dataclass

from loom.family_run.protocols import FamilyStateLike
from loom.family_run.spec import AdvanceDecision


@dataclass(frozen=True)
class NextFamilyState:
    """Result of applying an advance decision - the caller writes this back."""

    state: str
    current_index: int
    attempt_count: int


def apply_advance_decision(
    family: FamilyStateLike, decision: AdvanceDecision,
) -> NextFamilyState:
    """Return the family's next persisted state given a per-trial decision.

    ``advance`` transitions to ``adapting`` (orchestrator picks up next).
    ``retry``  keeps ``current_index``, bumps ``attempt_count``, re-queues.
    ``skip``   bumps ``current_index``; if past the sequence end -> ``done``.
    ``abort``  terminates the family.
    """
    match decision:
        case AdvanceDecision.ADVANCE:
            return NextFamilyState(
                state="adapting",
                current_index=family.current_index,
                attempt_count=0,
            )
        case AdvanceDecision.RETRY:
            return NextFamilyState(
                state="pending",
                current_index=family.current_index,
                attempt_count=family.attempt_count + 1,
            )
        case AdvanceDecision.SKIP:
            next_index = family.current_index + 1
            if next_index >= len(family.task_sequence):
                return NextFamilyState(
                    state="done",
                    current_index=next_index,
                    attempt_count=0,
                )
            return NextFamilyState(
                state="pending",
                current_index=next_index,
                attempt_count=0,
            )
        case AdvanceDecision.ABORT:
            return NextFamilyState(
                state="aborted",
                current_index=family.current_index,
                attempt_count=family.attempt_count,
            )
