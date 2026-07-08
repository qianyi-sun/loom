"""Family advance-predicate plugins (#672)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loom.family_run.protocols import FamilyStateLike, TrialLike
from loom.family_run.spec import AdvanceDecision, ResolvedFamilyRunSpec


@dataclass
class AlwaysOnTerminalPredicate:
    default_params: dict[str, Any] = field(default_factory=dict)

    def decide(
        self,
        *,
        trial: TrialLike,
        family: FamilyStateLike,
        spec: ResolvedFamilyRunSpec,
        params: dict[str, Any],
    ) -> AdvanceDecision:
        return AdvanceDecision.ADVANCE


@dataclass
class SuccessOrRetryExhaustedPredicate:
    default_params: dict[str, Any] = field(default_factory=dict)

    def decide(
        self,
        *,
        trial: TrialLike,
        family: FamilyStateLike,
        spec: ResolvedFamilyRunSpec,
        params: dict[str, Any],
    ) -> AdvanceDecision:
        if trial.state == "succeeded":
            return AdvanceDecision.ADVANCE
        retry_budget = int(params.get("retry_budget", 1))
        # ``family.attempt_count`` counts prior retries for this task; a
        # new task starts at 0.
        if family.attempt_count + 1 >= retry_budget:
            return AdvanceDecision.ADVANCE
        return AdvanceDecision.RETRY
