"""Family failure-policy plugins (#672)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loom.family_run.protocols import FamilyStateLike
from loom.family_run.spec import FailureAction


@dataclass
class StallFamilyPolicy:
    """Retry the adapter with exponential backoff; abort after N attempts.

    Terminal action (``abort_family``) transitions the family to
    ``stalled`` in the orchestrator - the operator can inspect
    ``last_error`` and run ``loom admin family-run resume`` to advance.
    """

    default_params: dict[str, Any] = field(default_factory=dict)

    def on_adapter_failure(
        self,
        *,
        family: FamilyStateLike,
        exception: BaseException,
        params: dict[str, Any],
    ) -> FailureAction:
        max_retries = int(params.get("max_retries", 3))
        base = float(params.get("backoff_sec", 30.0))
        if family.attempt_count >= max_retries:
            return FailureAction.abort_family()
        backoff = base * (2**family.attempt_count)
        return FailureAction.retry_with_backoff(backoff)


@dataclass
class SkipAndAdvancePolicy:
    default_params: dict[str, Any] = field(default_factory=dict)

    def on_adapter_failure(
        self,
        *,
        family: FamilyStateLike,
        exception: BaseException,
        params: dict[str, Any],
    ) -> FailureAction:
        return FailureAction.skip_and_advance()


@dataclass
class AbortFamilyPolicy:
    default_params: dict[str, Any] = field(default_factory=dict)

    def on_adapter_failure(
        self,
        *,
        family: FamilyStateLike,
        exception: BaseException,
        params: dict[str, Any],
    ) -> FailureAction:
        return FailureAction.abort_family()
