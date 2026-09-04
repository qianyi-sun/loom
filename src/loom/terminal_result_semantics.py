"""Shared terminal-state consistency checks for persisted trial results.

The worker projects ``TrialResult`` before it reports the terminal row state.
That split write is intentional, but neither write boundary may accept a
payload that can later make a successful trial carry terminal failure
semantics.  Delivery uses the same checks so historical drift fails closed.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from numbers import Real
from typing import Any

TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})

TerminalResultConflict = dict[str, Any]


def _conflict(*, field: str, expected: Any, actual: Any) -> TerminalResultConflict:
    return {"field": field, "expected": expected, "actual": actual}


def explicit_skip_verifier(config: Any) -> bool:
    """Return true only for the persisted, explicit verifier-skip contract."""

    return isinstance(config, Mapping) and config.get("skip_verifier") is True


def has_numeric_reward(value: Any) -> bool:
    """Accept scalar or reward-map numbers, including zero, but not booleans/NaN."""

    if isinstance(value, Real) and not isinstance(value, bool):
        return math.isfinite(float(value))
    if isinstance(value, Mapping) and value:
        return all(has_numeric_reward(item) for item in value.values())
    return False


def result_has_numeric_reward(result: Mapping[str, Any]) -> bool:
    return any(
        has_numeric_reward(result.get(key)) for key in ("reward", "aggregate_reward", "score")
    )


def projected_result_conflicts(result: Any) -> list[TerminalResultConflict]:
    """Return contradictions contained within a projected terminal result.

    Output projection is the modern worker write path and must carry its own
    terminal ``state``.  This stricter entry-point check does not change the
    legacy terminal PATCH contract, where an older result may lack ``state``.
    """

    if not isinstance(result, Mapping):
        return [_conflict(field="result", expected="object", actual=type(result).__name__)]

    result_state = result.get("state")
    if result_state not in TERMINAL_TRIAL_STATES:
        return [
            _conflict(
                field="result.state",
                expected=sorted(TERMINAL_TRIAL_STATES),
                actual=result_state,
            )
        ]
    return terminal_result_conflicts(
        state=str(result_state),
        result=result,
        failure_reason=None,
        config=result.get("config"),
    )


def terminal_result_conflicts(
    *,
    state: str,
    result: Any,
    failure_reason: Any,
    config: Any,
) -> list[TerminalResultConflict]:
    """Compare a terminal row transition with its effective persisted result.

    A missing result remains valid for failed/cancelled setup paths.  Legacy
    result objects without an embedded ``state`` also remain valid.  Numeric
    zero is a valid score; a missing score is valid only when verifier skipping
    was explicitly persisted in the trial config.
    """

    conflicts: list[TerminalResultConflict] = []
    if state not in TERMINAL_TRIAL_STATES:
        conflicts.append(
            _conflict(
                field="state",
                expected=sorted(TERMINAL_TRIAL_STATES),
                actual=state,
            )
        )
        return conflicts

    if result is None:
        if state == "succeeded":
            conflicts.append(_conflict(field="result", expected="object", actual=None))
        return conflicts
    if not isinstance(result, Mapping):
        conflicts.append(_conflict(field="result", expected="object", actual=type(result).__name__))
        return conflicts

    result_state = result.get("state")
    if result_state is not None and result_state != state:
        conflicts.append(_conflict(field="result.state", expected=state, actual=result_state))

    result_failure_reason = result.get("failure_reason")
    if state == "succeeded":
        if failure_reason is not None:
            conflicts.append(
                _conflict(field="failure_reason", expected=None, actual=failure_reason)
            )

        if not result_has_numeric_reward(result) and not explicit_skip_verifier(config):
            conflicts.append(
                _conflict(
                    field="result.reward",
                    expected="numeric reward or config.skip_verifier=true",
                    actual=result.get("reward"),
                )
            )
        if result_failure_reason is not None:
            conflicts.append(
                _conflict(
                    field="result.failure_reason",
                    expected=None,
                    actual=result_failure_reason,
                )
            )
    elif (
        failure_reason is not None
        and result_failure_reason is not None
        and result_failure_reason != failure_reason
    ):
        conflicts.append(
            _conflict(
                field="result.failure_reason",
                expected=failure_reason,
                actual=result_failure_reason,
            )
        )

    return conflicts
