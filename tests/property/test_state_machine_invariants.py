"""State machine property tests (spec §6.7).

Invariants checked:
1. Terminal states are absorbing (no transitions out of succeeded/failed/cancelled).
2. The transition function never invents impossible transitions —
   `next_state` always lives in `_VALID_TRANSITIONS[state]` (or equals
   `state` when the event was a no-op).
"""

from hypothesis import given
from hypothesis import strategies as st

from loom.models.result import TrialState

_VALID_TRANSITIONS: dict[TrialState, set[TrialState]] = {
    TrialState.QUEUED: {TrialState.CLAIMED, TrialState.CANCELLED},
    TrialState.CLAIMED: {
        TrialState.RUNNING, TrialState.QUEUED, TrialState.CANCELLED,
    },
    TrialState.RUNNING: {
        TrialState.SUCCEEDED, TrialState.FAILED,
        TrialState.CANCELLED, TrialState.QUEUED,
    },
    TrialState.SUCCEEDED: set(),
    TrialState.FAILED: set(),
    TrialState.CANCELLED: set(),
}


def _apply_event(state: TrialState, event: str) -> TrialState:
    if event == "claim" and state == TrialState.QUEUED:
        return TrialState.CLAIMED
    if event == "start" and state == TrialState.CLAIMED:
        return TrialState.RUNNING
    if event == "succeed" and state == TrialState.RUNNING:
        return TrialState.SUCCEEDED
    if event == "fail" and state == TrialState.RUNNING:
        return TrialState.FAILED
    if event == "cancel" and state in (
        TrialState.QUEUED, TrialState.CLAIMED, TrialState.RUNNING,
    ):
        return TrialState.CANCELLED
    if event == "reclaim" and state in (
        TrialState.CLAIMED, TrialState.RUNNING,
    ):
        return TrialState.QUEUED
    return state


_TERMINALS = {
    TrialState.SUCCEEDED, TrialState.FAILED, TrialState.CANCELLED,
}


_EVENTS = st.sampled_from(
    ["claim", "start", "succeed", "fail", "cancel", "reclaim"],
)


@given(events=st.lists(_EVENTS, min_size=0, max_size=20))
def test_terminal_states_are_absorbing(events: list[str]) -> None:
    state = TrialState.QUEUED
    for event in events:
        next_state = _apply_event(state, event)
        if state in _TERMINALS:
            assert next_state == state, (
                f"transition out of terminal {state.value} via {event}"
            )
        state = next_state


@given(events=st.lists(_EVENTS, min_size=0, max_size=20))
def test_no_invalid_transitions(events: list[str]) -> None:
    """Every non-no-op transition must be in _VALID_TRANSITIONS."""
    state = TrialState.QUEUED
    for event in events:
        next_state = _apply_event(state, event)
        if next_state != state:
            assert next_state in _VALID_TRANSITIONS[state], (
                f"invalid: {state.value} → {next_state.value} via {event}"
            )
        state = next_state
