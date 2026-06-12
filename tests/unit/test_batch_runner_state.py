"""next_batch_state state-machine rules (Plan 19 Task 4)."""

from __future__ import annotations

from loom_service.batch_runner import next_batch_state


def _empty() -> dict[str, int]:
    return {
        "queued": 0, "claimed": 0, "running": 0,
        "succeeded": 0, "failed": 0, "cancelled": 0,
    }


def test_no_trials_yet_remains_submitted() -> None:
    assert next_batch_state(
        current="submitted", expected=5, counts=_empty(),
    ) == "submitted"


def test_some_in_flight_running() -> None:
    counts = _empty() | {"queued": 1, "claimed": 1, "succeeded": 1}
    assert next_batch_state(
        current="submitted", expected=5, counts=counts,
    ) == "running"


def test_all_terminal_finished() -> None:
    counts = _empty() | {"succeeded": 2, "failed": 1}
    assert next_batch_state(
        current="running", expected=3, counts=counts,
    ) == "finished"


def test_terminal_cancelled_counts_toward_finished() -> None:
    """A cancelled trial is terminal — a batch whose expected
    count is fully accounted for by succeeded+failed+cancelled is
    finished."""
    counts = _empty() | {"succeeded": 1, "cancelled": 2}
    assert next_batch_state(
        current="running", expected=3, counts=counts,
    ) == "finished"


def test_cancelled_is_absorbing() -> None:
    """No matter what trial counts say, a cancelled batch stays
    cancelled."""
    counts = _empty() | {"succeeded": 1, "running": 1}
    assert next_batch_state(
        current="cancelled", expected=3, counts=counts,
    ) == "cancelled"


def test_in_flight_blocks_finished_even_if_terminal_count_reaches_expected() -> None:
    """3 expected, 3 succeeded, but 1 still queued — the queued one
    must drain (likely a stragglers-from-retry case) before finished."""
    counts = _empty() | {"succeeded": 3, "queued": 1}
    assert next_batch_state(
        current="running", expected=3, counts=counts,
    ) == "running"


def test_zero_expected_stays_submitted_with_no_trials() -> None:
    """Edge: a batch whose task_filter matched zero tasks shouldn't
    auto-finish on the first tick — the operator likely intends to
    re-edit the filter. This is a defensive flag for the rollup."""
    assert next_batch_state(
        current="submitted", expected=0, counts=_empty(),
    ) == "submitted"


def test_first_in_flight_promotes_submitted_to_running() -> None:
    counts = _empty() | {"queued": 1}
    assert next_batch_state(
        current="submitted", expected=2, counts=counts,
    ) == "running"
