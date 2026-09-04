from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def test_attempt_deadline_is_an_immutable_monotonic_absolute_boundary() -> None:
    clock = _Clock(100.0)
    deadline = AttemptDeadline.after(5.0, clock=clock)

    assert deadline.monotonic_deadline == 105.0
    assert deadline.remaining() == 5.0

    clock.now = 103.5
    assert deadline.remaining() == 1.5

    with pytest.raises(FrozenInstanceError):
        deadline.monotonic_deadline = 200.0  # type: ignore[misc]


def test_attempt_deadline_never_returns_negative_remaining_time() -> None:
    clock = _Clock(10.0)
    deadline = AttemptDeadline.after(0.0, clock=clock)

    assert deadline.remaining() == 0.0
    assert deadline.reached
    with pytest.raises(AttemptDeadlineExceededError):
        deadline.require_remaining()


@pytest.mark.parametrize("timeout_sec", [-1.0, float("inf"), float("nan")])
def test_attempt_deadline_rejects_invalid_timeout(timeout_sec: float) -> None:
    with pytest.raises(ValueError, match="finite and non-negative"):
        AttemptDeadline.after(timeout_sec)
