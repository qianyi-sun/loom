from datetime import UTC, datetime

import pytest

from loom.models.trial import BackoffSpec
from loom.retry import next_attempt_at


def test_first_attempt_uses_base():
    """attempt_count=1 → delay = base × multiplier^0 = base."""
    spec = BackoffSpec(base_sec=10, multiplier=2.0, jitter=0.0)
    now = datetime(2026, 6, 5, tzinfo=UTC)
    result = next_attempt_at(attempt_count=1, backoff=spec, now=now)
    assert (result - now).total_seconds() == pytest.approx(10.0)


def test_second_attempt_doubles():
    spec = BackoffSpec(base_sec=10, multiplier=2.0, jitter=0.0)
    now = datetime(2026, 6, 5, tzinfo=UTC)
    result = next_attempt_at(attempt_count=2, backoff=spec, now=now)
    assert (result - now).total_seconds() == pytest.approx(20.0)


def test_third_attempt_quadruples():
    spec = BackoffSpec(base_sec=10, multiplier=2.0, jitter=0.0)
    now = datetime(2026, 6, 5, tzinfo=UTC)
    result = next_attempt_at(attempt_count=3, backoff=spec, now=now)
    assert (result - now).total_seconds() == pytest.approx(40.0)


def test_max_sec_cap():
    spec = BackoffSpec(base_sec=10, multiplier=2.0, jitter=0.0, max_sec=25)
    now = datetime(2026, 6, 5, tzinfo=UTC)
    result = next_attempt_at(attempt_count=5, backoff=spec, now=now)
    assert (result - now).total_seconds() == pytest.approx(25.0)


def test_jitter_within_bounds():
    """With jitter=0.2 and base=10, result is in [8, 12] seconds."""
    spec = BackoffSpec(base_sec=10, multiplier=1.0, jitter=0.2, max_sec=10)
    now = datetime(2026, 6, 5, tzinfo=UTC)
    samples = [
        (next_attempt_at(attempt_count=1, backoff=spec, now=now) - now).total_seconds()
        for _ in range(200)
    ]
    assert all(8.0 <= s <= 12.0 for s in samples)
    assert min(samples) < 9.5     # some jitter actually fires low
    assert max(samples) > 10.5    # and high
