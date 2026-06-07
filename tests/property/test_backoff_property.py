"""Property: jittered delays stay within [base × (1-j), base × (1+j)]
and never exceed max_sec × (1+j)."""

from datetime import UTC, datetime

from hypothesis import given
from hypothesis import strategies as st

from loom.models.trial import BackoffSpec
from loom.retry import next_attempt_at


@given(
    attempt=st.integers(min_value=1, max_value=20),
    base=st.floats(min_value=1.0, max_value=600.0, allow_nan=False),
    mult=st.floats(min_value=1.0, max_value=4.0, allow_nan=False),
    max_sec=st.floats(min_value=60.0, max_value=3600.0, allow_nan=False),
    jitter=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
)
def test_delay_within_bounds(
    attempt: int, base: float, mult: float, max_sec: float, jitter: float,
) -> None:
    spec = BackoffSpec(
        base_sec=base, multiplier=mult, max_sec=max_sec, jitter=jitter,
    )
    now = datetime(2026, 6, 5, tzinfo=UTC)
    result = next_attempt_at(attempt_count=attempt, backoff=spec, now=now)
    delta = (result - now).total_seconds()
    expected_raw = min(base * (mult ** (attempt - 1)), max_sec)
    lo = expected_raw * (1 - jitter)
    hi = expected_raw * (1 + jitter)
    assert lo - 1e-6 <= delta <= hi + 1e-6


@given(
    base=st.floats(min_value=1.0, max_value=600.0, allow_nan=False),
    mult=st.floats(min_value=1.0, max_value=4.0, allow_nan=False),
    max_sec=st.floats(min_value=60.0, max_value=3600.0, allow_nan=False),
)
def test_zero_jitter_is_deterministic(
    base: float, mult: float, max_sec: float,
) -> None:
    """With jitter=0, repeated calls return the exact same delay."""
    spec = BackoffSpec(
        base_sec=base, multiplier=mult, max_sec=max_sec, jitter=0.0,
    )
    now = datetime(2026, 6, 5, tzinfo=UTC)
    a = next_attempt_at(attempt_count=3, backoff=spec, now=now)
    b = next_attempt_at(attempt_count=3, backoff=spec, now=now)
    assert a == b
