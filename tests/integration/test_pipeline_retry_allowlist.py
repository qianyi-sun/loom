from __future__ import annotations

from loom.pipeline.retry import retry_decision
from loom.pipeline.state import RetryClass


def test_non_allowlisted_reason_never_retries() -> None:
    assert not retry_decision(
        completed_attempt_number=1,
        max_attempts=3,
        retry_class=RetryClass.INFRASTRUCTURE_TRANSIENT,
        reason_code="input_cache_capacity",
        terminal_cause=None,
    ).retry
