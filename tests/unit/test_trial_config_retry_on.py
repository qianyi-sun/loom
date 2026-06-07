"""Regression: every retry_on value used in tests/fixtures and
tests/system/ must be a valid `RetryReason` enum value.

Bug 2 from the post-Plan-7 review: test_full_stack_worker_crash.py used
`retry_on=["crash"]`, which Pydantic rejects (the enum value is
`"worker_crash"`). Tests that silently fail at submit-time don't
actually exercise the crash + retry path.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.models.trial import RetryPolicy, RetryReason


@pytest.mark.parametrize(
    "reason",
    [r.value for r in RetryReason],
)
def test_each_enum_value_is_a_valid_retry_on_input(reason: str) -> None:
    """Every published RetryReason value MUST round-trip through
    RetryPolicy.model_validate. If this fails after a schema change,
    update the docs + system tests too."""
    policy = RetryPolicy.model_validate({
        "max_attempts": 2,
        "retry_on": [reason],
    })
    assert reason in {r.value for r in policy.retry_on}


def test_unknown_retry_on_value_rejected() -> None:
    """The exact bug we shipped: `"crash"` is NOT a valid RetryReason."""
    with pytest.raises(ValidationError):
        RetryPolicy.model_validate({
            "max_attempts": 2,
            "retry_on": ["crash"],
        })
