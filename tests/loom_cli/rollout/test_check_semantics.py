from __future__ import annotations

import pytest

from loom_cli.rollout.check_semantics import (
    FailureSemantics,
    RetryDecision,
    RetryPolicy,
    TypedCheck,
    classify_outcome,
)


def _decide(semantics: FailureSemantics, *, passed: bool, attempt: int, max_attempts: int = 3):
    return classify_outcome(
        semantics=semantics, passed=passed, attempt=attempt, max_attempts=max_attempts
    )


def test_passing_check_always_passes() -> None:
    assert _decide(FailureSemantics.TRANSIENT, passed=True, attempt=1) is RetryDecision.PASS
    assert _decide(FailureSemantics.DURABLE, passed=True, attempt=1) is RetryDecision.PASS


def test_durable_failure_blocks_immediately() -> None:
    assert _decide(FailureSemantics.DURABLE, passed=False, attempt=1) is RetryDecision.BLOCK


def test_transient_failure_retries_until_budget_then_escalates() -> None:
    # max_attempts=3: attempts 1 and 2 retry, attempt 3 (last) escalates to block.
    assert _decide(FailureSemantics.TRANSIENT, passed=False, attempt=1) is RetryDecision.RETRY
    assert _decide(FailureSemantics.TRANSIENT, passed=False, attempt=2) is RetryDecision.RETRY
    assert _decide(FailureSemantics.TRANSIENT, passed=False, attempt=3) is RetryDecision.BLOCK
    # attempts past the budget also block (no infinite retry).
    assert _decide(FailureSemantics.TRANSIENT, passed=False, attempt=4) is RetryDecision.BLOCK


def test_single_attempt_transient_blocks_on_first_failure() -> None:
    assert (
        _decide(FailureSemantics.TRANSIENT, passed=False, attempt=1, max_attempts=1)
        is RetryDecision.BLOCK
    )


def test_backoff_is_exponential_and_capped() -> None:
    policy = RetryPolicy(max_attempts=6, backoff_base_seconds=2.0, backoff_max_seconds=10.0)
    assert policy.backoff_for(1) == 2.0
    assert policy.backoff_for(2) == 4.0
    assert policy.backoff_for(3) == 8.0
    assert policy.backoff_for(4) == 10.0  # 16 capped to 10
    assert policy.backoff_for(5) == 10.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"backoff_base_seconds": 0},
        {"backoff_max_seconds": -1},
    ],
)
def test_retry_policy_rejects_invalid_config(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_backoff_and_classify_reject_bad_attempt() -> None:
    with pytest.raises(ValueError, match="attempt must be >= 1"):
        RetryPolicy().backoff_for(0)
    with pytest.raises(ValueError, match="attempt must be >= 1"):
        classify_outcome(
            semantics=FailureSemantics.TRANSIENT, passed=False, attempt=0, max_attempts=3
        )


def test_typed_check_convenience_matches_low_level() -> None:
    check = TypedCheck(
        check_id="rehearsal.browser",
        semantics=FailureSemantics.TRANSIENT,
        retry_policy=RetryPolicy(max_attempts=2, backoff_base_seconds=5.0),
    )
    assert check.decide(passed=False, attempt=1) is RetryDecision.RETRY
    assert check.decide(passed=False, attempt=2) is RetryDecision.BLOCK
    assert check.decide(passed=True, attempt=1) is RetryDecision.PASS
    assert check.backoff_for(1) == 5.0


def test_default_typed_check_uses_default_policy() -> None:
    check = TypedCheck(check_id="final.migration", semantics=FailureSemantics.DURABLE)
    assert check.retry_policy.max_attempts == 3
    assert check.decide(passed=False, attempt=1) is RetryDecision.BLOCK
