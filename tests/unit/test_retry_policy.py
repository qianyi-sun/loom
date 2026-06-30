import pytest
from pydantic import ValidationError

from loom.models.trial import BackoffSpec, RetryPolicy, RetryReason


def test_backoff_defaults():
    b = BackoffSpec()
    assert b.base_sec == 30
    assert b.max_sec == 600
    assert b.multiplier == 2.0
    assert b.jitter == 0.2


def test_backoff_rejects_negative_jitter():
    with pytest.raises(ValidationError):
        BackoffSpec(jitter=-0.1)


def test_backoff_rejects_jitter_above_one():
    with pytest.raises(ValidationError):
        BackoffSpec(jitter=1.5)


def test_retry_policy_default_retries_transient_gateway_errors():
    p = RetryPolicy()
    assert p.max_attempts == 3
    assert p.retry_on == frozenset({
        RetryReason.GATEWAY_ERROR,
        RetryReason.PROVIDER_TRANSPORT_DISCONNECT,
    })


def test_retry_reason_values():
    for v in (
        "worker_crash",
        "env_start_failure",
        "agent_timeout",
        "verifier_timeout",
        "trajectory_flush_failed",
        "gateway_error",
        "provider_transport_disconnect",
    ):
        assert RetryReason(v).value == v
