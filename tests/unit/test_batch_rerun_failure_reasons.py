from types import SimpleNamespace

import pytest

from loom_service.failure_taxonomy import is_auto_safe_rerun


@pytest.mark.parametrize(
    "failure_reason",
    [
        "gateway_error",
        "provider_transport_disconnect",
        "retry_exhausted",
        "exhausted_retries",
    ],
)
def test_transient_failures_are_rerunnable(failure_reason: str) -> None:
    trial = SimpleNamespace(state="failed", failure_reason=failure_reason)

    assert is_auto_safe_rerun(trial) is True


def test_non_transient_failures_are_not_rerunnable() -> None:
    trial = SimpleNamespace(state="failed", failure_reason="verifier_error")

    assert is_auto_safe_rerun(trial) is False


def test_reward_zero_success_is_not_rerunnable() -> None:
    trial = SimpleNamespace(
        state="succeeded",
        failure_reason=None,
        result={"aggregate_reward": 0.0},
    )

    assert is_auto_safe_rerun(trial) is False
