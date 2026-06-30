from types import SimpleNamespace

import pytest

from loom_service.routes.batches import _is_rerunnable_failure


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

    assert _is_rerunnable_failure(trial) is True


def test_non_transient_failures_are_not_rerunnable() -> None:
    trial = SimpleNamespace(state="failed", failure_reason="verifier_error")

    assert _is_rerunnable_failure(trial) is False
