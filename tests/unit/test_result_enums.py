from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from loom.models.result import (
    AgentInfo,
    FailureReason,
    StepError,
    TrialState,
)
from loom.models.types import ModelSpec


def test_trial_state_values():
    for v in ("queued", "claimed", "running", "succeeded", "failed", "cancelled"):
        assert TrialState(v).value == v


def test_failure_reason_includes_exhausted_retries():
    assert FailureReason.EXHAUSTED_RETRIES.value == "exhausted_retries"


def test_failure_reason_includes_task_image_build_timeout():
    assert FailureReason.TASK_IMAGE_BUILD_TIMEOUT.value == "task_image_build_timeout"


def test_failure_reason_includes_provider_transport_disconnect():
    assert (
        FailureReason.PROVIDER_TRANSPORT_DISCONNECT.value
        == "provider_transport_disconnect"
    )


def test_agent_info():
    info = AgentInfo(
        name="claude-code-agent", version="1.4.0", mode="in-box",
        model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
    )
    assert info.mode == "in-box"
    assert info.model is not None
    assert info.model.provider == "anthropic"


def test_step_error_for_agent_timeout():
    err = StepError(
        phase="agent", reason="timeout",
        message="Agent run exceeded 1800s",
        occurred_at=datetime.now(UTC),
    )
    assert err.phase == "agent"
    assert err.traceback is None


def test_step_error_for_exception_has_traceback():
    err = StepError(
        phase="agent", reason="exception",
        message="RuntimeError: kaboom",
        traceback="Traceback ...",
        occurred_at=datetime.now(UTC),
    )
    assert err.traceback is not None


def test_step_error_phase_validation():
    with pytest.raises(ValidationError):
        StepError(
            phase="invalid", reason="timeout",  # type: ignore[arg-type]
            message="x", occurred_at=datetime.now(UTC),
        )
