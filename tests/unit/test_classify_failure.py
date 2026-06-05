from loom.errors import (
    AgentSetupTimeoutError,
    DriverError,
    TrajectoryFlushFailedError,
    VerifierError,
    classify_failure,
)
from loom.models.result import FailureReason


def test_agent_setup_timeout_to_agent_error():
    assert classify_failure(AgentSetupTimeoutError("x")) == FailureReason.AGENT_ERROR


def test_driver_error_to_env_start_failure():
    assert classify_failure(DriverError("x")) == FailureReason.ENV_START_FAILURE


def test_verifier_error():
    assert classify_failure(VerifierError("x")) == FailureReason.VERIFIER_ERROR


def test_trajectory_flush_failed():
    assert classify_failure(TrajectoryFlushFailedError("x")) == FailureReason.TRAJECTORY_FLUSH_FAILED


def test_generic_exception_is_internal():
    assert classify_failure(RuntimeError("x")) == FailureReason.INTERNAL_ERROR


def test_timeout_error_classification_is_phase_dependent():
    """Per spec §5.2: TimeoutError at trial-level → AGENT_TIMEOUT is the
    fallback. Phase-local handlers catch first and shouldn't reach this."""
    assert classify_failure(TimeoutError()) == FailureReason.AGENT_TIMEOUT
