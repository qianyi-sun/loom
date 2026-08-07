from datetime import UTC, datetime
from uuid import uuid4

from loom.models.result import (
    AgentInfo,
    FailureReason,
    StepError,
    StepResult,
    TrialResult,
    TrialState,
)
from loom.models.types import ModelSpec
from loom.models.verifier import VerifierError, VerifierResult
from loom.trial.trial import _first_terminal_step_failure
from tests._trial_config_defaults import stub_trial_config


def _trial_result(
    step: StepResult,
    *,
    model: ModelSpec | None = None,
) -> TrialResult:
    return TrialResult(
        task_id="task",
        task_checksum="0" * 64,
        team_id=uuid4(),
        agent=AgentInfo(name="oracle", version="1.0", mode="out-of-box", model=model),
        config=stub_trial_config(),
        state=TrialState.RUNNING,
        steps=[step],
    )


def _model() -> ModelSpec:
    return ModelSpec(provider="openai", name="glm-5.1", source="api")


def test_step_agent_error_promotes_to_agent_error() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                error=StepError(
                    phase="agent",
                    reason="exception",
                    message="solve.sh missing",
                    occurred_at=datetime.now(UTC),
                ),
            )
        )
    )

    assert failure == (FailureReason.AGENT_ERROR, "solve.sh missing")


def test_step_agent_transport_disconnect_promotes_to_provider_transport_disconnect() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                error=StepError(
                    phase="agent",
                    reason="exception",
                    message=(
                        "codex exited rc=1 on step main; stderr: "
                        "Server disconnected without sending a response."
                    ),
                    occurred_at=datetime.now(UTC),
                ),
            )
        )
    )

    assert failure is not None
    assert failure[0].value == "provider_transport_disconnect"
    assert "Server disconnected without sending a response" in str(failure[1])


def test_scored_verifier_result_suppresses_agent_error_terminal_failure() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                error=StepError(
                    phase="agent",
                    reason="exception",
                    message="solve.sh exited rc=1",
                    occurred_at=datetime.now(UTC),
                ),
                verifier_result=VerifierResult(
                    rewards={"passed": 0.0, "pytest_pass_rate": 0.0},
                ),
            )
        )
    )

    assert failure is None


def test_scored_model_backed_agent_error_without_llm_calls_is_terminal() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                error=StepError(
                    phase="agent",
                    reason="exception",
                    message="solve.sh exited rc=1",
                    occurred_at=datetime.now(UTC),
                ),
                verifier_result=VerifierResult(rewards={"passed": 0.0}),
            ),
            model=_model(),
        ),
        llm_call_count=0,
    )

    assert failure == (FailureReason.AGENT_ERROR, "solve.sh exited rc=1")


def test_scored_model_backed_agent_error_with_llm_calls_stays_suppressed() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                error=StepError(
                    phase="agent",
                    reason="exception",
                    message="solve.sh exited rc=1",
                    occurred_at=datetime.now(UTC),
                ),
                verifier_result=VerifierResult(rewards={"passed": 0.0}),
            ),
            model=_model(),
        ),
        llm_call_count=3,
    )

    assert failure is None


def test_harbor_setup_error_with_rewards_is_task_compatibility() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                error=StepError(
                    phase="agent",
                    reason="exception",
                    message=(
                        "terminus-2 requires harbor@527d50d preinstalled "
                        "in the worker image"
                    ),
                    occurred_at=datetime.now(UTC),
                ),
                verifier_result=VerifierResult(rewards={"passed": 0.0}),
            ),
            model=_model(),
        ),
        llm_call_count=0,
    )

    assert failure is not None
    assert failure[0] == FailureReason.TASK_COMPATIBILITY
    assert failure[1] is not None
    assert "Harbor pin" in failure[1]


def test_harbor_setup_error_never_suppressed_even_with_llm_calls() -> None:
    """Platform setup failures are never scored-success carve-outs (#1186)."""
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                error=StepError(
                    phase="agent",
                    reason="exception",
                    message=(
                        "terminus-2 requires harbor@527d50d preinstalled "
                        "in the worker image"
                    ),
                    occurred_at=datetime.now(UTC),
                ),
                verifier_result=VerifierResult(rewards={"passed": 0.0}),
            ),
            model=_model(),
        ),
        llm_call_count=2,
    )

    assert failure is not None
    assert failure[0] == FailureReason.TASK_COMPATIBILITY


def test_empty_reward_verifier_error_promotes_to_verifier_error() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                verifier_result=VerifierResult(
                    rewards={},
                    error=VerifierError(kind="missing_tests", message="no junit"),
                ),
            )
        )
    )

    assert failure == (FailureReason.VERIFIER_ERROR, "no junit")


def test_scored_verifier_error_is_not_terminal_failure() -> None:
    failure = _first_terminal_step_failure(
        _trial_result(
            StepResult(
                step_name="main",
                verifier_result=VerifierResult(
                    rewards={"valid": 0.0},
                    error=VerifierError(kind="exec_failure", message="schema mismatch"),
                ),
            )
        )
    )

    assert failure is None
