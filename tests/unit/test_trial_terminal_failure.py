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
from loom.models.verifier import VerifierError, VerifierResult
from loom.trial.trial import _first_terminal_step_failure
from tests._trial_config_defaults import stub_trial_config


def _trial_result(step: StepResult) -> TrialResult:
    return TrialResult(
        task_id="task",
        task_checksum="0" * 64,
        team_id=uuid4(),
        agent=AgentInfo(name="oracle", version="1.0", mode="out-of-box", model=None),
        config=stub_trial_config(),
        state=TrialState.RUNNING,
        steps=[step],
    )


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
