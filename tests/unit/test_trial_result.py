from uuid import uuid4

from loom.models.result import (
    AgentInfo,
    FailureReason,
    StepResult,
    TrialResult,
    TrialState,
)
from loom.models.trial import TrialConfig


def _trial_skeleton() -> TrialResult:
    return TrialResult(
        id=uuid4(),
        task_id="hello-world",
        task_checksum="0" * 64,
        team_id=uuid4(),
        agent=AgentInfo(name="oracle", version="1.0", mode="out-of-box"),
        config=TrialConfig(),
        state=TrialState.QUEUED,
    )


def test_trial_result_minimum():
    r = _trial_skeleton()
    assert r.steps == []
    assert r.reward is None
    assert r.trajectory_uri is None


def test_trial_result_failed_with_reason():
    r = _trial_skeleton().model_copy(
        update={"state": TrialState.FAILED, "failure_reason": FailureReason.AGENT_TIMEOUT},
    )
    assert r.failure_reason == FailureReason.AGENT_TIMEOUT


def test_step_result_aggregates():
    r = _trial_skeleton().model_copy(update={
        "steps": [
            StepResult(step_name="step-1"),
            StepResult(step_name="step-2"),
        ],
    })
    assert [s.step_name for s in r.steps] == ["step-1", "step-2"]


def test_trial_result_json_roundtrip():
    r = _trial_skeleton()
    dumped = r.model_dump_json()
    parsed = TrialResult.model_validate_json(dumped)
    assert parsed.id == r.id
    assert parsed.state == TrialState.QUEUED
