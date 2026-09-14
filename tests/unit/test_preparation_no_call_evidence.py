"""Known build failures precede model execution and keep their durable diagnosis."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.db.schema import Trial
from loom_service.debug_evidence import build_trial_debug_evidence
from loom_service.diagnosis import build_trial_diagnosis
from loom_service.usage_accounting import (
    classify_no_call_evidence_failure,
    project_trial_llm_evidence,
    summarize_llm_evidence_for_trials,
)


class Request:
    def url_for(self, name, **values):
        return f"http://test/{name}/{values.get('trial_id', '')}"


def trial(*, state="failed", reason="task_image_build_failed", started_at=None, result=None):
    now = datetime.now(UTC)
    return Trial(
        id=uuid4(),
        team_id=uuid4(),
        task_id="build-before-agent",
        batch_id=None,
        state=state,
        failure_reason=reason,
        failure_message="Task image build failed before any execution lease",
        result=result,
        config={
            "agent_name": "terminus-2",
            "agent_model": {
                "source": "api",
                "provider": "openai",
                "name": "glm-5.2",
            },
        },
        submitted_at=now - timedelta(minutes=1),
        finished_at=now,
        started_at=started_at,
        trajectory_index={},
        attempt_count=1,
        requires_caps={},
    )


@pytest.mark.parametrize("reason", ["task_image_build_failed", "task_image_build_timeout"])
def test_build_failure_keeps_durable_debug_diagnosis_and_has_no_model_rerun(reason):
    row = trial(reason=reason)
    evidence = build_trial_debug_evidence(Request(), row, task=None, llm_calls=[])
    diagnosis = build_trial_diagnosis(evidence)

    assert evidence["failure"]["reason_code"] == f"trial.{reason}"
    assert evidence["failure"]["attribution"] != "model"
    assert diagnosis["primary_cause"]["reason_code"] == f"trial.{reason}"
    assert evidence["provider"]["llm_evidence_status"] == "not_applicable"
    assert evidence["provider"]["no_call_reason"] == reason
    assert "model path records" not in str(evidence["next_actions"])
    assert classify_no_call_evidence_failure(row, llm_calls_count=0) is None


def test_cancel_before_execution_is_not_an_agent_or_model_failure():
    row = trial(state="cancelled", reason=None)
    evidence = build_trial_debug_evidence(Request(), row, task=None, llm_calls=[])

    assert evidence["failure"]["reason_code"] == "trial.cancelled"
    assert evidence["provider"]["llm_evidence_status"] == "not_applicable"
    assert "model path records" not in str(evidence["next_actions"])


def test_prep_only_batch_retains_zero_calls_without_invalid_model_evidence():
    rows = [trial(), trial(state="cancelled", reason=None)]
    summary = summarize_llm_evidence_for_trials(rows, llm_call_counts={})

    assert summary["llm_evidence_status"] == "not_applicable"
    assert summary["no_call_trial_count"] == 2
    assert summary["no_call_reason_counts"] == {"task_image_build_failed": 1, "cancelled": 1}


def test_real_agent_no_call_and_mixed_batch_still_flag_missing_usage():
    failed_agent = trial(reason="agent_error", started_at=datetime.now(UTC))
    classification = classify_no_call_evidence_failure(failed_agent, llm_calls_count=0)
    assert classification["reason_code"] == "trial.agent_step_no_call"
    assert classification["attribution"] == "model"
    summary = summarize_llm_evidence_for_trials([trial(), failed_agent], llm_call_counts={})
    assert summary["llm_evidence_status"] == "no_calls_invalid"


def test_cancellation_after_calls_retains_usage_and_after_agent_error_retains_evidence():
    row = trial(state="cancelled", reason="agent_error", started_at=datetime.now(UTC))
    assert (
        project_trial_llm_evidence(row, llm_calls_count=2)["llm_evidence_status"]
        == "calls_observed"
    )
    assert (
        project_trial_llm_evidence(row, llm_calls_count=0)["llm_evidence_status"]
        == "no_calls_invalid"
    )
    row.started_at = None
    row.result = {"steps": [{"error": {"phase": "agent", "message": "agent failure"}}]}
    assert (
        project_trial_llm_evidence(row, llm_calls_count=0)["llm_evidence_status"]
        == "no_calls_invalid"
    )


def test_no_call_message_does_not_copy_arbitrary_build_logs():
    row = trial()
    row.failure_message = "opaque private registry credential payload"
    evidence = project_trial_llm_evidence(row, llm_calls_count=0)
    assert evidence["no_call_reason"] == "task_image_build_failed"
    assert "opaque" not in evidence["no_call_message"]
