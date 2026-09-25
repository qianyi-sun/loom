from copy import deepcopy

import pytest

from loom.execution_failure_diagnosis import execution_failure_diagnosis
from tests.unit.test_execution_resource_allocation import _plan


def _event(ordinal, *, reason="Error", restarts=1, previous=True, started="2026-09-23T21:06:14Z"):
    return {"ordinal": ordinal, "payload": {
        "job_uid": "job", "pod_uid": "pod", "reason": "SandboxRestarted",
        "normalized_state": "failed", "container_diagnostics": [{
            "name": "task-sandbox", "restart_count": restarts,
            "previous_termination" if previous else "current_termination": {
                "reason": reason, "exit_code": 137, "signal": 9,
                "started_at": started, "finished_at": "2026-09-23T21:19:33Z",
            },
        }],
    }}


def _diagnose(events):
    return execution_failure_diagnosis(events, plan=_plan()[1], job_uid="job", pod_uid="pod")


def test_late_oom_enriches_original_incarnation_without_trusting_exit_137():
    first = _event(1)
    assert _diagnose([first]) is None
    later = _event(3, reason="OOMKilled")
    cleanup = _event(4, reason="Error", restarts=2)
    events = [cleanup, later, first]
    unchanged = deepcopy(events)
    result = _diagnose(events)
    assert result["reason"] == "oom_killed"
    assert result["memory_limit_mib"] == 4096
    assert result["container_incarnation"] == 0
    assert result["evidence_ordinal"] == 3
    assert "4 GiB" in result["message"]
    assert events == unchanged


@pytest.mark.parametrize("mutation", ["pod", "job", "replacement", "started"])
def test_oom_of_another_identity_cannot_reclassify_original_failure(mutation):
    first = _event(1)
    later = _event(2, reason="OOMKilled")
    if mutation in {"pod", "job"}:
        later["payload"][mutation + "_uid"] = "other"
    elif mutation == "replacement":
        later["payload"]["container_diagnostics"][0]["restart_count"] = 2
    else:
        later["payload"]["container_diagnostics"][0]["previous_termination"]["started_at"] = "2026-09-23T21:20:00Z"
    assert _diagnose([first, later]) is None


def test_initial_missing_last_state_can_be_enriched_and_diagnosis_uses_existing_report():
    from loom_service.diagnosis import build_trial_diagnosis

    first = _event(1)
    first["payload"]["container_diagnostics"][0].pop("previous_termination")
    result = _diagnose([first, _event(2, reason="OOMKilled")])
    assert result is not None
    report = build_trial_diagnosis({
        "entity": {"type": "trial", "id": "trial"},
        "failure": {"reason_code": "trial.oom_killed", "platform_outcome": "failed"},
        "execution_failure": result,
    })
    assert report["summary"] == result["message"]
    assert report["primary_cause"]["attribution"] == "resource_limit"
    assert any("missing peak data is not zero" in e for e in report["evidence"])


def test_fixture_oom_uses_the_bound_fixture_limit_and_cannot_name_another_fixture():
    from tests.unit.test_task_fixtures import _plan as fixture_plan

    event = _event(1, reason="OOMKilled")
    event["payload"]["container_diagnostics"][0]["name"] = "fixture-server"
    result = execution_failure_diagnosis([event], plan=fixture_plan(), job_uid="job", pod_uid="pod")
    assert result is not None
    assert result["container_role"] == "fixture-server" and result["stage"] == "fixture"
    assert result["memory_limit_mib"] == 128
    event["payload"]["container_diagnostics"][0]["name"] = "fixture-other"
    assert execution_failure_diagnosis([event], plan=fixture_plan(), job_uid="job", pod_uid="pod") is None
