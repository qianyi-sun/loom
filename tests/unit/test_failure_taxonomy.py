from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from loom_service.failure_taxonomy import (
    build_supplemental_rerun_plan,
    classify_trial_outcome,
)


def _trial(
    *,
    task_id: str,
    state: str,
    failure_reason: str | None = None,
    result: dict[str, object] | None = None,
    sample_idx: int = 0,
    combination_idx: int = 0,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        batch_id=uuid4(),
        task_id=task_id,
        state=state,
        failure_reason=failure_reason,
        failure_message=f"{failure_reason} message" if failure_reason else None,
        result=result,
        sample_idx=sample_idx,
        combination_idx=combination_idx,
    )


def test_reward_zero_is_score_failure_not_platform_failure() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="source-useful/task-score-zero",
            state="succeeded",
            result={"aggregate_reward": 0.0},
        )
    )

    assert classification["reason_code"] == "trial.score_failure"
    assert classification["failure_class"] == "score_failure"
    assert classification["platform_outcome"] == "success"
    assert classification["score_outcome"] == "failed"
    assert classification["root_cause"] == "model_or_task_score"
    assert classification["rerun_recommendation"] == "not_rerunnable"


def test_failed_platform_gateway_is_auto_safe_rerun() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="source-useful/task-provider-flake",
            state="failed",
            failure_reason="provider_transport_disconnect",
        )
    )

    assert classification["failure_class"] == "platform_failure"
    assert classification["root_cause"] == "provider_transport"
    assert classification["rerun_recommendation"] == "auto_safe"
    assert classification["rerunnable"] is True


def test_task_compatibility_requires_task_change_before_rerun() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="source-useful/task-incompatible",
            state="failed",
            failure_reason="task_compatibility",
        )
    )

    assert classification["failure_class"] == "task_failure"
    assert classification["root_cause"] == "task_compatibility"
    assert classification["rerun_recommendation"] == "not_rerunnable"
    assert classification["requires_task_change"] is True


def test_supplemental_plan_is_deterministic_and_task_filtered() -> None:
    batch = SimpleNamespace(id=uuid4(), team_id=uuid4())
    trials = [
        _trial(
            task_id="source-useful/task-score-zero",
            state="succeeded",
            result={"aggregate_reward": 0.0},
        ),
        _trial(
            task_id="source-useful/task-auto",
            state="failed",
            failure_reason="gateway_error",
        ),
        _trial(
            task_id="source-useful/task-operator",
            state="failed",
            failure_reason="verifier_timeout",
        ),
        _trial(
            task_id="source-useful/task-incompatible",
            state="failed",
            failure_reason="task_compatibility",
        ),
    ]

    plan = build_supplemental_rerun_plan(
        batch,
        trials,
        task_ids=[
            "source-useful/task-incompatible",
            "source-useful/task-auto",
            "source-useful/task-score-zero",
            "source-useful/task-operator",
        ],
    )

    assert plan["supplemental_task_ids"] == ["source-useful/task-auto"]
    assert [item["task_id"] for item in plan["auto_safe"]] == [
        "source-useful/task-auto"
    ]
    assert [item["task_id"] for item in plan["operator_approval"]] == [
        "source-useful/task-operator"
    ]
    assert [item["task_id"] for item in plan["not_rerunnable"]] == [
        "source-useful/task-incompatible",
        "source-useful/task-score-zero",
    ]
    assert plan["summary"] == {
        "auto_safe": 1,
        "operator_approval": 1,
        "not_rerunnable": 2,
        "already_covered": 0,
        "selected_final_trials": 4,
    }


def test_successful_supplemental_trial_becomes_final_selection() -> None:
    batch_id = uuid4()
    child_batch_id = uuid4()
    original = _trial(
        task_id="source-useful/task-auto",
        state="failed",
        failure_reason="gateway_error",
    )
    original.batch_id = batch_id
    rerun = _trial(
        task_id="source-useful/task-auto",
        state="succeeded",
        result={"aggregate_reward": 1.0},
    )
    rerun.batch_id = child_batch_id

    plan = build_supplemental_rerun_plan(
        SimpleNamespace(id=batch_id, team_id=uuid4()),
        [original],
        supplemental_trials=[rerun],
    )

    assert plan["summary"]["already_covered"] == 1
    assert plan["supplemental_task_ids"] == []
    assert plan["final_trial_selection"] == [
        {
            "task_id": original.task_id,
            "sample_idx": 0,
            "combination_idx": 0,
            "selected_trial_id": str(rerun.id),
            "selected_batch_id": str(child_batch_id),
            "selected_source": "supplemental",
            "original_trial_id": str(original.id),
            "original_failure_class": "platform_failure",
        }
    ]


def test_5003_manual_classification_fixture_replays() -> None:
    fixture = (
        Path(__file__).parents[1]
        / "fixtures"
        / "source_useful_5003_failure_classification.json"
    )
    rows = json.loads(fixture.read_text(encoding="utf-8"))["items"]

    actual = {
        item["task_id"]: classify_trial_outcome(
            _trial(
                task_id=item["task_id"],
                state=item["state"],
                failure_reason=item.get("failure_reason"),
                result=item.get("result"),
            )
        )["failure_class"]
        for item in rows
    }

    assert actual == {item["task_id"]: item["manual_failure_class"] for item in rows}


def test_5003_production_supplemental_targets_replay() -> None:
    fixture = (
        Path(__file__).parents[1]
        / "fixtures"
        / "source_useful_5003_failure_classification.json"
    )
    data = json.loads(fixture.read_text(encoding="utf-8"))
    evidence = data["production_evidence"]
    rows = data["production_items"]
    batch = SimpleNamespace(id=evidence["main_batch_id"], team_id=uuid4())
    trials = [
        _trial(
            task_id=item["task_id"],
            state=item["state"],
            failure_reason=item.get("failure_reason"),
            result=item.get("result"),
        )
        for item in rows
    ]

    assert sum(1 for item in rows if item["state"] == "failed") == evidence[
        "main_failed_count"
    ]
    assert sum(1 for item in rows if item["state"] == "cancelled") == evidence[
        "main_cancelled_count"
    ]
    actual_classes = {
        item["task_id"]: classify_trial_outcome(trial)["failure_class"]
        for item, trial in zip(rows, trials, strict=True)
    }
    assert actual_classes == {
        item["task_id"]: item["manual_failure_class"] for item in rows
    }

    plan = build_supplemental_rerun_plan(
        batch,
        trials,
        include_operator_approval=True,
    )

    assert plan["supplemental_task_ids"] == evidence["supplemental_target_task_ids"]
    assert plan["summary"] == {
        "auto_safe": 0,
        "operator_approval": 32,
        "not_rerunnable": 0,
        "already_covered": 0,
        "selected_final_trials": 32,
    }
