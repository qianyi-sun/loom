from __future__ import annotations

import json

import pytest

from loom_service.diagnosis import build_batch_diagnosis, build_trial_diagnosis


def test_trial_gateway_error_diagnosis_is_human_readable_and_redacted() -> None:
    evidence = {
        "schema_version": "1",
        "entity": {"type": "trial", "id": "trial-1", "team_id": "team-a"},
        "lifecycle": {"state": "failed", "attempt_count": 2},
        "failure": {
            "reason_code": "trial.gateway_error",
            "category": "gateway",
            "attribution": "provider",
            "message": (
                "Gateway call failed for http://loom-control-plane:8080 "
                "with Authorization: Bearer loom_api_supersecret and "
                "https://minio.internal/a?X-Amz-Signature=secret"
            ),
        },
        "provider": {
            "llm_calls_count": 1,
            "models": ["openai/gpt-4o-mini"],
            "provider_model_id": "gpt-4o-mini",
        },
        "reward": {"aggregate_reward": None},
        "next_actions": ["Retry the run and run provider preflight."],
    }

    report = build_trial_diagnosis(evidence)

    assert report["schema_version"] == "1"
    assert report["entity"] == {"type": "trial", "id": "trial-1"}
    assert report["summary"] == (
        "The trial failed before scoring because the provider gateway returned "
        "an error."
    )
    assert report["primary_cause"] == {
        "reason_code": "trial.gateway_error",
        "category": "gateway",
        "attribution": "provider",
        "confidence": "high",
        "affected_trials": 1,
        "affected_ratio": 1.0,
    }
    assert "not reliable" in report["impact"]
    assert any("trial.gateway_error" in item for item in report["evidence"])
    assert {
        "label": "Run provider preflight",
        "kind": "cli_command",
        "command": "loom providers models --preflight gpt-4o-mini",
    } in report["next_actions"]
    rendered = json.dumps(report)
    assert "loom_api_supersecret" not in rendered
    assert "loom-control-plane" not in rendered
    assert "X-Amz-Signature=secret" not in rendered


def test_batch_diagnosis_clusters_dominant_failure_reason() -> None:
    evidence = {
        "schema_version": "1",
        "entity": {"type": "batch", "id": "batch-1", "team_id": "team-a"},
        "lifecycle": {"state": "finished", "terminal_status": "all_failed"},
        "failure": {
            "reason_code": "batch.all_failed",
            "category": "aggregate",
            "attribution": "mixed",
            "message": "All child trials failed.",
        },
        "provider": {"provider_model_id": "qwen2.5-coder"},
        "task_selection": {"expected_trial_count": 4},
        "trials": {
            "summary": {
                "queued": 0,
                "claimed": 0,
                "running": 0,
                "succeeded": 0,
                "failed": 4,
                "cancelled": 0,
            },
            "failed_count": 4,
        },
        "reward": {"aggregate_reward": None, "scored_trial_count": 0},
        "next_actions": ["Open failed child trials and inspect their debug evidence."],
    }
    failures = [
        {
            "id": "trial-gw-1",
            "task_id": "task-1",
            "reason_code": "trial.gateway_error",
            "failure_reason": "gateway_error",
        },
        {
            "id": "trial-gw-2",
            "task_id": "task-2",
            "reason_code": "trial.gateway_error",
            "failure_reason": "gateway_error",
        },
        {
            "id": "trial-gw-3",
            "task_id": "task-3",
            "reason_code": "trial.gateway_error",
            "failure_reason": "gateway_error",
        },
        {
            "id": "trial-verifier-1",
            "task_id": "task-4",
            "reason_code": "trial.verifier_error",
            "failure_reason": "verifier_error",
        },
    ]

    report = build_batch_diagnosis(evidence, trial_failures=failures)

    assert report["summary"] == (
        "The batch failed because most failed child trials hit provider "
        "gateway errors before scoring."
    )
    assert report["primary_cause"]["reason_code"] == "trial.gateway_error"
    assert report["primary_cause"]["category"] == "gateway"
    assert report["primary_cause"]["attribution"] == "provider"
    assert report["primary_cause"]["confidence"] == "medium"
    assert report["primary_cause"]["affected_trials"] == 3
    assert report["primary_cause"]["affected_ratio"] == pytest.approx(0.75)
    assert report["reason_clusters"][0] == {
        "reason_code": "trial.gateway_error",
        "category": "gateway",
        "attribution": "provider",
        "count": 3,
        "affected_ratio": pytest.approx(0.75),
        "representative_trial_id": "trial-gw-1",
        "representative_task_id": "task-1",
    }
    assert "not reliable" in report["impact"]
    assert {
        "label": "Rerun failed trials after the provider path is healthy",
        "kind": "web_action",
        "action": "rerun_failed",
    } in report["next_actions"]


def test_batch_diagnosis_handles_fanout_failure_without_child_trials() -> None:
    evidence = {
        "schema_version": "1",
        "entity": {"type": "batch", "id": "batch-fanout", "team_id": "team-a"},
        "lifecycle": {"state": "finished", "terminal_status": "all_failed"},
        "failure": {
            "reason_code": "batch.fanout_submit_failed",
            "category": "submit",
            "attribution": "platform",
            "message": "task local/mit-0 submit failed",
        },
        "task_selection": {
            "expected_trial_count": 0,
            "fanout_errors": [{"task_id": "local/mit-0"}],
        },
        "trials": {"summary": {}, "failed_count": 0},
        "reward": {"aggregate_reward": None, "scored_trial_count": 0},
        "next_actions": ["Inspect batch fan-out errors."],
    }

    report = build_batch_diagnosis(evidence, trial_failures=[])

    assert report["summary"] == (
        "The batch could not submit child trials during fan-out."
    )
    assert report["primary_cause"] == {
        "reason_code": "batch.fanout_submit_failed",
        "category": "submit",
        "attribution": "platform",
        "confidence": "high",
        "affected_trials": 0,
        "affected_ratio": 0.0,
    }
    assert "not reliable" in report["impact"]
    assert {
        "label": "Inspect batch fan-out errors",
        "kind": "manual",
    } in report["next_actions"]
