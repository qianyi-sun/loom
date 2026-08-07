from __future__ import annotations

from typing import Any

import pytest
from scripts.ops.authoritative_gate_metrics import MetricsError
from scripts.ops.ci_reliability_metrics import summarize_reliability


def _job(
    name: str,
    conclusion: str,
    *,
    runner: str = "GitHub Actions 1",
    queued: int = 3,
) -> dict[str, Any]:
    return {
        "name": name,
        "conclusion": conclusion,
        "created_at": "2026-08-07T12:00:00Z",
        "started_at": f"2026-08-07T12:00:{queued:02d}Z",
        "runner_name": runner,
        "labels": ["ubuntu-latest"],
    }


def _payload(reason: str = "platform_transient") -> dict[str, Any]:
    return {
        "repository": "qianyi-sun/loom",
        "attempts": [
            {
                "workflow": "CI",
                "run_id": 42,
                "attempt": 1,
                "jobs": [_job("lint-and-static", "failure", queued=4)],
            },
            {
                "workflow": "CI",
                "run_id": 42,
                "attempt": 2,
                "jobs": [_job("lint-and-static", "success", queued=2)],
            },
            {
                "workflow": "images",
                "run_id": 43,
                "attempt": 1,
                "jobs": [_job("build (linux/amd64)", "success", runner="oldlab5-kvm-image-1")],
            },
        ],
        "classifications": [
            {
                "run_id": 42,
                "failed_attempt": 1,
                "reason": reason,
                "evidence_url": "https://github.com/qianyi-sun/loom/issues/1130",
            },
        ],
    }


def test_governed_retry_flake_queue_and_causes_are_segmented() -> None:
    summary = summarize_reliability(_payload(), require_governance=True, minimum_runs=2)

    assert summary["status"] == "pass"
    assert summary["runs"] == 2
    assert summary["attempts"] == 3
    assert summary["retries"] == 1
    assert summary["retried_runs"] == 1
    assert summary["retry_rate"] == 0.5
    assert summary["retry_attempt_rate"] == pytest.approx(1 / 3, abs=0.0001)
    assert summary["flakes"] == 1
    assert summary["flake_rate"] == 0.5
    assert summary["terminal_causes"] == {"platform_transient": 1, "success": 2}
    assert summary["by_workflow"]["CI"]["queue_seconds"] == {"average": 3.0, "max": 4}
    assert summary["by_runner_class"]["oldlab5"]["jobs"] == 1
    assert "CI/lint-and-static" in summary["by_job"]


def test_code_failure_retry_is_a_policy_violation() -> None:
    summary = summarize_reliability(_payload("code_failure"), require_governance=True)

    assert summary["status"] == "fail"
    assert summary["policy_violations"] == ["run 42 retried deterministic code failure"]


def test_missing_or_untrusted_classification_fails_closed() -> None:
    payload = _payload()
    payload["classifications"] = []
    with pytest.raises(MetricsError, match="lacks terminal classification"):
        summarize_reliability(payload)

    payload = _payload()
    payload["classifications"][0]["evidence_url"] = "https://example.com/incident"
    with pytest.raises(MetricsError, match="unique, governed, and evidenced"):
        summarize_reliability(payload)


def test_non_contiguous_attempts_fail_closed() -> None:
    payload = _payload()
    payload["attempts"][1]["attempt"] = 3
    with pytest.raises(MetricsError, match="contiguous"):
        summarize_reliability(payload)


def test_acceptance_requires_sample_floor_and_observed_retry() -> None:
    payload = _payload()
    payload["attempts"] = [payload["attempts"][2]]
    payload["classifications"] = []

    summary = summarize_reliability(payload, require_governance=True, minimum_runs=30)

    assert summary["status"] == "fail"
    assert "sample has 1 runs; minimum is 30" in summary["policy_violations"]
    assert "acceptance sample contains no governed retry" in summary["policy_violations"]
