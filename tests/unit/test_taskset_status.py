"""Unit tests for TaskSet status aggregation (#242 sub-plan 3)."""

from __future__ import annotations

from loom.taskset.status import cap_error_summary, compute_task_set_status


def test_compute_task_set_status_ready() -> None:
    assert compute_task_set_status(materialized=10, skipped=0) == ("ready", None)


def test_compute_task_set_status_partial() -> None:
    assert compute_task_set_status(materialized=7, skipped=3) == ("partial", None)


def test_compute_task_set_status_majority_skipped() -> None:
    status, reason = compute_task_set_status(materialized=4, skipped=6)
    assert status == "failed"
    assert reason == "majority_skipped"


def test_compute_task_set_status_no_tasks() -> None:
    assert compute_task_set_status(materialized=0, skipped=5) == (
        "failed",
        "no_tasks_materialized",
    )


def test_cap_error_summary() -> None:
    errors = [{"row": str(i), "code": "x", "message": "m"} for i in range(60)]
    assert len(cap_error_summary(errors)) == 50
