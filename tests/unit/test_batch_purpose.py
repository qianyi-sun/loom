"""Unit tests for batch purpose validation helpers."""

from __future__ import annotations

from loom_service.batch_purpose import (
    validate_purpose_resolved_tasks,
    validate_purpose_task_filter,
    validate_purpose_trial_config,
)


def test_evaluation_rejects_skip_verifier() -> None:
    assert (
        validate_purpose_trial_config(
            "evaluation",
            {"skip_verifier": True},
        )
        == "evaluation_disallows_skip_verifier"
    )
    assert validate_purpose_trial_config("evaluation", {}) is None
    assert (
        validate_purpose_trial_config(
            "trajectory_generation",
            {"skip_verifier": True},
        )
        is None
    )


def test_evaluation_rejects_task_set_filter_keys() -> None:
    assert (
        validate_purpose_task_filter(
            "evaluation",
            {"task_set_id": "ts/team/x"},
        )
        == "evaluation_disallows_task_set"
    )
    assert (
        validate_purpose_task_filter(
            "evaluation",
            {"task_set_ids": ["ts/team/x"]},
        )
        == "evaluation_disallows_task_set"
    )
    assert (
        validate_purpose_task_filter(
            "evaluation",
            {"benchmark_id": "humaneval"},
        )
        is None
    )
    assert (
        validate_purpose_task_filter(
            "trajectory_generation",
            {"task_set_id": "ts/team/x"},
        )
        is None
    )


def test_evaluation_rejects_resolved_task_set_rows() -> None:
    assert (
        validate_purpose_resolved_tasks(
            "evaluation",
            task_set_ids=[None, "ts/team/x"],
        )
        == "evaluation_explicit_task_ids_must_be_benchmark"
    )
    assert (
        validate_purpose_resolved_tasks(
            "evaluation",
            task_set_ids=[None, None],
        )
        is None
    )
    assert (
        validate_purpose_resolved_tasks(
            "trajectory_generation",
            task_set_ids=["ts/team/x"],
        )
        is None
    )
