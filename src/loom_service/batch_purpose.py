"""Batch purpose validation (evaluation vs trajectory_generation)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

BatchPurpose = Literal["evaluation", "trajectory_generation"]

PURPOSE_EVALUATION: BatchPurpose = "evaluation"
PURPOSE_TRAJECTORY_GENERATION: BatchPurpose = "trajectory_generation"
BATCH_PURPOSES: frozenset[str] = frozenset(
    {PURPOSE_EVALUATION, PURPOSE_TRAJECTORY_GENERATION},
)


def task_filter_selects_task_set(task_filter: Mapping[str, Any]) -> bool:
    """True when the filter explicitly targets one or more TaskSets."""
    task_set_ids = task_filter.get("task_set_ids")
    if isinstance(task_set_ids, (list, tuple)) and any(
        isinstance(item, str) and item for item in task_set_ids
    ):
        return True
    task_set_id = task_filter.get("task_set_id")
    return isinstance(task_set_id, str) and bool(task_set_id)


def validate_purpose_trial_config(
    purpose: str,
    trial_config: Mapping[str, Any],
) -> str | None:
    """Return a machine reason if purpose conflicts with trial_config."""
    if purpose == PURPOSE_EVALUATION and bool(trial_config.get("skip_verifier")):
        return "evaluation_disallows_skip_verifier"
    return None


def validate_purpose_task_filter(
    purpose: str,
    task_filter: Mapping[str, Any],
) -> str | None:
    """Return a machine reason if purpose conflicts with task_filter keys."""
    if purpose == PURPOSE_EVALUATION and task_filter_selects_task_set(task_filter):
        return "evaluation_disallows_task_set"
    return None


def validate_purpose_resolved_tasks(
    purpose: str,
    *,
    task_set_ids: Sequence[str | None],
) -> str | None:
    """Return a machine reason if resolved tasks violate purpose rules.

    Evaluation may only run native benchmark tasks (``task_set_id`` NULL).
    """
    if purpose != PURPOSE_EVALUATION:
        return None
    if any(task_set_id is not None for task_set_id in task_set_ids):
        return "evaluation_explicit_task_ids_must_be_benchmark"
    return None


__all__ = [
    "BATCH_PURPOSES",
    "PURPOSE_EVALUATION",
    "PURPOSE_TRAJECTORY_GENERATION",
    "BatchPurpose",
    "task_filter_selects_task_set",
    "validate_purpose_resolved_tasks",
    "validate_purpose_task_filter",
    "validate_purpose_trial_config",
]
