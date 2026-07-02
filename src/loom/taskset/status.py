"""TaskSet status aggregation helpers (#242 sub-plan 3)."""

from __future__ import annotations


def compute_task_set_status(
    *,
    materialized: int,
    skipped: int,
) -> tuple[str, str | None]:
    """Return ``(status, status_reason)`` per user-brought-tasksets spec."""
    total = materialized + skipped
    if materialized == 0:
        return "failed", "no_tasks_materialized"
    if skipped == 0:
        return "ready", None
    if skipped / total <= 0.5:
        return "partial", None
    return "failed", "majority_skipped"


def cap_error_summary(errors: list[dict[str, str]], *, limit: int = 50) -> list[dict[str, str]]:
    return errors[:limit]
