"""Team-scoped TaskSet visibility helper (#242 sub-plan 1)."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import Select, and_, false, select

from loom.db.schema import TaskSet


def visible_task_sets(*, team_id: UUID | None) -> Select[tuple[TaskSet]]:
    """Return TaskSets visible to the given team.

    v1 is team-private: only non-soft-deleted rows owned by ``team_id``
    with ``visibility='private'`` are visible. ``team_id=None`` yields an
    empty result set.
    """
    if team_id is None:
        return select(TaskSet).where(false())
    return select(TaskSet).where(
        and_(
            TaskSet.owning_team_id == team_id,
            TaskSet.visibility == "private",
            TaskSet.soft_deleted_at.is_(None),
        )
    )
