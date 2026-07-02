"""Unit tests for ``visible_task_sets`` (#242 sub-plan 1)."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy.dialects import postgresql

from loom.db.task_set_visibility import visible_task_sets


def _sql(statement: object, *, literal_binds: bool = False) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": literal_binds},
        ),
    )


def test_none_team_id_yields_empty_result() -> None:
    sql = _sql(visible_task_sets(team_id=None))
    assert "false()" in sql.lower() or "1 = 0" in sql or "false" in sql.lower()


def test_team_id_filters_by_owner_visibility_and_soft_delete() -> None:
    team_id = uuid4()
    sql = _sql(visible_task_sets(team_id=team_id), literal_binds=True)
    assert "task_sets.owning_team_id" in sql
    assert "task_sets.visibility" in sql
    assert "task_sets.soft_deleted_at" in sql
    assert str(team_id) in sql
    assert "'private'" in sql


def test_team_id_requires_soft_deleted_null() -> None:
    team_id = uuid4()
    sql = _sql(visible_task_sets(team_id=team_id))
    assert "IS NULL" in sql
