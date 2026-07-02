"""Link materialized tasks to user TaskSets (#242 sub-plan 3).

Adds nullable ``tasks.task_set_id`` FK. User TaskSet tasks must not
also carry ``benchmark_id``.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("task_set_id", sa.Text(), nullable=True))
    op.create_foreign_key(
        "tasks_task_set_id_fkey",
        "tasks",
        "task_sets",
        ["task_set_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("tasks_task_set_id_idx", "tasks", ["task_set_id"])
    op.create_check_constraint(
        "tasks_benchmark_or_taskset_check",
        "tasks",
        "task_set_id IS NULL OR benchmark_id IS NULL",
    )


def downgrade() -> None:
    op.drop_constraint("tasks_benchmark_or_taskset_check", "tasks", type_="check")
    op.drop_index("tasks_task_set_id_idx", table_name="tasks")
    op.drop_constraint("tasks_task_set_id_fkey", "tasks", type_="foreignkey")
    op.drop_column("tasks", "task_set_id")
