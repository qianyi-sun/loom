from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260528_0002"
down_revision = "20260528_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "runs",
        sa.Column("created_by_user_id", sa.String(length=128), nullable=True),
    )
    if op.get_bind().dialect.name != "sqlite":
        op.create_foreign_key("fk_runs_created_by_user_id", "runs", "users", ["created_by_user_id"], ["user_id"])
    op.add_column("runs", sa.Column("evaluator_configs", sa.JSON(), nullable=True))
    op.create_table(
        "run_status_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(length=128), nullable=False, unique=True),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("attempt_id", sa.String(length=160), sa.ForeignKey("run_attempts.attempt_id"), nullable=True),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("from_status", sa.String(length=64), nullable=True),
        sa.Column("to_status", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("actor_user_id", sa.String(length=128), sa.ForeignKey("users.user_id"), nullable=True),
        sa.Column("request_id", sa.String(length=128), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_runs_lifecycle_filters", "runs", ["project_id", "status", "created_at"])
    op.create_index("ix_runs_benchmark_task_filters", "runs", ["benchmark_suite", "task_family", "task_instance_id"])
    op.create_index("ix_runs_created_by_user_id", "runs", ["created_by_user_id"])
    op.create_index("ix_run_status_events_run_created", "run_status_events", ["run_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_run_status_events_run_created", table_name="run_status_events")
    op.drop_index("ix_runs_created_by_user_id", table_name="runs")
    op.drop_index("ix_runs_benchmark_task_filters", table_name="runs")
    op.drop_index("ix_runs_lifecycle_filters", table_name="runs")
    op.drop_table("run_status_events")
    op.drop_column("runs", "evaluator_configs")
    if op.get_bind().dialect.name != "sqlite":
        op.drop_constraint("fk_runs_created_by_user_id", "runs", type_="foreignkey")
    op.drop_column("runs", "created_by_user_id")
