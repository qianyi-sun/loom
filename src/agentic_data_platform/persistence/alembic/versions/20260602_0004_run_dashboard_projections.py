from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260602_0004"
down_revision = "20260529_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_dashboard_projections",
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), primary_key=True),
        sa.Column("project_id", sa.String(length=128), nullable=False),
        sa.Column("owner_team_name_snapshot", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("is_terminal", sa.Boolean(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("source_attempt_id", sa.String(length=160), nullable=True),
        sa.Column("source_event_seq", sa.Integer(), nullable=True),
        sa.Column("refresh_reason", sa.String(length=128), nullable=False),
        sa.Column("dirty", sa.Boolean(), nullable=False),
        sa.Column("error_reason", sa.Text(), nullable=True),
        sa.Column("refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_run_dashboard_projections_project_status",
        "run_dashboard_projections",
        ["project_id", "status", "updated_at"],
    )
    op.create_index(
        "ix_run_dashboard_projections_dirty_terminal",
        "run_dashboard_projections",
        ["dirty", "is_terminal", "updated_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_run_dashboard_projections_dirty_terminal", table_name="run_dashboard_projections")
    op.drop_index("ix_run_dashboard_projections_project_status", table_name="run_dashboard_projections")
    op.drop_table("run_dashboard_projections")
