"""Materialization job queue for user TaskSets (#242 sub-plan 2).

Sibling queue to ``trials`` — sub-plan 3 implements the consumer.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0053"
down_revision = "0052"
branch_labels = None
depends_on = None

_ACTIVE_STATES = ("queued", "claimed", "running")


def upgrade() -> None:
    op.create_table(
        "task_set_materialization_jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("task_set_id", sa.Text(), nullable=False),
        sa.Column("owning_team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "max_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("3"),
        ),
        sa.Column("next_attempt_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "enqueued_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("claimed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("failure_message", sa.Text(), nullable=True),
        sa.Column(
            "error_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["task_set_id"],
            ["task_sets.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["owning_team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_check_constraint(
        "task_set_materialization_jobs_state_check",
        "task_set_materialization_jobs",
        "state IN ('queued', 'claimed', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_index(
        "task_set_materialization_jobs_active_uidx",
        "task_set_materialization_jobs",
        ["task_set_id"],
        unique=True,
        postgresql_where=sa.text(
            "state IN ('queued', 'claimed', 'running')",
        ),
    )
    op.create_index(
        "task_set_materialization_jobs_queued_idx",
        "task_set_materialization_jobs",
        ["enqueued_at"],
        postgresql_where=sa.text("state = 'queued'"),
    )


def downgrade() -> None:
    op.drop_table("task_set_materialization_jobs")
