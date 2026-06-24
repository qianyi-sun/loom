"""track slurm worker job registry

Revision ID: 0038
Revises: 0037
Create Date: 2026-06-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0038"
down_revision = "0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "slurm_worker_jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("pool_name", sa.Text(), nullable=False),
        sa.Column("nodelist", sa.Text(), nullable=False),
        sa.Column("requested_cpus", sa.Integer(), nullable=True),
        sa.Column("requested_memory_mib", sa.Integer(), nullable=True),
        sa.Column("requested_concurrency", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Text(), nullable=True),
        sa.Column("slurm_state", sa.Text(), nullable=True),
        sa.Column(
            "state",
            sa.Text(),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column("pending_reason", sa.Text(), nullable=True),
        sa.Column("worker_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "redacted_env",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("submission_error", sa.Text(), nullable=True),
        sa.Column(
            "submitted_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "last_reconciled_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column("stale_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'stale')",
            name="slurm_worker_jobs_state_check",
        ),
        sa.CheckConstraint(
            "requested_cpus IS NULL OR requested_cpus > 0",
            name="slurm_worker_jobs_requested_cpus_positive_check",
        ),
        sa.CheckConstraint(
            "requested_memory_mib IS NULL OR requested_memory_mib > 0",
            name="slurm_worker_jobs_requested_memory_positive_check",
        ),
        sa.CheckConstraint(
            "requested_concurrency > 0",
            name="slurm_worker_jobs_requested_concurrency_positive_check",
        ),
        sa.ForeignKeyConstraint(
            ["worker_id"],
            ["workers.id"],
            name="slurm_worker_jobs_worker_id_fkey",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "slurm_worker_jobs_job_id_uidx",
        "slurm_worker_jobs",
        ["job_id"],
        unique=True,
        postgresql_where=sa.text("job_id IS NOT NULL"),
    )
    op.create_index(
        "slurm_worker_jobs_active_capacity_uidx",
        "slurm_worker_jobs",
        [
            "environment",
            "pool_name",
            "nodelist",
            sa.text("coalesce(requested_cpus, -1)"),
            sa.text("coalesce(requested_memory_mib, -1)"),
            "requested_concurrency",
        ],
        unique=True,
        postgresql_where=sa.text("state IN ('pending', 'running')"),
    )
    op.create_index(
        "slurm_worker_jobs_pool_state_idx",
        "slurm_worker_jobs",
        ["environment", "pool_name", "state"],
    )


def downgrade() -> None:
    op.drop_index("slurm_worker_jobs_pool_state_idx", table_name="slurm_worker_jobs")
    op.drop_index("slurm_worker_jobs_active_capacity_uidx", table_name="slurm_worker_jobs")
    op.drop_index("slurm_worker_jobs_job_id_uidx", table_name="slurm_worker_jobs")
    op.drop_table("slurm_worker_jobs")
