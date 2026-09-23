"""Persist Slurm sandbox containment identity and exact resource requests."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0074"
down_revision = "0073"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "slurm_worker_jobs",
        sa.Column("requested_pids", sa.Integer(), nullable=True),
    )
    op.add_column(
        "slurm_worker_jobs",
        sa.Column("requested_gpu_tres", sa.Text(), nullable=True),
    )
    op.add_column(
        "slurm_worker_jobs",
        sa.Column(
            "requested_gpus",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "slurm_worker_jobs",
        sa.Column("sandbox_identity", sa.Text(), nullable=True),
    )
    op.add_column(
        "slurm_worker_jobs",
        sa.Column("candidate_sha", sa.String(length=40), nullable=True),
    )
    op.add_column(
        "slurm_worker_jobs",
        sa.Column("compose_project", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        "slurm_worker_jobs_requested_pids_positive_check",
        "slurm_worker_jobs",
        "requested_pids IS NULL OR requested_pids > 0",
    )
    op.create_check_constraint(
        "slurm_worker_jobs_requested_gpus_nonnegative_check",
        "slurm_worker_jobs",
        "requested_gpus >= 0",
    )
    op.create_check_constraint(
        "slurm_worker_jobs_candidate_sha_check",
        "slurm_worker_jobs",
        "candidate_sha IS NULL OR candidate_sha ~ '^[0-9a-f]{40}$'",
    )
    op.drop_index(
        "slurm_worker_jobs_active_capacity_uidx",
        table_name="slurm_worker_jobs",
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
            sa.text("coalesce(requested_pids, -1)"),
            sa.text("coalesce(requested_gpu_tres, '')"),
            "requested_gpus",
            "requested_concurrency",
        ],
        unique=True,
        postgresql_where=sa.text("state IN ('pending', 'running')"),
    )
    op.create_index(
        "slurm_worker_jobs_sandbox_candidate_state_idx",
        "slurm_worker_jobs",
        ["sandbox_identity", "candidate_sha", "state"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "slurm_worker_jobs_sandbox_candidate_state_idx",
        table_name="slurm_worker_jobs",
    )
    op.drop_constraint(
        "slurm_worker_jobs_candidate_sha_check",
        "slurm_worker_jobs",
        type_="check",
    )
    op.drop_constraint(
        "slurm_worker_jobs_requested_gpus_nonnegative_check",
        "slurm_worker_jobs",
        type_="check",
    )
    op.drop_constraint(
        "slurm_worker_jobs_requested_pids_positive_check",
        "slurm_worker_jobs",
        type_="check",
    )
    op.drop_index(
        "slurm_worker_jobs_active_capacity_uidx",
        table_name="slurm_worker_jobs",
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
    op.drop_column("slurm_worker_jobs", "compose_project")
    op.drop_column("slurm_worker_jobs", "candidate_sha")
    op.drop_column("slurm_worker_jobs", "sandbox_identity")
    op.drop_column("slurm_worker_jobs", "requested_gpus")
    op.drop_column("slurm_worker_jobs", "requested_gpu_tres")
    op.drop_column("slurm_worker_jobs", "requested_pids")
