"""Persist one-use deployment authority for the TaskSet fence canary.

Revision ID: 0064
Revises: 0063
Create Date: 2026-07-10

The record contains only the candidate/TaskSet/job/checksum binding and a
SHA-256 digest of a per-launch nonce.  It contains neither a bearer token nor
TaskSet inputs, generated output, owner identity, object-store location, or
user-facing control state.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0064"
down_revision = "0063"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_set_fence_canary_authorizations",
        sa.Column("task_set_id", sa.Text(), nullable=False),
        sa.Column("materialization_job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_sha", sa.String(length=40), nullable=False),
        sa.Column("image_tag", sa.String(length=64), nullable=False),
        sa.Column("expected_task_checksum", sa.String(length=64), nullable=False),
        sa.Column("nonce_digest", sa.LargeBinary(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("consumed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("consumed_lease_epoch", sa.Integer(), nullable=True),
        sa.CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{40}$'",
            name="task_set_fence_canary_authorizations_candidate_sha_check",
        ),
        sa.CheckConstraint(
            "image_tag ~ '^staging-[0-9a-f]{7}$'",
            name="task_set_fence_canary_authorizations_image_tag_check",
        ),
        sa.CheckConstraint(
            "expected_task_checksum ~ '^[0-9a-f]{64}$'",
            name="task_set_fence_canary_authorizations_checksum_check",
        ),
        sa.CheckConstraint(
            "octet_length(nonce_digest) = 32",
            name="task_set_fence_canary_authorizations_nonce_digest_check",
        ),
        sa.CheckConstraint(
            "(consumed_at IS NULL AND consumed_lease_epoch IS NULL) OR "
            "(consumed_at IS NOT NULL AND consumed_lease_epoch > 0)",
            name="task_set_fence_canary_authorizations_consumption_check",
        ),
        sa.ForeignKeyConstraint(
            ["task_set_id"],
            ["task_sets.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["materialization_job_id"],
            ["task_set_materialization_jobs.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("task_set_id"),
        sa.UniqueConstraint(
            "materialization_job_id",
            name="task_set_fence_canary_authorizations_job_uidx",
        ),
    )


def downgrade() -> None:
    op.drop_table("task_set_fence_canary_authorizations")
