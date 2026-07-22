"""Add freshness-bound staging lifecycle capacity authority.

Revision ID: 0069
Revises: 0068
Create Date: 2026-07-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0069"
down_revision = "0068"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "staging_lifecycle_capacity",
        sa.Column("environment", sa.Text(), primary_key=True),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("object_count", sa.BigInteger(), nullable=False),
        sa.Column("bytes_used", sa.BigInteger(), nullable=False),
        sa.Column("disk_free_percent", sa.Integer(), nullable=False),
        sa.Column("inode_free_percent", sa.Integer(), nullable=False),
        sa.Column("policy_sha256", sa.Text(), nullable=False),
        sa.Column("evidence_sha256", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("observed_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint("environment = 'staging'", name="staging_lifecycle_capacity_env_check"),
        sa.CheckConstraint(
            "namespace <> '' AND source = 'exact-object-inventory-v1'",
            name="staging_lifecycle_capacity_identity_check",
        ),
        sa.CheckConstraint(
            "object_count >= 0 AND bytes_used >= 0",
            name="staging_lifecycle_capacity_counters_check",
        ),
        sa.CheckConstraint(
            "disk_free_percent BETWEEN 0 AND 100 AND inode_free_percent BETWEEN 0 AND 100",
            name="staging_lifecycle_capacity_percent_check",
        ),
        sa.CheckConstraint(
            "policy_sha256 ~ '^[0-9a-f]{64}$' AND evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="staging_lifecycle_capacity_digest_check",
        ),
    )


def downgrade() -> None:
    op.drop_table("staging_lifecycle_capacity")
