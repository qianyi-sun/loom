"""Normalize exact GC resume evidence into journal items.

Revision ID: 0068
Revises: 0067
Create Date: 2026-07-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0068"
down_revision = "0067"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "data_lifecycle_gc_items",
        sa.Column("authority_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column("data_lifecycle_gc_items", sa.Column("bucket", sa.Text(), nullable=True))
    op.add_column("data_lifecycle_gc_items", sa.Column("object_key", sa.Text(), nullable=True))
    op.add_column("data_lifecycle_gc_items", sa.Column("version_id", sa.Text(), nullable=True))
    op.add_column("data_lifecycle_gc_items", sa.Column("content_sha256", sa.Text(), nullable=True))
    op.add_column(
        "data_lifecycle_gc_items", sa.Column("size_bytes", sa.BigInteger(), nullable=True)
    )
    op.create_check_constraint(
        "data_lifecycle_gc_items_exact_evidence_check",
        "data_lifecycle_gc_items",
        "(authority_id IS NULL AND bucket IS NULL AND object_key IS NULL "
        "AND version_id IS NULL AND content_sha256 IS NULL AND size_bytes IS NULL) "
        "OR (authority_id IS NOT NULL AND bucket IS NOT NULL AND bucket <> '' "
        "AND object_key IS NOT NULL AND object_key <> '' "
        "AND size_bytes IS NOT NULL AND size_bytes >= 0 "
        "AND (content_sha256 IS NULL OR content_sha256 ~ '^[0-9a-f]{64}$'))",
    )
    op.create_table(
        "data_lifecycle_gc_authorities",
        sa.Column(
            "gc_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("data_lifecycle_gc_runs.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "authority_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column(
            "deletion_token",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("data_lifecycle_gc_authorities")
    op.drop_constraint(
        "data_lifecycle_gc_items_exact_evidence_check",
        "data_lifecycle_gc_items",
        type_="check",
    )
    for column in (
        "size_bytes",
        "content_sha256",
        "version_id",
        "object_key",
        "bucket",
        "authority_id",
    ):
        op.drop_column("data_lifecycle_gc_items", column)
