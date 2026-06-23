"""run library visibility and provenance (#336)

Revision ID: 0035
Revises: 0034
Create Date: 2026-06-22

Adds run-level visibility/share-state fields for the org-wide Run Library.
This does not add quota or rate-limit enforcement.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("batches", "trials"):
        op.add_column(
            table,
            sa.Column(
                "visibility",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'org'"),
            ),
        )
        op.add_column(
            table,
            sa.Column(
                "share_status",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'shared'"),
            ),
        )
        op.add_column(
            table,
            sa.Column(
                "source_provenance",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            ),
        )
        op.create_check_constraint(
            f"{table}_visibility_check",
            table,
            "visibility IN ('team', 'org', 'private')",
        )
        op.create_check_constraint(
            f"{table}_share_status_check",
            table,
            "share_status IN ('pending_scan', 'shared', 'blocked')",
        )


def downgrade() -> None:
    for table in ("trials", "batches"):
        op.drop_constraint(f"{table}_share_status_check", table, type_="check")
        op.drop_constraint(f"{table}_visibility_check", table, type_="check")
        op.drop_column(table, "source_provenance")
        op.drop_column(table, "share_status")
        op.drop_column(table, "visibility")
