"""drop workflows table + batches.workflow_id back-reference

Revision ID: 0012
Revises: 0011
Create Date: 2026-06-11

Workflows landed shortly before this migration, had no curl-driven
users yet, and the saved-recipe value prop was unvalidated. Per the
loom-spa-v3 spec, they were dropped entirely. The table goes; the
back-reference column on batches goes; the SPA pages, routes,
tests, and architecture doc all go in the same change.

If saved recipes return later, they get a cleaner design.

Downgrade path recreates the table + column from migration 0009
+ migration 0011's rename for completeness, but it's a one-way
trip in practice — once we've shipped this, no one should be
rolling back through it. The downgrade exists so alembic's
history is symmetric, not because it's expected to run.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Drop the batches → workflows back-reference first (FK on
    # batches.workflow_id) so the workflows table can drop cleanly.
    op.drop_index(
        "batches_workflow_id_idx", table_name="batches",
        if_exists=True,
    )
    op.drop_column("batches", "workflow_id")

    # Drop the partial unique index then the table itself.
    op.drop_index("workflows_name_unique", table_name="workflows")
    op.drop_table("workflows")


def downgrade() -> None:
    # Recreate the workflows table per migration 0009's schema.
    op.create_table(
        "workflows",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True), primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "benchmark_id", sa.Text(),
            sa.ForeignKey("benchmarks.id"), nullable=False,
        ),
        sa.Column("agent_name", sa.Text(), nullable=False),
        sa.Column("agent_version", sa.Text(), nullable=False),
        sa.Column("model_provider", sa.Text(), nullable=False),
        sa.Column("model_name", sa.Text(), nullable=False),
        sa.Column(
            "backend", sa.Text(), nullable=False,
            server_default=sa.text("'docker'"),
        ),
        sa.Column(
            "concurrency", sa.Integer(), nullable=False,
            server_default="1",
        ),
        sa.Column(
            "task_filter", postgresql.JSONB, nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "trial_config", postgresql.JSONB, nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
        sa.Column("created_by_token_prefix", sa.Text(), nullable=False),
        sa.Column(
            "deleted_at", sa.TIMESTAMP(timezone=True), nullable=True,
        ),
        # n_per_task was added in 0010.
        sa.Column(
            "n_per_task", sa.Integer(), nullable=False,
            server_default=sa.text("1"),
        ),
    )
    op.create_index(
        "workflows_name_unique", "workflows", ["name"],
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )

    # Recreate the batches → workflows back-reference column. The
    # column was originally added on the `campaigns` table in 0009
    # then renamed via 0011 — by the time we get here, the table
    # is already named batches, so we add directly to batches.
    op.add_column(
        "batches",
        sa.Column(
            "workflow_id", postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflows.id"), nullable=True,
        ),
    )
    op.create_index(
        "batches_workflow_id_idx", "batches", ["workflow_id"],
        postgresql_where=sa.text("workflow_id IS NOT NULL"),
    )
