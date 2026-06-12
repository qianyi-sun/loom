"""rename campaigns → batches; trials.campaign_id → trials.batch_id

Revision ID: 0011
Revises: 0010
Create Date: 2026-06-11

User audit: "I don't want to confuse users or us." Campaign is the
batch-submission concept (atomic submit, cancel-all, progress
aggregation, runner idempotency) but the name was inherited from a
Loom-internal v0.7 spec, not from Harbor — Harbor doesn't have it.
Rename to `Batch` everywhere so the SPA, docs, code, and DB all
speak the same word.

Pure metadata rename. No data movement. No downtime — Postgres
DDL on these RENAMEs is fast (locks briefly, no rewrite).

This migration ALSO renames:
- The `state` check constraint (campaigns_state_chk → batches_state_chk)
- The FK from trials → campaigns (trials_campaign_fk → trials_batch_fk)
- All campaign-named indexes
- The workflows.campaign_id back-reference column added in 0009
  becomes workflows.batch_id (cleanup happens in 0012 when workflows
  is dropped, but the column rename keeps the schema introspectable
  until then)
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Table rename
    op.rename_table("campaigns", "batches")

    # Column rename on trials
    op.alter_column("trials", "campaign_id", new_column_name="batch_id")

    # Index renames on batches
    op.execute("ALTER INDEX campaigns_team_idx RENAME TO batches_team_idx")
    op.execute("ALTER INDEX campaigns_state_idx RENAME TO batches_state_idx")

    # Index renames on trials
    op.execute("ALTER INDEX trials_campaign_idx RENAME TO trials_batch_idx")
    # Migration 0009 added this index for the workflow → campaign back-ref.
    op.execute(
        "ALTER INDEX IF EXISTS campaigns_workflow_id_idx "
        "RENAME TO batches_workflow_id_idx",
    )

    # FK rename
    op.execute(
        "ALTER TABLE trials RENAME CONSTRAINT trials_campaign_fk "
        "TO trials_batch_fk",
    )

    # Check constraint rename
    op.execute(
        "ALTER TABLE batches RENAME CONSTRAINT campaigns_state_chk "
        "TO batches_state_chk",
    )

    # Primary key rename
    op.execute("ALTER INDEX campaigns_pkey RENAME TO batches_pkey")


def downgrade() -> None:
    # Reverse the renames in reverse order.
    op.execute("ALTER INDEX batches_pkey RENAME TO campaigns_pkey")
    op.execute(
        "ALTER TABLE batches RENAME CONSTRAINT batches_state_chk "
        "TO campaigns_state_chk",
    )
    op.execute(
        "ALTER TABLE trials RENAME CONSTRAINT trials_batch_fk "
        "TO trials_campaign_fk",
    )
    op.execute(
        "ALTER INDEX IF EXISTS batches_workflow_id_idx "
        "RENAME TO campaigns_workflow_id_idx",
    )
    op.execute("ALTER INDEX trials_batch_idx RENAME TO trials_campaign_idx")
    op.execute("ALTER INDEX batches_state_idx RENAME TO campaigns_state_idx")
    op.execute("ALTER INDEX batches_team_idx RENAME TO campaigns_team_idx")
    op.alter_column("trials", "batch_id", new_column_name="campaign_id")
    op.rename_table("batches", "campaigns")
