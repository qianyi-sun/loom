"""Persist the complete fan-out item authority on every child StageRun.

Revision ID: 0100
Revises: 0099
"""

from alembic import op

revision = "0100"
down_revision = "0099"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing child rows cannot be reconstructed from the digest and
    # parameters alone. Refuse an online lossy upgrade instead of inventing
    # authority for already-expanded work.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pipeline_stage_runs WHERE fanout_expansion_id IS NOT NULL
          ) THEN
            RAISE EXCEPTION
              'cannot upgrade 0100 while legacy fanout child StageRuns exist';
          END IF;
        END
        $$;

        ALTER TABLE pipeline_stage_runs
          DROP CONSTRAINT pipeline_stage_runs_fanout_group_check;

        ALTER TABLE pipeline_stage_runs
          ADD COLUMN fanout_item_json JSONB;

        ALTER TABLE pipeline_stage_runs
          ADD CONSTRAINT pipeline_stage_runs_fanout_group_check
          CHECK (
            (fanout_expansion_id IS NULL AND fanout_parameters_json IS NULL
             AND fanout_item_json IS NULL AND fanout_item_digest IS NULL)
            OR
            (fanout_expansion_id IS NOT NULL AND fanout_parameters_json IS NOT NULL
             AND fanout_item_json IS NOT NULL AND fanout_item_digest IS NOT NULL)
          );
        """
    )


def downgrade() -> None:
    # Once new child rows exist, dropping fanout_item_json would make their
    # frozen input authority unrecoverable.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pipeline_stage_runs WHERE fanout_expansion_id IS NOT NULL
          ) THEN
            RAISE EXCEPTION
              'cannot downgrade 0100 while fanout child StageRuns exist';
          END IF;
        END
        $$;

        ALTER TABLE pipeline_stage_runs
          DROP CONSTRAINT pipeline_stage_runs_fanout_group_check;
        ALTER TABLE pipeline_stage_runs DROP COLUMN fanout_item_json;
        ALTER TABLE pipeline_stage_runs
          ADD CONSTRAINT pipeline_stage_runs_fanout_group_check
          CHECK (
            (fanout_expansion_id IS NULL AND fanout_parameters_json IS NULL
             AND fanout_item_digest IS NULL)
            OR
            (fanout_expansion_id IS NOT NULL AND fanout_parameters_json IS NOT NULL
             AND fanout_item_digest IS NOT NULL)
          );
        """
    )
