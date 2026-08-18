"""Add immutable Pipeline Artifact access classes.

Revision ID: 0099
Revises: 0098
"""

from alembic import op

revision = "0099"
down_revision = "0098"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE artifacts
          ADD COLUMN access_class TEXT NOT NULL DEFAULT 'team_runtime';

        ALTER TABLE artifacts
          ADD CONSTRAINT artifacts_access_class_check
          CHECK (access_class IN (
            'team_runtime', 'authoring_restricted', 'sanitized_audit'
          ));

        CREATE INDEX artifacts_team_access_class_idx
          ON artifacts (team_id, access_class, created_at, id);
        """
    )


def downgrade() -> None:
    # A binary that does not understand restricted authoring data must never
    # silently reinterpret it as ordinary team-visible content.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM artifacts WHERE access_class <> 'team_runtime'
          ) THEN
            RAISE EXCEPTION
              'cannot downgrade 0099 while non-team-runtime artifacts exist';
          END IF;
        END
        $$;

        DROP INDEX artifacts_team_access_class_idx;
        ALTER TABLE artifacts DROP CONSTRAINT artifacts_access_class_check;
        ALTER TABLE artifacts DROP COLUMN access_class;
        """
    )
