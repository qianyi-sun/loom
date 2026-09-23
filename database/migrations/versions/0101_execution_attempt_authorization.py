"""Persist immutable execution authorization snapshots on Attempts.

Revision ID: 0101
Revises: 0100
"""

from alembic import op

revision = "0101"
down_revision = "0100"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE execution_attempts
          ADD COLUMN execution_authorization_json JSONB,
          ADD COLUMN execution_authorization_bytes BYTEA,
          ADD COLUMN execution_authorization_digest TEXT,
          ADD CONSTRAINT execution_attempts_authorization_group_check
          CHECK (
            (execution_authorization_json IS NULL
             AND execution_authorization_bytes IS NULL
             AND execution_authorization_digest IS NULL)
            OR
            (execution_authorization_json IS NOT NULL
             AND execution_authorization_bytes IS NOT NULL
             AND execution_authorization_digest
                 ~ '^sha256:[0-9a-f]{64}$')
          );
        """
    )


def downgrade() -> None:
    # Authorization bytes are the recovery identity for an already-created
    # Attempt. Refuse a lossy rollback once any snapshot has been frozen.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
              FROM execution_attempts
             WHERE execution_authorization_json IS NOT NULL
                OR execution_authorization_bytes IS NOT NULL
                OR execution_authorization_digest IS NOT NULL
          ) THEN
            RAISE EXCEPTION
              'cannot downgrade 0101 while execution authorization snapshots exist';
          END IF;
        END
        $$;

        ALTER TABLE execution_attempts
          DROP CONSTRAINT execution_attempts_authorization_group_check,
          DROP COLUMN execution_authorization_digest,
          DROP COLUMN execution_authorization_bytes,
          DROP COLUMN execution_authorization_json;
        """
    )
