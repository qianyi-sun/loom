"""Reserve the deployment-owned TaskSet fence-canary Team.

Revision ID: 0065
Revises: 0064
Create Date: 2026-07-10

The canary must not discover a Team by mutable display name. This migration
reserves the fixed UUID/name pair and its quota row before any deployment can
prepare a one-use canary authorization. A conflicting pre-existing UUID or
name fails the upgrade rather than selecting an arbitrary Team.
"""

from __future__ import annotations

from alembic import op

revision = "0065"
down_revision = "0064"
branch_labels = None
depends_on = None

_SYSTEM_CANARY_TEAM_ID = "2c9506e1-7d5e-4b49-b532-4b8f0a3f5ea9"
_SYSTEM_CANARY_TEAM_NAME = "loom-system-taskset-fence-canary"


def upgrade() -> None:
    op.execute(
        f"""
        DO $$
        DECLARE
            existing_name text;
        BEGIN
            SELECT name
              INTO existing_name
              FROM teams
             WHERE id = '{_SYSTEM_CANARY_TEAM_ID}'::uuid
             FOR UPDATE;

            IF FOUND THEN
                IF existing_name <> '{_SYSTEM_CANARY_TEAM_NAME}' THEN
                    RAISE EXCEPTION
                        'reserved TaskSet fence-canary Team id has unexpected name';
                END IF;
            ELSE
                IF EXISTS (
                    SELECT 1
                      FROM teams
                     WHERE name = '{_SYSTEM_CANARY_TEAM_NAME}'
                ) THEN
                    RAISE EXCEPTION
                        'reserved TaskSet fence-canary Team name is already in use';
                END IF;

                INSERT INTO teams (id, name)
                VALUES (
                    '{_SYSTEM_CANARY_TEAM_ID}'::uuid,
                    '{_SYSTEM_CANARY_TEAM_NAME}'
                );
            END IF;

            INSERT INTO team_quotas (team_id)
            VALUES ('{_SYSTEM_CANARY_TEAM_ID}'::uuid)
            ON CONFLICT (team_id) DO NOTHING;
        END
        $$;
        """
    )


def downgrade() -> None:
    """Retain the reserved identity to prevent a downgrade from reusing it."""
