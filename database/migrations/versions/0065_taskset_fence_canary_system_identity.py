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
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM teams
                 WHERE id = '{_SYSTEM_CANARY_TEAM_ID}'::uuid
                    OR name = '{_SYSTEM_CANARY_TEAM_NAME}'
            ) THEN
                RAISE EXCEPTION
                    'reserved TaskSet fence-canary Team identity already exists';
            END IF;

            INSERT INTO teams (id, name)
            VALUES (
                '{_SYSTEM_CANARY_TEAM_ID}'::uuid,
                '{_SYSTEM_CANARY_TEAM_NAME}'
            );

            INSERT INTO team_quotas (team_id)
            VALUES ('{_SYSTEM_CANARY_TEAM_ID}'::uuid)
            ON CONFLICT (team_id) DO NOTHING;

            ALTER TABLE task_set_fence_canary_authorizations
                DROP CONSTRAINT task_set_fence_canary_authorizations_image_tag_check;
            ALTER TABLE task_set_fence_canary_authorizations
                ADD CONSTRAINT task_set_fence_canary_authorizations_image_tag_check
                CHECK (
                    image_tag ~ '^staging(-[a-z0-9][a-z0-9_-]*)?-[0-9a-f]{{7}}$'
                );
        END
        $$;
        """
    )


def downgrade() -> None:
    """Restore 0064 without deleting or adopting deployment data."""
    op.execute(
        f"""
        DO $$
        DECLARE
            dependent_table regclass;
            dependent_column name;
            has_dependent_row boolean;
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM teams
                 WHERE id = '{_SYSTEM_CANARY_TEAM_ID}'::uuid
                    OR name = '{_SYSTEM_CANARY_TEAM_NAME}'
            ) THEN
                IF NOT EXISTS (
                    SELECT 1
                      FROM teams AS team
                      JOIN team_quotas AS quota
                        ON quota.team_id = team.id
                     WHERE team.id = '{_SYSTEM_CANARY_TEAM_ID}'::uuid
                       AND team.name = '{_SYSTEM_CANARY_TEAM_NAME}'
                       AND team.disabled_at IS NULL
                       AND team.disabled_reason IS NULL
                       AND team.submissions_paused_at IS NULL
                       AND team.submissions_paused_reason IS NULL
                       AND quota.fair_share_weight = 1.0
                       AND quota.max_attempts_ceiling = 3
                       AND quota.in_flight_count = 0
                       AND quota.license_allowlist = ARRAY[
                           'MIT', 'Apache-2.0', 'BSD-3-Clause', 'CC-BY-4.0'
                       ]::text[]
                       AND quota.taskset_max_count IS NULL
                       AND quota.taskset_max_storage_bytes IS NULL
                       AND quota.allow_private_endpoints IS FALSE
                ) THEN
                    RAISE EXCEPTION
                        'cannot downgrade TaskSet fence-canary system identity: Team is altered';
                END IF;

                FOR dependent_table, dependent_column IN
                    SELECT constraint_row.conrelid::regclass, attribute_row.attname
                      FROM pg_constraint AS constraint_row
                      JOIN LATERAL unnest(constraint_row.conkey) AS key_column(attnum)
                        ON TRUE
                      JOIN pg_attribute AS attribute_row
                        ON attribute_row.attrelid = constraint_row.conrelid
                       AND attribute_row.attnum = key_column.attnum
                     WHERE constraint_row.contype = 'f'
                       AND constraint_row.confrelid = 'teams'::regclass
                       AND constraint_row.conrelid <> 'team_quotas'::regclass
                LOOP
                    EXECUTE format(
                        'SELECT EXISTS (SELECT 1 FROM %s WHERE %I = $1)',
                        dependent_table,
                        dependent_column
                    )
                    INTO has_dependent_row
                    USING '{_SYSTEM_CANARY_TEAM_ID}'::uuid;

                    IF has_dependent_row THEN
                        RAISE EXCEPTION
                            'cannot downgrade TaskSet fence-canary system identity: references remain in %.%',
                            dependent_table,
                            dependent_column;
                    END IF;
                END LOOP;

                DELETE FROM team_quotas
                 WHERE team_id = '{_SYSTEM_CANARY_TEAM_ID}'::uuid;
                DELETE FROM teams
                 WHERE id = '{_SYSTEM_CANARY_TEAM_ID}'::uuid;
            END IF;

            IF EXISTS (
                SELECT 1
                  FROM task_set_fence_canary_authorizations
                 WHERE image_tag !~ '^staging-[0-9a-f]{{7}}$'
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade TaskSet fence-canary system identity: retry-tag authorization data remains';
            END IF;

            ALTER TABLE task_set_fence_canary_authorizations
                DROP CONSTRAINT task_set_fence_canary_authorizations_image_tag_check;
            ALTER TABLE task_set_fence_canary_authorizations
                ADD CONSTRAINT task_set_fence_canary_authorizations_image_tag_check
                CHECK (image_tag ~ '^staging-[0-9a-f]{{7}}$');
        END
        $$;
        """
    )
