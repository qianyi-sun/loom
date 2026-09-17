"""Preserve a renewable native builder waiting head without claiming an attempt.

Revision ID: 0149
Revises: 0148
"""
from alembic import op

revision = "0149"
down_revision = "0148"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE task_image_materializations, execution_targets "
               "IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    op.execute("""
        CREATE TABLE task_image_capacity_waits (
            target_id text PRIMARY KEY REFERENCES execution_targets(id) ON DELETE CASCADE,
            materialization_id uuid NOT NULL REFERENCES task_image_materializations(id) ON DELETE CASCADE,
            lease_epoch bigint NOT NULL,
            pool_id text NOT NULL,
            cpu_millis bigint NOT NULL,
            memory_mib bigint NOT NULL,
            storage_mib bigint NOT NULL,
            first_waited_at timestamptz NOT NULL,
            renewed_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            CONSTRAINT task_image_capacity_waits_materialization_key UNIQUE (materialization_id),
            CONSTRAINT task_image_capacity_waits_epoch_check CHECK (lease_epoch >= 0),
            CONSTRAINT task_image_capacity_waits_resources_check
                CHECK (cpu_millis > 0 AND memory_mib > 0 AND storage_mib > 0),
            CONSTRAINT task_image_capacity_waits_lifetime_check CHECK (
                expires_at > renewed_at AND renewed_at >= first_waited_at
                AND expires_at <= renewed_at + interval '120 seconds')
        )
    """)


def downgrade() -> None:
    # Removing the child also removes FK triggers from its parents. Acquire
    # those DROP-time locks explicitly so even a parent reader fails fast.
    op.execute("LOCK TABLE task_image_materializations, execution_targets, "
               "task_image_capacity_waits IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM task_image_capacity_waits WHERE expires_at > clock_timestamp()) THEN
            RAISE EXCEPTION 'native builder capacity waits must expire before downgrade';
        END IF;
    END $$""")
    op.drop_table("task_image_capacity_waits")
