"""Add durable task-image materialization prerequisites.

Revision ID: 0096
Revises: 0095
"""

from alembic import op

revision = "0096"
down_revision = "0095"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE task_image_materializations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          materialization_key VARCHAR(64) NOT NULL,
          task_id TEXT NOT NULL,
          task_checksum VARCHAR(64) NOT NULL,
          cpu_arch VARCHAR(16) NOT NULL,
          task_config JSONB NOT NULL,
          task_source TEXT,
          task_source_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
          state TEXT NOT NULL DEFAULT 'queued',
          attempt_count INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 3,
          next_attempt_at TIMESTAMPTZ,
          claimed_by VARCHAR(128),
          lease_epoch BIGINT NOT NULL DEFAULT 0,
          lease_expires_at TIMESTAMPTZ,
          registry_images JSONB NOT NULL DEFAULT '{}'::jsonb,
          failure_reason TEXT,
          failure_message TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          claimed_at TIMESTAMPTZ,
          started_at TIMESTAMPTZ,
          ready_at TIMESTAMPTZ,
          finished_at TIMESTAMPTZ,
          last_referenced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          unreferenced_at TIMESTAMPTZ,
          CONSTRAINT task_image_materializations_key_check
            CHECK (materialization_key ~ '^[0-9a-f]{64}$'),
          CONSTRAINT task_image_materializations_checksum_check
            CHECK (task_checksum ~ '^[0-9a-f]{64}$'),
          CONSTRAINT task_image_materializations_cpu_arch_check
            CHECK (cpu_arch IN ('x86_64', 'arm64')),
          CONSTRAINT task_image_materializations_state_check
            CHECK (state IN ('queued', 'claimed', 'running', 'ready', 'failed')),
          CONSTRAINT task_image_materializations_counters_check
            CHECK (attempt_count >= 0 AND max_attempts > 0 AND lease_epoch >= 0),
          CONSTRAINT task_image_materializations_key_uidx UNIQUE (materialization_key),
          CONSTRAINT task_image_materializations_task_arch_uidx
            UNIQUE (task_id, task_checksum, cpu_arch)
        );

        CREATE INDEX task_image_materializations_queue_idx
          ON task_image_materializations
          (cpu_arch, state, next_attempt_at, created_at);
        CREATE INDEX task_image_materializations_reference_idx
          ON task_image_materializations (task_id, task_checksum);

        CREATE TABLE trial_task_image_materializations (
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
          materialization_id UUID NOT NULL
            REFERENCES task_image_materializations(id) ON DELETE RESTRICT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT trial_task_image_materializations_pkey
            PRIMARY KEY (trial_id, materialization_id)
        );

        CREATE INDEX trial_task_image_materializations_materialization_idx
          ON trial_task_image_materializations (materialization_id);
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TABLE trial_task_image_materializations; DROP TABLE task_image_materializations"
    )
