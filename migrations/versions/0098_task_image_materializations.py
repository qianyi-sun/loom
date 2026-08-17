"""Add durable task-image materialization prerequisites.

Revision ID: 0098
Revises: 0097
"""

import hashlib
from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0098"
down_revision = "0097"
branch_labels = None
depends_on = None

_KEY_DOMAIN = "task-image-materialization-v1"


def _has_dockerfile(task_config: dict[str, object]) -> bool:
    environment = task_config.get("environment")
    if not isinstance(environment, dict):
        return False
    if environment.get("dockerfile") is not None:
        return True
    sidecars = environment.get("sidecars", [])
    return isinstance(sidecars, list) and any(
        isinstance(sidecar, dict) and sidecar.get("dockerfile") is not None
        for sidecar in sidecars
    )


def _architectures(task_config: dict[str, object]) -> tuple[str, ...]:
    environment = task_config.get("environment")
    cpu_arch = environment.get("cpu_arch", "x86_64") if isinstance(environment, dict) else None
    if cpu_arch == "any":
        return ("x86_64", "arm64")
    if cpu_arch in {"x86_64", "arm64"}:
        return (str(cpu_arch),)
    return ()


def _materialization_key(*, task_id: str, task_checksum: str, cpu_arch: str) -> str:
    material = "\0".join((_KEY_DOMAIN, task_id, task_checksum, cpu_arch))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


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
          registry_image_history JSONB NOT NULL DEFAULT '[]'::jsonb,
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
            CHECK (state IN (
              'queued', 'claimed', 'running', 'ready', 'failed',
              'retiring', 'retired'
            )),
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
        CREATE INDEX task_image_materializations_registry_gc_idx
          ON task_image_materializations
          (state, unreferenced_at, lease_expires_at);

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
    connection = op.get_bind()
    insert_materialization = sa.text(
        """
        INSERT INTO task_image_materializations (
          id, materialization_key, task_id, task_checksum, cpu_arch,
          task_config, task_source, task_source_provenance, state
        ) VALUES (
          :id, :materialization_key, :task_id, :task_checksum, :cpu_arch,
          :task_config, :task_source, :task_source_provenance, 'queued'
        )
        ON CONFLICT (materialization_key) DO NOTHING
        """
    ).bindparams(
        sa.bindparam("task_config", type_=JSONB),
        sa.bindparam("task_source_provenance", type_=JSONB),
    )
    tasks = connection.execute(
        sa.text(
            "SELECT id, checksum, config, source, source_provenance FROM tasks ORDER BY id"
        )
    ).mappings()
    for task in tasks:
        task_config = task["config"]
        if not isinstance(task_config, dict) or not _has_dockerfile(task_config):
            continue
        task_checksum = str(task["checksum"]).removeprefix("sha256:")
        if len(task_checksum) != 64:
            continue
        for cpu_arch in _architectures(task_config):
            connection.execute(
                insert_materialization,
                {
                    "id": uuid4(),
                    "materialization_key": _materialization_key(
                        task_id=str(task["id"]),
                        task_checksum=task_checksum,
                        cpu_arch=cpu_arch,
                    ),
                    "task_id": task["id"],
                    "task_checksum": task_checksum,
                    "cpu_arch": cpu_arch,
                    "task_config": task_config,
                    "task_source": task["source"],
                    "task_source_provenance": task["source_provenance"] or {},
                },
            )
    op.execute(
        """
        INSERT INTO trial_task_image_materializations (trial_id, materialization_id)
        SELECT trial.id, materialization.id
          FROM trials trial
          JOIN tasks task ON task.id = trial.task_id
          JOIN task_image_materializations materialization
            ON materialization.task_id = task.id
           AND materialization.task_checksum =
               regexp_replace(task.checksum, '^sha256:', '')
         WHERE trial.state NOT IN ('succeeded', 'failed', 'cancelled')
        ON CONFLICT (trial_id, materialization_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TABLE trial_task_image_materializations; DROP TABLE task_image_materializations"
    )
