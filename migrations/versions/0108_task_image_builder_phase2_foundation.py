"""Add inert rootless builder grants and append-only publication evidence.

Revision ID: 0108
Revises: 0107
"""

from alembic import op

revision = "0108"
down_revision = "0107"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE TABLE task_image_build_grants (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          environment TEXT NOT NULL,
          provider TEXT NOT NULL,
          slurm_cluster_id TEXT NOT NULL,
          cpu_arch TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'issued',
          submitting_identity TEXT NOT NULL,
          slurm_account TEXT NOT NULL,
          slurm_partition TEXT NOT NULL,
          slurm_qos TEXT NOT NULL,
          request_spec JSONB NOT NULL,
          request_sha256 VARCHAR(64) NOT NULL,
          slurm_comment TEXT NOT NULL,
          ambiguity_settle_seconds INTEGER NOT NULL,
          ambiguity_settle_until TIMESTAMPTZ,
          invocation_started_at TIMESTAMPTZ,
          slurm_job_id TEXT,
          revoke_reason TEXT,
          journal_sequence INTEGER NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          bound_at TIMESTAMPTZ,
          released_at TIMESTAMPTZ,
          revoked_at TIMESTAMPTZ,
          CONSTRAINT task_image_build_grants_environment_check
            CHECK (environment ~ '^[A-Za-z0-9_.-]+$'),
          CONSTRAINT task_image_build_grants_provider_check
            CHECK (provider = 'slurm-rootless-v1'),
          CONSTRAINT task_image_build_grants_cluster_check
            CHECK (slurm_cluster_id IN ('oldlab','gb10')),
          CONSTRAINT task_image_build_grants_arch_check
            CHECK (cpu_arch IN ('x86_64','arm64')),
          CONSTRAINT task_image_build_grants_native_check
            CHECK (
              (slurm_cluster_id = 'oldlab' AND cpu_arch = 'x86_64'
                AND slurm_qos = 'loom-task-image-builder-rootless-oldlab')
              OR
              (slurm_cluster_id = 'gb10' AND cpu_arch = 'arm64'
                AND slurm_qos = 'loom-task-image-builder-rootless-gb10')
            ),
          CONSTRAINT task_image_build_grants_state_check
            CHECK (state IN ('issued','submitting','bound','released','revoked')),
          CONSTRAINT task_image_build_grants_identity_check
            CHECK (
              submitting_identity = 'loom-builder'
              AND slurm_account = 'loom-task-builder'
              AND slurm_partition = 'loom-task-builder'
            ),
          CONSTRAINT task_image_build_grants_request_digest_check
            CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
          CONSTRAINT task_image_build_grants_comment_check
            CHECK (
              slurm_comment = 'loom-task-builder-v1:grant=' || id::text
            ),
          CONSTRAINT task_image_build_grants_job_id_check
            CHECK (slurm_job_id IS NULL OR slurm_job_id ~ '^[0-9]+$'),
          CONSTRAINT task_image_build_grants_journal_check
            CHECK (journal_sequence >= 0),
          CONSTRAINT task_image_build_grants_settle_check
            CHECK (ambiguity_settle_seconds > 0),
          CONSTRAINT task_image_build_grants_state_fields_check CHECK (
            (state = 'issued'
              AND invocation_started_at IS NULL AND slurm_job_id IS NULL
              AND ambiguity_settle_until IS NULL
              AND bound_at IS NULL AND released_at IS NULL
              AND revoked_at IS NULL AND revoke_reason IS NULL)
            OR
            (state = 'submitting'
              AND invocation_started_at IS NOT NULL AND slurm_job_id IS NULL
              AND ambiguity_settle_until IS NOT NULL
              AND bound_at IS NULL AND released_at IS NULL
              AND revoked_at IS NULL AND revoke_reason IS NULL)
            OR
            (state = 'bound'
              AND invocation_started_at IS NOT NULL AND slurm_job_id IS NOT NULL
              AND ambiguity_settle_until IS NOT NULL
              AND bound_at IS NOT NULL AND released_at IS NULL
              AND revoked_at IS NULL AND revoke_reason IS NULL)
            OR
            (state = 'released'
              AND invocation_started_at IS NOT NULL AND slurm_job_id IS NOT NULL
              AND ambiguity_settle_until IS NOT NULL
              AND bound_at IS NOT NULL AND released_at IS NOT NULL
              AND revoked_at IS NULL AND revoke_reason IS NULL)
            OR
            (state = 'revoked'
              AND ambiguity_settle_until IS NOT NULL
              AND released_at IS NULL AND revoked_at IS NOT NULL
              AND revoke_reason IS NOT NULL)
          )
        );
        CREATE UNIQUE INDEX task_image_build_grants_comment_uidx
          ON task_image_build_grants (slurm_comment);
        CREATE UNIQUE INDEX task_image_build_grants_job_uidx
          ON task_image_build_grants (slurm_cluster_id, slurm_job_id)
          WHERE slurm_job_id IS NOT NULL;
        CREATE INDEX task_image_build_grants_reconcile_idx
          ON task_image_build_grants (environment, slurm_cluster_id, state, created_at);

        CREATE TABLE task_image_build_grant_events (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          grant_id UUID NOT NULL
            REFERENCES task_image_build_grants(id) ON DELETE RESTRICT,
          sequence INTEGER NOT NULL,
          event_type TEXT NOT NULL,
          payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT task_image_build_grant_events_sequence_check
            CHECK (sequence > 0),
          CONSTRAINT task_image_build_grant_events_type_check
            CHECK (event_type IN (
              'issued', 'submission_started', 'reconciliation_wait',
              'cancellation_requested', 'bound', 'released', 'revoked'
            )),
          CONSTRAINT task_image_build_grant_events_sequence_uidx
            UNIQUE (grant_id, sequence)
        );
        CREATE INDEX task_image_build_grant_events_created_idx
          ON task_image_build_grant_events (grant_id, created_at, sequence);

        CREATE TABLE task_image_materialization_attempts (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          materialization_id UUID NOT NULL
            REFERENCES task_image_materializations(id) ON DELETE RESTRICT,
          attempt_number INTEGER NOT NULL,
          lease_epoch BIGINT NOT NULL,
          builder_id VARCHAR(128) NOT NULL,
          claimed_at TIMESTAMPTZ NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT task_image_materialization_attempts_counters_check
            CHECK (attempt_number > 0 AND lease_epoch > 0),
          CONSTRAINT task_image_materialization_attempts_lease_uidx
            UNIQUE (materialization_id, lease_epoch),
          CONSTRAINT task_image_materialization_attempts_binding_uidx
            UNIQUE (
              id, materialization_id, attempt_number, lease_epoch, builder_id
            )
        );
        CREATE INDEX task_image_materialization_attempts_lookup_idx
          ON task_image_materialization_attempts
          (materialization_id, attempt_number, lease_epoch);

        INSERT INTO task_image_materialization_attempts (
          materialization_id, attempt_number, lease_epoch, builder_id, claimed_at
        )
        SELECT id, attempt_count, lease_epoch, claimed_by,
               coalesce(claimed_at, updated_at)
          FROM task_image_materializations
         WHERE state IN ('claimed','running')
           AND attempt_count > 0
           AND lease_epoch > 0
           AND claimed_by IS NOT NULL;

        CREATE TABLE task_image_publication_evidence (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          materialization_attempt_id UUID NOT NULL,
          materialization_id UUID NOT NULL,
          attempt_number INTEGER NOT NULL,
          lease_epoch BIGINT NOT NULL,
          builder_id VARCHAR(128) NOT NULL,
          component TEXT NOT NULL,
          registry_image TEXT NOT NULL,
          recorded_at TIMESTAMPTZ NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT task_image_publication_evidence_counters_check
            CHECK (attempt_number > 0 AND lease_epoch > 0),
          CONSTRAINT task_image_publication_evidence_component_check
            CHECK (length(component) BETWEEN 1 AND 256),
          CONSTRAINT task_image_publication_evidence_image_check
            CHECK (length(registry_image) BETWEEN 1 AND 2048),
          CONSTRAINT task_image_publication_evidence_attempt_fkey
            FOREIGN KEY (
              materialization_attempt_id, materialization_id,
              attempt_number, lease_epoch, builder_id
            ) REFERENCES task_image_materialization_attempts (
              id, materialization_id, attempt_number, lease_epoch, builder_id
            ) ON DELETE RESTRICT,
          CONSTRAINT task_image_publication_evidence_replay_uidx
            UNIQUE (materialization_attempt_id, component, registry_image)
        );
        CREATE INDEX task_image_publication_evidence_materialization_idx
          ON task_image_publication_evidence
          (materialization_id, attempt_number, lease_epoch, recorded_at);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE task_image_publication_evidence;
        DROP TABLE task_image_materialization_attempts;
        DROP TABLE task_image_build_grant_events;
        DROP TABLE task_image_build_grants;
        """
    )
