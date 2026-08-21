"""Durable per-trial-attempt resource accounting.

Revision ID: 0107
Revises: 0106
"""

from alembic import op

revision = "0107"
down_revision = "0106"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE trial_resource_usage (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
          worker_id UUID NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
          lifecycle_authority_id UUID
            REFERENCES data_lifecycle_authorities(id) ON DELETE RESTRICT,
          attempt_count INTEGER NOT NULL,
          execution_key TEXT NOT NULL,
          runtime_id_hash TEXT,
          container_role TEXT NOT NULL,
          role_name TEXT NOT NULL,
          backend TEXT NOT NULL,
          architecture TEXT,
          candidate_sha TEXT,
          image_digest TEXT,
          source TEXT NOT NULL,
          observation_seq BIGINT NOT NULL,
          container_started_at TIMESTAMPTZ,
          first_observed_at TIMESTAMPTZ NOT NULL,
          last_observed_at TIMESTAMPTZ NOT NULL,
          finalized_at TIMESTAMPTZ,
          terminal_reason TEXT,
          completeness TEXT NOT NULL,
          diagnostic_code TEXT,
          cpu_limit_cores DOUBLE PRECISION,
          memory_limit_bytes BIGINT,
          pids_limit INTEGER,
          resource_profile TEXT,
          cpu_usage_usec BIGINT,
          cpu_user_usec BIGINT,
          cpu_system_usec BIGINT,
          cpu_throttled_usec BIGINT,
          cpu_periods BIGINT,
          cpu_throttled_periods BIGINT,
          memory_current_bytes BIGINT,
          memory_peak_bytes BIGINT,
          memory_events_low BIGINT,
          memory_events_high BIGINT,
          memory_events_max BIGINT,
          memory_events_oom BIGINT,
          memory_events_oom_kill BIGINT,
          pids_current BIGINT,
          pids_peak BIGINT,
          io_read_bytes BIGINT,
          io_write_bytes BIGINT,
          io_read_ops BIGINT,
          io_write_ops BIGINT,
          schema_version INTEGER NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT trial_resource_usage_attempt_check CHECK (attempt_count > 0),
          CONSTRAINT trial_resource_usage_execution_key_check
            CHECK (execution_key ~ '^[0-9a-f]{64}$'),
          CONSTRAINT trial_resource_usage_runtime_hash_check
            CHECK (runtime_id_hash IS NULL OR runtime_id_hash ~ '^[0-9a-f]{64}$'),
          CONSTRAINT trial_resource_usage_role_check
            CHECK (container_role IN ('agent','verifier','sidecar')),
          CONSTRAINT trial_resource_usage_source_check
            CHECK (source IN ('docker_stats','provider','unsupported')),
          CONSTRAINT trial_resource_usage_completeness_check
            CHECK (completeness IN ('complete','partial','unavailable')),
          CONSTRAINT trial_resource_usage_candidate_check
            CHECK (candidate_sha IS NULL OR candidate_sha ~ '^[0-9a-f]{40}$'),
          CONSTRAINT trial_resource_usage_image_check CHECK (
            image_digest IS NULL OR image_digest ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT trial_resource_usage_times_check
            CHECK (last_observed_at >= first_observed_at),
          CONSTRAINT trial_resource_usage_final_check CHECK (
            (finalized_at IS NULL AND completeness = 'partial')
            OR finalized_at IS NOT NULL
          ),
          CONSTRAINT trial_resource_usage_nonnegative_check CHECK (
            observation_seq >= 0
            AND (cpu_limit_cores IS NULL OR cpu_limit_cores > 0)
            AND (memory_limit_bytes IS NULL OR memory_limit_bytes > 0)
            AND (pids_limit IS NULL OR pids_limit > 0)
            AND (cpu_usage_usec IS NULL OR cpu_usage_usec >= 0)
            AND (cpu_user_usec IS NULL OR cpu_user_usec >= 0)
            AND (cpu_system_usec IS NULL OR cpu_system_usec >= 0)
            AND (cpu_throttled_usec IS NULL OR cpu_throttled_usec >= 0)
            AND (cpu_periods IS NULL OR cpu_periods >= 0)
            AND (cpu_throttled_periods IS NULL OR cpu_throttled_periods >= 0)
            AND (memory_current_bytes IS NULL OR memory_current_bytes >= 0)
            AND (memory_peak_bytes IS NULL OR memory_peak_bytes >= 0)
            AND (memory_events_low IS NULL OR memory_events_low >= 0)
            AND (memory_events_high IS NULL OR memory_events_high >= 0)
            AND (memory_events_max IS NULL OR memory_events_max >= 0)
            AND (memory_events_oom IS NULL OR memory_events_oom >= 0)
            AND (memory_events_oom_kill IS NULL OR memory_events_oom_kill >= 0)
            AND (pids_current IS NULL OR pids_current >= 0)
            AND (pids_peak IS NULL OR pids_peak >= 0)
            AND (io_read_bytes IS NULL OR io_read_bytes >= 0)
            AND (io_write_bytes IS NULL OR io_write_bytes >= 0)
            AND (io_read_ops IS NULL OR io_read_ops >= 0)
            AND (io_write_ops IS NULL OR io_write_ops >= 0)
          ),
          CONSTRAINT trial_resource_usage_identity_uidx
            UNIQUE (trial_id, attempt_count, execution_key)
        );
        CREATE INDEX trial_resource_usage_trial_attempt_idx
          ON trial_resource_usage (trial_id, attempt_count, container_role, role_name);
        CREATE INDEX trial_resource_usage_incomplete_idx
          ON trial_resource_usage (updated_at, trial_id)
          WHERE finalized_at IS NULL;
        CREATE INDEX trial_resource_usage_lifecycle_idx
          ON trial_resource_usage (lifecycle_authority_id)
          WHERE lifecycle_authority_id IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM trial_resource_usage) THEN
            RAISE EXCEPTION 'cannot downgrade 0107 with resource accounting data';
          END IF;
        END $$;
        DROP TABLE trial_resource_usage;
        """
    )
