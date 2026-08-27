"""Add evidence-gated execution resource calibration and forecast bindings.

Revision ID: 0120
Revises: 0119
"""

from alembic import op

revision = "0120"
down_revision = "0119"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE TABLE execution_resource_calibrations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          target_id TEXT NOT NULL REFERENCES execution_targets(id) ON DELETE RESTRICT,
          source_pool_id TEXT NOT NULL,
          source_architecture TEXT NOT NULL,
          resource_profile TEXT NOT NULL,
          candidate_sha TEXT NOT NULL,
          source_version TEXT NOT NULL,
          window_started_at TIMESTAMPTZ NOT NULL,
          window_stopped_at TIMESTAMPTZ NOT NULL,
          trial_attempts INTEGER NOT NULL,
          distinct_tasks INTEGER NOT NULL,
          usage_records INTEGER NOT NULL,
          incomplete_attempts INTEGER NOT NULL,
          evidence_duration_seconds BIGINT NOT NULL,
          peak_batch_concurrency INTEGER NOT NULL,
          throttled_attempts INTEGER NOT NULL,
          oom_attempts INTEGER NOT NULL,
          memory_limit_attempts INTEGER NOT NULL,
          eligible BOOLEAN NOT NULL,
          blockers_json JSONB NOT NULL,
          percentiles_json JSONB NOT NULL,
          recommended_cpu_millis INTEGER NOT NULL,
          recommended_memory_mib INTEGER NOT NULL,
          recommended_ephemeral_storage_mib INTEGER NOT NULL,
          recommended_pids INTEGER NOT NULL,
          evidence_json JSONB NOT NULL,
          evidence_sha256 TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_resource_calibrations_identity_check CHECK (
            length(trim(source_pool_id)) BETWEEN 1 AND 80
            AND source_architecture IN ('x86_64','arm64')
            AND length(trim(resource_profile)) BETWEEN 1 AND 120
            AND candidate_sha ~ '^[0-9a-f]{40}$'
            AND length(trim(source_version)) BETWEEN 1 AND 160
            AND window_stopped_at > window_started_at
          ),
          CONSTRAINT execution_resource_calibrations_counts_check CHECK (
            trial_attempts >= 0
            AND distinct_tasks >= 0
            AND usage_records >= 0
            AND incomplete_attempts >= 0
            AND evidence_duration_seconds >= 0
            AND peak_batch_concurrency >= 0
            AND throttled_attempts >= 0
            AND oom_attempts >= 0
            AND memory_limit_attempts >= 0
          ),
          CONSTRAINT execution_resource_calibrations_recommendation_check CHECK (
            recommended_cpu_millis > 0
            AND recommended_memory_mib > 0
            AND recommended_ephemeral_storage_mib > 0
            AND recommended_pids > 0
          ),
          CONSTRAINT execution_resource_calibrations_digest_check CHECK (
            evidence_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT execution_resource_calibrations_source_uidx UNIQUE (
            target_id, source_pool_id, resource_profile, candidate_sha, source_version
          ),
          CONSTRAINT execution_resource_calibrations_id_target_uidx UNIQUE (id, target_id)
        );
        CREATE INDEX execution_resource_calibrations_target_created_idx
          ON execution_resource_calibrations (target_id, created_at DESC, id);

        CREATE TABLE execution_resource_profile_bindings (
          target_id TEXT PRIMARY KEY REFERENCES execution_targets(id) ON DELETE RESTRICT,
          calibration_id UUID NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT false,
          reason TEXT,
          version BIGINT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_resource_profile_bindings_reason_check CHECK (
            (reason IS NULL OR length(trim(reason)) BETWEEN 1 AND 500)
            AND (NOT enabled OR reason IS NOT NULL)
          ),
          CONSTRAINT execution_resource_profile_bindings_version_check CHECK (version > 0),
          CONSTRAINT execution_resource_profile_bindings_calibration_fk
            FOREIGN KEY (calibration_id, target_id)
            REFERENCES execution_resource_calibrations(id, target_id) ON DELETE RESTRICT
        );

        CREATE FUNCTION loom_reject_execution_resource_calibration_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'execution resource calibration snapshots are immutable';
        END;
        $$;
        CREATE TRIGGER execution_resource_calibrations_immutable
          BEFORE UPDATE OR DELETE ON execution_resource_calibrations
          FOR EACH ROW EXECUTE FUNCTION loom_reject_execution_resource_calibration_mutation();

        CREATE FUNCTION loom_validate_execution_resource_profile_binding()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.enabled AND NEW.reason IS NULL THEN
            RAISE EXCEPTION 'enabled resource profile binding requires acceptance reason';
          END IF;
          IF NEW.enabled AND NOT EXISTS (
            SELECT 1 FROM execution_resource_calibrations c
             WHERE c.id = NEW.calibration_id
               AND c.target_id = NEW.target_id
               AND c.eligible
          ) THEN
            RAISE EXCEPTION 'enabled resource profile binding requires eligible calibration';
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_resource_profile_bindings_validate
          BEFORE INSERT OR UPDATE ON execution_resource_profile_bindings
          FOR EACH ROW EXECUTE FUNCTION loom_validate_execution_resource_profile_binding();
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM execution_resource_profile_bindings)
             OR EXISTS (SELECT 1 FROM execution_resource_calibrations) THEN
            RAISE EXCEPTION 'cannot downgrade 0120 with resource calibration evidence';
          END IF;
        END $$;
        DROP TRIGGER execution_resource_profile_bindings_validate
          ON execution_resource_profile_bindings;
        DROP FUNCTION loom_validate_execution_resource_profile_binding();
        DROP TRIGGER execution_resource_calibrations_immutable
          ON execution_resource_calibrations;
        DROP FUNCTION loom_reject_execution_resource_calibration_mutation();
        DROP TABLE execution_resource_profile_bindings;
        DROP TABLE execution_resource_calibrations;
        """
    )
