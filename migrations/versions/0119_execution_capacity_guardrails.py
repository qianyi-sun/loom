"""Add Nebius quota observations and provisioning authorizations.

Revision ID: 0119
Revises: 0118
"""

from alembic import op

revision = "0119"
down_revision = "0118"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE TABLE execution_capacity_policies (
          target_id TEXT PRIMARY KEY REFERENCES execution_targets(id) ON DELETE RESTRICT,
          enabled BOOLEAN NOT NULL DEFAULT false,
          max_nodes INTEGER NOT NULL,
          max_vcpu_millis BIGINT NOT NULL,
          max_memory_mib BIGINT NOT NULL,
          max_storage_mib BIGINT NOT NULL,
          node_cpu_millis INTEGER NOT NULL,
          node_memory_mib INTEGER NOT NULL,
          node_storage_mib INTEGER NOT NULL,
          max_pending_jobs INTEGER NOT NULL,
          max_unschedulable_jobs INTEGER NOT NULL,
          max_image_pull_backoff_jobs INTEGER NOT NULL,
          max_create_per_minute INTEGER NOT NULL,
          observation_max_age_seconds INTEGER NOT NULL,
          reason TEXT,
          version BIGINT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_capacity_policies_limits_check CHECK (
            max_nodes > 0
            AND max_vcpu_millis > 0
            AND max_memory_mib > 0
            AND max_storage_mib > 0
            AND node_cpu_millis > 0
            AND node_memory_mib > 0
            AND node_storage_mib > 0
            AND node_cpu_millis <= max_vcpu_millis
            AND node_memory_mib <= max_memory_mib
            AND node_storage_mib <= max_storage_mib
            AND max_pending_jobs > 0
            AND max_unschedulable_jobs >= 0
            AND max_image_pull_backoff_jobs >= 0
            AND max_create_per_minute > 0
            AND observation_max_age_seconds BETWEEN 10 AND 900
          ),
          CONSTRAINT execution_capacity_policies_reason_check CHECK (
            reason IS NULL OR length(trim(reason)) BETWEEN 1 AND 500
          )
        );

        CREATE TABLE execution_capacity_observations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          target_id TEXT NOT NULL REFERENCES execution_targets(id) ON DELETE RESTRICT,
          provider TEXT NOT NULL,
          source TEXT NOT NULL,
          source_version TEXT NOT NULL,
          observed_at TIMESTAMPTZ NOT NULL,
          provider_capacity_state TEXT NOT NULL,
          provider_capacity_reason TEXT,
          autoscaler_state TEXT NOT NULL,
          autoscaler_reason TEXT,
          provider_quota_nodes INTEGER NOT NULL,
          provider_quota_vcpu_millis BIGINT NOT NULL,
          provider_quota_memory_mib BIGINT NOT NULL,
          provider_quota_storage_mib BIGINT NOT NULL,
          provider_used_nodes INTEGER NOT NULL,
          provider_used_vcpu_millis BIGINT NOT NULL,
          provider_used_memory_mib BIGINT NOT NULL,
          provider_used_storage_mib BIGINT NOT NULL,
          active_nodes INTEGER NOT NULL,
          provisioned_vcpu_millis BIGINT NOT NULL,
          provisioned_memory_mib BIGINT NOT NULL,
          provisioned_storage_mib BIGINT NOT NULL,
          allocatable_cpu_millis BIGINT NOT NULL,
          allocatable_memory_mib BIGINT NOT NULL,
          allocatable_storage_mib BIGINT NOT NULL,
          requested_cpu_millis BIGINT NOT NULL,
          requested_memory_mib BIGINT NOT NULL,
          requested_storage_mib BIGINT NOT NULL,
          pending_jobs INTEGER NOT NULL,
          unschedulable_jobs INTEGER NOT NULL,
          image_pull_backoff_jobs INTEGER NOT NULL,
          pending_reasons_json JSONB NOT NULL,
          observation_json JSONB NOT NULL,
          observation_sha256 TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_capacity_observations_identity_check CHECK (
            length(trim(provider)) BETWEEN 1 AND 80
            AND length(trim(source)) BETWEEN 1 AND 120
            AND length(trim(source_version)) BETWEEN 1 AND 160
            AND (provider_capacity_reason IS NULL
                 OR length(trim(provider_capacity_reason)) BETWEEN 1 AND 500)
            AND (autoscaler_reason IS NULL
                 OR length(trim(autoscaler_reason)) BETWEEN 1 AND 500)
          ),
          CONSTRAINT execution_capacity_observations_state_check CHECK (
            provider_capacity_state IN ('available','insufficient','unknown')
            AND autoscaler_state IN ('ready','scaling','stalled','unknown')
          ),
          CONSTRAINT execution_capacity_observations_quota_check CHECK (
            provider_quota_nodes > 0
            AND provider_quota_vcpu_millis > 0
            AND provider_quota_memory_mib > 0
            AND provider_quota_storage_mib > 0
            AND provider_used_nodes >= 0
            AND provider_used_vcpu_millis >= 0
            AND provider_used_memory_mib >= 0
            AND provider_used_storage_mib >= 0
          ),
          CONSTRAINT execution_capacity_observations_cluster_check CHECK (
            active_nodes >= 0
            AND provisioned_vcpu_millis >= 0
            AND provisioned_memory_mib >= 0
            AND provisioned_storage_mib >= 0
            AND allocatable_cpu_millis >= 0
            AND allocatable_memory_mib >= 0
            AND allocatable_storage_mib >= 0
            AND requested_cpu_millis >= 0
            AND requested_memory_mib >= 0
            AND requested_storage_mib >= 0
            AND pending_jobs >= 0
            AND unschedulable_jobs >= 0
            AND image_pull_backoff_jobs >= 0
          ),
          CONSTRAINT execution_capacity_observations_digest_check CHECK (
            observation_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT execution_capacity_observations_source_uidx UNIQUE (
            target_id, source, source_version
          ),
          CONSTRAINT execution_capacity_observations_target_time_uidx UNIQUE (
            target_id, observed_at
          )
        );
        CREATE INDEX execution_capacity_observations_target_time_idx
          ON execution_capacity_observations (target_id, observed_at DESC, id DESC);

        CREATE TABLE execution_provisioning_authorizations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          lease_id UUID NOT NULL UNIQUE REFERENCES execution_leases(id) ON DELETE RESTRICT,
          target_id TEXT NOT NULL REFERENCES execution_targets(id) ON DELETE RESTRICT,
          observation_id UUID NOT NULL
            REFERENCES execution_capacity_observations(id) ON DELETE RESTRICT,
          policy_version BIGINT NOT NULL,
          requested_cpu_millis INTEGER NOT NULL,
          requested_memory_mib INTEGER NOT NULL,
          requested_storage_mib INTEGER NOT NULL,
          incremental_nodes INTEGER NOT NULL,
          incremental_vcpu_millis BIGINT NOT NULL,
          incremental_memory_mib BIGINT NOT NULL,
          incremental_storage_mib BIGINT NOT NULL,
          decision_reason TEXT NOT NULL,
          authorization_sha256 TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'authorized',
          authorized_at TIMESTAMPTZ NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL,
          released_at TIMESTAMPTZ,
          CONSTRAINT execution_provisioning_authorizations_values_check CHECK (
            policy_version > 0
            AND requested_cpu_millis > 0
            AND requested_memory_mib > 0
            AND requested_storage_mib > 0
            AND incremental_nodes >= 0
            AND incremental_vcpu_millis >= 0
            AND incremental_memory_mib >= 0
            AND incremental_storage_mib >= 0
            AND length(trim(decision_reason)) BETWEEN 1 AND 120
            AND authorization_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT execution_provisioning_authorizations_state_check CHECK (
            state IN (
              'authorized','pending','unschedulable',
              'image_pull_backoff','running','released'
            )
          ),
          CONSTRAINT execution_provisioning_authorizations_state_group_check CHECK (
            (state = 'released' AND released_at IS NOT NULL)
            OR (state <> 'released' AND released_at IS NULL)
          )
        );
        CREATE INDEX execution_provisioning_authorizations_target_state_idx
          ON execution_provisioning_authorizations (target_id, state, authorized_at, id);

        CREATE FUNCTION loom_execution_capacity_observation_immutable()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'execution capacity observations are immutable';
        END;
        $$;
        CREATE TRIGGER execution_capacity_observation_immutable_trigger
          BEFORE UPDATE ON execution_capacity_observations
          FOR EACH ROW EXECUTE FUNCTION loom_execution_capacity_observation_immutable();

        CREATE FUNCTION loom_execution_provisioning_authorization_immutable()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.lease_id, NEW.target_id, NEW.observation_id, NEW.policy_version,
                 NEW.requested_cpu_millis, NEW.requested_memory_mib,
                 NEW.requested_storage_mib, NEW.incremental_nodes,
                 NEW.incremental_vcpu_millis, NEW.incremental_memory_mib,
                 NEW.incremental_storage_mib, NEW.decision_reason,
                 NEW.authorization_sha256, NEW.authorized_at)
             IS DISTINCT FROM
             ROW(OLD.lease_id, OLD.target_id, OLD.observation_id, OLD.policy_version,
                 OLD.requested_cpu_millis, OLD.requested_memory_mib,
                 OLD.requested_storage_mib, OLD.incremental_nodes,
                 OLD.incremental_vcpu_millis, OLD.incremental_memory_mib,
                 OLD.incremental_storage_mib, OLD.decision_reason,
                 OLD.authorization_sha256, OLD.authorized_at) THEN
            RAISE EXCEPTION 'execution provisioning authorization identity is immutable';
          END IF;
          IF OLD.state = 'released' AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'released execution provisioning authorization is immutable';
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_provisioning_authorization_immutable_trigger
          BEFORE UPDATE ON execution_provisioning_authorizations
          FOR EACH ROW EXECUTE FUNCTION loom_execution_provisioning_authorization_immutable();

        CREATE FUNCTION loom_transition_execution_provisioning_authorization()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        DECLARE
          next_state TEXT;
        BEGIN
          IF NEW.observed_state IN (
            'cancelled','timed_out','failed','finalized','deleted'
          ) THEN
            next_state := 'released';
          ELSIF NEW.observed_state = 'running' THEN
            next_state := 'running';
          ELSIF NEW.error_code = 'unschedulable' THEN
            next_state := 'unschedulable';
          ELSIF NEW.error_code = 'image_pull_backoff' THEN
            next_state := 'image_pull_backoff';
          ELSIF NEW.observed_state = 'creating' THEN
            next_state := 'pending';
          ELSE
            RETURN NEW;
          END IF;

          UPDATE execution_provisioning_authorizations
             SET state = next_state,
                 updated_at = NOW(),
                 released_at = CASE WHEN next_state = 'released' THEN NOW() ELSE NULL END
           WHERE lease_id = NEW.id AND state <> 'released';
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_provisioning_authorization_transition_trigger
          AFTER UPDATE OF observed_state, error_code ON execution_leases
          FOR EACH ROW EXECUTE FUNCTION loom_transition_execution_provisioning_authorization();
        """
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM execution_capacity_policies)
             OR EXISTS (SELECT 1 FROM execution_capacity_observations)
             OR EXISTS (SELECT 1 FROM execution_provisioning_authorizations) THEN
            RAISE EXCEPTION 'cannot downgrade 0119 with execution capacity evidence';
          END IF;
        END;
        $$;
        DROP TRIGGER IF EXISTS execution_provisioning_authorization_transition_trigger
          ON execution_leases;
        DROP FUNCTION IF EXISTS loom_transition_execution_provisioning_authorization();
        DROP TRIGGER IF EXISTS execution_provisioning_authorization_immutable_trigger
          ON execution_provisioning_authorizations;
        DROP FUNCTION IF EXISTS loom_execution_provisioning_authorization_immutable();
        DROP TRIGGER IF EXISTS execution_capacity_observation_immutable_trigger
          ON execution_capacity_observations;
        DROP FUNCTION IF EXISTS loom_execution_capacity_observation_immutable();
        DROP TABLE IF EXISTS execution_provisioning_authorizations;
        DROP TABLE IF EXISTS execution_capacity_observations;
        DROP TABLE IF EXISTS execution_capacity_policies;
        """
    )
