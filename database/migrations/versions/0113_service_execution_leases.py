"""Durable generation-fenced service execution desired state.

Revision ID: 0113
Revises: 0112
"""

from alembic import op

revision = "0113"
down_revision = "0112"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE TABLE execution_classes (
          id TEXT PRIMARY KEY,
          schema_version TEXT NOT NULL,
          spec_json JSONB NOT NULL,
          spec_sha256 TEXT NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT true,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          retired_at TIMESTAMPTZ,
          CONSTRAINT execution_classes_id_check
            CHECK (id ~ '^[a-z0-9][a-z0-9-]{0,79}$'),
          CONSTRAINT execution_classes_schema_check
            CHECK (schema_version = 'loom.execution-class.v1'),
          CONSTRAINT execution_classes_digest_check
            CHECK (spec_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT execution_classes_retired_check
            CHECK ((enabled AND retired_at IS NULL) OR NOT enabled)
        );

        CREATE TABLE execution_targets (
          id TEXT PRIMARY KEY,
          logical_pool_id TEXT NOT NULL,
          execution_class_id TEXT NOT NULL
            REFERENCES execution_classes(id) ON DELETE RESTRICT,
          schema_version TEXT NOT NULL,
          spec_json JSONB NOT NULL,
          spec_sha256 TEXT NOT NULL,
          environment TEXT NOT NULL,
          provider TEXT NOT NULL,
          region TEXT NOT NULL,
          failure_domain TEXT NOT NULL,
          data_residency TEXT NOT NULL,
          desired_state TEXT NOT NULL DEFAULT 'disabled',
          observed_state TEXT NOT NULL DEFAULT 'unknown',
          health_status TEXT NOT NULL DEFAULT 'unknown',
          health_observed_at TIMESTAMPTZ,
          health_error_code TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT execution_targets_id_check
            CHECK (id ~ '^[a-z0-9][a-z0-9-]{0,79}$'),
          CONSTRAINT execution_targets_pool_check
            CHECK (logical_pool_id ~ '^[a-z0-9][a-z0-9-]{0,79}$'),
          CONSTRAINT execution_targets_schema_check
            CHECK (schema_version = 'loom.execution-target.v1'),
          CONSTRAINT execution_targets_digest_check
            CHECK (spec_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT execution_targets_environment_check
            CHECK (environment IN ('development','staging','production')),
          CONSTRAINT execution_targets_provider_check CHECK (provider = 'nebius'),
          CONSTRAINT execution_targets_residency_check CHECK (data_residency = 'eu'),
          CONSTRAINT execution_targets_desired_check
            CHECK (desired_state IN ('disabled','active','draining','retired')),
          CONSTRAINT execution_targets_observed_check
            CHECK (observed_state IN ('unknown','absent','provisioning','ready','degraded','failed','retired')),
          CONSTRAINT execution_targets_health_check
            CHECK (health_status IN ('unknown','healthy','unhealthy','stale')),
          CONSTRAINT execution_targets_health_group_check CHECK (
            (health_status = 'unknown' AND health_observed_at IS NULL)
            OR (health_status <> 'unknown' AND health_observed_at IS NOT NULL)
          ),
          CONSTRAINT execution_targets_error_bound_check
            CHECK (health_error_code IS NULL OR length(health_error_code) <= 120)
        );
        CREATE INDEX execution_targets_placement_idx
          ON execution_targets (environment, desired_state, health_status, region, id);

        CREATE TABLE execution_leases (
          id UUID PRIMARY KEY,
          request_id UUID NOT NULL UNIQUE,
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE RESTRICT,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
          lifecycle_authority_id UUID
            REFERENCES data_lifecycle_authorities(id) ON DELETE RESTRICT,
          attempt INTEGER NOT NULL,
          generation BIGINT NOT NULL,
          resource_generation BIGINT NOT NULL,
          execution_class_id TEXT NOT NULL
            REFERENCES execution_classes(id) ON DELETE RESTRICT,
          target_id TEXT REFERENCES execution_targets(id) ON DELETE RESTRICT,
          workload_requirements_json JSONB NOT NULL,
          workload_requirements_sha256 TEXT NOT NULL,
          desired_state TEXT NOT NULL,
          observed_state TEXT NOT NULL,
          cleanup_state TEXT NOT NULL,
          cleanup_requested_at TIMESTAMPTZ,
          cleanup_deadline_at TIMESTAMPTZ,
          provider_scope_key TEXT NOT NULL UNIQUE,
          namespace_name TEXT NOT NULL,
          job_name TEXT NOT NULL UNIQUE,
          execution_unit_key UUID NOT NULL UNIQUE,
          job_uid TEXT,
          pod_uid TEXT,
          kubernetes_resource_version TEXT,
          node_name TEXT,
          deadline_at TIMESTAMPTZ NOT NULL,
          pod_scheduled_at TIMESTAMPTZ,
          pod_started_at TIMESTAMPTZ,
          pod_terminated_at TIMESTAMPTZ,
          last_reconciled_at TIMESTAMPTZ,
          last_heartbeat_at TIMESTAMPTZ,
          last_event_ordinal BIGINT NOT NULL DEFAULT 0,
          revoked_at TIMESTAMPTZ,
          finalized_at TIMESTAMPTZ,
          deleted_at TIMESTAMPTZ,
          error_class TEXT,
          error_code TEXT,
          error_message TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT execution_leases_attempt_check CHECK (attempt > 0),
          CONSTRAINT execution_leases_generation_check CHECK (generation > 0),
          CONSTRAINT execution_leases_resource_generation_check
            CHECK (resource_generation > 0 AND resource_generation <= generation),
          CONSTRAINT execution_leases_event_ordinal_check CHECK (last_event_ordinal >= 0),
          CONSTRAINT execution_leases_workload_digest_check
            CHECK (workload_requirements_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT execution_leases_desired_check CHECK (
            desired_state IN ('create','start','cancel','timeout','retry','finalize','delete_pending','deleted')
          ),
          CONSTRAINT execution_leases_observed_check CHECK (
            observed_state IN ('reserved','creating','created','starting','running','cancelling',
              'cancelled','timed_out','retry_pending','finalizing','finalized',
              'delete_pending','deleted','failed')
          ),
          CONSTRAINT execution_leases_cleanup_check
            CHECK (cleanup_state IN ('not_requested','pending','in_progress','complete','blocked')),
          CONSTRAINT execution_leases_cleanup_time_check CHECK (
            (cleanup_state = 'not_requested' AND cleanup_requested_at IS NULL
              AND cleanup_deadline_at IS NULL)
            OR (cleanup_state <> 'not_requested' AND cleanup_requested_at IS NOT NULL
              AND cleanup_deadline_at IS NOT NULL
              AND cleanup_deadline_at > cleanup_requested_at)
          ),
          CONSTRAINT execution_leases_namespace_check
            CHECK (namespace_name ~ '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'),
          CONSTRAINT execution_leases_job_check
            CHECK (job_name ~ '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'),
          CONSTRAINT execution_leases_kubernetes_identity_bound_check CHECK (
            (job_uid IS NULL OR length(job_uid) BETWEEN 1 AND 128)
            AND (pod_uid IS NULL OR length(pod_uid) BETWEEN 1 AND 128)
            AND (kubernetes_resource_version IS NULL
              OR length(kubernetes_resource_version) BETWEEN 1 AND 128)
            AND (node_name IS NULL OR length(node_name) BETWEEN 1 AND 253)
          ),
          CONSTRAINT execution_leases_pod_time_order_check CHECK (
            (pod_started_at IS NULL OR pod_scheduled_at IS NULL
              OR pod_started_at >= pod_scheduled_at)
            AND (pod_terminated_at IS NULL OR pod_started_at IS NULL
              OR pod_terminated_at >= pod_started_at)
          ),
          CONSTRAINT execution_leases_error_class_check
            CHECK (error_class IS NULL OR error_class IN ('transient','permanent','policy')),
          CONSTRAINT execution_leases_error_group_check CHECK (
            (error_class IS NULL AND error_code IS NULL AND error_message IS NULL)
            OR (error_class IS NOT NULL AND error_code IS NOT NULL)
          ),
          CONSTRAINT execution_leases_error_bound_check CHECK (
            (error_code IS NULL OR length(error_code) <= 120)
            AND (error_message IS NULL OR length(error_message) <= 2000)
          ),
          CONSTRAINT execution_leases_terminal_time_check CHECK (
            (desired_state = 'deleted') = (deleted_at IS NOT NULL)
            AND (observed_state <> 'finalized' OR finalized_at IS NOT NULL)
            AND (finalized_at IS NULL OR observed_state IN ('finalized','delete_pending','deleted'))
          ),
          CONSTRAINT execution_leases_trial_attempt_uidx UNIQUE (trial_id, attempt)
        );
        CREATE UNIQUE INDEX execution_leases_trial_authoritative_uidx
          ON execution_leases (trial_id) WHERE revoked_at IS NULL;
        CREATE INDEX execution_leases_team_created_idx
          ON execution_leases (team_id, created_at DESC, id);
        CREATE INDEX execution_leases_reconcile_idx
          ON execution_leases (desired_state, observed_state, updated_at, id)
          WHERE deleted_at IS NULL;
        CREATE INDEX execution_leases_cleanup_idx
          ON execution_leases (cleanup_state, updated_at, id)
          WHERE cleanup_state <> 'complete';
        CREATE INDEX execution_leases_lifecycle_idx
          ON execution_leases (lifecycle_authority_id)
          WHERE lifecycle_authority_id IS NOT NULL;

        CREATE TABLE execution_commands (
          id UUID PRIMARY KEY,
          lease_id UUID NOT NULL REFERENCES execution_leases(id) ON DELETE CASCADE,
          generation BIGINT NOT NULL,
          sequence BIGINT NOT NULL,
          command_type TEXT NOT NULL,
          idempotency_key TEXT NOT NULL UNIQUE,
          payload_json JSONB NOT NULL,
          payload_sha256 TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'pending',
          delivery_count INTEGER NOT NULL DEFAULT 0,
          available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          claimed_by TEXT,
          claim_expires_at TIMESTAMPTZ,
          acknowledged_at TIMESTAMPTZ,
          acknowledgement_sha256 TEXT,
          last_error_code TEXT,
          last_error_message TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT execution_commands_generation_check CHECK (generation > 0),
          CONSTRAINT execution_commands_sequence_check CHECK (sequence > 0),
          CONSTRAINT execution_commands_type_check CHECK (
            command_type IN ('create','start','cancel','timeout','retry','finalize','delete')
          ),
          CONSTRAINT execution_commands_digest_check
            CHECK (payload_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT execution_commands_state_check
            CHECK (state IN ('pending','leased','acknowledged','dead_letter')),
          CONSTRAINT execution_commands_delivery_check
            CHECK (delivery_count >= 0 AND delivery_count <= 1000),
          CONSTRAINT execution_commands_claim_group_check CHECK (
            (state = 'leased' AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL)
            OR (state <> 'leased' AND claimed_by IS NULL AND claim_expires_at IS NULL)
          ),
          CONSTRAINT execution_commands_ack_group_check CHECK (
            (state = 'acknowledged' AND acknowledged_at IS NOT NULL
              AND acknowledgement_sha256 ~ '^sha256:[0-9a-f]{64}$')
            OR (state <> 'acknowledged' AND acknowledged_at IS NULL
              AND acknowledgement_sha256 IS NULL)
          ),
          CONSTRAINT execution_commands_payload_bound_check
            CHECK (octet_length(payload_json::text) <= 65536),
          CONSTRAINT execution_commands_error_bound_check CHECK (
            (last_error_code IS NULL OR length(last_error_code) <= 120)
            AND (last_error_message IS NULL OR length(last_error_message) <= 2000)
          ),
          CONSTRAINT execution_commands_lease_generation_sequence_uidx
            UNIQUE (lease_id, generation, sequence)
        );
        CREATE INDEX execution_commands_delivery_idx
          ON execution_commands (state, available_at, created_at, id)
          WHERE state IN ('pending','leased');

        CREATE TABLE execution_events (
          id UUID PRIMARY KEY,
          lease_id UUID NOT NULL REFERENCES execution_leases(id) ON DELETE CASCADE,
          generation BIGINT NOT NULL,
          ordinal BIGINT NOT NULL,
          event_kind TEXT NOT NULL,
          idempotency_key TEXT NOT NULL UNIQUE,
          payload_json JSONB NOT NULL,
          payload_sha256 TEXT NOT NULL,
          observed_at TIMESTAMPTZ NOT NULL,
          accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT execution_events_generation_check CHECK (generation > 0),
          CONSTRAINT execution_events_ordinal_check CHECK (ordinal BETWEEN 1 AND 10000),
          CONSTRAINT execution_events_kind_check CHECK (
            event_kind IN ('created','started','heartbeat','gateway_call','artifact_committed',
              'trajectory_committed','usage_reported','result_reported','kubernetes_observed','cancelled',
              'timed_out','failed','finalized','deleted')
          ),
          CONSTRAINT execution_events_digest_check
            CHECK (payload_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT execution_events_payload_bound_check
            CHECK (octet_length(payload_json::text) <= 65536),
          CONSTRAINT execution_events_lease_generation_ordinal_uidx
            UNIQUE (lease_id, generation, ordinal)
        );
        CREATE INDEX execution_events_lease_observed_idx
          ON execution_events (lease_id, observed_at, ordinal);

        CREATE TABLE execution_lease_history (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          lease_id UUID NOT NULL REFERENCES execution_leases(id) ON DELETE CASCADE,
          transition_ordinal BIGINT NOT NULL,
          generation BIGINT NOT NULL,
          desired_state TEXT NOT NULL,
          observed_state TEXT NOT NULL,
          cleanup_state TEXT NOT NULL,
          snapshot_json JSONB NOT NULL,
          snapshot_sha256 TEXT NOT NULL,
          changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT execution_lease_history_ordinal_check
            CHECK (transition_ordinal BETWEEN 1 AND 20000),
          CONSTRAINT execution_lease_history_generation_check CHECK (generation > 0),
          CONSTRAINT execution_lease_history_digest_check
            CHECK (snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT execution_lease_history_payload_bound_check
            CHECK (octet_length(snapshot_json::text) <= 65536),
          CONSTRAINT execution_lease_history_lease_ordinal_uidx
            UNIQUE (lease_id, transition_ordinal)
        );

        CREATE FUNCTION validate_execution_lease_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.request_id, NEW.trial_id, NEW.team_id, NEW.attempt,
                 NEW.resource_generation,
                 NEW.execution_class_id, NEW.workload_requirements_json,
                 NEW.workload_requirements_sha256, NEW.provider_scope_key,
                 NEW.namespace_name, NEW.job_name, NEW.execution_unit_key,
                 NEW.created_at)
             IS DISTINCT FROM
             ROW(OLD.request_id, OLD.trial_id, OLD.team_id, OLD.attempt,
                 OLD.resource_generation,
                 OLD.execution_class_id, OLD.workload_requirements_json,
                 OLD.workload_requirements_sha256, OLD.provider_scope_key,
                 OLD.namespace_name, OLD.job_name, OLD.execution_unit_key,
                 OLD.created_at) THEN
            RAISE EXCEPTION 'execution lease immutable identity changed';
          END IF;
          IF NEW.generation < OLD.generation OR NEW.generation > OLD.generation + 1 THEN
            RAISE EXCEPTION 'execution lease generation must advance monotonically by one';
          END IF;
          IF OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS NULL THEN
            RAISE EXCEPTION 'revoked execution generation cannot regain authority';
          END IF;
          IF OLD.deleted_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'deleted execution lease is immutable';
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER execution_leases_mutation_trigger
          BEFORE UPDATE ON execution_leases
          FOR EACH ROW EXECUTE FUNCTION validate_execution_lease_mutation();

        CREATE FUNCTION validate_execution_lease_outbox() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE expected_type TEXT;
        BEGIN
          IF NEW.desired_state = 'deleted' THEN
            RETURN NULL;
          END IF;
          expected_type := CASE NEW.desired_state
            WHEN 'delete_pending' THEN 'delete'
            ELSE NEW.desired_state
          END;
          IF NOT EXISTS (
            SELECT 1 FROM execution_commands c
             WHERE c.lease_id = NEW.id
               AND c.generation = NEW.generation
               AND c.command_type = expected_type
          ) THEN
            RAISE EXCEPTION
              'execution lease % generation % desired state % lacks durable % command',
              NEW.id, NEW.generation, NEW.desired_state, expected_type;
          END IF;
          RETURN NULL;
        END $$;
        CREATE CONSTRAINT TRIGGER execution_leases_outbox_trigger
          AFTER INSERT OR UPDATE OF generation, desired_state ON execution_leases
          DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW EXECUTE FUNCTION validate_execution_lease_outbox();

        CREATE FUNCTION append_execution_lease_history() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE next_ordinal BIGINT;
        DECLARE snapshot JSONB;
        DECLARE digest TEXT;
        BEGIN
          SELECT COALESCE(MAX(transition_ordinal), 0) + 1 INTO next_ordinal
            FROM execution_lease_history WHERE lease_id = NEW.id;
          snapshot := jsonb_build_object(
            'generation', NEW.generation,
            'desired_state', NEW.desired_state,
            'observed_state', NEW.observed_state,
            'cleanup_state', NEW.cleanup_state,
            'cleanup_requested_at', NEW.cleanup_requested_at,
            'cleanup_deadline_at', NEW.cleanup_deadline_at,
            'last_event_ordinal', NEW.last_event_ordinal,
            'target_id', NEW.target_id,
            'job_uid', NEW.job_uid,
            'pod_uid', NEW.pod_uid,
            'kubernetes_resource_version', NEW.kubernetes_resource_version,
            'revoked_at', NEW.revoked_at,
            'finalized_at', NEW.finalized_at,
            'deleted_at', NEW.deleted_at,
            'error_class', NEW.error_class,
            'error_code', NEW.error_code
          );
          digest := 'sha256:' || encode(sha256(convert_to(snapshot::text, 'UTF8')), 'hex');
          INSERT INTO execution_lease_history (
            lease_id, transition_ordinal, generation, desired_state,
            observed_state, cleanup_state, snapshot_json, snapshot_sha256
          ) VALUES (
            NEW.id, next_ordinal, NEW.generation, NEW.desired_state,
            NEW.observed_state, NEW.cleanup_state, snapshot, digest
          );
          RETURN NULL;
        END $$;
        CREATE TRIGGER execution_leases_history_trigger
          AFTER INSERT OR UPDATE OF generation, desired_state, observed_state,
            cleanup_state, cleanup_requested_at, cleanup_deadline_at, target_id,
            last_event_ordinal, job_uid, pod_uid, kubernetes_resource_version,
            node_name, pod_scheduled_at, pod_started_at, pod_terminated_at,
            last_reconciled_at, revoked_at, finalized_at, deleted_at,
            error_class, error_code ON execution_leases
          FOR EACH ROW EXECUTE FUNCTION append_execution_lease_history();
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM execution_leases)
             OR EXISTS (SELECT 1 FROM execution_targets)
             OR EXISTS (SELECT 1 FROM execution_classes) THEN
            RAISE EXCEPTION 'cannot downgrade 0113 with service execution state';
          END IF;
        END $$;
        DROP TRIGGER execution_leases_history_trigger ON execution_leases;
        DROP FUNCTION append_execution_lease_history();
        DROP TRIGGER execution_leases_outbox_trigger ON execution_leases;
        DROP FUNCTION validate_execution_lease_outbox();
        DROP TRIGGER execution_leases_mutation_trigger ON execution_leases;
        DROP FUNCTION validate_execution_lease_mutation();
        DROP TABLE execution_lease_history;
        DROP TABLE execution_events;
        DROP TABLE execution_commands;
        DROP TABLE execution_leases;
        DROP TABLE execution_targets;
        DROP TABLE execution_classes;
        """
    )
