"""Persist immutable execution runtime contracts on service leases.

Revision ID: 0114
Revises: 0113
"""

from alembic import op

revision = "0114"
down_revision = "0113"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        ALTER TABLE execution_leases
          ADD COLUMN execution_role TEXT NOT NULL DEFAULT 'attempt',
          ADD COLUMN parent_lease_id UUID REFERENCES execution_leases(id) ON DELETE RESTRICT,
          ADD COLUMN runtime_contract_json JSONB,
          ADD COLUMN runtime_contract_sha256 TEXT,
          ADD CONSTRAINT execution_leases_role_check
            CHECK (execution_role IN ('attempt','verifier')),
          ADD CONSTRAINT execution_leases_parent_role_check CHECK (
            (execution_role = 'attempt' AND parent_lease_id IS NULL)
            OR (execution_role = 'verifier' AND parent_lease_id IS NOT NULL)
          ),
          ADD CONSTRAINT execution_leases_runtime_contract_group_check CHECK (
            (runtime_contract_json IS NULL AND runtime_contract_sha256 IS NULL)
            OR (
              runtime_contract_json IS NOT NULL
              AND runtime_contract_sha256 ~ '^sha256:[0-9a-f]{64}$'
              AND runtime_contract_json->>'schema_version' = 'loom.execution-runtime-plan.v1'
              AND runtime_contract_json->>'execution_role' = execution_role
              AND octet_length(runtime_contract_json::text) <= 262144
            )
          );
        ALTER TABLE execution_leases ALTER COLUMN execution_role DROP DEFAULT;
        ALTER TABLE execution_leases
          DROP CONSTRAINT execution_leases_trial_attempt_uidx,
          ADD CONSTRAINT execution_leases_trial_attempt_role_uidx
            UNIQUE (trial_id, attempt, execution_role);
        DROP INDEX execution_leases_trial_authoritative_uidx;
        CREATE UNIQUE INDEX execution_leases_trial_authoritative_uidx
          ON execution_leases (trial_id)
          WHERE revoked_at IS NULL AND execution_role = 'attempt';

        CREATE OR REPLACE FUNCTION validate_execution_lease_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.request_id, NEW.trial_id, NEW.team_id, NEW.attempt,
                 NEW.execution_role, NEW.parent_lease_id,
                 NEW.resource_generation,
                 NEW.execution_class_id, NEW.workload_requirements_json,
                 NEW.workload_requirements_sha256, NEW.runtime_contract_json,
                 NEW.runtime_contract_sha256, NEW.provider_scope_key,
                 NEW.namespace_name, NEW.job_name, NEW.execution_unit_key,
                 NEW.created_at)
             IS DISTINCT FROM
             ROW(OLD.request_id, OLD.trial_id, OLD.team_id, OLD.attempt,
                 OLD.execution_role, OLD.parent_lease_id,
                 OLD.resource_generation,
                 OLD.execution_class_id, OLD.workload_requirements_json,
                 OLD.workload_requirements_sha256, OLD.runtime_contract_json,
                 OLD.runtime_contract_sha256, OLD.provider_scope_key,
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

        CREATE OR REPLACE FUNCTION append_execution_lease_history() RETURNS trigger
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
            'execution_role', NEW.execution_role,
            'parent_lease_id', NEW.parent_lease_id,
            'runtime_contract_sha256', NEW.runtime_contract_sha256,
            'candidate_sha', NEW.runtime_contract_json->>'candidate_sha',
            'task_revision_sha256', NEW.runtime_contract_json->>'task_revision_sha256',
            'task_image_ref', NEW.runtime_contract_json->>'task_image_ref',
            'runtime_image_ref', NEW.runtime_contract_json->>'runtime_image_ref',
            'command_identity_sha256', NEW.runtime_contract_json->>'command_identity_sha256',
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
        """
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM execution_leases
             WHERE runtime_contract_json IS NOT NULL OR execution_role = 'verifier'
          ) THEN
            RAISE EXCEPTION 'cannot downgrade 0114 with runtime-bound execution leases';
          END IF;
        END $$;

        CREATE OR REPLACE FUNCTION validate_execution_lease_mutation() RETURNS trigger
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

        CREATE OR REPLACE FUNCTION append_execution_lease_history() RETURNS trigger
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

        ALTER TABLE execution_leases
          DROP CONSTRAINT execution_leases_trial_attempt_role_uidx;
        DROP INDEX execution_leases_trial_authoritative_uidx;
        CREATE UNIQUE INDEX execution_leases_trial_authoritative_uidx
          ON execution_leases (trial_id) WHERE revoked_at IS NULL;
        ALTER TABLE execution_leases
          ADD CONSTRAINT execution_leases_trial_attempt_uidx UNIQUE (trial_id, attempt),
          DROP CONSTRAINT execution_leases_runtime_contract_group_check,
          DROP CONSTRAINT execution_leases_parent_role_check,
          DROP CONSTRAINT execution_leases_role_check,
          DROP COLUMN runtime_contract_sha256,
          DROP COLUMN runtime_contract_json,
          DROP COLUMN parent_lease_id,
          DROP COLUMN execution_role;
        """
    )
