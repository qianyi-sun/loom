"""Add restart-safe canonical materialization state for service executions.

Revision ID: 0129
Revises: 0128
Create Date: 2026-09-03
"""

from __future__ import annotations

from alembic import op

revision: str = "0129"
down_revision: str | None = "0128"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.execution_leases
          ADD COLUMN materialization_state TEXT NOT NULL DEFAULT 'not_started',
          ADD COLUMN materialization_attempts INTEGER NOT NULL DEFAULT 0,
          ADD COLUMN materialization_next_attempt_at TIMESTAMPTZ,
          ADD COLUMN materialization_claim_id UUID,
          ADD COLUMN materialization_claim_expires_at TIMESTAMPTZ,
          ADD COLUMN materialization_started_at TIMESTAMPTZ,
          ADD COLUMN materialization_committed_at TIMESTAMPTZ,
          ADD COLUMN materialization_error_code TEXT,
          ADD COLUMN materialization_error_message TEXT,
          ADD COLUMN canonical_trajectory_sha256 TEXT,
          ADD COLUMN canonical_atif_sha256 TEXT,
          ADD COLUMN source_cleanup_state TEXT NOT NULL DEFAULT 'not_ready',
          ADD COLUMN source_retain_until TIMESTAMPTZ,
          ADD COLUMN source_cleanup_attempts INTEGER NOT NULL DEFAULT 0,
          ADD COLUMN source_cleanup_claim_id UUID,
          ADD COLUMN source_cleanup_claim_expires_at TIMESTAMPTZ,
          ADD COLUMN source_cleanup_error_message TEXT,
          ADD CONSTRAINT execution_leases_materialization_state_check CHECK (
            materialization_state IN
              ('not_started','pending','running','committed','unavailable')
          ),
          ADD CONSTRAINT execution_leases_materialization_attempts_check CHECK (
            materialization_attempts >= 0
          ),
          ADD CONSTRAINT execution_leases_materialization_group_check CHECK (
            (materialization_state='not_started'
             AND materialization_attempts=0
             AND materialization_next_attempt_at IS NULL
             AND materialization_claim_id IS NULL
             AND materialization_claim_expires_at IS NULL
             AND materialization_started_at IS NULL
             AND materialization_committed_at IS NULL
             AND materialization_error_code IS NULL
             AND materialization_error_message IS NULL
             AND canonical_trajectory_sha256 IS NULL
             AND canonical_atif_sha256 IS NULL)
            OR
            (materialization_state='pending'
             AND output_commit_state='committed'
             AND materialization_next_attempt_at IS NOT NULL
             AND materialization_claim_id IS NULL
             AND materialization_claim_expires_at IS NULL
             AND materialization_committed_at IS NULL
             AND canonical_trajectory_sha256 IS NULL
             AND canonical_atif_sha256 IS NULL)
            OR
            (materialization_state='running'
             AND output_commit_state='committed'
             AND materialization_attempts > 0
             AND materialization_claim_id IS NOT NULL
             AND materialization_claim_expires_at IS NOT NULL
             AND materialization_started_at IS NOT NULL
             AND materialization_committed_at IS NULL
             AND canonical_trajectory_sha256 IS NULL
             AND canonical_atif_sha256 IS NULL)
            OR
            (materialization_state='committed'
             AND output_commit_state='committed'
             AND materialization_attempts > 0
             AND materialization_claim_id IS NULL
             AND materialization_claim_expires_at IS NULL
             AND materialization_next_attempt_at IS NULL
             AND materialization_committed_at IS NOT NULL
             AND materialization_error_code IS NULL
             AND materialization_error_message IS NULL
             AND canonical_trajectory_sha256 ~ '^sha256:[0-9a-f]{64}$'
             AND canonical_atif_sha256 ~ '^sha256:[0-9a-f]{64}$')
            OR
            (materialization_state='unavailable'
             AND output_commit_state='committed'
             AND materialization_claim_id IS NULL
             AND materialization_claim_expires_at IS NULL
             AND materialization_next_attempt_at IS NULL
             AND materialization_committed_at IS NULL
             AND length(materialization_error_code) BETWEEN 1 AND 120
             AND length(materialization_error_message) BETWEEN 1 AND 2000
             AND canonical_trajectory_sha256 IS NULL
             AND canonical_atif_sha256 IS NULL)
          ),
          ADD CONSTRAINT execution_leases_source_cleanup_state_check CHECK (
            source_cleanup_state IN ('not_ready','retained','running','complete')
          ),
          ADD CONSTRAINT execution_leases_source_cleanup_group_check CHECK (
            source_cleanup_attempts >= 0 AND (
              (source_cleanup_state='not_ready' AND source_retain_until IS NULL
               AND source_cleanup_claim_id IS NULL
               AND source_cleanup_claim_expires_at IS NULL
               AND source_cleanup_error_message IS NULL)
              OR
              (source_cleanup_state='retained' AND materialization_state='committed'
               AND source_retain_until IS NOT NULL AND source_cleanup_claim_id IS NULL
               AND source_cleanup_claim_expires_at IS NULL)
              OR
              (source_cleanup_state='running' AND materialization_state='committed'
               AND source_retain_until IS NOT NULL AND source_cleanup_attempts > 0
               AND source_cleanup_claim_id IS NOT NULL
               AND source_cleanup_claim_expires_at IS NOT NULL)
              OR
              (source_cleanup_state='complete' AND materialization_state='committed'
               AND source_retain_until IS NOT NULL AND source_cleanup_attempts > 0
               AND source_cleanup_claim_id IS NULL
               AND source_cleanup_claim_expires_at IS NULL
               AND source_cleanup_error_message IS NULL)
            )
          )
        """
    )
    op.execute(
        """
        UPDATE public.execution_leases
           SET materialization_state = 'pending',
               materialization_next_attempt_at = output_committed_at
         WHERE output_commit_state = 'committed';

        CREATE INDEX execution_leases_materialization_queue_idx
          ON public.execution_leases
            (materialization_next_attempt_at, output_committed_at, id)
          WHERE materialization_state IN ('pending','running')
        ;
        CREATE INDEX execution_leases_source_cleanup_queue_idx
          ON public.execution_leases (source_retain_until, id)
          WHERE source_cleanup_state IN ('retained','running')
        """
    )
    op.execute(
        r"""
        CREATE OR REPLACE FUNCTION validate_execution_lease_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.request_id, NEW.trial_id, NEW.team_id, NEW.attempt,
                 NEW.execution_role, NEW.parent_lease_id,
                 NEW.resource_generation,
                 NEW.execution_class_id, NEW.workload_requirements_json,
                 NEW.workload_requirements_sha256, NEW.runtime_contract_json,
                 NEW.runtime_contract_sha256, NEW.routing_generation,
                 NEW.selected_pool_id, NEW.routing_reason,
                 NEW.routing_decision_sha256, NEW.provider_scope_key,
                 NEW.namespace_name, NEW.job_name, NEW.execution_unit_key,
                 NEW.created_at)
             IS DISTINCT FROM
             ROW(OLD.request_id, OLD.trial_id, OLD.team_id, OLD.attempt,
                 OLD.execution_role, OLD.parent_lease_id,
                 OLD.resource_generation,
                 OLD.execution_class_id, OLD.workload_requirements_json,
                 OLD.workload_requirements_sha256, OLD.runtime_contract_json,
                 OLD.runtime_contract_sha256, OLD.routing_generation,
                 OLD.selected_pool_id, OLD.routing_reason,
                 OLD.routing_decision_sha256, OLD.provider_scope_key,
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
          IF OLD.materialization_attempts > NEW.materialization_attempts THEN
            RAISE EXCEPTION 'materialization attempts cannot decrease';
          END IF;
          IF OLD.source_cleanup_attempts > NEW.source_cleanup_attempts THEN
            RAISE EXCEPTION 'source cleanup attempts cannot decrease';
          END IF;
          IF OLD.materialization_state IN ('committed','unavailable')
             AND ROW(NEW.materialization_state, NEW.materialization_attempts,
                     NEW.materialization_next_attempt_at, NEW.materialization_claim_id,
                     NEW.materialization_claim_expires_at, NEW.materialization_started_at,
                     NEW.materialization_committed_at, NEW.materialization_error_code,
                     NEW.materialization_error_message, NEW.canonical_trajectory_sha256,
                     NEW.canonical_atif_sha256)
                 IS DISTINCT FROM
                 ROW(OLD.materialization_state, OLD.materialization_attempts,
                     OLD.materialization_next_attempt_at, OLD.materialization_claim_id,
                     OLD.materialization_claim_expires_at, OLD.materialization_started_at,
                     OLD.materialization_committed_at, OLD.materialization_error_code,
                     OLD.materialization_error_message, OLD.canonical_trajectory_sha256,
                     OLD.canonical_atif_sha256) THEN
            RAISE EXCEPTION 'terminal materialization state is immutable';
          END IF;
          IF OLD.source_cleanup_state = 'complete'
             AND ROW(NEW.source_cleanup_state, NEW.source_retain_until,
                     NEW.source_cleanup_attempts, NEW.source_cleanup_claim_id,
                     NEW.source_cleanup_claim_expires_at,
                     NEW.source_cleanup_error_message)
                 IS DISTINCT FROM
                 ROW(OLD.source_cleanup_state, OLD.source_retain_until,
                     OLD.source_cleanup_attempts, OLD.source_cleanup_claim_id,
                     OLD.source_cleanup_claim_expires_at,
                     OLD.source_cleanup_error_message) THEN
            RAISE EXCEPTION 'complete source cleanup state is immutable';
          END IF;
          IF OLD.deleted_at IS NOT NULL
             AND (to_jsonb(NEW) - ARRAY[
                    'materialization_state','materialization_attempts',
                    'materialization_next_attempt_at','materialization_claim_id',
                    'materialization_claim_expires_at','materialization_started_at',
                    'materialization_committed_at','materialization_error_code',
                    'materialization_error_message','canonical_trajectory_sha256',
                    'canonical_atif_sha256','updated_at'
                    ,'source_cleanup_state','source_retain_until',
                    'source_cleanup_attempts','source_cleanup_claim_id',
                    'source_cleanup_claim_expires_at','source_cleanup_error_message'
                 ]::text[])
                 IS DISTINCT FROM
                 (to_jsonb(OLD) - ARRAY[
                    'materialization_state','materialization_attempts',
                    'materialization_next_attempt_at','materialization_claim_id',
                    'materialization_claim_expires_at','materialization_started_at',
                    'materialization_committed_at','materialization_error_code',
                    'materialization_error_message','canonical_trajectory_sha256',
                    'canonical_atif_sha256','updated_at'
                    ,'source_cleanup_state','source_retain_until',
                    'source_cleanup_attempts','source_cleanup_claim_id',
                    'source_cleanup_claim_expires_at','source_cleanup_error_message'
                 ]::text[]) THEN
            RAISE EXCEPTION 'deleted execution lease is immutable outside materialization';
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
            'routing_generation', NEW.routing_generation,
            'selected_pool_id', NEW.selected_pool_id,
            'routing_reason', NEW.routing_reason,
            'routing_decision_sha256', NEW.routing_decision_sha256,
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
            'pod_ip', NEW.pod_ip,
            'kubernetes_resource_version', NEW.kubernetes_resource_version,
            'output_commit_state', NEW.output_commit_state,
            'output_upload_session_id', NEW.output_upload_session_id,
            'output_generation', NEW.output_generation,
            'output_manifest_sha256', NEW.output_manifest_sha256,
            'output_marker_sha256', NEW.output_marker_sha256,
            'output_committed_at', NEW.output_committed_at,
            'output_unavailable_reason', NEW.output_unavailable_reason,
            'materialization_state', NEW.materialization_state,
            'materialization_attempts', NEW.materialization_attempts,
            'materialization_next_attempt_at', NEW.materialization_next_attempt_at,
            'materialization_claim_id', NEW.materialization_claim_id,
            'materialization_claim_expires_at', NEW.materialization_claim_expires_at,
            'materialization_started_at', NEW.materialization_started_at,
            'materialization_committed_at', NEW.materialization_committed_at,
            'materialization_error_code', NEW.materialization_error_code,
            'canonical_trajectory_sha256', NEW.canonical_trajectory_sha256,
            'canonical_atif_sha256', NEW.canonical_atif_sha256
          ) || jsonb_build_object(
            'source_cleanup_state', NEW.source_cleanup_state,
            'source_retain_until', NEW.source_retain_until,
            'source_cleanup_attempts', NEW.source_cleanup_attempts,
            'source_cleanup_claim_id', NEW.source_cleanup_claim_id,
            'source_cleanup_claim_expires_at', NEW.source_cleanup_claim_expires_at,
            'source_cleanup_error_message', NEW.source_cleanup_error_message,
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
        DO $downgrade$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM public.execution_leases
             WHERE materialization_state <> 'not_started'
          ) THEN
            RAISE EXCEPTION
              'cannot downgrade 0129 while service output materializations exist';
          END IF;
        END
        $downgrade$;
        CREATE OR REPLACE FUNCTION validate_execution_lease_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.request_id, NEW.trial_id, NEW.team_id, NEW.attempt,
                 NEW.execution_role, NEW.parent_lease_id, NEW.resource_generation,
                 NEW.execution_class_id, NEW.workload_requirements_json,
                 NEW.workload_requirements_sha256, NEW.runtime_contract_json,
                 NEW.runtime_contract_sha256, NEW.routing_generation,
                 NEW.selected_pool_id, NEW.routing_reason,
                 NEW.routing_decision_sha256, NEW.provider_scope_key,
                 NEW.namespace_name, NEW.job_name, NEW.execution_unit_key,
                 NEW.created_at)
             IS DISTINCT FROM
             ROW(OLD.request_id, OLD.trial_id, OLD.team_id, OLD.attempt,
                 OLD.execution_role, OLD.parent_lease_id, OLD.resource_generation,
                 OLD.execution_class_id, OLD.workload_requirements_json,
                 OLD.workload_requirements_sha256, OLD.runtime_contract_json,
                 OLD.runtime_contract_sha256, OLD.routing_generation,
                 OLD.selected_pool_id, OLD.routing_reason,
                 OLD.routing_decision_sha256, OLD.provider_scope_key,
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
            'routing_generation', NEW.routing_generation,
            'selected_pool_id', NEW.selected_pool_id,
            'routing_reason', NEW.routing_reason,
            'routing_decision_sha256', NEW.routing_decision_sha256,
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
            'pod_ip', NEW.pod_ip,
            'kubernetes_resource_version', NEW.kubernetes_resource_version,
            'output_commit_state', NEW.output_commit_state,
            'output_upload_session_id', NEW.output_upload_session_id,
            'output_generation', NEW.output_generation,
            'output_manifest_sha256', NEW.output_manifest_sha256,
            'output_marker_sha256', NEW.output_marker_sha256,
            'output_committed_at', NEW.output_committed_at,
            'output_unavailable_reason', NEW.output_unavailable_reason,
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
        DROP INDEX public.execution_leases_source_cleanup_queue_idx;
        DROP INDEX public.execution_leases_materialization_queue_idx;
        ALTER TABLE public.execution_leases
          DROP CONSTRAINT execution_leases_source_cleanup_group_check,
          DROP CONSTRAINT execution_leases_source_cleanup_state_check,
          DROP CONSTRAINT execution_leases_materialization_group_check,
          DROP CONSTRAINT execution_leases_materialization_attempts_check,
          DROP CONSTRAINT execution_leases_materialization_state_check,
          DROP COLUMN canonical_atif_sha256,
          DROP COLUMN canonical_trajectory_sha256,
          DROP COLUMN source_cleanup_error_message,
          DROP COLUMN source_cleanup_claim_expires_at,
          DROP COLUMN source_cleanup_claim_id,
          DROP COLUMN source_cleanup_attempts,
          DROP COLUMN source_retain_until,
          DROP COLUMN source_cleanup_state,
          DROP COLUMN materialization_error_message,
          DROP COLUMN materialization_error_code,
          DROP COLUMN materialization_committed_at,
          DROP COLUMN materialization_started_at,
          DROP COLUMN materialization_claim_expires_at,
          DROP COLUMN materialization_claim_id,
          DROP COLUMN materialization_next_attempt_at,
          DROP COLUMN materialization_attempts,
          DROP COLUMN materialization_state
        """
    )
