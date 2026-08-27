"""Persist fenced Pod identity and durable service-execution output commits.

Revision ID: 0115
Revises: 0114
"""

from alembic import op

revision = "0115"
down_revision = "0114"
branch_labels = None
depends_on = None


_PRODUCER_SHAPE_V1 = r"""
control_producer_kind IS NULL AND control_producer_id IS NULL AND (
  (commit_kind='final_output' AND pipeline_run_id IS NOT NULL
   AND pipeline_stage_run_id IS NOT NULL AND execution_attempt_id IS NOT NULL
   AND attempt_number IS NOT NULL AND checkpoint_sequence IS NULL
   AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL
   AND pipeline_acceptance_authorization_id IS NULL
   AND pipeline_profile_calibration_authorization_id IS NULL AND actor_user_id IS NULL
   AND stage_result_json IS NOT NULL AND stage_result_digest IS NOT NULL
   AND inventory_digest IS NOT NULL) OR
  (commit_kind='checkpoint' AND pipeline_run_id IS NOT NULL
   AND pipeline_stage_run_id IS NOT NULL AND execution_attempt_id IS NOT NULL
   AND attempt_number IS NOT NULL AND checkpoint_sequence IS NOT NULL
   AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL
   AND pipeline_acceptance_authorization_id IS NULL
   AND pipeline_profile_calibration_authorization_id IS NULL AND actor_user_id IS NULL
   AND stage_result_json IS NULL AND stage_result_digest IS NULL AND inventory_digest IS NULL) OR
  (commit_kind='input_import' AND pipeline_input_import_id IS NOT NULL
   AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL
   AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL
   AND pipeline_input_materialization_id IS NULL
   AND pipeline_acceptance_authorization_id IS NULL
   AND pipeline_profile_calibration_authorization_id IS NULL) OR
  (commit_kind='input_materialization' AND pipeline_input_materialization_id IS NOT NULL
   AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL
   AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL
   AND pipeline_input_import_id IS NULL AND pipeline_acceptance_authorization_id IS NULL
   AND pipeline_profile_calibration_authorization_id IS NULL) OR
  (commit_kind='acceptance_evidence' AND pipeline_acceptance_authorization_id IS NOT NULL
   AND acceptance_action IN ('matrix','soak') AND acceptance_candidate_sha256 IS NOT NULL
   AND acceptance_result_kind IN ('success','terminal') AND actor_user_id IS NOT NULL
   AND ((acceptance_result_kind='success' AND acceptance_termination_reason IS NULL) OR
        (acceptance_result_kind='terminal' AND acceptance_termination_reason IS NOT NULL))
   AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL
   AND execution_attempt_id IS NULL AND pipeline_input_import_id IS NULL
   AND pipeline_input_materialization_id IS NULL
   AND pipeline_profile_calibration_authorization_id IS NULL) OR
  (commit_kind='profile_calibration_evidence'
   AND pipeline_profile_calibration_authorization_id IS NOT NULL
   AND profile_calibration_spec_sha256 IS NOT NULL
   AND profile_calibration_result_kind IN ('certification','catalog','terminal')
   AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL
   AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL
   AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL
   AND pipeline_acceptance_authorization_id IS NULL)
)
"""


def upgrade() -> None:
    op.execute(
        r"""
        ALTER TABLE execution_leases
          ADD COLUMN pod_ip INET,
          ADD COLUMN output_commit_state TEXT NOT NULL DEFAULT 'not_started',
          ADD COLUMN output_upload_session_id UUID,
          ADD COLUMN output_generation BIGINT,
          ADD COLUMN output_manifest_sha256 TEXT,
          ADD COLUMN output_marker_sha256 TEXT,
          ADD COLUMN output_committed_at TIMESTAMPTZ,
          ADD COLUMN output_unavailable_reason TEXT,
          ADD CONSTRAINT execution_leases_output_state_check CHECK (
            output_commit_state IN ('not_started','uploading','committed','unavailable')
          ),
          ADD CONSTRAINT execution_leases_output_group_check CHECK (
            (output_commit_state='not_started' AND output_upload_session_id IS NULL
             AND output_generation IS NULL AND output_manifest_sha256 IS NULL
             AND output_marker_sha256 IS NULL AND output_committed_at IS NULL
             AND output_unavailable_reason IS NULL) OR
            (output_commit_state='uploading' AND output_upload_session_id IS NOT NULL
             AND output_generation > 0 AND output_manifest_sha256 IS NULL
             AND output_marker_sha256 IS NULL AND output_committed_at IS NULL
             AND output_unavailable_reason IS NULL) OR
            (output_commit_state='committed' AND output_upload_session_id IS NOT NULL
             AND output_generation > 0
             AND output_manifest_sha256 ~ '^sha256:[0-9a-f]{64}$'
             AND output_marker_sha256 ~ '^sha256:[0-9a-f]{64}$'
             AND output_committed_at IS NOT NULL AND output_unavailable_reason IS NULL) OR
            (output_commit_state='unavailable' AND output_generation > 0
             AND output_manifest_sha256 IS NULL AND output_marker_sha256 IS NULL
             AND output_committed_at IS NULL
             AND length(output_unavailable_reason) BETWEEN 1 AND 120)
          );
        ALTER TABLE execution_leases ALTER COLUMN output_commit_state DROP DEFAULT;

        ALTER TABLE artifact_upload_sessions
          ADD COLUMN service_execution_lease_id UUID
            REFERENCES execution_leases(id) ON DELETE CASCADE,
          ADD COLUMN service_execution_generation BIGINT,
          ADD COLUMN service_execution_role TEXT,
          ADD COLUMN service_execution_runtime_contract_sha256 TEXT,
          ADD COLUMN service_execution_candidate_sha TEXT,
          ADD COLUMN service_execution_task_revision_sha256 TEXT,
          ADD COLUMN service_execution_command_identity_sha256 TEXT,
          DROP CONSTRAINT artifact_upload_sessions_commit_kind_check,
          DROP CONSTRAINT artifact_upload_sessions_producer_shape_check,
          ADD CONSTRAINT artifact_upload_sessions_commit_kind_check CHECK (
            commit_kind IN ('final_output','checkpoint','service_execution_output',
              'input_import','input_materialization','acceptance_evidence',
              'profile_calibration_evidence')
          ),
          ADD CONSTRAINT artifact_upload_sessions_service_execution_group_check CHECK (
            (commit_kind='service_execution_output') =
              (service_execution_lease_id IS NOT NULL) AND
            ((service_execution_lease_id IS NULL
              AND service_execution_generation IS NULL
              AND service_execution_role IS NULL
              AND service_execution_runtime_contract_sha256 IS NULL
              AND service_execution_candidate_sha IS NULL
              AND service_execution_task_revision_sha256 IS NULL
              AND service_execution_command_identity_sha256 IS NULL) OR
             (service_execution_lease_id IS NOT NULL
              AND service_execution_generation > 0
              AND service_execution_role IN ('attempt','verifier')
              AND service_execution_runtime_contract_sha256 ~ '^sha256:[0-9a-f]{64}$'
              AND service_execution_candidate_sha ~ '^[0-9a-f]{40}$'
              AND service_execution_task_revision_sha256 ~ '^sha256:[0-9a-f]{64}$'
              AND service_execution_command_identity_sha256 ~ '^sha256:[0-9a-f]{64}$'))),
          ADD CONSTRAINT artifact_upload_sessions_producer_shape_check CHECK (
            control_producer_kind IS NULL AND control_producer_id IS NULL AND (
              (commit_kind='service_execution_output'
               AND service_execution_lease_id IS NOT NULL
               AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL
               AND execution_attempt_id IS NULL AND attempt_number IS NULL
               AND checkpoint_sequence IS NULL AND pipeline_input_import_id IS NULL
               AND pipeline_input_materialization_id IS NULL
               AND pipeline_acceptance_authorization_id IS NULL
               AND pipeline_profile_calibration_authorization_id IS NULL
               AND actor_user_id IS NULL AND stage_result_json IS NULL
               AND stage_result_digest IS NULL AND inventory_digest IS NULL) OR
              (service_execution_lease_id IS NULL AND ("""
        + _PRODUCER_SHAPE_V1
        + r"""))
            )
          );
        CREATE UNIQUE INDEX artifact_upload_sessions_service_execution_uidx
          ON artifact_upload_sessions
            (service_execution_lease_id, service_execution_generation, idempotency_key)
          WHERE commit_kind='service_execution_output';
        """
    )
    op.execute(
        r"""
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
        """
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM artifact_upload_sessions
             WHERE commit_kind='service_execution_output'
          ) OR EXISTS (
            SELECT 1 FROM execution_leases
             WHERE pod_ip IS NOT NULL OR output_commit_state <> 'not_started'
          ) THEN
            RAISE EXCEPTION 'cannot downgrade 0115 with service execution output evidence';
          END IF;
        END $$;

        DROP INDEX artifact_upload_sessions_service_execution_uidx;
        ALTER TABLE artifact_upload_sessions
          DROP CONSTRAINT artifact_upload_sessions_producer_shape_check,
          DROP CONSTRAINT artifact_upload_sessions_service_execution_group_check,
          DROP CONSTRAINT artifact_upload_sessions_commit_kind_check,
          ADD CONSTRAINT artifact_upload_sessions_commit_kind_check CHECK (
            commit_kind IN ('final_output','checkpoint','input_import',
              'input_materialization','acceptance_evidence','profile_calibration_evidence')
          ),
          ADD CONSTRAINT artifact_upload_sessions_producer_shape_check CHECK (
        """
        + _PRODUCER_SHAPE_V1
        + r"""
          ),
          DROP COLUMN service_execution_command_identity_sha256,
          DROP COLUMN service_execution_task_revision_sha256,
          DROP COLUMN service_execution_candidate_sha,
          DROP COLUMN service_execution_runtime_contract_sha256,
          DROP COLUMN service_execution_role,
          DROP COLUMN service_execution_generation,
          DROP COLUMN service_execution_lease_id;

        ALTER TABLE execution_leases
          DROP CONSTRAINT execution_leases_output_group_check,
          DROP CONSTRAINT execution_leases_output_state_check,
          DROP COLUMN output_unavailable_reason,
          DROP COLUMN output_committed_at,
          DROP COLUMN output_marker_sha256,
          DROP COLUMN output_manifest_sha256,
          DROP COLUMN output_generation,
          DROP COLUMN output_upload_session_id,
          DROP COLUMN output_commit_state,
          DROP COLUMN pod_ip;
        """
    )
    op.execute(
        r"""
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
