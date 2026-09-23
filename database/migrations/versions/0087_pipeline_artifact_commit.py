"""Add the Pipeline Artifact streaming commit ledger.

Revision ID: 0087
Revises: 0086
Create Date: 2026-08-11
"""

from __future__ import annotations

from alembic import op

revision = "0087"
down_revision = "0086"
branch_labels = None
depends_on = None


def upgrade() -> None:
    statements = (
        """
        CREATE TABLE pipeline_input_imports (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            team_id UUID NOT NULL REFERENCES teams(id),
            created_by_user_id UUID NOT NULL REFERENCES users(id),
            recipe_name TEXT NOT NULL, recipe_version INTEGER NOT NULL, recipe_digest TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('dataset','policy','mop_bank')),
            target_artifact_type TEXT NOT NULL,
            input_manifest_json JSONB NOT NULL, input_manifest_digest TEXT NOT NULL,
            trust_class TEXT NOT NULL DEFAULT 'internal_trusted' CHECK (trust_class='internal_trusted'),
            max_bundle_bytes BIGINT NOT NULL CHECK (max_bundle_bytes > 0),
            max_file_count INTEGER NOT NULL CHECK (max_file_count > 0),
            state TEXT NOT NULL CHECK (state IN ('preparing','uploading','committing','committed','aborted')),
            artifact_upload_session_id UUID UNIQUE,
            committed_artifact_id UUID UNIQUE,
            idempotency_key TEXT NOT NULL, request_digest TEXT NOT NULL,
            abort_reason TEXT, expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            committed_at TIMESTAMPTZ, aborted_at TIMESTAMPTZ, version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT pipeline_input_imports_idempotency_uidx UNIQUE(team_id,idempotency_key),
            CONSTRAINT pipeline_input_imports_committed_group_check CHECK (
                (state='committed' AND committed_artifact_id IS NOT NULL AND committed_at IS NOT NULL) OR
                (state!='committed' AND committed_artifact_id IS NULL AND committed_at IS NULL)),
            CONSTRAINT pipeline_input_imports_aborted_group_check CHECK (
                (state='aborted' AND aborted_at IS NOT NULL AND abort_reason IS NOT NULL) OR
                (state!='aborted' AND aborted_at IS NULL AND abort_reason IS NULL))
        )
        """,
        """
        CREATE TABLE pipeline_input_materializations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            team_id UUID NOT NULL REFERENCES teams(id),
            created_by_user_id UUID NOT NULL REFERENCES users(id),
            recipe_name TEXT NOT NULL, recipe_version INTEGER NOT NULL, recipe_digest TEXT NOT NULL,
            source_snapshot_json JSONB NOT NULL, source_snapshot_digest TEXT NOT NULL,
            parameters_json JSONB NOT NULL, parameters_digest TEXT NOT NULL,
            materialization_identity_digest TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('preparing','committing','committed','aborted')),
            declared_outputs_json JSONB NOT NULL, declared_outputs_digest TEXT NOT NULL,
            artifact_upload_session_id UUID UNIQUE, result_bindings_json JSONB,
            official_materialization_kind TEXT, official_materialization_authority_id UUID,
            official_materialization_authority_snapshot_digest TEXT,
            official_materialization_identity_digest TEXT,
            idempotency_key TEXT NOT NULL, request_digest TEXT NOT NULL, abort_reason TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            committed_at TIMESTAMPTZ, aborted_at TIMESTAMPTZ, version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT pipeline_input_materializations_idempotency_uidx UNIQUE(team_id,idempotency_key),
            CONSTRAINT pipeline_input_materializations_official_group_check CHECK (
                (official_materialization_kind IS NULL AND official_materialization_authority_id IS NULL
                 AND official_materialization_authority_snapshot_digest IS NULL
                 AND official_materialization_identity_digest IS NULL) OR
                (official_materialization_kind IS NOT NULL AND official_materialization_authority_id IS NOT NULL
                 AND official_materialization_authority_snapshot_digest IS NOT NULL
                 AND official_materialization_identity_digest IS NOT NULL)),
            CONSTRAINT pipeline_input_materializations_committed_group_check CHECK (
                (state='committed' AND result_bindings_json IS NOT NULL AND committed_at IS NOT NULL) OR
                (state!='committed' AND result_bindings_json IS NULL AND committed_at IS NULL)),
            CONSTRAINT pipeline_input_materializations_aborted_group_check CHECK (
                (state='aborted' AND aborted_at IS NOT NULL AND abort_reason IS NOT NULL) OR
                (state!='aborted' AND aborted_at IS NULL AND abort_reason IS NULL))
        )
        """,
        "CREATE UNIQUE INDEX pipeline_input_materializations_official_uidx ON "
        "pipeline_input_materializations(team_id,official_materialization_kind,"
        "official_materialization_authority_id,official_materialization_identity_digest) "
        "WHERE official_materialization_kind IS NOT NULL",
        """
        CREATE TABLE artifact_upload_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(), team_id UUID NOT NULL REFERENCES teams(id),
            commit_kind TEXT NOT NULL CHECK (commit_kind IN ('final_output','checkpoint','input_import',
                'input_materialization','acceptance_evidence','profile_calibration_evidence')),
            pipeline_run_id UUID REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            pipeline_stage_run_id UUID REFERENCES pipeline_stage_runs(id) ON DELETE CASCADE,
            execution_attempt_id UUID REFERENCES execution_attempts(id) ON DELETE CASCADE,
            attempt_number INTEGER, checkpoint_sequence BIGINT,
            control_producer_kind TEXT, control_producer_id UUID,
            pipeline_input_import_id UUID REFERENCES pipeline_input_imports(id) ON DELETE CASCADE,
            pipeline_input_materialization_id UUID REFERENCES pipeline_input_materializations(id) ON DELETE CASCADE,
            pipeline_acceptance_authorization_id UUID, acceptance_action TEXT,
            acceptance_candidate_sha256 TEXT, acceptance_result_kind TEXT,
            acceptance_termination_reason TEXT,
            pipeline_profile_calibration_authorization_id UUID,
            profile_calibration_spec_sha256 TEXT, profile_calibration_result_kind TEXT,
            profile_calibration_scenario_id TEXT, profile_calibration_candidate_identity_sha256 TEXT,
            profile_calibration_run_ordinal INTEGER,
            profile_calibration_source_pipeline_run_id UUID REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
            profile_calibration_termination_reason TEXT,
            actor_user_id UUID REFERENCES users(id), idempotency_key TEXT NOT NULL,
            request_digest TEXT NOT NULL, stage_result_json JSONB, stage_result_digest TEXT,
            inventory_digest TEXT, prefix TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL CHECK (state IN ('preparing','uploading','uploaded','committing',
                'committed_ready','committed','aborted')),
            expected_total_max_bytes BIGINT NOT NULL CHECK (expected_total_max_bytes > 0),
            actual_total_bytes BIGINT NOT NULL DEFAULT 0,
            canonical_manifest_json JSONB, manifest_sha256 TEXT, committed_marker_sha256 TEXT,
            upload_token_digest BYTEA, expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            committed_ready_at TIMESTAMPTZ, committed_at TIMESTAMPTZ, aborted_at TIMESTAMPTZ,
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT artifact_upload_sessions_bytes_check CHECK (
                actual_total_bytes >= 0 AND actual_total_bytes <= expected_total_max_bytes),
            CONSTRAINT artifact_upload_sessions_manifest_group_check CHECK (
                (state IN ('committed_ready','committed') AND canonical_manifest_json IS NOT NULL
                 AND manifest_sha256 IS NOT NULL AND committed_marker_sha256 IS NOT NULL) OR
                (state NOT IN ('committed_ready','committed') AND canonical_manifest_json IS NULL
                 AND manifest_sha256 IS NULL AND committed_marker_sha256 IS NULL)),
            CONSTRAINT artifact_upload_sessions_ready_kind_check CHECK (
                state != 'committed_ready' OR commit_kind='final_output'),
            CONSTRAINT artifact_upload_sessions_profile_shape_check CHECK (
                commit_kind != 'profile_calibration_evidence' OR
                (profile_calibration_result_kind='certification'
                 AND profile_calibration_scenario_id IS NOT NULL
                 AND profile_calibration_candidate_identity_sha256 IS NOT NULL
                 AND profile_calibration_run_ordinal BETWEEN 1 AND 3
                 AND profile_calibration_source_pipeline_run_id IS NOT NULL
                 AND profile_calibration_termination_reason IS NULL) OR
                (profile_calibration_result_kind='catalog'
                 AND profile_calibration_scenario_id IS NULL
                 AND profile_calibration_candidate_identity_sha256 IS NULL
                 AND profile_calibration_run_ordinal IS NULL
                 AND profile_calibration_source_pipeline_run_id IS NULL
                 AND profile_calibration_termination_reason IS NULL) OR
                (profile_calibration_result_kind='terminal'
                 AND profile_calibration_scenario_id IS NULL
                 AND profile_calibration_candidate_identity_sha256 IS NULL
                 AND profile_calibration_run_ordinal IS NULL
                 AND profile_calibration_source_pipeline_run_id IS NULL
                 AND profile_calibration_termination_reason IS NOT NULL)),
            CONSTRAINT artifact_upload_sessions_producer_shape_check CHECK (
                control_producer_kind IS NULL AND control_producer_id IS NULL AND (
                (commit_kind='final_output' AND pipeline_run_id IS NOT NULL AND pipeline_stage_run_id IS NOT NULL
                 AND execution_attempt_id IS NOT NULL AND attempt_number IS NOT NULL AND checkpoint_sequence IS NULL
                 AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL
                 AND pipeline_acceptance_authorization_id IS NULL
                 AND pipeline_profile_calibration_authorization_id IS NULL AND actor_user_id IS NULL
                 AND stage_result_json IS NOT NULL AND stage_result_digest IS NOT NULL AND inventory_digest IS NOT NULL) OR
                (commit_kind='checkpoint' AND pipeline_run_id IS NOT NULL AND pipeline_stage_run_id IS NOT NULL
                 AND execution_attempt_id IS NOT NULL AND attempt_number IS NOT NULL AND checkpoint_sequence IS NOT NULL
                 AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL
                 AND pipeline_acceptance_authorization_id IS NULL
                 AND pipeline_profile_calibration_authorization_id IS NULL AND actor_user_id IS NULL
                 AND stage_result_json IS NULL AND stage_result_digest IS NULL AND inventory_digest IS NULL) OR
                (commit_kind='input_import' AND pipeline_input_import_id IS NOT NULL AND actor_user_id IS NOT NULL
                 AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL
                 AND pipeline_input_materialization_id IS NULL AND pipeline_acceptance_authorization_id IS NULL
                 AND pipeline_profile_calibration_authorization_id IS NULL) OR
                (commit_kind='input_materialization' AND pipeline_input_materialization_id IS NOT NULL
                 AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL
                 AND execution_attempt_id IS NULL AND pipeline_input_import_id IS NULL
                 AND pipeline_acceptance_authorization_id IS NULL
                 AND pipeline_profile_calibration_authorization_id IS NULL) OR
                (commit_kind='acceptance_evidence' AND pipeline_acceptance_authorization_id IS NOT NULL
                 AND acceptance_action IN ('matrix','soak') AND acceptance_candidate_sha256 IS NOT NULL
                 AND acceptance_result_kind IN ('success','terminal') AND actor_user_id IS NOT NULL
                 AND ((acceptance_result_kind='success' AND acceptance_termination_reason IS NULL) OR
                      (acceptance_result_kind='terminal' AND acceptance_termination_reason IS NOT NULL))
                 AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL
                 AND pipeline_input_import_id IS NULL AND pipeline_input_materialization_id IS NULL
                 AND pipeline_profile_calibration_authorization_id IS NULL) OR
                (commit_kind='profile_calibration_evidence'
                 AND pipeline_profile_calibration_authorization_id IS NOT NULL
                 AND profile_calibration_spec_sha256 IS NOT NULL
                 AND profile_calibration_result_kind IN ('certification','catalog','terminal')
                 AND actor_user_id IS NOT NULL AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL
                 AND execution_attempt_id IS NULL AND pipeline_input_import_id IS NULL
                 AND pipeline_input_materialization_id IS NULL
                 AND pipeline_acceptance_authorization_id IS NULL)))
        )
        """,
        "CREATE UNIQUE INDEX artifact_upload_sessions_final_request_uidx ON artifact_upload_sessions"
        "(execution_attempt_id,idempotency_key) WHERE commit_kind='final_output'",
        "CREATE UNIQUE INDEX artifact_upload_sessions_checkpoint_request_uidx ON artifact_upload_sessions"
        "(execution_attempt_id,checkpoint_sequence,idempotency_key) WHERE commit_kind='checkpoint'",
        "CREATE UNIQUE INDEX artifact_upload_sessions_import_request_uidx ON artifact_upload_sessions"
        "(pipeline_input_import_id,idempotency_key) WHERE commit_kind='input_import'",
        "CREATE UNIQUE INDEX artifact_upload_sessions_materialization_request_uidx ON artifact_upload_sessions"
        "(pipeline_input_materialization_id,idempotency_key) WHERE commit_kind='input_materialization'",
        "CREATE UNIQUE INDEX artifact_upload_sessions_acceptance_uidx ON artifact_upload_sessions"
        "(pipeline_acceptance_authorization_id,acceptance_action,acceptance_candidate_sha256) "
        "WHERE commit_kind='acceptance_evidence'",
        "CREATE UNIQUE INDEX artifact_upload_sessions_profile_certification_uidx ON artifact_upload_sessions"
        "(pipeline_profile_calibration_authorization_id,profile_calibration_spec_sha256,"
        "profile_calibration_scenario_id,profile_calibration_candidate_identity_sha256,"
        "profile_calibration_run_ordinal) WHERE commit_kind='profile_calibration_evidence' "
        "AND profile_calibration_result_kind='certification'",
        "CREATE UNIQUE INDEX artifact_upload_sessions_profile_final_uidx ON artifact_upload_sessions"
        "(pipeline_profile_calibration_authorization_id,profile_calibration_spec_sha256) "
        "WHERE commit_kind='profile_calibration_evidence' "
        "AND profile_calibration_result_kind IN ('catalog','terminal')",
        "ALTER TABLE pipeline_input_imports ADD CONSTRAINT pipeline_input_imports_upload_session_fk "
        "FOREIGN KEY(artifact_upload_session_id) REFERENCES artifact_upload_sessions(id)",
        "ALTER TABLE pipeline_input_materializations ADD CONSTRAINT "
        "pipeline_input_materializations_upload_session_fk FOREIGN KEY(artifact_upload_session_id) "
        "REFERENCES artifact_upload_sessions(id)",
        """
        CREATE TABLE artifact_upload_files (
            session_id UUID NOT NULL REFERENCES artifact_upload_sessions(id) ON DELETE CASCADE,
            file_index INTEGER NOT NULL CHECK (file_index >= 0), preallocated_artifact_id UUID NOT NULL,
            relative_path TEXT NOT NULL, artifact_name TEXT NOT NULL, artifact_type TEXT NOT NULL,
            producer TEXT NOT NULL CHECK (producer IN ('container','platform','service')),
            media_type TEXT NOT NULL, role TEXT NOT NULL CHECK (role IN ('semantic_document','payload','payload_archive')),
            archive_format TEXT NOT NULL CHECK (archive_format IN ('none','tar','tar.zst','zip')),
            expected_max_bytes BIGINT NOT NULL CHECK (expected_max_bytes > 0),
            expected_sha256 TEXT, expected_size BIGINT, multipart_upload_id TEXT,
            computed_sha256 TEXT, actual_size BIGINT,
            state TEXT NOT NULL DEFAULT 'planned' CHECK (state IN ('planned','uploading','uploaded','verified','aborted')),
            ordered_part_receipts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            PRIMARY KEY(session_id,file_index),
            CONSTRAINT artifact_upload_files_path_uidx UNIQUE(
                session_id,preallocated_artifact_id,relative_path),
            CONSTRAINT artifact_upload_files_expected_group_check CHECK (
                (expected_sha256 IS NULL) = (expected_size IS NULL)),
            CONSTRAINT artifact_upload_files_actual_group_check CHECK (
                (computed_sha256 IS NULL) = (actual_size IS NULL))
        )
        """,
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_producer_kind_check",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_identity_group_check",
        "ALTER TABLE artifacts ADD COLUMN pipeline_input_import_id UUID REFERENCES pipeline_input_imports(id) ON DELETE RESTRICT",
        "ALTER TABLE artifacts ADD COLUMN pipeline_input_materialization_id UUID REFERENCES pipeline_input_materializations(id) ON DELETE RESTRICT",
        "ALTER TABLE artifacts ADD COLUMN pipeline_acceptance_authorization_id UUID",
        "ALTER TABLE artifacts ADD COLUMN acceptance_action TEXT",
        "ALTER TABLE artifacts ADD COLUMN acceptance_candidate_sha256 TEXT",
        "ALTER TABLE artifacts ADD COLUMN acceptance_result_kind TEXT",
        "ALTER TABLE artifacts ADD COLUMN acceptance_termination_reason TEXT",
        "ALTER TABLE artifacts ADD COLUMN pipeline_profile_calibration_authorization_id UUID",
        "ALTER TABLE artifacts ADD COLUMN profile_calibration_spec_sha256 TEXT",
        "ALTER TABLE artifacts ADD COLUMN profile_calibration_result_kind TEXT",
        "ALTER TABLE artifacts ADD COLUMN profile_calibration_scenario_id TEXT",
        "ALTER TABLE artifacts ADD COLUMN profile_calibration_candidate_identity_sha256 TEXT",
        "ALTER TABLE artifacts ADD COLUMN profile_calibration_run_ordinal INTEGER",
        "ALTER TABLE artifacts ADD COLUMN profile_calibration_source_pipeline_run_id UUID REFERENCES pipeline_runs(id) ON DELETE RESTRICT",
        "ALTER TABLE artifacts ADD COLUMN profile_calibration_termination_reason TEXT",
        "ALTER TABLE artifacts ADD COLUMN actor_user_id UUID REFERENCES users(id)",
        "ALTER TABLE artifacts ADD COLUMN artifact_upload_session_id UUID REFERENCES artifact_upload_sessions(id) ON DELETE RESTRICT",
        "ALTER TABLE artifacts ADD COLUMN manifest_sha256 TEXT",
        "ALTER TABLE artifacts ADD COLUMN stored_size_bytes BIGINT",
        "ALTER TABLE artifacts ADD COLUMN unpacked_size_bytes BIGINT",
        "ALTER TABLE artifacts ADD COLUMN file_count INTEGER",
        "ALTER TABLE artifacts ADD CONSTRAINT artifacts_pipeline_producer_kind_check CHECK "
        "(producer_kind IS NULL OR producer_kind IN ('container','platform','checkpoint','input_import',"
        "'recipe_input_materialization','pipeline_acceptance_evidence','pipeline_profile_calibration_evidence'))",
        "ALTER TABLE artifacts ADD CONSTRAINT artifacts_pipeline_identity_group_check CHECK "
        "(producer_kind IS NULL OR "
        "(producer_kind IN ('container','platform','checkpoint') AND pipeline_run_id IS NOT NULL "
        "AND pipeline_stage_run_id IS NOT NULL AND execution_attempt_id IS NOT NULL) OR "
        "(producer_kind='input_import' AND pipeline_input_import_id IS NOT NULL AND pipeline_run_id IS NULL "
        "AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL) OR "
        "(producer_kind='recipe_input_materialization' AND pipeline_input_materialization_id IS NOT NULL "
        "AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL) OR "
        "(producer_kind IN ('pipeline_acceptance_evidence','pipeline_profile_calibration_evidence') "
        "AND pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL))",
        "ALTER TABLE artifacts ADD CONSTRAINT artifacts_pipeline_manifest_group_check CHECK "
        "((artifact_upload_session_id IS NULL AND manifest_sha256 IS NULL AND stored_size_bytes IS NULL "
        "AND unpacked_size_bytes IS NULL AND file_count IS NULL) OR "
        "(artifact_upload_session_id IS NOT NULL AND manifest_sha256 IS NOT NULL AND stored_size_bytes IS NOT NULL "
        "AND unpacked_size_bytes IS NOT NULL AND file_count IS NOT NULL))",
        "CREATE UNIQUE INDEX artifacts_acceptance_result_uidx ON artifacts "
        "(pipeline_acceptance_authorization_id,acceptance_action,acceptance_candidate_sha256) "
        "WHERE producer_kind='pipeline_acceptance_evidence'",
        "CREATE UNIQUE INDEX artifacts_profile_certification_uidx ON artifacts "
        "(pipeline_profile_calibration_authorization_id,profile_calibration_spec_sha256,"
        "profile_calibration_scenario_id,profile_calibration_candidate_identity_sha256,profile_calibration_run_ordinal) "
        "WHERE producer_kind='pipeline_profile_calibration_evidence' AND profile_calibration_result_kind='certification'",
        "CREATE UNIQUE INDEX artifacts_profile_final_uidx ON artifacts "
        "(pipeline_profile_calibration_authorization_id,profile_calibration_spec_sha256) "
        "WHERE producer_kind='pipeline_profile_calibration_evidence' "
        "AND profile_calibration_result_kind IN ('catalog','terminal')",
        "ALTER TABLE pipeline_input_imports ADD CONSTRAINT pipeline_input_imports_committed_artifact_fk "
        "FOREIGN KEY(committed_artifact_id) REFERENCES artifacts(id)",
        """
        CREATE TABLE pipeline_checkpoints (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            execution_attempt_id UUID NOT NULL REFERENCES execution_attempts(id) ON DELETE CASCADE,
            checkpoint_sequence BIGINT NOT NULL CHECK (checkpoint_sequence >= 0),
            artifact_id UUID NOT NULL UNIQUE REFERENCES artifacts(id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT pipeline_checkpoints_sequence_uidx UNIQUE(execution_attempt_id,checkpoint_sequence)
        )
        """,
        """
        CREATE TABLE pipeline_acceptance_evidence_runs (
            artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
            run_ordinal INTEGER NOT NULL, result_kind TEXT NOT NULL, run_kind TEXT NOT NULL,
            scenario_id TEXT, lane_or_input_set TEXT, provenance_digest TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(artifact_id,run_ordinal)
        )
        """,
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    statements = (
        "DROP TABLE pipeline_acceptance_evidence_runs",
        "DROP TABLE pipeline_checkpoints",
        "ALTER TABLE pipeline_input_imports DROP CONSTRAINT pipeline_input_imports_committed_artifact_fk",
        "DROP INDEX artifacts_profile_final_uidx",
        "DROP INDEX artifacts_profile_certification_uidx",
        "DROP INDEX artifacts_acceptance_result_uidx",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_manifest_group_check",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_identity_group_check",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_producer_kind_check",
        "ALTER TABLE artifacts DROP COLUMN file_count",
        "ALTER TABLE artifacts DROP COLUMN unpacked_size_bytes",
        "ALTER TABLE artifacts DROP COLUMN stored_size_bytes",
        "ALTER TABLE artifacts DROP COLUMN manifest_sha256",
        "ALTER TABLE artifacts DROP COLUMN artifact_upload_session_id",
        "ALTER TABLE artifacts DROP COLUMN actor_user_id",
        "ALTER TABLE artifacts DROP COLUMN profile_calibration_termination_reason",
        "ALTER TABLE artifacts DROP COLUMN profile_calibration_source_pipeline_run_id",
        "ALTER TABLE artifacts DROP COLUMN profile_calibration_run_ordinal",
        "ALTER TABLE artifacts DROP COLUMN profile_calibration_candidate_identity_sha256",
        "ALTER TABLE artifacts DROP COLUMN profile_calibration_scenario_id",
        "ALTER TABLE artifacts DROP COLUMN profile_calibration_result_kind",
        "ALTER TABLE artifacts DROP COLUMN profile_calibration_spec_sha256",
        "ALTER TABLE artifacts DROP COLUMN pipeline_profile_calibration_authorization_id",
        "ALTER TABLE artifacts DROP COLUMN acceptance_termination_reason",
        "ALTER TABLE artifacts DROP COLUMN acceptance_result_kind",
        "ALTER TABLE artifacts DROP COLUMN acceptance_candidate_sha256",
        "ALTER TABLE artifacts DROP COLUMN acceptance_action",
        "ALTER TABLE artifacts DROP COLUMN pipeline_acceptance_authorization_id",
        "ALTER TABLE artifacts DROP COLUMN pipeline_input_materialization_id",
        "ALTER TABLE artifacts DROP COLUMN pipeline_input_import_id",
        "ALTER TABLE artifacts ADD CONSTRAINT artifacts_pipeline_producer_kind_check CHECK "
        "(producer_kind IS NULL OR producer_kind IN ('container','platform','checkpoint'))",
        "ALTER TABLE artifacts ADD CONSTRAINT artifacts_pipeline_identity_group_check CHECK "
        "((pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL AND execution_attempt_id IS NULL "
        "AND producer_kind IS NULL) OR (pipeline_run_id IS NOT NULL AND pipeline_stage_run_id IS NOT NULL "
        "AND execution_attempt_id IS NOT NULL AND producer_kind IS NOT NULL))",
        "DROP TABLE artifact_upload_files",
        "ALTER TABLE pipeline_input_materializations DROP CONSTRAINT pipeline_input_materializations_upload_session_fk",
        "ALTER TABLE pipeline_input_imports DROP CONSTRAINT pipeline_input_imports_upload_session_fk",
        "DROP TABLE artifact_upload_sessions",
        "DROP TABLE pipeline_input_materializations",
        "DROP TABLE pipeline_input_imports",
    )
    for statement in statements:
        op.execute(statement)
