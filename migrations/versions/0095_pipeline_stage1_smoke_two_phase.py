"""Make Stage 1 capacity and cleanup explicit two-phase mutations.

Revision ID: 0095
Revises: 0094
"""

from alembic import op

revision = "0095"
down_revision = "0094"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_stage1_smoke_authorizations LIMIT 1) THEN
            RAISE EXCEPTION
              '0095 upgrade refused: pre-two-phase Stage 1 smoke state exists';
          END IF;
        END $$;

        DROP INDEX pipeline_stage1_smoke_authorizations_active_environment_uidx;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_state_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_digest_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_identity_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_document_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_result_digest_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_terminal_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_team_idempotency_uidx;

        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN idempotency_key TO execute_idempotency_key;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN request_digest TO execute_request_digest;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN signature_key_id TO execute_signature_key_id;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN signature_sha256 TO execute_signature_sha256;

        ALTER TABLE pipeline_stage1_smoke_authorizations
          ALTER COLUMN preflight_json DROP NOT NULL,
          ALTER COLUMN preflight_bytes DROP NOT NULL,
          ALTER COLUMN preflight_sha256 DROP NOT NULL,
          ALTER COLUMN execute_idempotency_key DROP NOT NULL,
          ALTER COLUMN execute_request_digest DROP NOT NULL,
          ALTER COLUMN execute_signature_key_id DROP NOT NULL,
          ALTER COLUMN execute_signature_sha256 DROP NOT NULL,
          ALTER COLUMN pipeline_run_id DROP NOT NULL,
          ALTER COLUMN consumed_at DROP NOT NULL,
          ADD COLUMN capacity_idempotency_key TEXT NOT NULL,
          ADD COLUMN capacity_request_digest TEXT NOT NULL,
          ADD COLUMN capacity_signature_key_id TEXT NOT NULL,
          ADD COLUMN capacity_signature_sha256 TEXT NOT NULL,
          ADD COLUMN cleanup_begin_json JSONB,
          ADD COLUMN cleanup_begin_bytes BYTEA,
          ADD COLUMN cleanup_begin_sha256 TEXT,
          ADD COLUMN cleanup_begin_signature_key_id TEXT,
          ADD COLUMN cleanup_begin_signature_sha256 TEXT,
          ADD COLUMN cleanup_began_at TIMESTAMPTZ,
          ADD COLUMN cleanup_signature_key_id TEXT,
          ADD COLUMN cleanup_signature_sha256 TEXT;

        ALTER TABLE pipeline_stage1_smoke_authorizations
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_state_check CHECK (
            state IN ('capacity_pending','capacity_draining','capacity_aborted',
                      'submitted','running','cleanup_required','cleanup_draining',
                      'accepted','rejected')),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_digest_check CHECK (
            candidate_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            authorization_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            nonce_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            capacity_request_digest ~ '^sha256:[0-9a-f]{64}$' AND
            capacity_signature_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            (preflight_sha256 IS NULL OR preflight_sha256 ~ '^sha256:[0-9a-f]{64}$') AND
            (execute_request_digest IS NULL OR
             execute_request_digest ~ '^sha256:[0-9a-f]{64}$') AND
            (execute_signature_sha256 IS NULL OR
             execute_signature_sha256 ~ '^sha256:[0-9a-f]{64}$')),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_identity_check CHECK (
            length(environment) BETWEEN 1 AND 256 AND
            length(capacity_idempotency_key) BETWEEN 1 AND 128 AND
            capacity_idempotency_key = btrim(capacity_idempotency_key) AND
            capacity_idempotency_key ~ '^[ -~]+$' AND
            (execute_idempotency_key IS NULL OR (
              length(execute_idempotency_key) BETWEEN 1 AND 128 AND
              execute_idempotency_key = btrim(execute_idempotency_key) AND
              execute_idempotency_key ~ '^[ -~]+$')) AND
            capacity_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$' AND
            (execute_signature_key_id IS NULL OR
             execute_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$') AND
            (cleanup_begin_signature_key_id IS NULL OR
             cleanup_begin_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$') AND
            (cleanup_signature_key_id IS NULL OR
             cleanup_signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$')),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_document_check CHECK (
            octet_length(candidate_bytes) BETWEEN 2 AND 1048576 AND
            get_byte(candidate_bytes, octet_length(candidate_bytes)-1)=10 AND
            octet_length(authorization_bytes) > 1 AND
            get_byte(authorization_bytes, octet_length(authorization_bytes)-1)=10 AND
            ((preflight_json IS NULL AND preflight_bytes IS NULL) OR
             (jsonb_typeof(preflight_json) = 'object' AND
              octet_length(preflight_bytes) > 1 AND
              get_byte(preflight_bytes, octet_length(preflight_bytes)-1)=10)) AND
            ((cleanup_begin_json IS NULL AND cleanup_begin_bytes IS NULL) OR
             (jsonb_typeof(cleanup_begin_json) = 'object' AND
              octet_length(cleanup_begin_bytes) > 1 AND
              get_byte(cleanup_begin_bytes, octet_length(cleanup_begin_bytes)-1)=10))),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_execution_phase_check CHECK (
            (state IN ('capacity_pending','capacity_draining','capacity_aborted') AND
             preflight_json IS NULL AND
             preflight_bytes IS NULL AND preflight_sha256 IS NULL AND
             execute_idempotency_key IS NULL AND execute_request_digest IS NULL AND
             execute_signature_key_id IS NULL AND execute_signature_sha256 IS NULL AND
             pipeline_run_id IS NULL AND consumed_at IS NULL) OR
            (state NOT IN ('capacity_pending','capacity_draining','capacity_aborted') AND
             preflight_json IS NOT NULL AND
             preflight_bytes IS NOT NULL AND preflight_sha256 IS NOT NULL AND
             execute_idempotency_key IS NOT NULL AND execute_request_digest IS NOT NULL AND
             execute_signature_key_id IS NOT NULL AND execute_signature_sha256 IS NOT NULL AND
             pipeline_run_id IS NOT NULL AND consumed_at IS NOT NULL)),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_cleanup_phase_check CHECK (
            (state IN ('capacity_pending','submitted','running','cleanup_required') AND
             cleanup_begin_json IS NULL AND cleanup_begin_bytes IS NULL AND
             cleanup_begin_sha256 IS NULL AND cleanup_begin_signature_key_id IS NULL AND
             cleanup_begin_signature_sha256 IS NULL AND cleanup_began_at IS NULL) OR
            (state IN ('capacity_draining','capacity_aborted','cleanup_draining',
                       'accepted','rejected') AND
             cleanup_begin_json IS NOT NULL AND cleanup_begin_bytes IS NOT NULL AND
             cleanup_begin_sha256 IS NOT NULL AND cleanup_begin_signature_key_id IS NOT NULL AND
             cleanup_begin_signature_sha256 IS NOT NULL AND cleanup_began_at IS NOT NULL)),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_result_digest_check CHECK (
            (evidence_sha256 IS NULL OR evidence_sha256 ~ '^sha256:[0-9a-f]{64}$') AND
            (cleanup_begin_sha256 IS NULL OR
             cleanup_begin_sha256 ~ '^sha256:[0-9a-f]{64}$') AND
            (cleanup_begin_signature_sha256 IS NULL OR
             cleanup_begin_signature_sha256 ~ '^sha256:[0-9a-f]{64}$') AND
            (cleanup_sha256 IS NULL OR cleanup_sha256 ~ '^sha256:[0-9a-f]{64}$') AND
            (cleanup_signature_sha256 IS NULL OR
             cleanup_signature_sha256 ~ '^sha256:[0-9a-f]{64}$')),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_evidence_phase_check CHECK (
            (state IN ('capacity_pending','capacity_draining','capacity_aborted',
                       'submitted','running') AND
             evidence_sha256 IS NULL) OR
            (state IN ('cleanup_required','cleanup_draining','accepted','rejected') AND
             evidence_sha256 IS NOT NULL)),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_terminal_check CHECK (
            (state IN ('capacity_aborted','accepted','rejected') AND
             cleanup_sha256 IS NOT NULL AND
             cleanup_signature_key_id IS NOT NULL AND
             cleanup_signature_sha256 IS NOT NULL AND finished_at IS NOT NULL) OR
            (state NOT IN ('capacity_aborted','accepted','rejected') AND
             cleanup_sha256 IS NULL AND
             cleanup_signature_key_id IS NULL AND
             cleanup_signature_sha256 IS NULL AND finished_at IS NULL)),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_team_capacity_idempotency_uidx
            UNIQUE(team_id,capacity_idempotency_key),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_team_execute_idempotency_uidx
            UNIQUE(team_id,execute_idempotency_key);

        CREATE UNIQUE INDEX pipeline_stage1_smoke_authorizations_active_environment_uidx
          ON pipeline_stage1_smoke_authorizations(environment)
          WHERE state IN ('capacity_pending','capacity_draining','submitted','running',
                          'cleanup_required','cleanup_draining');

        ALTER TABLE pipeline_stage1_smoke_events
          DROP CONSTRAINT pipeline_stage1_smoke_events_kind_check,
          ADD CONSTRAINT pipeline_stage1_smoke_events_kind_check CHECK (
            event_kind IN ('capacity_preflight_started','live_action_consumed',
                           'evidence_recorded','cleanup_started','cleanup_complete',
                           'capacity_aborted','accepted','rejected'));
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_stage1_smoke_authorizations LIMIT 1) THEN
            RAISE EXCEPTION '0095 downgrade refused: two-phase Stage 1 smoke state exists';
          END IF;
        END $$;

        ALTER TABLE pipeline_stage1_smoke_events
          DROP CONSTRAINT pipeline_stage1_smoke_events_kind_check,
          ADD CONSTRAINT pipeline_stage1_smoke_events_kind_check CHECK (
            event_kind IN ('live_action_consumed','evidence_recorded','cleanup_complete',
                           'accepted','rejected'));

        DROP INDEX pipeline_stage1_smoke_authorizations_active_environment_uidx;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_state_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_digest_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_identity_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_document_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_execution_phase_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_cleanup_phase_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_result_digest_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_evidence_phase_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_terminal_check,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_team_capacity_idempotency_uidx,
          DROP CONSTRAINT pipeline_stage1_smoke_authorizations_team_execute_idempotency_uidx,
          DROP COLUMN capacity_idempotency_key,
          DROP COLUMN capacity_request_digest,
          DROP COLUMN capacity_signature_key_id,
          DROP COLUMN capacity_signature_sha256,
          DROP COLUMN cleanup_begin_json,
          DROP COLUMN cleanup_begin_bytes,
          DROP COLUMN cleanup_begin_sha256,
          DROP COLUMN cleanup_begin_signature_key_id,
          DROP COLUMN cleanup_begin_signature_sha256,
          DROP COLUMN cleanup_began_at,
          DROP COLUMN cleanup_signature_key_id,
          DROP COLUMN cleanup_signature_sha256;

        ALTER TABLE pipeline_stage1_smoke_authorizations
          ALTER COLUMN preflight_json SET NOT NULL,
          ALTER COLUMN preflight_bytes SET NOT NULL,
          ALTER COLUMN preflight_sha256 SET NOT NULL,
          ALTER COLUMN execute_idempotency_key SET NOT NULL,
          ALTER COLUMN execute_request_digest SET NOT NULL,
          ALTER COLUMN execute_signature_key_id SET NOT NULL,
          ALTER COLUMN execute_signature_sha256 SET NOT NULL,
          ALTER COLUMN pipeline_run_id SET NOT NULL,
          ALTER COLUMN consumed_at SET NOT NULL;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN execute_idempotency_key TO idempotency_key;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN execute_request_digest TO request_digest;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN execute_signature_key_id TO signature_key_id;
        ALTER TABLE pipeline_stage1_smoke_authorizations
          RENAME COLUMN execute_signature_sha256 TO signature_sha256;

        ALTER TABLE pipeline_stage1_smoke_authorizations
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_state_check CHECK (
            state IN ('submitted','running','cleanup_required','accepted','rejected')),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_digest_check CHECK (
            candidate_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            authorization_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            preflight_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            nonce_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            request_digest ~ '^sha256:[0-9a-f]{64}$' AND
            signature_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_identity_check CHECK (
            length(environment) BETWEEN 1 AND 256 AND
            length(idempotency_key) BETWEEN 1 AND 128 AND
            idempotency_key = btrim(idempotency_key) AND
            idempotency_key ~ '^[ -~]+$' AND
            signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_document_check CHECK (
            octet_length(candidate_bytes) BETWEEN 2 AND 1048576 AND
            get_byte(candidate_bytes, octet_length(candidate_bytes)-1)=10 AND
            octet_length(authorization_bytes) > 1 AND
            get_byte(authorization_bytes, octet_length(authorization_bytes)-1)=10 AND
            octet_length(preflight_bytes) > 1 AND
            get_byte(preflight_bytes, octet_length(preflight_bytes)-1)=10),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_result_digest_check CHECK (
            (evidence_sha256 IS NULL OR evidence_sha256 ~ '^sha256:[0-9a-f]{64}$') AND
            (cleanup_sha256 IS NULL OR cleanup_sha256 ~ '^sha256:[0-9a-f]{64}$')),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_terminal_check CHECK (
            state NOT IN ('accepted','rejected') OR
            (evidence_sha256 IS NOT NULL AND cleanup_sha256 IS NOT NULL AND
             finished_at IS NOT NULL)),
          ADD CONSTRAINT pipeline_stage1_smoke_authorizations_team_idempotency_uidx
            UNIQUE(team_id,idempotency_key);
        CREATE UNIQUE INDEX pipeline_stage1_smoke_authorizations_active_environment_uidx
          ON pipeline_stage1_smoke_authorizations(environment)
          WHERE state IN ('submitted','running','cleanup_required');
        """
    )
