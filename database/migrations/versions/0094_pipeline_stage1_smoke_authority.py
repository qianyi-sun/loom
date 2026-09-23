"""Add the candidate-bound BEHAVIOR Stage 1 smoke authority ledger.

Revision ID: 0094
Revises: 0093
"""

from alembic import op

revision = "0094"
down_revision = "0093"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE pipeline_stage1_smoke_authorizations (
          authorization_id UUID PRIMARY KEY,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
          operator_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          environment TEXT NOT NULL,
          candidate_json JSONB NOT NULL,
          candidate_bytes BYTEA NOT NULL,
          candidate_sha256 TEXT NOT NULL,
          authorization_json JSONB NOT NULL,
          authorization_bytes BYTEA NOT NULL,
          authorization_sha256 TEXT NOT NULL,
          preflight_json JSONB NOT NULL,
          preflight_bytes BYTEA NOT NULL,
          preflight_sha256 TEXT NOT NULL,
          nonce_sha256 TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          request_digest TEXT NOT NULL,
          signature_key_id TEXT NOT NULL,
          signature_sha256 TEXT NOT NULL,
          policy_activation_id UUID NOT NULL UNIQUE
            REFERENCES pipeline_scoped_policy_activations(id) ON DELETE RESTRICT,
          pipeline_run_id UUID NOT NULL UNIQUE
            REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
          state TEXT NOT NULL,
          evidence_sha256 TEXT,
          cleanup_sha256 TEXT,
          authorized_at TIMESTAMPTZ NOT NULL,
          expires_at TIMESTAMPTZ NOT NULL,
          start_by TIMESTAMPTZ NOT NULL,
          cleanup_deadline TIMESTAMPTZ NOT NULL,
          consumed_at TIMESTAMPTZ NOT NULL,
          finished_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          version BIGINT NOT NULL DEFAULT 0,
          CONSTRAINT pipeline_stage1_smoke_authorizations_state_check CHECK (
            state IN ('submitted','running','cleanup_required','accepted','rejected')),
          CONSTRAINT pipeline_stage1_smoke_authorizations_digest_check CHECK (
            candidate_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            authorization_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            preflight_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            nonce_sha256 ~ '^sha256:[0-9a-f]{64}$' AND
            request_digest ~ '^sha256:[0-9a-f]{64}$' AND
            signature_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT pipeline_stage1_smoke_authorizations_identity_check CHECK (
            length(environment) BETWEEN 1 AND 256 AND
            length(idempotency_key) BETWEEN 1 AND 128 AND
            idempotency_key = btrim(idempotency_key) AND
            idempotency_key ~ '^[ -~]+$' AND
            signature_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'),
          CONSTRAINT pipeline_stage1_smoke_authorizations_document_check CHECK (
            octet_length(candidate_bytes) BETWEEN 2 AND 1048576 AND
            get_byte(candidate_bytes, octet_length(candidate_bytes)-1)=10 AND
            octet_length(authorization_bytes) > 1 AND
            get_byte(authorization_bytes, octet_length(authorization_bytes)-1)=10 AND
            octet_length(preflight_bytes) > 1 AND
            get_byte(preflight_bytes, octet_length(preflight_bytes)-1)=10),
          CONSTRAINT pipeline_stage1_smoke_authorizations_window_check CHECK (
            expires_at > authorized_at AND cleanup_deadline > start_by),
          CONSTRAINT pipeline_stage1_smoke_authorizations_version_check CHECK (version >= 0),
          CONSTRAINT pipeline_stage1_smoke_authorizations_result_digest_check CHECK (
            (evidence_sha256 IS NULL OR evidence_sha256 ~ '^sha256:[0-9a-f]{64}$') AND
            (cleanup_sha256 IS NULL OR cleanup_sha256 ~ '^sha256:[0-9a-f]{64}$')),
          CONSTRAINT pipeline_stage1_smoke_authorizations_terminal_check CHECK (
            state NOT IN ('accepted','rejected') OR
            (evidence_sha256 IS NOT NULL AND cleanup_sha256 IS NOT NULL AND
             finished_at IS NOT NULL)),
          CONSTRAINT pipeline_stage1_smoke_authorizations_candidate_uidx
            UNIQUE(candidate_sha256),
          CONSTRAINT pipeline_stage1_smoke_authorizations_nonce_uidx UNIQUE(nonce_sha256),
          CONSTRAINT pipeline_stage1_smoke_authorizations_team_idempotency_uidx
            UNIQUE(team_id,idempotency_key)
        );
        CREATE UNIQUE INDEX pipeline_stage1_smoke_authorizations_active_environment_uidx
          ON pipeline_stage1_smoke_authorizations(environment)
          WHERE state IN ('submitted','running','cleanup_required');

        CREATE TABLE pipeline_stage1_smoke_events (
          authorization_id UUID NOT NULL
            REFERENCES pipeline_stage1_smoke_authorizations(authorization_id) ON DELETE CASCADE,
          seq BIGINT NOT NULL,
          event_kind TEXT NOT NULL,
          payload_json JSONB NOT NULL,
          payload_bytes BYTEA NOT NULL,
          payload_sha256 TEXT NOT NULL,
          observed_at TIMESTAMPTZ NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY(authorization_id,seq),
          CONSTRAINT pipeline_stage1_smoke_events_seq_check CHECK (seq > 0),
          CONSTRAINT pipeline_stage1_smoke_events_kind_check CHECK (
            event_kind IN ('live_action_consumed','evidence_recorded','cleanup_complete',
                           'accepted','rejected')),
          CONSTRAINT pipeline_stage1_smoke_events_digest_check CHECK (
            payload_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT pipeline_stage1_smoke_events_document_check CHECK (
            octet_length(payload_bytes) > 1 AND
            get_byte(payload_bytes, octet_length(payload_bytes)-1)=10),
          CONSTRAINT pipeline_stage1_smoke_events_kind_uidx
            UNIQUE(authorization_id,event_kind)
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_stage1_smoke_authorizations LIMIT 1) THEN
            RAISE EXCEPTION '0094 downgrade refused: Stage 1 smoke authority state exists';
          END IF;
        END $$;
        DROP TABLE pipeline_stage1_smoke_events;
        DROP TABLE pipeline_stage1_smoke_authorizations;
        """
    )
