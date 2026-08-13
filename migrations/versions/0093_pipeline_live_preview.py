"""Bounded ephemeral Pipeline live preview backend.

Revision ID: 0093
Revises: 0092
"""

from alembic import op

revision = "0093"
down_revision = "0092"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE pipeline_live_preview_generations (
          execution_attempt_id UUID PRIMARY KEY REFERENCES execution_attempts(id) ON DELETE CASCADE,
          generation UUID NOT NULL UNIQUE,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
          pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
          pipeline_stage_run_id UUID NOT NULL REFERENCES pipeline_stage_runs(id) ON DELETE CASCADE,
          worker_id UUID NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
          claim_id UUID NOT NULL,
          lease_epoch BIGINT NOT NULL,
          state TEXT NOT NULL DEFAULT 'waiting'
            CHECK (state IN ('waiting','live','handoff','ended')),
          latest_sequence BIGINT,
          latest_step_idx NUMERIC(20,0),
          received_at TIMESTAMPTZ,
          frame_count INTEGER NOT NULL DEFAULT 0,
          total_bytes BIGINT NOT NULL DEFAULT 0,
          expires_at TIMESTAMPTZ NOT NULL,
          purge_reason TEXT,
          purged_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT pipeline_live_preview_generations_latest_group_check CHECK (
            (latest_sequence IS NULL AND latest_step_idx IS NULL AND received_at IS NULL) OR
            (latest_sequence BETWEEN 0 AND 9007199254740991 AND
             latest_step_idx BETWEEN 0 AND 18446744073709551615 AND
             received_at IS NOT NULL)),
          CONSTRAINT pipeline_live_preview_generations_bounds_check CHECK (
            frame_count BETWEEN 0 AND 64 AND total_bytes BETWEEN 0 AND 33554432),
          CONSTRAINT pipeline_live_preview_generations_purge_group_check CHECK (
            (purged_at IS NULL) = (purge_reason IS NULL)),
          CONSTRAINT pipeline_live_preview_generations_attempt_generation_check CHECK (
            generation = execution_attempt_id),
          CONSTRAINT pipeline_live_preview_generations_lease_epoch_positive_check CHECK (
            lease_epoch > 0)
        );
        CREATE INDEX pipeline_live_preview_generations_expiry_idx
          ON pipeline_live_preview_generations(expires_at);
        CREATE INDEX pipeline_live_preview_generations_team_state_idx
          ON pipeline_live_preview_generations(team_id,state);

        CREATE TABLE pipeline_live_preview_frames (
          execution_attempt_id UUID NOT NULL
            REFERENCES pipeline_live_preview_generations(execution_attempt_id) ON DELETE CASCADE,
          sequence BIGINT NOT NULL,
          step_idx NUMERIC(20,0) NOT NULL,
          jpeg_sha256 TEXT NOT NULL,
          jpeg_size_bytes INTEGER NOT NULL,
          jpeg_bytes BYTEA NOT NULL,
          idempotency_key TEXT NOT NULL,
          received_at TIMESTAMPTZ NOT NULL,
          PRIMARY KEY(execution_attempt_id,sequence),
          CONSTRAINT pipeline_live_preview_frames_sequence_check CHECK (
            sequence BETWEEN 0 AND 9007199254740991),
          CONSTRAINT pipeline_live_preview_frames_step_check CHECK (
            step_idx BETWEEN 0 AND 18446744073709551615),
          CONSTRAINT pipeline_live_preview_frames_digest_check CHECK (
            jpeg_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT pipeline_live_preview_frames_size_check CHECK (
            jpeg_size_bytes BETWEEN 1 AND 524288 AND
            octet_length(jpeg_bytes) = jpeg_size_bytes),
          CONSTRAINT pipeline_live_preview_frames_idempotency_uidx UNIQUE (
            execution_attempt_id,idempotency_key)
        );
        CREATE INDEX pipeline_live_preview_frames_received_idx
          ON pipeline_live_preview_frames(received_at);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_live_preview_generations LIMIT 1) THEN
            RAISE EXCEPTION '0093 downgrade refused: live preview state exists';
          END IF;
        END $$;
        DROP TABLE pipeline_live_preview_frames;
        DROP TABLE pipeline_live_preview_generations;
        """
    )
