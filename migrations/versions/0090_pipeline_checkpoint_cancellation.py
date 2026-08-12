"""Pipeline checkpoint authority and positive cleanup acknowledgement.

Revision ID: 0090
Revises: 0089
"""

from alembic import op

revision = "0090"
down_revision = "0089"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE execution_attempts
          ADD COLUMN cleanup_acknowledged_at TIMESTAMPTZ,
          ADD COLUMN cleanup_proof_json JSONB,
          ADD COLUMN cleanup_proof_digest TEXT,
          ADD CONSTRAINT execution_attempts_cleanup_proof_group_check CHECK (
            (cleanup_acknowledged_at IS NULL AND cleanup_proof_json IS NULL
             AND cleanup_proof_digest IS NULL) OR
            (cleanup_acknowledged_at IS NOT NULL AND cleanup_proof_json IS NOT NULL
             AND cleanup_proof_digest ~ '^sha256:[0-9a-f]{64}$'));

        ALTER TABLE artifact_upload_sessions
          ADD COLUMN checkpoint_envelope_json JSONB,
          ADD COLUMN checkpoint_envelope_digest TEXT,
          ADD CONSTRAINT artifact_upload_sessions_checkpoint_envelope_group_check CHECK (
            ((checkpoint_envelope_json IS NULL) = (checkpoint_envelope_digest IS NULL)) AND
            (checkpoint_envelope_json IS NULL OR commit_kind='checkpoint') AND
            (checkpoint_envelope_digest IS NULL OR
             checkpoint_envelope_digest ~ '^sha256:[0-9a-f]{64}$'));

        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_checkpoints) THEN
            RAISE EXCEPTION '0090 requires empty pre-authority pipeline_checkpoints';
          END IF;
        END $$;
        ALTER TABLE pipeline_checkpoints
          RENAME COLUMN created_at TO committed_at;
        ALTER TABLE pipeline_checkpoints
          ADD COLUMN pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
          ADD COLUMN pipeline_stage_run_id UUID NOT NULL REFERENCES pipeline_stage_runs(id) ON DELETE CASCADE,
          ADD COLUMN attempt_number INTEGER NOT NULL,
          ADD COLUMN recipe_digest TEXT NOT NULL,
          ADD COLUMN resolved_input_bindings_digest TEXT NOT NULL,
          ADD COLUMN execution_spec_digest TEXT NOT NULL,
          ADD COLUMN image_digest TEXT NOT NULL,
          ADD COLUMN resume_compatibility_key TEXT NOT NULL,
          ADD COLUMN checkpoint_json JSONB NOT NULL,
          ADD COLUMN checkpoint_digest TEXT NOT NULL,
          ADD COLUMN source_attempt_state TEXT NOT NULL,
          ADD CONSTRAINT pipeline_checkpoints_attempt_check
            CHECK (attempt_number BETWEEN 1 AND 3),
          ADD CONSTRAINT pipeline_checkpoints_source_state_check
            CHECK (source_attempt_state IN ('claimed','running')),
          ADD CONSTRAINT pipeline_checkpoints_recipe_digest_check
            CHECK (recipe_digest ~ '^sha256:[0-9a-f]{64}$'),
          ADD CONSTRAINT pipeline_checkpoints_bindings_digest_check
            CHECK (resolved_input_bindings_digest ~ '^sha256:[0-9a-f]{64}$'),
          ADD CONSTRAINT pipeline_checkpoints_spec_digest_check
            CHECK (execution_spec_digest ~ '^sha256:[0-9a-f]{64}$'),
          ADD CONSTRAINT pipeline_checkpoints_resume_digest_check
            CHECK (resume_compatibility_key ~ '^sha256:[0-9a-f]{64}$'),
          ADD CONSTRAINT pipeline_checkpoints_document_digest_check
            CHECK (checkpoint_digest ~ '^sha256:[0-9a-f]{64}$');
        CREATE INDEX pipeline_checkpoints_stage_latest_idx
          ON pipeline_checkpoints(
            pipeline_stage_run_id, attempt_number DESC, checkpoint_sequence DESC);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_checkpoints)
             OR EXISTS (
               SELECT 1 FROM execution_attempts WHERE cleanup_acknowledged_at IS NOT NULL)
             OR EXISTS (
               SELECT 1 FROM artifact_upload_sessions WHERE checkpoint_envelope_json IS NOT NULL)
          THEN
            RAISE EXCEPTION 'cannot downgrade 0090 after checkpoint or cleanup authority exists';
          END IF;
        END $$;
        DROP INDEX IF EXISTS pipeline_checkpoints_stage_latest_idx;
        ALTER TABLE pipeline_checkpoints
          DROP CONSTRAINT IF EXISTS pipeline_checkpoints_document_digest_check,
          DROP CONSTRAINT IF EXISTS pipeline_checkpoints_resume_digest_check,
          DROP CONSTRAINT IF EXISTS pipeline_checkpoints_spec_digest_check,
          DROP CONSTRAINT IF EXISTS pipeline_checkpoints_bindings_digest_check,
          DROP CONSTRAINT IF EXISTS pipeline_checkpoints_recipe_digest_check,
          DROP CONSTRAINT IF EXISTS pipeline_checkpoints_source_state_check,
          DROP CONSTRAINT IF EXISTS pipeline_checkpoints_attempt_check,
          DROP COLUMN IF EXISTS source_attempt_state,
          DROP COLUMN IF EXISTS checkpoint_digest,
          DROP COLUMN IF EXISTS checkpoint_json,
          DROP COLUMN IF EXISTS resume_compatibility_key,
          DROP COLUMN IF EXISTS image_digest,
          DROP COLUMN IF EXISTS execution_spec_digest,
          DROP COLUMN IF EXISTS resolved_input_bindings_digest,
          DROP COLUMN IF EXISTS recipe_digest,
          DROP COLUMN IF EXISTS attempt_number,
          DROP COLUMN IF EXISTS pipeline_stage_run_id,
          DROP COLUMN IF EXISTS pipeline_run_id;
        ALTER TABLE pipeline_checkpoints RENAME COLUMN committed_at TO created_at;
        ALTER TABLE artifact_upload_sessions
          DROP CONSTRAINT IF EXISTS artifact_upload_sessions_checkpoint_envelope_group_check,
          DROP COLUMN IF EXISTS checkpoint_envelope_digest,
          DROP COLUMN IF EXISTS checkpoint_envelope_json;
        ALTER TABLE execution_attempts
          DROP CONSTRAINT IF EXISTS execution_attempts_cleanup_proof_group_check,
          DROP COLUMN IF EXISTS cleanup_proof_digest,
          DROP COLUMN IF EXISTS cleanup_proof_json,
          DROP COLUMN IF EXISTS cleanup_acknowledged_at;
        """
    )
