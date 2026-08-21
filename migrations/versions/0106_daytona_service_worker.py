"""Durable Daytona service-worker sandbox lifecycle ledger.

Revision ID: 0106
Revises: 0105
"""

from alembic import op

revision = "0106"
down_revision = "0105"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE daytona_sandboxes (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE RESTRICT,
          attempt_count INTEGER NOT NULL,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
          worker_id UUID NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
          candidate_sha TEXT NOT NULL,
          provider_scope TEXT NOT NULL,
          artifact_ref TEXT NOT NULL,
          sandbox_name TEXT NOT NULL,
          sandbox_id TEXT,
          state TEXT NOT NULL,
          deadline_at TIMESTAMPTZ NOT NULL,
          started_at TIMESTAMPTZ,
          deleted_at TIMESTAMPTZ,
          usage_reported_at TIMESTAMPTZ,
          cleanup_lease_worker_id UUID REFERENCES workers(id) ON DELETE SET NULL,
          cleanup_lease_expires_at TIMESTAMPTZ,
          last_error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT daytona_sandboxes_attempt_check CHECK (attempt_count > 0),
          CONSTRAINT daytona_sandboxes_candidate_check
            CHECK (candidate_sha ~ '^[0-9a-f]{40}$'),
          CONSTRAINT daytona_sandboxes_provider_scope_check
            CHECK (provider_scope ~ '^[0-9a-f]{64}$'),
          CONSTRAINT daytona_sandboxes_artifact_check
            CHECK (artifact_ref ~ '^[^[:space:]@]+@sha256:[0-9a-f]{64}$'),
          CONSTRAINT daytona_sandboxes_state_check
            CHECK (state IN ('reserved','running','delete_pending','deleted')),
          CONSTRAINT daytona_sandboxes_terminal_check CHECK (
            (state = 'deleted' AND deleted_at IS NOT NULL)
            OR (state <> 'deleted' AND deleted_at IS NULL)
          ),
          CONSTRAINT daytona_sandboxes_trial_attempt_uidx
            UNIQUE (trial_id, attempt_count),
          CONSTRAINT daytona_sandboxes_name_uidx UNIQUE (sandbox_name),
          CONSTRAINT daytona_sandboxes_id_uidx UNIQUE (sandbox_id)
        );
        CREATE INDEX daytona_sandboxes_cleanup_idx
          ON daytona_sandboxes (state, deadline_at, cleanup_lease_expires_at)
          WHERE state <> 'deleted';
        CREATE INDEX daytona_sandboxes_team_created_idx
          ON daytona_sandboxes (team_id, created_at, id);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM daytona_sandboxes) THEN
            RAISE EXCEPTION 'cannot downgrade 0106 with Daytona sandbox ledger data';
          END IF;
        END $$;
        DROP TABLE daytona_sandboxes;
        """
    )
