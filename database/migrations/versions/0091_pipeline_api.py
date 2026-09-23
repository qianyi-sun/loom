"""Public Pipeline API idempotency and reserved controller identity.

Revision ID: 0091
Revises: 0090
"""

from alembic import op

revision = "0091"
down_revision = "0090"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE pipeline_runs ADD COLUMN display_name TEXT;
        ALTER TABLE pipeline_runs ADD COLUMN control_binding_snapshots_json JSONB
          NOT NULL DEFAULT '[]'::jsonb;
        ALTER TABLE pipeline_runs ADD COLUMN control_binding_snapshots_digest TEXT
          NOT NULL DEFAULT 'sha256:37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570';
        ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_control_bindings_digest_check
          CHECK (control_binding_snapshots_digest ~ '^sha256:[0-9a-f]{64}$');
        CREATE TABLE api_idempotency_records (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          team_id UUID REFERENCES teams(id) ON DELETE CASCADE,
          endpoint TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          request_digest TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'pending',
          resource_type TEXT,
          resource_id UUID,
          response_status INTEGER,
          response_json JSONB,
          owner_token UUID NOT NULL DEFAULT gen_random_uuid(),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          completed_at TIMESTAMPTZ,
          expires_at TIMESTAMPTZ NOT NULL,
          CONSTRAINT api_idempotency_records_state_check
            CHECK (state IN ('pending','completed','failed')),
          CONSTRAINT api_idempotency_records_digest_check
            CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT api_idempotency_records_key_check CHECK (
            octet_length(idempotency_key) BETWEEN 1 AND 128
            AND idempotency_key=btrim(idempotency_key)
            AND idempotency_key !~ '[[:cntrl:]]'),
          CONSTRAINT api_idempotency_records_response_check CHECK (
            (state='pending' AND response_status IS NULL AND resource_id IS NULL
             AND completed_at IS NULL) OR
            (state IN ('completed','failed') AND response_status IS NOT NULL
             AND completed_at IS NOT NULL)),
          CONSTRAINT api_idempotency_records_scope_check CHECK (
            (team_id IS NULL AND endpoint IN
              ('judge_profile_apply','provider_binding_apply')) OR team_id IS NOT NULL)
        );
        CREATE UNIQUE INDEX api_idempotency_records_team_endpoint_key_uidx
          ON api_idempotency_records(team_id,endpoint,idempotency_key)
          WHERE team_id IS NOT NULL;
        CREATE UNIQUE INDEX api_idempotency_records_global_endpoint_key_uidx
          ON api_idempotency_records(endpoint,idempotency_key)
          WHERE team_id IS NULL;
        CREATE INDEX api_idempotency_records_expiry_idx
          ON api_idempotency_records(expires_at);

        INSERT INTO users (
          id, email, username, username_normalized, display_name, password_hash,
          status, disabled_at, is_platform_admin
        ) VALUES (
          '00000000-0000-4000-8000-000000001216', NULL,
          'loom-pipeline-acceptance-controller',
          'loom-pipeline-acceptance-controller',
          'Loom Pipeline acceptance controller', NULL,
          'active', NULL, false
        ) ON CONFLICT (id) DO NOTHING;
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM users
             WHERE id='00000000-0000-4000-8000-000000001216'
               AND username='loom-pipeline-acceptance-controller'
               AND username_normalized='loom-pipeline-acceptance-controller'
               AND email IS NULL AND password_hash IS NULL
               AND is_platform_admin=false AND disabled_at IS NULL)
          OR EXISTS (
            SELECT 1 FROM team_memberships
             WHERE user_id='00000000-0000-4000-8000-000000001216')
          THEN
            RAISE EXCEPTION 'reserved Pipeline controller identity collision or drift';
          END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM api_idempotency_records)
             OR EXISTS (SELECT 1 FROM pipeline_runs WHERE display_name IS NOT NULL)
          THEN
            RAISE EXCEPTION 'cannot downgrade 0091 after Pipeline API state exists';
          END IF;
        END $$;
        DELETE FROM users
         WHERE id='00000000-0000-4000-8000-000000001216'
           AND username='loom-pipeline-acceptance-controller';
        DROP INDEX IF EXISTS api_idempotency_records_expiry_idx;
        DROP INDEX IF EXISTS api_idempotency_records_global_endpoint_key_uidx;
        DROP INDEX IF EXISTS api_idempotency_records_team_endpoint_key_uidx;
        DROP TABLE api_idempotency_records;
        ALTER TABLE pipeline_runs DROP CONSTRAINT pipeline_runs_control_bindings_digest_check;
        ALTER TABLE pipeline_runs DROP COLUMN control_binding_snapshots_digest;
        ALTER TABLE pipeline_runs DROP COLUMN control_binding_snapshots_json;
        ALTER TABLE pipeline_runs DROP COLUMN display_name;
        """
    )
