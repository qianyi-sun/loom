"""Immutable judge profiles and recipe Provider bindings.

Revision ID: 0092
Revises: 0091
"""

from alembic import op

revision = "0092"
down_revision = "0091"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE judge_execution_profiles (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          profile_id UUID NOT NULL,
          recipe_name TEXT NOT NULL,
          recipe_version INTEGER NOT NULL,
          profile_name TEXT NOT NULL,
          version INTEGER NOT NULL CHECK (version > 0),
          status TEXT NOT NULL CHECK (status IN ('active','disabled')),
          environment TEXT NOT NULL,
          provider_connection_id UUID NOT NULL REFERENCES provider_connections(id) ON DELETE RESTRICT,
          agent_adapter TEXT NOT NULL,
          recipe_digest TEXT NOT NULL,
          snapshot_json JSONB NOT NULL,
          snapshot_bytes BYTEA NOT NULL,
          snapshot_sha256 TEXT NOT NULL CHECK (snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          allowed_team_ids UUID[] NOT NULL,
          is_current BOOLEAN NOT NULL DEFAULT true,
          created_by UUID NOT NULL REFERENCES users(id),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_by UUID NOT NULL REFERENCES users(id),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT judge_execution_profiles_document_check CHECK (
            octet_length(snapshot_bytes)>1 AND
            get_byte(snapshot_bytes,octet_length(snapshot_bytes)-1)=10),
          CONSTRAINT judge_execution_profiles_identity_uidx UNIQUE
            (recipe_name,recipe_version,profile_name,version)
        );
        CREATE UNIQUE INDEX judge_execution_profiles_current_uidx
          ON judge_execution_profiles(recipe_name,recipe_version,profile_name)
          WHERE is_current;
        CREATE INDEX judge_execution_profiles_profile_version_idx
          ON judge_execution_profiles(profile_id,version);

        CREATE TABLE recipe_provider_bindings (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          binding_id UUID NOT NULL,
          recipe_name TEXT NOT NULL,
          recipe_version INTEGER NOT NULL,
          logical_name TEXT NOT NULL,
          version INTEGER NOT NULL CHECK (version > 0),
          status TEXT NOT NULL CHECK (status IN ('active','disabled')),
          environment TEXT NOT NULL,
          provider_connection_id UUID NOT NULL REFERENCES provider_connections(id) ON DELETE RESTRICT,
          recipe_digest TEXT NOT NULL,
          snapshot_json JSONB NOT NULL,
          snapshot_bytes BYTEA NOT NULL,
          snapshot_sha256 TEXT NOT NULL CHECK (snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          allowed_team_ids UUID[] NOT NULL,
          is_current BOOLEAN NOT NULL DEFAULT true,
          created_by UUID NOT NULL REFERENCES users(id),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_by UUID NOT NULL REFERENCES users(id),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT recipe_provider_bindings_document_check CHECK (
            octet_length(snapshot_bytes)>1 AND
            get_byte(snapshot_bytes,octet_length(snapshot_bytes)-1)=10),
          CONSTRAINT recipe_provider_bindings_identity_uidx UNIQUE
            (recipe_name,recipe_version,logical_name,version)
        );
        CREATE UNIQUE INDEX recipe_provider_bindings_current_uidx
          ON recipe_provider_bindings(recipe_name,recipe_version,logical_name)
          WHERE is_current;
        CREATE INDEX recipe_provider_bindings_object_version_idx
          ON recipe_provider_bindings(binding_id,version);

        CREATE TABLE pipeline_run_control_bindings (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
          logical_name TEXT NOT NULL,
          kind TEXT NOT NULL CHECK (kind IN ('judge_profile','provider')),
          node_key TEXT NOT NULL,
          source_object_id UUID NOT NULL,
          source_version INTEGER NOT NULL CHECK (source_version > 0),
          snapshot_json JSONB NOT NULL,
          snapshot_bytes BYTEA NOT NULL,
          snapshot_sha256 TEXT NOT NULL CHECK (snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          provider_connection_id UUID NOT NULL REFERENCES provider_connections(id) ON DELETE RESTRICT,
          provider_request_limit INTEGER NOT NULL CHECK (provider_request_limit > 0),
          provider_cost_limit_microusd BIGINT NOT NULL CHECK (provider_cost_limit_microusd > 0),
          per_call_timeout_seconds INTEGER NOT NULL CHECK (per_call_timeout_seconds > 0),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT pipeline_run_control_bindings_document_check CHECK (
            octet_length(snapshot_bytes)>1 AND
            get_byte(snapshot_bytes,octet_length(snapshot_bytes)-1)=10),
          CONSTRAINT pipeline_run_control_bindings_name_uidx UNIQUE
            (pipeline_run_id,logical_name),
          CONSTRAINT pipeline_run_control_bindings_node_uidx UNIQUE
            (pipeline_run_id,node_key)
        );
        CREATE INDEX pipeline_run_control_bindings_source_idx
          ON pipeline_run_control_bindings(kind,source_object_id,source_version);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_run_control_bindings)
             OR EXISTS (SELECT 1 FROM judge_execution_profiles)
             OR EXISTS (SELECT 1 FROM recipe_provider_bindings)
          THEN RAISE EXCEPTION 'cannot downgrade 0092 after control-binding state exists';
          END IF;
        END $$;
        DROP TABLE pipeline_run_control_bindings;
        DROP TABLE recipe_provider_bindings;
        DROP TABLE judge_execution_profiles;
        """
    )
