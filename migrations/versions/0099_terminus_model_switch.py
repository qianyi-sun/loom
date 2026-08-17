"""Terminus-2 model switch plan, execution, and LLM correlation.

Revision ID: 0099
Revises: 0098
"""

from alembic import op

revision = "0099"
down_revision = "0098"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE model_switch_plans (
          id UUID PRIMARY KEY,
          trial_id UUID NOT NULL UNIQUE REFERENCES trials(id) ON DELETE CASCADE,
          combination_idx INTEGER NOT NULL DEFAULT 0,
          k1 INTEGER NOT NULL CHECK (k1 >= 2),
          k2 INTEGER NOT NULL CHECK (k2 > k1),
          teacher_episodes INTEGER NOT NULL CHECK (teacher_episodes >= 1),
          seed TEXT NOT NULL,
          prng_version TEXT NOT NULL,
          student_model_snapshot JSONB NOT NULL,
          teacher_model_snapshot JSONB NOT NULL,
          provider_connection_id UUID REFERENCES provider_connections(id) ON DELETE RESTRICT,
          pricing_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
          capability_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
          inherited_from_plan_id UUID REFERENCES model_switch_plans(id) ON DELETE SET NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX model_switch_plans_provider_idx
          ON model_switch_plans(provider_connection_id);

        CREATE TABLE terminus_agent_executions (
          id UUID PRIMARY KEY,
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
          step_id TEXT NOT NULL,
          model_switch_plan_id UUID REFERENCES model_switch_plans(id) ON DELETE SET NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (trial_id, step_id)
        );

        CREATE TABLE terminus_agent_run_attempts (
          id UUID PRIMARY KEY,
          execution_id UUID NOT NULL
            REFERENCES terminus_agent_executions(id) ON DELETE CASCADE,
          attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
          worker_id UUID REFERENCES workers(id) ON DELETE SET NULL,
          state TEXT NOT NULL DEFAULT 'running'
            CHECK (state IN ('running','succeeded','failed','recovery_failed')),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (execution_id, attempt_number)
        );

        CREATE TABLE episode_checkpoints (
          id UUID PRIMARY KEY,
          execution_id UUID NOT NULL
            REFERENCES terminus_agent_executions(id) ON DELETE CASCADE,
          run_attempt_id UUID NOT NULL
            REFERENCES terminus_agent_run_attempts(id) ON DELETE CASCADE,
          version INTEGER NOT NULL CHECK (version >= 1),
          episode INTEGER NOT NULL CHECK (episode >= 1),
          tmux_session_id TEXT,
          active_role TEXT NOT NULL CHECK (active_role IN ('student','teacher')),
          last_call_ordinal INTEGER NOT NULL DEFAULT 0,
          last_seq INTEGER NOT NULL DEFAULT 0,
          checksum TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (execution_id, version)
        );
        CREATE INDEX episode_checkpoints_execution_idx
          ON episode_checkpoints(execution_id, version DESC);

        CREATE TABLE llm_call_intents (
          id UUID PRIMARY KEY,
          client_call_id UUID NOT NULL UNIQUE,
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
          step_id TEXT NOT NULL,
          agent_execution_id UUID
            REFERENCES terminus_agent_executions(id) ON DELETE SET NULL,
          agent_run_attempt_id UUID
            REFERENCES terminus_agent_run_attempts(id) ON DELETE SET NULL,
          episode INTEGER NOT NULL CHECK (episode >= 0),
          call_ordinal INTEGER NOT NULL CHECK (call_ordinal >= 1),
          requested_model TEXT NOT NULL,
          role TEXT NOT NULL CHECK (role IN ('student','teacher')),
          status TEXT NOT NULL DEFAULT 'registered'
            CHECK (status IN ('registered','completed','failed')),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (agent_execution_id, agent_run_attempt_id, episode, call_ordinal)
        );

        ALTER TABLE llm_calls
          ADD COLUMN client_call_id UUID,
          ADD COLUMN agent_execution_id UUID
            REFERENCES terminus_agent_executions(id) ON DELETE SET NULL,
          ADD COLUMN agent_run_attempt_id UUID
            REFERENCES terminus_agent_run_attempts(id) ON DELETE SET NULL,
          ADD COLUMN episode INTEGER,
          ADD COLUMN call_ordinal INTEGER,
          ADD COLUMN requested_model TEXT,
          ADD COLUMN response_model TEXT,
          ADD COLUMN role TEXT
            CHECK (role IS NULL OR role IN ('student','teacher')),
          ADD COLUMN correlation_status TEXT NOT NULL DEFAULT 'legacy_uncorrelated'
            CHECK (correlation_status IN ('correlated','legacy_uncorrelated'));
        CREATE UNIQUE INDEX llm_calls_client_call_id_uidx
          ON llm_calls(client_call_id) WHERE client_call_id IS NOT NULL;
        CREATE UNIQUE INDEX llm_calls_execution_ordinal_uidx
          ON llm_calls(agent_execution_id, agent_run_attempt_id, episode, call_ordinal)
          WHERE agent_execution_id IS NOT NULL
            AND agent_run_attempt_id IS NOT NULL
            AND episode IS NOT NULL
            AND call_ordinal IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS llm_calls_execution_ordinal_uidx;
        DROP INDEX IF EXISTS llm_calls_client_call_id_uidx;
        ALTER TABLE llm_calls
          DROP COLUMN IF EXISTS correlation_status,
          DROP COLUMN IF EXISTS role,
          DROP COLUMN IF EXISTS response_model,
          DROP COLUMN IF EXISTS requested_model,
          DROP COLUMN IF EXISTS call_ordinal,
          DROP COLUMN IF EXISTS episode,
          DROP COLUMN IF EXISTS agent_run_attempt_id,
          DROP COLUMN IF EXISTS agent_execution_id,
          DROP COLUMN IF EXISTS client_call_id;
        DROP TABLE IF EXISTS llm_call_intents;
        DROP TABLE IF EXISTS episode_checkpoints;
        DROP TABLE IF EXISTS terminus_agent_run_attempts;
        DROP TABLE IF EXISTS terminus_agent_executions;
        DROP TABLE IF EXISTS model_switch_plans;
        """
    )
