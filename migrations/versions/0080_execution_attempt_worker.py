"""Add the unified ExecutionAttempt worker protocol.

Revision ID: 0080
Revises: 0079
Create Date: 2026-08-10
"""

from __future__ import annotations

from alembic import op

revision = "0080"
down_revision = "0079"
branch_labels = None
depends_on = None


def upgrade() -> None:
    statements = (
        "ALTER TABLE workers ADD COLUMN supported_work_kinds TEXT[] NOT NULL "
        "DEFAULT ARRAY['trial']::text[]",
        "ALTER TABLE workers ADD COLUMN capability_snapshot_digest TEXT",
        "ALTER TABLE workers ADD COLUMN auth_token_hash BYTEA",
        "ALTER TABLE workers ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 1",
        "ALTER TABLE workers ADD CONSTRAINT workers_supported_work_kinds_check "
        "CHECK (supported_work_kinds = ARRAY['trial']::text[] OR "
        "supported_work_kinds = ARRAY['trial','execution_attempt']::text[])",
        "ALTER TABLE workers ADD CONSTRAINT workers_lease_epoch_positive_check "
        "CHECK (lease_epoch > 0)",
        "ALTER TABLE execution_attempts ADD COLUMN heartbeat_phase TEXT",
        "ALTER TABLE execution_attempts ADD COLUMN heartbeat_runtime_seconds DOUBLE PRECISION",
        "ALTER TABLE execution_attempts ADD COLUMN last_heartbeat_at TIMESTAMPTZ",
        "ALTER TABLE execution_attempts ADD COLUMN container_id TEXT",
        "ALTER TABLE execution_attempts ADD COLUMN runtime_started_at TIMESTAMPTZ",
        "ALTER TABLE execution_attempts ADD COLUMN input_view_digest TEXT",
        "ALTER TABLE execution_attempts ADD COLUMN step_jwt_id UUID",
        "ALTER TABLE pipeline_stage_runs ADD COLUMN image_runtime_contract_json JSONB",
        "ALTER TABLE pipeline_stage_runs ADD COLUMN image_runtime_contract_digest TEXT",
        "ALTER TABLE pipeline_stage_runs ADD COLUMN provider_connection_ref UUID",
        "ALTER TABLE pipeline_stage_runs ADD COLUMN secret_refs TEXT[] NOT NULL DEFAULT '{}'::text[]",
        "ALTER TABLE pipeline_stage_runs ADD CONSTRAINT pipeline_stage_runs_image_runtime_group_check "
        "CHECK ((image_runtime_contract_json IS NULL) = (image_runtime_contract_digest IS NULL))",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "ADD COLUMN authorization_snapshot_sha256 TEXT",
        "ALTER TABLE execution_attempts ADD CONSTRAINT execution_attempts_heartbeat_phase_check "
        "CHECK (heartbeat_phase IS NULL OR heartbeat_phase IN "
        "('input_materializing','container_starting','running','output_committing','cancelling'))",
        "ALTER TABLE execution_attempts ADD CONSTRAINT execution_attempts_runtime_nonnegative_check "
        "CHECK (heartbeat_runtime_seconds IS NULL OR heartbeat_runtime_seconds >= 0)",
        "ALTER TABLE execution_attempts ADD CONSTRAINT execution_attempts_runtime_start_group_check "
        "CHECK ((container_id IS NULL AND runtime_started_at IS NULL AND input_view_digest IS NULL) "
        "OR (container_id IS NOT NULL AND runtime_started_at IS NOT NULL "
        "AND input_view_digest IS NOT NULL))",
        """
        CREATE TABLE execution_attempt_requests (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            execution_attempt_id UUID NOT NULL REFERENCES execution_attempts(id) ON DELETE CASCADE,
            route TEXT NOT NULL,
            request_id UUID NOT NULL,
            request_digest TEXT NOT NULL,
            response_json JSONB NOT NULL,
            status_code INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT execution_attempt_requests_route_uidx
                UNIQUE (execution_attempt_id, route, request_id)
        )
        """,
        """
        CREATE TABLE execution_attempt_worker_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            execution_attempt_id UUID NOT NULL REFERENCES execution_attempts(id) ON DELETE CASCADE,
            local_seq BIGINT NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            stream TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            message_bytes INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT execution_attempt_events_seq_nonnegative CHECK (local_seq >= 0),
            CONSTRAINT execution_attempt_events_stream_check
                CHECK (stream IN ('stdout','stderr','worker')),
            CONSTRAINT execution_attempt_events_message_bytes_check
                CHECK (message_bytes BETWEEN 0 AND 65536),
            CONSTRAINT execution_attempt_events_seq_uidx UNIQUE (execution_attempt_id, local_seq)
        )
        """,
        """
        CREATE TABLE execution_attempt_control_commands (
            execution_attempt_id UUID NOT NULL REFERENCES execution_attempts(id) ON DELETE CASCADE,
            seq BIGINT NOT NULL,
            command TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (execution_attempt_id, seq),
            CONSTRAINT execution_attempt_commands_seq_positive CHECK (seq > 0),
            CONSTRAINT execution_attempt_commands_command_check CHECK (
                command IN ('cancel_requested','rotate_step_jwt','drain_after_attempt')
            )
        )
        """,
        """
        CREATE FUNCTION execution_attempts_inflight_delta() RETURNS TRIGGER AS $$
        DECLARE
            was_active boolean := OLD.state IN ('claimed', 'running');
            is_active boolean := NEW.state IN ('claimed', 'running');
            attempt_team_id uuid;
        BEGIN
            IF was_active = is_active THEN
                RETURN NEW;
            END IF;
            SELECT r.team_id INTO STRICT attempt_team_id
              FROM pipeline_stage_runs s
              JOIN pipeline_runs r ON r.id = s.pipeline_run_id
             WHERE s.id = NEW.stage_run_id;
            UPDATE team_quotas
               SET in_flight_count = GREATEST(
                   in_flight_count + CASE WHEN is_active THEN 1 ELSE -1 END,
                   0
               )
             WHERE team_id = attempt_team_id;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
        "CREATE TRIGGER execution_attempts_inflight_count "
        "AFTER UPDATE OF state ON execution_attempts "
        "FOR EACH ROW EXECUTE FUNCTION execution_attempts_inflight_delta()",
        "ALTER TABLE llm_calls ALTER COLUMN trial_id DROP NOT NULL",
        "ALTER TABLE llm_calls ADD COLUMN execution_attempt_id UUID "
        "REFERENCES execution_attempts(id) ON DELETE RESTRICT",
        "ALTER TABLE llm_calls ADD CONSTRAINT llm_calls_exactly_one_subject_check "
        "CHECK ((trial_id IS NOT NULL)::integer + "
        "(execution_attempt_id IS NOT NULL)::integer = 1)",
        "CREATE INDEX llm_calls_execution_attempt_idx "
        "ON llm_calls(execution_attempt_id, captured_at)",
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    statements = (
        "DROP INDEX IF EXISTS llm_calls_execution_attempt_idx",
        "ALTER TABLE llm_calls DROP CONSTRAINT IF EXISTS llm_calls_exactly_one_subject_check",
        "ALTER TABLE llm_calls DROP COLUMN execution_attempt_id",
        "ALTER TABLE llm_calls ALTER COLUMN trial_id SET NOT NULL",
        "DROP TRIGGER IF EXISTS execution_attempts_inflight_count ON execution_attempts",
        "DROP FUNCTION IF EXISTS execution_attempts_inflight_delta()",
        "DROP TABLE execution_attempt_control_commands",
        "DROP TABLE execution_attempt_worker_events",
        "DROP TABLE execution_attempt_requests",
        "ALTER TABLE execution_attempts DROP CONSTRAINT execution_attempts_runtime_start_group_check",
        "ALTER TABLE execution_attempts DROP CONSTRAINT execution_attempts_runtime_nonnegative_check",
        "ALTER TABLE execution_attempts DROP CONSTRAINT execution_attempts_heartbeat_phase_check",
        "ALTER TABLE execution_attempts DROP COLUMN step_jwt_id",
        "ALTER TABLE execution_attempts DROP COLUMN input_view_digest",
        "ALTER TABLE execution_attempts DROP COLUMN runtime_started_at",
        "ALTER TABLE execution_attempts DROP COLUMN container_id",
        "ALTER TABLE execution_attempts DROP COLUMN last_heartbeat_at",
        "ALTER TABLE execution_attempts DROP COLUMN heartbeat_runtime_seconds",
        "ALTER TABLE execution_attempts DROP COLUMN heartbeat_phase",
        "ALTER TABLE pipeline_stage_runs DROP CONSTRAINT pipeline_stage_runs_image_runtime_group_check",
        "ALTER TABLE pipeline_stage_runs DROP COLUMN secret_refs",
        "ALTER TABLE pipeline_stage_runs DROP COLUMN provider_connection_ref",
        "ALTER TABLE pipeline_stage_runs DROP COLUMN image_runtime_contract_digest",
        "ALTER TABLE pipeline_stage_runs DROP COLUMN image_runtime_contract_json",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "DROP COLUMN authorization_snapshot_sha256",
        "ALTER TABLE workers DROP CONSTRAINT workers_supported_work_kinds_check",
        "ALTER TABLE workers DROP CONSTRAINT workers_lease_epoch_positive_check",
        "ALTER TABLE workers DROP COLUMN capability_snapshot_digest",
        "ALTER TABLE workers DROP COLUMN auth_token_hash",
        "ALTER TABLE workers DROP COLUMN lease_epoch",
        "ALTER TABLE workers DROP COLUMN supported_work_kinds",
    )
    for statement in statements:
        op.execute(statement)
