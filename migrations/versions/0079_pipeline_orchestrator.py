"""Add fenced Pipeline orchestration and hard-budget persistence.

Revision ID: 0079
Revises: 0078
Create Date: 2026-08-10
"""

from __future__ import annotations

from alembic import op

revision = "0079"
down_revision = "0078"
branch_labels = None
depends_on = None


def upgrade() -> None:
    statements = (
        "ALTER TABLE pipeline_runs ADD COLUMN claimed_by TEXT",
        "ALTER TABLE pipeline_runs ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE pipeline_runs ADD COLUMN lease_expires_at TIMESTAMPTZ",
        "ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_lease_epoch_nonnegative "
        "CHECK (lease_epoch >= 0)",
        "ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_controller_lease_group_check "
        "CHECK ((claimed_by IS NULL AND lease_expires_at IS NULL) OR "
        "(claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL))",
        "CREATE INDEX pipeline_runs_controller_picker_idx "
        "ON pipeline_runs(state, lease_expires_at, created_at, id)",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "DROP CONSTRAINT pipeline_preflight_fence_fields_check",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "ADD COLUMN worker_lease_epoch BIGINT",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "ADD COLUMN slurm_cluster_id TEXT",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "ADD COLUMN slurm_cluster_config_sha256 TEXT",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "ADD COLUMN slurm_allocation_id TEXT",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "ADD CONSTRAINT pipeline_preflight_worker_lease_epoch_positive "
        "CHECK (worker_lease_epoch IS NULL OR worker_lease_epoch > 0)",
        """
        ALTER TABLE pipeline_acceptance_preflight_prerequisites
        ADD CONSTRAINT pipeline_preflight_fence_fields_check CHECK (
            (fence_state = 'pending' AND worker_id IS NULL
             AND worker_capability_snapshot_digest IS NULL AND policy_id IS NULL
             AND policy_config_sha256 IS NULL AND policy_activation_epoch IS NULL
             AND worker_lease_epoch IS NULL AND slurm_cluster_id IS NULL
             AND slurm_cluster_config_sha256 IS NULL AND slurm_allocation_id IS NULL
             AND exclusive_fence_id IS NULL AND fence_acquired_at IS NULL
             AND fence_released_at IS NULL AND fence_release_reason IS NULL)
            OR
            (fence_state = 'active' AND worker_id IS NOT NULL
             AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL
             AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL
             AND worker_lease_epoch IS NOT NULL AND slurm_cluster_id IS NOT NULL
             AND slurm_cluster_config_sha256 IS NOT NULL AND slurm_allocation_id IS NOT NULL
             AND exclusive_fence_id IS NOT NULL AND fence_acquired_at IS NOT NULL
             AND fence_released_at IS NULL AND fence_release_reason IS NULL)
            OR
            (fence_state = 'released' AND worker_id IS NOT NULL
             AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL
             AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL
             AND worker_lease_epoch IS NOT NULL AND slurm_cluster_id IS NOT NULL
             AND slurm_cluster_config_sha256 IS NOT NULL AND slurm_allocation_id IS NOT NULL
             AND exclusive_fence_id IS NOT NULL AND fence_acquired_at IS NOT NULL
             AND fence_released_at IS NOT NULL AND fence_release_reason IS NOT NULL)
        )
        """,
        """
        CREATE TABLE pipeline_budget_ledgers (
            pipeline_run_id UUID PRIMARY KEY REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            provider_limit_microusd BIGINT NOT NULL,
            provider_reserved_microusd BIGINT NOT NULL DEFAULT 0,
            provider_settled_microusd BIGINT NOT NULL DEFAULT 0,
            gpu_limit_seconds BIGINT NOT NULL,
            gpu_reserved_seconds BIGINT NOT NULL DEFAULT 0,
            gpu_settled_seconds BIGINT NOT NULL DEFAULT 0,
            artifact_limit_bytes BIGINT NOT NULL,
            artifact_reserved_bytes BIGINT NOT NULL DEFAULT 0,
            artifact_settled_bytes BIGINT NOT NULL DEFAULT 0,
            stage_run_limit BIGINT NOT NULL,
            stage_runs_created BIGINT NOT NULL DEFAULT 0,
            attempt_limit BIGINT NOT NULL,
            attempts_created BIGINT NOT NULL DEFAULT 0,
            wall_deadline_at TIMESTAMPTZ NOT NULL,
            terminal_cause TEXT,
            terminal_cause_at TIMESTAMPTZ,
            version BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT pipeline_budget_ledgers_nonnegative_check CHECK (
                provider_limit_microusd >= 0 AND provider_reserved_microusd >= 0
                AND provider_settled_microusd >= 0 AND gpu_limit_seconds >= 0
                AND gpu_reserved_seconds >= 0 AND gpu_settled_seconds >= 0
                AND artifact_limit_bytes >= 0 AND artifact_reserved_bytes >= 0
                AND artifact_settled_bytes >= 0 AND stage_run_limit >= 0
                AND stage_runs_created >= 0 AND attempt_limit >= 0 AND attempts_created >= 0
            ),
            CONSTRAINT pipeline_budget_ledgers_hard_limits_check CHECK (
                artifact_reserved_bytes + artifact_settled_bytes <= artifact_limit_bytes
                AND stage_runs_created <= stage_run_limit AND attempts_created <= attempt_limit
            ),
            CONSTRAINT pipeline_budget_ledgers_metered_limits_check CHECK (
                terminal_cause = 'accounting_violation'
                OR (provider_reserved_microusd + provider_settled_microusd <= provider_limit_microusd
                    AND gpu_reserved_seconds + gpu_settled_seconds <= gpu_limit_seconds)
            ),
            CONSTRAINT pipeline_budget_ledgers_terminal_cause_check CHECK (
                terminal_cause IS NULL OR terminal_cause IN
                ('user_cancel','provider_budget','gpu_budget','artifact_budget','stage_run_budget',
                 'attempt_budget','wall_budget','accounting_violation')
            ),
            CONSTRAINT pipeline_budget_ledgers_terminal_cause_group_check CHECK (
                (terminal_cause IS NULL) = (terminal_cause_at IS NULL)
            ),
            CONSTRAINT pipeline_budget_ledgers_version_nonnegative CHECK (version >= 0)
        )
        """,
        "CREATE INDEX pipeline_budget_ledgers_deadline_idx "
        "ON pipeline_budget_ledgers(wall_deadline_at, pipeline_run_id)",
        """
        CREATE TABLE pipeline_budget_reservations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            execution_attempt_id UUID REFERENCES execution_attempts(id) ON DELETE RESTRICT,
            kind TEXT NOT NULL,
            reservation_key TEXT NOT NULL,
            request_digest TEXT NOT NULL,
            reserved_amount BIGINT NOT NULL,
            settled_amount BIGINT,
            state TEXT NOT NULL DEFAULT 'active',
            metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            settled_at TIMESTAMPTZ,
            CONSTRAINT pipeline_budget_reservations_kind_check CHECK (
                kind IN ('provider','gpu','artifact')
            ),
            CONSTRAINT pipeline_budget_reservations_state_check CHECK (
                state IN ('active','settled','released')
            ),
            CONSTRAINT pipeline_budget_reservations_amount_check CHECK (
                reserved_amount >= 0 AND (settled_amount IS NULL OR settled_amount >= 0)
            ),
            CONSTRAINT pipeline_budget_reservations_terminal_fields_check CHECK (
                (state = 'active' AND settled_amount IS NULL AND settled_at IS NULL)
                OR (state = 'settled' AND settled_amount IS NOT NULL AND settled_at IS NOT NULL)
                OR (state = 'released' AND settled_amount IS NULL AND settled_at IS NOT NULL)
            ),
            CONSTRAINT pipeline_budget_reservations_key_namespace_check CHECK (
                (kind = 'provider' AND reservation_key ~
                    '^provider:[0-9a-f-]{36}:[0-9a-f-]{36}$')
                OR (kind = 'gpu' AND reservation_key ~ '^gpu:[0-9a-f-]{36}$')
                OR (kind = 'artifact' AND reservation_key ~
                    '^artifact:(final:[0-9a-f-]{36}|checkpoint:[0-9a-f-]{36}:[0-9]{12}|control:[a-z][a-z0-9_]{0,62}:[0-9a-f-]{36})$')
            ),
            CONSTRAINT pipeline_budget_reservations_key_uidx
                UNIQUE (pipeline_run_id, kind, reservation_key)
        )
        """,
        "CREATE INDEX pipeline_budget_reservations_run_state_idx "
        "ON pipeline_budget_reservations(pipeline_run_id, state, id)",
        """
        CREATE TABLE execution_attempt_provider_budgets (
            attempt_id UUID PRIMARY KEY REFERENCES execution_attempts(id) ON DELETE CASCADE,
            binding_snapshot_sha256 TEXT NOT NULL,
            request_limit BIGINT NOT NULL,
            requests_reserved BIGINT NOT NULL DEFAULT 0,
            requests_settled BIGINT NOT NULL DEFAULT 0,
            cost_limit_microusd BIGINT NOT NULL,
            cost_reserved_microusd BIGINT NOT NULL DEFAULT 0,
            cost_settled_microusd BIGINT NOT NULL DEFAULT 0,
            per_call_timeout_seconds BIGINT NOT NULL,
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT execution_attempt_provider_budgets_counter_check CHECK (
                request_limit > 0 AND requests_reserved >= 0 AND requests_settled >= 0
                AND requests_reserved + requests_settled <= request_limit
                AND cost_limit_microusd > 0 AND cost_reserved_microusd >= 0
                AND cost_settled_microusd >= 0 AND per_call_timeout_seconds > 0
            ),
            CONSTRAINT execution_attempt_provider_budgets_version_nonnegative CHECK (version >= 0)
        )
        """,
        """
        CREATE TABLE pipeline_cancellation_outbox (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            execution_attempt_id UUID NOT NULL REFERENCES execution_attempts(id) ON DELETE CASCADE,
            terminal_cause TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            request_json JSONB NOT NULL,
            request_digest TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',
            ack_json JSONB,
            ack_digest TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            acked_at TIMESTAMPTZ,
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT pipeline_cancellation_outbox_attempt_uidx UNIQUE (execution_attempt_id),
            CONSTRAINT pipeline_cancellation_outbox_cause_check CHECK (
                terminal_cause IN
                ('user_cancel','provider_budget','gpu_budget','artifact_budget','stage_run_budget',
                 'attempt_budget','wall_budget','accounting_violation')
            ),
            CONSTRAINT pipeline_cancellation_outbox_state_check CHECK (state IN ('pending','acked')),
            CONSTRAINT pipeline_cancellation_outbox_ack_group_check CHECK (
                (state = 'pending' AND ack_json IS NULL AND ack_digest IS NULL AND acked_at IS NULL)
                OR (state = 'acked' AND ack_json IS NOT NULL AND ack_digest IS NOT NULL
                    AND acked_at IS NOT NULL)
            ),
            CONSTRAINT pipeline_cancellation_outbox_version_nonnegative CHECK (version >= 0)
        )
        """,
        "CREATE INDEX pipeline_cancellation_outbox_state_idx "
        "ON pipeline_cancellation_outbox(state, created_at, id)",
        """
        CREATE FUNCTION validate_pipeline_budget_ledger_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.terminal_cause IS NOT NULL AND NEW.terminal_cause IS DISTINCT FROM OLD.terminal_cause
            THEN
                RAISE EXCEPTION 'Pipeline terminal cause is immutable'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.provider_limit_microusd != OLD.provider_limit_microusd
               OR NEW.gpu_limit_seconds != OLD.gpu_limit_seconds
               OR NEW.artifact_limit_bytes != OLD.artifact_limit_bytes
               OR NEW.stage_run_limit != OLD.stage_run_limit
               OR NEW.attempt_limit != OLD.attempt_limit
               OR NEW.wall_deadline_at != OLD.wall_deadline_at
               OR NEW.provider_settled_microusd < OLD.provider_settled_microusd
               OR NEW.gpu_settled_seconds < OLD.gpu_settled_seconds
               OR NEW.artifact_settled_bytes < OLD.artifact_settled_bytes
               OR NEW.stage_runs_created < OLD.stage_runs_created
               OR NEW.attempts_created < OLD.attempts_created
            THEN
                RAISE EXCEPTION 'Pipeline budget limits and settled counters are immutable/monotonic'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE TRIGGER pipeline_budget_ledger_update_trigger
        BEFORE UPDATE ON pipeline_budget_ledgers
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_budget_ledger_update()
        """,
        """
        CREATE FUNCTION validate_pipeline_budget_reservation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'UPDATE' AND OLD.state != 'active' THEN
                RAISE EXCEPTION 'terminal Pipeline budget reservation is immutable'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.execution_attempt_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM execution_attempts a
                JOIN pipeline_stage_runs s ON s.id = a.stage_run_id
                WHERE a.id = NEW.execution_attempt_id
                  AND s.pipeline_run_id = NEW.pipeline_run_id
            ) THEN
                RAISE EXCEPTION 'budget reservation Attempt belongs to another PipelineRun'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.execution_attempt_id IS NULL
               AND NEW.reservation_key !~ '^artifact:control:' THEN
                RAISE EXCEPTION 'only Pipeline control Artifacts may reserve without an Attempt'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_budget_reservation_linkage_trigger
        AFTER INSERT OR UPDATE ON pipeline_budget_reservations
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_budget_reservation()
        """,
        """
        CREATE FUNCTION validate_pipeline_cancellation_outbox() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM execution_attempts a
                JOIN pipeline_stage_runs s ON s.id = a.stage_run_id
                JOIN pipeline_budget_ledgers l ON l.pipeline_run_id = s.pipeline_run_id
                WHERE a.id = NEW.execution_attempt_id
                  AND s.pipeline_run_id = NEW.pipeline_run_id
                  AND l.terminal_cause = NEW.terminal_cause
            ) THEN
                RAISE EXCEPTION 'cancellation outbox does not match Attempt/run terminal cause'
                    USING ERRCODE = '23514';
            END IF;
            IF TG_OP = 'UPDATE' AND OLD.state = 'acked' THEN
                RAISE EXCEPTION 'acknowledged cancellation outbox row is immutable'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_cancellation_outbox_linkage_trigger
        AFTER INSERT OR UPDATE ON pipeline_cancellation_outbox
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_cancellation_outbox()
        """,
        """
        CREATE FUNCTION validate_execution_attempt_provider_budget() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.cost_reserved_microusd + NEW.cost_settled_microusd > NEW.cost_limit_microusd
               AND NOT EXISTS (
                   SELECT 1 FROM execution_attempts attempt
                   JOIN pipeline_stage_runs stage ON stage.id=attempt.stage_run_id
                   JOIN pipeline_budget_ledgers ledger
                     ON ledger.pipeline_run_id=stage.pipeline_run_id
                   WHERE attempt.id=NEW.attempt_id
                     AND attempt.reason_code='accounting_violation'
                     AND ledger.terminal_cause='accounting_violation'
               )
            THEN
                RAISE EXCEPTION 'provider Attempt budget overage requires accounting violation latch'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER execution_attempt_provider_budget_linkage_trigger
        AFTER INSERT OR UPDATE ON execution_attempt_provider_budgets
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_execution_attempt_provider_budget()
        """,
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    statements = (
        "DROP TRIGGER IF EXISTS execution_attempt_provider_budget_linkage_trigger "
        "ON execution_attempt_provider_budgets",
        "DROP FUNCTION IF EXISTS validate_execution_attempt_provider_budget()",
        "DROP TRIGGER IF EXISTS pipeline_cancellation_outbox_linkage_trigger "
        "ON pipeline_cancellation_outbox",
        "DROP FUNCTION IF EXISTS validate_pipeline_cancellation_outbox()",
        "DROP TRIGGER IF EXISTS pipeline_budget_reservation_linkage_trigger "
        "ON pipeline_budget_reservations",
        "DROP FUNCTION IF EXISTS validate_pipeline_budget_reservation()",
        "DROP TRIGGER IF EXISTS pipeline_budget_ledger_update_trigger ON pipeline_budget_ledgers",
        "DROP FUNCTION IF EXISTS validate_pipeline_budget_ledger_update()",
        "DROP TABLE pipeline_cancellation_outbox",
        "DROP TABLE execution_attempt_provider_budgets",
        "DROP TABLE pipeline_budget_reservations",
        "DROP TABLE pipeline_budget_ledgers",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "DROP CONSTRAINT pipeline_preflight_fence_fields_check",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "DROP CONSTRAINT pipeline_preflight_worker_lease_epoch_positive",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites DROP COLUMN slurm_allocation_id",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites "
        "DROP COLUMN slurm_cluster_config_sha256",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites DROP COLUMN slurm_cluster_id",
        "ALTER TABLE pipeline_acceptance_preflight_prerequisites DROP COLUMN worker_lease_epoch",
        """
        ALTER TABLE pipeline_acceptance_preflight_prerequisites
        ADD CONSTRAINT pipeline_preflight_fence_fields_check CHECK (
            (fence_state = 'pending' AND worker_id IS NULL
             AND worker_capability_snapshot_digest IS NULL AND policy_id IS NULL
             AND policy_config_sha256 IS NULL AND policy_activation_epoch IS NULL
             AND exclusive_fence_id IS NULL AND fence_acquired_at IS NULL
             AND fence_released_at IS NULL AND fence_release_reason IS NULL)
            OR
            (fence_state = 'active' AND worker_id IS NOT NULL
             AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL
             AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL
             AND exclusive_fence_id IS NOT NULL AND fence_acquired_at IS NOT NULL
             AND fence_released_at IS NULL AND fence_release_reason IS NULL)
            OR
            (fence_state = 'released' AND worker_id IS NOT NULL
             AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL
             AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL
             AND exclusive_fence_id IS NOT NULL AND fence_acquired_at IS NOT NULL
             AND fence_released_at IS NOT NULL AND fence_release_reason IS NOT NULL)
        )
        """,
        "DROP INDEX pipeline_runs_controller_picker_idx",
        "ALTER TABLE pipeline_runs DROP CONSTRAINT pipeline_runs_controller_lease_group_check",
        "ALTER TABLE pipeline_runs DROP CONSTRAINT pipeline_runs_lease_epoch_nonnegative",
        "ALTER TABLE pipeline_runs DROP COLUMN lease_expires_at",
        "ALTER TABLE pipeline_runs DROP COLUMN lease_epoch",
        "ALTER TABLE pipeline_runs DROP COLUMN claimed_by",
    )
    for statement in statements:
        op.execute(statement)
