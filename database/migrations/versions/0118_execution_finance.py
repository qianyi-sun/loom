"""Add versioned execution pricing, spend guardrails, and node-cost attribution.

Revision ID: 0118
Revises: 0117
"""

from alembic import op

revision = "0118"
down_revision = "0117"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE TABLE execution_price_snapshots (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          provider TEXT NOT NULL,
          region TEXT NOT NULL,
          sku TEXT NOT NULL,
          currency TEXT NOT NULL DEFAULT 'USD',
          source TEXT NOT NULL,
          source_version TEXT NOT NULL,
          source_uri TEXT NOT NULL,
          effective_at TIMESTAMPTZ NOT NULL,
          observed_at TIMESTAMPTZ NOT NULL,
          base_microusd_per_hour BIGINT NOT NULL DEFAULT 0,
          vcpu_microusd_per_hour BIGINT NOT NULL DEFAULT 0,
          memory_gib_microusd_per_hour BIGINT NOT NULL DEFAULT 0,
          ephemeral_storage_gib_microusd_per_hour BIGINT NOT NULL DEFAULT 0,
          rate_card_json JSONB NOT NULL,
          rate_card_sha256 TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_price_snapshots_identity_check CHECK (
            length(trim(provider)) BETWEEN 1 AND 80
            AND length(trim(region)) BETWEEN 1 AND 120
            AND length(trim(sku)) BETWEEN 1 AND 120
            AND length(trim(source)) BETWEEN 1 AND 120
            AND length(trim(source_version)) BETWEEN 1 AND 160
            AND length(trim(source_uri)) BETWEEN 1 AND 2048
          ),
          CONSTRAINT execution_price_snapshots_currency_check CHECK (currency = 'USD'),
          CONSTRAINT execution_price_snapshots_rates_check CHECK (
            base_microusd_per_hour >= 0
            AND vcpu_microusd_per_hour >= 0
            AND memory_gib_microusd_per_hour >= 0
            AND ephemeral_storage_gib_microusd_per_hour >= 0
            AND base_microusd_per_hour + vcpu_microusd_per_hour
                + memory_gib_microusd_per_hour
                + ephemeral_storage_gib_microusd_per_hour > 0
          ),
          CONSTRAINT execution_price_snapshots_digest_check CHECK (
            rate_card_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT execution_price_snapshots_source_uidx UNIQUE (
            provider, region, sku, source, source_version
          )
        );

        CREATE TABLE execution_target_price_bindings (
          target_id TEXT PRIMARY KEY REFERENCES execution_targets(id) ON DELETE RESTRICT,
          price_snapshot_id UUID NOT NULL
            REFERENCES execution_price_snapshots(id) ON DELETE RESTRICT,
          enabled BOOLEAN NOT NULL DEFAULT false,
          reason TEXT,
          version BIGINT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_target_price_bindings_reason_check CHECK (
            reason IS NULL OR length(trim(reason)) BETWEEN 1 AND 500
          )
        );

        CREATE TABLE execution_budget_policies (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          scope_kind TEXT NOT NULL,
          scope_key TEXT NOT NULL,
          daily_limit_microusd BIGINT NOT NULL,
          monthly_limit_microusd BIGINT NOT NULL,
          per_attempt_limit_microusd BIGINT NOT NULL,
          max_estimate_duration_seconds INTEGER NOT NULL,
          emergency_stop BOOLEAN NOT NULL DEFAULT false,
          enabled BOOLEAN NOT NULL DEFAULT false,
          reason TEXT,
          current_day DATE,
          current_month DATE,
          daily_reserved_microusd BIGINT NOT NULL DEFAULT 0,
          daily_settled_microusd BIGINT NOT NULL DEFAULT 0,
          monthly_reserved_microusd BIGINT NOT NULL DEFAULT 0,
          monthly_settled_microusd BIGINT NOT NULL DEFAULT 0,
          version BIGINT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_budget_policies_scope_check CHECK (
            scope_kind IN ('pool','target')
            AND length(trim(scope_key)) BETWEEN 1 AND 120
          ),
          CONSTRAINT execution_budget_policies_limits_check CHECK (
            daily_limit_microusd > 0
            AND monthly_limit_microusd >= daily_limit_microusd
            AND per_attempt_limit_microusd > 0
            AND per_attempt_limit_microusd <= daily_limit_microusd
            AND max_estimate_duration_seconds BETWEEN 1 AND 604800
          ),
          CONSTRAINT execution_budget_policies_counters_check CHECK (
            daily_reserved_microusd >= 0
            AND daily_settled_microusd >= 0
            AND monthly_reserved_microusd >= 0
            AND monthly_settled_microusd >= 0
          ),
          CONSTRAINT execution_budget_policies_period_group_check CHECK (
            (current_day IS NULL AND current_month IS NULL
             AND daily_reserved_microusd = 0 AND daily_settled_microusd = 0
             AND monthly_reserved_microusd = 0 AND monthly_settled_microusd = 0)
            OR (current_day IS NOT NULL AND current_month IS NOT NULL
                AND current_month = date_trunc('month', current_day)::date)
          ),
          CONSTRAINT execution_budget_policies_reason_check CHECK (
            reason IS NULL OR length(trim(reason)) BETWEEN 1 AND 500
          ),
          CONSTRAINT execution_budget_policies_scope_uidx UNIQUE (scope_kind, scope_key)
        );

        CREATE TABLE execution_cost_reservations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          lease_id UUID NOT NULL UNIQUE REFERENCES execution_leases(id) ON DELETE RESTRICT,
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE RESTRICT,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
          batch_id UUID REFERENCES batches(id) ON DELETE RESTRICT,
          attempt INTEGER NOT NULL,
          execution_role TEXT NOT NULL,
          pool_id TEXT NOT NULL,
          target_id TEXT NOT NULL REFERENCES execution_targets(id) ON DELETE RESTRICT,
          price_snapshot_id UUID NOT NULL
            REFERENCES execution_price_snapshots(id) ON DELETE RESTRICT,
          estimate_duration_seconds INTEGER NOT NULL,
          requested_cpu_millis INTEGER NOT NULL,
          requested_memory_mib INTEGER NOT NULL,
          requested_ephemeral_storage_mib INTEGER NOT NULL,
          estimated_cost_microusd BIGINT NOT NULL,
          estimate_sha256 TEXT NOT NULL,
          state TEXT NOT NULL DEFAULT 'reserved',
          acquired_at TIMESTAMPTZ NOT NULL,
          terminal_at TIMESTAMPTZ,
          released_at TIMESTAMPTZ,
          settled_at TIMESTAMPTZ,
          billing_complete_through TIMESTAMPTZ,
          actual_allocated_microusd BIGINT,
          release_reason TEXT,
          CONSTRAINT execution_cost_reservations_attempt_check CHECK (attempt > 0),
          CONSTRAINT execution_cost_reservations_role_check CHECK (
            execution_role IN ('attempt','verifier')
          ),
          CONSTRAINT execution_cost_reservations_resources_check CHECK (
            estimate_duration_seconds > 0
            AND requested_cpu_millis > 0
            AND requested_memory_mib > 0
            AND requested_ephemeral_storage_mib > 0
            AND estimated_cost_microusd > 0
          ),
          CONSTRAINT execution_cost_reservations_digest_check CHECK (
            estimate_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT execution_cost_reservations_state_check CHECK (
            state IN ('reserved','awaiting_settlement','settled','released')
          ),
          CONSTRAINT execution_cost_reservations_state_group_check CHECK (
            (state = 'reserved' AND terminal_at IS NULL AND released_at IS NULL
             AND settled_at IS NULL AND billing_complete_through IS NULL
             AND actual_allocated_microusd IS NULL AND release_reason IS NULL)
            OR (state = 'awaiting_settlement' AND terminal_at IS NOT NULL
                AND released_at IS NULL AND settled_at IS NULL
                AND billing_complete_through IS NULL
                AND actual_allocated_microusd IS NULL AND release_reason IS NULL)
            OR (state = 'settled' AND terminal_at IS NOT NULL
                AND released_at IS NULL AND settled_at IS NOT NULL
                AND billing_complete_through IS NOT NULL
                AND actual_allocated_microusd IS NOT NULL
                AND actual_allocated_microusd >= 0 AND release_reason IS NULL)
            OR (state = 'released' AND released_at IS NOT NULL
                AND settled_at IS NULL AND billing_complete_through IS NULL
                AND actual_allocated_microusd IS NULL
                AND length(trim(release_reason)) BETWEEN 1 AND 120)
          ),
          CONSTRAINT execution_cost_reservations_trial_attempt_role_uidx UNIQUE (
            trial_id, attempt, execution_role
          )
        );
        CREATE INDEX execution_cost_reservations_pool_state_idx
          ON execution_cost_reservations (pool_id, state, acquired_at, id);
        CREATE INDEX execution_cost_reservations_team_time_idx
          ON execution_cost_reservations (team_id, acquired_at, id);

        CREATE TABLE execution_cost_reservation_debits (
          reservation_id UUID NOT NULL
            REFERENCES execution_cost_reservations(id) ON DELETE RESTRICT,
          policy_id UUID NOT NULL REFERENCES execution_budget_policies(id) ON DELETE RESTRICT,
          budget_day DATE NOT NULL,
          budget_month DATE NOT NULL,
          reserved_microusd BIGINT NOT NULL,
          state TEXT NOT NULL DEFAULT 'active',
          actual_microusd BIGINT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (reservation_id, policy_id, budget_day),
          CONSTRAINT execution_cost_reservation_debits_amount_check CHECK (
            reserved_microusd > 0 AND (actual_microusd IS NULL OR actual_microusd >= 0)
          ),
          CONSTRAINT execution_cost_reservation_debits_period_check CHECK (
            budget_month = date_trunc('month', budget_day)::date
          ),
          CONSTRAINT execution_cost_reservation_debits_state_check CHECK (
            (state = 'active' AND actual_microusd IS NULL)
            OR (state = 'released' AND actual_microusd IS NULL)
            OR (state = 'settled' AND actual_microusd IS NOT NULL)
          )
        );

        CREATE TABLE execution_node_cost_records (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          target_id TEXT NOT NULL REFERENCES execution_targets(id) ON DELETE RESTRICT,
          price_snapshot_id UUID NOT NULL
            REFERENCES execution_price_snapshots(id) ON DELETE RESTRICT,
          provider TEXT NOT NULL,
          provider_record_id TEXT NOT NULL,
          node_identity_sha256 TEXT NOT NULL,
          interval_started_at TIMESTAMPTZ NOT NULL,
          interval_stopped_at TIMESTAMPTZ NOT NULL,
          node_cpu_millis INTEGER NOT NULL,
          node_memory_mib INTEGER NOT NULL,
          node_ephemeral_storage_mib INTEGER NOT NULL,
          provider_billed_microusd BIGINT NOT NULL,
          allocated_microusd BIGINT NOT NULL,
          idle_system_fragmentation_microusd BIGINT NOT NULL,
          currency TEXT NOT NULL DEFAULT 'USD',
          billing_source TEXT NOT NULL,
          billing_source_version TEXT NOT NULL,
          allocation_method TEXT NOT NULL,
          evidence_sha256 TEXT NOT NULL,
          observed_at TIMESTAMPTZ NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_node_cost_records_identity_check CHECK (
            length(trim(provider)) BETWEEN 1 AND 80
            AND length(trim(provider_record_id)) BETWEEN 1 AND 240
            AND node_identity_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND length(trim(billing_source)) BETWEEN 1 AND 120
            AND length(trim(billing_source_version)) BETWEEN 1 AND 160
          ),
          CONSTRAINT execution_node_cost_records_time_check CHECK (
            interval_stopped_at > interval_started_at
            AND observed_at >= interval_stopped_at
            AND interval_stopped_at <= (
              date_trunc('day', interval_started_at AT TIME ZONE 'UTC')
              + INTERVAL '1 day'
            ) AT TIME ZONE 'UTC'
          ),
          CONSTRAINT execution_node_cost_records_resource_check CHECK (
            node_cpu_millis > 0 AND node_memory_mib > 0
            AND node_ephemeral_storage_mib > 0
          ),
          CONSTRAINT execution_node_cost_records_amount_check CHECK (
            provider_billed_microusd >= 0 AND allocated_microusd >= 0
            AND idle_system_fragmentation_microusd >= 0
            AND allocated_microusd + idle_system_fragmentation_microusd
                = provider_billed_microusd
          ),
          CONSTRAINT execution_node_cost_records_currency_check CHECK (currency = 'USD'),
          CONSTRAINT execution_node_cost_records_method_check CHECK (
            allocation_method = 'dominant_requested_resource_time_v1'
          ),
          CONSTRAINT execution_node_cost_records_digest_check CHECK (
            evidence_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT execution_node_cost_records_provider_uidx UNIQUE (
            provider, provider_record_id
          )
        );
        CREATE INDEX execution_node_cost_records_target_time_idx
          ON execution_node_cost_records (target_id, interval_started_at, interval_stopped_at);

        CREATE TABLE execution_node_cost_allocations (
          node_cost_record_id UUID NOT NULL
            REFERENCES execution_node_cost_records(id) ON DELETE RESTRICT,
          cost_reservation_id UUID NOT NULL
            REFERENCES execution_cost_reservations(id) ON DELETE RESTRICT,
          lease_id UUID NOT NULL REFERENCES execution_leases(id) ON DELETE RESTRICT,
          overlap_seconds INTEGER NOT NULL,
          dominant_resource_fraction_ppb BIGINT NOT NULL,
          allocated_microusd BIGINT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (node_cost_record_id, cost_reservation_id),
          CONSTRAINT execution_node_cost_allocations_values_check CHECK (
            overlap_seconds > 0
            AND dominant_resource_fraction_ppb BETWEEN 1 AND 1000000000
            AND allocated_microusd >= 0
          )
        );
        CREATE INDEX execution_node_cost_allocations_reservation_idx
          ON execution_node_cost_allocations (cost_reservation_id, node_cost_record_id);

        CREATE FUNCTION loom_execution_price_snapshot_immutable()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'execution price snapshots are immutable';
        END;
        $$;
        CREATE TRIGGER execution_price_snapshot_immutable_trigger
          BEFORE UPDATE ON execution_price_snapshots
          FOR EACH ROW EXECUTE FUNCTION loom_execution_price_snapshot_immutable();

        CREATE FUNCTION loom_execution_node_cost_immutable()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'execution node cost evidence is immutable';
        END;
        $$;
        CREATE TRIGGER execution_node_cost_immutable_trigger
          BEFORE UPDATE ON execution_node_cost_records
          FOR EACH ROW EXECUTE FUNCTION loom_execution_node_cost_immutable();
        CREATE TRIGGER execution_node_cost_allocation_immutable_trigger
          BEFORE UPDATE ON execution_node_cost_allocations
          FOR EACH ROW EXECUTE FUNCTION loom_execution_node_cost_immutable();

        CREATE FUNCTION loom_execution_cost_reservation_immutable()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.lease_id, NEW.trial_id, NEW.team_id, NEW.batch_id,
                 NEW.attempt, NEW.execution_role, NEW.pool_id, NEW.target_id,
                 NEW.price_snapshot_id, NEW.estimate_duration_seconds,
                 NEW.requested_cpu_millis, NEW.requested_memory_mib,
                 NEW.requested_ephemeral_storage_mib,
                 NEW.estimated_cost_microusd, NEW.estimate_sha256,
                 NEW.acquired_at)
             IS DISTINCT FROM
             ROW(OLD.lease_id, OLD.trial_id, OLD.team_id, OLD.batch_id,
                 OLD.attempt, OLD.execution_role, OLD.pool_id, OLD.target_id,
                 OLD.price_snapshot_id, OLD.estimate_duration_seconds,
                 OLD.requested_cpu_millis, OLD.requested_memory_mib,
                 OLD.requested_ephemeral_storage_mib,
                 OLD.estimated_cost_microusd, OLD.estimate_sha256,
                 OLD.acquired_at) THEN
            RAISE EXCEPTION 'execution cost reservation identity is immutable';
          END IF;
          IF OLD.state IN ('settled','released') AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'terminal execution cost reservation is immutable';
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_cost_reservation_immutable_trigger
          BEFORE UPDATE ON execution_cost_reservations
          FOR EACH ROW EXECUTE FUNCTION loom_execution_cost_reservation_immutable();

        CREATE FUNCTION loom_transition_execution_cost_reservation()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        DECLARE
          reservation_row RECORD;
          debit_row RECORD;
        BEGIN
          IF NEW.observed_state IN ('cancelled','timed_out','failed','finalized','deleted')
             AND OLD.observed_state IS DISTINCT FROM NEW.observed_state THEN
            PERFORM pg_advisory_xact_lock_shared(
              hashtextextended('execution-finance-policy-mutation', 1552)
            );
            SELECT * INTO reservation_row
              FROM execution_cost_reservations
             WHERE lease_id = NEW.id AND state = 'reserved'
             FOR UPDATE;
            IF FOUND AND NEW.pod_started_at IS NULL THEN
              UPDATE execution_cost_reservations
                 SET state = 'released', terminal_at = NOW(), released_at = NOW(),
                     release_reason = 'lease_terminal_before_pod_start'
               WHERE id = reservation_row.id;
              FOR debit_row IN
                UPDATE execution_cost_reservation_debits
                   SET state = 'released', updated_at = NOW()
                 WHERE reservation_id = reservation_row.id AND state = 'active'
                RETURNING *
              LOOP
                UPDATE execution_budget_policies policy
                   SET daily_reserved_microusd = CASE
                         WHEN policy.current_day = debit_row.budget_day THEN
                           GREATEST(0, policy.daily_reserved_microusd
                                      - debit_row.reserved_microusd)
                         ELSE policy.daily_reserved_microusd END,
                       monthly_reserved_microusd = CASE
                         WHEN policy.current_month = debit_row.budget_month THEN
                           GREATEST(0, policy.monthly_reserved_microusd
                                      - debit_row.reserved_microusd)
                         ELSE policy.monthly_reserved_microusd END,
                       updated_at = NOW()
                 WHERE policy.id = debit_row.policy_id;
              END LOOP;
            ELSIF FOUND THEN
              UPDATE execution_cost_reservations
                 SET state = 'awaiting_settlement', terminal_at = NOW()
               WHERE id = reservation_row.id;
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_cost_reservation_terminal_trigger
          AFTER UPDATE OF observed_state ON execution_leases
          FOR EACH ROW EXECUTE FUNCTION loom_transition_execution_cost_reservation();
        """
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM execution_price_snapshots)
             OR EXISTS (SELECT 1 FROM execution_budget_policies)
             OR EXISTS (SELECT 1 FROM execution_cost_reservations)
             OR EXISTS (SELECT 1 FROM execution_node_cost_records) THEN
            RAISE EXCEPTION 'cannot downgrade 0118 with execution finance evidence';
          END IF;
        END;
        $$;
        DROP TRIGGER IF EXISTS execution_cost_reservation_terminal_trigger ON execution_leases;
        DROP FUNCTION IF EXISTS loom_transition_execution_cost_reservation();
        DROP TRIGGER IF EXISTS execution_cost_reservation_immutable_trigger
          ON execution_cost_reservations;
        DROP FUNCTION IF EXISTS loom_execution_cost_reservation_immutable();
        DROP TRIGGER IF EXISTS execution_node_cost_allocation_immutable_trigger
          ON execution_node_cost_allocations;
        DROP TRIGGER IF EXISTS execution_node_cost_immutable_trigger ON execution_node_cost_records;
        DROP FUNCTION IF EXISTS loom_execution_node_cost_immutable();
        DROP TRIGGER IF EXISTS execution_price_snapshot_immutable_trigger
          ON execution_price_snapshots;
        DROP FUNCTION IF EXISTS loom_execution_price_snapshot_immutable();
        DROP TABLE IF EXISTS execution_node_cost_allocations;
        DROP TABLE IF EXISTS execution_node_cost_records;
        DROP TABLE IF EXISTS execution_cost_reservation_debits;
        DROP TABLE IF EXISTS execution_cost_reservations;
        DROP TABLE IF EXISTS execution_budget_policies;
        DROP TABLE IF EXISTS execution_target_price_bindings;
        DROP TABLE IF EXISTS execution_price_snapshots;
        """
    )
