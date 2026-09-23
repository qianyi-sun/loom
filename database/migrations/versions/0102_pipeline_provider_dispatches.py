"""Persist provider dispatch identity, outcome, and accounting correlation.

Revision ID: 0102
Revises: 0101
"""

from alembic import op

revision = "0102"
down_revision = "0101"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE pipeline_provider_dispatches (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            execution_attempt_id UUID NOT NULL
                REFERENCES execution_attempts(id) ON DELETE CASCADE,
            provider_request_id UUID NOT NULL,
            reservation_id UUID NOT NULL
                REFERENCES pipeline_budget_reservations(id) ON DELETE RESTRICT,
            binding_snapshot_sha256 TEXT NOT NULL,
            request_digest TEXT NOT NULL,
            provider_connection_id UUID NOT NULL
                REFERENCES provider_connections(id) ON DELETE RESTRICT,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            wire_api TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'reserved',
            outcome TEXT,
            reserved_cost_microusd BIGINT NOT NULL,
            actual_cost_microusd BIGINT,
            upstream_attempt_count INTEGER NOT NULL DEFAULT 0,
            response_digest TEXT,
            llm_call_id UUID UNIQUE REFERENCES llm_calls(id) ON DELETE RESTRICT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            dispatched_at TIMESTAMPTZ,
            outcome_at TIMESTAMPTZ,
            settled_at TIMESTAMPTZ,
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT pipeline_provider_dispatches_attempt_request_uidx
                UNIQUE (execution_attempt_id, provider_request_id),
            CONSTRAINT pipeline_provider_dispatches_reservation_uidx
                UNIQUE (reservation_id),
            CONSTRAINT pipeline_provider_dispatches_digest_check CHECK (
                binding_snapshot_sha256 ~ '^sha256:[0-9a-f]{64}$'
                AND request_digest ~ '^sha256:[0-9a-f]{64}$'
                AND (response_digest IS NULL
                     OR response_digest ~ '^sha256:[0-9a-f]{64}$')
            ),
            CONSTRAINT pipeline_provider_dispatches_text_check CHECK (
                provider <> '' AND model <> ''
                AND wire_api IN ('responses','messages')
            ),
            CONSTRAINT pipeline_provider_dispatches_amount_check CHECK (
                reserved_cost_microusd >= 0
                AND (actual_cost_microusd IS NULL OR actual_cost_microusd >= 0)
                AND upstream_attempt_count BETWEEN 0 AND 1
                AND version >= 0
            ),
            CONSTRAINT pipeline_provider_dispatches_state_check CHECK (
                state IN ('reserved','dispatched','settled')
                AND (outcome IS NULL OR outcome IN
                     ('not_dispatched','succeeded','failed','uncertain'))
            ),
            CONSTRAINT pipeline_provider_dispatches_lifecycle_check CHECK (
                (state = 'reserved' AND outcome IS NULL
                 AND actual_cost_microusd IS NULL AND upstream_attempt_count = 0
                 AND response_digest IS NULL AND llm_call_id IS NULL
                 AND dispatched_at IS NULL AND outcome_at IS NULL AND settled_at IS NULL)
                OR
                (state = 'dispatched' AND outcome IS NULL
                 AND actual_cost_microusd IS NULL AND upstream_attempt_count = 1
                 AND response_digest IS NULL AND llm_call_id IS NULL
                 AND dispatched_at IS NOT NULL AND outcome_at IS NULL AND settled_at IS NULL)
                OR
                (state = 'settled' AND actual_cost_microusd IS NOT NULL
                 AND outcome_at IS NOT NULL AND settled_at IS NOT NULL
                 AND ((outcome = 'not_dispatched' AND actual_cost_microusd = 0
                       AND upstream_attempt_count = 0 AND llm_call_id IS NULL
                       AND dispatched_at IS NULL AND response_digest IS NULL)
                      OR (outcome IN ('succeeded','failed','uncertain')
                          AND upstream_attempt_count = 1 AND llm_call_id IS NOT NULL
                          AND dispatched_at IS NOT NULL
                          AND ((outcome = 'succeeded' AND response_digest IS NOT NULL)
                               OR (outcome <> 'succeeded' AND response_digest IS NULL)))))
            )
        );

        CREATE INDEX pipeline_provider_dispatches_attempt_state_idx
            ON pipeline_provider_dispatches(execution_attempt_id, state, created_at, id);
        CREATE INDEX pipeline_provider_dispatches_unsettled_idx
            ON pipeline_provider_dispatches(state, dispatched_at, id)
            WHERE state <> 'settled';

        CREATE FUNCTION validate_pipeline_provider_dispatch_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.execution_attempt_id != OLD.execution_attempt_id
             OR NEW.provider_request_id != OLD.provider_request_id
             OR NEW.reservation_id != OLD.reservation_id
             OR NEW.binding_snapshot_sha256 != OLD.binding_snapshot_sha256
             OR NEW.request_digest != OLD.request_digest
             OR NEW.provider_connection_id != OLD.provider_connection_id
             OR NEW.provider != OLD.provider OR NEW.model != OLD.model
             OR NEW.wire_api != OLD.wire_api
             OR NEW.reserved_cost_microusd != OLD.reserved_cost_microusd
             OR NEW.created_at != OLD.created_at THEN
            RAISE EXCEPTION 'provider dispatch identity is immutable'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.version != OLD.version + 1 THEN
            RAISE EXCEPTION 'provider dispatch version must advance exactly once'
              USING ERRCODE = '23514';
          END IF;
          IF NOT ((OLD.state = 'reserved' AND NEW.state IN ('dispatched','settled'))
                  OR (OLD.state = 'dispatched' AND NEW.state = 'settled')) THEN
            RAISE EXCEPTION 'provider dispatch state transition is not monotonic'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END
        $$;

        CREATE TRIGGER pipeline_provider_dispatches_update_guard
        BEFORE UPDATE ON pipeline_provider_dispatches
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_provider_dispatch_update();
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pipeline_provider_dispatches) THEN
            RAISE EXCEPTION
              'cannot downgrade 0102 while provider dispatch records exist';
          END IF;
        END
        $$;

        DROP TABLE pipeline_provider_dispatches;
        DROP FUNCTION validate_pipeline_provider_dispatch_update();
        """
    )
