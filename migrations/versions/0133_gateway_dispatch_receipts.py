"""Add non-billing Gateway transport observations.

Revision ID: 0133
Revises: 0132
"""

from alembic import op

revision = "0133"
down_revision = "0132"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE gateway_dispatch_receipts (
            id UUID PRIMARY KEY,
            request_id UUID NOT NULL,
            dispatch_ordinal INTEGER NOT NULL,
            attempt INTEGER NOT NULL,
            team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
            trial_id UUID REFERENCES trials(id) ON DELETE CASCADE,
            execution_attempt_id UUID REFERENCES execution_attempts(id) ON DELETE CASCADE,
            step_id TEXT NOT NULL,
            step_jwt_id UUID,
            agent_attempt_id UUID,
            provider_connection_id UUID,
            dialect TEXT NOT NULL,
            purpose TEXT NOT NULL,
            attempt_deadline_wall_clock TIMESTAMPTZ,
            admitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            provider_outcome TEXT NOT NULL DEFAULT 'admitted',
            provider_observed_at TIMESTAMPTZ,
            provider_http_status INTEGER,
            gateway_outcome TEXT NOT NULL DEFAULT 'pending',
            gateway_observed_at TIMESTAMPTZ,
            gateway_http_status INTEGER,
            CONSTRAINT gateway_receipt_request_uidx UNIQUE(request_id, dispatch_ordinal),
            CONSTRAINT gateway_receipt_subject_check CHECK (
                (trial_id IS NOT NULL)::integer +
                (execution_attempt_id IS NOT NULL)::integer = 1),
            CONSTRAINT gateway_receipt_count_check CHECK (dispatch_ordinal > 0 AND attempt > 0),
            CONSTRAINT gateway_receipt_provider_check CHECK (provider_outcome IN (
                'admitted','response_received','stream_completed','deadline','cancelled',
                'transport_error','not_dispatched')),
            CONSTRAINT gateway_receipt_gateway_check CHECK (
                gateway_outcome IN ('pending','completed','error','cancelled','deadline')),
            CONSTRAINT gateway_receipt_purpose_check CHECK (
                purpose IN ('model_call','capability_probe','adapter_call'))
        );
        CREATE INDEX gateway_receipt_trial_idx ON gateway_dispatch_receipts(trial_id, admitted_at);
        CREATE INDEX gateway_receipt_pending_idx
            ON gateway_dispatch_receipts(provider_outcome, admitted_at);
        """
    )


def downgrade() -> None:
    # Observations cannot be reconstructed from llm_calls. Never silently drop them.
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM gateway_dispatch_receipts) THEN
            RAISE EXCEPTION 'cannot drop nonempty gateway dispatch receipts';
          END IF;
        END $$;
        DROP TABLE gateway_dispatch_receipts;
        """
    )
