"""Application-only Kubernetes write-ahead evidence.

Revision ID: 0162
Revises: 0161
"""
from alembic import op

revision = "0162"
down_revision = "0161"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nebius_application_effects (
            operation_id uuid NOT NULL REFERENCES nebius_application_operations(operation_id) ON DELETE RESTRICT,
            effect_key text NOT NULL,
            sequence bigint NOT NULL,
            intent_json jsonb NOT NULL,
            phase text NOT NULL,
            dispatch_epoch bigint,
            observed_uid text,
            observed_resource_version text,
            rejection_status smallint,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(operation_id,effect_key),
            CONSTRAINT nebius_application_effect_sequence_key UNIQUE(operation_id,sequence),
            CONSTRAINT nebius_application_effect_key_check CHECK (sequence > 0 AND effect_key ~ '^[a-zA-Z0-9._:-]{1,128}$'),
            CONSTRAINT nebius_application_effect_shape_check CHECK (
                phase IN ('prepared','dispatched','observed','rejected') AND jsonb_typeof(intent_json) = 'object'),
            CONSTRAINT nebius_application_effect_rejection_check CHECK (
                (phase = 'rejected') = (rejection_status IS NOT NULL) AND
                (rejection_status IS NULL OR rejection_status IN (409,422))),
            CONSTRAINT nebius_application_effect_dispatch_check CHECK (
                (phase = 'prepared') = (dispatch_epoch IS NULL) AND (dispatch_epoch IS NULL OR dispatch_epoch > 0)),
            CONSTRAINT nebius_application_effect_observation_check CHECK (
                (phase = 'observed') = (observed_uid IS NOT NULL) AND
                (observed_resource_version IS NULL OR observed_uid IS NOT NULL))
        );
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE nebius_application_effects IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_application_effects) THEN
                RAISE EXCEPTION 'cannot remove application effect history';
            END IF;
        END $$;
        DROP TABLE nebius_application_effects;
    """)
