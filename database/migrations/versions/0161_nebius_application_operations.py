"""Application lifecycle intent and shared platform reservation accounting.

Revision ID: 0161
Revises: 0160
"""
from alembic import op

revision = "0161"
down_revision = "0160"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE nebius_applications ADD CONSTRAINT nebius_application_cluster_key UNIQUE(application_id,cluster_id);
        CREATE TABLE nebius_application_reservations (
            application_id uuid PRIMARY KEY,
            cluster_id text NOT NULL REFERENCES nebius_platform_budgets(cluster_id) ON DELETE RESTRICT,
            cpu_millis bigint NOT NULL, memory_mib bigint NOT NULL,
            storage_mib bigint NOT NULL, ephemeral_storage_mib bigint NOT NULL,
            CONSTRAINT nebius_application_reservation_owner_fk FOREIGN KEY(application_id,cluster_id)
              REFERENCES nebius_applications(application_id,cluster_id) ON DELETE RESTRICT,
            CONSTRAINT nebius_application_reservation_envelope_check CHECK (
              cpu_millis >= 0 AND memory_mib >= 0 AND storage_mib = 0 AND ephemeral_storage_mib >= 0)
        );
        CREATE TABLE nebius_application_operations (
            operation_id uuid PRIMARY KEY,
            application_id uuid NOT NULL REFERENCES nebius_applications(application_id) ON DELETE RESTRICT,
            owner_user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
            idempotency_key text NOT NULL,
            request_sha256 text NOT NULL,
            deployment_generation bigint NOT NULL, access_generation bigint NOT NULL,
            action text NOT NULL, phase text NOT NULL, error_code text,
            plan_json jsonb NOT NULL,
            runner_epoch bigint NOT NULL DEFAULT 0,
            lease_token uuid, lease_expires_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT nebius_application_operation_replay_key UNIQUE(owner_user_id,idempotency_key),
            CONSTRAINT nebius_application_operation_generation_key UNIQUE(application_id,deployment_generation),
            CONSTRAINT nebius_application_operation_action_check CHECK (action IN ('create','update','suspend','resume','destroy_retained')),
            CONSTRAINT nebius_application_operation_phase_check CHECK (phase IN ('pending','running','blocked','completed','superseded')),
            CONSTRAINT nebius_application_operation_generation_check CHECK (deployment_generation > 0 AND access_generation > 0 AND runner_epoch >= 0),
            CONSTRAINT nebius_application_operation_plan_check CHECK (request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(plan_json) = 'object'),
            CONSTRAINT nebius_application_operation_lease_check CHECK (
              (phase = 'running') = (lease_token IS NOT NULL) AND (lease_token IS NULL) = (lease_expires_at IS NULL))
        );
        CREATE INDEX nebius_application_operation_runnable_idx ON nebius_application_operations(phase,created_at);
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE nebius_application_operations, nebius_application_reservations IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_application_operations) OR EXISTS (SELECT 1 FROM nebius_application_reservations) THEN
                RAISE EXCEPTION 'cannot remove application operation or reservation history';
            END IF;
        END $$;
        DROP TABLE nebius_application_operations;
        DROP TABLE nebius_application_reservations;
        ALTER TABLE nebius_applications DROP CONSTRAINT nebius_application_cluster_key;
    """)
