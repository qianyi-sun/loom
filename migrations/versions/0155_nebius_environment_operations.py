"""Journaled managed creation and atomic platform reservations.

Revision ID: 0155
Revises: 0154
"""

from alembic import op

revision = "0155"
down_revision = "0154"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nebius_platform_budgets (
            cluster_id text PRIMARY KEY,
            cpu_millis bigint NOT NULL,
            memory_mib bigint NOT NULL,
            storage_mib bigint NOT NULL,
            ephemeral_storage_mib bigint NOT NULL,
            CONSTRAINT nebius_platform_budget_nonnegative CHECK
              (cpu_millis >= 0 AND memory_mib >= 0 AND storage_mib >= 0 AND ephemeral_storage_mib >= 0)
        );
        CREATE TABLE nebius_platform_reservations (
            environment_id uuid PRIMARY KEY,
            cluster_id text NOT NULL REFERENCES nebius_platform_budgets(cluster_id) ON DELETE RESTRICT,
            cpu_millis bigint NOT NULL,
            memory_mib bigint NOT NULL,
            storage_mib bigint NOT NULL,
            ephemeral_storage_mib bigint NOT NULL,
            CONSTRAINT nebius_platform_reservation_environment_fk FOREIGN KEY (environment_id, cluster_id)
              REFERENCES nebius_environments(environment_id, cluster_id) ON DELETE RESTRICT,
            CONSTRAINT nebius_platform_reservation_nonnegative CHECK
              (cpu_millis >= 0 AND memory_mib >= 0 AND storage_mib >= 0 AND ephemeral_storage_mib >= 0)
        );
        CREATE TABLE nebius_environment_operations (
            operation_id uuid PRIMARY KEY,
            environment_id uuid NOT NULL REFERENCES nebius_environments(environment_id) ON DELETE RESTRICT,
            owner_user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
            idempotency_key text NOT NULL,
            request_sha256 text NOT NULL,
            deployment_generation bigint NOT NULL,
            action text NOT NULL,
            phase text NOT NULL,
            error_code text,
            plan_json jsonb NOT NULL,
            runner_epoch bigint NOT NULL DEFAULT 0,
            lease_token uuid,
            lease_expires_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT nebius_environment_operation_replay_key UNIQUE (owner_user_id, idempotency_key),
            CONSTRAINT nebius_environment_operation_generation_key UNIQUE (environment_id, deployment_generation),
            CONSTRAINT nebius_environment_operation_action_check CHECK (action IN ('create', 'destroy_retained')),
            CONSTRAINT nebius_environment_operation_phase_check CHECK (phase IN ('pending', 'running', 'blocked', 'completed')),
            CONSTRAINT nebius_environment_operation_generation_check CHECK (deployment_generation > 0 AND runner_epoch >= 0),
            CONSTRAINT nebius_environment_operation_plan_check CHECK
              (request_sha256 ~ '^[0-9a-f]{64}$' AND jsonb_typeof(plan_json) = 'object'),
            CONSTRAINT nebius_environment_operation_lease_check CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL))
        );
        CREATE TABLE nebius_environment_resources (
            operation_id uuid NOT NULL REFERENCES nebius_environment_operations(operation_id) ON DELETE RESTRICT,
            resource_key text NOT NULL,
            sequence bigint NOT NULL,
            kind text NOT NULL,
            payload_json jsonb NOT NULL,
            phase text NOT NULL,
            provider_identity text,
            PRIMARY KEY (operation_id, resource_key),
            CONSTRAINT nebius_environment_resource_sequence_key UNIQUE (operation_id, sequence),
            CONSTRAINT nebius_environment_resource_sequence_check CHECK (sequence >= 0),
            CONSTRAINT nebius_environment_resource_kind_check CHECK
              (kind IN ('kubernetes', 'object_bucket', 'credentials', 'database_ready', 'job_ready', 'application_ready')),
            CONSTRAINT nebius_environment_resource_phase_check CHECK
              (phase IN ('planned', 'applied') AND ((phase = 'applied') = (provider_identity IS NOT NULL))),
            CONSTRAINT nebius_environment_resource_payload_check CHECK (jsonb_typeof(payload_json) = 'object')
        );
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE nebius_environment_resources, nebius_environment_operations, nebius_platform_reservations, nebius_platform_budgets IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_environment_operations)
              OR EXISTS (SELECT 1 FROM nebius_platform_reservations)
              OR EXISTS (SELECT 1 FROM nebius_platform_budgets) THEN
                RAISE EXCEPTION 'cannot remove managed provisioning or platform budget history';
            END IF;
        END $$;
        DROP TABLE nebius_environment_resources;
        DROP TABLE nebius_environment_operations;
        DROP TABLE nebius_platform_reservations;
        DROP TABLE nebius_platform_budgets;
    """)
