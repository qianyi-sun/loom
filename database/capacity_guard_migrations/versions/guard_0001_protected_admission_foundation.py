"""Protected admission foundation.

Revision ID: guard_0001
Revises:
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
TABLES = ("authority_state", "trial_requirements", "trial_attempts", "audit_events")


def _install_append_only_guards() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.reject_append_only_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          RAISE EXCEPTION 'protected capacity records are append-only in Package 2A'
            USING ERRCODE = '55000';
        END
        $function$
        """
    )
    for table in TABLES:
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_row
            BEFORE UPDATE OR DELETE ON {SCHEMA}.{table}
            FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_truncate
            BEFORE TRUNCATE ON {SCHEMA}.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )


def _close_privileges() -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM PUBLIC")
    # PostgreSQL's built-in PUBLIC EXECUTE default for functions is global.
    # A per-schema REVOKE cannot subtract that global default, so close the
    # dedicated owner's global future-object defaults before also recording
    # the protected-schema-specific defaults below.
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC")
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC")
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA} REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA} "
        "REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA} "
        "REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC"
    )


def upgrade() -> None:
    op.create_table(
        "authority_state",
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("environment_id", sa.Text(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("authority_mode", sa.Text(), nullable=False),
        sa.Column("authority_incarnation", sa.Uuid(), nullable=False),
        sa.Column("reporter_incarnation", sa.Uuid(), nullable=False),
        sa.Column(
            "reporter_high_water", sa.BigInteger(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("allocation_epoch", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("configuration_generation", sa.BigInteger(), nullable=False),
        sa.Column("candidate_digest", sa.Text(), nullable=False),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="guard_authority_singleton_check"),
        sa.CheckConstraint("schema_version = 1", name="guard_authority_schema_check"),
        sa.CheckConstraint(
            "environment_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_authority_environment_check",
        ),
        sa.CheckConstraint(
            "authority_mode = 'disabled'", name="guard_authority_disabled_only_check"
        ),
        sa.CheckConstraint(
            "reporter_high_water >= 0", name="guard_authority_reporter_high_water_check"
        ),
        sa.CheckConstraint("allocation_epoch = 0", name="guard_authority_allocation_epoch_check"),
        sa.CheckConstraint(
            "deployment_generation > 0", name="guard_authority_deployment_generation_check"
        ),
        sa.CheckConstraint(
            "configuration_generation > 0",
            name="guard_authority_configuration_generation_check",
        ),
        sa.CheckConstraint(
            "candidate_digest ~ '^[0-9a-f]{64}$'",
            name="guard_authority_candidate_digest_check",
        ),
        sa.PrimaryKeyConstraint("singleton_id"),
        schema=SCHEMA,
    )
    op.create_table(
        "trial_requirements",
        sa.Column("trial_id", sa.Uuid(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("requirements_digest", sa.Text(), nullable=False),
        sa.Column("requirements", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("schema_version = 1", name="guard_requirements_schema_check"),
        sa.CheckConstraint(
            "requirements_digest ~ '^[0-9a-f]{64}$'",
            name="guard_requirements_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(requirements) = 'object' AND octet_length(requirements::text) <= 8388608",
            name="guard_requirements_payload_check",
        ),
        sa.ForeignKeyConstraint(["trial_id"], ["public.trials.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("trial_id"),
        sa.UniqueConstraint(
            "trial_id", "requirements_digest", name="guard_requirements_binding_key"
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "trial_attempts",
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("trial_id", sa.Uuid(), nullable=False),
        sa.Column("execution_generation", sa.BigInteger(), nullable=False),
        sa.Column("requirements_digest", sa.Text(), nullable=False),
        sa.Column("claim_state", sa.Text(), nullable=False),
        sa.Column("assigned_pool", sa.Text(), nullable=True),
        sa.Column("assignment_epoch", sa.BigInteger(), nullable=True),
        sa.Column("worker_id", sa.Uuid(), nullable=True),
        sa.Column("claim_epoch", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("execution_generation > 0", name="guard_attempt_generation_check"),
        sa.CheckConstraint(
            "requirements_digest ~ '^[0-9a-f]{64}$'",
            name="guard_attempt_requirements_digest_check",
        ),
        sa.CheckConstraint("claim_state = 'queued'", name="guard_attempt_queued_only_check"),
        sa.CheckConstraint(
            "assigned_pool IS NULL AND assignment_epoch IS NULL "
            "AND worker_id IS NULL AND claim_epoch IS NULL",
            name="guard_attempt_unassigned_only_check",
        ),
        sa.ForeignKeyConstraint(
            ["trial_id", "requirements_digest"],
            [
                f"{SCHEMA}.trial_requirements.trial_id",
                f"{SCHEMA}.trial_requirements.requirements_digest",
            ],
            ondelete="RESTRICT",
            name="guard_attempt_requirements_binding_fk",
        ),
        sa.PrimaryKeyConstraint("protected_attempt_id"),
        sa.UniqueConstraint(
            "trial_id", "execution_generation", name="guard_attempt_trial_generation_key"
        ),
        sa.UniqueConstraint(
            "protected_attempt_id", "trial_id", name="guard_attempt_trial_binding_key"
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "audit_events",
        sa.Column("event_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("trial_id", sa.Uuid(), nullable=True),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "event_type ~ '^[a-z][a-z0-9_.-]{0,126}$'", name="guard_audit_event_type_check"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 16384",
            name="guard_audit_payload_check",
        ),
        sa.CheckConstraint(
            "payload_digest ~ '^[0-9a-f]{64}$'", name="guard_audit_payload_digest_check"
        ),
        sa.CheckConstraint(
            "protected_attempt_id IS NULL OR trial_id IS NOT NULL",
            name="guard_audit_attempt_requires_trial_check",
        ),
        sa.ForeignKeyConstraint(
            ["trial_id"], [f"{SCHEMA}.trial_requirements.trial_id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id", "trial_id"],
            [f"{SCHEMA}.trial_attempts.protected_attempt_id", f"{SCHEMA}.trial_attempts.trial_id"],
            ondelete="RESTRICT",
            name="guard_audit_attempt_trial_binding_fk",
        ),
        sa.PrimaryKeyConstraint("event_id"),
        schema=SCHEMA,
    )
    _install_append_only_guards()
    _close_privileges()


def downgrade() -> None:
    op.drop_table("audit_events", schema=SCHEMA)
    op.drop_table("trial_attempts", schema=SCHEMA)
    op.drop_table("trial_requirements", schema=SCHEMA)
    op.drop_table("authority_state", schema=SCHEMA)
    op.execute(f"DROP FUNCTION {SCHEMA}.reject_append_only_mutation()")
