"""Add fenced personal-development environment lifecycle operations.

Revision ID: 0082
Revises: 0081
Create Date: 2026-08-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0082"
down_revision = "0081"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dev_instances",
        sa.Column(
            "subject_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
    )
    op.add_column(
        "dev_instances",
        sa.Column(
            "subject_incarnation",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
    )
    op.add_column(
        "dev_instances",
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_unique_constraint(
        "dev_instances_subject_id_uidx",
        "dev_instances",
        ["subject_id"],
    )
    op.create_foreign_key(
        "dev_instances_candidate_id_fkey",
        "dev_instances",
        "personal_dev_candidates",
        ["candidate_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint("dev_instances_status_check", "dev_instances", type_="check")
    op.create_check_constraint(
        "dev_instances_status_check",
        "dev_instances",
        "status IN ('provisioning', 'ready', 'updating', 'activating', "
        "'deleting', 'draining', 'failed', 'deleted')",
    )
    op.drop_constraint("dev_instances_candidate_sha_check", "dev_instances", type_="check")
    op.alter_column(
        "dev_instances",
        "candidate_sha",
        existing_type=sa.String(length=40),
        type_=sa.String(length=64),
        existing_nullable=False,
    )
    op.create_check_constraint(
        "dev_instances_candidate_sha_check",
        "dev_instances",
        "(candidate_id IS NULL AND candidate_sha ~ '^[0-9a-f]{40}$') OR "
        "(candidate_id IS NOT NULL AND candidate_sha ~ '^[0-9a-f]{64}$')",
    )

    op.create_table(
        "dev_lifecycle_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment_name", sa.Text(), nullable=False),
        sa.Column("subject_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("expected_operation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "attempt_sequence",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("request_sha256", sa.String(length=64), nullable=False),
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_sha", sa.String(length=64), nullable=False),
        sa.Column("min_slots", sa.Integer(), nullable=False),
        sa.Column("max_slots", sa.Integer(), nullable=False),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("readiness_evidence_sha256", sa.String(length=64), nullable=True),
        sa.Column(
            "activation_acknowledgement_sha256",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "checkpoint",
            sa.String(length=32),
            server_default=sa.text("'claimed'"),
            nullable=False,
        ),
        sa.Column("failure_reason", sa.String(length=256), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "operation_epoch >= expected_operation_epoch "
            "AND operation_epoch <= expected_operation_epoch + 1 "
            "AND expected_operation_epoch >= 0 AND attempt_sequence >= 0",
            name="dev_lifecycle_operations_epochs_check",
        ),
        sa.CheckConstraint(
            "kind IN ('create', 'update', 'capacity', 'noop')",
            name="dev_lifecycle_operations_kind_check",
        ),
        sa.CheckConstraint(
            "state IN ('requested', 'running', 'activating', 'succeeded', "
            "'failed', 'cancelling', 'cancelled')",
            name="dev_lifecycle_operations_state_check",
        ),
        sa.CheckConstraint(
            "request_sha256 ~ '^[0-9a-f]{64}$' "
            "AND candidate_sha ~ '^[0-9a-f]{64}$' "
            "AND (readiness_evidence_sha256 IS NULL OR "
            "readiness_evidence_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (activation_acknowledgement_sha256 IS NULL OR "
            "activation_acknowledgement_sha256 ~ '^[0-9a-f]{64}$')",
            name="dev_lifecycle_operations_digests_check",
        ),
        sa.CheckConstraint(
            "min_slots >= 0 AND max_slots >= min_slots AND max_slots <= 8 "
            "AND deployment_generation > 0",
            name="dev_lifecycle_operations_target_check",
        ),
        sa.CheckConstraint(
            "(kind = 'noop' AND operation_epoch = expected_operation_epoch "
            "AND state = 'succeeded') OR "
            "(kind <> 'noop' AND operation_epoch = expected_operation_epoch + 1)",
            name="dev_lifecycle_operations_transition_check",
        ),
        sa.CheckConstraint(
            "(state IN ('requested', 'running', 'activating', 'cancelling') "
            "AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state = 'succeeded' AND finished_at IS NOT NULL "
            "AND failure_reason IS NULL) OR "
            "(state IN ('failed', 'cancelled') AND finished_at IS NOT NULL)",
            name="dev_lifecycle_operations_terminal_fields_check",
        ),
        sa.CheckConstraint(
            "(kind IN ('capacity', 'noop') "
            "AND readiness_evidence_sha256 IS NULL "
            "AND activation_acknowledgement_sha256 IS NULL) OR "
            "(kind IN ('create', 'update') AND ("
            "(state IN ('requested', 'running', 'failed', 'cancelling', 'cancelled') "
            "AND readiness_evidence_sha256 IS NULL "
            "AND activation_acknowledgement_sha256 IS NULL) OR "
            "(state = 'activating' AND readiness_evidence_sha256 IS NOT NULL) OR "
            "(state = 'succeeded' AND readiness_evidence_sha256 IS NOT NULL "
            "AND activation_acknowledgement_sha256 IS NOT NULL)))",
            name="dev_lifecycle_operations_activation_evidence_check",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["personal_dev_candidates.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["environment_name"],
            ["dev_instances.name"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["owner_team_id"], ["teams.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "owner_user_id",
            "idempotency_key",
            name="dev_lifecycle_operations_owner_idempotency_uidx",
        ),
        sa.UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "expected_operation_epoch",
            "request_sha256",
            name="dev_lifecycle_operations_request_uidx",
        ),
        sa.UniqueConstraint(
            "attempt_id",
            name="dev_lifecycle_operations_attempt_id_uidx",
        ),
    )
    op.create_index(
        "dev_lifecycle_operations_environment_created_idx",
        "dev_lifecycle_operations",
        ["environment_name", "created_at", "id"],
    )
    op.create_index(
        "dev_lifecycle_operations_active_environment_uidx",
        "dev_lifecycle_operations",
        ["environment_name"],
        unique=True,
        postgresql_where=sa.text(
            "state IN ('requested', 'running', 'activating', 'cancelling')",
        ),
    )
    op.create_table(
        "dev_lifecycle_operation_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("attempt_sequence", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("checkpoint", sa.String(length=32), nullable=False),
        sa.Column("failure_reason", sa.String(length=256), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "operation_epoch > 0 AND attempt_sequence >= 0",
            name="dev_lifecycle_operation_attempts_counters_check",
        ),
        sa.CheckConstraint(
            "state IN ('running', 'activating', 'succeeded', 'failed', 'cancelled')",
            name="dev_lifecycle_operation_attempts_state_check",
        ),
        sa.CheckConstraint(
            "(state IN ('running', 'activating') AND finished_at IS NULL "
            "AND failure_reason IS NULL) OR "
            "(state = 'succeeded' AND finished_at IS NOT NULL "
            "AND failure_reason IS NULL) OR "
            "(state IN ('failed', 'cancelled') AND finished_at IS NOT NULL)",
            name="dev_lifecycle_operation_attempts_terminal_fields_check",
        ),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["dev_lifecycle_operations.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "operation_id",
            "attempt_sequence",
            name="dev_lifecycle_operation_attempts_sequence_uidx",
        ),
    )
    op.create_index(
        "dev_lifecycle_operation_attempts_active_operation_uidx",
        "dev_lifecycle_operation_attempts",
        ["operation_id"],
        unique=True,
        postgresql_where=sa.text("state IN ('running', 'activating')"),
    )
    op.execute(
        """
        CREATE FUNCTION loom_guard_dev_lifecycle_operation_binding()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
               OR NEW.environment_name IS DISTINCT FROM OLD.environment_name
               OR NEW.subject_id IS DISTINCT FROM OLD.subject_id
               OR NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
               OR NEW.owner_user_id IS DISTINCT FROM OLD.owner_user_id
               OR NEW.owner_team_id IS DISTINCT FROM OLD.owner_team_id
               OR NEW.operation_epoch IS DISTINCT FROM OLD.operation_epoch
               OR NEW.expected_operation_epoch IS DISTINCT FROM OLD.expected_operation_epoch
               OR NEW.kind IS DISTINCT FROM OLD.kind
               OR NEW.request_sha256 IS DISTINCT FROM OLD.request_sha256
               OR NEW.candidate_id IS DISTINCT FROM OLD.candidate_id
               OR NEW.candidate_sha IS DISTINCT FROM OLD.candidate_sha
               OR NEW.min_slots IS DISTINCT FROM OLD.min_slots
               OR NEW.max_slots IS DISTINCT FROM OLD.max_slots
               OR NEW.deployment_generation IS DISTINCT FROM OLD.deployment_generation
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'dev lifecycle operation binding is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END
        $$
        """,
    )
    op.execute(
        """
        CREATE TRIGGER dev_lifecycle_operations_binding_guard
        BEFORE UPDATE ON dev_lifecycle_operations
        FOR EACH ROW
        EXECUTE FUNCTION loom_guard_dev_lifecycle_operation_binding()
        """,
    )
    op.execute(
        """
        CREATE FUNCTION loom_guard_dev_lifecycle_attempt_binding()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.operation_id IS DISTINCT FROM OLD.operation_id
               OR NEW.subject_id IS DISTINCT FROM OLD.subject_id
               OR NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
               OR NEW.operation_epoch IS DISTINCT FROM OLD.operation_epoch
               OR NEW.attempt_sequence IS DISTINCT FROM OLD.attempt_sequence
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.started_at IS DISTINCT FROM OLD.started_at THEN
                RAISE EXCEPTION 'dev lifecycle operation attempt binding is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END
        $$
        """,
    )
    op.execute(
        """
        CREATE TRIGGER dev_lifecycle_operation_attempts_binding_guard
        BEFORE UPDATE ON dev_lifecycle_operation_attempts
        FOR EACH ROW
        EXECUTE FUNCTION loom_guard_dev_lifecycle_attempt_binding()
        """,
    )
    op.execute(
        """
        CREATE FUNCTION loom_check_dev_lifecycle_current_attempt()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM dev_lifecycle_operation_attempts AS attempt
                 WHERE attempt.id = NEW.attempt_id
                   AND attempt.operation_id = NEW.id
                   AND attempt.subject_id = NEW.subject_id
                   AND attempt.subject_incarnation = NEW.subject_incarnation
                   AND attempt.operation_epoch = NEW.operation_epoch
                   AND attempt.attempt_sequence = NEW.attempt_sequence
            ) THEN
                RAISE EXCEPTION 'dev lifecycle current attempt binding is invalid'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NULL;
        END
        $$
        """,
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER dev_lifecycle_operations_current_attempt_guard
        AFTER INSERT OR UPDATE OF attempt_id, attempt_sequence
        ON dev_lifecycle_operations
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION loom_check_dev_lifecycle_current_attempt()
        """,
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operation_attempts)
               OR EXISTS (SELECT 1 FROM dev_lifecycle_operations)
               OR EXISTS (SELECT 1 FROM dev_instances WHERE candidate_id IS NOT NULL)
               OR EXISTS (
                   SELECT 1 FROM dev_instances
                    WHERE status IN ('updating', 'activating', 'draining')
               ) THEN
                RAISE EXCEPTION
                    'cannot downgrade 0082 while personal-dev lifecycle state exists';
            END IF;
        END
        $$
        """,
    )
    op.execute(
        "DROP TRIGGER dev_lifecycle_operations_current_attempt_guard "
        "ON dev_lifecycle_operations",
    )
    op.execute(
        "DROP TRIGGER dev_lifecycle_operation_attempts_binding_guard "
        "ON dev_lifecycle_operation_attempts",
    )
    op.execute(
        "DROP TRIGGER dev_lifecycle_operations_binding_guard ON dev_lifecycle_operations",
    )
    op.execute("DROP FUNCTION loom_check_dev_lifecycle_current_attempt()")
    op.execute("DROP FUNCTION loom_guard_dev_lifecycle_attempt_binding()")
    op.execute("DROP FUNCTION loom_guard_dev_lifecycle_operation_binding()")
    op.drop_index(
        "dev_lifecycle_operation_attempts_active_operation_uidx",
        table_name="dev_lifecycle_operation_attempts",
    )
    op.drop_table("dev_lifecycle_operation_attempts")
    op.drop_index(
        "dev_lifecycle_operations_active_environment_uidx",
        table_name="dev_lifecycle_operations",
    )
    op.drop_index(
        "dev_lifecycle_operations_environment_created_idx",
        table_name="dev_lifecycle_operations",
    )
    op.drop_table("dev_lifecycle_operations")

    op.drop_constraint("dev_instances_candidate_sha_check", "dev_instances", type_="check")
    op.alter_column(
        "dev_instances",
        "candidate_sha",
        existing_type=sa.String(length=64),
        type_=sa.String(length=40),
        existing_nullable=False,
    )
    op.create_check_constraint(
        "dev_instances_candidate_sha_check",
        "dev_instances",
        "candidate_sha ~ '^[0-9a-f]{40}$'",
    )
    op.drop_constraint("dev_instances_status_check", "dev_instances", type_="check")
    op.create_check_constraint(
        "dev_instances_status_check",
        "dev_instances",
        "status IN ('provisioning', 'ready', 'deleting', 'failed', 'deleted')",
    )
    op.drop_constraint("dev_instances_candidate_id_fkey", "dev_instances", type_="foreignkey")
    op.drop_constraint("dev_instances_subject_id_uidx", "dev_instances", type_="unique")
    op.drop_column("dev_instances", "candidate_id")
    op.drop_column("dev_instances", "subject_incarnation")
    op.drop_column("dev_instances", "subject_id")
