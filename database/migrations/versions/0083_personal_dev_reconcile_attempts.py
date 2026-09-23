"""Bind personal-dev credentials and add durable reconciler leases.

Revision ID: 0083
Revises: 0082
Create Date: 2026-08-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0083"
down_revision = "0082"
branch_labels = None
depends_on = None


def _attempt_binding_guard(*, include_credential: bool) -> None:
    credential_guard = ""
    if include_credential:
        credential_guard = """
               OR NEW.credential_binding_version IS DISTINCT FROM OLD.credential_binding_version
               OR NEW.bootstrap_auth_kind IS DISTINCT FROM OLD.bootstrap_auth_kind
               OR NEW.bootstrap_credential_hash IS DISTINCT FROM OLD.bootstrap_credential_hash
"""
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION loom_guard_dev_lifecycle_attempt_binding()
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
               OR NEW.started_at IS DISTINCT FROM OLD.started_at
{credential_guard}            THEN
                RAISE EXCEPTION 'dev lifecycle operation attempt binding is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END
        $$
        """,
    )


def upgrade() -> None:
    # Package 4 was still activation-blocked at 0082. Refuse to invent an
    # ambient credential for a request that was admitted before exact
    # attempt-bound credential capture existed.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operation_attempts) THEN
                RAISE EXCEPTION
                    'cannot upgrade 0083 with unbound personal-dev lifecycle attempts';
            END IF;
        END
        $$
        """,
    )
    op.add_column(
        "dev_lifecycle_operation_attempts",
        sa.Column(
            "credential_binding_version",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
    )
    op.add_column(
        "dev_lifecycle_operation_attempts",
        sa.Column("bootstrap_auth_kind", sa.String(length=16), nullable=False),
    )
    op.add_column(
        "dev_lifecycle_operation_attempts",
        sa.Column("bootstrap_credential_hash", sa.LargeBinary(length=32), nullable=False),
    )
    op.add_column(
        "dev_lifecycle_operation_attempts",
        sa.Column(
            "lease_epoch",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "dev_lifecycle_operation_attempts",
        sa.Column("claimed_by", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "dev_lifecycle_operation_attempts",
        sa.Column(
            "lease_expires_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.drop_constraint(
        "dev_lifecycle_operation_attempts_counters_check",
        "dev_lifecycle_operation_attempts",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operation_attempts_counters_check",
        "dev_lifecycle_operation_attempts",
        "operation_epoch > 0 AND attempt_sequence >= 0 AND lease_epoch >= 0",
    )
    op.create_check_constraint(
        "dev_lifecycle_operation_attempts_credential_check",
        "dev_lifecycle_operation_attempts",
        "credential_binding_version = 1 "
        "AND bootstrap_auth_kind IN ('bearer', 'session') "
        "AND octet_length(bootstrap_credential_hash) = 32",
    )
    op.create_check_constraint(
        "dev_lifecycle_operation_attempts_lease_check",
        "dev_lifecycle_operation_attempts",
        "(claimed_by IS NULL AND lease_expires_at IS NULL) OR "
        "(claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL)",
    )
    op.create_index(
        "dev_lifecycle_operation_attempts_picker_idx",
        "dev_lifecycle_operation_attempts",
        ["state", "checkpoint", "lease_expires_at", "created_at", "id"],
    )
    op.create_table(
        "dev_lifecycle_activation_acknowledgements",
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment_name", sa.Text(), nullable=False),
        sa.Column("subject_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_sha", sa.String(length=64), nullable=False),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("readiness_evidence_sha256", sa.String(length=64), nullable=False),
        sa.Column("local_activation_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("signature_sha256", sa.String(length=64), nullable=False),
        sa.Column("agent_key_id", sa.String(length=64), nullable=False),
        sa.Column("observed_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("received_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint(
            "operation_epoch > 0 AND deployment_generation > 0",
            name="dev_lifecycle_activation_acknowledgements_counters_check",
        ),
        sa.CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{64}$' "
            "AND readiness_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND local_activation_sha256 ~ '^[0-9a-f]{64}$' "
            "AND payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND signature_sha256 ~ '^[0-9a-f]{64}$'",
            name="dev_lifecycle_activation_acknowledgements_digests_check",
        ),
        sa.CheckConstraint(
            "agent_key_id ~ '^[a-z][a-z0-9._-]{0,63}$'",
            name="dev_lifecycle_activation_acknowledgements_key_check",
        ),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["dev_lifecycle_operations.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("operation_id"),
        sa.UniqueConstraint(
            "payload_sha256",
            name="dev_lifecycle_activation_acknowledgements_payload_uidx",
        ),
    )
    op.execute(
        """
        CREATE FUNCTION loom_guard_dev_lifecycle_activation_acknowledgement()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'dev lifecycle activation acknowledgement is append-only'
                USING ERRCODE = 'integrity_constraint_violation';
        END
        $$
        """,
    )
    op.execute(
        """
        CREATE TRIGGER dev_lifecycle_activation_acknowledgements_append_guard
        BEFORE UPDATE OR DELETE ON dev_lifecycle_activation_acknowledgements
        FOR EACH ROW
        EXECUTE FUNCTION loom_guard_dev_lifecycle_activation_acknowledgement()
        """,
    )
    _attempt_binding_guard(include_credential=True)


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operation_attempts) THEN
                RAISE EXCEPTION
                    'cannot downgrade 0083 while credential-bound lifecycle attempts exist';
            END IF;
        END
        $$
        """,
    )
    op.execute(
        "DROP TRIGGER dev_lifecycle_activation_acknowledgements_append_guard "
        "ON dev_lifecycle_activation_acknowledgements",
    )
    op.execute("DROP FUNCTION loom_guard_dev_lifecycle_activation_acknowledgement()")
    op.drop_table("dev_lifecycle_activation_acknowledgements")
    _attempt_binding_guard(include_credential=False)
    op.drop_index(
        "dev_lifecycle_operation_attempts_picker_idx",
        table_name="dev_lifecycle_operation_attempts",
    )
    op.drop_constraint(
        "dev_lifecycle_operation_attempts_lease_check",
        "dev_lifecycle_operation_attempts",
        type_="check",
    )
    op.drop_constraint(
        "dev_lifecycle_operation_attempts_credential_check",
        "dev_lifecycle_operation_attempts",
        type_="check",
    )
    op.drop_constraint(
        "dev_lifecycle_operation_attempts_counters_check",
        "dev_lifecycle_operation_attempts",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operation_attempts_counters_check",
        "dev_lifecycle_operation_attempts",
        "operation_epoch > 0 AND attempt_sequence >= 0",
    )
    op.drop_column("dev_lifecycle_operation_attempts", "lease_expires_at")
    op.drop_column("dev_lifecycle_operation_attempts", "claimed_by")
    op.drop_column("dev_lifecycle_operation_attempts", "lease_epoch")
    op.drop_column("dev_lifecycle_operation_attempts", "bootstrap_credential_hash")
    op.drop_column("dev_lifecycle_operation_attempts", "bootstrap_auth_kind")
    op.drop_column("dev_lifecycle_operation_attempts", "credential_binding_version")
