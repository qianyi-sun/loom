"""Persist exact authenticated terminal inventory evidence.

Revision ID: capacity_0015
Revises: capacity_0014
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0015"
down_revision: str | Sequence[str] | None = "capacity_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "capacity_executable_terminal_inventory_evidence",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("intent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("executor_id", sa.Text(), nullable=False),
        sa.Column("executor_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("inventory_sequence", sa.BigInteger(), nullable=False),
        sa.Column("inventory_digest", sa.Text(), nullable=False),
        sa.Column("journal_sequence", sa.BigInteger(), nullable=False),
        sa.Column("journal_digest", sa.Text(), nullable=False),
        sa.Column("physical_kind", sa.Text(), nullable=False),
        sa.Column("physical_identity", sa.Text(), nullable=False),
        sa.Column("controller_evidence_sha256", sa.Text(), nullable=False),
        sa.Column("terminal_evidence_sha256", sa.Text(), nullable=False),
        sa.Column("evidence_digest", sa.Text(), nullable=False),
        sa.Column("evidence_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND pool_generation > 0 "
            "AND inventory_sequence > 0 AND journal_sequence >= 0",
            name="capacity_executable_terminal_inventory_quantity_check",
        ),
        sa.CheckConstraint(
            "pool_id IN ('oldlab','gb10') "
            "AND physical_kind IN ('slurm-job','worker')",
            name="capacity_executable_terminal_inventory_kind_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND inventory_digest ~ '^[0-9a-f]{64}$' "
            "AND journal_digest ~ '^[0-9a-f]{64}$' "
            "AND controller_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND terminal_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND evidence_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_terminal_inventory_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(evidence_payload) = 'object' "
            "AND octet_length(evidence_payload::text) <= 8388608",
            name="capacity_executable_terminal_inventory_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["intent_id"],
            ["public.capacity_executable_intents.intent_id"],
            name="capacity_executable_terminal_inventory_intent_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "intent_id",
            name="capacity_executable_terminal_inventory_intent_key",
        ),
        sa.UniqueConstraint(
            "executor_incarnation",
            "physical_kind",
            "physical_identity",
            name="capacity_executable_terminal_inventory_physical_key",
        ),
        schema="public",
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_terminal_inventory_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $$
        DECLARE
          intent_record record;
          executor_record record;
          payload_record jsonb;
        BEGIN
          SELECT intent.* INTO intent_record
            FROM public.capacity_executable_intents AS intent
           WHERE intent.intent_id = NEW.intent_id
           FOR SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'terminal inventory intent is unavailable'
              USING ERRCODE = '23514';
          END IF;
          SELECT executor.* INTO executor_record
            FROM public.capacity_executable_executor_states AS executor
           WHERE executor.execution_epoch = NEW.execution_epoch
             AND executor.execution_manifest_sha256 = NEW.execution_manifest_sha256
             AND executor.executor_id = NEW.executor_id
             AND executor.executor_incarnation = NEW.executor_incarnation
             AND executor.pool_id = NEW.pool_id
             AND executor.pool_generation = NEW.pool_generation
           FOR SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'terminal inventory executor is unavailable'
              USING ERRCODE = '23514';
          END IF;
          payload_record := NEW.evidence_payload -> 'record';
          IF NOT (
               NEW.evidence_payload ?& ARRAY[
                 'schema_version', 'binding', 'inventory_execution',
                 'inventory_sequence', 'inventory_digest', 'journal_sequence',
                 'journal_digest', 'record', 'observed_at', 'executable'
               ]
             )
             OR (SELECT count(*) FROM pg_catalog.jsonb_object_keys(NEW.evidence_payload)) <> 10
             OR NEW.evidence_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
             OR NEW.evidence_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb
             OR NEW.evidence_payload -> 'binding' IS DISTINCT FROM intent_record.binding_payload
             OR NEW.evidence_payload -> 'inventory_execution'
                  IS DISTINCT FROM executor_record.inventory_payload -> 'execution'
             OR (NEW.evidence_payload ->> 'inventory_sequence')::bigint
                  IS DISTINCT FROM NEW.inventory_sequence
             OR NEW.evidence_payload ->> 'inventory_digest'
                  IS DISTINCT FROM NEW.inventory_digest
             OR (NEW.evidence_payload ->> 'journal_sequence')::bigint
                  IS DISTINCT FROM NEW.journal_sequence
             OR NEW.evidence_payload ->> 'journal_digest' IS DISTINCT FROM NEW.journal_digest
             OR (NEW.evidence_payload ->> 'observed_at')::timestamptz
                  IS DISTINCT FROM NEW.observed_at
             OR pg_catalog.encode(
                  pg_catalog.sha256(
                    pg_catalog.convert_to(
                      public.capacity_executable_canonical_jsonb_text(NEW.evidence_payload),
                      'UTF8'
                    )
                  ),
                  'hex'
                ) IS DISTINCT FROM NEW.evidence_digest
             OR intent_record.subject_id IS DISTINCT FROM NEW.subject_id
             OR intent_record.subject_incarnation IS DISTINCT FROM NEW.subject_incarnation
             OR intent_record.execution_epoch IS DISTINCT FROM NEW.execution_epoch
             OR intent_record.execution_manifest_sha256
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR intent_record.executor_id IS DISTINCT FROM NEW.executor_id
             OR intent_record.executor_incarnation IS DISTINCT FROM NEW.executor_incarnation
             OR intent_record.pool_id IS DISTINCT FROM NEW.pool_id
             OR intent_record.pool_generation IS DISTINCT FROM NEW.pool_generation
             OR intent_record.state NOT IN ('terminal','closing','released')
             OR intent_record.inventory_sequence IS DISTINCT FROM NEW.inventory_sequence
             OR intent_record.observed_state IS DISTINCT FROM 'terminal'
             OR intent_record.terminal_kind IS DISTINCT FROM NEW.physical_kind
             OR intent_record.terminal_identity IS DISTINCT FROM NEW.physical_identity
             OR intent_record.terminal_evidence_sha256
                  IS DISTINCT FROM NEW.terminal_evidence_sha256
             OR executor_record.inventory_high_water IS DISTINCT FROM NEW.inventory_sequence
             OR executor_record.last_inventory_digest IS DISTINCT FROM NEW.inventory_digest
             OR executor_record.journal_high_water IS DISTINCT FROM NEW.journal_sequence
             OR executor_record.journal_digest IS DISTINCT FROM NEW.journal_digest
             OR executor_record.last_inventory_at IS DISTINCT FROM NEW.observed_at
             OR executor_record.inventory_payload IS NULL
             OR NEW.inventory_digest IS DISTINCT FROM pg_catalog.encode(
                  pg_catalog.sha256(
                    pg_catalog.convert_to(
                      public.capacity_executable_canonical_jsonb_text(
                        executor_record.inventory_payload
                      ),
                      'UTF8'
                    )
                  ),
                  'hex'
                )
             OR NOT EXISTS (
                  SELECT 1
                    FROM pg_catalog.jsonb_array_elements(
                           executor_record.inventory_payload -> 'records'
                         ) AS item(value)
                   WHERE item.value = payload_record
                )
             OR payload_record ->> 'state' IS DISTINCT FROM 'terminal'
             OR payload_record ->> 'authority_scope'
                  IS DISTINCT FROM 'dedicated-loom-association'
             OR payload_record ->> 'physical_kind' IS DISTINCT FROM NEW.physical_kind
             OR payload_record ->> 'physical_identity' IS DISTINCT FROM NEW.physical_identity
             OR payload_record ->> 'controller_evidence_sha256'
                  IS DISTINCT FROM NEW.controller_evidence_sha256
             OR payload_record ->> 'terminal_evidence_sha256'
                  IS DISTINCT FROM NEW.terminal_evidence_sha256
             OR payload_record -> 'ownership_proof' -> 'metadata' -> 'binding'
                  IS DISTINCT FROM intent_record.binding_payload THEN
            RAISE EXCEPTION 'terminal inventory evidence is not exact'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON FUNCTION "
        "public.capacity_executable_terminal_inventory_insert_guard() FROM PUBLIC"
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_terminal_inventory_insert_guard
        BEFORE INSERT ON public.capacity_executable_terminal_inventory_evidence
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_terminal_inventory_insert_guard()
        """
    )
    for suffix, operation in (
        ("append_only_guard", "UPDATE OR DELETE"),
        ("truncate_guard", "TRUNCATE"),
    ):
        level = "ROW" if operation != "TRUNCATE" else "STATEMENT"
        op.execute(
            f"""
            CREATE TRIGGER capacity_executable_terminal_inventory_{suffix}
            BEFORE {operation} ON public.capacity_executable_terminal_inventory_evidence
            FOR EACH {level}
            EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard()
            """
        )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.capacity_executable_terminal_inventory_evidence "
        "IN ACCESS EXCLUSIVE MODE"
    )
    exists = op.get_bind().execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM "
            "public.capacity_executable_terminal_inventory_evidence)"
        )
    ).scalar_one()
    if exists:
        raise RuntimeError(
            "cannot downgrade capacity_0015 while terminal inventory evidence exists"
        )
    op.execute(
        "DROP TRIGGER capacity_executable_terminal_inventory_truncate_guard ON "
        "public.capacity_executable_terminal_inventory_evidence"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_terminal_inventory_append_only_guard ON "
        "public.capacity_executable_terminal_inventory_evidence"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_terminal_inventory_insert_guard ON "
        "public.capacity_executable_terminal_inventory_evidence"
    )
    op.execute(
        "DROP FUNCTION public.capacity_executable_terminal_inventory_insert_guard()"
    )
    op.drop_table(
        "capacity_executable_terminal_inventory_evidence",
        schema="public",
    )
