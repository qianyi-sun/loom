"""Retain exact per-intent authority atomically with final physical release.

Revision ID: capacity_0023
Revises: capacity_0022
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0023"
down_revision: str | Sequence[str] | None = "capacity_0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "capacity_executable_final_release_witnesses"


def upgrade() -> None:
    op.execute("LOCK TABLE public.capacity_executable_intents IN ACCESS EXCLUSIVE MODE")
    op.create_table(
        _TABLE,
        sa.Column("intent_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("capacity_executable_intents.intent_id", ondelete="RESTRICT"), primary_key=True),
        sa.Column("protected_receipt_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("capacity_executable_protected_release_receipts.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("command_receipt_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("capacity_executable_command_receipts.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("release_payload", postgresql.JSONB(), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("jsonb_typeof(release_payload) = 'object' AND octet_length(release_payload::text) <= 8388608", name="capacity_final_release_payload_check"),
    )
    op.execute(f"""
        CREATE FUNCTION public.capacity_final_release_insert_guard()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        DECLARE
          intent public.capacity_executable_intents%ROWTYPE;
          protected public.capacity_executable_protected_release_receipts%ROWTYPE;
          command public.capacity_executable_command_receipts%ROWTYPE;
          expected jsonb;
        BEGIN
          SELECT * INTO intent FROM public.capacity_executable_intents
            WHERE intent_id=NEW.intent_id FOR UPDATE;
          IF NOT FOUND OR intent.state <> 'closing' THEN
            RAISE EXCEPTION 'final release requires a closing intent' USING ERRCODE='23514';
          END IF;
          SELECT * INTO protected FROM public.capacity_executable_protected_release_receipts
            WHERE intent_id=NEW.intent_id ORDER BY protected_registration_epoch DESC LIMIT 1 FOR SHARE;
          IF NOT FOUND OR protected.id <> NEW.protected_receipt_id
            OR protected.release_payload->'binding' IS DISTINCT FROM intent.binding_payload
            OR protected.bootstrap_registration_epoch IS DISTINCT FROM intent.bootstrap_registration_epoch THEN
            RAISE EXCEPTION 'final release protected receipt changed' USING ERRCODE='23514';
          END IF;
          SELECT * INTO command FROM public.capacity_executable_command_receipts
            WHERE id=NEW.command_receipt_id FOR SHARE;
          IF NOT FOUND OR command.operation_kind <> 'release'
            OR command.execution_epoch <> intent.execution_epoch
            OR command.executor_incarnation <> intent.executor_incarnation
            OR command.result_payload->>'tranche_id' IS DISTINCT FROM intent.tranche_id::text
            OR NOT coalesce(command.result_payload->'released_shape_ids' ? intent.shape_instance_id, false)
            OR command.result_payload->'executable' IS DISTINCT FROM 'true'::jsonb THEN
            RAISE EXCEPTION 'final release command binding changed' USING ERRCODE='23514';
          END IF;
          expected := jsonb_build_object(
            'schema_version',2, 'binding',intent.binding_payload,
            'inventory_sequence',intent.inventory_sequence,
            'terminal_kind',intent.terminal_kind, 'terminal_identity',intent.terminal_identity,
            'terminal_evidence_sha256',intent.terminal_evidence_sha256,
            'protected_registration_epoch',protected.protected_registration_epoch,
            'bootstrap_revoked',true, 'protected_release_sha256',protected.protected_release_sha256);
          IF NEW.release_payload IS DISTINCT FROM expected
            OR intent.inventory_sequence IS NULL OR intent.terminal_kind IS NULL
            OR intent.terminal_identity IS NULL OR intent.terminal_evidence_sha256 IS NULL THEN
            RAISE EXCEPTION 'final release physical evidence changed' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END $$;
        REVOKE ALL ON FUNCTION public.capacity_final_release_insert_guard() FROM PUBLIC;
        CREATE TRIGGER capacity_final_release_insert_guard BEFORE INSERT ON public.{_TABLE}
          FOR EACH ROW EXECUTE FUNCTION public.capacity_final_release_insert_guard();
        CREATE TRIGGER capacity_final_release_mutation_guard BEFORE UPDATE OR DELETE ON public.{_TABLE}
          FOR EACH ROW EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard();
        CREATE TRIGGER capacity_final_release_truncate_guard BEFORE TRUNCATE ON public.{_TABLE}
          FOR EACH STATEMENT EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard();

        CREATE FUNCTION public.capacity_final_release_pair_guard()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN
          IF TG_TABLE_NAME='capacity_executable_intents' THEN
            IF OLD.state <> 'closing' OR NEW.state <> 'released' THEN
              RETURN NULL;
            END IF;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM public.{_TABLE} AS witness
              JOIN public.capacity_executable_intents AS intent USING (intent_id)
            WHERE witness.intent_id=NEW.intent_id AND intent.state='released'
              AND intent.released_at=witness.released_at
          ) THEN
            RAISE EXCEPTION 'final release requires atomic retained witness and transition' USING ERRCODE='23514';
          END IF;
          RETURN NULL;
        END $$;
        REVOKE ALL ON FUNCTION public.capacity_final_release_pair_guard() FROM PUBLIC;
        CREATE CONSTRAINT TRIGGER capacity_final_release_witness_pair
          AFTER INSERT ON public.{_TABLE} DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW EXECUTE FUNCTION public.capacity_final_release_pair_guard();
        CREATE CONSTRAINT TRIGGER capacity_final_release_intent_pair
          AFTER UPDATE ON public.capacity_executable_intents DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW WHEN (OLD.state='closing' AND NEW.state='released')
          EXECUTE FUNCTION public.capacity_final_release_pair_guard();
    """)


def downgrade() -> None:
    op.execute(f"LOCK TABLE public.capacity_executable_intents, public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM public.{_TABLE})")):
        raise RuntimeError("cannot downgrade capacity_0023 with retained final release witnesses")
    op.execute("DROP TRIGGER capacity_final_release_intent_pair ON public.capacity_executable_intents")
    op.drop_table(_TABLE)
    op.execute("DROP FUNCTION public.capacity_final_release_pair_guard()")
    op.execute("DROP FUNCTION public.capacity_final_release_insert_guard()")
