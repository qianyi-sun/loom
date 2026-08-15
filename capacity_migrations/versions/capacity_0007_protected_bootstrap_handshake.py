"""require protected subject acknowledgement before executable bootstrap

Revision ID: capacity_0007
Revises: capacity_0006
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0007"
down_revision: str | Sequence[str] | None = "capacity_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _append_only(table_name: str) -> None:
    op.execute(
        f"""
        CREATE TRIGGER {table_name}_append_only_guard
        BEFORE UPDATE OR DELETE ON public.{table_name}
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {table_name}_truncate_guard
        BEFORE TRUNCATE ON public.{table_name}
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard()
        """
    )


def upgrade() -> None:
    op.create_table(
        "capacity_executable_bootstrap_proposals",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("intent_id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("proposal_epoch", sa.BigInteger(), nullable=False),
        sa.Column("command_sequence", sa.BigInteger(), nullable=False),
        sa.Column("bootstrap_sha256", sa.Text(), nullable=False),
        sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("proposal_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND proposal_epoch > 0 AND command_sequence > 0",
            name="capacity_executable_bootstrap_proposal_quantity_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND bootstrap_sha256 ~ '^[0-9a-f]{64}$' "
            "AND proposal_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_bootstrap_proposal_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(proposal_payload) = 'object' "
            "AND octet_length(proposal_payload::text) <= 8388608",
            name="capacity_executable_bootstrap_proposal_payload_check",
        ),
        sa.CheckConstraint(
            "expires_at > created_at AND expires_at <= created_at + interval '10 minutes'",
            name="capacity_executable_bootstrap_proposal_expiry_check",
        ),
        sa.ForeignKeyConstraint(
            ["intent_id"],
            ["public.capacity_executable_intents.intent_id"],
            name="capacity_executable_bootstrap_proposal_intent_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "intent_id",
            "proposal_epoch",
            name="capacity_executable_bootstrap_proposal_epoch_key",
        ),
        sa.UniqueConstraint(
            "intent_id",
            "execution_epoch",
            "execution_manifest_sha256",
            "proposal_epoch",
            "proposal_digest",
            name="capacity_executable_bootstrap_proposal_exact_key",
        ),
        schema="public",
    )
    op.create_table(
        "capacity_executable_bootstrap_acknowledgements",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("intent_id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("proposal_epoch", sa.BigInteger(), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("reporter_incarnation", sa.UUID(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("bootstrap_evidence_sha256", sa.Text(), nullable=False),
        sa.Column("protected_admission_sha256", sa.Text(), nullable=False),
        sa.Column("acknowledgement_digest", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=False),
        sa.Column(
            "acknowledgement_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND proposal_epoch > 0 "
            "AND bootstrap_registration_epoch > 0",
            name="capacity_executable_bootstrap_ack_quantity_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND proposal_digest ~ '^[0-9a-f]{64}$' "
            "AND bootstrap_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
            "AND acknowledgement_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_bootstrap_ack_digest_check",
        ),
        sa.CheckConstraint(
            "actor_id <> '' AND octet_length(actor_id) <= 256 "
            "AND jsonb_typeof(acknowledgement_payload) = 'object' "
            "AND octet_length(acknowledgement_payload::text) <= 8388608",
            name="capacity_executable_bootstrap_ack_payload_check",
        ),
        sa.ForeignKeyConstraint(
            [
                "intent_id",
                "execution_epoch",
                "execution_manifest_sha256",
                "proposal_epoch",
                "proposal_digest",
            ],
            [
                "public.capacity_executable_bootstrap_proposals.intent_id",
                "public.capacity_executable_bootstrap_proposals.execution_epoch",
                "public.capacity_executable_bootstrap_proposals.execution_manifest_sha256",
                "public.capacity_executable_bootstrap_proposals.proposal_epoch",
                "public.capacity_executable_bootstrap_proposals.proposal_digest",
            ],
            name="capacity_executable_bootstrap_ack_proposal_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="capacity_executable_bootstrap_ack_idempotency_key",
        ),
        sa.UniqueConstraint("intent_id", name="capacity_executable_bootstrap_ack_intent_key"),
        schema="public",
    )
    _append_only("capacity_executable_bootstrap_proposals")
    _append_only("capacity_executable_bootstrap_acknowledgements")
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_bootstrap_proposal_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          intent_binding jsonb;
          intent_state text;
          latest_epoch bigint;
          latest_expires_at timestamptz;
        BEGIN
          SELECT intent.binding_payload, intent.state
            INTO intent_binding, intent_state
            FROM public.capacity_executable_intents AS intent
           WHERE intent.intent_id = NEW.intent_id
             AND intent.execution_epoch = NEW.execution_epoch
             AND intent.execution_manifest_sha256 = NEW.execution_manifest_sha256
           FOR UPDATE;
          IF NOT FOUND OR intent_state IS DISTINCT FROM 'accepted' THEN
            RAISE EXCEPTION 'executable bootstrap proposal intent is not accepted'
              USING ERRCODE = '23514';
          END IF;
          IF pg_catalog.jsonb_typeof(NEW.proposal_payload) IS DISTINCT FROM 'object'
             OR NEW.proposal_payload - ARRAY[
                  'schema_version',
                  'binding',
                  'command_sequence',
                  'proposal_epoch',
                  'bootstrap_sha256',
                  'expires_at',
                  'executable'
                ]::text[] IS DISTINCT FROM '{}'::jsonb
             OR (NEW.proposal_payload ->> 'schema_version')::bigint IS DISTINCT FROM 2
             OR NEW.proposal_payload -> 'binding' IS DISTINCT FROM intent_binding
             OR (NEW.proposal_payload ->> 'command_sequence')::bigint
                  IS DISTINCT FROM NEW.command_sequence
             OR (NEW.proposal_payload ->> 'proposal_epoch')::bigint
                  IS DISTINCT FROM NEW.proposal_epoch
             OR NEW.proposal_payload ->> 'bootstrap_sha256'
                  IS DISTINCT FROM NEW.bootstrap_sha256
             OR (NEW.proposal_payload ->> 'expires_at')::timestamptz
                  IS DISTINCT FROM NEW.expires_at
             OR NEW.proposal_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb THEN
            RAISE EXCEPTION 'executable bootstrap proposal payload binding changed'
              USING ERRCODE = '23514';
          END IF;
          SELECT proposal.proposal_epoch, proposal.expires_at
            INTO latest_epoch, latest_expires_at
            FROM public.capacity_executable_bootstrap_proposals AS proposal
           WHERE proposal.intent_id = NEW.intent_id
           ORDER BY proposal.proposal_epoch DESC
           LIMIT 1;
          IF latest_epoch IS NULL THEN
            IF NEW.proposal_epoch IS DISTINCT FROM 1 THEN
              RAISE EXCEPTION 'executable bootstrap proposal epoch must start at one'
                USING ERRCODE = '23514';
            END IF;
          ELSIF NEW.proposal_epoch IS DISTINCT FROM latest_epoch + 1
             OR latest_expires_at > pg_catalog.clock_timestamp() THEN
            RAISE EXCEPTION 'executable bootstrap proposal epoch cannot advance'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_bootstrap_proposal_insert_guard
        BEFORE INSERT ON public.capacity_executable_bootstrap_proposals
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_bootstrap_proposal_insert_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_bootstrap_ack_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          intent_binding jsonb;
          intent_state text;
          intent_subject_id uuid;
          intent_subject_incarnation uuid;
          proposal_record record;
        BEGIN
          SELECT intent.binding_payload, intent.state,
                 intent.subject_id, intent.subject_incarnation
            INTO intent_binding, intent_state,
                 intent_subject_id, intent_subject_incarnation
            FROM public.capacity_executable_intents AS intent
           WHERE intent.intent_id = NEW.intent_id
             AND intent.execution_epoch = NEW.execution_epoch
             AND intent.execution_manifest_sha256 = NEW.execution_manifest_sha256
           FOR UPDATE;
          IF NOT FOUND OR intent_state IS DISTINCT FROM 'accepted' THEN
            RAISE EXCEPTION 'executable bootstrap acknowledgement intent is not accepted'
              USING ERRCODE = '23514';
          END IF;
          SELECT proposal.* INTO proposal_record
            FROM public.capacity_executable_bootstrap_proposals AS proposal
           WHERE proposal.intent_id = NEW.intent_id
           ORDER BY proposal.proposal_epoch DESC
           LIMIT 1;
          IF NOT FOUND
             OR proposal_record.proposal_epoch IS DISTINCT FROM NEW.proposal_epoch
             OR proposal_record.proposal_digest IS DISTINCT FROM NEW.proposal_digest
             OR proposal_record.expires_at <= pg_catalog.clock_timestamp() THEN
            RAISE EXCEPTION 'executable bootstrap acknowledgement proposal changed or expired'
              USING ERRCODE = '23514';
          END IF;
          PERFORM 1
            FROM public.capacity_demand_reporters AS reporter
           WHERE reporter.subject_id = intent_subject_id
             AND reporter.subject_incarnation = intent_subject_incarnation
             AND reporter.reporter_incarnation = NEW.reporter_incarnation
             AND reporter.state = 'current'
           FOR SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'executable bootstrap acknowledgement reporter changed'
              USING ERRCODE = '23514';
          END IF;
          PERFORM 1
            FROM public.capacity_execution_epochs AS epoch,
                 LATERAL pg_catalog.jsonb_array_elements(
                   epoch.manifest_payload -> 'subject_acknowledgements'
                 ) AS acknowledgement
           WHERE epoch.execution_epoch = NEW.execution_epoch
             AND epoch.execution_manifest_sha256 = NEW.execution_manifest_sha256
             AND (acknowledgement ->> 'subject_id')::uuid = intent_subject_id
             AND (acknowledgement ->> 'subject_incarnation')::uuid
                  = intent_subject_incarnation
             AND (acknowledgement ->> 'reporter_incarnation')::uuid
                  = NEW.reporter_incarnation
             AND acknowledgement ->> 'protected_admission_sha256'
                  = NEW.protected_admission_sha256
           FOR SHARE OF epoch;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'executable bootstrap acknowledgement protected admission changed'
              USING ERRCODE = '23514';
          END IF;
          IF pg_catalog.jsonb_typeof(NEW.acknowledgement_payload) IS DISTINCT FROM 'object'
             OR NEW.acknowledgement_payload - ARRAY[
                  'schema_version',
                  'binding',
                  'proposal_epoch',
                  'proposal_digest',
                  'reporter_incarnation',
                  'bootstrap_registration_epoch',
                  'bootstrap_evidence_sha256',
                  'protected_admission_sha256',
                  'executable'
                ]::text[] IS DISTINCT FROM '{}'::jsonb
             OR (NEW.acknowledgement_payload ->> 'schema_version')::bigint
                  IS DISTINCT FROM 2
             OR NEW.acknowledgement_payload -> 'binding' IS DISTINCT FROM intent_binding
             OR (NEW.acknowledgement_payload ->> 'proposal_epoch')::bigint
                  IS DISTINCT FROM NEW.proposal_epoch
             OR NEW.acknowledgement_payload ->> 'proposal_digest'
                  IS DISTINCT FROM NEW.proposal_digest
             OR (NEW.acknowledgement_payload ->> 'reporter_incarnation')::uuid
                  IS DISTINCT FROM NEW.reporter_incarnation
             OR (NEW.acknowledgement_payload ->> 'bootstrap_registration_epoch')::bigint
                  IS DISTINCT FROM NEW.bootstrap_registration_epoch
             OR NEW.acknowledgement_payload ->> 'bootstrap_evidence_sha256'
                  IS DISTINCT FROM NEW.bootstrap_evidence_sha256
             OR NEW.acknowledgement_payload ->> 'protected_admission_sha256'
                  IS DISTINCT FROM NEW.protected_admission_sha256
             OR NEW.acknowledgement_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb THEN
            RAISE EXCEPTION 'executable bootstrap acknowledgement payload binding changed'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_bootstrap_ack_insert_guard
        BEFORE INSERT ON public.capacity_executable_bootstrap_acknowledgements
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_bootstrap_ack_insert_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          IF OLD.state = 'accepted' AND NEW.state = 'launch-ready' THEN
            PERFORM 1
              FROM public.capacity_executable_bootstrap_acknowledgements AS acknowledgement
             WHERE acknowledgement.intent_id = NEW.intent_id
               AND acknowledgement.execution_epoch = NEW.execution_epoch
               AND acknowledgement.execution_manifest_sha256
                    = NEW.execution_manifest_sha256
               AND acknowledgement.bootstrap_registration_epoch
                    = NEW.bootstrap_registration_epoch
               AND acknowledgement.bootstrap_evidence_sha256
                    = NEW.bootstrap_evidence_sha256;
            IF NOT FOUND THEN
              RAISE EXCEPTION
                'executable intent launch readiness requires protected bootstrap acknowledgement'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_intent_protected_bootstrap_guard
        BEFORE UPDATE ON public.capacity_executable_intents
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()
        """
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.capacity_executable_bootstrap_acknowledgements, "
        "public.capacity_executable_bootstrap_proposals IN ACCESS EXCLUSIVE MODE"
    )
    if op.get_bind().execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_executable_bootstrap_acknowledgements"
            ") OR EXISTS ("
            "SELECT 1 FROM public.capacity_executable_bootstrap_proposals"
            ")"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade capacity_0007 while protected bootstrap evidence exists"
        )
    op.execute(
        "DROP TRIGGER capacity_executable_intent_protected_bootstrap_guard "
        "ON public.capacity_executable_intents"
    )
    op.execute("DROP FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()")
    op.execute(
        "DROP TRIGGER capacity_executable_bootstrap_ack_insert_guard "
        "ON public.capacity_executable_bootstrap_acknowledgements"
    )
    op.execute("DROP FUNCTION public.capacity_executable_bootstrap_ack_insert_guard()")
    op.execute(
        "DROP TRIGGER capacity_executable_bootstrap_proposal_insert_guard "
        "ON public.capacity_executable_bootstrap_proposals"
    )
    op.execute("DROP FUNCTION public.capacity_executable_bootstrap_proposal_insert_guard()")
    for table_name in (
        "capacity_executable_bootstrap_acknowledgements",
        "capacity_executable_bootstrap_proposals",
    ):
        op.execute(f"DROP TRIGGER {table_name}_truncate_guard ON public.{table_name}")
        op.execute(f"DROP TRIGGER {table_name}_append_only_guard ON public.{table_name}")
    op.drop_table("capacity_executable_bootstrap_acknowledgements", schema="public")
    op.drop_table("capacity_executable_bootstrap_proposals", schema="public")
