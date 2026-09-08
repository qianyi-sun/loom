"""Persist append-only active personal membership checkpoints.

Revision ID: capacity_0016
Revises: capacity_0015
Create Date: 2026-09-08
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0016"
down_revision: str | Sequence[str] | None = "capacity_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "capacity_personal_membership_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("authority_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("writer_epoch", sa.BigInteger(), nullable=False),
        sa.Column("namespace_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("previous_sha256", sa.Text(), nullable=False),
        sa.Column("head_sha256", sa.Text(), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("idempotency_key", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("request_digest", sa.Text(), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("subject_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("configuration_generation", sa.BigInteger(), nullable=False),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("reporter_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("result_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND writer_epoch > 0 AND revision > 0 "
            "AND configuration_generation > 0 AND deployment_generation > 0",
            name="capacity_personal_membership_quantity_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND request_digest ~ '^[0-9a-f]{64}$' "
            "AND previous_sha256 ~ '^[0-9a-f]{64}$' "
            "AND head_sha256 ~ '^[0-9a-f]{64}$'",
            name="capacity_personal_membership_digest_check",
        ),
        sa.CheckConstraint(
            "octet_length(actor) BETWEEN 1 AND 128",
            name="capacity_personal_membership_actor_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_payload) = 'object' "
            "AND jsonb_typeof(result_payload) = 'object' "
            "AND octet_length(request_payload::text) <= 8388608 "
            "AND octet_length(result_payload::text) <= 8388608",
            name="capacity_personal_membership_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["execution_epoch"],
            ["public.capacity_execution_epochs.execution_epoch"],
            name="capacity_personal_membership_execution_epoch_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "execution_epoch",
            "revision",
            name="capacity_personal_membership_epoch_revision_key",
        ),
        sa.UniqueConstraint(
            "operation_id",
            name="capacity_personal_membership_operation_key",
        ),
        sa.UniqueConstraint(
            "idempotency_key",
            name="capacity_personal_membership_idempotency_key",
        ),
        schema="public",
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_personal_membership_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $$
        DECLARE
          authority_record record;
          epoch_record record;
          previous_record record;
          calculated_head text;
        BEGIN
          SELECT authority.* INTO authority_record
            FROM public.capacity_authority_state AS authority
           WHERE authority.singleton_id = 1
           FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'personal membership authority is unavailable'
              USING ERRCODE = '23514';
          END IF;
          SELECT epoch.* INTO epoch_record
            FROM public.capacity_execution_epochs AS epoch
           WHERE epoch.execution_epoch = NEW.execution_epoch
           FOR SHARE;
          IF NOT FOUND
             OR authority_record.execution_state IS DISTINCT FROM 'active'
             OR epoch_record.state IS DISTINCT FROM 'active'
             OR authority_record.execution_epoch IS DISTINCT FROM NEW.execution_epoch
             OR epoch_record.execution_manifest_sha256
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR authority_record.execution_manifest_sha256
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR authority_record.authority_incarnation
                  IS DISTINCT FROM NEW.authority_incarnation
             OR epoch_record.authority_incarnation
                  IS DISTINCT FROM NEW.authority_incarnation
             OR authority_record.writer_epoch IS DISTINCT FROM NEW.writer_epoch
             OR epoch_record.current_writer_epoch IS DISTINCT FROM NEW.writer_epoch
             OR epoch_record.manifest_payload -> 'schema_version' IS DISTINCT FROM '3'::jsonb
             OR (epoch_record.manifest_payload -> 'personal_membership' ->> 'namespace_id')::uuid
                  IS DISTINCT FROM NEW.namespace_id
             OR epoch_record.manifest_payload -> 'personal_membership'
                    ->> 'management_principal_id' IS DISTINCT FROM NEW.actor THEN
            RAISE EXCEPTION 'personal membership execution authority changed'
              USING ERRCODE = '23514';
          END IF;

          SELECT event.* INTO previous_record
            FROM public.capacity_personal_membership_events AS event
           WHERE event.execution_epoch = NEW.execution_epoch
           ORDER BY event.revision DESC
           LIMIT 1
           FOR SHARE;
          IF (NOT FOUND AND (NEW.revision <> 1 OR NEW.previous_sha256 <> repeat('0', 64)))
             OR (FOUND AND (
                  NEW.revision IS DISTINCT FROM previous_record.revision + 1
                  OR NEW.previous_sha256 IS DISTINCT FROM previous_record.head_sha256
                )) THEN
            RAISE EXCEPTION 'personal membership revision is not consecutive'
              USING ERRCODE = '23514';
          END IF;

          calculated_head := pg_catalog.encode(
            pg_catalog.sha256(
              pg_catalog.convert_to(
                public.capacity_executable_canonical_jsonb_text(
                  pg_catalog.jsonb_build_object(
                    'actor', NEW.actor,
                    'execution_epoch', NEW.execution_epoch,
                    'idempotency_key', NEW.idempotency_key::text,
                    'operation_id', NEW.operation_id::text,
                    'previous_sha256', NEW.previous_sha256,
                    'request_digest', NEW.request_digest,
                    'request_payload', NEW.request_payload,
                    'result_member', NEW.result_payload -> 'member',
                    'revision', NEW.revision
                  )
                ),
                'UTF8'
              )
            ),
            'hex'
          );
          IF NEW.request_payload -> 'schema_version' IS DISTINCT FROM '1'::jsonb
             OR NEW.request_payload -> 'namespace_id'
                  IS DISTINCT FROM pg_catalog.to_jsonb(NEW.namespace_id::text)
             OR (NEW.request_payload ->> 'expected_revision')::bigint
                  IS DISTINCT FROM NEW.revision - 1
             OR NEW.request_payload -> 'execution' ->> 'execution_state'
                  IS DISTINCT FROM 'active'
             OR (NEW.request_payload -> 'execution' ->> 'execution_epoch')::bigint
                  IS DISTINCT FROM NEW.execution_epoch
             OR NEW.request_payload -> 'execution' ->> 'execution_manifest_sha256'
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR (NEW.request_payload -> 'execution' ->> 'writer_epoch')::bigint
                  IS DISTINCT FROM NEW.writer_epoch
             OR (NEW.request_payload -> 'execution' ->> 'authority_incarnation')::uuid
                  IS DISTINCT FROM NEW.authority_incarnation
             OR (NEW.request_payload -> 'projection' ->> 'operation_id')::uuid
                  IS DISTINCT FROM NEW.operation_id
             OR (NEW.request_payload -> 'projection' ->> 'expected_configuration_epoch')::bigint
                  IS DISTINCT FROM epoch_record.configuration_epoch
             OR (NEW.request_payload -> 'projection' ->> 'subject_id')::uuid
                  IS DISTINCT FROM NEW.subject_id
             OR (NEW.request_payload -> 'projection' ->> 'subject_incarnation')::uuid
                  IS DISTINCT FROM NEW.subject_incarnation
             OR (NEW.request_payload -> 'projection' ->> 'owner_id')::uuid
                  IS DISTINCT FROM NEW.owner_id
             OR (NEW.request_payload -> 'projection' ->> 'configuration_generation')::bigint
                  IS DISTINCT FROM NEW.configuration_generation
             OR (NEW.request_payload -> 'projection' ->> 'deployment_generation')::bigint
                  IS DISTINCT FROM NEW.deployment_generation
             OR (NEW.request_payload -> 'projection' ->> 'candidate_generation')::bigint
                  IS DISTINCT FROM (
                    NEW.result_payload -> 'member' -> 'configuration'
                      ->> 'candidate_generation'
                  )::bigint
             OR (NEW.request_payload -> 'projection' ->> 'demand_reporter_incarnation')::uuid
                  IS DISTINCT FROM NEW.reporter_incarnation
             OR NEW.request_digest IS DISTINCT FROM pg_catalog.encode(
                  pg_catalog.sha256(
                    pg_catalog.convert_to(
                      public.capacity_executable_canonical_jsonb_text(NEW.request_payload),
                      'UTF8'
                    )
                  ),
                  'hex'
                )
             OR NEW.result_payload -> 'schema_version' IS DISTINCT FROM '1'::jsonb
             OR (NEW.result_payload ->> 'revision')::bigint IS DISTINCT FROM NEW.revision
             OR NEW.result_payload ->> 'head_sha256' IS DISTINCT FROM NEW.head_sha256
             OR NEW.result_payload -> 'replayed' IS DISTINCT FROM 'false'::jsonb
             OR (NEW.result_payload -> 'member' ->> 'revision')::bigint
                  IS DISTINCT FROM NEW.revision
             OR (NEW.result_payload -> 'member' ->> 'owner_id')::uuid
                  IS DISTINCT FROM NEW.owner_id
             OR (NEW.result_payload -> 'member' -> 'configuration' ->> 'subject_id')::uuid
                  IS DISTINCT FROM NEW.subject_id
             OR (NEW.result_payload -> 'member' -> 'configuration'
                    ->> 'subject_incarnation')::uuid IS DISTINCT FROM NEW.subject_incarnation
             OR (NEW.result_payload -> 'member' -> 'configuration'
                    ->> 'configuration_generation')::bigint
                  IS DISTINCT FROM NEW.configuration_generation
             OR (NEW.result_payload -> 'member' -> 'configuration'
                    ->> 'deployment_generation')::bigint
                  IS DISTINCT FROM NEW.deployment_generation
             OR (NEW.result_payload -> 'member' -> 'configuration'
                    ->> 'demand_reporter_incarnation')::uuid
                  IS DISTINCT FROM NEW.reporter_incarnation
             OR NEW.request_payload -> 'acknowledgement'
                  IS DISTINCT FROM NEW.result_payload -> 'member' -> 'acknowledgement'
             OR (NEW.result_payload -> 'member' -> 'acknowledgement'
                    ->> 'subject_id')::uuid IS DISTINCT FROM NEW.subject_id
             OR (NEW.result_payload -> 'member' -> 'acknowledgement'
                    ->> 'subject_incarnation')::uuid
                  IS DISTINCT FROM NEW.subject_incarnation
             OR (NEW.result_payload -> 'member' -> 'acknowledgement'
                    ->> 'configuration_generation')::bigint
                  IS DISTINCT FROM NEW.configuration_generation
             OR (NEW.result_payload -> 'member' -> 'acknowledgement'
                    ->> 'deployment_generation')::bigint
                  IS DISTINCT FROM NEW.deployment_generation
             OR (NEW.result_payload -> 'member' -> 'acknowledgement'
                    ->> 'reporter_incarnation')::uuid
                  IS DISTINCT FROM NEW.reporter_incarnation
             OR NEW.result_payload -> 'member' -> 'acknowledgement'
                    -> 'candidate' ->> 'algorithm' IS DISTINCT FROM 'source-sha256'
             OR NEW.result_payload -> 'member' -> 'acknowledgement'
                    -> 'candidate' ->> 'identity'
                  IS DISTINCT FROM NEW.request_payload -> 'projection' ->> 'candidate_sha256'
             OR NEW.result_payload -> 'member' -> 'acknowledgement'
                    -> 'candidate' ->> 'publication_sha256'
                  IS DISTINCT FROM NEW.request_payload -> 'projection'
                    ->> 'candidate_publication_sha256'
             OR NEW.result_payload -> 'member' -> 'acknowledgement'
                    ->> 'protected_admission_sha256'
                  IS DISTINCT FROM NEW.request_payload -> 'projection'
                    ->> 'protected_admission_sha256'
             OR (
                  NEW.request_payload -> 'projection' ->> 'operation_kind' = 'destroy'
                  AND (
                    (NEW.result_payload -> 'member' -> 'configuration'
                      ->> 'min_slots')::bigint IS DISTINCT FROM 0
                    OR (NEW.result_payload -> 'member' -> 'configuration'
                      ->> 'max_slots')::bigint IS DISTINCT FROM 0
                  )
                )
             OR (
                  NEW.request_payload -> 'projection' ->> 'operation_kind' <> 'destroy'
                  AND (
                    (NEW.result_payload -> 'member' -> 'configuration'
                      ->> 'min_slots')::bigint IS DISTINCT FROM (
                        NEW.request_payload -> 'projection' ->> 'min_slots'
                      )::bigint
                    OR (NEW.result_payload -> 'member' -> 'configuration'
                      ->> 'max_slots')::bigint IS DISTINCT FROM (
                        NEW.request_payload -> 'projection' ->> 'max_slots'
                      )::bigint
                  )
                )
             OR NEW.result_payload -> 'member' -> 'configuration' ->> 'display_name'
                  IS DISTINCT FROM 'dev-' || (
                    NEW.request_payload -> 'projection' ->> 'environment_name'
                  )
             OR NEW.request_payload -> 'projection' ->> 'operation_kind' IS NULL
             OR NEW.request_payload -> 'projection' ->> 'operation_kind'
                  NOT IN ('create', 'update', 'capacity', 'destroy')
             OR (
                  NEW.request_payload -> 'projection' ->> 'operation_kind' = 'destroy'
                  AND NEW.result_payload -> 'member' -> 'configuration'
                        ->> 'lifecycle_state' IS DISTINCT FROM 'disabled'
                )
             OR (
                  NEW.request_payload -> 'projection' ->> 'operation_kind' <> 'destroy'
                  AND NEW.result_payload -> 'member' -> 'configuration'
                        ->> 'lifecycle_state' IS DISTINCT FROM 'active'
                )
             OR NEW.head_sha256 IS DISTINCT FROM calculated_head THEN
            RAISE EXCEPTION 'personal membership event payload is not exact'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON FUNCTION "
        "public.capacity_personal_membership_insert_guard() FROM PUBLIC"
    )
    op.execute(
        """
        CREATE TRIGGER capacity_personal_membership_insert_guard
        BEFORE INSERT ON public.capacity_personal_membership_events
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_personal_membership_insert_guard()
        """
    )
    for suffix, operation in (
        ("append_only_guard", "UPDATE OR DELETE"),
        ("truncate_guard", "TRUNCATE"),
    ):
        level = "ROW" if operation != "TRUNCATE" else "STATEMENT"
        op.execute(
            f"""
            CREATE TRIGGER capacity_personal_membership_{suffix}
            BEFORE {operation} ON public.capacity_personal_membership_events
            FOR EACH {level}
            EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard()
            """
        )


def downgrade() -> None:
    op.execute("LOCK TABLE public.capacity_personal_membership_events IN ACCESS EXCLUSIVE MODE")
    op.execute(
        "LOCK TABLE public.capacity_execution_epochs, public.capacity_executable_intents "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    bind = op.get_bind()
    if bind.execute(
        sa.text("SELECT EXISTS (SELECT 1 FROM public.capacity_personal_membership_events)")
    ).scalar_one():
        raise RuntimeError("cannot downgrade capacity_0016 while personal membership exists")
    if bind.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM public.capacity_execution_epochs "
            "WHERE manifest_payload -> 'schema_version' = '3'::jsonb "
            "AND state <> 'retired')"
        )
    ).scalar_one():
        raise RuntimeError("cannot downgrade capacity_0016 while delegated execution exists")
    if bind.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM public.capacity_executable_intents AS intent "
            "JOIN public.capacity_execution_epochs AS epoch "
            "ON epoch.execution_epoch = intent.execution_epoch "
            "WHERE epoch.manifest_payload -> 'schema_version' = '3'::jsonb "
            "AND intent.state NOT IN ('released', 'quarantined'))"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade capacity_0016 while delegated intents remain unreleased"
        )
    op.execute(
        "DROP TRIGGER capacity_personal_membership_truncate_guard ON "
        "public.capacity_personal_membership_events"
    )
    op.execute(
        "DROP TRIGGER capacity_personal_membership_append_only_guard ON "
        "public.capacity_personal_membership_events"
    )
    op.execute(
        "DROP TRIGGER capacity_personal_membership_insert_guard ON "
        "public.capacity_personal_membership_events"
    )
    op.execute("DROP FUNCTION public.capacity_personal_membership_insert_guard()")
    op.drop_table("capacity_personal_membership_events", schema="public")
