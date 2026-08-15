"""add the fenced executable work queue

Revision ID: capacity_0006
Revises: capacity_0005
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0006"
down_revision: str | Sequence[str] | None = "capacity_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "capacity_allocation_epochs",
        sa.Column("input_valid_until", postgresql.TIMESTAMP(timezone=True), nullable=True),
        schema="public",
    )
    op.execute(
        "ALTER TABLE public.capacity_allocation_epochs "
        "DISABLE TRIGGER capacity_allocation_epoch_binding_guard"
    )
    op.execute(
        sa.text(
            "UPDATE public.capacity_allocation_epochs "
            "SET input_valid_until = committed_at "
            "WHERE status = 'executable' AND input_valid_until IS NULL"
        )
    )
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    op.execute(
        "ALTER TABLE public.capacity_allocation_epochs "
        "ENABLE TRIGGER capacity_allocation_epoch_binding_guard"
    )
    op.drop_constraint(
        "capacity_allocation_epoch_mode_check",
        "capacity_allocation_epochs",
        schema="public",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_allocation_epoch_mode_check",
        "capacity_allocation_epochs",
        "(status IN ('shadow','failed') AND executable = false "
        "AND execution_epoch IS NULL AND execution_manifest_sha256 IS NULL "
        "AND input_valid_until IS NULL "
        "AND sealed = true AND allocation_count IS NULL) OR "
        "(status = 'executable' AND executable = true "
        "AND execution_epoch IS NOT NULL AND execution_manifest_sha256 IS NOT NULL "
        "AND input_valid_until IS NOT NULL "
        "AND allocation_count IS NOT NULL AND allocation_count >= 0 "
        "AND COALESCE(jsonb_typeof(complete_payload -> 'allocations') = 'array', false) "
        "AND COALESCE(jsonb_array_length(complete_payload -> 'allocations') "
        "= allocation_count, false))",
        schema="public",
    )
    op.create_unique_constraint(
        "capacity_execution_executor_exact_binding_key",
        "capacity_execution_executors",
        [
            "execution_epoch",
            "execution_manifest_sha256",
            "executor_id",
            "executor_incarnation",
            "pool_id",
            "pool_generation",
        ],
        schema="public",
    )
    op.create_table(
        "capacity_executable_executor_states",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("executor_id", sa.Text(), nullable=False),
        sa.Column("executor_incarnation", sa.UUID(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.Text(), server_default=sa.text("'current'"), nullable=False),
        sa.Column("heartbeat_high_water", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("last_heartbeat_digest", sa.Text(), nullable=True),
        sa.Column("command_high_water", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("last_command_digest", sa.Text(), nullable=True),
        sa.Column("journal_high_water", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column(
            "journal_digest",
            sa.Text(),
            server_default=sa.text("repeat('0', 64)"),
            nullable=False,
        ),
        sa.Column("inventory_high_water", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("last_inventory_digest", sa.Text(), nullable=True),
        sa.Column("inventory_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_inventory_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("lease_expires_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_heartbeat_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND pool_generation > 0 "
            "AND pool_id IN ('gb10','oldlab') AND heartbeat_high_water >= 0 "
            "AND command_high_water >= 0 AND journal_high_water >= 0 "
            "AND inventory_high_water >= 0",
            name="capacity_executable_executor_state_quantity_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND ((heartbeat_high_water = 0 AND last_heartbeat_digest IS NULL) "
            "OR (heartbeat_high_water > 0 AND last_heartbeat_digest ~ '^[0-9a-f]{64}$')) "
            "AND ((command_high_water = 0 AND last_command_digest IS NULL) "
            "OR (command_high_water > 0 AND last_command_digest ~ '^[0-9a-f]{64}$')) "
            "AND ((journal_high_water = 0 AND journal_digest = repeat('0', 64)) "
            "OR (journal_high_water > 0 AND journal_digest ~ '^[0-9a-f]{64}$' "
            "AND journal_digest <> repeat('0', 64))) "
            "AND ((inventory_high_water = 0 AND last_inventory_digest IS NULL) "
            "OR (inventory_high_water > 0 AND last_inventory_digest ~ '^[0-9a-f]{64}$'))",
            name="capacity_executable_executor_state_digest_check",
        ),
        sa.CheckConstraint(
            "state IN ('current','fenced','equivocal')",
            name="capacity_executable_executor_state_check",
        ),
        sa.ForeignKeyConstraint(
            [
                "execution_epoch",
                "execution_manifest_sha256",
                "executor_id",
                "executor_incarnation",
                "pool_id",
                "pool_generation",
            ],
            [
                "public.capacity_execution_executors.execution_epoch",
                "public.capacity_execution_executors.execution_manifest_sha256",
                "public.capacity_execution_executors.executor_id",
                "public.capacity_execution_executors.executor_incarnation",
                "public.capacity_execution_executors.pool_id",
                "public.capacity_execution_executors.pool_generation",
            ],
            name="capacity_executable_executor_registration_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "execution_epoch", "pool_id", name="capacity_executable_executor_state_pool_key"
        ),
        sa.UniqueConstraint(
            "executor_incarnation", name="capacity_executable_executor_state_incarnation_key"
        ),
        sa.UniqueConstraint(
            "execution_epoch",
            "executor_incarnation",
            name="capacity_executable_executor_state_epoch_incarnation_key",
        ),
        schema="public",
    )
    op.create_table(
        "capacity_executable_intents",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("intent_id", sa.UUID(), nullable=False),
        sa.Column("tranche_id", sa.UUID(), nullable=False),
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("configuration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("executor_id", sa.Text(), nullable=False),
        sa.Column("executor_incarnation", sa.UUID(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("subject_id", sa.UUID(), nullable=False),
        sa.Column("subject_incarnation", sa.UUID(), nullable=False),
        sa.Column("launch_rank", sa.BigInteger(), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("proposal_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("binding_digest", sa.Text(), nullable=False),
        sa.Column("binding_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("state", sa.Text(), server_default=sa.text("'proposed'"), nullable=False),
        sa.Column("accepted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=True),
        sa.Column("bootstrap_evidence_sha256", sa.Text(), nullable=True),
        sa.Column("launch_ready_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("permit_id", sa.UUID(), nullable=True),
        sa.Column("permit_epoch", sa.BigInteger(), nullable=True),
        sa.Column("permit_digest", sa.Text(), nullable=True),
        sa.Column("permit_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("permit_expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("permit_consumed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("inventory_sequence", sa.BigInteger(), nullable=True),
        sa.Column("observed_state", sa.Text(), nullable=True),
        sa.Column("terminal_kind", sa.Text(), nullable=True),
        sa.Column("terminal_identity", sa.Text(), nullable=True),
        sa.Column("terminal_evidence_sha256", sa.Text(), nullable=True),
        sa.Column("released_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND allocation_epoch > 0 AND configuration_epoch > 0 "
            "AND pool_generation > 0 AND launch_rank > 0",
            name="capacity_executable_intent_quantity_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND proposal_digest ~ '^[0-9a-f]{64}$' "
            "AND binding_digest ~ '^[0-9a-f]{64}$' "
            "AND (bootstrap_evidence_sha256 IS NULL OR "
            "bootstrap_evidence_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (permit_digest IS NULL OR permit_digest ~ '^[0-9a-f]{64}$') "
            "AND (terminal_evidence_sha256 IS NULL OR "
            "terminal_evidence_sha256 ~ '^[0-9a-f]{64}$')",
            name="capacity_executable_intent_digest_check",
        ),
        sa.CheckConstraint(
            "state IN ('proposed','accepted','launch-ready','permitted',"
            "'submitting-unknown','bound','observed','terminal','closing','released',"
            "'quarantined')",
            name="capacity_executable_intent_state_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(binding_payload) = 'object' "
            "AND jsonb_typeof(proposal_payload) = 'object'",
            name="capacity_executable_intent_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["execution_epoch", "execution_manifest_sha256"],
            [
                "public.capacity_execution_epochs.execution_epoch",
                "public.capacity_execution_epochs.execution_manifest_sha256",
            ],
            name="capacity_executable_intent_execution_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["allocation_epoch", "execution_epoch", "execution_manifest_sha256"],
            [
                "public.capacity_allocation_epochs.allocation_epoch",
                "public.capacity_allocation_epochs.execution_epoch",
                "public.capacity_allocation_epochs.execution_manifest_sha256",
            ],
            name="capacity_executable_intent_allocation_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "execution_epoch",
                "execution_manifest_sha256",
                "executor_id",
                "executor_incarnation",
                "pool_id",
                "pool_generation",
            ],
            [
                "public.capacity_execution_executors.execution_epoch",
                "public.capacity_execution_executors.execution_manifest_sha256",
                "public.capacity_execution_executors.executor_id",
                "public.capacity_execution_executors.executor_incarnation",
                "public.capacity_execution_executors.pool_id",
                "public.capacity_execution_executors.pool_generation",
            ],
            name="capacity_executable_intent_executor_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("intent_id", name="capacity_executable_intent_identity_key"),
        sa.UniqueConstraint("shape_instance_id", name="capacity_executable_shape_identity_key"),
        sa.UniqueConstraint("tranche_id", name="capacity_executable_tranche_identity_key"),
        sa.UniqueConstraint("permit_id", name="capacity_executable_permit_identity_key"),
        sa.UniqueConstraint(
            "execution_epoch",
            "allocation_epoch",
            "launch_rank",
            name="capacity_executable_launch_rank_key",
        ),
        schema="public",
    )
    op.create_table(
        "capacity_executable_protected_release_receipts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("intent_id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("reporter_incarnation", sa.UUID(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("protected_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("protected_release_sha256", sa.Text(), nullable=False),
        sa.Column("acknowledgement_digest", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=False),
        sa.Column("release_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND bootstrap_registration_epoch >= 0 "
            "AND protected_registration_epoch > bootstrap_registration_epoch",
            name="capacity_executable_protected_release_receipt_epoch_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND protected_release_sha256 ~ '^[0-9a-f]{64}$' "
            "AND acknowledgement_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_protected_release_receipt_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(release_payload) = 'object'",
            name="capacity_executable_protected_release_receipt_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["intent_id"],
            ["public.capacity_executable_intents.intent_id"],
            name="capacity_executable_protected_release_receipt_intent_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="capacity_executable_protected_release_receipt_idempotency_key",
        ),
        sa.UniqueConstraint(
            "intent_id",
            "protected_registration_epoch",
            name="capacity_executable_protected_release_receipt_epoch_key",
        ),
        schema="public",
    )
    op.create_table(
        "capacity_executable_command_receipts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("executor_incarnation", sa.UUID(), nullable=False),
        sa.Column("command_sequence", sa.BigInteger(), nullable=False),
        sa.Column("operation_kind", sa.Text(), nullable=False),
        sa.Column("request_digest", sa.Text(), nullable=False),
        sa.Column("result_digest", sa.Text(), nullable=False),
        sa.Column("result_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND command_sequence > 0",
            name="capacity_executable_command_receipt_quantity_check",
        ),
        sa.CheckConstraint(
            "request_digest ~ '^[0-9a-f]{64}$' AND result_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_command_receipt_digest_check",
        ),
        sa.ForeignKeyConstraint(
            ["execution_epoch", "executor_incarnation"],
            [
                "public.capacity_executable_executor_states.execution_epoch",
                "public.capacity_executable_executor_states.executor_incarnation",
            ],
            name="capacity_executable_command_receipt_executor_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "executor_incarnation",
            "command_sequence",
            name="capacity_executable_command_receipt_sequence_key",
        ),
        schema="public",
    )
    op.create_table(
        "capacity_executable_launch_rate_buckets",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("configuration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("scope_identity", sa.Text(), nullable=False),
        sa.Column("rate_per_minute", sa.BigInteger(), nullable=False),
        sa.Column("capacity_microtokens", sa.BigInteger(), nullable=False),
        sa.Column("available_microtokens", sa.BigInteger(), nullable=False),
        sa.Column("refill_remainder", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("last_refill_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint(
            "scope IN ('global','account','subject','pool')",
            name="capacity_executable_launch_rate_bucket_scope_check",
        ),
        sa.CheckConstraint(
            "configuration_epoch > 0 AND execution_epoch > 0 "
            "AND rate_per_minute BETWEEN 0 AND 9223372036854 "
            "AND capacity_microtokens = rate_per_minute * 1000000 "
            "AND available_microtokens >= 0 "
            "AND available_microtokens <= capacity_microtokens "
            "AND refill_remainder >= 0 AND refill_remainder < 60",
            name="capacity_executable_launch_rate_bucket_quantity_check",
        ),
        sa.ForeignKeyConstraint(
            ["execution_epoch"],
            ["public.capacity_execution_epochs.execution_epoch"],
            name="capacity_executable_launch_rate_bucket_execution_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["configuration_epoch"],
            ["public.capacity_configuration_epochs.configuration_epoch"],
            name="capacity_executable_launch_rate_bucket_configuration_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "execution_epoch",
            "scope",
            "scope_identity",
            name="capacity_executable_launch_rate_bucket_scope_key",
        ),
        schema="public",
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_intent_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          accepted_changed boolean;
          bootstrap_changed boolean;
          permit_changed boolean;
          consumption_changed boolean;
          inventory_changed boolean;
          release_changed boolean;
        BEGIN
          IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'executable intents are append-only'
              USING ERRCODE = '23514';
          END IF;
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'executable intent is append-only'
              USING ERRCODE = '23514';
          END IF;
          IF TG_OP = 'INSERT' THEN
            PERFORM 1
            FROM public.capacity_allocation_epochs AS parent
            WHERE parent.allocation_epoch = NEW.allocation_epoch
              AND parent.execution_epoch = NEW.execution_epoch
              AND parent.execution_manifest_sha256 = NEW.execution_manifest_sha256
              AND parent.status = 'executable'
              AND parent.executable = true
              AND parent.sealed = true;
            IF NOT FOUND THEN
              RAISE EXCEPTION 'executable intent requires sealed executable allocation parent'
                USING ERRCODE = '23514';
            END IF;
            IF NEW.state IS DISTINCT FROM 'proposed'
               OR NEW.accepted_at IS NOT NULL
               OR NEW.bootstrap_registration_epoch IS NOT NULL
               OR NEW.bootstrap_evidence_sha256 IS NOT NULL
               OR NEW.launch_ready_at IS NOT NULL
               OR NEW.permit_id IS NOT NULL
               OR NEW.permit_epoch IS NOT NULL
               OR NEW.permit_digest IS NOT NULL
               OR NEW.permit_payload IS NOT NULL
               OR NEW.permit_expires_at IS NOT NULL
               OR NEW.permit_consumed_at IS NOT NULL
               OR NEW.inventory_sequence IS NOT NULL
               OR NEW.observed_state IS NOT NULL
               OR NEW.terminal_kind IS NOT NULL
               OR NEW.terminal_identity IS NOT NULL
               OR NEW.terminal_evidence_sha256 IS NOT NULL
               OR NEW.released_at IS NOT NULL THEN
              RAISE EXCEPTION 'executable intent must start as a pristine proposal'
                USING ERRCODE = '23514';
            END IF;
            IF pg_catalog.jsonb_typeof(NEW.binding_payload) IS DISTINCT FROM 'object'
               OR (NEW.binding_payload ->> 'schema_version')::bigint IS DISTINCT FROM 2
               OR NEW.binding_payload ->> 'intent_id' IS DISTINCT FROM NEW.intent_id::text
               OR NEW.binding_payload ->> 'tranche_id' IS DISTINCT FROM NEW.tranche_id::text
               OR NEW.binding_payload ->> 'shape_instance_id'
                    IS DISTINCT FROM NEW.shape_instance_id
               OR NEW.binding_payload ->> 'subject_id'
                    IS DISTINCT FROM NEW.subject_id::text
               OR NEW.binding_payload ->> 'subject_incarnation'
                    IS DISTINCT FROM NEW.subject_incarnation::text
               OR NEW.binding_payload ->> 'pool_id' IS DISTINCT FROM NEW.pool_id
               OR (NEW.binding_payload ->> 'pool_generation')::bigint
                    IS DISTINCT FROM NEW.pool_generation
               OR NEW.binding_payload ->> 'executor_id' IS DISTINCT FROM NEW.executor_id
               OR NEW.binding_payload ->> 'executor_incarnation'
                    IS DISTINCT FROM NEW.executor_incarnation::text
               OR (NEW.binding_payload -> 'execution' ->> 'configuration_epoch')::bigint
                    IS DISTINCT FROM NEW.configuration_epoch
               OR (NEW.binding_payload -> 'execution' ->> 'allocation_epoch')::bigint
                    IS DISTINCT FROM NEW.allocation_epoch
               OR (NEW.binding_payload -> 'execution' ->> 'execution_epoch')::bigint
                    IS DISTINCT FROM NEW.execution_epoch
               OR NEW.binding_payload -> 'execution' ->> 'execution_manifest_sha256'
                    IS DISTINCT FROM NEW.execution_manifest_sha256 THEN
              RAISE EXCEPTION 'executable intent binding payload changed'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;

          IF ROW(
            NEW.id, NEW.intent_id, NEW.tranche_id, NEW.shape_instance_id,
            NEW.execution_epoch, NEW.execution_manifest_sha256,
            NEW.configuration_epoch, NEW.allocation_epoch,
            NEW.executor_id, NEW.executor_incarnation,
            NEW.pool_id, NEW.pool_generation,
            NEW.subject_id, NEW.subject_incarnation, NEW.launch_rank,
            NEW.proposal_digest, NEW.proposal_payload,
            NEW.binding_digest, NEW.binding_payload, NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.id, OLD.intent_id, OLD.tranche_id, OLD.shape_instance_id,
            OLD.execution_epoch, OLD.execution_manifest_sha256,
            OLD.configuration_epoch, OLD.allocation_epoch,
            OLD.executor_id, OLD.executor_incarnation,
            OLD.pool_id, OLD.pool_generation,
            OLD.subject_id, OLD.subject_incarnation, OLD.launch_rank,
            OLD.proposal_digest, OLD.proposal_payload,
            OLD.binding_digest, OLD.binding_payload, OLD.created_at
          ) THEN
            RAISE EXCEPTION 'executable intent identity, binding, and receipt fields are immutable'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.state IS DISTINCT FROM OLD.state AND NOT (
            (OLD.state = 'proposed' AND NEW.state IN ('accepted','released','quarantined')) OR
            (OLD.state = 'accepted' AND NEW.state IN ('launch-ready','closing','quarantined')) OR
            (OLD.state = 'launch-ready' AND NEW.state IN ('permitted','closing','quarantined')) OR
            (OLD.state = 'permitted' AND NEW.state IN ('submitting-unknown','closing','quarantined')) OR
            (OLD.state = 'submitting-unknown' AND NEW.state IN
              ('bound','observed','terminal','closing','quarantined')) OR
            (OLD.state = 'bound' AND NEW.state IN
              ('observed','terminal','closing','quarantined')) OR
            (OLD.state = 'observed' AND NEW.state IN ('terminal','closing','quarantined')) OR
            (OLD.state = 'terminal' AND NEW.state IN ('closing','quarantined')) OR
            (OLD.state = 'closing' AND NEW.state IN ('released','quarantined'))
          ) THEN
            RAISE EXCEPTION 'executable intent state transition is invalid'
              USING ERRCODE = '23514';
          END IF;
          accepted_changed := NEW.accepted_at IS DISTINCT FROM OLD.accepted_at;
          bootstrap_changed := ROW(
            NEW.bootstrap_registration_epoch,
            NEW.bootstrap_evidence_sha256,
            NEW.launch_ready_at
          ) IS DISTINCT FROM ROW(
            OLD.bootstrap_registration_epoch,
            OLD.bootstrap_evidence_sha256,
            OLD.launch_ready_at
          );
          permit_changed := ROW(
            NEW.permit_id,
            NEW.permit_epoch,
            NEW.permit_digest,
            NEW.permit_payload,
            NEW.permit_expires_at
          ) IS DISTINCT FROM ROW(
            OLD.permit_id,
            OLD.permit_epoch,
            OLD.permit_digest,
            OLD.permit_payload,
            OLD.permit_expires_at
          );
          consumption_changed :=
            NEW.permit_consumed_at IS DISTINCT FROM OLD.permit_consumed_at;
          inventory_changed := ROW(
            NEW.inventory_sequence,
            NEW.observed_state,
            NEW.terminal_kind,
            NEW.terminal_identity,
            NEW.terminal_evidence_sha256
          ) IS DISTINCT FROM ROW(
            OLD.inventory_sequence,
            OLD.observed_state,
            OLD.terminal_kind,
            OLD.terminal_identity,
            OLD.terminal_evidence_sha256
          );
          release_changed := NEW.released_at IS DISTINCT FROM OLD.released_at;

          IF (NEW.bootstrap_registration_epoch IS NULL)
                <> (NEW.bootstrap_evidence_sha256 IS NULL)
             OR (NEW.bootstrap_registration_epoch IS NULL)
                <> (NEW.launch_ready_at IS NULL) THEN
            RAISE EXCEPTION 'executable intent bootstrap tuple is incomplete'
              USING ERRCODE = '23514';
          END IF;
          IF (NEW.permit_id IS NULL) <> (NEW.permit_epoch IS NULL)
             OR (NEW.permit_id IS NULL) <> (NEW.permit_digest IS NULL)
             OR (NEW.permit_id IS NULL) <> (NEW.permit_payload IS NULL)
             OR (NEW.permit_id IS NULL) <> (NEW.permit_expires_at IS NULL) THEN
            RAISE EXCEPTION 'executable intent permit tuple is incomplete'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.permit_payload IS NOT NULL AND (
               pg_catalog.jsonb_typeof(NEW.permit_payload) IS DISTINCT FROM 'object'
               OR (NEW.permit_payload ->> 'schema_version')::bigint IS DISTINCT FROM 2
               OR NEW.permit_payload ->> 'permit_id' IS DISTINCT FROM NEW.permit_id::text
               OR NEW.permit_payload -> 'binding' IS DISTINCT FROM NEW.binding_payload
               OR (NEW.permit_payload ->> 'permit_epoch')::bigint
                    IS DISTINCT FROM NEW.permit_epoch
               OR (NEW.permit_payload ->> 'launch_rank')::bigint
                    IS DISTINCT FROM NEW.launch_rank
               OR (NEW.permit_payload ->> 'expires_at')::timestamptz
                    IS DISTINCT FROM NEW.permit_expires_at
               OR NEW.permit_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb
          ) THEN
            RAISE EXCEPTION 'executable intent permit payload changed'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.inventory_sequence IS NULL AND (
               NEW.observed_state IS NOT NULL
               OR NEW.terminal_kind IS NOT NULL
               OR NEW.terminal_identity IS NOT NULL
               OR NEW.terminal_evidence_sha256 IS NOT NULL
             )
             OR NEW.inventory_sequence IS NOT NULL AND (
               NEW.terminal_kind IS NULL OR NEW.terminal_identity IS NULL
             )
             OR NEW.terminal_kind IS NOT NULL
                AND NEW.terminal_kind NOT IN ('unused','slurm-job','worker')
             OR NEW.observed_state IS NOT NULL
                AND NEW.observed_state NOT IN ('pending','active','draining','terminal','unknown')
          THEN
            RAISE EXCEPTION 'executable intent inventory tuple is incomplete'
              USING ERRCODE = '23514';
          END IF;
          IF inventory_changed
             AND OLD.inventory_sequence IS NOT NULL
             AND (
               NEW.inventory_sequence IS NULL
               OR NEW.inventory_sequence <= OLD.inventory_sequence
             ) THEN
            RAISE EXCEPTION 'executable intent inventory high-water did not advance'
              USING ERRCODE = '23514';
          END IF;
          IF (NEW.state = 'released') IS DISTINCT FROM (NEW.released_at IS NOT NULL) THEN
            RAISE EXCEPTION 'executable intent release evidence does not match state'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.state = 'terminal' AND (
               NEW.observed_state IS DISTINCT FROM 'terminal'
               OR NEW.terminal_evidence_sha256 IS NULL
             ) THEN
            RAISE EXCEPTION 'executable intent terminal evidence does not match state'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.terminal_kind = 'unused' AND (
               NEW.terminal_identity IS DISTINCT FROM NEW.shape_instance_id
               OR NEW.terminal_evidence_sha256 IS NULL
             ) THEN
            RAISE EXCEPTION 'executable intent unused evidence changed'
              USING ERRCODE = '23514';
          END IF;

          IF NEW.state = 'quarantined' AND OLD.state <> 'quarantined' THEN
            IF accepted_changed OR bootstrap_changed OR permit_changed
               OR consumption_changed OR inventory_changed OR release_changed THEN
              RAISE EXCEPTION 'executable intent quarantine carried unrelated evidence'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;

          IF NEW.state = OLD.state THEN
            IF NOT accepted_changed AND NOT bootstrap_changed AND NOT permit_changed
               AND NOT consumption_changed AND NOT inventory_changed
               AND NOT release_changed THEN
              RETURN NEW;
            END IF;
            IF OLD.state = 'permitted'
               AND permit_changed
               AND NOT accepted_changed AND NOT bootstrap_changed
               AND NOT consumption_changed AND NOT inventory_changed
               AND NOT release_changed
               AND NEW.permit_epoch = OLD.permit_epoch + 1 THEN
              RETURN NEW;
            END IF;
            IF OLD.state = 'observed'
               AND inventory_changed
               AND NOT accepted_changed AND NOT bootstrap_changed
               AND NOT permit_changed AND NOT consumption_changed
               AND NOT release_changed
               AND NEW.terminal_kind IS NOT DISTINCT FROM OLD.terminal_kind
               AND NEW.terminal_identity IS NOT DISTINCT FROM OLD.terminal_identity
               AND NEW.terminal_evidence_sha256 IS NULL THEN
              RETURN NEW;
            END IF;
            RAISE EXCEPTION 'executable intent same-state evidence mutation is invalid'
              USING ERRCODE = '23514';
          END IF;

          IF OLD.state = 'proposed' AND NEW.state = 'accepted'
             AND accepted_changed AND NEW.accepted_at IS NOT NULL
             AND NOT bootstrap_changed AND NOT permit_changed
             AND NOT consumption_changed AND NOT inventory_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'proposed' AND NEW.state = 'released'
             AND release_changed AND NEW.released_at IS NOT NULL
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT consumption_changed
             AND NOT inventory_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'accepted' AND NEW.state = 'launch-ready'
             AND bootstrap_changed
             AND NEW.bootstrap_registration_epoch IS NOT NULL
             AND NOT accepted_changed AND NOT permit_changed
             AND NOT consumption_changed AND NOT inventory_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'launch-ready' AND NEW.state = 'permitted'
             AND permit_changed AND NEW.permit_epoch = 1
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT consumption_changed AND NOT inventory_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'permitted' AND NEW.state = 'submitting-unknown'
             AND consumption_changed AND NEW.permit_consumed_at IS NOT NULL
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT inventory_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state IN ('accepted','launch-ready','permitted')
             AND NEW.state = 'closing'
             AND inventory_changed
             AND NEW.inventory_sequence IS NOT NULL
             AND NEW.observed_state IS NULL
             AND NEW.terminal_kind = 'unused'
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT consumption_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'submitting-unknown' AND NEW.state = 'closing'
             AND inventory_changed
             AND NEW.inventory_sequence IS NOT NULL
             AND NEW.observed_state = 'terminal'
             AND NEW.terminal_kind = 'unused'
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT consumption_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'submitting-unknown' AND NEW.state IN ('bound','observed')
             AND inventory_changed
             AND NEW.observed_state IN ('pending','active','draining','unknown')
             AND NEW.terminal_kind IN ('slurm-job','worker')
             AND NEW.terminal_evidence_sha256 IS NULL
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT consumption_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state IN ('submitting-unknown','bound','observed')
             AND NEW.state = 'terminal'
             AND inventory_changed
             AND NEW.observed_state = 'terminal'
             AND NEW.terminal_kind IN ('slurm-job','worker')
             AND NEW.terminal_evidence_sha256 IS NOT NULL
             AND (OLD.terminal_kind IS NULL
                  OR NEW.terminal_kind IS NOT DISTINCT FROM OLD.terminal_kind)
             AND (OLD.terminal_identity IS NULL
                  OR NEW.terminal_identity IS NOT DISTINCT FROM OLD.terminal_identity)
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT consumption_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'bound' AND NEW.state = 'observed'
             AND inventory_changed
             AND NEW.observed_state IN ('pending','active','draining','unknown')
             AND NEW.terminal_kind IS NOT DISTINCT FROM OLD.terminal_kind
             AND NEW.terminal_identity IS NOT DISTINCT FROM OLD.terminal_identity
             AND NEW.terminal_evidence_sha256 IS NULL
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT consumption_changed
             AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state IN ('bound','observed','terminal') AND NEW.state = 'closing'
             AND NOT accepted_changed AND NOT bootstrap_changed
             AND NOT permit_changed AND NOT consumption_changed
             AND NOT inventory_changed AND NOT release_changed THEN
            RETURN NEW;
          END IF;
          IF OLD.state = 'closing' AND NEW.state = 'released' THEN
            IF NEW.inventory_sequence IS NULL
               OR NEW.terminal_kind NOT IN ('unused','slurm-job','worker')
               OR NEW.terminal_identity IS NULL
               OR NEW.terminal_evidence_sha256 IS NULL
               OR NOT EXISTS (
                 SELECT 1
                 FROM public.capacity_executable_protected_release_receipts AS receipt
                 WHERE receipt.intent_id = NEW.intent_id
                   AND receipt.execution_epoch = NEW.execution_epoch
                   AND receipt.execution_manifest_sha256 = NEW.execution_manifest_sha256
                   AND receipt.bootstrap_registration_epoch
                        = NEW.bootstrap_registration_epoch
                   AND receipt.release_payload -> 'binding' = NEW.binding_payload
               ) THEN
              RAISE EXCEPTION
                'executable intent release requires exact protected and physical terminal evidence'
                USING ERRCODE = '23514';
            END IF;
            IF release_changed AND NEW.released_at IS NOT NULL
               AND NOT accepted_changed AND NOT bootstrap_changed
               AND NOT permit_changed AND NOT consumption_changed
               AND NOT inventory_changed THEN
              RETURN NEW;
            END IF;
          END IF;
          RAISE EXCEPTION 'executable intent evidence transition is invalid'
            USING ERRCODE = '23514';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_intent_mutation_guard
        BEFORE INSERT OR UPDATE OR DELETE ON public.capacity_executable_intents
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_intent_guard()
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_intent_truncate_guard
        BEFORE TRUNCATE ON public.capacity_executable_intents
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.capacity_executable_intent_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_executor_state_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'executable executor states are append-only'
              USING ERRCODE = '23514';
          END IF;
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'executable executor state is append-only'
              USING ERRCODE = '23514';
          END IF;
          IF ROW(
            NEW.id, NEW.execution_epoch, NEW.execution_manifest_sha256,
            NEW.executor_id, NEW.executor_incarnation,
            NEW.pool_id, NEW.pool_generation, NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.id, OLD.execution_epoch, OLD.execution_manifest_sha256,
            OLD.executor_id, OLD.executor_incarnation,
            OLD.pool_id, OLD.pool_generation, OLD.created_at
          ) THEN
            RAISE EXCEPTION 'executable executor identity is immutable'
              USING ERRCODE = '23514';
          END IF;
          IF OLD.state IN ('fenced','equivocal') AND NEW.state IS DISTINCT FROM OLD.state THEN
            RAISE EXCEPTION 'executable executor cannot be unfenced'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.heartbeat_high_water < OLD.heartbeat_high_water
             OR NEW.command_high_water < OLD.command_high_water
             OR NEW.journal_high_water < OLD.journal_high_water
             OR NEW.inventory_high_water < OLD.inventory_high_water THEN
            RAISE EXCEPTION 'executable executor high-water regressed'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.heartbeat_high_water = OLD.heartbeat_high_water AND ROW(
               NEW.last_heartbeat_digest, NEW.last_heartbeat_at, NEW.lease_expires_at
             ) IS DISTINCT FROM ROW(
               OLD.last_heartbeat_digest, OLD.last_heartbeat_at, OLD.lease_expires_at
             )
             OR NEW.command_high_water = OLD.command_high_water
                AND NEW.last_command_digest IS DISTINCT FROM OLD.last_command_digest
             OR NEW.journal_high_water = OLD.journal_high_water
                AND NEW.journal_digest IS DISTINCT FROM OLD.journal_digest
             OR NEW.inventory_high_water = OLD.inventory_high_water AND ROW(
               NEW.last_inventory_digest, NEW.inventory_payload, NEW.last_inventory_at
             ) IS DISTINCT FROM ROW(
               OLD.last_inventory_digest, OLD.inventory_payload, OLD.last_inventory_at
             )
          THEN
            RAISE EXCEPTION 'executable executor high-water evidence changed in place'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_executor_state_mutation_guard
        BEFORE UPDATE OR DELETE ON public.capacity_executable_executor_states
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_executor_state_guard()
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_executor_state_truncate_guard
        BEFORE TRUNCATE ON public.capacity_executable_executor_states
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.capacity_executable_executor_state_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_receipt_append_only_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          RAISE EXCEPTION 'executable receipt is append-only'
            USING ERRCODE = '23514';
        END;
        $$
        """
    )
    for table_name in (
        "capacity_executable_command_receipts",
        "capacity_executable_protected_release_receipts",
    ):
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
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_protected_release_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          intent_binding jsonb;
          intent_bootstrap_registration_epoch bigint;
          intent_subject_id uuid;
          intent_subject_incarnation uuid;
          expected_release_payload jsonb;
        BEGIN
          SELECT
            intent.binding_payload,
            intent.bootstrap_registration_epoch,
            intent.subject_id,
            intent.subject_incarnation
          INTO
            intent_binding,
            intent_bootstrap_registration_epoch,
            intent_subject_id,
            intent_subject_incarnation
          FROM public.capacity_executable_intents AS intent
          WHERE intent.intent_id = NEW.intent_id
            AND intent.execution_epoch = NEW.execution_epoch
            AND intent.execution_manifest_sha256 = NEW.execution_manifest_sha256
          FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected release receipt intent binding changed'
              USING ERRCODE = '23514';
          END IF;
          IF intent_bootstrap_registration_epoch IS NULL
             OR NEW.bootstrap_registration_epoch
                IS DISTINCT FROM intent_bootstrap_registration_epoch THEN
            RAISE EXCEPTION 'protected release receipt bootstrap binding changed'
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
            RAISE EXCEPTION 'protected release receipt reporter binding changed'
              USING ERRCODE = '23514';
          END IF;
          expected_release_payload := pg_catalog.jsonb_build_object(
            'schema_version', 2,
            'binding', intent_binding,
            'reporter_incarnation', NEW.reporter_incarnation::text,
            'bootstrap_registration_epoch', NEW.bootstrap_registration_epoch,
            'protected_registration_epoch', NEW.protected_registration_epoch,
            'bootstrap_revoked', true,
            'protected_release_sha256', NEW.protected_release_sha256,
            'executable', true
          );
          IF pg_catalog.jsonb_typeof(NEW.release_payload) IS DISTINCT FROM 'object'
             OR pg_catalog.jsonb_typeof(NEW.release_payload -> 'binding')
                IS DISTINCT FROM 'object'
             OR NEW.release_payload IS DISTINCT FROM expected_release_payload THEN
            RAISE EXCEPTION 'protected release receipt payload binding changed'
              USING ERRCODE = '23514';
          END IF;
          IF EXISTS (
            SELECT 1
            FROM public.capacity_executable_protected_release_receipts AS prior
            WHERE prior.intent_id = NEW.intent_id
              AND prior.protected_registration_epoch >= NEW.protected_registration_epoch
          ) THEN
            RAISE EXCEPTION 'protected release receipt epoch must advance'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_protected_release_insert_guard
        BEFORE INSERT ON public.capacity_executable_protected_release_receipts
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_protected_release_insert_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_launch_rate_bucket_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          elapsed_microseconds numeric;
          refill_numerator numeric;
          refilled_microtokens numeric;
          expected_remainder bigint;
        BEGIN
          IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'executable launch rate buckets are append-only'
              USING ERRCODE = '23514';
          END IF;
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'executable launch rate bucket is append-only'
              USING ERRCODE = '23514';
          END IF;
          IF ROW(
            NEW.id, NEW.execution_epoch, NEW.configuration_epoch,
            NEW.scope, NEW.scope_identity,
            NEW.rate_per_minute, NEW.capacity_microtokens
          ) IS DISTINCT FROM ROW(
            OLD.id, OLD.execution_epoch, OLD.configuration_epoch,
            OLD.scope, OLD.scope_identity,
            OLD.rate_per_minute, OLD.capacity_microtokens
          ) THEN
            RAISE EXCEPTION 'executable launch rate bucket identity is immutable'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.last_refill_at < OLD.last_refill_at THEN
            RAISE EXCEPTION 'executable launch rate refill time regressed'
              USING ERRCODE = '23514';
          END IF;
          elapsed_microseconds :=
            extract(epoch FROM (NEW.last_refill_at - OLD.last_refill_at)) * 1000000;
          refill_numerator :=
            elapsed_microseconds * OLD.rate_per_minute + OLD.refill_remainder;
          refilled_microtokens := least(
            OLD.capacity_microtokens::numeric,
            OLD.available_microtokens::numeric + floor(refill_numerator / 60)
          );
          expected_remainder := mod(refill_numerator, 60)::bigint;
          IF refilled_microtokens = OLD.capacity_microtokens THEN
            expected_remainder := 0;
          END IF;
          IF NEW.available_microtokens::numeric <> refilled_microtokens
             AND NEW.available_microtokens::numeric
                   <> refilled_microtokens - 1000000
             OR NEW.refill_remainder <> expected_remainder THEN
            RAISE EXCEPTION 'executable launch rate bucket transition is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_launch_rate_bucket_mutation_guard
        BEFORE UPDATE OR DELETE ON public.capacity_executable_launch_rate_buckets
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_launch_rate_bucket_guard()
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_launch_rate_bucket_truncate_guard
        BEFORE TRUNCATE ON public.capacity_executable_launch_rate_buckets
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.capacity_executable_launch_rate_bucket_guard()
        """
    )


def downgrade() -> None:
    connection = op.get_bind()
    if connection.get_isolation_level().upper() != "READ COMMITTED":
        raise RuntimeError("capacity_0006 downgrade requires READ COMMITTED")
    connection.execute(
        sa.text(
            "LOCK TABLE public.capacity_executable_executor_states, "
            "public.capacity_executable_intents, "
            "public.capacity_executable_protected_release_receipts, "
            "public.capacity_executable_command_receipts, "
            "public.capacity_executable_launch_rate_buckets "
            "IN ACCESS EXCLUSIVE MODE"
        )
    )
    if connection.execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_executable_executor_states UNION ALL "
            "SELECT 1 FROM public.capacity_executable_intents UNION ALL "
            "SELECT 1 FROM public.capacity_executable_protected_release_receipts UNION ALL "
            "SELECT 1 FROM public.capacity_executable_command_receipts UNION ALL "
            "SELECT 1 FROM public.capacity_executable_launch_rate_buckets"
            ")"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade capacity_0006 with executable queue history"
        )
    op.execute(
        "DROP TRIGGER capacity_executable_launch_rate_bucket_truncate_guard "
        "ON public.capacity_executable_launch_rate_buckets"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_launch_rate_bucket_mutation_guard "
        "ON public.capacity_executable_launch_rate_buckets"
    )
    op.execute("DROP FUNCTION public.capacity_executable_launch_rate_bucket_guard()")
    op.execute(
        "DROP TRIGGER capacity_executable_protected_release_insert_guard "
        "ON public.capacity_executable_protected_release_receipts"
    )
    op.execute("DROP FUNCTION public.capacity_executable_protected_release_insert_guard()")
    for table_name in (
        "capacity_executable_protected_release_receipts",
        "capacity_executable_command_receipts",
    ):
        op.execute(
            f"DROP TRIGGER {table_name}_truncate_guard ON public.{table_name}"
        )
        op.execute(
            f"DROP TRIGGER {table_name}_append_only_guard ON public.{table_name}"
        )
    op.execute("DROP FUNCTION public.capacity_executable_receipt_append_only_guard()")
    op.execute(
        "DROP TRIGGER capacity_executable_executor_state_truncate_guard "
        "ON public.capacity_executable_executor_states"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_executor_state_mutation_guard "
        "ON public.capacity_executable_executor_states"
    )
    op.execute("DROP FUNCTION public.capacity_executable_executor_state_guard()")
    op.execute(
        "DROP TRIGGER capacity_executable_intent_truncate_guard "
        "ON public.capacity_executable_intents"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_intent_mutation_guard "
        "ON public.capacity_executable_intents"
    )
    op.execute("DROP FUNCTION public.capacity_executable_intent_guard()")
    op.drop_table("capacity_executable_launch_rate_buckets", schema="public")
    op.drop_table("capacity_executable_command_receipts", schema="public")
    op.drop_table("capacity_executable_protected_release_receipts", schema="public")
    op.drop_table("capacity_executable_intents", schema="public")
    op.drop_table("capacity_executable_executor_states", schema="public")
    op.drop_constraint(
        "capacity_execution_executor_exact_binding_key",
        "capacity_execution_executors",
        type_="unique",
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocation_epoch_mode_check",
        "capacity_allocation_epochs",
        type_="check",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_allocation_epoch_mode_check",
        "capacity_allocation_epochs",
        "(status IN ('shadow','failed') AND executable = false "
        "AND execution_epoch IS NULL AND execution_manifest_sha256 IS NULL "
        "AND sealed = true AND allocation_count IS NULL) OR "
        "(status = 'executable' AND executable = true "
        "AND execution_epoch IS NOT NULL AND execution_manifest_sha256 IS NOT NULL "
        "AND allocation_count IS NOT NULL AND allocation_count >= 0 "
        "AND COALESCE(jsonb_typeof(complete_payload -> 'allocations') = 'array', false) "
        "AND COALESCE(jsonb_array_length(complete_payload -> 'allocations') "
        "= allocation_count, false))",
        schema="public",
    )
    op.drop_column("capacity_allocation_epochs", "input_valid_until", schema="public")
