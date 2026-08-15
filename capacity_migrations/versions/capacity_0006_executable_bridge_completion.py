"""complete executable bridge lifecycle on official allocation lineage

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
    op.add_column("capacity_execution_epochs", sa.Column("drain_actor", sa.Text(), nullable=True))
    op.add_column(
        "capacity_execution_epochs", sa.Column("drain_idempotency_key", sa.UUID(), nullable=True)
    )
    op.add_column(
        "capacity_execution_epochs", sa.Column("drain_request_digest", sa.Text(), nullable=True)
    )
    op.add_column(
        "capacity_execution_epochs",
        sa.Column("drain_request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "capacity_execution_epochs", sa.Column("retirement_actor", sa.Text(), nullable=True)
    )
    op.add_column(
        "capacity_execution_epochs",
        sa.Column("retirement_idempotency_key", sa.UUID(), nullable=True),
    )
    op.add_column(
        "capacity_execution_epochs",
        sa.Column("retirement_request_digest", sa.Text(), nullable=True),
    )
    op.add_column(
        "capacity_execution_epochs",
        sa.Column(
            "retirement_request_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.drop_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_digest_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_state_time_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        "execution_epoch > 0 AND prepared_writer_epoch > 0 "
        "AND current_writer_epoch > 0 "
        "AND configuration_epoch > 0 AND fleet_generation > 0 "
        "AND oldlab_pool_generation > 0 AND gb10_pool_generation > 0 "
        "AND requested_ceiling > 0 AND effective_ceiling >= 0 "
        "AND effective_ceiling <= requested_ceiling "
        "AND requested_rate_per_minute > 0 AND effective_rate_per_minute >= 0 "
        "AND effective_rate_per_minute <= requested_rate_per_minute",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_digest_check",
        "capacity_execution_epochs",
        "fleet_digest ~ '^[0-9a-f]{64}$' "
        "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND trusted_fleet_release_sha256 ~ '^[0-9a-f]{64}$' "
        "AND oldlab_signing_key_sha256 ~ '^[0-9a-f]{64}$' "
        "AND oldlab_local_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND oldlab_controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND gb10_signing_key_sha256 ~ '^[0-9a-f]{64}$' "
        "AND gb10_local_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND gb10_controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND environment_acknowledgements_sha256 ~ '^[0-9a-f]{64}$' "
        "AND legacy_writer_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND rollback_evidence_sha256 ~ '^[0-9a-f]{64}$' "
        "AND request_digest ~ '^[0-9a-f]{64}$' "
        "AND (activation_request_digest IS NULL OR "
        "activation_request_digest ~ '^[0-9a-f]{64}$') "
        "AND (drain_request_digest IS NULL OR "
        "drain_request_digest ~ '^[0-9a-f]{64}$') "
        "AND (retirement_request_digest IS NULL OR "
        "retirement_request_digest ~ '^[0-9a-f]{64}$')",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_lifecycle_actor_check",
        "capacity_execution_epochs",
        "(drain_actor IS NULL OR octet_length(drain_actor) BETWEEN 1 AND 256) "
        "AND (retirement_actor IS NULL OR "
        "octet_length(retirement_actor) BETWEEN 1 AND 256)",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_lifecycle_payload_check",
        "capacity_execution_epochs",
        "(drain_request_payload IS NULL OR "
        "(jsonb_typeof(drain_request_payload) = 'object' "
        "AND octet_length(drain_request_payload::text) <= 8388608)) "
        "AND (retirement_request_payload IS NULL OR "
        "(jsonb_typeof(retirement_request_payload) = 'object' "
        "AND octet_length(retirement_request_payload::text) <= 8388608))",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_state_time_check",
        "capacity_execution_epochs",
        "(state = 'prepared' AND effective_ceiling = 0 "
        "AND effective_rate_per_minute = 0 "
        "AND activation_actor IS NULL AND activation_idempotency_key IS NULL "
        "AND activation_request_digest IS NULL AND activated_at IS NULL "
        "AND drain_actor IS NULL AND drain_idempotency_key IS NULL "
        "AND drain_request_digest IS NULL AND drain_request_payload IS NULL "
        "AND retirement_actor IS NULL AND retirement_idempotency_key IS NULL "
        "AND retirement_request_digest IS NULL "
        "AND retirement_request_payload IS NULL "
        "AND drain_only_at IS NULL AND retired_at IS NULL) OR "
        "(state = 'active' AND effective_ceiling > 0 "
        "AND effective_rate_per_minute > 0 "
        "AND activation_actor IS NOT NULL AND activation_idempotency_key IS NOT NULL "
        "AND activation_request_digest IS NOT NULL AND activated_at IS NOT NULL "
        "AND drain_actor IS NULL AND drain_idempotency_key IS NULL "
        "AND drain_request_digest IS NULL AND drain_request_payload IS NULL "
        "AND retirement_actor IS NULL AND retirement_idempotency_key IS NULL "
        "AND retirement_request_digest IS NULL "
        "AND retirement_request_payload IS NULL "
        "AND drain_only_at IS NULL AND retired_at IS NULL) OR "
        "(state = 'drain-only' AND effective_ceiling = 0 "
        "AND effective_rate_per_minute = 0 "
        "AND activation_actor IS NOT NULL AND activation_idempotency_key IS NOT NULL "
        "AND activation_request_digest IS NOT NULL AND activated_at IS NOT NULL "
        "AND drain_actor IS NOT NULL AND drain_idempotency_key IS NOT NULL "
        "AND drain_request_digest IS NOT NULL AND drain_request_payload IS NOT NULL "
        "AND retirement_actor IS NULL AND retirement_idempotency_key IS NULL "
        "AND retirement_request_digest IS NULL "
        "AND retirement_request_payload IS NULL "
        "AND drain_only_at IS NOT NULL "
        "AND retired_at IS NULL) OR "
        "(state = 'retired' AND effective_ceiling = 0 "
        "AND effective_rate_per_minute = 0 "
        "AND retirement_actor IS NOT NULL "
        "AND retirement_idempotency_key IS NOT NULL "
        "AND retirement_request_digest IS NOT NULL "
        "AND retirement_request_payload IS NOT NULL "
        "AND retired_at IS NOT NULL AND ((activation_actor IS NULL "
        "AND activation_idempotency_key IS NULL "
        "AND activation_request_digest IS NULL AND activated_at IS NULL "
        "AND drain_actor IS NULL AND drain_idempotency_key IS NULL "
        "AND drain_request_digest IS NULL AND drain_request_payload IS NULL "
        "AND drain_only_at IS NULL) OR (activation_actor IS NOT NULL "
        "AND activation_idempotency_key IS NOT NULL "
        "AND activation_request_digest IS NOT NULL AND activated_at IS NOT NULL "
        "AND drain_actor IS NOT NULL AND drain_idempotency_key IS NOT NULL "
        "AND drain_request_digest IS NOT NULL AND drain_request_payload IS NOT NULL "
        "AND drain_only_at IS NOT NULL)))",
    )
    op.create_unique_constraint(
        "capacity_execution_epoch_drain_idempotency_key",
        "capacity_execution_epochs",
        ["drain_idempotency_key"],
    )
    op.create_unique_constraint(
        "capacity_execution_epoch_retirement_idempotency_key",
        "capacity_execution_epochs",
        ["retirement_idempotency_key"],
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
        sa.Column(
            "retirement_safe",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("retirement_inventory_digest", sa.Text(), nullable=True),
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
        sa.CheckConstraint(
            "(retirement_safe AND retirement_inventory_digest IS NOT NULL "
            "AND retirement_inventory_digest ~ '^[0-9a-f]{64}$' "
            "AND retirement_inventory_digest = last_inventory_digest "
            "AND inventory_high_water > 0 AND inventory_payload IS NOT NULL "
            "AND jsonb_typeof(inventory_payload) = 'object' "
            "AND last_inventory_at IS NOT NULL "
            "AND inventory_payload -> 'schema_version' = '2'::jsonb "
            "AND inventory_payload -> 'inventory_sequence' "
            "= to_jsonb(inventory_high_water) "
            "AND inventory_payload ->> 'executor_id' = executor_id "
            "AND inventory_payload ->> 'executor_incarnation' "
            "= executor_incarnation::text "
            "AND inventory_payload ->> 'pool_id' = pool_id "
            "AND inventory_payload -> 'pool_generation' = to_jsonb(pool_generation) "
            "AND inventory_payload -> 'journal_sequence' = to_jsonb(journal_high_water) "
            "AND inventory_payload ->> 'journal_digest' = journal_digest "
            "AND inventory_payload -> 'execution' -> 'execution_epoch' "
            "= to_jsonb(execution_epoch) "
            "AND inventory_payload -> 'execution' ->> 'execution_manifest_sha256' "
            "= execution_manifest_sha256) OR "
            "(NOT retirement_safe AND retirement_inventory_digest IS NULL)",
            name="capacity_executable_executor_retirement_check",
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
                "capacity_execution_executors.execution_epoch",
                "capacity_execution_executors.execution_manifest_sha256",
                "capacity_execution_executors.executor_id",
                "capacity_execution_executors.executor_incarnation",
                "capacity_execution_executors.pool_id",
                "capacity_execution_executors.pool_generation",
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
        sa.Column("protected_registration_epoch", sa.BigInteger(), nullable=True),
        sa.Column("protected_release_idempotency_key", sa.UUID(), nullable=True),
        sa.Column("protected_release_digest", sa.Text(), nullable=True),
        sa.Column("protected_release_actor", sa.Text(), nullable=True),
        sa.Column("protected_release_sha256", sa.Text(), nullable=True),
        sa.Column(
            "protected_release_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
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
            "AND (protected_release_digest IS NULL OR "
            "protected_release_digest ~ '^[0-9a-f]{64}$') "
            "AND (protected_release_sha256 IS NULL OR "
            "protected_release_sha256 ~ '^[0-9a-f]{64}$') "
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
                "capacity_execution_epochs.execution_epoch",
                "capacity_execution_epochs.execution_manifest_sha256",
            ],
            name="capacity_executable_intent_execution_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["allocation_epoch", "execution_epoch", "execution_manifest_sha256"],
            [
                "capacity_allocation_epochs.allocation_epoch",
                "capacity_allocation_epochs.execution_epoch",
                "capacity_allocation_epochs.execution_manifest_sha256",
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
                "capacity_execution_executors.execution_epoch",
                "capacity_execution_executors.execution_manifest_sha256",
                "capacity_execution_executors.executor_id",
                "capacity_execution_executors.executor_incarnation",
                "capacity_execution_executors.pool_id",
                "capacity_execution_executors.pool_generation",
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
            "protected_release_idempotency_key",
            name="capacity_executable_protected_release_idempotency_key",
        ),
        sa.UniqueConstraint(
            "execution_epoch",
            "allocation_epoch",
            "launch_rank",
            name="capacity_executable_launch_rank_key",
        ),
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
                "capacity_executable_executor_states.execution_epoch",
                "capacity_executable_executor_states.executor_incarnation",
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
            "AND refill_remainder >= 0 AND refill_remainder < 60000000",
            name="capacity_executable_launch_rate_bucket_quantity_check",
        ),
        sa.ForeignKeyConstraint(
            ["execution_epoch"],
            ["capacity_execution_epochs.execution_epoch"],
            name="capacity_executable_launch_rate_bucket_execution_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["configuration_epoch"],
            ["capacity_configuration_epochs.configuration_epoch"],
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
    )
    op.execute(
        """
CREATE OR REPLACE FUNCTION capacity_execution_epoch_transition_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          IF TG_OP = 'INSERT' THEN
            IF NEW.state <> 'prepared'
               OR NEW.effective_ceiling <> 0
               OR NEW.effective_rate_per_minute <> 0
               OR NEW.activation_actor IS NOT NULL
               OR NEW.activation_idempotency_key IS NOT NULL
               OR NEW.activation_request_digest IS NOT NULL
               OR NEW.activated_at IS NOT NULL
               OR NEW.drain_actor IS NOT NULL
               OR NEW.drain_idempotency_key IS NOT NULL
               OR NEW.drain_request_digest IS NOT NULL
               OR NEW.drain_request_payload IS NOT NULL
               OR NEW.drain_only_at IS NOT NULL
               OR NEW.retirement_actor IS NOT NULL
               OR NEW.retirement_idempotency_key IS NOT NULL
               OR NEW.retirement_request_digest IS NOT NULL
               OR NEW.retirement_request_payload IS NOT NULL
               OR NEW.retired_at IS NOT NULL THEN
              RAISE EXCEPTION 'capacity execution epoch must be inserted prepared'
                USING ERRCODE = '23514';
            END IF;
            IF NEW.current_writer_epoch <> NEW.prepared_writer_epoch THEN
              RAISE EXCEPTION 'capacity execution epoch initial writer evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;

          IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'capacity execution epochs are append-only'
              USING ERRCODE = '23514';
          END IF;

          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'capacity execution epochs are append-only'
              USING ERRCODE = '23514';
          END IF;

          IF ROW(
            NEW.execution_epoch,
            NEW.authority_incarnation,
            NEW.prepared_writer_epoch,
            NEW.configuration_epoch,
            NEW.fleet_generation,
            NEW.fleet_digest,
            NEW.execution_manifest_sha256,
            NEW.manifest_payload,
            NEW.trusted_fleet_release_sha256,
            NEW.oldlab_executor_id,
            NEW.oldlab_executor_incarnation,
            NEW.oldlab_pool_id,
            NEW.oldlab_pool_generation,
            NEW.oldlab_signing_key_sha256,
            NEW.oldlab_local_authority_sha256,
            NEW.oldlab_controller_authority_sha256,
            NEW.gb10_executor_id,
            NEW.gb10_executor_incarnation,
            NEW.gb10_pool_id,
            NEW.gb10_pool_generation,
            NEW.gb10_signing_key_sha256,
            NEW.gb10_local_authority_sha256,
            NEW.gb10_controller_authority_sha256,
            NEW.environment_acknowledgements_sha256,
            NEW.legacy_writer_manifest_sha256,
            NEW.rollback_evidence_sha256,
            NEW.requested_ceiling,
            NEW.requested_rate_per_minute,
            NEW.actor,
            NEW.idempotency_key,
            NEW.request_digest,
            NEW.prepared_at
          ) IS DISTINCT FROM ROW(
            OLD.execution_epoch,
            OLD.authority_incarnation,
            OLD.prepared_writer_epoch,
            OLD.configuration_epoch,
            OLD.fleet_generation,
            OLD.fleet_digest,
            OLD.execution_manifest_sha256,
            OLD.manifest_payload,
            OLD.trusted_fleet_release_sha256,
            OLD.oldlab_executor_id,
            OLD.oldlab_executor_incarnation,
            OLD.oldlab_pool_id,
            OLD.oldlab_pool_generation,
            OLD.oldlab_signing_key_sha256,
            OLD.oldlab_local_authority_sha256,
            OLD.oldlab_controller_authority_sha256,
            OLD.gb10_executor_id,
            OLD.gb10_executor_incarnation,
            OLD.gb10_pool_id,
            OLD.gb10_pool_generation,
            OLD.gb10_signing_key_sha256,
            OLD.gb10_local_authority_sha256,
            OLD.gb10_controller_authority_sha256,
            OLD.environment_acknowledgements_sha256,
            OLD.legacy_writer_manifest_sha256,
            OLD.rollback_evidence_sha256,
            OLD.requested_ceiling,
            OLD.requested_rate_per_minute,
            OLD.actor,
            OLD.idempotency_key,
            OLD.request_digest,
            OLD.prepared_at
          ) THEN
            RAISE EXCEPTION 'execution epoch immutable evidence changed'
              USING ERRCODE = '23514';
          END IF;

          IF OLD.state = NEW.state THEN
            IF ROW(
              NEW.effective_ceiling,
              NEW.effective_rate_per_minute,
              NEW.activation_actor,
              NEW.activation_idempotency_key,
              NEW.activation_request_digest,
              NEW.activated_at,
              NEW.drain_actor,
              NEW.drain_idempotency_key,
              NEW.drain_request_digest,
              NEW.drain_request_payload,
              NEW.drain_only_at,
              NEW.retirement_actor,
              NEW.retirement_idempotency_key,
              NEW.retirement_request_digest,
              NEW.retirement_request_payload,
              NEW.retired_at
            ) IS DISTINCT FROM ROW(
              OLD.effective_ceiling,
              OLD.effective_rate_per_minute,
              OLD.activation_actor,
              OLD.activation_idempotency_key,
              OLD.activation_request_digest,
              OLD.activated_at,
              OLD.drain_actor,
              OLD.drain_idempotency_key,
              OLD.drain_request_digest,
              OLD.drain_request_payload,
              OLD.drain_only_at,
              OLD.retirement_actor,
              OLD.retirement_idempotency_key,
              OLD.retirement_request_digest,
              OLD.retirement_request_payload,
              OLD.retired_at
            )
            OR (
              NEW.current_writer_epoch <> OLD.current_writer_epoch
              AND NOT (
                OLD.state = 'drain-only'
                AND NEW.current_writer_epoch = OLD.current_writer_epoch + 1
              )
            ) THEN
              RAISE EXCEPTION 'execution epoch state evidence changed without transition'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'prepared' AND NEW.state = 'active' THEN
            IF NEW.effective_ceiling <= 0
               OR NEW.effective_rate_per_minute <= 0
               OR NEW.current_writer_epoch <> OLD.current_writer_epoch
               OR NEW.activation_actor IS NULL
               OR NEW.activation_idempotency_key IS NULL
               OR NEW.activation_request_digest IS NULL
               OR NEW.activated_at IS NULL
               OR NEW.drain_actor IS NOT NULL
               OR NEW.drain_idempotency_key IS NOT NULL
               OR NEW.drain_request_digest IS NOT NULL
               OR NEW.drain_request_payload IS NOT NULL
               OR NEW.drain_only_at IS NOT NULL
               OR NEW.retirement_actor IS NOT NULL
               OR NEW.retirement_idempotency_key IS NOT NULL
               OR NEW.retirement_request_digest IS NOT NULL
               OR NEW.retirement_request_payload IS NOT NULL
               OR NEW.retired_at IS NOT NULL THEN
              RAISE EXCEPTION 'execution epoch activation evidence is incomplete'
                USING ERRCODE = '23514';
            END IF;
            IF (
              SELECT count(*)
              FROM public.capacity_execution_executors executor
              WHERE executor.execution_epoch = NEW.execution_epoch
                AND executor.execution_manifest_sha256 = NEW.execution_manifest_sha256
                AND (
                  (
                    executor.pool_id = 'oldlab'
                    AND executor.executor_id = NEW.oldlab_executor_id
                    AND executor.executor_incarnation = NEW.oldlab_executor_incarnation
                    AND executor.pool_generation = NEW.oldlab_pool_generation
                    AND executor.signing_key_sha256 = NEW.oldlab_signing_key_sha256
                    AND executor.local_authority_sha256 = NEW.oldlab_local_authority_sha256
                    AND executor.controller_authority_sha256 =
                      NEW.oldlab_controller_authority_sha256
                  )
                  OR (
                    executor.pool_id = 'gb10'
                    AND executor.executor_id = NEW.gb10_executor_id
                    AND executor.executor_incarnation = NEW.gb10_executor_incarnation
                    AND executor.pool_generation = NEW.gb10_pool_generation
                    AND executor.signing_key_sha256 = NEW.gb10_signing_key_sha256
                    AND executor.local_authority_sha256 = NEW.gb10_local_authority_sha256
                    AND executor.controller_authority_sha256 =
                      NEW.gb10_controller_authority_sha256
                  )
                )
            ) <> 2 THEN
              RAISE EXCEPTION 'execution epoch executable executor evidence is incomplete'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'prepared' AND NEW.state = 'retired' THEN
            IF NEW.effective_ceiling <> 0
               OR NEW.effective_rate_per_minute <> 0
               OR NEW.current_writer_epoch <> OLD.current_writer_epoch + 1
               OR NEW.activation_actor IS NOT NULL
               OR NEW.activation_idempotency_key IS NOT NULL
               OR NEW.activation_request_digest IS NOT NULL
               OR NEW.activated_at IS NOT NULL
               OR NEW.drain_actor IS NOT NULL
               OR NEW.drain_idempotency_key IS NOT NULL
               OR NEW.drain_request_digest IS NOT NULL
               OR NEW.drain_request_payload IS NOT NULL
               OR NEW.drain_only_at IS NOT NULL
               OR NEW.retirement_actor IS NULL
               OR NEW.retirement_idempotency_key IS NULL
               OR NEW.retirement_request_digest IS NULL
               OR NEW.retirement_request_payload IS NULL
               OR NEW.retired_at IS NULL THEN
              RAISE EXCEPTION 'prepared execution retirement evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'active' AND NEW.state = 'drain-only' THEN
            IF NEW.effective_ceiling <> 0
               OR NEW.effective_rate_per_minute <> 0
               OR NEW.current_writer_epoch NOT IN (
                 OLD.current_writer_epoch,
                 OLD.current_writer_epoch + 1
               )
               OR ROW(
                 NEW.activation_actor,
                 NEW.activation_idempotency_key,
                 NEW.activation_request_digest,
                 NEW.activated_at
               ) IS DISTINCT FROM ROW(
                 OLD.activation_actor,
                 OLD.activation_idempotency_key,
                 OLD.activation_request_digest,
                 OLD.activated_at
               )
               OR NEW.drain_actor IS NULL
               OR NEW.drain_idempotency_key IS NULL
               OR NEW.drain_request_digest IS NULL
               OR NEW.drain_request_payload IS NULL
               OR NEW.drain_only_at IS NULL
               OR NEW.retirement_actor IS NOT NULL
               OR NEW.retirement_idempotency_key IS NOT NULL
               OR NEW.retirement_request_digest IS NOT NULL
               OR NEW.retirement_request_payload IS NOT NULL
               OR NEW.retired_at IS NOT NULL THEN
              RAISE EXCEPTION 'execution drain-only evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'drain-only' AND NEW.state = 'retired' THEN
            IF NEW.effective_ceiling <> 0
               OR NEW.effective_rate_per_minute <> 0
               OR NEW.current_writer_epoch <> OLD.current_writer_epoch
               OR ROW(
                 NEW.activation_actor,
                 NEW.activation_idempotency_key,
                 NEW.activation_request_digest,
                 NEW.activated_at,
                 NEW.drain_actor,
                 NEW.drain_idempotency_key,
                 NEW.drain_request_digest,
                 NEW.drain_request_payload,
                 NEW.drain_only_at
               ) IS DISTINCT FROM ROW(
                 OLD.activation_actor,
                 OLD.activation_idempotency_key,
                 OLD.activation_request_digest,
                 OLD.activated_at,
                 OLD.drain_actor,
                 OLD.drain_idempotency_key,
                 OLD.drain_request_digest,
                 OLD.drain_request_payload,
                 OLD.drain_only_at
               )
               OR NEW.retirement_actor IS NULL
               OR NEW.retirement_idempotency_key IS NULL
               OR NEW.retirement_request_digest IS NULL
               OR NEW.retirement_request_payload IS NULL
               OR NEW.retired_at IS NULL THEN
              RAISE EXCEPTION 'execution retirement evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
            IF jsonb_typeof(NEW.retirement_request_payload) IS DISTINCT FROM 'object'
               OR (
                 SELECT count(*)
                 FROM jsonb_object_keys(NEW.retirement_request_payload)
               ) <> 7
               OR NOT (
                 NEW.retirement_request_payload ?& ARRAY[
                   'schema_version',
                   'authority_incarnation',
                   'expected_writer_epoch',
                   'execution_epoch',
                   'execution_manifest_sha256',
                   'executor_checkpoints',
                   'executable'
                 ]
               )
               OR NEW.retirement_request_payload -> 'schema_version'
                  IS DISTINCT FROM '2'::jsonb
               OR NEW.retirement_request_payload ->> 'authority_incarnation'
                  IS DISTINCT FROM NEW.authority_incarnation::text
               OR NEW.retirement_request_payload -> 'expected_writer_epoch'
                  IS DISTINCT FROM to_jsonb(NEW.current_writer_epoch)
               OR NEW.retirement_request_payload -> 'execution_epoch'
                  IS DISTINCT FROM to_jsonb(NEW.execution_epoch)
               OR NEW.retirement_request_payload ->> 'execution_manifest_sha256'
                  IS DISTINCT FROM NEW.execution_manifest_sha256
               OR NEW.retirement_request_payload -> 'executable'
                  IS DISTINCT FROM 'true'::jsonb
               OR jsonb_typeof(
                 NEW.retirement_request_payload -> 'executor_checkpoints'
               ) IS DISTINCT FROM 'array'
               OR jsonb_array_length(
                 NEW.retirement_request_payload -> 'executor_checkpoints'
               ) <> 2
               OR NEW.retirement_request_payload
                    -> 'executor_checkpoints' -> 0 ->> 'pool_id'
                  IS DISTINCT FROM 'gb10'
               OR NEW.retirement_request_payload
                    -> 'executor_checkpoints' -> 1 ->> 'pool_id'
                  IS DISTINCT FROM 'oldlab' THEN
              RAISE EXCEPTION 'execution retirement request payload is invalid'
                USING ERRCODE = '23514';
            END IF;
            PERFORM executor.id
            FROM public.capacity_executable_executor_states executor
            WHERE executor.execution_epoch = NEW.execution_epoch
            ORDER BY executor.pool_id
            FOR UPDATE;
            IF (
              SELECT count(*)
              FROM jsonb_array_elements(
                NEW.retirement_request_payload -> 'executor_checkpoints'
              ) WITH ORDINALITY AS checkpoint(value, position)
              JOIN public.capacity_executable_executor_states executor
                ON executor.execution_epoch = NEW.execution_epoch
               AND executor.execution_manifest_sha256 = NEW.execution_manifest_sha256
               AND executor.pool_id = checkpoint.value ->> 'pool_id'
               AND executor.executor_id = checkpoint.value ->> 'executor_id'
               AND executor.executor_incarnation::text
                   = checkpoint.value ->> 'executor_incarnation'
               AND checkpoint.value -> 'pool_generation'
                   = to_jsonb(executor.pool_generation)
              WHERE jsonb_typeof(checkpoint.value) = 'object'
                AND (
                  SELECT count(*) FROM jsonb_object_keys(checkpoint.value)
                ) = 11
                AND checkpoint.value ?& ARRAY[
                  'schema_version',
                  'executor_id',
                  'executor_incarnation',
                  'pool_id',
                  'pool_generation',
                  'heartbeat_sequence',
                  'command_sequence',
                  'journal_sequence',
                  'journal_digest',
                  'inventory_sequence',
                  'inventory_digest'
                ]
                AND checkpoint.value -> 'schema_version' = '2'::jsonb
                AND (
                  (checkpoint.position = 1 AND executor.pool_id = 'gb10')
                  OR (checkpoint.position = 2 AND executor.pool_id = 'oldlab')
                )
                AND executor.state = 'current'
                AND checkpoint.value -> 'heartbeat_sequence'
                    = to_jsonb(executor.heartbeat_high_water)
                AND checkpoint.value -> 'command_sequence'
                    = to_jsonb(executor.command_high_water)
                AND checkpoint.value -> 'journal_sequence'
                    = to_jsonb(executor.journal_high_water)
                AND executor.journal_digest = checkpoint.value ->> 'journal_digest'
                AND checkpoint.value -> 'inventory_sequence'
                    = to_jsonb(executor.inventory_high_water)
                AND executor.last_inventory_digest
                    = checkpoint.value ->> 'inventory_digest'
                AND executor.retirement_safe
                AND executor.retirement_inventory_digest
                    = checkpoint.value ->> 'inventory_digest'
                AND executor.inventory_payload IS NOT NULL
                AND executor.last_inventory_at IS NOT NULL
                AND executor.last_heartbeat_at > executor.last_inventory_at
                AND executor.lease_expires_at > clock_timestamp()
            ) <> 2 THEN
              RAISE EXCEPTION 'execution retirement executor evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
            PERFORM intent.id
            FROM public.capacity_executable_intents intent
            WHERE intent.execution_epoch = NEW.execution_epoch
            ORDER BY intent.launch_rank
            FOR UPDATE;
            IF EXISTS (
              SELECT 1
              FROM public.capacity_executable_intents intent
              WHERE intent.execution_epoch = NEW.execution_epoch
                AND intent.state <> 'released'
            ) THEN
              RAISE EXCEPTION 'execution retirement intent evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            RAISE EXCEPTION 'execution epoch state transition is not monotonic'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
CREATE OR REPLACE FUNCTION capacity_authority_execution_transition_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF OLD.execution_state = NEW.execution_state THEN
            IF ROW(
              NEW.execution_epoch,
              NEW.execution_manifest_sha256,
              NEW.executable_new_capacity_ceiling
            ) IS DISTINCT FROM ROW(
              OLD.execution_epoch,
              OLD.execution_manifest_sha256,
              OLD.executable_new_capacity_ceiling
            ) THEN
              RAISE EXCEPTION 'authority execution transition is not monotonic'
                USING ERRCODE = '23514';
            END IF;
            IF OLD.execution_state IN ('prepared', 'active')
               AND NEW.writer_epoch <> OLD.writer_epoch THEN
              RAISE EXCEPTION 'authority execution writer changed without transition'
                USING ERRCODE = '23514';
            END IF;
            IF OLD.execution_state = 'shadow'
               AND NEW.writer_epoch < OLD.writer_epoch THEN
              RAISE EXCEPTION 'authority execution writer did not advance monotonically'
                USING ERRCODE = '23514';
            END IF;
            IF OLD.execution_state = 'drain-only'
               AND NEW.writer_epoch NOT IN (OLD.writer_epoch, OLD.writer_epoch + 1) THEN
              RAISE EXCEPTION 'authority execution writer did not advance monotonically'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;

          IF OLD.authority_incarnation <> NEW.authority_incarnation THEN
            RAISE EXCEPTION 'authority incarnation changed during execution transition'
              USING ERRCODE = '23514';
          ELSIF OLD.execution_state = 'shadow' AND NEW.execution_state = 'prepared' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch
               OR NEW.execution_epoch <= 0
               OR NEW.execution_manifest_sha256 IS NULL
               OR NEW.executable_new_capacity_ceiling <> 0 THEN
              RAISE EXCEPTION 'authority execution preparation is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.execution_state = 'prepared' AND NEW.execution_state = 'active' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch
               OR NEW.execution_epoch <> OLD.execution_epoch
               OR NEW.execution_manifest_sha256 <> OLD.execution_manifest_sha256
               OR NEW.executable_new_capacity_ceiling <= 0 THEN
              RAISE EXCEPTION 'authority execution activation is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.execution_state = 'prepared' AND NEW.execution_state = 'shadow' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch + 1
               OR NEW.execution_epoch <> 0
               OR NEW.execution_manifest_sha256 IS NOT NULL
               OR NEW.executable_new_capacity_ceiling <> 0 THEN
              RAISE EXCEPTION 'authority prepared retirement is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.execution_state = 'active' AND NEW.execution_state = 'drain-only' THEN
            IF NEW.writer_epoch NOT IN (OLD.writer_epoch, OLD.writer_epoch + 1)
               OR NEW.execution_epoch <> OLD.execution_epoch
               OR NEW.execution_manifest_sha256 <> OLD.execution_manifest_sha256
               OR NEW.executable_new_capacity_ceiling <> 0 THEN
              RAISE EXCEPTION 'authority execution drain is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.execution_state = 'drain-only' AND NEW.execution_state = 'shadow' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch
               OR NEW.execution_epoch <> 0
               OR NEW.execution_manifest_sha256 IS NOT NULL
               OR NEW.executable_new_capacity_ceiling <> 0 THEN
              RAISE EXCEPTION 'authority execution retirement is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            RAISE EXCEPTION 'authority execution transition is not monotonic'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        sa.text("LOCK TABLE public.capacity_authority_state IN ACCESS EXCLUSIVE MODE")
    )
    connection.execute(
        sa.text(
            "LOCK TABLE public.capacity_execution_epochs, "
            "public.capacity_execution_executors, "
            "public.capacity_executable_executor_states, "
            "public.capacity_executable_intents, "
            "public.capacity_executable_command_receipts, "
            "public.capacity_executable_launch_rate_buckets "
            "IN ACCESS EXCLUSIVE MODE"
        )
    )
    if connection.execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_executable_executor_states "
            "UNION ALL SELECT 1 FROM public.capacity_executable_intents "
            "UNION ALL SELECT 1 FROM public.capacity_executable_command_receipts "
            "UNION ALL SELECT 1 FROM public.capacity_executable_launch_rate_buckets)"
        )
    ).scalar_one():
        raise RuntimeError("cannot downgrade capacity_0006 with executable bridge child evidence")
    if connection.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM public.capacity_execution_epochs "
            "WHERE requested_ceiling <> 1 "
            "OR drain_actor IS NOT NULL OR drain_idempotency_key IS NOT NULL "
            "OR drain_request_digest IS NOT NULL OR drain_request_payload IS NOT NULL "
            "OR retirement_actor IS NOT NULL OR retirement_idempotency_key IS NOT NULL "
            "OR retirement_request_digest IS NOT NULL "
            "OR retirement_request_payload IS NOT NULL)"
        )
    ).scalar_one():
        raise RuntimeError("cannot downgrade capacity_0006 with executable lifecycle evidence")

    op.execute(
        "DROP TRIGGER capacity_execution_epoch_transition_guard ON capacity_execution_epochs"
    )
    op.execute("DROP TRIGGER capacity_execution_epoch_truncate_guard ON capacity_execution_epochs")
    op.execute(
        "DROP TRIGGER capacity_authority_execution_transition_guard ON capacity_authority_state"
    )
    op.drop_table("capacity_executable_launch_rate_buckets")
    op.drop_table("capacity_executable_command_receipts")
    op.drop_table("capacity_executable_intents")
    op.drop_table("capacity_executable_executor_states")
    op.drop_constraint(
        "capacity_execution_executor_exact_binding_key",
        "capacity_execution_executors",
        type_="unique",
    )
    op.drop_constraint(
        "capacity_execution_epoch_retirement_idempotency_key",
        "capacity_execution_epochs",
        type_="unique",
    )
    op.drop_constraint(
        "capacity_execution_epoch_drain_idempotency_key",
        "capacity_execution_epochs",
        type_="unique",
    )
    op.drop_constraint(
        "capacity_execution_epoch_state_time_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_lifecycle_payload_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_lifecycle_actor_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_digest_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        "execution_epoch > 0 AND prepared_writer_epoch > 0 "
        "AND current_writer_epoch > 0 "
        "AND configuration_epoch > 0 AND fleet_generation > 0 "
        "AND oldlab_pool_generation > 0 AND gb10_pool_generation > 0 "
        "AND requested_ceiling = 1 AND effective_ceiling >= 0 "
        "AND effective_ceiling <= requested_ceiling "
        "AND requested_rate_per_minute > 0 AND effective_rate_per_minute >= 0 "
        "AND effective_rate_per_minute <= requested_rate_per_minute",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_digest_check",
        "capacity_execution_epochs",
        "fleet_digest ~ '^[0-9a-f]{64}$' "
        "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND trusted_fleet_release_sha256 ~ '^[0-9a-f]{64}$' "
        "AND oldlab_signing_key_sha256 ~ '^[0-9a-f]{64}$' "
        "AND oldlab_local_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND oldlab_controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND gb10_signing_key_sha256 ~ '^[0-9a-f]{64}$' "
        "AND gb10_local_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND gb10_controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
        "AND environment_acknowledgements_sha256 ~ '^[0-9a-f]{64}$' "
        "AND legacy_writer_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND rollback_evidence_sha256 ~ '^[0-9a-f]{64}$' "
        "AND request_digest ~ '^[0-9a-f]{64}$' "
        "AND (activation_request_digest IS NULL OR "
        "activation_request_digest ~ '^[0-9a-f]{64}$')",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_state_time_check",
        "capacity_execution_epochs",
        "(state = 'prepared' AND effective_ceiling = 0 "
        "AND effective_rate_per_minute = 0 "
        "AND activation_actor IS NULL AND activation_idempotency_key IS NULL "
        "AND activation_request_digest IS NULL AND activated_at IS NULL "
        "AND drain_only_at IS NULL AND retired_at IS NULL) OR "
        "(state = 'active' AND effective_ceiling > 0 "
        "AND effective_rate_per_minute > 0 "
        "AND activation_actor IS NOT NULL AND activation_idempotency_key IS NOT NULL "
        "AND activation_request_digest IS NOT NULL AND activated_at IS NOT NULL "
        "AND drain_only_at IS NULL AND retired_at IS NULL) OR "
        "(state = 'drain-only' AND effective_ceiling = 0 "
        "AND effective_rate_per_minute = 0 "
        "AND activation_actor IS NOT NULL AND activation_idempotency_key IS NOT NULL "
        "AND activation_request_digest IS NOT NULL AND activated_at IS NOT NULL "
        "AND drain_only_at IS NOT NULL "
        "AND retired_at IS NULL) OR "
        "(state = 'retired' AND effective_ceiling = 0 "
        "AND effective_rate_per_minute = 0 AND retired_at IS NOT NULL)",
    )
    op.drop_column("capacity_execution_epochs", "retirement_request_payload")
    op.drop_column("capacity_execution_epochs", "retirement_request_digest")
    op.drop_column("capacity_execution_epochs", "retirement_idempotency_key")
    op.drop_column("capacity_execution_epochs", "retirement_actor")
    op.drop_column("capacity_execution_epochs", "drain_request_payload")
    op.drop_column("capacity_execution_epochs", "drain_request_digest")
    op.drop_column("capacity_execution_epochs", "drain_idempotency_key")
    op.drop_column("capacity_execution_epochs", "drain_actor")
    op.execute(
        """
CREATE OR REPLACE FUNCTION capacity_execution_epoch_transition_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          IF TG_OP = 'INSERT' THEN
            IF NEW.state <> 'prepared'
               OR NEW.effective_ceiling <> 0
               OR NEW.activation_actor IS NOT NULL
               OR NEW.activation_idempotency_key IS NOT NULL
               OR NEW.activation_request_digest IS NOT NULL
               OR NEW.activated_at IS NOT NULL
               OR NEW.drain_only_at IS NOT NULL
               OR NEW.retired_at IS NOT NULL THEN
              RAISE EXCEPTION 'capacity execution epoch must be inserted prepared'
                USING ERRCODE = '23514';
            END IF;
            IF NEW.current_writer_epoch <> NEW.prepared_writer_epoch THEN
              RAISE EXCEPTION 'capacity execution epoch initial writer evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;

          IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'capacity execution epochs are append-only'
              USING ERRCODE = '23514';
          END IF;

          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'capacity execution epochs are append-only'
              USING ERRCODE = '23514';
          END IF;

          IF ROW(
            NEW.execution_epoch,
            NEW.authority_incarnation,
            NEW.prepared_writer_epoch,
            NEW.configuration_epoch,
            NEW.fleet_generation,
            NEW.fleet_digest,
            NEW.execution_manifest_sha256,
            NEW.manifest_payload,
            NEW.trusted_fleet_release_sha256,
            NEW.oldlab_executor_id,
            NEW.oldlab_executor_incarnation,
            NEW.oldlab_pool_id,
            NEW.oldlab_pool_generation,
            NEW.oldlab_signing_key_sha256,
            NEW.oldlab_local_authority_sha256,
            NEW.oldlab_controller_authority_sha256,
            NEW.gb10_executor_id,
            NEW.gb10_executor_incarnation,
            NEW.gb10_pool_id,
            NEW.gb10_pool_generation,
            NEW.gb10_signing_key_sha256,
            NEW.gb10_local_authority_sha256,
            NEW.gb10_controller_authority_sha256,
            NEW.environment_acknowledgements_sha256,
            NEW.legacy_writer_manifest_sha256,
            NEW.rollback_evidence_sha256,
            NEW.requested_ceiling,
            NEW.requested_rate_per_minute,
            NEW.actor,
            NEW.idempotency_key,
            NEW.request_digest,
            NEW.prepared_at
          ) IS DISTINCT FROM ROW(
            OLD.execution_epoch,
            OLD.authority_incarnation,
            OLD.prepared_writer_epoch,
            OLD.configuration_epoch,
            OLD.fleet_generation,
            OLD.fleet_digest,
            OLD.execution_manifest_sha256,
            OLD.manifest_payload,
            OLD.trusted_fleet_release_sha256,
            OLD.oldlab_executor_id,
            OLD.oldlab_executor_incarnation,
            OLD.oldlab_pool_id,
            OLD.oldlab_pool_generation,
            OLD.oldlab_signing_key_sha256,
            OLD.oldlab_local_authority_sha256,
            OLD.oldlab_controller_authority_sha256,
            OLD.gb10_executor_id,
            OLD.gb10_executor_incarnation,
            OLD.gb10_pool_id,
            OLD.gb10_pool_generation,
            OLD.gb10_signing_key_sha256,
            OLD.gb10_local_authority_sha256,
            OLD.gb10_controller_authority_sha256,
            OLD.environment_acknowledgements_sha256,
            OLD.legacy_writer_manifest_sha256,
            OLD.rollback_evidence_sha256,
            OLD.requested_ceiling,
            OLD.requested_rate_per_minute,
            OLD.actor,
            OLD.idempotency_key,
            OLD.request_digest,
            OLD.prepared_at
          ) THEN
            RAISE EXCEPTION 'execution epoch immutable evidence changed'
              USING ERRCODE = '23514';
          END IF;

          IF OLD.state = NEW.state THEN
            IF ROW(
              NEW.effective_ceiling,
              NEW.effective_rate_per_minute,
              NEW.activation_actor,
              NEW.activation_idempotency_key,
              NEW.activation_request_digest,
              NEW.activated_at,
              NEW.drain_only_at,
              NEW.retired_at
            ) IS DISTINCT FROM ROW(
              OLD.effective_ceiling,
              OLD.effective_rate_per_minute,
              OLD.activation_actor,
              OLD.activation_idempotency_key,
              OLD.activation_request_digest,
              OLD.activated_at,
              OLD.drain_only_at,
              OLD.retired_at
            )
            OR (
              NEW.current_writer_epoch <> OLD.current_writer_epoch
              AND NOT (
                OLD.state = 'drain-only'
                AND NEW.current_writer_epoch = OLD.current_writer_epoch + 1
              )
            ) THEN
              RAISE EXCEPTION 'execution epoch state evidence changed without transition'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'prepared' AND NEW.state = 'active' THEN
            IF NEW.effective_ceiling <= 0
               OR NEW.effective_rate_per_minute <= 0
               OR NEW.current_writer_epoch <> OLD.current_writer_epoch
               OR NEW.activation_actor IS NULL
               OR NEW.activation_idempotency_key IS NULL
               OR NEW.activation_request_digest IS NULL
               OR NEW.activated_at IS NULL
               OR NEW.drain_only_at IS NOT NULL
               OR NEW.retired_at IS NOT NULL THEN
              RAISE EXCEPTION 'execution epoch activation evidence is incomplete'
                USING ERRCODE = '23514';
            END IF;
            IF (
              SELECT count(*)
              FROM public.capacity_execution_executors executor
              WHERE executor.execution_epoch = NEW.execution_epoch
                AND executor.execution_manifest_sha256 = NEW.execution_manifest_sha256
                AND (
                  (
                    executor.pool_id = 'oldlab'
                    AND executor.executor_id = NEW.oldlab_executor_id
                    AND executor.executor_incarnation = NEW.oldlab_executor_incarnation
                    AND executor.pool_generation = NEW.oldlab_pool_generation
                    AND executor.signing_key_sha256 = NEW.oldlab_signing_key_sha256
                    AND executor.local_authority_sha256 = NEW.oldlab_local_authority_sha256
                    AND executor.controller_authority_sha256 =
                      NEW.oldlab_controller_authority_sha256
                  )
                  OR (
                    executor.pool_id = 'gb10'
                    AND executor.executor_id = NEW.gb10_executor_id
                    AND executor.executor_incarnation = NEW.gb10_executor_incarnation
                    AND executor.pool_generation = NEW.gb10_pool_generation
                    AND executor.signing_key_sha256 = NEW.gb10_signing_key_sha256
                    AND executor.local_authority_sha256 = NEW.gb10_local_authority_sha256
                    AND executor.controller_authority_sha256 =
                      NEW.gb10_controller_authority_sha256
                  )
                )
            ) <> 2 THEN
              RAISE EXCEPTION 'execution epoch executable executor evidence is incomplete'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'prepared' AND NEW.state = 'retired' THEN
            IF NEW.effective_ceiling <> 0
               OR NEW.effective_rate_per_minute <> 0
               OR NEW.current_writer_epoch <> OLD.current_writer_epoch
               OR NEW.activation_actor IS NOT NULL
               OR NEW.activation_idempotency_key IS NOT NULL
               OR NEW.activation_request_digest IS NOT NULL
               OR NEW.activated_at IS NOT NULL
               OR NEW.drain_only_at IS NOT NULL
               OR NEW.retired_at IS NULL THEN
              RAISE EXCEPTION 'prepared execution retirement evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'active' AND NEW.state = 'drain-only' THEN
            IF NEW.effective_ceiling <> 0
               OR NEW.effective_rate_per_minute <> 0
               OR NEW.current_writer_epoch <> OLD.current_writer_epoch + 1
               OR ROW(
                 NEW.activation_actor,
                 NEW.activation_idempotency_key,
                 NEW.activation_request_digest,
                 NEW.activated_at
               ) IS DISTINCT FROM ROW(
                 OLD.activation_actor,
                 OLD.activation_idempotency_key,
                 OLD.activation_request_digest,
                 OLD.activated_at
               )
               OR NEW.drain_only_at IS NULL
               OR NEW.retired_at IS NOT NULL THEN
              RAISE EXCEPTION 'execution drain-only evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.state = 'drain-only' AND NEW.state = 'retired' THEN
            IF NEW.effective_ceiling <> 0
               OR NEW.effective_rate_per_minute <> 0
               OR NEW.current_writer_epoch <> OLD.current_writer_epoch
               OR ROW(
                 NEW.activation_actor,
                 NEW.activation_idempotency_key,
                 NEW.activation_request_digest,
                 NEW.activated_at,
                 NEW.drain_only_at
               ) IS DISTINCT FROM ROW(
                 OLD.activation_actor,
                 OLD.activation_idempotency_key,
                 OLD.activation_request_digest,
                 OLD.activated_at,
                 OLD.drain_only_at
               )
               OR NEW.retired_at IS NULL THEN
              RAISE EXCEPTION 'execution retirement evidence is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            RAISE EXCEPTION 'execution epoch state transition is not monotonic'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_execution_epoch_transition_guard
        BEFORE INSERT OR UPDATE OR DELETE ON capacity_execution_epochs
        FOR EACH ROW EXECUTE FUNCTION capacity_execution_epoch_transition_guard()
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_execution_epoch_truncate_guard
        BEFORE TRUNCATE ON capacity_execution_epochs
        FOR EACH STATEMENT EXECUTE FUNCTION capacity_execution_epoch_transition_guard()
        """
    )
    op.execute(
        """
CREATE OR REPLACE FUNCTION capacity_authority_execution_transition_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF OLD.execution_state = NEW.execution_state THEN
            IF ROW(
              NEW.execution_epoch,
              NEW.execution_manifest_sha256,
              NEW.executable_new_capacity_ceiling
            ) IS DISTINCT FROM ROW(
              OLD.execution_epoch,
              OLD.execution_manifest_sha256,
              OLD.executable_new_capacity_ceiling
            ) THEN
              RAISE EXCEPTION 'authority execution transition is not monotonic'
                USING ERRCODE = '23514';
            END IF;
            IF OLD.execution_state IN ('prepared', 'active')
               AND NEW.writer_epoch <> OLD.writer_epoch THEN
              RAISE EXCEPTION 'authority execution writer changed without transition'
                USING ERRCODE = '23514';
            END IF;
            IF OLD.execution_state = 'shadow'
               AND NEW.writer_epoch < OLD.writer_epoch THEN
              RAISE EXCEPTION 'authority execution writer did not advance monotonically'
                USING ERRCODE = '23514';
            END IF;
            IF OLD.execution_state = 'drain-only'
               AND NEW.writer_epoch NOT IN (OLD.writer_epoch, OLD.writer_epoch + 1) THEN
              RAISE EXCEPTION 'authority execution writer did not advance monotonically'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;

          IF OLD.authority_incarnation <> NEW.authority_incarnation THEN
            RAISE EXCEPTION 'authority incarnation changed during execution transition'
              USING ERRCODE = '23514';
          ELSIF OLD.execution_state = 'shadow' AND NEW.execution_state = 'prepared' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch
               OR NEW.execution_epoch <= 0
               OR NEW.execution_manifest_sha256 IS NULL
               OR NEW.executable_new_capacity_ceiling <> 0 THEN
              RAISE EXCEPTION 'authority execution preparation is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.execution_state = 'prepared' AND NEW.execution_state = 'active' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch
               OR NEW.execution_epoch <> OLD.execution_epoch
               OR NEW.execution_manifest_sha256 <> OLD.execution_manifest_sha256
               OR NEW.executable_new_capacity_ceiling <= 0 THEN
              RAISE EXCEPTION 'authority execution activation is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.execution_state = 'prepared' AND NEW.execution_state = 'shadow' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch + 1
               OR NEW.execution_epoch <> 0
               OR NEW.execution_manifest_sha256 IS NOT NULL
               OR NEW.executable_new_capacity_ceiling <> 0 THEN
              RAISE EXCEPTION 'authority prepared retirement is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.execution_state = 'active' AND NEW.execution_state = 'drain-only' THEN
            IF NEW.writer_epoch <> OLD.writer_epoch + 1
               OR NEW.execution_epoch <> OLD.execution_epoch
               OR NEW.execution_manifest_sha256 <> OLD.execution_manifest_sha256
               OR NEW.executable_new_capacity_ceiling <> 0 THEN
              RAISE EXCEPTION 'authority execution drain is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSE
            RAISE EXCEPTION 'authority execution transition is not monotonic'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_authority_execution_transition_guard
        BEFORE UPDATE ON capacity_authority_state
        FOR EACH ROW EXECUTE FUNCTION capacity_authority_execution_transition_guard()
        """
    )
