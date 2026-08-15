"""fenced executable bridge epoch

Revision ID: capacity_0004
Revises: capacity_0003
Create Date: 2026-08-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0004"
down_revision: str | Sequence[str] | None = "capacity_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "capacity_candidates",
        sa.Column("candidate_identity_algorithm", sa.Text(), nullable=True),
    )
    op.add_column(
        "capacity_candidates",
        sa.Column("candidate_identity", sa.Text(), nullable=True),
    )
    # Before capacity_0004 the only CapacityCandidate producer is the
    # personal dynamic projection, whose candidate_digest is its exact
    # source-sha256 identity. Protected git-sha1 producers arrive later and
    # must write the tagged fields explicitly.
    op.execute(
        "UPDATE capacity_candidates SET "
        "candidate_identity_algorithm = 'source-sha256', "
        "candidate_identity = candidate_digest"
    )
    op.alter_column(
        "capacity_candidates",
        "candidate_identity_algorithm",
        nullable=False,
    )
    op.alter_column(
        "capacity_candidates",
        "candidate_identity",
        nullable=False,
    )
    op.create_check_constraint(
        "capacity_candidate_identity_check",
        "capacity_candidates",
        "(candidate_identity_algorithm = 'git-sha1' "
        "AND candidate_identity ~ '^[0-9a-f]{40}$') OR "
        "(candidate_identity_algorithm = 'source-sha256' "
        "AND candidate_identity ~ '^[0-9a-f]{64}$')",
    )
    op.create_table(
        "capacity_execution_epochs",
        sa.Column("execution_epoch", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("authority_incarnation", sa.UUID(), nullable=False),
        sa.Column("prepared_writer_epoch", sa.BigInteger(), nullable=False),
        sa.Column("current_writer_epoch", sa.BigInteger(), nullable=False),
        sa.Column("configuration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("fleet_generation", sa.BigInteger(), nullable=False),
        sa.Column("fleet_digest", sa.Text(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("manifest_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trusted_fleet_release_sha256", sa.Text(), nullable=False),
        sa.Column("oldlab_executor_id", sa.Text(), nullable=False),
        sa.Column("oldlab_executor_incarnation", sa.UUID(), nullable=False),
        sa.Column("oldlab_pool_id", sa.Text(), nullable=False),
        sa.Column("oldlab_pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("gb10_executor_id", sa.Text(), nullable=False),
        sa.Column("gb10_executor_incarnation", sa.UUID(), nullable=False),
        sa.Column("gb10_pool_id", sa.Text(), nullable=False),
        sa.Column("gb10_pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("environment_acknowledgements_sha256", sa.Text(), nullable=False),
        sa.Column("legacy_writer_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("rollback_evidence_sha256", sa.Text(), nullable=False),
        sa.Column("requested_ceiling", sa.BigInteger(), nullable=False),
        sa.Column("effective_ceiling", sa.BigInteger(), nullable=False),
        sa.Column("requested_rate_per_minute", sa.BigInteger(), nullable=False),
        sa.Column("effective_rate_per_minute", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("request_digest", sa.Text(), nullable=False),
        sa.Column("activation_actor", sa.Text(), nullable=True),
        sa.Column("activation_idempotency_key", sa.UUID(), nullable=True),
        sa.Column("activation_request_digest", sa.Text(), nullable=True),
        sa.Column(
            "prepared_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("activated_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("drain_only_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("retired_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "execution_epoch > 0 AND prepared_writer_epoch > 0 "
            "AND current_writer_epoch > 0 "
            "AND configuration_epoch > 0 AND fleet_generation > 0 "
            "AND oldlab_pool_generation > 0 AND gb10_pool_generation > 0 "
            "AND requested_ceiling = 1 AND effective_ceiling >= 0 "
            "AND effective_ceiling <= requested_ceiling "
            "AND requested_rate_per_minute > 0 AND effective_rate_per_minute >= 0 "
            "AND effective_rate_per_minute <= requested_rate_per_minute",
            name="capacity_execution_epoch_quantity_check",
        ),
        sa.CheckConstraint(
            "fleet_digest ~ '^[0-9a-f]{64}$' "
            "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND trusted_fleet_release_sha256 ~ '^[0-9a-f]{64}$' "
            "AND environment_acknowledgements_sha256 ~ '^[0-9a-f]{64}$' "
            "AND legacy_writer_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND rollback_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND request_digest ~ '^[0-9a-f]{64}$' "
            "AND (activation_request_digest IS NULL OR "
            "activation_request_digest ~ '^[0-9a-f]{64}$')",
            name="capacity_execution_epoch_digest_check",
        ),
        sa.CheckConstraint(
            "oldlab_pool_id = 'oldlab' AND gb10_pool_id = 'gb10' "
            "AND oldlab_executor_id <> gb10_executor_id "
            "AND oldlab_executor_incarnation <> gb10_executor_incarnation",
            name="capacity_execution_epoch_pool_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(manifest_payload) = 'object' "
            "AND octet_length(manifest_payload::text) <= 8388608",
            name="capacity_execution_epoch_manifest_check",
        ),
        sa.CheckConstraint(
            "state IN ('prepared','active','drain-only','retired')",
            name="capacity_execution_epoch_state_check",
        ),
        sa.CheckConstraint(
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
            name="capacity_execution_epoch_state_time_check",
        ),
        sa.ForeignKeyConstraint(
            ["configuration_epoch"],
            ["capacity_configuration_epochs.configuration_epoch"],
            name="capacity_execution_epoch_configuration_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["configuration_epoch", "oldlab_pool_id", "oldlab_pool_generation"],
            [
                "capacity_pools.configuration_epoch",
                "capacity_pools.pool_id",
                "capacity_pools.pool_generation",
            ],
            name="capacity_execution_epoch_oldlab_pool_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["configuration_epoch", "gb10_pool_id", "gb10_pool_generation"],
            [
                "capacity_pools.configuration_epoch",
                "capacity_pools.pool_id",
                "capacity_pools.pool_generation",
            ],
            name="capacity_execution_epoch_gb10_pool_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("execution_epoch"),
        sa.UniqueConstraint(
            "activation_idempotency_key",
            name="capacity_execution_epoch_activation_idempotency_key",
        ),
        sa.UniqueConstraint("idempotency_key", name="capacity_execution_epoch_idempotency_key"),
        sa.UniqueConstraint(
            "execution_epoch",
            "execution_manifest_sha256",
            "state",
            "effective_ceiling",
            name="capacity_execution_epoch_authority_binding_key",
        ),
    )
    op.create_table(
        "capacity_execution_executors",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("executor_id", sa.Text(), nullable=False),
        sa.Column("executor_incarnation", sa.UUID(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("signing_key_id", sa.Text(), nullable=False),
        sa.Column("signing_key_sha256", sa.Text(), nullable=False),
        sa.Column("local_authority_sha256", sa.Text(), nullable=False),
        sa.Column("controller_authority_sha256", sa.Text(), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("registration_digest", sa.Text(), nullable=False),
        sa.Column("registration_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "registered_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND pool_generation > 0 AND pool_id IN ('gb10','oldlab')",
            name="capacity_execution_executor_binding_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND signing_key_sha256 ~ '^[0-9a-f]{64}$' "
            "AND local_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND registration_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_execution_executor_digest_check",
        ),
        sa.ForeignKeyConstraint(
            ["execution_epoch"],
            ["capacity_execution_epochs.execution_epoch"],
            name="capacity_execution_executor_epoch_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "execution_epoch",
            "pool_id",
            name="capacity_execution_executor_pool_key",
        ),
        sa.UniqueConstraint(
            "executor_incarnation",
            name="capacity_execution_executor_incarnation_key",
        ),
        sa.UniqueConstraint(
            "idempotency_key",
            name="capacity_execution_executor_idempotency_key",
        ),
    )
    op.add_column(
        "capacity_authority_state",
        sa.Column(
            "execution_epoch",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "capacity_authority_state",
        sa.Column(
            "execution_state",
            sa.Text(),
            server_default=sa.text("'shadow'"),
            nullable=False,
        ),
    )
    op.add_column(
        "capacity_authority_state",
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=True),
    )
    op.drop_constraint(
        "capacity_authority_shadow_only_check",
        "capacity_authority_state",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_authority_execution_check",
        "capacity_authority_state",
        "(execution_state = 'shadow' AND execution_epoch = 0 "
        "AND execution_manifest_sha256 IS NULL "
        "AND executable_new_capacity_ceiling = 0) OR "
        "(execution_state = 'prepared' AND execution_epoch > 0 "
        "AND execution_manifest_sha256 IS NOT NULL "
        "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND executable_new_capacity_ceiling = 0) OR "
        "(execution_state = 'active' AND execution_epoch > 0 "
        "AND execution_manifest_sha256 IS NOT NULL "
        "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND executable_new_capacity_ceiling > 0) OR "
        "(execution_state = 'drain-only' AND execution_epoch > 0 "
        "AND execution_manifest_sha256 IS NOT NULL "
        "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND executable_new_capacity_ceiling = 0)",
    )
    op.create_foreign_key(
        "capacity_authority_execution_epoch_fkey",
        "capacity_authority_state",
        "capacity_execution_epochs",
        [
            "execution_epoch",
            "execution_manifest_sha256",
            "execution_state",
            "executable_new_capacity_ceiling",
        ],
        [
            "execution_epoch",
            "execution_manifest_sha256",
            "state",
            "effective_ceiling",
        ],
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    op.execute(
        """
        CREATE FUNCTION capacity_execution_epoch_transition_guard()
        RETURNS trigger
        LANGUAGE plpgsql
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
            NEW.gb10_executor_id,
            NEW.gb10_executor_incarnation,
            NEW.gb10_pool_id,
            NEW.gb10_pool_generation,
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
            OLD.gb10_executor_id,
            OLD.gb10_executor_incarnation,
            OLD.gb10_pool_id,
            OLD.gb10_pool_generation,
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
              FROM capacity_execution_executors executor
              WHERE executor.execution_epoch = NEW.execution_epoch
                AND executor.execution_manifest_sha256 = NEW.execution_manifest_sha256
                AND (
                  (
                    executor.pool_id = 'oldlab'
                    AND executor.executor_id = NEW.oldlab_executor_id
                    AND executor.executor_incarnation = NEW.oldlab_executor_incarnation
                    AND executor.pool_generation = NEW.oldlab_pool_generation
                  )
                  OR (
                    executor.pool_id = 'gb10'
                    AND executor.executor_id = NEW.gb10_executor_id
                    AND executor.executor_incarnation = NEW.gb10_executor_incarnation
                    AND executor.pool_generation = NEW.gb10_pool_generation
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
        CREATE FUNCTION capacity_execution_evidence_append_only_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          RAISE EXCEPTION 'capacity execution evidence is append-only'
            USING ERRCODE = '23514';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_execution_executor_append_only_guard
        BEFORE UPDATE OR DELETE ON capacity_execution_executors
        FOR EACH ROW EXECUTE FUNCTION capacity_execution_evidence_append_only_guard()
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_execution_executor_truncate_guard
        BEFORE TRUNCATE ON capacity_execution_executors
        FOR EACH STATEMENT EXECUTE FUNCTION capacity_execution_evidence_append_only_guard()
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER capacity_execution_executor_truncate_guard ON capacity_execution_executors"
    )
    op.execute(
        "DROP TRIGGER capacity_execution_executor_append_only_guard ON capacity_execution_executors"
    )
    op.execute("DROP FUNCTION capacity_execution_evidence_append_only_guard()")
    op.execute("DROP TRIGGER capacity_execution_epoch_truncate_guard ON capacity_execution_epochs")
    op.execute(
        "DROP TRIGGER capacity_execution_epoch_transition_guard ON capacity_execution_epochs"
    )
    op.execute("DROP FUNCTION capacity_execution_epoch_transition_guard()")
    op.drop_constraint(
        "capacity_authority_execution_epoch_fkey",
        "capacity_authority_state",
        type_="foreignkey",
    )
    op.drop_constraint(
        "capacity_authority_execution_check",
        "capacity_authority_state",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_authority_shadow_only_check",
        "capacity_authority_state",
        "executable_new_capacity_ceiling = 0",
    )
    op.drop_column("capacity_authority_state", "execution_manifest_sha256")
    op.drop_column("capacity_authority_state", "execution_state")
    op.drop_column("capacity_authority_state", "execution_epoch")
    op.drop_table("capacity_execution_executors")
    op.drop_table("capacity_execution_epochs")
    op.drop_constraint(
        "capacity_candidate_identity_check",
        "capacity_candidates",
        type_="check",
    )
    op.drop_column("capacity_candidates", "candidate_identity")
    op.drop_column("capacity_candidates", "candidate_identity_algorithm")
