"""Append-only inert protected-release registration fence.

Revision ID: guard_0010
Revises: guard_0009
Create Date: 2026-08-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0010"
down_revision: str | Sequence[str] | None = "guard_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected release migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def upgrade() -> None:
    quoted_agent = _agent_role()
    op.create_table(
        "protected_release_acknowledgements",
        sa.Column("release_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("admission_incarnation", sa.Uuid(), nullable=False),
        sa.Column("manager_authority_incarnation", sa.Uuid(), nullable=False),
        sa.Column("manager_writer_epoch", sa.BigInteger(), nullable=False),
        sa.Column("manager_configuration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("manager_allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("tranche_id", sa.Uuid(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("protected_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("bootstrap_revoked", sa.Boolean(), nullable=False),
        sa.Column("release_state", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "manager_writer_epoch > 0 AND manager_configuration_epoch > 0 "
            "AND manager_allocation_epoch > 0 AND pool_generation > 0 "
            "AND bootstrap_registration_epoch >= 0 "
            "AND protected_registration_epoch > bootstrap_registration_epoch",
            name="guard_protected_release_epochs_check",
        ),
        sa.CheckConstraint(
            "pool_id IN ('oldlab', 'gb10')",
            name="guard_protected_release_pool_check",
        ),
        sa.CheckConstraint(
            "shape_instance_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_protected_release_shape_check",
        ),
        sa.CheckConstraint(
            "bootstrap_revoked = true AND release_state = 'acknowledged' AND executable = false",
            name="guard_protected_release_inert_check",
        ),
        sa.CheckConstraint(
            "payload_digest ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(payload) = 'object' "
            "AND octet_length(payload::text) <= 8388608",
            name="guard_protected_release_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["plan_id", "shape_instance_id", "submission_intent_id", "pool_id"],
            [
                f"{SCHEMA}.prepared_worker_shapes.plan_id",
                f"{SCHEMA}.prepared_worker_shapes.shape_instance_id",
                f"{SCHEMA}.prepared_worker_shapes.submission_intent_id",
                f"{SCHEMA}.prepared_worker_shapes.pool_id",
            ],
            name="guard_protected_release_shape_binding_fk",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("release_id"),
        sa.UniqueConstraint(
            "shape_instance_id",
            name="guard_protected_release_shape_key",
        ),
        schema=SCHEMA,
    )
    for suffix, operation in (
        ("row", "UPDATE OR DELETE"),
        ("truncate", "TRUNCATE"),
    ):
        level = "ROW" if suffix == "row" else "STATEMENT"
        op.execute(
            f"""
            CREATE TRIGGER protected_release_acknowledgements_append_only_{suffix}
            BEFORE {operation} ON {SCHEMA}.protected_release_acknowledgements
            FOR EACH {level} EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )

    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.reject_released_shape_registration()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.protected_release_acknowledgements
             WHERE shape_instance_id = NEW.shape_instance_id
          ) THEN
            RAISE EXCEPTION 'protected release fence forbids delayed registration'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    for table in ("prepared_bootstrap_bindings", "prepared_worker_bindings"):
        op.execute(
            f"""
            CREATE TRIGGER {table}_protected_release_fence
            BEFORE INSERT ON {SCHEMA}.{table}
            FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_released_shape_registration()
            """
        )

    allowed_fields = ", ".join(
        f"'{field}'"
        for field in (
            "schema_version",
            "environment_id",
            "subject_id",
            "subject_incarnation",
            "authority_incarnation",
            "agent_incarnation",
            "reporter_incarnation",
            "authority_mode",
            "allocation_epoch",
            "reporter_high_water",
            "candidate_digest",
            "deployment_generation",
            "configuration_generation",
            "release_id",
            "plan_id",
            "admission_incarnation",
            "manager_authority_incarnation",
            "manager_writer_epoch",
            "manager_configuration_epoch",
            "manager_allocation_epoch",
            "tranche_id",
            "pool_id",
            "pool_generation",
            "shape_instance_id",
            "submission_intent_id",
            "bootstrap_registration_epoch",
            "protected_registration_epoch",
            "bootstrap_revoked",
            "release_state",
            "executable",
        )
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.acknowledge_inert_protected_release(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_release_id uuid;
          v_existing {SCHEMA}.protected_release_acknowledgements%ROWTYPE;
          v_bootstrap_epoch bigint;
          v_audit_payload jsonb;
        BEGIN
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF (p_payload - ARRAY[{allowed_fields}]::text[]) <> '{{}}'::jsonb THEN
            RAISE EXCEPTION 'protected release payload fields are invalid'
              USING ERRCODE = '22023';
          END IF;

          v_release_id := (p_payload->>'release_id')::uuid;
          SELECT * INTO v_existing
            FROM {SCHEMA}.protected_release_acknowledgements
           WHERE release_id = v_release_id
           FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest THEN
              RAISE EXCEPTION 'conflicting protected release replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;
          SELECT * INTO v_existing
            FROM {SCHEMA}.protected_release_acknowledgements
           WHERE shape_instance_id = p_payload->>'shape_instance_id'
           FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest THEN
              RAISE EXCEPTION 'protected release shape acknowledgement conflicts'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          IF p_payload->>'release_state' IS DISTINCT FROM 'acknowledged'
             OR (p_payload->>'bootstrap_revoked')::boolean IS DISTINCT FROM true
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR (p_payload->>'manager_writer_epoch')::bigint <= 0
             OR (p_payload->>'manager_configuration_epoch')::bigint <= 0
             OR (p_payload->>'manager_allocation_epoch')::bigint <= 0
             OR (p_payload->>'pool_generation')::bigint <= 0
             OR (p_payload->>'bootstrap_registration_epoch')::bigint < 0
             OR (p_payload->>'protected_registration_epoch')::bigint <=
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR p_payload->>'pool_id' NOT IN ('oldlab', 'gb10')
             OR p_payload->>'shape_instance_id' !~
                '^[a-z0-9][a-z0-9_.-]{{0,127}}$' THEN
            RAISE EXCEPTION 'protected release fence is not inert and monotonic'
              USING ERRCODE = '22023';
          END IF;
          IF NOT EXISTS (
            SELECT 1
              FROM {SCHEMA}.prepared_admission_plans AS p
              JOIN {SCHEMA}.prepared_worker_shapes AS s
                ON s.plan_id = p.plan_id
               AND s.shape_instance_id = p_payload->>'shape_instance_id'
               AND s.submission_intent_id =
                   (p_payload->>'submission_intent_id')::uuid
               AND s.pool_id = p_payload->>'pool_id'
             WHERE p.plan_id = (p_payload->>'plan_id')::uuid
               AND p.agent_incarnation = p_agent_incarnation
               AND p.admission_incarnation =
                   (p_payload->>'admission_incarnation')::uuid
               AND p.manager_authority_incarnation =
                   (p_payload->>'manager_authority_incarnation')::uuid
               AND p.manager_writer_epoch =
                   (p_payload->>'manager_writer_epoch')::bigint
               AND p.manager_allocation_epoch =
                   (p_payload->>'manager_allocation_epoch')::bigint
               AND p.pool_generation = (p_payload->>'pool_generation')::bigint
               AND p.plan_state = 'prepared' AND p.executable = false
               AND s.shape_state = 'prepared' AND s.executable = false
          ) THEN
            RAISE EXCEPTION 'protected release differs from its exact prepared shape'
              USING ERRCODE = '55000';
          END IF;
          SELECT max(bootstrap_registration_epoch) INTO v_bootstrap_epoch
            FROM {SCHEMA}.prepared_bootstrap_bindings
           WHERE shape_instance_id = p_payload->>'shape_instance_id'
             AND submission_intent_id =
                 (p_payload->>'submission_intent_id')::uuid;
          IF COALESCE(v_bootstrap_epoch, 0) IS DISTINCT FROM
             (p_payload->>'bootstrap_registration_epoch')::bigint THEN
            RAISE EXCEPTION 'protected release bootstrap high-water changed'
              USING ERRCODE = '55000';
          END IF;
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.prepared_worker_bindings
             WHERE shape_instance_id = p_payload->>'shape_instance_id'
               AND submission_intent_id =
                   (p_payload->>'submission_intent_id')::uuid
          ) THEN
            RAISE EXCEPTION 'protected release still has a prepared worker binding'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.protected_release_acknowledgements
            (release_id, plan_id, agent_incarnation, admission_incarnation,
             manager_authority_incarnation, manager_writer_epoch,
             manager_configuration_epoch, manager_allocation_epoch, tranche_id,
             pool_id, pool_generation, shape_instance_id, submission_intent_id,
             bootstrap_registration_epoch, protected_registration_epoch,
             bootstrap_revoked, release_state, executable, payload, payload_digest)
          VALUES
            (v_release_id, (p_payload->>'plan_id')::uuid, p_agent_incarnation,
             (p_payload->>'admission_incarnation')::uuid,
             (p_payload->>'manager_authority_incarnation')::uuid,
             (p_payload->>'manager_writer_epoch')::bigint,
             (p_payload->>'manager_configuration_epoch')::bigint,
             (p_payload->>'manager_allocation_epoch')::bigint,
             (p_payload->>'tranche_id')::uuid, p_payload->>'pool_id',
             (p_payload->>'pool_generation')::bigint,
             p_payload->>'shape_instance_id',
             (p_payload->>'submission_intent_id')::uuid,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             (p_payload->>'protected_registration_epoch')::bigint,
             true, 'acknowledged', false, p_payload, p_payload_digest);
          v_audit_payload := jsonb_build_object(
            'schema_version', 1, 'release_id', v_release_id,
            'plan_id', p_payload->>'plan_id',
            'tranche_id', p_payload->>'tranche_id',
            'shape_instance_id', p_payload->>'shape_instance_id',
            'submission_intent_id', p_payload->>'submission_intent_id',
            'bootstrap_registration_epoch',
              (p_payload->>'bootstrap_registration_epoch')::bigint,
            'protected_registration_epoch',
              (p_payload->>'protected_registration_epoch')::bigint,
            'bootstrap_revoked', true, 'executable', false
          );
          INSERT INTO {SCHEMA}.audit_events (event_type, payload, payload_digest)
          VALUES ('protected_release_acknowledged.v1', v_audit_payload,
                  p_payload_digest);
          RETURN p_payload;
        END
        $function$
        """
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON {SCHEMA}.protected_release_acknowledgements FROM PUBLIC")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION "
        f"{SCHEMA}.acknowledge_inert_protected_release(uuid,jsonb,bytea,text) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION "
        f"{SCHEMA}.acknowledge_inert_protected_release(uuid,jsonb,bytea,text) "
        f"TO {quoted_agent}"
    )


def downgrade() -> None:
    quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION "
        f"{SCHEMA}.acknowledge_inert_protected_release(uuid,jsonb,bytea,text) "
        f"FROM {quoted_agent}"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.acknowledge_inert_protected_release(uuid,jsonb,bytea,text)")
    for table in ("prepared_worker_bindings", "prepared_bootstrap_bindings"):
        op.execute(f"DROP TRIGGER {table}_protected_release_fence ON {SCHEMA}.{table}")
    op.execute(f"DROP FUNCTION {SCHEMA}.reject_released_shape_registration()")
    op.drop_table("protected_release_acknowledgements", schema=SCHEMA)
