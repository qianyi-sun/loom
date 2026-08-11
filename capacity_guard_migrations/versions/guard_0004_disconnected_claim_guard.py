"""Disconnected, zero-executable claim-guard lifecycle.

Revision ID: guard_0004
Revises: guard_0003
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0004"
down_revision: str | Sequence[str] | None = "guard_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
APPEND_ONLY_TABLES = (
    "claim_guard_activation",
    "attempt_lifecycle_events",
    "protected_claim_leases",
)


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("claim-guard migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def _install_append_only_guards() -> None:
    for table in APPEND_ONLY_TABLES:
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_row
            BEFORE UPDATE OR DELETE ON {SCHEMA}.{table}
            FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_truncate
            BEFORE TRUNCATE ON {SCHEMA}.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )


def _install_lifecycle_initializer() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.initialize_attempt_lifecycle()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_payload jsonb;
          v_transition_id uuid := gen_random_uuid();
        BEGIN
          v_payload := jsonb_build_object(
            'schema_version', 1,
            'transition_id', v_transition_id,
            'protected_attempt_id', NEW.protected_attempt_id,
            'execution_generation', NEW.execution_generation,
            'requirements_digest', NEW.requirements_digest,
            'transition_sequence', 0,
            'operation', 'initialize',
            'previous_state', NULL,
            'lifecycle_state', 'pending-unassigned',
            'executable', false
          );
          INSERT INTO {SCHEMA}.attempt_lifecycle_events
            (transition_id, protected_attempt_id, execution_generation,
             requirements_digest, transition_sequence, operation,
             previous_state, lifecycle_state, executable, payload, payload_digest)
          VALUES
            (v_transition_id, NEW.protected_attempt_id,
             NEW.execution_generation, NEW.requirements_digest, 0, 'initialize',
             NULL, 'pending-unassigned', false, v_payload,
             encode(sha256(convert_to(v_payload::text, 'UTF8')), 'hex'));
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER trial_attempts_initialize_lifecycle
        AFTER INSERT ON {SCHEMA}.trial_attempts
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.initialize_attempt_lifecycle()
        """
    )
    op.execute(
        f"""
        DO $block$
        DECLARE
          v_attempt record;
          v_payload jsonb;
          v_transition_id uuid;
        BEGIN
          FOR v_attempt IN
            SELECT protected_attempt_id, execution_generation, requirements_digest
              FROM {SCHEMA}.trial_attempts
             ORDER BY protected_attempt_id
          LOOP
            v_transition_id := gen_random_uuid();
            v_payload := jsonb_build_object(
              'schema_version', 1,
              'transition_id', v_transition_id,
              'protected_attempt_id', v_attempt.protected_attempt_id,
              'execution_generation', v_attempt.execution_generation,
              'requirements_digest', v_attempt.requirements_digest,
              'transition_sequence', 0,
              'operation', 'initialize',
              'previous_state', NULL,
              'lifecycle_state', 'pending-unassigned',
              'executable', false
            );
            INSERT INTO {SCHEMA}.attempt_lifecycle_events
              (transition_id, protected_attempt_id, execution_generation,
               requirements_digest, transition_sequence, operation,
               previous_state, lifecycle_state, executable, payload, payload_digest)
            VALUES
              (v_transition_id, v_attempt.protected_attempt_id,
               v_attempt.execution_generation, v_attempt.requirements_digest,
               0, 'initialize', NULL, 'pending-unassigned', false, v_payload,
               encode(sha256(convert_to(v_payload::text, 'UTF8')), 'hex'));
          END LOOP;
        END
        $block$
        """
    )


def _install_transition_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.apply_inert_attempt_transition(
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
          v_transition_id uuid;
          v_attempt_id uuid;
          v_existing {SCHEMA}.attempt_lifecycle_events%ROWTYPE;
          v_current {SCHEMA}.attempt_lifecycle_events%ROWTYPE;
          v_operation text;
          v_expected_state text;
          v_target_state text;
          v_assignment_required boolean;
        BEGIN
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          v_transition_id := (p_payload->>'transition_id')::uuid;
          v_attempt_id := (p_payload->>'protected_attempt_id')::uuid;
          SELECT * INTO v_existing
            FROM {SCHEMA}.attempt_lifecycle_events
           WHERE transition_id = v_transition_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.operation = 'initialize'
               OR v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest THEN
              RAISE EXCEPTION 'conflicting inert lifecycle replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          IF NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.claim_guard_activation
             WHERE singleton_id = 1
               AND activation_state = 'disabled'
               AND authority_mode = 'disabled'
               AND activation_epoch = 0
               AND executable_new_capacity_ceiling = 0
               AND live_claim_entry_enabled = false
          ) THEN
            RAISE EXCEPTION 'claim guard is not in its immutable disabled state'
              USING ERRCODE = '55000';
          END IF;

          SELECT l.* INTO v_current
            FROM {SCHEMA}.attempt_lifecycle_events AS l
            JOIN {SCHEMA}.trial_attempts AS a
              ON a.protected_attempt_id = l.protected_attempt_id
             AND a.execution_generation = l.execution_generation
             AND a.requirements_digest = l.requirements_digest
           WHERE l.protected_attempt_id = v_attempt_id
           ORDER BY l.transition_sequence DESC
           LIMIT 1
           FOR KEY SHARE OF l, a;
          IF NOT FOUND
             OR v_current.execution_generation IS DISTINCT FROM
                (p_payload->>'execution_generation')::bigint
             OR v_current.requirements_digest IS DISTINCT FROM
                p_payload->>'requirements_digest'
             OR v_current.transition_sequence IS DISTINCT FROM
                (p_payload->>'expected_transition_sequence')::bigint
             OR v_current.lifecycle_state IS DISTINCT FROM p_payload->>'expected_state' THEN
            RAISE EXCEPTION 'inert lifecycle compare-and-set failed'
              USING ERRCODE = '40001';
          END IF;

          v_operation := p_payload->>'operation';
          v_expected_state := p_payload->>'expected_state';
          v_target_state := p_payload->>'target_state';
          v_assignment_required := v_operation IN ('assign', 'withdraw')
            OR (v_operation = 'cancel' AND v_expected_state = 'assigned');
          IF (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR p_payload->>'transition_reason' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR (v_operation = 'assign' AND
                 (v_expected_state, v_target_state) IS DISTINCT FROM
                 ('pending-unassigned', 'assigned'))
             OR (v_operation = 'withdraw' AND
                 (v_expected_state, v_target_state) IS DISTINCT FROM
                 ('assigned', 'pending-unassigned'))
             OR (v_operation = 'cancel' AND
                 (v_expected_state NOT IN ('pending-unassigned', 'assigned')
                  OR v_target_state <> 'cancelled-terminal'))
             OR v_operation NOT IN ('assign', 'withdraw', 'cancel') THEN
            RAISE EXCEPTION 'inert lifecycle operation is invalid'
              USING ERRCODE = '22023';
          END IF;

          IF v_assignment_required THEN
            IF (p_payload->>'allowance_id') IS NULL
               OR (p_payload->>'plan_id') IS NULL
               OR (p_payload->>'admission_incarnation') IS NULL
               OR (p_payload->>'manager_allocation_epoch') IS NULL
               OR (p_payload->>'pool_id') IS NULL
               OR (p_payload->>'shape_instance_id') IS NULL
               OR (p_payload->>'submission_intent_id') IS NULL THEN
              RAISE EXCEPTION 'inert lifecycle assignment binding is incomplete'
                USING ERRCODE = '22023';
            END IF;
          ELSIF (p_payload->>'allowance_id') IS NOT NULL
             OR (p_payload->>'plan_id') IS NOT NULL
             OR (p_payload->>'admission_incarnation') IS NOT NULL
             OR (p_payload->>'manager_allocation_epoch') IS NOT NULL
             OR (p_payload->>'pool_id') IS NOT NULL
             OR (p_payload->>'shape_instance_id') IS NOT NULL
             OR (p_payload->>'submission_intent_id') IS NOT NULL THEN
            RAISE EXCEPTION 'unassigned lifecycle operation carries an assignment binding'
              USING ERRCODE = '22023';
          END IF;

          IF v_operation = 'assign' THEN
            IF NOT EXISTS (
              SELECT 1
                FROM {SCHEMA}.prepared_placement_allowances AS a
                JOIN {SCHEMA}.prepared_admission_plans AS p ON p.plan_id = a.plan_id
                JOIN {SCHEMA}.trial_attempts AS ta
                  ON ta.protected_attempt_id = a.protected_attempt_id
                 AND ta.execution_generation = a.execution_generation
                 AND ta.requirements_digest = a.requirements_digest
                JOIN public.trials AS t ON t.id = ta.trial_id
               WHERE a.allowance_id = (p_payload->>'allowance_id')::uuid
                 AND a.plan_id = (p_payload->>'plan_id')::uuid
                 AND a.protected_attempt_id = v_attempt_id
                 AND a.execution_generation =
                     (p_payload->>'execution_generation')::bigint
                 AND a.requirements_digest = p_payload->>'requirements_digest'
                 AND a.pool_id = p_payload->>'pool_id'
                 AND a.shape_instance_id = p_payload->>'shape_instance_id'
                 AND a.submission_intent_id =
                     (p_payload->>'submission_intent_id')::uuid
                 AND a.allowance_state = 'prepared' AND a.executable = false
                 AND p.agent_incarnation = p_agent_incarnation
                 AND p.admission_incarnation =
                     (p_payload->>'admission_incarnation')::uuid
                 AND p.manager_allocation_epoch =
                     (p_payload->>'manager_allocation_epoch')::bigint
                 AND p.plan_state = 'prepared' AND p.executable = false
                 AND p.lease_not_after > statement_timestamp()
                 AND ta.claim_state = 'queued'
                 AND t.state = 'queued'
                 AND t.cancellation_requested_at IS NULL
                 AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= statement_timestamp())
                 AND t.autoscaler_pool_name IS NULL
            ) THEN
              RAISE EXCEPTION 'inert assignment differs from its prepared allowance'
                USING ERRCODE = '55000';
            END IF;
          ELSIF v_assignment_required THEN
            IF v_current.allowance_id IS DISTINCT FROM
                 (p_payload->>'allowance_id')::uuid
               OR v_current.plan_id IS DISTINCT FROM (p_payload->>'plan_id')::uuid
               OR v_current.admission_incarnation IS DISTINCT FROM
                  (p_payload->>'admission_incarnation')::uuid
               OR v_current.manager_allocation_epoch IS DISTINCT FROM
                  (p_payload->>'manager_allocation_epoch')::bigint
               OR v_current.pool_id IS DISTINCT FROM p_payload->>'pool_id'
               OR v_current.shape_instance_id IS DISTINCT FROM
                  p_payload->>'shape_instance_id'
               OR v_current.submission_intent_id IS DISTINCT FROM
                  (p_payload->>'submission_intent_id')::uuid THEN
              RAISE EXCEPTION 'inert transition differs from its current assignment'
                USING ERRCODE = '55000';
            END IF;
          END IF;

          INSERT INTO {SCHEMA}.attempt_lifecycle_events
            (transition_id, protected_attempt_id, execution_generation,
             requirements_digest, transition_sequence, operation,
             previous_state, lifecycle_state, allowance_id, plan_id,
             admission_incarnation, manager_allocation_epoch, pool_id,
             shape_instance_id, submission_intent_id, executable,
             payload, payload_digest)
          VALUES
            (v_transition_id, v_attempt_id,
             (p_payload->>'execution_generation')::bigint,
             p_payload->>'requirements_digest',
             (p_payload->>'expected_transition_sequence')::bigint + 1,
             v_operation, v_expected_state, v_target_state,
             (p_payload->>'allowance_id')::uuid,
             (p_payload->>'plan_id')::uuid,
             (p_payload->>'admission_incarnation')::uuid,
             (p_payload->>'manager_allocation_epoch')::bigint,
             p_payload->>'pool_id', p_payload->>'shape_instance_id',
             (p_payload->>'submission_intent_id')::uuid,
             false, p_payload, p_payload_digest);
          RETURN p_payload;
        END
        $function$
        """
    )


def _install_inspection_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.inspect_inert_claim_proposal(
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
          v_binding_valid boolean := false;
          v_reason text;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'claim inspection requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          IF NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.claim_guard_activation
             WHERE singleton_id = 1
               AND activation_state = 'disabled'
               AND authority_mode = 'disabled'
               AND activation_epoch = 0
               AND executable_new_capacity_ceiling = 0
               AND live_claim_entry_enabled = false
          ) THEN
            RAISE EXCEPTION 'claim guard is not in its immutable disabled state'
              USING ERRCODE = '55000';
          END IF;

          SELECT EXISTS (
            SELECT 1
              FROM {SCHEMA}.attempt_lifecycle_events AS l
              JOIN {SCHEMA}.prepared_placement_allowances AS a
                ON a.allowance_id = l.allowance_id
               AND a.plan_id = l.plan_id
               AND a.protected_attempt_id = l.protected_attempt_id
               AND a.execution_generation = l.execution_generation
               AND a.requirements_digest = l.requirements_digest
               AND a.pool_id = l.pool_id
               AND a.shape_instance_id = l.shape_instance_id
               AND a.submission_intent_id = l.submission_intent_id
              JOIN {SCHEMA}.prepared_admission_plans AS p ON p.plan_id = a.plan_id
              JOIN {SCHEMA}.prepared_worker_shapes AS s
                ON s.plan_id = p.plan_id
               AND s.shape_instance_id = a.shape_instance_id
               AND s.submission_intent_id = a.submission_intent_id
               AND s.pool_id = a.pool_id
              JOIN {SCHEMA}.prepared_worker_bindings AS w
                ON w.plan_id = p.plan_id
               AND w.admission_incarnation = p.admission_incarnation
               AND w.manager_allocation_epoch = p.manager_allocation_epoch
               AND w.pool_id = a.pool_id
               AND w.shape_instance_id = a.shape_instance_id
               AND w.submission_intent_id = a.submission_intent_id
              JOIN {SCHEMA}.prepared_bootstrap_bindings AS b
                ON b.bootstrap_id = w.bootstrap_id
               AND b.plan_id = w.plan_id
               AND b.admission_incarnation = w.admission_incarnation
               AND b.manager_allocation_epoch = w.manager_allocation_epoch
               AND b.pool_id = w.pool_id
               AND b.shape_instance_id = w.shape_instance_id
               AND b.submission_intent_id = w.submission_intent_id
              JOIN {SCHEMA}.trial_attempts AS ta
                ON ta.protected_attempt_id = l.protected_attempt_id
               AND ta.execution_generation = l.execution_generation
               AND ta.requirements_digest = l.requirements_digest
              JOIN {SCHEMA}.trial_requirements AS r
                ON r.trial_id = ta.trial_id
               AND r.requirements_digest = ta.requirements_digest
              JOIN public.trials AS t ON t.id = ta.trial_id
             WHERE l.protected_attempt_id =
                   (p_payload->>'protected_attempt_id')::uuid
               AND l.transition_sequence =
                   (p_payload->>'expected_transition_sequence')::bigint
               AND l.transition_sequence = (
                   SELECT max(latest.transition_sequence)
                     FROM {SCHEMA}.attempt_lifecycle_events AS latest
                    WHERE latest.protected_attempt_id = l.protected_attempt_id
               )
               AND l.lifecycle_state = 'assigned'
               AND l.executable = false
               AND l.execution_generation =
                   (p_payload->>'execution_generation')::bigint
               AND l.requirements_digest = p_payload->>'requirements_digest'
               AND l.allowance_id = (p_payload->>'allowance_id')::uuid
               AND l.plan_id = (p_payload->>'plan_id')::uuid
               AND l.admission_incarnation =
                   (p_payload->>'admission_incarnation')::uuid
               AND l.manager_allocation_epoch =
                   (p_payload->>'manager_allocation_epoch')::bigint
               AND l.pool_id = p_payload->>'pool_id'
               AND l.shape_instance_id = p_payload->>'shape_instance_id'
               AND l.submission_intent_id =
                   (p_payload->>'submission_intent_id')::uuid
               AND a.allowance_state = 'prepared' AND a.executable = false
               AND p.agent_incarnation = p_agent_incarnation
               AND p.plan_state = 'prepared' AND p.executable = false
               AND p.lease_not_after > statement_timestamp()
               AND s.shape_state = 'prepared' AND s.executable = false
               AND w.worker_id = (p_payload->>'worker_id')::uuid
               AND w.worker_incarnation = (p_payload->>'worker_incarnation')::uuid
               AND w.bootstrap_id = (p_payload->>'bootstrap_id')::uuid
               AND w.claim_authorization_epoch = 0
               AND w.worker_state = 'prepared' AND w.executable = false
               AND b.bootstrap_state = 'registered' AND b.executable = false
               AND b.expires_at > statement_timestamp()
               AND ta.claim_state = 'queued'
               AND t.state = 'queued'
               AND t.cancellation_requested_at IS NULL
               AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= statement_timestamp())
               AND t.autoscaler_pool_name IS NULL
               AND (r.requirements->>'required_pool' IS NULL
                    OR r.requirements->>'required_pool' = l.pool_id)
               AND (s.payload->'worker_shape'->'capabilities')
                   ? ('os.' || (r.requirements->>'os'))
               AND ((r.requirements->>'cpu_arch') = 'any'
                    OR (s.payload->'worker_shape'->'capabilities')
                       ? ('cpu_arch.' || (r.requirements->>'cpu_arch')))
               AND (s.payload->'worker_shape'->'capabilities')
                   ? ('gpu_vendor.' || (r.requirements->>'gpu_vendor'))
               AND NOT EXISTS (
                 SELECT 1
                   FROM jsonb_array_elements_text(
                     r.requirements->'network_policies'
                   ) AS network(policy)
                  WHERE NOT (s.payload->'worker_shape'->'capabilities')
                    ? ('network.' || network.policy)
               )
          ) INTO v_binding_valid;
          v_reason := CASE
            WHEN v_binding_valid THEN 'activation-disabled'
            ELSE 'not-admitted'
          END;
          RETURN jsonb_build_object(
            'schema_version', 1,
            'proposal_id', p_payload->>'proposal_id',
            'agent_incarnation', p_agent_incarnation,
            'activation_state', 'disabled',
            'activation_epoch', 0,
            'executable_new_capacity_ceiling', 0,
            'admitted', false,
            'reason', v_reason,
            'claim_id', NULL,
            'concurrency_lease_id', NULL,
            'executable', false
          );
        END
        $function$
        """
    )


def upgrade() -> None:
    quoted_agent = _agent_role()
    op.create_unique_constraint(
        "guard_allowance_exact_lifecycle_key",
        "prepared_placement_allowances",
        [
            "allowance_id",
            "plan_id",
            "protected_attempt_id",
            "execution_generation",
            "requirements_digest",
            "pool_id",
            "shape_instance_id",
            "submission_intent_id",
        ],
        schema=SCHEMA,
    )
    op.create_table(
        "claim_guard_activation",
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("activation_state", sa.Text(), nullable=False),
        sa.Column("authority_mode", sa.Text(), nullable=False),
        sa.Column("activation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("executable_new_capacity_ceiling", sa.BigInteger(), nullable=False),
        sa.Column("live_claim_entry_enabled", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="guard_claim_activation_singleton_check"),
        sa.CheckConstraint(
            "activation_state = 'disabled' AND authority_mode = 'disabled' "
            "AND activation_epoch = 0 AND executable_new_capacity_ceiling = 0 "
            "AND live_claim_entry_enabled = false",
            name="guard_claim_activation_disabled_check",
        ),
        sa.PrimaryKeyConstraint("singleton_id"),
        schema=SCHEMA,
    )
    op.execute(
        f"INSERT INTO {SCHEMA}.claim_guard_activation "
        "(singleton_id, activation_state, authority_mode, activation_epoch, "
        "executable_new_capacity_ceiling, live_claim_entry_enabled) "
        "VALUES (1, 'disabled', 'disabled', 0, 0, false)"
    )
    op.create_table(
        "attempt_lifecycle_events",
        sa.Column("transition_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("execution_generation", sa.BigInteger(), nullable=False),
        sa.Column("requirements_digest", sa.Text(), nullable=False),
        sa.Column("transition_sequence", sa.BigInteger(), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("previous_state", sa.Text(), nullable=True),
        sa.Column("lifecycle_state", sa.Text(), nullable=False),
        sa.Column("allowance_id", sa.Uuid(), nullable=True),
        sa.Column("plan_id", sa.Uuid(), nullable=True),
        sa.Column("admission_incarnation", sa.Uuid(), nullable=True),
        sa.Column("manager_allocation_epoch", sa.BigInteger(), nullable=True),
        sa.Column("pool_id", sa.Text(), nullable=True),
        sa.Column("shape_instance_id", sa.Text(), nullable=True),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=True),
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
            "execution_generation > 0 AND transition_sequence >= 0",
            name="guard_lifecycle_sequences_check",
        ),
        sa.CheckConstraint(
            "requirements_digest ~ '^[0-9a-f]{64}$' "
            "AND payload_digest ~ '^[0-9a-f]{64}$'",
            name="guard_lifecycle_digests_check",
        ),
        sa.CheckConstraint(
            "operation IN ('initialize', 'assign', 'withdraw', 'cancel') "
            "AND lifecycle_state IN "
            "('pending-unassigned', 'assigned', 'cancelled-terminal') "
            "AND (previous_state IS NULL OR previous_state IN "
            "('pending-unassigned', 'assigned'))",
            name="guard_lifecycle_states_check",
        ),
        sa.CheckConstraint(
            "(operation = 'initialize' AND transition_sequence = 0 "
            " AND previous_state IS NULL AND lifecycle_state = 'pending-unassigned') OR "
            "(operation = 'assign' AND transition_sequence > 0 "
            " AND previous_state = 'pending-unassigned' AND lifecycle_state = 'assigned') OR "
            "(operation = 'withdraw' AND transition_sequence > 0 "
            " AND previous_state = 'assigned' AND lifecycle_state = 'pending-unassigned') OR "
            "(operation = 'cancel' AND transition_sequence > 0 "
            " AND previous_state IN ('pending-unassigned', 'assigned') "
            " AND lifecycle_state = 'cancelled-terminal')",
            name="guard_lifecycle_transition_check",
        ),
        sa.CheckConstraint(
            "((operation IN ('assign', 'withdraw') OR previous_state = 'assigned') "
            " AND allowance_id IS NOT NULL AND plan_id IS NOT NULL "
            " AND admission_incarnation IS NOT NULL "
            " AND manager_allocation_epoch > 0 AND pool_id IN ('oldlab', 'gb10') "
            " AND shape_instance_id IS NOT NULL AND submission_intent_id IS NOT NULL) OR "
            "((operation = 'initialize' OR "
            "  (operation = 'cancel' AND previous_state = 'pending-unassigned')) "
            " AND allowance_id IS NULL AND plan_id IS NULL "
            " AND admission_incarnation IS NULL AND manager_allocation_epoch IS NULL "
            " AND pool_id IS NULL AND shape_instance_id IS NULL "
            " AND submission_intent_id IS NULL)",
            name="guard_lifecycle_assignment_check",
        ),
        sa.CheckConstraint("executable = false", name="guard_lifecycle_inert_check"),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 8388608",
            name="guard_lifecycle_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id", "execution_generation", "requirements_digest"],
            [
                f"{SCHEMA}.trial_attempts.protected_attempt_id",
                f"{SCHEMA}.trial_attempts.execution_generation",
                f"{SCHEMA}.trial_attempts.requirements_digest",
            ],
            ondelete="RESTRICT",
            name="guard_lifecycle_attempt_binding_fk",
        ),
        sa.ForeignKeyConstraint(
            [
                "allowance_id",
                "plan_id",
                "protected_attempt_id",
                "execution_generation",
                "requirements_digest",
                "pool_id",
                "shape_instance_id",
                "submission_intent_id",
            ],
            [
                f"{SCHEMA}.prepared_placement_allowances.allowance_id",
                f"{SCHEMA}.prepared_placement_allowances.plan_id",
                f"{SCHEMA}.prepared_placement_allowances.protected_attempt_id",
                f"{SCHEMA}.prepared_placement_allowances.execution_generation",
                f"{SCHEMA}.prepared_placement_allowances.requirements_digest",
                f"{SCHEMA}.prepared_placement_allowances.pool_id",
                f"{SCHEMA}.prepared_placement_allowances.shape_instance_id",
                f"{SCHEMA}.prepared_placement_allowances.submission_intent_id",
            ],
            ondelete="RESTRICT",
            name="guard_lifecycle_allowance_binding_fk",
        ),
        sa.PrimaryKeyConstraint("transition_id"),
        sa.UniqueConstraint(
            "protected_attempt_id",
            "transition_sequence",
            name="guard_lifecycle_attempt_sequence_key",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "guard_lifecycle_allowance_consumed_key",
        "attempt_lifecycle_events",
        ["allowance_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("operation = 'assign'"),
    )
    op.create_table(
        "protected_claim_leases",
        sa.Column("concurrency_lease_id", sa.Uuid(), nullable=False),
        sa.Column("claim_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("execution_generation", sa.BigInteger(), nullable=False),
        sa.Column("requirements_digest", sa.Text(), nullable=False),
        sa.Column("worker_id", sa.Uuid(), nullable=False),
        sa.Column("worker_incarnation", sa.Uuid(), nullable=False),
        sa.Column("allowance_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("admission_incarnation", sa.Uuid(), nullable=False),
        sa.Column("claim_authorization_epoch", sa.BigInteger(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=False),
        sa.Column("lease_state", sa.Text(), nullable=False),
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
            "execution_generation > 0 AND claim_authorization_epoch > 0",
            name="guard_claim_lease_generations_check",
        ),
        sa.CheckConstraint(
            "lease_state IN ('live', 'completion-pending', 'cancel-pending', "
            "'retry-pending', 'unknown', 'terminal', 'infrastructure-lost')",
            name="guard_claim_lease_states_check",
        ),
        sa.CheckConstraint(
            "pool_id IN ('oldlab', 'gb10') AND executable = false",
            name="guard_claim_lease_inert_check",
        ),
        sa.CheckConstraint(
            "requirements_digest ~ '^[0-9a-f]{64}$' "
            "AND payload_digest ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(payload) = 'object' "
            "AND octet_length(payload::text) <= 8388608",
            name="guard_claim_lease_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id", "execution_generation", "requirements_digest"],
            [
                f"{SCHEMA}.trial_attempts.protected_attempt_id",
                f"{SCHEMA}.trial_attempts.execution_generation",
                f"{SCHEMA}.trial_attempts.requirements_digest",
            ],
            ondelete="RESTRICT",
            name="guard_claim_lease_attempt_binding_fk",
        ),
        sa.ForeignKeyConstraint(
            ["worker_id"],
            [f"{SCHEMA}.prepared_worker_bindings.worker_id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["allowance_id"],
            [f"{SCHEMA}.prepared_placement_allowances.allowance_id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("concurrency_lease_id"),
        sa.UniqueConstraint("claim_id", name="guard_claim_lease_claim_key"),
        sa.UniqueConstraint(
            "protected_attempt_id", name="guard_claim_lease_attempt_key"
        ),
        schema=SCHEMA,
    )
    _install_append_only_guards()
    _install_lifecycle_initializer()
    _install_transition_function()
    _install_inspection_function()
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.apply_inert_attempt_transition"
        f"(uuid, jsonb, bytea, text) TO {quoted_agent}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.inspect_inert_claim_proposal"
        f"(uuid, jsonb, bytea, text) TO {quoted_agent}"
    )


def downgrade() -> None:
    quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.inspect_inert_claim_proposal"
        f"(uuid, jsonb, bytea, text) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.inspect_inert_claim_proposal(uuid, jsonb, bytea, text)"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.apply_inert_attempt_transition"
        f"(uuid, jsonb, bytea, text) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.apply_inert_attempt_transition(uuid, jsonb, bytea, text)"
    )
    op.execute(f"DROP TRIGGER trial_attempts_initialize_lifecycle ON {SCHEMA}.trial_attempts")
    op.execute(f"DROP FUNCTION {SCHEMA}.initialize_attempt_lifecycle()")
    op.drop_table("protected_claim_leases", schema=SCHEMA)
    op.drop_table("attempt_lifecycle_events", schema=SCHEMA)
    op.drop_table("claim_guard_activation", schema=SCHEMA)
    op.drop_constraint(
        "guard_allowance_exact_lifecycle_key",
        "prepared_placement_allowances",
        schema=SCHEMA,
        type_="unique",
    )
