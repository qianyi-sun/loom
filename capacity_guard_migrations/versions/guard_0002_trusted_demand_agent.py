"""Trusted demand-agent identity and bounded observation capture.

Revision ID: guard_0002
Revises: guard_0001
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0002"
down_revision: str | Sequence[str] | None = "guard_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
APPEND_ONLY_TABLES = (
    "agent_runtime_authority",
    "agent_registrations",
    "demand_observations",
)


def _agent_role() -> tuple[str, str]:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected capacity migration is missing the validated agent role")
    quoted = op.get_bind().dialect.identifier_preparer.quote(role)
    return role, quoted


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


def _install_reporter_state_guard() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_reporter_high_water_transition()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'agent reporter state cannot be deleted or truncated'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.agent_incarnation IS DISTINCT FROM OLD.agent_incarnation
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR NEW.high_water <> OLD.high_water + 1 THEN
            RAISE EXCEPTION 'agent reporter high-water transition is invalid'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER agent_reporter_state_monotonic_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.agent_reporter_state
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.enforce_reporter_high_water_transition()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER agent_reporter_state_no_truncate
        BEFORE TRUNCATE ON {SCHEMA}.agent_reporter_state
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.enforce_reporter_high_water_transition()
        """
    )


def _install_capture_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.capture_demand_observation(
          p_agent_incarnation uuid,
          p_expected_high_water bigint,
          p_max_attempts integer
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_sequence bigint;
          v_attempt_count bigint;
          v_attempt_bytes bigint;
          v_attempts jsonb;
          v_payload jsonb;
          v_observed_at timestamptz := statement_timestamp();
          v_agent_role text;
        BEGIN
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'capacity demand caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'capacity demand agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF p_expected_high_water < 0 OR p_max_attempts < 1 OR p_max_attempts > 10000 THEN
            RAISE EXCEPTION 'capacity demand capture bounds are invalid'
              USING ERRCODE = '22023';
          END IF;

          SELECT r.* INTO v_registration
            FROM {SCHEMA}.agent_registrations AS r
            JOIN {SCHEMA}.authority_state AS f ON f.singleton_id = r.singleton_id
             AND f.environment_id = r.environment_id
             AND f.subject_id = r.subject_id
             AND f.subject_incarnation = r.subject_incarnation
             AND f.authority_incarnation = r.authority_incarnation
             AND f.reporter_incarnation = r.reporter_incarnation
             AND f.authority_mode = r.authority_mode
             AND f.allocation_epoch = r.allocation_epoch
             AND f.deployment_generation = r.deployment_generation
             AND f.configuration_generation = r.configuration_generation
             AND f.candidate_digest = r.candidate_digest
           WHERE r.agent_incarnation = p_agent_incarnation
             AND r.registration_state = 'registered'
           FOR KEY SHARE OF r, f;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'capacity demand agent incarnation is not registered'
              USING ERRCODE = '55000';
          END IF;

          IF EXISTS (
            SELECT 1
              FROM {SCHEMA}.trial_attempts AS a
              JOIN public.trials AS t ON t.id = a.trial_id
             WHERE a.claim_state = 'queued'
               AND t.state = 'queued'
               AND t.cancellation_requested_at IS NULL
               AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= v_observed_at)
               AND t.autoscaler_pool_name IS NOT NULL
          ) THEN
            RAISE EXCEPTION 'protected demand overlaps a legacy pool assignment'
              USING ERRCODE = '55000';
          END IF;

          SELECT count(*) INTO v_attempt_count
            FROM {SCHEMA}.trial_attempts AS a
            JOIN public.trials AS t ON t.id = a.trial_id
           WHERE a.claim_state = 'queued'
             AND t.state = 'queued'
             AND t.cancellation_requested_at IS NULL
             AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= v_observed_at);
          IF v_attempt_count > p_max_attempts THEN
            RAISE EXCEPTION 'protected demand exceeds the capture row bound'
              USING ERRCODE = '54000';
          END IF;

          -- Size each row without retaining the aggregate. This prevents a
          -- corrupt-but-bounded requirements row set from forcing jsonb_agg
          -- to allocate an observation that can only be rejected afterward.
          SELECT COALESCE(
                   sum(
                     octet_length(
                       jsonb_build_object(
                         'schema_version', 1,
                         'protected_attempt_id', a.protected_attempt_id,
                         'execution_generation', a.execution_generation,
                         'requirements', r.requirements,
                         'requirements_digest', a.requirements_digest,
                         'claim_state', a.claim_state,
                         'assigned_pool', a.assigned_pool,
                         'assignment_epoch', a.assignment_epoch,
                         'worker_id', a.worker_id,
                         'claim_epoch', a.claim_epoch,
                         'submit_priority', t.submit_priority,
                         'submitted_at', t.submitted_at
                       )::text
                     )
                   ),
                   0
                 ) + GREATEST(count(*) - 1, 0) + 2
            INTO v_attempt_bytes
            FROM {SCHEMA}.trial_attempts AS a
            JOIN {SCHEMA}.trial_requirements AS r
              ON r.trial_id = a.trial_id
             AND r.requirements_digest = a.requirements_digest
            JOIN public.trials AS t ON t.id = a.trial_id
           WHERE a.claim_state = 'queued'
             AND t.state = 'queued'
             AND t.cancellation_requested_at IS NULL
             AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= v_observed_at);
          IF v_attempt_bytes > 8372224 THEN
            RAISE EXCEPTION 'protected demand observation exceeds its pre-aggregation payload bound'
              USING ERRCODE = '54000';
          END IF;

          UPDATE {SCHEMA}.agent_reporter_state
             SET high_water = high_water + 1
           WHERE agent_incarnation = p_agent_incarnation
             AND high_water = p_expected_high_water
          RETURNING high_water INTO v_sequence;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'capacity demand reporter high-water compare-and-set failed'
              USING ERRCODE = '40001';
          END IF;

          SELECT COALESCE(
                   jsonb_agg(
                     jsonb_build_object(
                       'schema_version', 1,
                       'protected_attempt_id', a.protected_attempt_id,
                       'execution_generation', a.execution_generation,
                       'requirements', r.requirements,
                       'requirements_digest', a.requirements_digest,
                       'claim_state', a.claim_state,
                       'assigned_pool', a.assigned_pool,
                       'assignment_epoch', a.assignment_epoch,
                       'worker_id', a.worker_id,
                       'claim_epoch', a.claim_epoch,
                       'submit_priority', t.submit_priority,
                       'submitted_at', t.submitted_at
                     ) ORDER BY a.protected_attempt_id
                   ),
                   '[]'::jsonb
                 ) INTO v_attempts
            FROM {SCHEMA}.trial_attempts AS a
            JOIN {SCHEMA}.trial_requirements AS r
              ON r.trial_id = a.trial_id
             AND r.requirements_digest = a.requirements_digest
            JOIN public.trials AS t ON t.id = a.trial_id
           WHERE a.claim_state = 'queued'
             AND t.state = 'queued'
             AND t.cancellation_requested_at IS NULL
             AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= v_observed_at);

          v_payload := jsonb_build_object(
            'schema_version', 1,
            'environment_id', v_registration.environment_id,
            'subject_id', v_registration.subject_id,
            'subject_incarnation', v_registration.subject_incarnation,
            'authority_incarnation', v_registration.authority_incarnation,
            'agent_incarnation', v_registration.agent_incarnation,
            'reporter_incarnation', v_registration.reporter_incarnation,
            'authority_mode', v_registration.authority_mode,
            'allocation_epoch', v_registration.allocation_epoch,
            'reporter_high_water', 0,
            'candidate_digest', v_registration.candidate_digest,
            'deployment_generation', v_registration.deployment_generation,
            'configuration_generation', v_registration.configuration_generation,
            'sequence', v_sequence,
            'source_observed_at', v_observed_at,
            'view_state', 'complete',
            'attempts', v_attempts
          );
          IF octet_length(v_payload::text) > 8388608 THEN
            RAISE EXCEPTION 'protected demand observation exceeds its payload bound'
              USING ERRCODE = '54000';
          END IF;

          INSERT INTO {SCHEMA}.demand_observations
            (agent_incarnation, sequence, attempt_count, payload)
          VALUES
            (p_agent_incarnation, v_sequence, v_attempt_count, v_payload);
          RETURN v_payload;
        END
        $function$
        """
    )


def upgrade() -> None:
    agent_role, quoted_agent = _agent_role()
    op.create_table(
        "agent_runtime_authority",
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("agent_role_name", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="guard_agent_authority_singleton_check"),
        sa.CheckConstraint(
            "agent_role_name ~ '^[a-z][a-z0-9_]{0,62}$'",
            name="guard_agent_authority_role_check",
        ),
        sa.PrimaryKeyConstraint("singleton_id"),
        schema=SCHEMA,
    )
    op.get_bind().execute(
        sa.text(
            f"INSERT INTO {SCHEMA}.agent_runtime_authority "
            "(singleton_id, agent_role_name) VALUES (1, :agent_role)"
        ),
        {"agent_role": agent_role},
    )
    op.create_table(
        "agent_registrations",
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("environment_id", sa.Text(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("authority_incarnation", sa.Uuid(), nullable=False),
        sa.Column("reporter_incarnation", sa.Uuid(), nullable=False),
        sa.Column("authority_mode", sa.Text(), nullable=False),
        sa.Column("allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("candidate_digest", sa.Text(), nullable=False),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("configuration_generation", sa.BigInteger(), nullable=False),
        sa.Column("registration_state", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="guard_agent_registration_singleton_check"),
        sa.CheckConstraint("schema_version = 1", name="guard_agent_registration_schema_check"),
        sa.CheckConstraint(
            "environment_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_agent_registration_environment_check",
        ),
        sa.CheckConstraint(
            "authority_mode = 'disabled' AND allocation_epoch = 0",
            name="guard_agent_registration_disabled_check",
        ),
        sa.CheckConstraint(
            "candidate_digest ~ '^[0-9a-f]{64}$'",
            name="guard_agent_registration_candidate_check",
        ),
        sa.CheckConstraint(
            "deployment_generation > 0 AND configuration_generation > 0",
            name="guard_agent_registration_generation_check",
        ),
        sa.CheckConstraint(
            "registration_state = 'registered'",
            name="guard_agent_registration_state_check",
        ),
        sa.ForeignKeyConstraint(
            ["singleton_id"], [f"{SCHEMA}.authority_state.singleton_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("agent_incarnation"),
        sa.UniqueConstraint("singleton_id", name="guard_agent_single_registration_key"),
        sa.UniqueConstraint("reporter_incarnation", name="guard_agent_reporter_incarnation_key"),
        schema=SCHEMA,
    )
    op.create_table(
        "agent_reporter_state",
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("high_water", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("high_water >= 0", name="guard_agent_high_water_check"),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("agent_incarnation"),
        schema=SCHEMA,
    )
    op.create_table(
        "demand_observations",
        sa.Column("observation_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("sequence > 0", name="guard_agent_observation_sequence_check"),
        sa.CheckConstraint(
            "attempt_count >= 0 AND attempt_count <= 10000",
            name="guard_agent_observation_count_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 8388608",
            name="guard_agent_observation_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("observation_id"),
        sa.UniqueConstraint(
            "agent_incarnation", "sequence", name="guard_agent_observation_sequence_key"
        ),
        schema=SCHEMA,
    )
    _install_append_only_guards()
    _install_reporter_state_guard()
    _install_capture_function()
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {quoted_agent}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.capture_demand_observation(uuid, bigint, integer) "
        f"TO {quoted_agent}"
    )


def downgrade() -> None:
    _agent_role_name, quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.capture_demand_observation(uuid, bigint, integer) "
        f"FROM {quoted_agent}"
    )
    op.execute(f"REVOKE USAGE ON SCHEMA {SCHEMA} FROM {quoted_agent}")
    op.execute(f"DROP FUNCTION {SCHEMA}.capture_demand_observation(uuid, bigint, integer)")
    op.drop_table("demand_observations", schema=SCHEMA)
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_reporter_high_water_transition() CASCADE")
    op.drop_table("agent_reporter_state", schema=SCHEMA)
    op.drop_table("agent_registrations", schema=SCHEMA)
    op.drop_table("agent_runtime_authority", schema=SCHEMA)
