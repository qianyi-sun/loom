"""Lifecycle-aware protected demand projection.

Revision ID: guard_0006
Revises: guard_0005
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0006"
down_revision: str | Sequence[str] | None = "guard_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("lifecycle demand migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def _install_lifecycle_head_guards() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_attempt_lifecycle_head_transition()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF TG_OP = 'DELETE' OR pg_trigger_depth() < 2 THEN
            RAISE EXCEPTION
              'attempt lifecycle head can change only through an appended lifecycle event'
              USING ERRCODE = '55000';
          END IF;
          IF TG_OP = 'INSERT' THEN
            IF NEW.transition_sequence <> 0
               OR NOT EXISTS (
                 SELECT 1 FROM {SCHEMA}.attempt_lifecycle_events AS event
                  WHERE event.transition_id = NEW.transition_id
                    AND event.protected_attempt_id = NEW.protected_attempt_id
                    AND event.transition_sequence = NEW.transition_sequence
                    AND event.previous_state IS NULL
                    AND event.lifecycle_state = NEW.lifecycle_state
                    AND event.executable = NEW.executable
                    AND event.created_at = NEW.updated_at
               ) THEN
              RAISE EXCEPTION 'initial attempt lifecycle head is not exact'
                USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
          END IF;
          IF NEW.protected_attempt_id IS DISTINCT FROM OLD.protected_attempt_id
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR NEW.transition_sequence <> OLD.transition_sequence + 1
             OR NEW.updated_at < OLD.updated_at
             OR NOT EXISTS (
               SELECT 1 FROM {SCHEMA}.attempt_lifecycle_events AS event
                WHERE event.transition_id = NEW.transition_id
                  AND event.protected_attempt_id = NEW.protected_attempt_id
                  AND event.transition_sequence = NEW.transition_sequence
                  AND event.previous_state = OLD.lifecycle_state
                  AND event.lifecycle_state = NEW.lifecycle_state
                  AND event.executable = NEW.executable
                  AND event.created_at = NEW.updated_at
             ) THEN
            RAISE EXCEPTION 'attempt lifecycle head transition is not exact and monotonic'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER attempt_lifecycle_heads_monotonic_row
        BEFORE INSERT OR UPDATE OR DELETE ON {SCHEMA}.attempt_lifecycle_heads
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.enforce_attempt_lifecycle_head_transition()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER attempt_lifecycle_heads_no_truncate
        BEFORE TRUNCATE ON {SCHEMA}.attempt_lifecycle_heads
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_attempt_lifecycle_projection_blocker()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF TG_OP = 'DELETE'
             OR pg_trigger_depth() < 2
             OR NEW.transition_id IS DISTINCT FROM OLD.transition_id
             OR NEW.protected_attempt_id IS DISTINCT FROM OLD.protected_attempt_id
             OR NEW.blocker_reason IS DISTINCT FROM OLD.blocker_reason
             OR NEW.executable IS DISTINCT FROM OLD.executable
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR OLD.resolved_at IS NOT NULL
             OR NEW.resolved_at IS NULL
             OR NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.attempt_lifecycle_projection_resolutions AS resolution
                WHERE resolution.transition_id = NEW.transition_id
                  AND resolution.protected_attempt_id = NEW.protected_attempt_id
                  AND resolution.resolved_at = NEW.resolved_at
             ) THEN
            RAISE EXCEPTION
              'lifecycle projection blocker can resolve only through appended evidence'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER attempt_lifecycle_projection_blockers_monotonic_row
        BEFORE UPDATE OR DELETE
        ON {SCHEMA}.attempt_lifecycle_projection_blockers
        FOR EACH ROW EXECUTE FUNCTION
          {SCHEMA}.enforce_attempt_lifecycle_projection_blocker()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER attempt_lifecycle_projection_resolutions_append_only_row
        BEFORE UPDATE OR DELETE
        ON {SCHEMA}.attempt_lifecycle_projection_resolutions
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    for table in (
        "attempt_lifecycle_projection_blockers",
        "attempt_lifecycle_projection_resolutions",
    ):
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_truncate
            BEFORE TRUNCATE ON {SCHEMA}.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.project_attempt_lifecycle_projection_resolution()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          UPDATE {SCHEMA}.attempt_lifecycle_projection_blockers
             SET resolved_at = NEW.resolved_at
           WHERE transition_id = NEW.transition_id
             AND protected_attempt_id = NEW.protected_attempt_id
             AND resolved_at IS NULL;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'lifecycle projection resolution has no unresolved blocker'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER attempt_lifecycle_projection_resolutions_project_blocker
        AFTER INSERT ON {SCHEMA}.attempt_lifecycle_projection_resolutions
        FOR EACH ROW EXECUTE FUNCTION
          {SCHEMA}.project_attempt_lifecycle_projection_resolution()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.project_attempt_lifecycle_head()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          UPDATE {SCHEMA}.attempt_lifecycle_heads
             SET transition_id = NEW.transition_id,
                 transition_sequence = NEW.transition_sequence,
                 lifecycle_state = NEW.lifecycle_state,
                 executable = NEW.executable,
                 updated_at = NEW.created_at
           WHERE protected_attempt_id = NEW.protected_attempt_id;
          IF NOT FOUND THEN
            INSERT INTO {SCHEMA}.attempt_lifecycle_heads
              (protected_attempt_id, transition_id, transition_sequence,
               lifecycle_state, executable, created_at, updated_at)
            VALUES
              (NEW.protected_attempt_id, NEW.transition_id, NEW.transition_sequence,
               NEW.lifecycle_state, NEW.executable, NEW.created_at, NEW.created_at);
          END IF;
          IF NEW.lifecycle_state = 'cancelled-terminal'
             AND EXISTS (
               SELECT 1
                 FROM {SCHEMA}.trial_attempts AS attempt
                 JOIN public.trials AS trial ON trial.id = attempt.trial_id
                WHERE attempt.protected_attempt_id = NEW.protected_attempt_id
                  AND trial.state = 'queued'
                  AND trial.cancellation_requested_at IS NULL
             ) THEN
            INSERT INTO {SCHEMA}.attempt_lifecycle_projection_blockers
              (transition_id, protected_attempt_id, blocker_reason, executable,
               created_at)
            VALUES
              (NEW.transition_id, NEW.protected_attempt_id,
               'terminal-public-queued', false, NEW.created_at);
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER attempt_lifecycle_events_project_head
        AFTER INSERT ON {SCHEMA}.attempt_lifecycle_events
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.project_attempt_lifecycle_head()
        """
    )


def _install_legacy_capture_guard() -> None:
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
        BEGIN
          -- Every disconnected lifecycle mutation serializes on this row. Hold
          -- the same lock through the legacy capture so assignment cannot race
          -- the v1 safety decision.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF EXISTS (
            SELECT 1
              FROM {SCHEMA}.trial_attempts AS a
              JOIN {SCHEMA}.attempt_lifecycle_heads AS current
                ON current.protected_attempt_id = a.protected_attempt_id
             WHERE current.lifecycle_state <> 'pending-unassigned'
          ) THEN
            RAISE EXCEPTION
              'legacy demand capture cannot represent the protected lifecycle state'
              USING ERRCODE = '55000';
          END IF;
          RETURN {SCHEMA}.capture_demand_observation_v1_legacy(
            p_agent_incarnation, p_expected_high_water, p_max_attempts
          );
        END
        $function$
        """
    )


def _install_lifecycle_capture() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.capture_lifecycle_demand_observation(
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
          v_source_count integer := 0;
          v_attempt_count integer := 0;
          v_attempt_bytes bigint := 2;
          v_attempt_items jsonb[] := ARRAY[]::jsonb[];
          v_attempts jsonb;
          v_item jsonb;
          v_payload jsonb;
          v_row record;
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
          IF p_expected_high_water IS NULL OR p_expected_high_water < 0
             OR p_max_attempts IS NULL
             OR p_max_attempts < 1 OR p_max_attempts > 10000 THEN
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

          -- Plans and lifecycle transitions use this same row as their writer
          -- mutex. The loop query therefore observes one stable protected view.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          -- A terminal transition observed while the public row was still
          -- runnable creates one append-only blocker. Resolve it only after a
          -- later public projection supplies durable non-runnable evidence.
          INSERT INTO {SCHEMA}.attempt_lifecycle_projection_resolutions
            (transition_id, protected_attempt_id, public_state,
             cancellation_requested_at, executable, resolved_at)
          SELECT blocker.transition_id, blocker.protected_attempt_id,
                 trial.state, trial.cancellation_requested_at, false, v_observed_at
            FROM {SCHEMA}.attempt_lifecycle_projection_blockers AS blocker
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.protected_attempt_id = blocker.protected_attempt_id
            JOIN public.trials AS trial ON trial.id = attempt.trial_id
           WHERE blocker.resolved_at IS NULL
             AND (trial.state <> 'queued'
                  OR trial.cancellation_requested_at IS NOT NULL)
          ON CONFLICT (transition_id) DO NOTHING;
          IF EXISTS (
            SELECT 1
              FROM {SCHEMA}.attempt_lifecycle_projection_blockers AS blocker
             WHERE blocker.resolved_at IS NULL
          ) THEN
            RAISE EXCEPTION
              'terminal protected lifecycle contradicts queued public state'
              USING ERRCODE = '55000';
          END IF;

          FOR v_row IN
            SELECT
              a.protected_attempt_id,
              a.execution_generation,
              a.requirements_digest,
              r.requirements,
              t.state AS public_state,
              t.cancellation_requested_at,
              t.next_attempt_at,
              t.autoscaler_pool_name,
              t.submit_priority,
              t.submitted_at,
              l.transition_sequence,
              l.lifecycle_state,
              l.allowance_id,
              l.plan_id,
              l.admission_incarnation,
              l.manager_allocation_epoch,
              l.pool_id,
              l.shape_instance_id,
              l.submission_intent_id,
              l.executable AS lifecycle_executable,
              allowance.allowance_id AS bound_allowance_id,
              plan.plan_id AS bound_plan_id,
              plan.agent_incarnation AS plan_agent_incarnation,
              plan.pool_generation,
              plan.profile_id,
              plan.profile_generation,
              plan.profile_digest,
              plan.plan_state,
              plan.executable AS plan_executable,
              shape.plan_id AS bound_shape_plan_id,
              shape.shape_state,
              shape.executable AS shape_executable,
              shape.payload AS shape_payload
            FROM {SCHEMA}.attempt_lifecycle_heads AS head
            JOIN {SCHEMA}.attempt_lifecycle_events AS l
              ON l.transition_id = head.transition_id
             AND l.protected_attempt_id = head.protected_attempt_id
             AND l.transition_sequence = head.transition_sequence
             AND l.lifecycle_state = head.lifecycle_state
             AND l.executable = head.executable
            JOIN {SCHEMA}.trial_attempts AS a
              ON a.protected_attempt_id = head.protected_attempt_id
            JOIN {SCHEMA}.trial_requirements AS r
              ON r.trial_id = a.trial_id
             AND r.requirements_digest = a.requirements_digest
            JOIN public.trials AS t ON t.id = a.trial_id
            LEFT JOIN {SCHEMA}.prepared_placement_allowances AS allowance
              ON allowance.allowance_id = l.allowance_id
             AND allowance.plan_id = l.plan_id
             AND allowance.protected_attempt_id = l.protected_attempt_id
             AND allowance.execution_generation = l.execution_generation
             AND allowance.requirements_digest = l.requirements_digest
             AND allowance.pool_id = l.pool_id
             AND allowance.shape_instance_id = l.shape_instance_id
             AND allowance.submission_intent_id = l.submission_intent_id
             AND allowance.allowance_state = 'prepared'
             AND allowance.executable = false
            LEFT JOIN {SCHEMA}.prepared_admission_plans AS plan
              ON plan.plan_id = l.plan_id
             AND plan.admission_incarnation = l.admission_incarnation
             AND plan.manager_allocation_epoch = l.manager_allocation_epoch
             AND plan.pool_id = l.pool_id
            LEFT JOIN {SCHEMA}.prepared_worker_shapes AS shape
              ON shape.plan_id = l.plan_id
             AND shape.shape_instance_id = l.shape_instance_id
             AND shape.submission_intent_id = l.submission_intent_id
             AND shape.pool_id = l.pool_id
            WHERE head.lifecycle_state IN ('pending-unassigned', 'assigned')
            ORDER BY head.protected_attempt_id
            LIMIT (p_max_attempts + 1)
          LOOP
            IF v_row.lifecycle_state IS NULL THEN
              RAISE EXCEPTION 'protected attempt has no lifecycle projection'
                USING ERRCODE = '55000';
            END IF;
            IF v_row.lifecycle_executable IS DISTINCT FROM false THEN
              RAISE EXCEPTION 'protected lifecycle projection became executable'
                USING ERRCODE = '55000';
            END IF;
            v_source_count := v_source_count + 1;
            IF v_source_count > p_max_attempts THEN
              RAISE EXCEPTION 'protected demand exceeds the capture source row bound'
                USING ERRCODE = '54000';
            END IF;

            IF v_row.lifecycle_state NOT IN ('pending-unassigned', 'assigned') THEN
              RAISE EXCEPTION 'protected lifecycle state is unsupported by demand projection'
                USING ERRCODE = '55000';
            END IF;
            IF v_row.public_state <> 'queued'
               OR v_row.cancellation_requested_at IS NOT NULL
               OR v_row.autoscaler_pool_name IS NOT NULL THEN
              RAISE EXCEPTION 'protected lifecycle contradicts public demand state'
                USING ERRCODE = '55000';
            END IF;
            IF v_row.lifecycle_state = 'assigned'
               AND v_row.next_attempt_at IS NOT NULL
               AND v_row.next_attempt_at > v_observed_at THEN
              RAISE EXCEPTION 'assigned protected lifecycle is deferred in public state'
                USING ERRCODE = '55000';
            END IF;
            IF v_row.lifecycle_state = 'pending-unassigned'
               AND v_row.next_attempt_at IS NOT NULL
               AND v_row.next_attempt_at > v_observed_at THEN
              CONTINUE;
            END IF;

            IF v_row.lifecycle_state = 'assigned' THEN
              IF v_row.bound_allowance_id IS NULL
                 OR v_row.bound_plan_id IS NULL
                 OR v_row.bound_shape_plan_id IS NULL
                 OR v_row.plan_agent_incarnation IS DISTINCT FROM p_agent_incarnation
                 OR v_row.plan_state IS DISTINCT FROM 'prepared'
                 OR v_row.plan_executable IS DISTINCT FROM false
                 OR v_row.shape_state IS DISTINCT FROM 'prepared'
                 OR v_row.shape_executable IS DISTINCT FROM false
                 OR jsonb_typeof(v_row.shape_payload->'worker_shape')
                    IS DISTINCT FROM 'object'
                 OR v_row.shape_payload->'worker_shape'->>'shape_id' IS NULL
                 OR v_row.requirements->>'required_pool' IS NOT NULL
                    AND v_row.requirements->>'required_pool' <> v_row.pool_id
                 OR NOT (v_row.shape_payload->'worker_shape'->'capabilities'
                         ? ('os.' || (v_row.requirements->>'os')))
                 OR ((v_row.requirements->>'cpu_arch') <> 'any'
                     AND NOT (v_row.shape_payload->'worker_shape'->'capabilities'
                              ? ('cpu_arch.' || (v_row.requirements->>'cpu_arch'))))
                 OR NOT (v_row.shape_payload->'worker_shape'->'capabilities'
                         ? ('gpu_vendor.' || (v_row.requirements->>'gpu_vendor')))
                 OR EXISTS (
                   SELECT 1
                     FROM jsonb_array_elements_text(
                       v_row.requirements->'network_policies'
                     ) AS network(policy)
                    WHERE NOT (v_row.shape_payload->'worker_shape'->'capabilities'
                               ? ('network.' || network.policy))
                 ) THEN
                RAISE EXCEPTION
                  'assigned protected lifecycle has an invalid prepared binding'
                  USING ERRCODE = '55000';
              END IF;
            END IF;

            v_attempt_count := v_attempt_count + 1;
            IF v_attempt_count > p_max_attempts THEN
              RAISE EXCEPTION 'protected demand exceeds the capture row bound'
                USING ERRCODE = '54000';
            END IF;
            v_item := jsonb_build_object(
              'schema_version', 1,
              'view_version', 2,
              'protected_attempt_id', v_row.protected_attempt_id,
              'execution_generation', v_row.execution_generation,
              'requirements', v_row.requirements,
              'requirements_digest', v_row.requirements_digest,
              'lifecycle_sequence', v_row.transition_sequence,
              'lifecycle_state', v_row.lifecycle_state,
              'allowance_id', CASE WHEN v_row.lifecycle_state = 'assigned'
                                   THEN v_row.allowance_id ELSE NULL END,
              'plan_id', CASE WHEN v_row.lifecycle_state = 'assigned'
                              THEN v_row.plan_id ELSE NULL END,
              'admission_incarnation', CASE WHEN v_row.lifecycle_state = 'assigned'
                                            THEN v_row.admission_incarnation ELSE NULL END,
              'manager_allocation_epoch', CASE WHEN v_row.lifecycle_state = 'assigned'
                                               THEN v_row.manager_allocation_epoch ELSE NULL END,
              'pool_id', CASE WHEN v_row.lifecycle_state = 'assigned'
                              THEN v_row.pool_id ELSE NULL END,
              'pool_generation', CASE WHEN v_row.lifecycle_state = 'assigned'
                                      THEN v_row.pool_generation ELSE NULL END,
              'profile_id', CASE WHEN v_row.lifecycle_state = 'assigned'
                                 THEN v_row.profile_id ELSE NULL END,
              'profile_generation', CASE WHEN v_row.lifecycle_state = 'assigned'
                                         THEN v_row.profile_generation ELSE NULL END,
              'profile_digest', CASE WHEN v_row.lifecycle_state = 'assigned'
                                     THEN v_row.profile_digest ELSE NULL END,
              'shape_id', CASE WHEN v_row.lifecycle_state = 'assigned'
                               THEN v_row.shape_payload->'worker_shape'->>'shape_id'
                               ELSE NULL END,
              'shape_instance_id', CASE WHEN v_row.lifecycle_state = 'assigned'
                                        THEN v_row.shape_instance_id ELSE NULL END,
              'submission_intent_id', CASE WHEN v_row.lifecycle_state = 'assigned'
                                           THEN v_row.submission_intent_id ELSE NULL END,
              'submit_priority', v_row.submit_priority,
              'submitted_at', v_row.submitted_at,
              'executable', false
            );
            v_attempt_bytes := v_attempt_bytes + octet_length(v_item::text)
              + CASE WHEN v_attempt_count > 1 THEN 1 ELSE 0 END;
            IF v_attempt_bytes > 8372224 THEN
              RAISE EXCEPTION
                'protected demand observation exceeds its pre-aggregation payload bound'
                USING ERRCODE = '54000';
            END IF;
            v_attempt_items := array_append(v_attempt_items, v_item);
          END LOOP;

          v_attempts := to_jsonb(v_attempt_items);
          UPDATE {SCHEMA}.agent_reporter_state
             SET high_water = high_water + 1
           WHERE agent_incarnation = p_agent_incarnation
             AND high_water = p_expected_high_water
          RETURNING high_water INTO v_sequence;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'capacity demand reporter high-water compare-and-set failed'
              USING ERRCODE = '40001';
          END IF;

          v_payload := jsonb_build_object(
            'schema_version', 1,
            'view_version', 2,
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
            'attempts', v_attempts,
            'executable', false
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
    quoted_agent = _agent_role()
    op.create_table(
        "attempt_lifecycle_heads",
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("transition_id", sa.Uuid(), nullable=False),
        sa.Column("transition_sequence", sa.BigInteger(), nullable=False),
        sa.Column("lifecycle_state", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "transition_sequence >= 0",
            name="guard_lifecycle_head_sequence_check",
        ),
        sa.CheckConstraint(
            "lifecycle_state IN "
            "('pending-unassigned', 'assigned', 'cancelled-terminal')",
            name="guard_lifecycle_head_state_check",
        ),
        sa.CheckConstraint(
            "executable = false",
            name="guard_lifecycle_head_inert_check",
        ),
        sa.ForeignKeyConstraint(
            ["transition_id"],
            [f"{SCHEMA}.attempt_lifecycle_events.transition_id"],
            ondelete="RESTRICT",
            name="guard_lifecycle_head_event_fk",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id"],
            [f"{SCHEMA}.trial_attempts.protected_attempt_id"],
            ondelete="RESTRICT",
            name="guard_lifecycle_head_attempt_fk",
        ),
        sa.PrimaryKeyConstraint("protected_attempt_id"),
        sa.UniqueConstraint("transition_id", name="guard_lifecycle_head_transition_key"),
        schema=SCHEMA,
    )
    op.create_index(
        "guard_lifecycle_head_state_key",
        "attempt_lifecycle_heads",
        ["lifecycle_state", "protected_attempt_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "guard_lifecycle_current_demand_key",
        "attempt_lifecycle_heads",
        ["protected_attempt_id"],
        schema=SCHEMA,
        postgresql_where=sa.text(
            "lifecycle_state IN ('pending-unassigned', 'assigned')"
        ),
    )
    op.create_table(
        "attempt_lifecycle_projection_blockers",
        sa.Column("transition_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("blocker_reason", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "resolved_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "blocker_reason = 'terminal-public-queued' AND executable = false",
            name="guard_lifecycle_projection_blocker_inert_check",
        ),
        sa.CheckConstraint(
            "resolved_at IS NULL OR resolved_at >= created_at",
            name="guard_lifecycle_projection_blocker_resolution_time_check",
        ),
        sa.ForeignKeyConstraint(
            ["transition_id"],
            [f"{SCHEMA}.attempt_lifecycle_events.transition_id"],
            ondelete="RESTRICT",
            name="guard_lifecycle_projection_blocker_event_fk",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id"],
            [f"{SCHEMA}.attempt_lifecycle_heads.protected_attempt_id"],
            ondelete="RESTRICT",
            name="guard_lifecycle_projection_blocker_head_fk",
        ),
        sa.PrimaryKeyConstraint("transition_id"),
        sa.UniqueConstraint(
            "protected_attempt_id",
            name="guard_lifecycle_projection_blocker_attempt_key",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "guard_lifecycle_projection_unresolved_blocker_key",
        "attempt_lifecycle_projection_blockers",
        ["transition_id"],
        schema=SCHEMA,
        postgresql_where=sa.text("resolved_at IS NULL"),
    )
    op.create_table(
        "attempt_lifecycle_projection_resolutions",
        sa.Column("transition_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("public_state", sa.Text(), nullable=False),
        sa.Column(
            "cancellation_requested_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column(
            "resolved_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(public_state <> 'queued' OR cancellation_requested_at IS NOT NULL) "
            "AND executable = false",
            name="guard_lifecycle_projection_resolution_inert_check",
        ),
        sa.ForeignKeyConstraint(
            ["transition_id"],
            [f"{SCHEMA}.attempt_lifecycle_projection_blockers.transition_id"],
            ondelete="RESTRICT",
            name="guard_lifecycle_projection_resolution_blocker_fk",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id"],
            [f"{SCHEMA}.attempt_lifecycle_heads.protected_attempt_id"],
            ondelete="RESTRICT",
            name="guard_lifecycle_projection_resolution_head_fk",
        ),
        sa.PrimaryKeyConstraint("transition_id"),
        sa.UniqueConstraint(
            "protected_attempt_id",
            name="guard_lifecycle_projection_resolution_attempt_key",
        ),
        schema=SCHEMA,
    )
    op.execute(
        f"""
        INSERT INTO {SCHEMA}.attempt_lifecycle_heads
          (protected_attempt_id, transition_id, transition_sequence,
           lifecycle_state, executable, created_at, updated_at)
        SELECT DISTINCT ON (event.protected_attempt_id)
          event.protected_attempt_id, event.transition_id,
          event.transition_sequence, event.lifecycle_state, event.executable,
          event.created_at, event.created_at
          FROM {SCHEMA}.attempt_lifecycle_events AS event
         ORDER BY event.protected_attempt_id, event.transition_sequence DESC
        """
    )
    op.execute(
        f"""
        INSERT INTO {SCHEMA}.attempt_lifecycle_projection_blockers
          (transition_id, protected_attempt_id, blocker_reason, executable,
           created_at)
        SELECT head.transition_id, head.protected_attempt_id,
               'terminal-public-queued', false, head.updated_at
          FROM {SCHEMA}.attempt_lifecycle_heads AS head
          JOIN {SCHEMA}.trial_attempts AS attempt
            ON attempt.protected_attempt_id = head.protected_attempt_id
          JOIN public.trials AS trial ON trial.id = attempt.trial_id
         WHERE head.lifecycle_state = 'cancelled-terminal'
           AND trial.state = 'queued'
           AND trial.cancellation_requested_at IS NULL
        """
    )
    op.create_foreign_key(
        "guard_attempt_current_lifecycle_fk",
        "trial_attempts",
        "attempt_lifecycle_heads",
        ["protected_attempt_id"],
        ["protected_attempt_id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    _install_lifecycle_head_guards()
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.capture_demand_observation(uuid, bigint, integer) "
        "RENAME TO capture_demand_observation_v1_legacy"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.capture_demand_observation_v1_legacy"
        f"(uuid, bigint, integer) FROM {quoted_agent}"
    )
    _install_legacy_capture_guard()
    _install_lifecycle_capture()
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.capture_demand_observation"
        "(uuid, bigint, integer) FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        "(uuid, bigint, integer) FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.capture_demand_observation_v1_legacy"
        "(uuid, bigint, integer) FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION "
        f"{SCHEMA}.enforce_attempt_lifecycle_head_transition() FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION "
        f"{SCHEMA}.project_attempt_lifecycle_head() FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION "
        f"{SCHEMA}.enforce_attempt_lifecycle_projection_blocker() FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION "
        f"{SCHEMA}.project_attempt_lifecycle_projection_resolution() FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.capture_demand_observation"
        f"(uuid, bigint, integer) TO {quoted_agent}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        f"(uuid, bigint, integer) TO {quoted_agent}"
    )


def downgrade() -> None:
    quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        f"(uuid, bigint, integer) FROM {quoted_agent}"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.capture_demand_observation"
        f"(uuid, bigint, integer) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.capture_lifecycle_demand_observation(uuid, bigint, integer)"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.capture_demand_observation(uuid, bigint, integer)")
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.capture_demand_observation_v1_legacy"
        "(uuid, bigint, integer) RENAME TO capture_demand_observation"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.capture_demand_observation"
        f"(uuid, bigint, integer) TO {quoted_agent}"
    )
    op.execute(
        f"DROP TRIGGER attempt_lifecycle_events_project_head "
        f"ON {SCHEMA}.attempt_lifecycle_events"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.project_attempt_lifecycle_head()")
    op.execute(
        f"DROP TRIGGER attempt_lifecycle_projection_resolutions_project_blocker "
        f"ON {SCHEMA}.attempt_lifecycle_projection_resolutions"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.project_attempt_lifecycle_projection_resolution()")
    op.drop_constraint(
        "guard_attempt_current_lifecycle_fk",
        "trial_attempts",
        schema=SCHEMA,
        type_="foreignkey",
    )
    op.drop_table("attempt_lifecycle_projection_resolutions", schema=SCHEMA)
    op.drop_table("attempt_lifecycle_projection_blockers", schema=SCHEMA)
    op.drop_table("attempt_lifecycle_heads", schema=SCHEMA)
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_attempt_lifecycle_projection_blocker()")
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_attempt_lifecycle_head_transition()")
