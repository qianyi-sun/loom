"""Lifecycle-aware protected demand projection.

Revision ID: guard_0006
Revises: guard_0005
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

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
              LEFT JOIN LATERAL (
                SELECT latest.lifecycle_state
                  FROM {SCHEMA}.attempt_lifecycle_events AS latest
                 WHERE latest.protected_attempt_id = a.protected_attempt_id
                 ORDER BY latest.transition_sequence DESC
                 LIMIT 1
              ) AS current ON true
             WHERE current.lifecycle_state IS NULL
                OR current.lifecycle_state <> 'pending-unassigned'
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
            FROM {SCHEMA}.trial_attempts AS a
            JOIN {SCHEMA}.trial_requirements AS r
              ON r.trial_id = a.trial_id
             AND r.requirements_digest = a.requirements_digest
            JOIN public.trials AS t ON t.id = a.trial_id
            LEFT JOIN LATERAL (
              SELECT latest.*
                FROM {SCHEMA}.attempt_lifecycle_events AS latest
               WHERE latest.protected_attempt_id = a.protected_attempt_id
               ORDER BY latest.transition_sequence DESC
               LIMIT 1
            ) AS l ON true
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
            ORDER BY a.protected_attempt_id
          LOOP
            IF v_row.lifecycle_state IS NULL THEN
              RAISE EXCEPTION 'protected attempt has no lifecycle projection'
                USING ERRCODE = '55000';
            END IF;
            IF v_row.lifecycle_executable IS DISTINCT FROM false THEN
              RAISE EXCEPTION 'protected lifecycle projection became executable'
                USING ERRCODE = '55000';
            END IF;

            IF v_row.lifecycle_state = 'cancelled-terminal' THEN
              IF v_row.public_state = 'queued'
                 AND v_row.cancellation_requested_at IS NULL THEN
                RAISE EXCEPTION
                  'terminal protected lifecycle contradicts queued public state'
                  USING ERRCODE = '55000';
              END IF;
              CONTINUE;
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
