"""Keep protected crash requeues inside the guarded attempt lifecycle.

Revision ID: guard_0026
Revises: guard_0025
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0026"
down_revision: str | Sequence[str] | None = "guard_0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = (
    "transform_protected_runtime_trial_requeue"
    "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)"
)
TRIGGER_FUNCTION = "public.loom_transform_protected_runtime_trial_requeue()"


def _trigger_function_owner() -> str:
    bind = op.get_bind()
    owner = bind.execute(
        sa.text(
            "SELECT pg_catalog.pg_get_userbyid(routine.proowner) "
            "FROM pg_catalog.pg_proc AS routine "
            "WHERE routine.oid = pg_catalog.to_regprocedure(:signature)"
        ),
        {"signature": TRIGGER_FUNCTION},
    ).scalar_one_or_none()
    if not isinstance(owner, str) or not owner:
        raise RuntimeError(
            "protected requeue requires the application requeue trigger"
        )
    return bind.dialect.identifier_preparer.quote(owner)


def upgrade() -> None:
    quoted_trigger_owner = _trigger_function_owner()
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.transform_protected_runtime_trial_requeue(
          p_trial_id uuid,
          p_previous_state text,
          p_previous_worker_id uuid,
          p_previous_attempt_count integer,
          p_next_worker_id uuid,
          p_next_attempt_count integer,
          p_failure_reason text,
          p_failure_message text,
          p_next_attempt_at timestamptz
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_current record;
          v_transition_id uuid := pg_catalog.gen_random_uuid();
          v_transition_payload jsonb;
          v_transition_digest text;
          v_next_attempt_id uuid := pg_catalog.gen_random_uuid();
          v_failure_reason text;
        BEGIN
          IF p_trial_id IS NULL
             OR p_previous_state NOT IN ('claimed', 'running')
             OR p_previous_worker_id IS NULL
             OR p_previous_attempt_count < 1
             OR p_next_worker_id IS NOT NULL
             OR p_next_attempt_count IS DISTINCT FROM p_previous_attempt_count
             OR p_next_attempt_at IS NULL
             OR p_next_attempt_at < pg_catalog.statement_timestamp()
             OR p_next_attempt_at >
                  pg_catalog.statement_timestamp() + interval '1 hour' THEN
            RAISE EXCEPTION 'protected runtime requeue input is invalid'
              USING ERRCODE = '22023';
          END IF;

          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected requeue authority is unavailable'
              USING ERRCODE = '55000';
          END IF;

          SELECT runtime.trial_id, runtime.protected_attempt_id,
                 runtime.attempt_sequence, runtime.public_requires_caps,
                 runtime.public_requires_caps_canonical,
                 runtime.public_requires_caps_digest,
                 attempt.execution_generation, attempt.requirements_digest,
                 head.transition_sequence,
                 assignment.allowance_id, assignment.plan_id,
                 assignment.admission_incarnation,
                 assignment.manager_allocation_epoch, assignment.pool_id,
                 assignment.shape_instance_id, assignment.submission_intent_id,
                 assignment.payload AS assignment_payload,
                 readiness.public_requires_caps_digest AS ready_caps_digest,
                 readiness.task_image_prerequisites_digest,
                 readiness.model_switch_prerequisite_digest,
                 quota.max_attempts_ceiling
            INTO v_current
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.protected_runtime_trial_readiness AS readiness
              ON readiness.trial_id = runtime.trial_id
             AND readiness.protected_attempt_id = runtime.protected_attempt_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
             AND attempt.attempt_sequence = runtime.attempt_sequence
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_events AS assignment
              ON assignment.transition_id = head.transition_id
             AND assignment.protected_attempt_id = head.protected_attempt_id
            JOIN {SCHEMA}.executable_claim_leases AS claim
              ON claim.protected_attempt_id = attempt.protected_attempt_id
             AND claim.execution_generation = attempt.execution_generation
             AND claim.requirements_digest = attempt.requirements_digest
            JOIN {SCHEMA}.executable_claim_state AS claim_state
              ON claim_state.intent_id = claim.intent_id
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
            JOIN public.team_quotas AS quota ON quota.team_id = trial.team_id
           WHERE runtime.trial_id = p_trial_id
             AND runtime.public_attempt_count + 1 = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND trial.state = p_previous_state
             AND trial.worker_id = p_previous_worker_id
             AND trial.attempt_count = p_previous_attempt_count
             AND trial.cancellation_requested_at IS NULL
             AND attempt.claim_state = 'queued'
             AND head.lifecycle_state = 'assigned'
             AND head.executable = false
             AND assignment.operation = 'assign'
             AND assignment.previous_state = 'pending-unassigned'
             AND assignment.lifecycle_state = 'assigned'
             AND assignment.transition_sequence = head.transition_sequence
             AND assignment.execution_generation = attempt.execution_generation
             AND assignment.requirements_digest = attempt.requirements_digest
             AND assignment.executable = false
             AND assignment.submission_intent_id = claim.intent_id
             AND claim.worker_id = p_previous_worker_id
             AND claim.lease_state = 'live'
             AND claim.executable = true
             AND claim_state.subject_id = claim.subject_id
             AND claim_state.subject_incarnation = claim.subject_incarnation
             AND claim_state.claim_high_water >= claim.claim_high_water
             AND claim_state.terminal_high_water < claim_state.claim_high_water
             AND NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.executable_claim_terminal_events AS terminal
                WHERE terminal.admitted_operation_id = claim.operation_id
                   OR terminal.protected_attempt_id = claim.protected_attempt_id
             )
           FOR UPDATE OF head, claim_state
           FOR KEY SHARE OF runtime, readiness, attempt, assignment, claim,
                            trial, quota;
          IF NOT FOUND THEN
            IF EXISTS (
              SELECT 1
                FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
               WHERE runtime.trial_id = p_trial_id
            ) THEN
              RAISE EXCEPTION 'protected runtime requeue is not exact'
                USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
          END IF;

          v_failure_reason := COALESCE(p_failure_reason, 'worker_lost_claim');
          IF p_previous_attempt_count >= v_current.max_attempts_ceiling THEN
            RETURN pg_catalog.jsonb_build_object(
              'state', 'failed',
              'attempt_count', p_previous_attempt_count,
              'failure_reason', 'retry_exhausted',
              'failure_message', p_failure_message,
              'finished_at', pg_catalog.statement_timestamp()
            );
          END IF;

          v_transition_payload := v_current.assignment_payload
            || pg_catalog.jsonb_build_object(
              'transition_id', v_transition_id,
              'expected_transition_sequence', v_current.transition_sequence,
              'operation', 'cancel',
              'expected_state', 'assigned',
              'target_state', 'cancelled-terminal',
              'transition_reason', 'claimed-attempt-reclaimed'
            );
          v_transition_digest := pg_catalog.encode(
            pg_catalog.sha256(
              pg_catalog.convert_to(
                {SCHEMA}.canonical_executable_publication_payload(
                  v_transition_payload
                ),
                'UTF8'
              )
            ),
            'hex'
          );
          INSERT INTO {SCHEMA}.attempt_lifecycle_events
            (transition_id, protected_attempt_id, execution_generation,
             requirements_digest, transition_sequence, operation,
             previous_state, lifecycle_state, allowance_id, plan_id,
             admission_incarnation, manager_allocation_epoch, pool_id,
             shape_instance_id, submission_intent_id, executable,
             payload, payload_digest)
          VALUES
            (v_transition_id, v_current.protected_attempt_id,
             v_current.execution_generation, v_current.requirements_digest,
             v_current.transition_sequence + 1, 'cancel', 'assigned',
             'cancelled-terminal', v_current.allowance_id, v_current.plan_id,
             v_current.admission_incarnation,
             v_current.manager_allocation_epoch, v_current.pool_id,
             v_current.shape_instance_id, v_current.submission_intent_id,
             false, v_transition_payload, v_transition_digest);

          INSERT INTO {SCHEMA}.trial_attempts
            (protected_attempt_id, trial_id, execution_generation,
             attempt_sequence, requirements_digest, claim_state,
             assigned_pool, assignment_epoch, worker_id, claim_epoch)
          VALUES
            (v_next_attempt_id, v_current.trial_id,
             v_current.execution_generation + 1,
             v_current.attempt_sequence + 1, v_current.requirements_digest,
             'queued', NULL, NULL, NULL, NULL);
          INSERT INTO {SCHEMA}.protected_runtime_trial_submissions
            (trial_id, protected_attempt_id, attempt_sequence,
             public_attempt_count, not_before, public_requires_caps,
             public_requires_caps_canonical, public_requires_caps_digest)
          VALUES
            (v_current.trial_id, v_next_attempt_id,
             v_current.attempt_sequence + 1, p_previous_attempt_count,
             p_next_attempt_at, v_current.public_requires_caps,
             v_current.public_requires_caps_canonical,
             v_current.public_requires_caps_digest);
          INSERT INTO {SCHEMA}.protected_runtime_trial_readiness
            (trial_id, protected_attempt_id, public_requires_caps_digest,
             task_image_prerequisites_digest,
             model_switch_prerequisite_digest)
          VALUES
            (v_current.trial_id, v_next_attempt_id,
             v_current.ready_caps_digest,
             v_current.task_image_prerequisites_digest,
             v_current.model_switch_prerequisite_digest);

          RETURN pg_catalog.jsonb_build_object(
            'state', 'protected-pending',
            'attempt_count', p_previous_attempt_count,
            'failure_reason', v_failure_reason,
            'failure_message', p_failure_message,
            'next_attempt_at', p_next_attempt_at,
            'protected_attempt_id', v_next_attempt_id,
            'attempt_sequence', v_current.attempt_sequence + 1,
            'execution_generation', v_current.execution_generation + 1,
            'executable', false
          );
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quoted_trigger_owner}"
    )


def downgrade() -> None:
    bind = op.get_bind()
    active = bind.execute(
        sa.text(
            f"""
            SELECT EXISTS (
              SELECT 1
                FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
                JOIN {SCHEMA}.trial_attempts AS attempt
                  ON attempt.trial_id = runtime.trial_id
                 AND attempt.protected_attempt_id = runtime.protected_attempt_id
                JOIN {SCHEMA}.attempt_lifecycle_heads AS head
                  ON head.protected_attempt_id = attempt.protected_attempt_id
                JOIN public.trials AS trial ON trial.id = runtime.trial_id
               WHERE trial.state IN ('claimed', 'running')
                 AND head.lifecycle_state = 'assigned'
            )
            """
        )
    ).scalar_one()
    if active:
        raise RuntimeError(
            "cannot downgrade guard_0026 while protected claims can be requeued"
        )
    quoted_trigger_owner = _trigger_function_owner()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} FROM {quoted_trigger_owner}"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
