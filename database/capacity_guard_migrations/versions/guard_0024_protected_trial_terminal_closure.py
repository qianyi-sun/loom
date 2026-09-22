"""Close protected executable claims with their public trials.

Revision ID: guard_0024
Revises: guard_0023
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0024"
down_revision: str | Sequence[str] | None = "guard_0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = "close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)"
TRIGGER_FUNCTION = "public.loom_close_protected_runtime_trial_claim()"


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
            "protected terminal closure requires the application terminal trigger"
        )
    return bind.dialect.identifier_preparer.quote(owner)


def upgrade() -> None:
    quoted_trigger_owner = _trigger_function_owner()
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.close_protected_runtime_trial_claim(
          p_trial_id uuid,
          p_previous_state text,
          p_terminal_state text,
          p_worker_id uuid,
          p_attempt_count integer
        )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_current record;
          v_transition_id uuid := pg_catalog.gen_random_uuid();
          v_payload jsonb;
          v_payload_digest text;
        BEGIN
          IF p_previous_state NOT IN ('claimed', 'running')
             OR p_terminal_state NOT IN ('succeeded', 'failed', 'cancelled')
             OR p_worker_id IS NULL
             OR p_attempt_count < 1 THEN
            RAISE EXCEPTION 'protected terminal transition input is invalid'
              USING ERRCODE = '22023';
          END IF;

          -- Serialize this closure with manager lifecycle writes, worker
          -- release, and executable claim admission.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected terminal authority is unavailable'
              USING ERRCODE = '55000';
          END IF;

          SELECT attempt.protected_attempt_id,
                 attempt.execution_generation,
                 attempt.requirements_digest,
                 head.transition_sequence,
                 assignment.allowance_id,
                 assignment.plan_id,
                 assignment.admission_incarnation,
                 assignment.manager_allocation_epoch,
                 assignment.pool_id,
                 assignment.shape_instance_id,
                 assignment.submission_intent_id,
                 assignment.payload AS assignment_payload
            INTO v_current
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
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
            JOIN {SCHEMA}.executable_admission_events AS worker
              ON worker.event_kind = 'worker-registered'
             AND worker.intent_id = claim.intent_id
             AND worker.worker_id = claim.worker_id
             AND worker.worker_incarnation = claim.worker_incarnation
            JOIN public.trials AS trial ON trial.id = attempt.trial_id
           WHERE runtime.trial_id = p_trial_id
             AND trial.id = p_trial_id
             AND trial.state = p_terminal_state
             AND trial.worker_id = p_worker_id
             AND trial.attempt_count = p_attempt_count
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
             AND claim.worker_id = p_worker_id
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
           FOR KEY SHARE OF attempt, assignment, claim, worker, trial;
          IF NOT FOUND THEN
            IF EXISTS (
              SELECT 1
                FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
               WHERE runtime.trial_id = p_trial_id
            ) THEN
              RAISE EXCEPTION 'protected terminal transition is not exact'
                USING ERRCODE = '55000';
            END IF;
            RETURN;
          END IF;

          v_payload := v_current.assignment_payload || pg_catalog.jsonb_build_object(
            'transition_id', v_transition_id,
            'expected_transition_sequence', v_current.transition_sequence,
            'operation', 'cancel',
            'expected_state', 'assigned',
            'target_state', 'cancelled-terminal',
            'transition_reason', 'claimed-attempt-terminal'
          );
          v_payload_digest := pg_catalog.encode(
            pg_catalog.sha256(
              pg_catalog.convert_to(
                {SCHEMA}.canonical_executable_publication_payload(v_payload),
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
             false, v_payload, v_payload_digest);
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
    quoted_trigger_owner = _trigger_function_owner()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} FROM {quoted_trigger_owner}"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
