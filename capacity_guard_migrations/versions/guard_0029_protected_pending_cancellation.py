"""Cancel protected pending trials and refresh the sealed mutation inventory.

Revision ID: guard_0029
Revises: guard_0028
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0029"
down_revision: str | Sequence[str] | None = "guard_0028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
CANCEL_FUNCTION = "cancel_protected_runtime_pending_trial(uuid,uuid)"
VALIDATION_FUNCTION = "current_protected_runtime_registration()"
OLD_MUTATION_INVENTORY_DIGEST = "607f561f4af380cb1452995512e2ec4ab32490c29552b7a6618a3f76fea168a4"
NEW_MUTATION_INVENTORY_DIGEST = "b9ec5d44880251d00237463a9f534199087a13f9056107078b3bac2d2d7fb1e1"
_LEGACY_VALIDATION_FUNCTIONS = (
    "prepare_inert_legacy_compatibility",
    "freeze_inert_legacy_compatibility",
)

_OLD_RUNTIME_FUNCTION_ALLOWLIST_TAIL = f"""'{SCHEMA}.retry_staging_claimed_trial(uuid,text,jsonb)'::regprocedure::oid
          ];"""
_NEW_RUNTIME_FUNCTION_ALLOWLIST_TAIL = f"""'{SCHEMA}.retry_staging_claimed_trial(uuid,text,jsonb)'::regprocedure::oid,
            '{SCHEMA}.{CANCEL_FUNCTION}'::regprocedure::oid
          ];"""


def _runtime_role() -> tuple[str, str]:
    role = op.get_context().config.attributes.get("capacity_guard_runtime_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected cancellation migration is missing the runtime role")
    return role, op.get_bind().dialect.identifier_preparer.quote(role)


def _replace_validation_clause(old: str, new: str) -> None:
    escaped_old = old.replace("'", "''")
    escaped_new = new.replace("'", "''")
    op.execute(
        f"""
        DO $$
        DECLARE
          v_definition text;
        BEGIN
          SELECT pg_catalog.pg_get_functiondef(
            '{SCHEMA}.{VALIDATION_FUNCTION}'::pg_catalog.regprocedure
          ) INTO v_definition;
          IF pg_catalog.strpos(v_definition, '{escaped_old}') = 0 THEN
            RAISE EXCEPTION
              'protected cancellation runtime allowlist clause was not found';
          END IF;
          IF pg_catalog.length(v_definition) - pg_catalog.length(
               pg_catalog.replace(v_definition, '{escaped_old}', '')
             ) <> pg_catalog.length('{escaped_old}') THEN
            RAISE EXCEPTION
              'protected cancellation runtime allowlist clause was ambiguous';
          END IF;
          EXECUTE pg_catalog.replace(
            v_definition, '{escaped_old}', '{escaped_new}'
          );
        END $$;
        """
    )


def _assert_legacy_fence_is_empty() -> None:
    counts = (
        op.get_bind()
        .execute(
            sa.text(
                f"""
                SELECT
                  (SELECT count(*) FROM {SCHEMA}.legacy_compatibility_preparations)
                    AS preparations,
                  (SELECT count(*) FROM {SCHEMA}.legacy_writer_cursors) AS cursors,
                  (SELECT count(*) FROM {SCHEMA}.legacy_compatibility_freezes) AS freezes
                """
            )
        )
        .mappings()
        .one()
    )
    if any(int(counts[key]) != 0 for key in ("preparations", "cursors", "freezes")):
        raise RuntimeError(
            "legacy compatibility evidence uses an obsolete mutation inventory; "
            "remove the unactivated evidence before retrying the protected cancellation migration"
        )


def _replace_mutation_inventory_digest(old: str, new: str) -> None:
    _assert_legacy_fence_is_empty()
    replacement_count = 0
    for name in _LEGACY_VALIDATION_FUNCTIONS:
        signature = f"{SCHEMA}.{name}(uuid,jsonb,bytea,text)"
        definition = (
            op.get_bind()
            .execute(
                sa.text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
                {"signature": signature},
            )
            .scalar_one()
        )
        if new in definition:
            raise RuntimeError(f"{signature} does not match the expected prior mutation inventory")
        occurrences = definition.count(old)
        replacement_count += occurrences
        rewritten = definition.replace(old, new) if occurrences else definition
        if old in rewritten:
            raise RuntimeError(f"{signature} retained the obsolete mutation inventory")
        if occurrences:
            op.execute(rewritten)
    if replacement_count != 1:
        raise RuntimeError("legacy validation functions have ambiguous mutation inventory")

    for constraint, table in (
        (
            "guard_legacy_preparation_inventory_check",
            "legacy_compatibility_preparations",
        ),
        ("guard_legacy_freeze_inventory_check", "legacy_compatibility_freezes"),
    ):
        op.drop_constraint(constraint, table, schema=SCHEMA, type_="check")
        op.create_check_constraint(
            constraint,
            table,
            f"mutation_inventory_digest = '{new}'",
            schema=SCHEMA,
        )


def upgrade() -> None:
    _replace_mutation_inventory_digest(
        OLD_MUTATION_INVENTORY_DIGEST,
        NEW_MUTATION_INVENTORY_DIGEST,
    )
    _, quoted_runtime = _runtime_role()
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.cancel_protected_runtime_pending_trial(
          p_trial_id uuid,
          p_team_id uuid
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_runtime_role text;
          v_current record;
          v_transition_id uuid := pg_catalog.gen_random_uuid();
          v_transition_payload jsonb;
          v_transition_digest text;
          v_cancelled_at timestamptz := pg_catalog.statement_timestamp();
        BEGIN
          IF pg_catalog.current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION
              'protected pending cancellation requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          IF p_trial_id IS NULL THEN
            RAISE EXCEPTION 'protected pending cancellation input is invalid'
              USING ERRCODE = '22023';
          END IF;

          SELECT runtime_role_name INTO v_runtime_role
            FROM {SCHEMA}.staging_worker_runtime_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;
          IF v_runtime_role IS NULL OR session_user::text <> v_runtime_role THEN
            RAISE EXCEPTION 'protected cancellation runtime caller is not bound'
              USING ERRCODE = '42501';
          END IF;
          IF pg_catalog.pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION
              'protected cancellation runtime unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;

          -- Serialize cancellation with assignment, claim, retry, requeue,
          -- and terminal-evidence imports before inspecting the current head.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected cancellation authority is unavailable'
              USING ERRCODE = '55000';
          END IF;

          SELECT trial.id AS trial_id,
                 attempt.protected_attempt_id,
                 attempt.execution_generation,
                 attempt.requirements_digest,
                 head.transition_sequence,
                 head.lifecycle_state,
                 lifecycle.allowance_id,
                 lifecycle.plan_id,
                 lifecycle.admission_incarnation,
                 lifecycle.manager_allocation_epoch,
                 lifecycle.pool_id,
                 lifecycle.shape_instance_id,
                 lifecycle.submission_intent_id,
                 lifecycle.payload AS lifecycle_payload
            INTO v_current
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
             AND attempt.attempt_sequence = runtime.attempt_sequence
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_events AS lifecycle
              ON lifecycle.transition_id = head.transition_id
             AND lifecycle.protected_attempt_id = head.protected_attempt_id
             AND lifecycle.transition_sequence = head.transition_sequence
             AND lifecycle.lifecycle_state = head.lifecycle_state
             AND lifecycle.executable = head.executable
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
           WHERE runtime.trial_id = p_trial_id
             AND runtime.public_attempt_count = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND (p_team_id IS NULL OR trial.team_id = p_team_id)
             AND trial.state = 'protected-pending'
             AND trial.worker_id IS NULL
             AND trial.cancellation_requested_at IS NULL
             AND trial.cancellation_observed_at IS NULL
             AND trial.finished_at IS NULL
             AND trial.autoscaler_pool_name IS NULL
             AND attempt.claim_state = 'queued'
             AND head.lifecycle_state IN ('pending-unassigned', 'assigned')
             AND head.executable = false
             AND (
               (head.lifecycle_state = 'pending-unassigned'
                AND lifecycle.operation IN ('initialize', 'withdraw'))
               OR
               (head.lifecycle_state = 'assigned'
                AND lifecycle.operation = 'assign'
                AND lifecycle.previous_state = 'pending-unassigned')
             )
             AND NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.executable_claim_leases AS claim
                WHERE claim.protected_attempt_id = attempt.protected_attempt_id
             )
           FOR UPDATE OF trial, head
           FOR KEY SHARE OF runtime, attempt, lifecycle;
          IF FOUND THEN
            v_transition_payload := v_current.lifecycle_payload
              || pg_catalog.jsonb_build_object(
                'transition_id', v_transition_id,
                'expected_transition_sequence', v_current.transition_sequence,
                'operation', 'cancel',
                'expected_state', v_current.lifecycle_state,
                'target_state', 'cancelled-terminal',
                'transition_reason', 'protected-runtime-user-cancel',
                'executable', false
              );
            IF v_current.lifecycle_state = 'pending-unassigned' THEN
              v_transition_payload := v_transition_payload - ARRAY[
                'allowance_id', 'plan_id', 'admission_incarnation',
                'manager_allocation_epoch', 'pool_id', 'shape_instance_id',
                'submission_intent_id'
              ];
            END IF;
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
               v_current.transition_sequence + 1, 'cancel',
               v_current.lifecycle_state, 'cancelled-terminal',
               CASE WHEN v_current.lifecycle_state = 'assigned'
                    THEN v_current.allowance_id END,
               CASE WHEN v_current.lifecycle_state = 'assigned'
                    THEN v_current.plan_id END,
               CASE WHEN v_current.lifecycle_state = 'assigned'
                    THEN v_current.admission_incarnation END,
               CASE WHEN v_current.lifecycle_state = 'assigned'
                    THEN v_current.manager_allocation_epoch END,
               CASE WHEN v_current.lifecycle_state = 'assigned'
                    THEN v_current.pool_id END,
               CASE WHEN v_current.lifecycle_state = 'assigned'
                    THEN v_current.shape_instance_id END,
               CASE WHEN v_current.lifecycle_state = 'assigned'
                    THEN v_current.submission_intent_id END,
               false, v_transition_payload, v_transition_digest);

            UPDATE public.trials AS trial
               SET state = 'cancelled',
                   cancellation_requested_at = v_cancelled_at,
                   cancellation_observed_at = v_cancelled_at,
                   finished_at = v_cancelled_at
             WHERE trial.id = v_current.trial_id
               AND trial.state = 'protected-pending'
               AND trial.worker_id IS NULL
               AND trial.cancellation_requested_at IS NULL
               AND trial.cancellation_observed_at IS NULL
               AND trial.finished_at IS NULL;
            IF NOT FOUND THEN
              RAISE EXCEPTION 'protected pending cancellation public transition raced'
                USING ERRCODE = '40001';
            END IF;

            RETURN pg_catalog.jsonb_build_object(
              'trial_id', v_current.trial_id,
              'state', 'cancelled',
              'replayed', false
            );
          END IF;

          SELECT trial.id AS trial_id
            INTO v_current
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
             AND attempt.attempt_sequence = runtime.attempt_sequence
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_events AS lifecycle
              ON lifecycle.transition_id = head.transition_id
             AND lifecycle.protected_attempt_id = head.protected_attempt_id
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
           WHERE runtime.trial_id = p_trial_id
             AND runtime.public_attempt_count = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND (p_team_id IS NULL OR trial.team_id = p_team_id)
             AND trial.state = 'cancelled'
             AND trial.worker_id IS NULL
             AND trial.cancellation_requested_at IS NOT NULL
             AND trial.cancellation_observed_at IS NOT NULL
             AND trial.finished_at IS NOT NULL
             AND head.lifecycle_state = 'cancelled-terminal'
             AND head.executable = false
             AND lifecycle.operation = 'cancel'
             AND lifecycle.lifecycle_state = 'cancelled-terminal'
             AND lifecycle.payload->>'transition_reason' =
                   'protected-runtime-user-cancel'
           FOR KEY SHARE OF runtime, attempt, head, lifecycle, trial;
          IF FOUND THEN
            RETURN pg_catalog.jsonb_build_object(
              'trial_id', v_current.trial_id,
              'state', 'cancelled',
              'replayed', true
            );
          END IF;
          RETURN NULL;
        END
        $function$
        """
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{CANCEL_FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{CANCEL_FUNCTION} TO {quoted_runtime}")
    _replace_validation_clause(
        _OLD_RUNTIME_FUNCTION_ALLOWLIST_TAIL,
        _NEW_RUNTIME_FUNCTION_ALLOWLIST_TAIL,
    )


def downgrade() -> None:
    _replace_mutation_inventory_digest(
        NEW_MUTATION_INVENTORY_DIGEST,
        OLD_MUTATION_INVENTORY_DIGEST,
    )
    _, quoted_runtime = _runtime_role()
    _replace_validation_clause(
        _NEW_RUNTIME_FUNCTION_ALLOWLIST_TAIL,
        _OLD_RUNTIME_FUNCTION_ALLOWLIST_TAIL,
    )
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{CANCEL_FUNCTION} FROM {quoted_runtime}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{CANCEL_FUNCTION}")
