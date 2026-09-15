"""Workerless pending cancellation through the shared frozen mutation ledger."""

from __future__ import annotations

from collections.abc import Callable

from alembic import op

_CANCEL = "loom_capacity_guard.cancel_protected_runtime_pending_trial(uuid,uuid)"
_ISSUER = "loom_capacity_guard.authorize_frozen_pending_cancel(uuid,uuid,bigint,uuid,uuid,timestamptz)"
_MUTEX = """          PERFORM 1 FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected cancellation authority is unavailable'
              USING ERRCODE = '55000';
          END IF;
"""


def _replacements() -> list[tuple[str, str]]:
    update = """            UPDATE public.trials AS trial
               SET state = 'cancelled',"""
    issue = """            v_cancel_permit := loom_capacity_guard.authorize_frozen_pending_cancel(
              v_current.trial_id, v_current.protected_attempt_id,
              v_current.execution_generation, v_transition_id, p_team_id, v_cancelled_at);

"""
    return [
        ("          v_cancelled_at timestamptz", "          v_cancel_permit uuid;\n          v_cancelled_at timestamptz"),
        (_MUTEX, "          -- Runtime mutex acquired before caller role-row locks.\n"),
        (
            "          SELECT runtime_role_name INTO v_runtime_role",
            _MUTEX.replace("FOR UPDATE;", "FOR UPDATE NOWAIT;")
            + "\n          SELECT runtime_role_name INTO v_runtime_role",
        ),
        ("           FOR KEY SHARE;", "           FOR KEY SHARE NOWAIT;"),
        ("           FOR UPDATE OF trial, head\n", "           FOR UPDATE OF trial, head NOWAIT\n"),
        ("           FOR KEY SHARE OF runtime, attempt, lifecycle;", "           FOR KEY SHARE OF runtime, attempt, lifecycle NOWAIT;"),
        ("           FOR KEY SHARE OF runtime, attempt, head, lifecycle, trial;", "           FOR KEY SHARE OF runtime, attempt, head, lifecycle, trial NOWAIT;"),
        (update, issue + update),
        (
            """            RETURN pg_catalog.jsonb_build_object(
              'trial_id', v_current.trial_id,
              'state', 'cancelled',
              'replayed', false""",
            """            PERFORM loom_capacity_guard.assert_frozen_trial_mutation_consumed(v_cancel_permit);
            RETURN pg_catalog.jsonb_build_object(
              'trial_id', v_current.trial_id,
              'state', 'cancelled',
              'replayed', false""",
        ),
    ]


def install_pending_cancellation(rewrite: Callable[..., None]) -> None:
    op.execute("""
        CREATE FUNCTION loom_capacity_guard.authorize_frozen_pending_cancel(
          p_trial uuid, p_attempt uuid, p_generation bigint, p_transition uuid,
          p_team uuid, p_cancelled_at timestamptz
        ) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_fence loom_capacity_guard.trial_writer_fence%ROWTYPE;
          v_binding jsonb;
          v_old jsonb;
          v_runtime_role text;
          v_permit uuid := pg_catalog.gen_random_uuid();
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'pending cancel permission requires SERIALIZABLE' USING ERRCODE = '25000';
          END IF;
          SELECT runtime_role_name INTO v_runtime_role
            FROM loom_capacity_guard.staging_worker_runtime_authority
           WHERE singleton_id = 1 FOR KEY SHARE NOWAIT;
          IF v_runtime_role IS NULL OR session_user::text <> v_runtime_role
             OR pg_catalog.pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'pending cancel permission runtime is not bound' USING ERRCODE = '42501';
          END IF;
          SELECT * INTO STRICT v_fence FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 FOR SHARE NOWAIT;
          IF NOT v_fence.frozen THEN RETURN NULL; END IF;
          SELECT pg_catalog.jsonb_build_object(
                   'registration', pg_catalog.to_jsonb(r),
                   'authority', pg_catalog.to_jsonb(f) - 'reporter_high_water' - 'updated_at')
            INTO v_binding
            FROM loom_capacity_guard.authority_state AS f
            JOIN loom_capacity_guard.agent_registrations AS r ON r.singleton_id = f.singleton_id
           WHERE f.singleton_id = 1
             AND r.agent_incarnation = (v_fence.registration->>'agent_incarnation')::uuid
             AND r.registration_state = 'registered'
           FOR SHARE OF f, r NOWAIT;
          IF v_binding IS NULL
             OR v_binding->'registration' IS DISTINCT FROM v_fence.registration
             OR v_binding->'authority' IS DISTINCT FROM v_fence.authority_binding THEN
            RAISE EXCEPTION 'frozen cancellation writer binding changed' USING ERRCODE = '55000';
          END IF;
          SELECT pg_catalog.jsonb_build_object(
                   'id', trial.id, 'team_id', trial.team_id, 'state', trial.state,
                   'worker_id', trial.worker_id, 'attempt_count', trial.attempt_count,
                   'cancellation_requested_at', trial.cancellation_requested_at,
                   'cancellation_observed_at', trial.cancellation_observed_at,
                   'finished_at', trial.finished_at)
            INTO v_old
            FROM public.trials AS trial
            JOIN loom_capacity_guard.protected_runtime_trial_submissions AS runtime
              ON runtime.trial_id = trial.id
            JOIN loom_capacity_guard.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
             AND attempt.attempt_sequence = runtime.attempt_sequence
            JOIN loom_capacity_guard.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN loom_capacity_guard.attempt_lifecycle_events AS event
              ON event.transition_id = head.transition_id
             AND event.protected_attempt_id = head.protected_attempt_id
             AND event.transition_sequence = head.transition_sequence
           WHERE trial.id = p_trial AND attempt.protected_attempt_id = p_attempt
             AND attempt.execution_generation = p_generation
             AND event.transition_id = p_transition
             AND event.execution_generation = attempt.execution_generation
             AND event.requirements_digest = attempt.requirements_digest
             AND event.operation = 'cancel'
             AND event.previous_state IN ('pending-unassigned', 'assigned')
             AND event.lifecycle_state = 'cancelled-terminal' AND NOT event.executable
             AND head.lifecycle_state = 'cancelled-terminal' AND NOT head.executable
             AND event.payload->>'transition_reason' = 'protected-runtime-user-cancel'
             AND runtime.public_attempt_count = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND (p_team IS NULL OR trial.team_id = p_team)
             AND trial.state = 'protected-pending' AND trial.worker_id IS NULL
             AND trial.cancellation_requested_at IS NULL
             AND trial.cancellation_observed_at IS NULL AND trial.finished_at IS NULL
             AND trial.autoscaler_pool_name IS NULL AND attempt.claim_state = 'queued'
             AND NOT EXISTS (
               SELECT 1 FROM loom_capacity_guard.executable_claim_leases AS claim
                WHERE claim.protected_attempt_id = attempt.protected_attempt_id)
           FOR UPDATE OF trial NOWAIT
           FOR KEY SHARE OF runtime, attempt, head, event NOWAIT;
          IF v_old IS NULL OR p_cancelled_at IS DISTINCT FROM statement_timestamp() THEN
            RAISE EXCEPTION 'frozen pending cancellation transition changed' USING ERRCODE = '55000';
          END IF;
          INSERT INTO loom_capacity_guard.trial_mutation_permits
            (permit_id, transaction_id, backend_pid, writer_incarnation, writer_epoch,
             freeze_operation_id, authority_binding, registration, trial_id,
             protected_attempt_id, execution_generation, cancellation_transition_id,
             operation, old_binding, changes)
          VALUES
            (v_permit, pg_catalog.pg_current_xact_id(), pg_catalog.pg_backend_pid(),
             v_fence.writer_incarnation, v_fence.writer_epoch, v_fence.freeze_operation_id,
             v_fence.authority_binding, v_fence.registration, p_trial, p_attempt,
             p_generation, p_transition, 'pending_cancel', v_old,
             pg_catalog.jsonb_build_object('state', 'cancelled',
               'cancellation_requested_at', p_cancelled_at,
               'cancellation_observed_at', p_cancelled_at, 'finished_at', p_cancelled_at));
          RETURN v_permit;
        END
        $function$;
    """)
    op.execute(f"REVOKE ALL ON FUNCTION {_ISSUER} FROM PUBLIC")
    rewrite(_CANCEL, _replacements(), upgrading=True)


def uninstall_pending_cancellation(rewrite: Callable[..., None]) -> None:
    """The owning migration already proved retained mutation evidence absent."""
    rewrite(_CANCEL, _replacements(), upgrading=False)
    op.execute(f"DROP FUNCTION {_ISSUER}")
