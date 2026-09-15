"""Authenticate trial progress and its frozen-writer permission in one transaction.

Installed as part of the unreleased guard_0035 trial-writer permission migration.
"""

from __future__ import annotations

from collections.abc import Callable

from alembic import op

_FUNCTION = "loom_capacity_guard.report_staging_trial_state(uuid,text,jsonb)"
_ISSUER = "loom_capacity_guard.authorize_frozen_trial_update(uuid,uuid,bigint,uuid,uuid,uuid,text,jsonb)"
_VALIDATOR = "loom_capacity_guard.current_protected_runtime_registration()"
_CLOSURE = "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)"
_OLD_CLOSURE = "p_previous_state NOT IN ('claimed', 'running')"
_NEW_CLOSURE = "p_previous_state NOT IN ('claimed', 'running', 'materializing')"
_OLD_ALLOWED = "'loom_capacity_guard.cancel_protected_runtime_pending_trial(uuid,uuid)'::regprocedure::oid\n          ];"
_NEW_ALLOWED = "'loom_capacity_guard.cancel_protected_runtime_pending_trial(uuid,uuid)'::regprocedure::oid,\n            'loom_capacity_guard.report_staging_trial_state(uuid,text,jsonb)'::regprocedure::oid\n          ];"
_OLD = """          ELSE
            RAISE EXCEPTION 'frozen retry operation is unavailable' USING ERRCODE = '55000';"""
_NEW = """          ELSIF p_operation = 'state' THEN
            IF v_old->>'state' NOT IN ('claimed', 'running', 'materializing')
               OR v_old->>'worker_id' IS DISTINCT FROM p_worker::text
               OR pg_catalog.jsonb_typeof(p_changes) IS DISTINCT FROM 'object'
               OR NOT (p_changes ?& ARRAY['state','result','failure_reason','failure_message','started_at','finished_at'])
               OR p_changes - ARRAY['state','result','failure_reason','failure_message','started_at','finished_at'] <> '{}'::jsonb
               OR p_changes->>'state' NOT IN ('running','materializing','succeeded','failed','cancelled')
               OR (v_old->>'state' = 'materializing' AND p_changes->>'state' NOT IN ('succeeded','failed')) THEN
              RAISE EXCEPTION 'frozen progress row transition changed' USING ERRCODE = '55000';
            END IF;
          ELSE
            RAISE EXCEPTION 'frozen retry operation is unavailable' USING ERRCODE = '55000';"""


def install_state_reporting(rewrite: Callable[..., None]) -> None:
    rewrite(_ISSUER, [(_OLD, _NEW)], upgrading=True)
    rewrite(_CLOSURE, [(_OLD_CLOSURE, _NEW_CLOSURE)], upgrading=True)
    op.execute("""
        CREATE FUNCTION loom_capacity_guard.report_staging_trial_state(
          p_worker_id uuid, p_worker_credential text, p_report jsonb
        ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_session jsonb;
          v_current record;
          v_lease record;
          v_changes jsonb;
          v_permit uuid;
          v_state text;
          v_family jsonb;
          v_decision text;
          v_family_state text;
          v_final_state text;
          v_index integer;
          v_count integer;
          v_cancellation jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'protected progress requires SERIALIZABLE' USING ERRCODE = '25000';
          END IF;
          IF jsonb_typeof(p_report) IS DISTINCT FROM 'object'
             OR p_report - ARRAY['trial_id','state','result','failure_reason','failure_message',
                                 'execution_lease_id','execution_generation','expected','family'] <> '{}'::jsonb
             OR NOT (p_report ?& ARRAY['trial_id','state','result','failure_reason','failure_message',
                                      'execution_lease_id','execution_generation','expected','family'])
             OR jsonb_typeof(p_report->'trial_id') IS DISTINCT FROM 'string'
             OR p_report->>'state' NOT IN ('running','materializing','succeeded','failed','cancelled')
             OR jsonb_typeof(p_report->'state') IS DISTINCT FROM 'string'
             OR jsonb_typeof(p_report->'failure_reason') NOT IN ('string','null')
             OR jsonb_typeof(p_report->'failure_message') NOT IN ('string','null') THEN
            RAISE EXCEPTION 'protected progress request is malformed' USING ERRCODE = '22023';
          END IF;
          -- Serialize before shared authentication locks can require an upgrade.
          PERFORM 1 FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          v_session := loom_capacity_guard.assert_staging_worker_session(p_worker_id, p_worker_credential);
          SELECT trial.id AS trial_id, trial.state, trial.result, trial.config, trial.family_key, trial.batch_id, trial.team_id,
                 trial.attempt_count, trial.failure_message, trial.started_at, trial.finished_at,
                 runtime.protected_attempt_id, attempt.execution_generation,
                 claim.operation_id AS claim_operation_id
            INTO v_current
            FROM loom_capacity_guard.protected_runtime_trial_submissions AS runtime
            JOIN loom_capacity_guard.protected_runtime_trial_readiness AS readiness
              ON readiness.trial_id = runtime.trial_id AND readiness.protected_attempt_id = runtime.protected_attempt_id
            JOIN loom_capacity_guard.atomic_trial_submissions AS submission ON submission.trial_id = runtime.trial_id
            JOIN loom_capacity_guard.trial_attempts AS attempt ON attempt.protected_attempt_id = runtime.protected_attempt_id
            JOIN loom_capacity_guard.attempt_lifecycle_heads AS head ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN loom_capacity_guard.attempt_lifecycle_events AS assignment
              ON assignment.transition_id = head.transition_id AND assignment.protected_attempt_id = head.protected_attempt_id
            JOIN loom_capacity_guard.executable_claim_leases AS claim
              ON claim.protected_attempt_id = attempt.protected_attempt_id
             AND claim.execution_generation = attempt.execution_generation AND claim.requirements_digest = attempt.requirements_digest
            JOIN loom_capacity_guard.executable_claim_state AS claim_state ON claim_state.intent_id = claim.intent_id
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
           WHERE trial.id = (p_report->>'trial_id')::uuid
             AND runtime.public_attempt_count + 1 = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND (trial.state IN ('claimed','running') OR trial.state = 'materializing'
                  AND p_report->>'state' IN ('succeeded','failed')) AND trial.worker_id = p_worker_id
             AND attempt.claim_state = 'queued' AND head.lifecycle_state = 'assigned' AND NOT head.executable
             AND assignment.operation = 'assign' AND assignment.previous_state = 'pending-unassigned'
             AND assignment.lifecycle_state = 'assigned' AND assignment.transition_sequence = head.transition_sequence
             AND assignment.execution_generation = attempt.execution_generation
             AND assignment.requirements_digest = attempt.requirements_digest AND NOT assignment.executable
             AND assignment.submission_intent_id = (v_session->>'intent_id')::uuid
             AND assignment.submission_intent_id = claim.intent_id
             AND claim.worker_id = p_worker_id AND claim.worker_incarnation = (v_session->>'worker_incarnation')::uuid
             AND claim.lease_state = 'live' AND claim.executable
             AND claim_state.subject_id = claim.subject_id AND claim_state.subject_incarnation = claim.subject_incarnation
             AND claim_state.claim_high_water >= claim.claim_high_water
             AND claim_state.terminal_high_water < claim_state.claim_high_water
             AND NOT EXISTS (SELECT 1 FROM loom_capacity_guard.executable_claim_terminal_events AS terminal
                             WHERE terminal.admitted_operation_id = claim.operation_id
                                OR terminal.protected_attempt_id = claim.protected_attempt_id)
           FOR UPDATE OF trial, head, claim_state NOWAIT
           FOR KEY SHARE OF runtime, readiness, submission, attempt, assignment, claim NOWAIT;
          IF NOT FOUND THEN RETURN NULL; END IF;

          IF p_report->>'state' IN ('succeeded','failed','cancelled') THEN
            IF p_report->'expected' IS DISTINCT FROM jsonb_build_object('result', v_current.result, 'config', v_current.config) THEN
              RAISE EXCEPTION 'protected terminal validation inputs changed' USING ERRCODE = '55000';
            END IF;
            SELECT jsonb_build_object(
                     'family_key', trial.family_key, 'batch_id', trial.batch_id, 'team_id', trial.team_id,
                     'task_id', trial.task_id, 'attempt_count', trial.attempt_count, 'trial_state', trial.state,
                     'result', trial.result, 'spec', batch.family_run_spec, 'task_sequence', family.task_sequence,
                     'current_index', family.current_index, 'family_attempt_count', family.attempt_count,
                     'family_state', family.state)
              INTO v_family FROM public.trials AS trial
              JOIN public.batches AS batch ON batch.id = trial.batch_id
              JOIN public.batch_family_state AS family
                ON family.batch_id = trial.batch_id AND family.family_key = trial.family_key
             WHERE trial.id = v_current.trial_id AND trial.family_key IS NOT NULL AND batch.family_run_spec IS NOT NULL
             FOR UPDATE OF family NOWAIT FOR SHARE OF batch NOWAIT;
            IF v_family IS NULL THEN
              IF p_report->'family' IS DISTINCT FROM 'null'::jsonb THEN
                RAISE EXCEPTION 'protected family appeared or disappeared' USING ERRCODE = '55000';
              END IF;
            ELSE
              IF jsonb_typeof(p_report->'family') IS DISTINCT FROM 'object'
                 OR (p_report->'family') - ARRAY['before','decision'] <> '{}'::jsonb
                 OR p_report->'family'->'before' IS DISTINCT FROM v_family
                 OR jsonb_typeof(p_report->'family'->'decision') IS DISTINCT FROM 'string'
                 OR p_report->'family'->>'decision' NOT IN ('advance','retry','skip','abort') THEN
                RAISE EXCEPTION 'protected family validation inputs changed' USING ERRCODE = '55000';
              END IF;
              v_decision := p_report->'family'->>'decision';
              v_index := (v_family->>'current_index')::integer;
              v_count := (v_family->>'family_attempt_count')::integer;
              CASE v_decision
                WHEN 'advance' THEN v_final_state := 'adapting'; v_count := 0;
                WHEN 'retry' THEN v_final_state := 'pending'; v_count := v_count + 1;
                WHEN 'skip' THEN
                  v_index := v_index + 1; v_count := 0;
                  v_final_state := CASE WHEN v_index >= jsonb_array_length(v_family->'task_sequence') THEN 'done' ELSE 'pending' END;
                WHEN 'abort' THEN v_final_state := 'aborted';
              END CASE;
              v_family_state := CASE WHEN v_decision = 'skip' THEN 'cancelling' ELSE v_final_state END;
            END IF;
          ELSIF p_report->'expected' IS DISTINCT FROM 'null'::jsonb OR p_report->'family' IS DISTINCT FROM 'null'::jsonb THEN
            RAISE EXCEPTION 'protected progress validation inputs are malformed' USING ERRCODE = '22023';
          END IF;

          SELECT lease.id, lease.generation, lease.revoked_at, lease.deleted_at INTO v_lease
            FROM public.execution_leases AS lease
           WHERE lease.trial_id = v_current.trial_id
             AND ((p_report->>'execution_lease_id' IS NULL AND lease.execution_role = 'attempt')
                  OR lease.id = (p_report->>'execution_lease_id')::uuid)
           ORDER BY lease.attempt DESC LIMIT 1 FOR UPDATE NOWAIT;
          IF FOUND AND (p_report->>'execution_lease_id' IS DISTINCT FROM v_lease.id::text
                        OR (p_report->>'execution_generation')::bigint IS DISTINCT FROM v_lease.generation
                        OR v_lease.revoked_at IS NOT NULL OR v_lease.deleted_at IS NOT NULL) THEN
            RAISE EXCEPTION 'protected progress execution generation changed' USING ERRCODE = '55000';
          END IF;
          IF NOT FOUND AND p_report->>'execution_lease_id' IS NOT NULL THEN
            RAISE EXCEPTION 'protected progress execution lease is unavailable' USING ERRCODE = '55000';
          END IF;
          v_changes := jsonb_build_object(
            'state', p_report->>'state',
            'result', CASE WHEN p_report->'result' = 'null'::jsonb THEN v_current.result ELSE p_report->'result' END,
            'failure_reason', p_report->>'failure_reason',
            'failure_message', COALESCE(p_report->>'failure_message', v_current.failure_message),
            'started_at', CASE WHEN p_report->>'state' = 'running' THEN COALESCE(v_current.started_at, now()) ELSE v_current.started_at END,
            'finished_at', CASE WHEN p_report->>'state' IN ('succeeded','failed','cancelled') THEN now() ELSE v_current.finished_at END);
          v_permit := loom_capacity_guard.authorize_frozen_trial_update(
            v_current.trial_id, v_current.protected_attempt_id, v_current.execution_generation,
            p_worker_id, (v_session->>'worker_incarnation')::uuid, v_current.claim_operation_id, 'state', v_changes);
          UPDATE public.trials AS trial
             SET state = v_changes->>'state', result = NULLIF(v_changes->'result', 'null'::jsonb),
                 failure_reason = v_changes->>'failure_reason', failure_message = v_changes->>'failure_message',
                 started_at = (v_changes->>'started_at')::timestamptz,
                 finished_at = (v_changes->>'finished_at')::timestamptz
           WHERE trial.id = v_current.trial_id AND trial.worker_id = p_worker_id AND trial.state = v_current.state
           RETURNING trial.state INTO v_state;
          IF NOT FOUND THEN RAISE EXCEPTION 'protected progress update lost its row' USING ERRCODE = '55000'; END IF;
          PERFORM loom_capacity_guard.assert_frozen_trial_mutation_consumed(v_permit);
          IF v_current.state = 'materializing' AND v_state IN ('succeeded','failed') THEN
            -- The historical application trigger handles claimed/running only.
            -- Retain the exact OLD state from this authenticated, locked update.
            PERFORM loom_capacity_guard.close_protected_runtime_trial_claim(
              v_current.trial_id, v_current.state, v_state, p_worker_id, v_current.attempt_count);
          END IF;
          IF v_family IS NOT NULL THEN
            UPDATE public.batch_family_state
               SET state = v_family_state, current_index = v_index, attempt_count = v_count, updated_at = now()
             WHERE batch_id = v_current.batch_id AND family_key = v_current.family_key;
            IF NOT FOUND OR NOT EXISTS (
              SELECT 1 FROM public.batch_family_state
               WHERE batch_id = v_current.batch_id AND family_key = v_current.family_key
                 AND state = v_family_state AND current_index = v_index AND attempt_count = v_count
            ) THEN
              RAISE EXCEPTION 'protected family update lost its exact row' USING ERRCODE = '55000';
            END IF;
            IF v_decision IN ('skip','abort') THEN
              SELECT jsonb_build_object(
                       'batch_id', v_current.batch_id, 'family_key', v_current.family_key, 'team_id', v_current.team_id,
                       'final_state', v_final_state, 'trial_ids', COALESCE(jsonb_agg(trial.id ORDER BY trial.id), '[]'::jsonb))
                INTO v_cancellation FROM public.trials AS trial
               WHERE trial.batch_id = v_current.batch_id AND trial.family_key = v_current.family_key
                 AND trial.state IN ('queued','protected-pending');
            END IF;
          END IF;
          RETURN jsonb_build_object('trial_id', v_current.trial_id, 'state', v_state, 'family_cancellation', v_cancellation);
        END
        $function$;
    """)
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("protected state migration is missing configuration")
    role = configuration.attributes.get("capacity_guard_runtime_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected progress migration is missing runtime role")
    quoted = op.get_bind().dialect.identifier_preparer.quote(role)
    op.execute(f"REVOKE ALL ON FUNCTION {_FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {_FUNCTION} TO {quoted}")
    rewrite(_VALIDATOR, [(_OLD_ALLOWED, _NEW_ALLOWED)], upgrading=True)


def uninstall_state_reporting(rewrite: Callable[..., None]) -> None:
    """Caller has already locked and proved the complete permission ledger empty."""
    rewrite(_VALIDATOR, [(_OLD_ALLOWED, _NEW_ALLOWED)], upgrading=False)
    op.execute(f"DROP FUNCTION {_FUNCTION}")
    rewrite(_CLOSURE, [(_OLD_CLOSURE, _NEW_CLOSURE)], upgrading=False)
