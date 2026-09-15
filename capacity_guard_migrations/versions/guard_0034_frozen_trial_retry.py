"""Permit authenticated, transaction-bound claim and retry through a frozen fence.

Revision ID: guard_0034
Revises: guard_0033
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from capacity_guard_migrations.trial_pending_cancel import (
    install_pending_cancellation,
    uninstall_pending_cancellation,
)

revision: str = "guard_0034"
down_revision: str | None = "guard_0033"
branch_labels: str | None = None
depends_on: str | None = None

_SCHEMA = "loom_capacity_guard"
_RETRY = f"{_SCHEMA}.retry_staging_claimed_trial(uuid,text,jsonb)"
_CLAIM = f"{_SCHEMA}.claim_staging_assigned_trial(uuid,text,jsonb)"
_FROZEN = """          IF v_fence.frozen THEN
            RAISE EXCEPTION 'legacy trial writer is frozen' USING ERRCODE = '55000';
          END IF;"""
_STATEMENT = """          IF v_fence.frozen AND (
            TG_OP <> 'UPDATE' OR NOT EXISTS (
              SELECT 1 FROM loom_capacity_guard.trial_mutation_permits AS permit
               WHERE permit.transaction_id = pg_catalog.pg_current_xact_id()
                 AND permit.backend_pid = pg_catalog.pg_backend_pid()
                 AND permit.state = 'issued'
                 AND permit.writer_incarnation = v_fence.writer_incarnation
                 AND permit.writer_epoch = v_fence.writer_epoch
                 AND permit.freeze_operation_id = v_fence.freeze_operation_id
            )
          ) THEN
            RAISE EXCEPTION 'legacy trial writer is frozen' USING ERRCODE = '55000';
          END IF;"""
_AFTER = """          IF v_fence.frozen THEN
            IF TG_OP <> 'UPDATE' THEN
              RAISE EXCEPTION 'legacy trial writer is frozen' USING ERRCODE = '55000';
            END IF;
            UPDATE loom_capacity_guard.trial_mutation_permits AS permit
               SET state = 'consumed',
                   observed_old_row = pg_catalog.to_jsonb(OLD),
                   observed_new_row = pg_catalog.to_jsonb(NEW)
             WHERE permit.transaction_id = pg_catalog.pg_current_xact_id()
               AND permit.backend_pid = pg_catalog.pg_backend_pid()
               AND permit.state = 'issued'
               AND permit.writer_incarnation = v_fence.writer_incarnation
               AND permit.writer_epoch = v_fence.writer_epoch
               AND permit.freeze_operation_id = v_fence.freeze_operation_id
               AND permit.trial_id = NEW.id
               AND pg_catalog.to_jsonb(OLD) @> permit.old_binding
               AND pg_catalog.to_jsonb(NEW) = pg_catalog.to_jsonb(OLD) || permit.changes;
            IF NOT FOUND THEN
              RAISE EXCEPTION 'frozen trial retry result is not authorized' USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
          END IF;"""


def _rewrite(signature: str, replacements: list[tuple[str, str]], *, upgrading: bool) -> None:
    row = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT pg_catalog.pg_get_functiondef(p.oid) AS definition, "
                "p.prosecdef, p.proconfig, p.proowner = current_user::regrole::oid AS owned "
                "FROM pg_catalog.pg_proc AS p WHERE p.oid = CAST(:signature AS regprocedure)"
            ),
            {"signature": signature},
        )
        .mappings()
        .one()
    )
    if not row["owned"] or not row["prosecdef"] or row["proconfig"] != ["search_path=pg_catalog"]:
        raise RuntimeError("frozen trial retry prior function authority changed")
    definition = row["definition"]
    for old, new in replacements if upgrading else reversed(replacements):
        source, target = (old, new) if upgrading else (new, old)
        if definition.count(source) != 1:
            raise RuntimeError("frozen trial retry prior function definition changed")
        definition = definition.replace(source, target, 1)
    op.execute(definition)


def _retry_replacements() -> list[tuple[str, str]]:
    first_update = """          UPDATE public.trials AS trial
             SET state = 'protected-pending',"""
    refund_update = """            UPDATE public.trials AS trial
               SET attempt_count = trial.attempt_count - 1"""
    issue = """          v_retry_permit := loom_capacity_guard.authorize_frozen_trial_update(
            v_current.trial_id, v_current.protected_attempt_id,
            v_current.execution_generation, p_worker_id,
            (v_session->>'worker_incarnation')::uuid, v_current.claim_operation_id,
            'retry', jsonb_build_object(
              'state', 'protected-pending', 'worker_id', NULL,
              'failure_reason', p_retry_request->>'failure_reason',
              'failure_message', NULLIF(p_retry_request->>'failure_message', ''),
              'next_attempt_at', v_next_attempt_at));
"""
    refund = """            v_retry_permit := loom_capacity_guard.authorize_frozen_trial_update(
              v_current.trial_id, v_current.protected_attempt_id,
              v_current.execution_generation, p_worker_id,
              (v_session->>'worker_incarnation')::uuid, v_current.claim_operation_id,
              'refund', jsonb_build_object('attempt_count', v_current.attempt_count - 1));
"""
    first_done = "          v_next_attempt_count := v_current.attempt_count;"
    refund_done = """          INSERT INTO loom_capacity_guard.trial_attempts
            (protected_attempt_id, trial_id, execution_generation,"""
    check = "          PERFORM loom_capacity_guard.assert_frozen_trial_mutation_consumed(v_retry_permit);\n"
    return [
        # Serialize before authentication takes shared claim/authority locks.
        # Otherwise concurrent callers can each block the other's upgrade.
        (
            """          v_session := loom_capacity_guard.assert_staging_worker_session(
            p_worker_id, p_worker_credential
          );
          PERFORM 1 FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;""",
            """          PERFORM 1 FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          v_session := loom_capacity_guard.assert_staging_worker_session(
            p_worker_id, p_worker_credential
          );""",
        ),
        (
            "           FOR UPDATE OF trial, head, claim_state\n",
            "           FOR UPDATE OF trial, head, claim_state NOWAIT\n",
        ),
        (
            "                            assignment, claim, quota;",
            "                            assignment, claim, quota NOWAIT;",
        ),
        (
            "          v_readiness jsonb;",
            "          v_readiness jsonb;\n          v_retry_permit uuid;",
        ),
        (
            "                 trial.attempt_count\n            INTO v_current",
            "                 trial.attempt_count, claim.operation_id AS claim_operation_id\n            INTO v_current",
        ),
        (first_update, issue + first_update),
        (first_done, check + first_done),
        (refund_update, refund + refund_update),
        (refund_done, check + refund_done),
    ]


def _claim_replacements() -> list[tuple[str, str]]:
    update = """          UPDATE public.trials AS trial
             SET state = 'claimed',"""
    issue = """          v_claim_permit := loom_capacity_guard.authorize_frozen_trial_update(
            v_candidate.id, v_candidate.protected_attempt_id,
            v_candidate.execution_generation, p_worker_id, v_worker.worker_incarnation,
            v_operation_id, 'claim', jsonb_build_object(
              'state', 'claimed', 'worker_id', p_worker_id,
              'claimed_at', statement_timestamp(), 'pre_start_heartbeat_at', NULL,
              'failure_reason', NULL, 'failure_message', NULL,
              'attempt_count', v_candidate.attempt_count + 1));

"""
    return [
        ("          v_claimed record;", "          v_claimed record;\n          v_claim_permit uuid;"),
        (
            "          v_session := loom_capacity_guard.assert_staging_worker_session(",
            """          PERFORM 1 FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          v_session := loom_capacity_guard.assert_staging_worker_session(""",
        ),
        (
            """          -- Serialize readiness publication, manager lifecycle mutation, and
          -- the executable claim against the same protected writer mutex.
          PERFORM 1 FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
""",
            "          -- The protected writer mutex was taken before authentication.\n",
        ),
        ("           FOR UPDATE OF worker\n", "           FOR UPDATE OF worker NOWAIT\n"),
        ("           FOR KEY SHARE OF event, job;", "           FOR KEY SHARE OF event, job NOWAIT;"),
        (
            "           WHERE state.intent_id = v_worker.intent_id\n           FOR UPDATE;",
            "           WHERE state.intent_id = v_worker.intent_id\n           FOR UPDATE NOWAIT;",
        ),
        (update, issue + update),
        (
            "          RETURN jsonb_build_object(\n            'trial_id', v_claimed.id,",
            """          PERFORM loom_capacity_guard.assert_frozen_trial_mutation_consumed(v_claim_permit);
          RETURN jsonb_build_object(
            'trial_id', v_claimed.id,""",
        ),
    ]


def upgrade() -> None:
    op.execute("""
        CREATE TABLE loom_capacity_guard.trial_mutation_permits (
          permit_id uuid PRIMARY KEY,
          transaction_id xid8 NOT NULL,
          backend_pid integer NOT NULL CHECK (backend_pid > 0),
          writer_incarnation uuid NOT NULL,
          writer_epoch bigint NOT NULL CHECK (writer_epoch > 0),
          freeze_operation_id uuid NOT NULL,
          authority_binding jsonb NOT NULL,
          registration jsonb NOT NULL,
          trial_id uuid NOT NULL,
          protected_attempt_id uuid NOT NULL,
          execution_generation bigint NOT NULL CHECK (execution_generation > 0),
          worker_id uuid,
          worker_incarnation uuid,
          claim_operation_id uuid,
          cancellation_transition_id uuid,
          adoption_operation_id uuid,
          operation text NOT NULL CHECK (operation IN ('claim', 'retry', 'refund', 'state', 'output', 'pending_cancel', 'adopt')),
          CONSTRAINT trial_mutation_permit_actor_binding CHECK (
            (operation = 'pending_cancel' AND worker_id IS NULL
             AND worker_incarnation IS NULL AND claim_operation_id IS NULL
             AND cancellation_transition_id IS NOT NULL AND adoption_operation_id IS NULL)
            OR
            (operation = 'adopt' AND worker_id IS NULL AND worker_incarnation IS NULL
             AND claim_operation_id IS NULL AND cancellation_transition_id IS NULL
             AND adoption_operation_id IS NOT NULL)
            OR
            (operation NOT IN ('pending_cancel', 'adopt') AND worker_id IS NOT NULL
             AND worker_incarnation IS NOT NULL AND claim_operation_id IS NOT NULL
             AND cancellation_transition_id IS NULL AND adoption_operation_id IS NULL)
          ),
          old_binding jsonb NOT NULL,
          changes jsonb NOT NULL,
          observed_old_row jsonb,
          observed_new_row jsonb,
          state text NOT NULL DEFAULT 'issued' CHECK (state IN ('issued', 'consumed'))
        );
        CREATE UNIQUE INDEX trial_mutation_one_pending_per_transaction
          ON loom_capacity_guard.trial_mutation_permits(transaction_id, backend_pid)
          WHERE state <> 'consumed';
        CREATE UNIQUE INDEX trial_mutation_one_operation_per_transaction
          ON loom_capacity_guard.trial_mutation_permits(transaction_id, backend_pid, trial_id, operation);
        REVOKE ALL ON TABLE loom_capacity_guard.trial_mutation_permits FROM PUBLIC;

        CREATE FUNCTION loom_capacity_guard.guard_trial_mutation_permit()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'retry mutation permission evidence is retained' USING ERRCODE = '55000';
          END IF;
          IF pg_catalog.to_jsonb(OLD) - ARRAY['state','observed_old_row','observed_new_row']
               <> pg_catalog.to_jsonb(NEW) - ARRAY['state','observed_old_row','observed_new_row']
             OR OLD.transaction_id <> pg_catalog.pg_current_xact_id()
             OR OLD.backend_pid <> pg_catalog.pg_backend_pid()
             OR OLD.state <> 'issued' OR NEW.state <> 'consumed' THEN
            RAISE EXCEPTION 'retry mutation permission transition changed' USING ERRCODE = '55000';
          END IF;
          IF OLD.observed_old_row IS NOT NULL OR OLD.observed_new_row IS NOT NULL
                OR NEW.observed_old_row IS NULL OR NEW.observed_new_row IS NULL
                OR NOT (NEW.observed_old_row @> OLD.old_binding)
                OR NEW.observed_new_row <> NEW.observed_old_row || OLD.changes THEN
            RAISE EXCEPTION 'retry mutation observed row changed' USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$;
        CREATE TRIGGER trial_mutation_permit_retained_row
          BEFORE UPDATE OR DELETE ON loom_capacity_guard.trial_mutation_permits
          FOR EACH ROW EXECUTE FUNCTION loom_capacity_guard.guard_trial_mutation_permit();
        CREATE TRIGGER trial_mutation_permit_retained_truncate
          BEFORE TRUNCATE ON loom_capacity_guard.trial_mutation_permits
          FOR EACH STATEMENT EXECUTE FUNCTION loom_capacity_guard.guard_trial_mutation_permit();

        CREATE FUNCTION loom_capacity_guard.authorize_frozen_trial_update(
          p_trial uuid, p_attempt uuid, p_generation bigint, p_worker uuid,
          p_worker_incarnation uuid, p_claim uuid, p_operation text, p_changes jsonb
        ) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_fence loom_capacity_guard.trial_writer_fence%ROWTYPE;
          v_binding jsonb;
          v_old jsonb;
          v_permit uuid := pg_catalog.gen_random_uuid();
        BEGIN
          IF pg_catalog.current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'protected retry permission requires SERIALIZABLE' USING ERRCODE = '25001';
          END IF;
          SELECT * INTO STRICT v_fence FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 FOR SHARE NOWAIT;
          IF NOT v_fence.frozen THEN
            RETURN NULL;
          END IF;
          -- Do not wait in reverse order behind authority reconfiguration.
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
            RAISE EXCEPTION 'frozen retry writer binding changed' USING ERRCODE = '55000';
          END IF;
          -- Read only columns already granted to the guard owner. The row
          -- trigger receives full OLD/NEW without broadening SELECT authority.
          SELECT pg_catalog.jsonb_build_object(
                   'id', trial.id, 'state', trial.state, 'worker_id', trial.worker_id,
                   'attempt_count', trial.attempt_count, 'started_at', trial.started_at,
                   'cancellation_requested_at', trial.cancellation_requested_at)
            INTO v_old FROM public.trials AS trial
           WHERE trial.id = p_trial FOR UPDATE NOWAIT;
          IF v_old IS NULL OR NOT EXISTS (
            SELECT 1 FROM loom_capacity_guard.executable_claim_leases AS claim
            JOIN loom_capacity_guard.trial_attempts AS attempt
              ON attempt.protected_attempt_id = claim.protected_attempt_id
             AND attempt.execution_generation = claim.execution_generation
             AND attempt.requirements_digest = claim.requirements_digest
             WHERE claim.operation_id = p_claim AND claim.protected_attempt_id = p_attempt
               AND claim.execution_generation = p_generation AND attempt.trial_id = p_trial
               AND claim.worker_id = p_worker AND claim.worker_incarnation = p_worker_incarnation
               AND claim.subject_id = v_fence.subject_id
          ) THEN
            RAISE EXCEPTION 'frozen retry exact claim is unavailable' USING ERRCODE = '55000';
          END IF;
          IF p_operation = 'claim' THEN
            IF v_old->>'state' IS DISTINCT FROM 'protected-pending'
               OR v_old->'worker_id' IS DISTINCT FROM 'null'::jsonb
               OR v_old->'started_at' IS DISTINCT FROM 'null'::jsonb
               OR v_old->'cancellation_requested_at' IS DISTINCT FROM 'null'::jsonb
               OR NOT EXISTS (
                 SELECT 1 FROM loom_capacity_guard.executable_claim_leases AS claim
                  WHERE claim.operation_id = p_claim AND claim.lease_state = 'live'
                    AND claim.executable)
               OR p_changes IS DISTINCT FROM pg_catalog.jsonb_build_object(
                 'state', 'claimed', 'worker_id', p_worker,
                 'claimed_at', statement_timestamp(), 'pre_start_heartbeat_at', NULL,
                 'failure_reason', NULL, 'failure_message', NULL,
                 'attempt_count', (v_old->>'attempt_count')::integer + 1) THEN
              RAISE EXCEPTION 'frozen claim row transition changed' USING ERRCODE = '55000';
            END IF;
          ELSIF p_operation = 'retry' THEN
            IF v_old->>'state' IS DISTINCT FROM 'claimed'
               OR v_old->>'worker_id' IS DISTINCT FROM p_worker::text
               OR v_old->'started_at' IS DISTINCT FROM 'null'::jsonb
               OR v_old->'cancellation_requested_at' IS DISTINCT FROM 'null'::jsonb
               OR pg_catalog.jsonb_typeof(p_changes) IS DISTINCT FROM 'object'
               OR NOT (p_changes ?& ARRAY['state','worker_id','failure_reason','failure_message','next_attempt_at'])
               OR p_changes - ARRAY['state','worker_id','failure_reason','failure_message','next_attempt_at'] <> '{}'::jsonb
               OR p_changes->>'state' IS DISTINCT FROM 'protected-pending'
               OR p_changes->'worker_id' IS DISTINCT FROM 'null'::jsonb THEN
              RAISE EXCEPTION 'frozen retry row transition changed' USING ERRCODE = '55000';
            END IF;
          ELSIF p_operation = 'refund' THEN
            IF v_old->>'state' IS DISTINCT FROM 'protected-pending'
               OR v_old->'worker_id' IS DISTINCT FROM 'null'::jsonb
               OR NOT EXISTS (
                 SELECT 1 FROM loom_capacity_guard.trial_mutation_permits AS previous
                  WHERE previous.transaction_id = pg_catalog.pg_current_xact_id()
                    AND previous.backend_pid = pg_catalog.pg_backend_pid()
                    AND previous.trial_id = p_trial AND previous.claim_operation_id = p_claim
                    AND previous.operation = 'retry' AND previous.state = 'consumed'
                    AND previous.changes->>'failure_reason' = 'node_setup_health')
               OR p_changes IS DISTINCT FROM pg_catalog.jsonb_build_object(
                 'attempt_count', (v_old->>'attempt_count')::integer - 1) THEN
              RAISE EXCEPTION 'frozen retry refund transition changed' USING ERRCODE = '55000';
            END IF;
          ELSE
            RAISE EXCEPTION 'frozen retry operation is unavailable' USING ERRCODE = '55000';
          END IF;
          INSERT INTO loom_capacity_guard.trial_mutation_permits
            (permit_id, transaction_id, backend_pid, writer_incarnation, writer_epoch,
             freeze_operation_id, authority_binding, registration, trial_id,
             protected_attempt_id, execution_generation, worker_id, worker_incarnation,
             claim_operation_id, operation, old_binding, changes)
          VALUES
            (v_permit, pg_catalog.pg_current_xact_id(), pg_catalog.pg_backend_pid(),
             v_fence.writer_incarnation, v_fence.writer_epoch, v_fence.freeze_operation_id,
             v_fence.authority_binding, v_fence.registration, p_trial, p_attempt, p_generation,
             p_worker, p_worker_incarnation, p_claim, p_operation, v_old, p_changes);
          RETURN v_permit;
        END
        $function$;

        CREATE FUNCTION loom_capacity_guard.assert_frozen_trial_mutation_consumed(p_permit uuid)
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF p_permit IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM loom_capacity_guard.trial_mutation_permits AS permit
             WHERE permit.permit_id = p_permit AND permit.state = 'consumed'
               AND permit.transaction_id = pg_catalog.pg_current_xact_id()
               AND permit.backend_pid = pg_catalog.pg_backend_pid()
          ) THEN
            RAISE EXCEPTION 'frozen retry permission was not consumed' USING ERRCODE = '55000';
          END IF;
        END
        $function$;
        REVOKE ALL ON FUNCTION loom_capacity_guard.guard_trial_mutation_permit() FROM PUBLIC;
        REVOKE ALL ON FUNCTION loom_capacity_guard.authorize_frozen_trial_update(uuid,uuid,bigint,uuid,uuid,uuid,text,jsonb) FROM PUBLIC;
        REVOKE ALL ON FUNCTION loom_capacity_guard.assert_frozen_trial_mutation_consumed(uuid) FROM PUBLIC;
    """)
    _rewrite(f"{_SCHEMA}.lock_trial_writer_statement()", [(_FROZEN, _STATEMENT)], upgrading=True)
    _rewrite(f"{_SCHEMA}.account_trial_writer_mutation()", [(_FROZEN, _AFTER)], upgrading=True)
    _rewrite(_RETRY, _retry_replacements(), upgrading=True)
    _rewrite(_CLAIM, _claim_replacements(), upgrading=True)

    from capacity_guard_migrations.trial_state import install_state_reporting

    install_state_reporting(_rewrite)
    from capacity_guard_migrations.trial_output import install_output_reporting

    install_output_reporting(_rewrite)
    from capacity_guard_migrations.trial_adoption import install_trial_adoption

    install_trial_adoption(_rewrite)
    install_pending_cancellation(_rewrite)


def downgrade() -> None:
    op.execute("""
        DO $retirement$
        DECLARE
          v_relation oid := 'loom_capacity_guard.trial_mutation_permits'::regclass;
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class
             WHERE oid = v_relation AND relkind = 'r' AND NOT relispartition
               AND relowner = current_user::regrole::oid
          ) THEN
            RAISE EXCEPTION 'frozen retry retirement relation ownership changed'
              USING ERRCODE = '42501';
          END IF;
          -- Neither LOCK nor the later retained-evidence read may recurse into
          -- another authority's descendants. Stabilize the parent first.
          LOCK TABLE ONLY loom_capacity_guard.trial_mutation_permits
            IN ACCESS EXCLUSIVE MODE NOWAIT;
          IF 'loom_capacity_guard.trial_mutation_permits'::regclass::oid <> v_relation
             OR NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_class
                WHERE oid = v_relation AND relkind = 'r' AND NOT relispartition
                  AND relowner = current_user::regrole::oid
             ) THEN
            RAISE EXCEPTION 'frozen retry retirement relation ownership changed'
              USING ERRCODE = '42501';
          END IF;
          IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_inherits
             WHERE inhparent = v_relation OR inhrelid = v_relation
          ) THEN
            RAISE EXCEPTION 'frozen retry retirement inheritance is unsupported'
              USING ERRCODE = '55000';
          END IF;
        END
        $retirement$;
    """)
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM ONLY loom_capacity_guard.trial_mutation_permits)"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError("frozen retry permission evidence requires protected retirement")
    from capacity_guard_migrations.trial_output import uninstall_output_reporting
    from capacity_guard_migrations.trial_state import uninstall_state_reporting
    from capacity_guard_migrations.trial_adoption import uninstall_trial_adoption

    uninstall_trial_adoption(_rewrite)
    uninstall_pending_cancellation(_rewrite)
    uninstall_output_reporting(_rewrite)
    uninstall_state_reporting(_rewrite)
    _rewrite(_CLAIM, _claim_replacements(), upgrading=False)
    _rewrite(_RETRY, _retry_replacements(), upgrading=False)
    _rewrite(f"{_SCHEMA}.account_trial_writer_mutation()", [(_FROZEN, _AFTER)], upgrading=False)
    _rewrite(f"{_SCHEMA}.lock_trial_writer_statement()", [(_FROZEN, _STATEMENT)], upgrading=False)
    op.execute("""
        DROP FUNCTION loom_capacity_guard.assert_frozen_trial_mutation_consumed(uuid);
        DROP FUNCTION loom_capacity_guard.authorize_frozen_trial_update(uuid,uuid,bigint,uuid,uuid,uuid,text,jsonb);
        DROP TABLE loom_capacity_guard.trial_mutation_permits;
        DROP FUNCTION loom_capacity_guard.guard_trial_mutation_permit();
    """)
