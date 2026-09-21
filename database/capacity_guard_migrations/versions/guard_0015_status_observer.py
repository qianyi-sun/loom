"""Authorize a distinct least-privilege executable-status observer.

Revision ID: guard_0015
Revises: guard_0014
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "guard_0015"
down_revision: str | Sequence[str] | None = "guard_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = "observe_executable_intent(uuid,uuid,uuid)"


def _role(attribute: str) -> tuple[str, str]:
    role = op.get_context().config.attributes.get(attribute)
    if not isinstance(role, str) or not role:
        raise RuntimeError("executable status observer migration is missing a canonical role")
    return role, op.get_bind().dialect.identifier_preparer.quote(role)


def upgrade() -> None:
    executor, quoted_executor = _role("capacity_guard_executor_role")
    observer, quoted_observer = _role("capacity_guard_observer_role")
    if executor == observer:
        raise RuntimeError("executable status observer role must differ from executor")
    # The old authority row is append-only. Store the canonical observer in a
    # separate forward singleton rather than rewriting historical authority.
    op.execute(
        f"CREATE TABLE {SCHEMA}.executable_observer_authority ("
        "singleton_id smallint PRIMARY KEY CHECK (singleton_id = 1), "
        "observer_role_name text NOT NULL CHECK "
        "(observer_role_name ~ '^[a-z][a-z0-9_]{0,62}$')"
        ")"
    )
    op.execute(
        f"INSERT INTO {SCHEMA}.executable_observer_authority "
        f"(singleton_id, observer_role_name) VALUES (1, '{observer}')"
    )
    op.execute(
        f"CREATE TRIGGER executable_observer_authority_append_only_row "
        f"BEFORE UPDATE OR DELETE ON {SCHEMA}.executable_observer_authority "
        f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()"
    )
    op.execute(
        f"CREATE TRIGGER executable_observer_authority_append_only_truncate "
        f"BEFORE TRUNCATE ON {SCHEMA}.executable_observer_authority "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()"
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.observe_executable_intent(
          p_subject_id uuid, p_subject_incarnation uuid, p_intent_id uuid
        ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
        DECLARE
          v_executor_role text; v_observer_role text;
          v_prepared {SCHEMA}.executable_admission_events%ROWTYPE;
          v_current {SCHEMA}.executable_admission_events%ROWTYPE;
          v_drain {SCHEMA}.executable_admission_events%ROWTYPE;
          v_release {SCHEMA}.executable_admission_events%ROWTYPE;
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'executable intent observation requires a SERIALIZABLE transaction' USING ERRCODE = '25000';
          END IF;
          SELECT executor_role_name INTO v_executor_role FROM {SCHEMA}.executable_admission_authority WHERE singleton_id = 1;
          SELECT observer_role_name INTO v_observer_role FROM {SCHEMA}.executable_observer_authority WHERE singleton_id = 1;
          IF session_user::text NOT IN (v_executor_role, v_observer_role) THEN
            RAISE EXCEPTION 'executable intent observer is not bound' USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') OR p_subject_id IS NULL OR p_subject_incarnation IS NULL OR p_intent_id IS NULL THEN
            RAISE EXCEPTION 'executable intent observer identity is invalid' USING ERRCODE = '42501';
          END IF;
          SELECT * INTO v_state FROM {SCHEMA}.executable_claim_state WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='prepared';
          IF v_state.intent_id IS NULL OR v_prepared.operation_id IS NULL OR v_state.binding IS DISTINCT FROM v_prepared.binding THEN
            RAISE EXCEPTION 'protected executable intent was not found at the exact subject binding' USING ERRCODE = '55000';
          END IF;
          SELECT * INTO v_current FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='worker-registered' ORDER BY protected_registration_epoch DESC,event_id DESC LIMIT 1;
          SELECT * INTO v_drain FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='draining' ORDER BY drain_epoch DESC,event_id DESC LIMIT 1;
          SELECT * INTO v_release FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='released';
          RETURN jsonb_build_object('schema_version',2,'binding',v_state.binding,'bootstrap_registration_epoch',v_prepared.bootstrap_registration_epoch,'worker_id',v_current.worker_id,'worker_incarnation',v_current.worker_incarnation,'protected_registration_epoch',COALESCE(v_current.protected_registration_epoch,0),'claim_high_water',v_state.claim_high_water,'drain',v_drain.receipt,'release',v_release.receipt,'executable',true);
        END $function$
        """
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quoted_executor}, {quoted_observer}"
    )


def downgrade() -> None:
    _executor, quoted_executor = _role("capacity_guard_executor_role")
    _observer, quoted_observer = _role("capacity_guard_observer_role")
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} FROM {quoted_executor}, {quoted_observer}"
    )
    # Restore the exact executor-only observation authority before removing
    # the forward observer relation.  A downgrade must never leave a function
    # whose body references a relation it just dropped.
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.observe_executable_intent(
          p_subject_id uuid, p_subject_incarnation uuid, p_intent_id uuid
        ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
        DECLARE
          v_executor_role text; v_prepared {SCHEMA}.executable_admission_events%ROWTYPE;
          v_current {SCHEMA}.executable_admission_events%ROWTYPE; v_drain {SCHEMA}.executable_admission_events%ROWTYPE;
          v_release {SCHEMA}.executable_admission_events%ROWTYPE; v_state {SCHEMA}.executable_claim_state%ROWTYPE;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN RAISE EXCEPTION 'executable intent observation requires a SERIALIZABLE transaction' USING ERRCODE = '25000'; END IF;
          SELECT executor_role_name INTO v_executor_role FROM {SCHEMA}.executable_admission_authority WHERE singleton_id = 1;
          IF v_executor_role IS NULL OR session_user::text <> v_executor_role THEN RAISE EXCEPTION 'executable intent observer is not the bound executor role' USING ERRCODE = '42501'; END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN RAISE EXCEPTION 'executable intent observer unexpectedly holds owner membership' USING ERRCODE = '42501'; END IF;
          IF p_subject_id IS NULL OR p_subject_incarnation IS NULL OR p_intent_id IS NULL THEN RAISE EXCEPTION 'executable intent observation requires an exact identity' USING ERRCODE = '22023'; END IF;
          SELECT * INTO v_state FROM {SCHEMA}.executable_claim_state WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation FOR KEY SHARE;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='prepared' FOR KEY SHARE;
          IF v_state.intent_id IS NULL OR v_prepared.operation_id IS NULL OR v_state.binding IS DISTINCT FROM v_prepared.binding THEN RAISE EXCEPTION 'protected executable intent was not found at the exact subject binding' USING ERRCODE = '55000'; END IF;
          SELECT * INTO v_current FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='worker-registered' ORDER BY protected_registration_epoch DESC,event_id DESC LIMIT 1 FOR KEY SHARE;
          SELECT * INTO v_drain FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='draining' ORDER BY drain_epoch DESC,event_id DESC LIMIT 1 FOR KEY SHARE;
          SELECT * INTO v_release FROM {SCHEMA}.executable_admission_events WHERE intent_id=p_intent_id AND subject_id=p_subject_id AND subject_incarnation=p_subject_incarnation AND event_kind='released' FOR KEY SHARE;
          RETURN jsonb_build_object('schema_version',2,'binding',v_state.binding,'bootstrap_registration_epoch',v_prepared.bootstrap_registration_epoch,'worker_id',v_current.worker_id,'worker_incarnation',v_current.worker_incarnation,'protected_registration_epoch',COALESCE(v_current.protected_registration_epoch,0),'claim_high_water',v_state.claim_high_water,'drain',v_drain.receipt,'release',v_release.receipt,'executable',true);
        END $function$
        """
    )
    op.execute(
        f"DROP TRIGGER executable_observer_authority_append_only_truncate ON {SCHEMA}.executable_observer_authority"
    )
    op.execute(
        f"DROP TRIGGER executable_observer_authority_append_only_row ON {SCHEMA}.executable_observer_authority"
    )
    op.execute(f"DROP TABLE {SCHEMA}.executable_observer_authority")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quoted_executor}")
