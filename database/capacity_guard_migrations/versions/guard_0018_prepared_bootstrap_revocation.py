"""Revoke prepared executable bootstraps before physical submission.

Revision ID: guard_0018
Revises: guard_0017
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "guard_0018"
down_revision: str | Sequence[str] | None = "guard_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = "revoke_prepared_executable_bootstrap(uuid,uuid,jsonb,bytea,text)"


def _role(attribute: str = "capacity_guard_executor_role") -> tuple[str, str]:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("prepared revocation migration is missing its Alembic config")
    role = config.attributes.get(attribute)
    if not isinstance(role, str) or not role:
        raise RuntimeError("prepared revocation migration is missing the executor role")
    return role, op.get_bind().dialect.identifier_preparer.quote(role)


def _replace_function_clause(function: str, old: str, new: str) -> None:
    escaped_old = old.replace("'", "''")
    escaped_new = new.replace("'", "''")
    op.execute(
        f"""
        DO $$
        DECLARE
          v_definition text;
        BEGIN
          SELECT pg_get_functiondef('{SCHEMA}.{function}'::regprocedure)
            INTO v_definition;
          IF position('{escaped_old}' in v_definition) = 0 THEN
            RAISE EXCEPTION 'prepared revocation function clause not found';
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


def _event_constraints(*, include_revoked: bool) -> None:
    kinds = "'prepared', 'physical-bound', 'worker-registered', 'draining', 'released', 'withdrawn'"
    revoked = ""
    if include_revoked:
        kinds += ", 'prepared-revoked'"
        revoked = (
            " OR (event_kind = 'prepared-revoked' "
            "AND bootstrap_registration_epoch > 0 "
            "AND protected_registration_epoch > bootstrap_registration_epoch "
            "AND physical_job_id IS NULL AND ownership_evidence_sha256 IS NULL "
            "AND worker_id IS NULL AND worker_incarnation IS NULL "
            "AND worker_credential_sha256 IS NULL AND claim_high_water = 0 "
            "AND drain_epoch IS NULL AND release_epoch IS NULL "
            "AND bootstrap_sha256 IS NULL AND bootstrap_revoked = true "
            "AND predecessor_credential_revoked IS NULL "
            "AND worker_credential_revoked IS NULL)"
        )
    op.execute(
        f"""
        ALTER TABLE {SCHEMA}.executable_admission_events
        ADD CONSTRAINT guard_exec_event_kind_check CHECK (event_kind IN ({kinds}))
        """
    )
    op.execute(
        f"""
        ALTER TABLE {SCHEMA}.executable_admission_events
        ADD CONSTRAINT guard_exec_event_evidence_check
        CHECK (
          ((event_kind = 'prepared'
             AND bootstrap_registration_epoch > 0 AND bootstrap_sha256 IS NOT NULL
             AND protected_registration_epoch IS NULL AND physical_job_id IS NULL
             AND worker_id IS NULL AND worker_incarnation IS NULL
             AND claim_high_water IS NULL AND drain_epoch IS NULL AND release_epoch IS NULL
             AND bootstrap_revoked IS NULL AND predecessor_credential_revoked IS NULL
             AND worker_credential_revoked IS NULL) OR
           (event_kind = 'physical-bound'
             AND bootstrap_registration_epoch > 0 AND physical_job_id IS NOT NULL
             AND ownership_evidence_sha256 IS NOT NULL
             AND protected_registration_epoch IS NULL AND worker_id IS NULL
             AND worker_incarnation IS NULL AND claim_high_water IS NULL
             AND drain_epoch IS NULL AND release_epoch IS NULL
             AND bootstrap_sha256 IS NULL AND bootstrap_revoked IS NULL
             AND predecessor_credential_revoked IS NULL
             AND worker_credential_revoked IS NULL) OR
           (event_kind = 'worker-registered'
             AND bootstrap_registration_epoch > 0 AND protected_registration_epoch > 0
             AND physical_job_id IS NOT NULL AND worker_id IS NOT NULL
             AND worker_incarnation IS NOT NULL AND worker_credential_sha256 IS NOT NULL
             AND bootstrap_revoked = true AND predecessor_credential_revoked IS NOT NULL
             AND worker_credential_revoked = false AND claim_high_water IS NULL
             AND drain_epoch IS NULL AND release_epoch IS NULL
             AND bootstrap_sha256 IS NULL) OR
           (event_kind = 'draining'
             AND worker_id IS NOT NULL AND worker_incarnation IS NOT NULL
             AND claim_high_water >= 0 AND drain_epoch > 0
             AND bootstrap_registration_epoch IS NULL
             AND protected_registration_epoch IS NULL AND physical_job_id IS NULL
             AND worker_credential_sha256 IS NULL AND release_epoch IS NULL
             AND bootstrap_sha256 IS NULL AND bootstrap_revoked IS NULL
             AND predecessor_credential_revoked IS NULL
             AND worker_credential_revoked IS NULL) OR
           (event_kind = 'released'
             AND bootstrap_registration_epoch > 0 AND protected_registration_epoch > 0
             AND worker_id IS NOT NULL AND worker_incarnation IS NOT NULL
             AND worker_credential_sha256 IS NOT NULL AND claim_high_water >= 0
             AND release_epoch > 0 AND bootstrap_revoked = true
             AND worker_credential_revoked = true
             AND predecessor_credential_revoked IS NULL AND physical_job_id IS NULL
             AND drain_epoch IS NULL AND bootstrap_sha256 IS NULL) OR
           (event_kind = 'withdrawn'
             AND bootstrap_registration_epoch > 0
             AND protected_registration_epoch > bootstrap_registration_epoch
             AND physical_job_id IS NOT NULL AND ownership_evidence_sha256 IS NOT NULL
             AND worker_id IS NULL AND worker_incarnation IS NULL
             AND worker_credential_sha256 IS NULL AND claim_high_water = 0
             AND drain_epoch IS NULL AND release_epoch IS NULL
             AND bootstrap_sha256 IS NULL AND bootstrap_revoked = true
             AND predecessor_credential_revoked IS NULL
             AND worker_credential_revoked IS NULL)
           {revoked}) IS TRUE
        )
        """
    )


def _install_revocation() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.revoke_prepared_executable_bootstrap(
          p_subject_id uuid,
          p_subject_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_request_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_incarnation uuid;
          v_reporter_incarnation uuid;
          v_operation_id uuid := (p_payload->>'operation_id')::uuid;
          v_intent_id uuid := (p_payload->'binding'->>'intent_id')::uuid;
          v_existing {SCHEMA}.executable_admission_events%ROWTYPE;
          v_prepared {SCHEMA}.executable_admission_events%ROWTYPE;
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
          v_claim_high_water bigint;
          v_live_claims bigint;
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          v_agent_incarnation := {SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'revoke-prepared', p_payload,
            p_canonical_payload, p_request_digest
          );
          SELECT reporter_incarnation INTO v_reporter_incarnation
            FROM {SCHEMA}.agent_registrations
           WHERE agent_incarnation = v_agent_incarnation;
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE operation_id = v_operation_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.event_kind <> 'prepared-revoked'
               OR v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
              RAISE EXCEPTION 'conflicting prepared bootstrap revocation replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'prepared' FOR KEY SHARE;
          SELECT * INTO v_state FROM {SCHEMA}.executable_claim_state
           WHERE intent_id = v_intent_id FOR UPDATE;
          SELECT count(lease.operation_id),
                 count(lease.operation_id) FILTER (
                   WHERE terminal.admitted_operation_id IS NULL
                 )
            INTO v_claim_high_water, v_live_claims
            FROM {SCHEMA}.executable_claim_leases AS lease
            LEFT JOIN {SCHEMA}.executable_claim_terminal_events AS terminal
              ON terminal.admitted_operation_id = lease.operation_id
             AND terminal.protected_attempt_id = lease.protected_attempt_id
             AND terminal.execution_generation = lease.execution_generation
             AND terminal.requirements_digest = lease.requirements_digest
             AND terminal.intent_id = lease.intent_id
             AND terminal.subject_id = lease.subject_id
             AND terminal.subject_incarnation = lease.subject_incarnation
             AND terminal.worker_id = lease.worker_id
             AND terminal.worker_incarnation = lease.worker_incarnation
             AND terminal.claim_high_water = lease.claim_high_water
             AND terminal.terminal_state = 'cancelled-terminal'
             AND terminal.executable = true
           WHERE lease.intent_id = v_intent_id;
          IF v_prepared.operation_id IS NULL
             OR v_state.intent_id IS NULL
             OR v_reporter_incarnation IS NULL
             OR v_prepared.binding IS DISTINCT FROM p_payload->'binding'
             OR v_state.binding IS DISTINCT FROM p_payload->'binding'
             OR v_prepared.bootstrap_registration_epoch IS DISTINCT FROM
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR (p_payload->>'protected_registration_epoch')::bigint <=
                v_prepared.bootstrap_registration_epoch
             OR (p_payload->>'expected_claim_high_water')::bigint <> 0
             OR v_claim_high_water <> 0 OR v_live_claims <> 0
             OR v_state.claim_high_water <> 0 OR v_state.draining
             OR EXISTS (
               SELECT 1 FROM {SCHEMA}.executable_admission_events
                WHERE intent_id = v_intent_id
                  AND event_kind IN (
                    'physical-bound', 'worker-registered', 'draining', 'released',
                    'withdrawn', 'prepared-revoked'
                  )
             ) THEN
            RAISE EXCEPTION 'prepared bootstrap revocation requires exact unbound unclaimed evidence'
              USING ERRCODE = '55000';
          END IF;
          UPDATE {SCHEMA}.executable_claim_state SET draining = true
           WHERE intent_id = v_intent_id;
          SELECT count(*) + 1 INTO v_high_water
            FROM {SCHEMA}.executable_admission_events;
          v_receipt := jsonb_build_object(
            'schema_version', 2, 'binding', p_payload->'binding',
            'reporter_incarnation', v_reporter_incarnation::text,
            'bootstrap_registration_epoch',
              (p_payload->>'bootstrap_registration_epoch')::bigint,
            'protected_registration_epoch',
              (p_payload->>'protected_registration_epoch')::bigint,
            'claim_high_water', 0, 'live_claim_count', 0,
            'bootstrap_revoked', true, 'request_digest', p_request_digest,
            'protected_release_sha256', p_request_digest,
            'protected_high_water', v_high_water,
            'revocation_state', 'revoked', 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_admission_events
            (operation_id, event_kind, agent_incarnation, subject_id,
             subject_incarnation, intent_id, bootstrap_registration_epoch,
             protected_registration_epoch, bootstrap_revoked, claim_high_water,
             binding, request_payload, request_digest, receipt)
          VALUES
            (v_operation_id, 'prepared-revoked', v_agent_incarnation, p_subject_id,
             p_subject_incarnation, v_intent_id,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             (p_payload->>'protected_registration_epoch')::bigint,
             true, 0, p_payload->'binding', p_payload, p_request_digest, v_receipt);
          RETURN v_receipt;
        END
        $function$
        """
    )


def _install_observe_executable_intent(
    *,
    allow_observer: bool,
    include_prepared_revocation: bool,
) -> None:
    _executor, quoted_executor = _role("capacity_guard_executor_role")
    if allow_observer:
        _observer, quoted_observer = _role("capacity_guard_observer_role")
    prepared_declaration = (
        f"          v_prepared_revocation {SCHEMA}.executable_admission_events%ROWTYPE;\n"
        if include_prepared_revocation
        else ""
    )
    prepared_query = (
        f"""
          SELECT * INTO v_prepared_revocation
            FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'prepared-revoked'
           FOR KEY SHARE;
        """
        if include_prepared_revocation
        else ""
    )
    prepared_field = (
        "            'prepared_revocation', v_prepared_revocation.receipt,\n"
        if include_prepared_revocation
        else ""
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.observe_executable_intent(
          p_subject_id uuid,
          p_subject_incarnation uuid,
          p_intent_id uuid
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_executor_role text;
          v_observer_role text;
          v_prepared {SCHEMA}.executable_admission_events%ROWTYPE;
          v_current {SCHEMA}.executable_admission_events%ROWTYPE;
          v_drain {SCHEMA}.executable_admission_events%ROWTYPE;
          v_release {SCHEMA}.executable_admission_events%ROWTYPE;
{prepared_declaration.rstrip()}
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'executable intent observation requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT executor_role_name INTO v_executor_role
            FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1;
        """
        + (
            f"""
          SELECT observer_role_name INTO v_observer_role
            FROM {SCHEMA}.executable_observer_authority
           WHERE singleton_id = 1;
          IF session_user::text NOT IN (v_executor_role, v_observer_role) THEN
            RAISE EXCEPTION 'executable intent observer is not bound'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER')
             OR p_subject_id IS NULL
             OR p_subject_incarnation IS NULL
             OR p_intent_id IS NULL THEN
            RAISE EXCEPTION 'executable intent observer identity is invalid'
              USING ERRCODE = '42501';
          END IF;
        """
            if allow_observer
            else """
          IF v_executor_role IS NULL OR session_user::text <> v_executor_role THEN
            RAISE EXCEPTION 'executable intent observer is not the bound executor role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'executable intent observer unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF p_subject_id IS NULL OR p_subject_incarnation IS NULL OR p_intent_id IS NULL THEN
            RAISE EXCEPTION 'executable intent observation requires an exact identity'
              USING ERRCODE = '22023';
          END IF;
        """
        )
        + f"""
          SELECT * INTO v_state FROM {SCHEMA}.executable_claim_state
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
           FOR KEY SHARE;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'prepared'
           FOR KEY SHARE;
          IF v_state.intent_id IS NULL
             OR v_prepared.operation_id IS NULL
             OR v_state.binding IS DISTINCT FROM v_prepared.binding THEN
            RAISE EXCEPTION 'protected executable intent was not found at the exact subject binding'
              USING ERRCODE = '55000';
          END IF;
          SELECT * INTO v_current FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'worker-registered'
           ORDER BY protected_registration_epoch DESC, event_id DESC
           LIMIT 1 FOR KEY SHARE;
          SELECT * INTO v_drain FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'draining'
           ORDER BY drain_epoch DESC, event_id DESC
           LIMIT 1 FOR KEY SHARE;
          SELECT * INTO v_release FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'released'
           FOR KEY SHARE;
{prepared_query.rstrip()}

          RETURN jsonb_build_object(
            'schema_version', 2,
            'binding', v_state.binding,
            'bootstrap_registration_epoch', v_prepared.bootstrap_registration_epoch,
            'worker_id', v_current.worker_id,
            'worker_incarnation', v_current.worker_incarnation,
            'protected_registration_epoch',
              COALESCE(v_current.protected_registration_epoch, 0),
            'claim_high_water', v_state.claim_high_water,
            'drain', v_drain.receipt,
            'release', v_release.receipt,
{prepared_field.rstrip()}
            'executable', true
          );
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.observe_executable_intent(uuid,uuid,uuid) FROM PUBLIC"
    )
    if allow_observer:
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {SCHEMA}.observe_executable_intent(uuid,uuid,uuid) "
            f"TO {quoted_executor}, {quoted_observer}"
        )
    else:
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {SCHEMA}.observe_executable_intent(uuid,uuid,uuid) "
            f"TO {quoted_executor}"
        )


_ASSERT_ALLOWED_OLD = (
    "WHEN 'withdraw' THEN ARRAY[\n"
    "              'binding', 'bootstrap_registration_epoch', 'executable',\n"
    "              'expected_claim_high_water', 'operation_id',\n"
    "              'ownership_evidence_sha256', 'protected_registration_epoch',\n"
    "              'schema_version', 'slurm_job_id'\n"
    "            ]\n"
    "            WHEN 'release' THEN ARRAY["
)
_ASSERT_ALLOWED_NEW = (
    "WHEN 'withdraw' THEN ARRAY[\n"
    "              'binding', 'bootstrap_registration_epoch', 'executable',\n"
    "              'expected_claim_high_water', 'operation_id',\n"
    "              'ownership_evidence_sha256', 'protected_registration_epoch',\n"
    "              'schema_version', 'slurm_job_id'\n"
    "            ]\n"
    "            WHEN 'revoke-prepared' THEN ARRAY[\n"
    "              'binding', 'bootstrap_registration_epoch', 'executable',\n"
    "              'expected_claim_high_water', 'operation_id',\n"
    "              'protected_registration_epoch', 'schema_version'\n"
    "            ]\n"
    "            WHEN 'release' THEN ARRAY["
)
_ASSERT_VALIDATION_OLD = "OR (p_operation = 'release' AND ("
_ASSERT_VALIDATION_NEW = (
    "OR (p_operation = 'revoke-prepared' AND (\n"
    "               jsonb_typeof(p_payload->'operation_id') IS DISTINCT FROM 'string'\n"
    "               OR (p_payload->>'operation_id' ~\n"
    "                  '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')\n"
    "                  IS DISTINCT FROM true\n"
    "               OR jsonb_typeof(p_payload->'bootstrap_registration_epoch')\n"
    "                  IS DISTINCT FROM 'number'\n"
    "               OR (p_payload->>'bootstrap_registration_epoch' ~ '^[1-9][0-9]*$')\n"
    "                  IS DISTINCT FROM true\n"
    "               OR (p_payload->>'bootstrap_registration_epoch')::numeric >\n"
    "                  9223372036854775807\n"
    "               OR jsonb_typeof(p_payload->'protected_registration_epoch')\n"
    "                  IS DISTINCT FROM 'number'\n"
    "               OR (p_payload->>'protected_registration_epoch' ~ '^[1-9][0-9]*$')\n"
    "                  IS DISTINCT FROM true\n"
    "               OR (p_payload->>'protected_registration_epoch')::numeric >\n"
    "                  9223372036854775807\n"
    "               OR jsonb_typeof(p_payload->'expected_claim_high_water')\n"
    "                  IS DISTINCT FROM 'number'\n"
    "               OR (p_payload->>'expected_claim_high_water')::bigint <> 0\n"
    "             ))\n"
    "             OR (p_operation = 'release' AND ("
)
_BIND_OLD = "AND event_kind IN ('physical-bound', 'released')) THEN"
_BIND_NEW = "AND event_kind IN ('physical-bound', 'released', 'prepared-revoked')) THEN"
_BIND_MESSAGE_OLD = "RAISE EXCEPTION 'conflicting executable physical binding'"
_BIND_MESSAGE_NEW = "RAISE EXCEPTION 'prepared revoked executable fence forbids physical binding'"
_REGISTER_OLD = "AND event_kind IN ('draining', 'released', 'withdrawn')) THEN"
_REGISTER_NEW = "AND event_kind IN ('draining', 'released', 'withdrawn', 'prepared-revoked')) THEN"
_REGISTER_MESSAGE_OLD = (
    "RAISE EXCEPTION 'protected withdrawal or release fence forbids delayed registration'"
)
_REGISTER_MESSAGE_NEW = (
    "RAISE EXCEPTION "
    "'protected withdrawal, prepared revoked, or release fence forbids delayed registration'"
)
_OBSERVE_DECLARE_OLD = (
    f"v_release {SCHEMA}.executable_admission_events%ROWTYPE;\n"
    f"          v_state {SCHEMA}.executable_claim_state%ROWTYPE;"
)
_OBSERVE_DECLARE_NEW = (
    f"v_release {SCHEMA}.executable_admission_events%ROWTYPE;\n"
    f"          v_prepared_revocation {SCHEMA}.executable_admission_events%ROWTYPE;\n"
    f"          v_state {SCHEMA}.executable_claim_state%ROWTYPE;"
)
_OBSERVE_RETURN_OLD = "'release', v_release.receipt,\n            'executable', true"
_OBSERVE_RETURN_NEW = (
    "'release', v_release.receipt,\n"
    "            'prepared_revocation', v_prepared_revocation.receipt,\n"
    "            'executable', true"
)


def upgrade() -> None:
    _executor, quoted_executor = _role()
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        _ASSERT_ALLOWED_OLD,
        _ASSERT_ALLOWED_NEW,
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        _ASSERT_VALIDATION_OLD,
        _ASSERT_VALIDATION_NEW,
    )
    _replace_function_clause(
        "bind_executable_slurm_job(uuid,uuid,jsonb,bytea,text)",
        _BIND_OLD,
        _BIND_NEW,
    )
    _replace_function_clause(
        "bind_executable_slurm_job(uuid,uuid,jsonb,bytea,text)",
        _BIND_MESSAGE_OLD,
        _BIND_MESSAGE_NEW,
    )
    _replace_function_clause(
        "register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
        _REGISTER_OLD,
        _REGISTER_NEW,
    )
    _replace_function_clause(
        "register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
        _REGISTER_MESSAGE_OLD,
        _REGISTER_MESSAGE_NEW,
    )
    _install_observe_executable_intent(
        allow_observer=True,
        include_prepared_revocation=True,
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_kind_check"
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_evidence_check"
    )
    _event_constraints(include_revoked=True)
    op.execute(
        f"CREATE UNIQUE INDEX guard_exec_prepared_revoked_intent_key "
        f"ON {SCHEMA}.executable_admission_events (intent_id) "
        "WHERE event_kind = 'prepared-revoked'"
    )
    _install_revocation()
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quoted_executor}")


def downgrade() -> None:
    _executor, quoted_executor = _role()
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.executable_admission_events
             WHERE event_kind = 'prepared-revoked'
          ) THEN
            RAISE EXCEPTION 'cannot downgrade guard_0018 with prepared revocation evidence';
          END IF;
        END $$;
        """
    )
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} FROM {quoted_executor}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
    op.execute(f"DROP INDEX {SCHEMA}.guard_exec_prepared_revoked_intent_key")
    _install_observe_executable_intent(
        allow_observer=True,
        include_prepared_revocation=False,
    )
    _replace_function_clause(
        "register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
        _REGISTER_NEW,
        _REGISTER_OLD,
    )
    _replace_function_clause(
        "register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
        _REGISTER_MESSAGE_NEW,
        _REGISTER_MESSAGE_OLD,
    )
    _replace_function_clause(
        "bind_executable_slurm_job(uuid,uuid,jsonb,bytea,text)",
        _BIND_MESSAGE_NEW,
        _BIND_MESSAGE_OLD,
    )
    _replace_function_clause(
        "bind_executable_slurm_job(uuid,uuid,jsonb,bytea,text)",
        _BIND_NEW,
        _BIND_OLD,
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        _ASSERT_VALIDATION_NEW,
        _ASSERT_VALIDATION_OLD,
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        _ASSERT_ALLOWED_NEW,
        _ASSERT_ALLOWED_OLD,
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_kind_check"
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_evidence_check"
    )
    _event_constraints(include_revoked=False)
