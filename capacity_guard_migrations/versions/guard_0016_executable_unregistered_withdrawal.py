"""Fence unregistered executable physical bindings before pending cancellation.

Revision ID: guard_0016
Revises: guard_0015
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "guard_0016"
down_revision: str | Sequence[str] | None = "guard_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = "withdraw_unregistered_executable_worker(uuid,uuid,jsonb,bytea,text)"


def _role(attribute: str = "capacity_guard_executor_role") -> tuple[str, str]:
    role = op.get_context().config.attributes.get(attribute)
    if not isinstance(role, str) or not role:
        raise RuntimeError("executable withdrawal migration is missing the executor role")
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
          SELECT pg_get_functiondef(
            '{SCHEMA}.{function}'::regprocedure
          ) INTO v_definition;
          IF position('{escaped_old}' in v_definition) = 0 THEN
            RAISE EXCEPTION 'executable withdrawal function clause not found';
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


def _event_kind_check(kinds: str) -> None:
    op.execute(
        f"""
        ALTER TABLE {SCHEMA}.executable_admission_events
        ADD CONSTRAINT guard_exec_event_kind_check
        CHECK (event_kind IN ({kinds}))
        """
    )


def _event_evidence_check(include_withdrawn: bool) -> None:
    withdrawn = (
        " OR "
        "(event_kind = 'withdrawn' "
        " AND bootstrap_registration_epoch > 0 "
        " AND protected_registration_epoch > bootstrap_registration_epoch "
        " AND physical_job_id IS NOT NULL "
        " AND ownership_evidence_sha256 IS NOT NULL "
        " AND worker_id IS NULL AND worker_incarnation IS NULL "
        " AND worker_credential_sha256 IS NULL AND claim_high_water = 0 "
        " AND drain_epoch IS NULL AND release_epoch IS NULL "
        " AND bootstrap_sha256 IS NULL AND bootstrap_revoked = true "
        " AND predecessor_credential_revoked IS NULL "
        " AND worker_credential_revoked IS NULL)"
        if include_withdrawn
        else ""
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
             AND drain_epoch IS NULL AND bootstrap_sha256 IS NULL)
           {withdrawn}) IS TRUE
        )
        """
    )


def _install_withdrawal() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.withdraw_unregistered_executable_worker(
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
          v_operation_id uuid := (p_payload->>'operation_id')::uuid;
          v_intent_id uuid := (p_payload->'binding'->>'intent_id')::uuid;
          v_existing {SCHEMA}.executable_admission_events%ROWTYPE;
          v_prepared {SCHEMA}.executable_admission_events%ROWTYPE;
          v_physical {SCHEMA}.executable_admission_events%ROWTYPE;
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
          v_claim_high_water bigint;
          v_live_claims bigint;
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          v_agent_incarnation := {SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'withdraw', p_payload,
            p_canonical_payload, p_request_digest
          );
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE operation_id = v_operation_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.event_kind <> 'withdrawn'
               OR v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
              RAISE EXCEPTION 'conflicting executable withdrawal replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'prepared' FOR KEY SHARE;
          SELECT * INTO v_physical FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'physical-bound' FOR KEY SHARE;
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
             OR v_physical.operation_id IS NULL
             OR v_state.intent_id IS NULL
             OR v_prepared.binding IS DISTINCT FROM p_payload->'binding'
             OR v_physical.binding IS DISTINCT FROM p_payload->'binding'
             OR v_state.binding IS DISTINCT FROM p_payload->'binding'
             OR v_prepared.bootstrap_registration_epoch IS DISTINCT FROM
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR v_physical.bootstrap_registration_epoch IS DISTINCT FROM
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR v_physical.physical_job_id IS DISTINCT FROM p_payload->>'slurm_job_id'
             OR v_physical.ownership_evidence_sha256 IS DISTINCT FROM
                p_payload->>'ownership_evidence_sha256'
             OR (p_payload->>'protected_registration_epoch')::bigint <=
                v_prepared.bootstrap_registration_epoch
             OR (p_payload->>'expected_claim_high_water')::bigint <> 0
             OR v_claim_high_water <> 0
             OR v_live_claims <> 0
             OR v_state.claim_high_water <> 0
             OR v_state.draining
             OR EXISTS (
               SELECT 1 FROM {SCHEMA}.executable_admission_events
                WHERE intent_id = v_intent_id
                  AND event_kind IN ('worker-registered', 'draining', 'released', 'withdrawn')
             ) THEN
            RAISE EXCEPTION 'unregistered executable withdrawal requires exact unclaimed physical binding'
              USING ERRCODE = '55000';
          END IF;
          UPDATE {SCHEMA}.executable_claim_state SET draining = true
           WHERE intent_id = v_intent_id;
          SELECT count(*) + 1 INTO v_high_water
            FROM {SCHEMA}.executable_admission_events;
          v_receipt := jsonb_build_object(
            'schema_version', 2, 'subject_id', p_subject_id,
            'subject_incarnation', p_subject_incarnation,
            'intent_id', v_intent_id,
            'bootstrap_registration_epoch',
              (p_payload->>'bootstrap_registration_epoch')::bigint,
            'protected_registration_epoch',
              (p_payload->>'protected_registration_epoch')::bigint,
            'slurm_job_id', p_payload->>'slurm_job_id',
            'ownership_evidence_sha256', p_payload->>'ownership_evidence_sha256',
            'claim_high_water', 0, 'live_claim_count', 0,
            'bootstrap_revoked', true,
            'request_digest', p_request_digest,
            'withdrawal_digest', p_request_digest,
            'protected_high_water', v_high_water,
            'withdrawal_state', 'withdrawn', 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_admission_events
            (operation_id, event_kind, agent_incarnation, subject_id,
             subject_incarnation, intent_id, bootstrap_registration_epoch,
             protected_registration_epoch, physical_job_id,
             ownership_evidence_sha256, bootstrap_revoked, claim_high_water,
             binding, request_payload, request_digest, receipt)
          VALUES
            (v_operation_id, 'withdrawn', v_agent_incarnation, p_subject_id,
             p_subject_incarnation, v_intent_id,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             (p_payload->>'protected_registration_epoch')::bigint,
             p_payload->>'slurm_job_id', p_payload->>'ownership_evidence_sha256',
             true, 0, p_payload->'binding', p_payload, p_request_digest,
             v_receipt);
          RETURN v_receipt;
        END
        $function$
        """
    )


def upgrade() -> None:
    _executor, quoted_executor = _role()
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        "WHEN 'drain' THEN ARRAY[\n"
        "              'binding', 'drain_epoch', 'executable',\n"
        "              'expected_claim_high_water', 'operation_id', 'schema_version',\n"
        "              'worker_id', 'worker_incarnation'\n"
        "            ]\n"
        "            WHEN 'release' THEN ARRAY[",
        "WHEN 'drain' THEN ARRAY[\n"
        "              'binding', 'drain_epoch', 'executable',\n"
        "              'expected_claim_high_water', 'operation_id', 'schema_version',\n"
        "              'worker_id', 'worker_incarnation'\n"
        "            ]\n"
        "            WHEN 'withdraw' THEN ARRAY[\n"
        "              'binding', 'bootstrap_registration_epoch', 'executable',\n"
        "              'expected_claim_high_water', 'operation_id',\n"
        "              'ownership_evidence_sha256', 'protected_registration_epoch',\n"
        "              'schema_version', 'slurm_job_id'\n"
        "            ]\n"
        "            WHEN 'release' THEN ARRAY[",
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        "OR (p_operation = 'release' AND (",
        "OR (p_operation = 'withdraw' AND (\n"
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
        "               OR (p_payload->>'expected_claim_high_water' ~ '^[0-9]+$')\n"
        "                  IS DISTINCT FROM true\n"
        "               OR (p_payload->>'expected_claim_high_water')::bigint <> 0\n"
        "               OR jsonb_typeof(p_payload->'slurm_job_id') IS DISTINCT FROM 'string'\n"
        "               OR (p_payload->>'slurm_job_id' ~ '^[a-z0-9][a-z0-9_.-]{0,127}$')\n"
        "                  IS DISTINCT FROM true\n"
        "               OR jsonb_typeof(p_payload->'ownership_evidence_sha256')\n"
        "                  IS DISTINCT FROM 'string'\n"
        "               OR (p_payload->>'ownership_evidence_sha256' ~ '^[0-9a-f]{64}$')\n"
        "                  IS DISTINCT FROM true\n"
        "             ))\n"
        "             OR (p_operation = 'release' AND (",
    )
    _replace_function_clause(
        "register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
        "AND event_kind IN ('draining', 'released')) THEN\n"
        "            RAISE EXCEPTION 'protected release fence forbids delayed registration'",
        "AND event_kind IN ('draining', 'released', 'withdrawn')) THEN\n"
        "            RAISE EXCEPTION 'protected withdrawal or release fence forbids delayed registration'",
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_kind_check"
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_evidence_check"
    )
    _event_kind_check(
        "'prepared', 'physical-bound', 'worker-registered', 'draining', 'released', 'withdrawn'"
    )
    _event_evidence_check(include_withdrawn=True)
    op.execute(
        f"CREATE UNIQUE INDEX guard_exec_withdrawn_intent_key "
        f"ON {SCHEMA}.executable_admission_events (intent_id) "
        "WHERE event_kind = 'withdrawn'"
    )
    _install_withdrawal()
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
             WHERE event_kind = 'withdrawn'
          ) THEN
            RAISE EXCEPTION 'cannot downgrade guard_0016 with executable withdrawal evidence';
          END IF;
        END $$;
        """
    )
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} FROM {quoted_executor}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
    op.execute(f"DROP INDEX {SCHEMA}.guard_exec_withdrawn_intent_key")
    _replace_function_clause(
        "register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
        "AND event_kind IN ('draining', 'released', 'withdrawn')) THEN\n"
        "            RAISE EXCEPTION 'protected withdrawal or release fence forbids delayed registration'",
        "AND event_kind IN ('draining', 'released')) THEN\n"
        "            RAISE EXCEPTION 'protected release fence forbids delayed registration'",
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        "WHEN 'drain' THEN ARRAY[\n"
        "              'binding', 'drain_epoch', 'executable',\n"
        "              'expected_claim_high_water', 'operation_id', 'schema_version',\n"
        "              'worker_id', 'worker_incarnation'\n"
        "            ]\n"
        "            WHEN 'withdraw' THEN ARRAY[\n"
        "              'binding', 'bootstrap_registration_epoch', 'executable',\n"
        "              'expected_claim_high_water', 'operation_id',\n"
        "              'ownership_evidence_sha256', 'protected_registration_epoch',\n"
        "              'schema_version', 'slurm_job_id'\n"
        "            ]\n"
        "            WHEN 'release' THEN ARRAY[",
        "WHEN 'drain' THEN ARRAY[\n"
        "              'binding', 'drain_epoch', 'executable',\n"
        "              'expected_claim_high_water', 'operation_id', 'schema_version',\n"
        "              'worker_id', 'worker_incarnation'\n"
        "            ]\n"
        "            WHEN 'release' THEN ARRAY[",
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        "OR (p_operation = 'withdraw' AND (\n"
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
        "               OR (p_payload->>'expected_claim_high_water' ~ '^[0-9]+$')\n"
        "                  IS DISTINCT FROM true\n"
        "               OR (p_payload->>'expected_claim_high_water')::bigint <> 0\n"
        "               OR jsonb_typeof(p_payload->'slurm_job_id') IS DISTINCT FROM 'string'\n"
        "               OR (p_payload->>'slurm_job_id' ~ '^[a-z0-9][a-z0-9_.-]{0,127}$')\n"
        "                  IS DISTINCT FROM true\n"
        "               OR jsonb_typeof(p_payload->'ownership_evidence_sha256')\n"
        "                  IS DISTINCT FROM 'string'\n"
        "               OR (p_payload->>'ownership_evidence_sha256' ~ '^[0-9a-f]{64}$')\n"
        "                  IS DISTINCT FROM true\n"
        "             ))\n"
        "             OR (p_operation = 'release' AND (",
        "OR (p_operation = 'release' AND (",
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_kind_check"
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.executable_admission_events "
        "DROP CONSTRAINT guard_exec_event_evidence_check"
    )
    _event_kind_check("'prepared', 'physical-bound', 'worker-registered', 'draining', 'released'")
    _event_evidence_check(include_withdrawn=False)
