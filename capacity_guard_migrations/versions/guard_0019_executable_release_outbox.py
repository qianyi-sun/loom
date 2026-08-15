"""Expose executable protected release publications to the agent.

Revision ID: guard_0019
Revises: guard_0018
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "guard_0019"
down_revision: str | Sequence[str] | None = "guard_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
CANONICAL_FUNCTION = "canonical_executable_publication_payload(jsonb)"
READ_FUNCTION = "read_next_executable_protected_release(uuid)"
ACK_FUNCTION = (
    "acknowledge_executable_protected_release_publication(uuid,bigint,jsonb,bytea,text,text)"
)
OBSERVATION_FUNCTION = "observe_executable_intent(uuid,uuid,uuid)"


def _role(attribute: str = "capacity_guard_agent_role") -> tuple[str, str]:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("executable release outbox migration is missing its Alembic config")
    role = config.attributes.get(attribute)
    if not isinstance(role, str) or not role:
        raise RuntimeError("executable release outbox migration is missing the agent role")
    return role, op.get_bind().dialect.identifier_preparer.quote(role)


def _install_canonical_payload_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.canonical_executable_publication_payload(
          p_payload jsonb
        )
        RETURNS text
        LANGUAGE plpgsql
        IMMUTABLE
        STRICT
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_rendered text;
        BEGIN
          CASE jsonb_typeof(p_payload)
            WHEN 'object' THEN
              SELECT string_agg(
                       to_jsonb(entry.key)::text || ':' ||
                         {SCHEMA}.canonical_executable_publication_payload(entry.value),
                       ',' ORDER BY entry.key
                     )
                INTO v_rendered
                FROM jsonb_each(p_payload) AS entry(key, value);
              RETURN '{{' || COALESCE(v_rendered, '') || '}}';
            WHEN 'array' THEN
              SELECT string_agg(
                       {SCHEMA}.canonical_executable_publication_payload(entry.value),
                       ',' ORDER BY entry.ordinality
                     )
                INTO v_rendered
                FROM jsonb_array_elements(p_payload) WITH ORDINALITY AS entry(value, ordinality);
              RETURN '[' || COALESCE(v_rendered, '') || ']';
            ELSE
              RETURN p_payload::text;
          END CASE;
        END
        $function$
        """
    )


def _install_read_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_next_executable_protected_release(
          p_agent_incarnation uuid
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_last_event_id bigint;
          v_event record;
          v_release jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'executable protected release outbox requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'executable protected release caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'executable protected release agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF p_agent_incarnation IS NULL THEN
            RAISE EXCEPTION 'executable protected release agent incarnation is required'
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
            RAISE EXCEPTION 'executable protected release agent incarnation is not registered'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.executable_release_publication_state
            (agent_incarnation, last_event_id)
          VALUES (p_agent_incarnation, 0)
          ON CONFLICT (agent_incarnation) DO NOTHING;
          SELECT last_event_id INTO v_last_event_id
            FROM {SCHEMA}.executable_release_publication_state
           WHERE agent_incarnation = p_agent_incarnation
           FOR KEY SHARE;

          SELECT e.event_id, e.event_kind, e.binding,
                 e.bootstrap_registration_epoch, e.protected_registration_epoch,
                 CASE
                   WHEN e.event_kind = 'withdrawn' THEN e.receipt->>'withdrawal_digest'
                   ELSE e.receipt->>'protected_release_sha256'
                 END AS protected_release_sha256
            INTO v_event
            FROM {SCHEMA}.executable_admission_events AS e
           WHERE e.agent_incarnation = p_agent_incarnation
             AND e.subject_id = v_registration.subject_id
             AND e.subject_incarnation = v_registration.subject_incarnation
             AND e.event_kind IN ('released', 'withdrawn', 'prepared-revoked')
             AND e.event_id > v_last_event_id
           ORDER BY e.event_id
           LIMIT 1
           FOR KEY SHARE;
          IF NOT FOUND THEN
            RETURN NULL;
          END IF;
          IF v_event.protected_release_sha256 IS NULL
             OR v_event.protected_release_sha256 !~ '^[0-9a-f]{{64}}$' THEN
            RAISE EXCEPTION 'executable protected release digest is invalid'
              USING ERRCODE = '55000';
          END IF;
          IF v_event.binding->>'subject_id' IS DISTINCT FROM v_registration.subject_id::text
             OR v_event.binding->>'subject_incarnation'
                IS DISTINCT FROM v_registration.subject_incarnation::text
             OR (v_event.binding->>'deployment_generation')::bigint
                IS DISTINCT FROM v_registration.deployment_generation
             OR v_event.binding->'candidate'->>'algorithm'
                IS DISTINCT FROM v_registration.candidate_identity_algorithm
             OR v_event.binding->'candidate'->>'identity'
                IS DISTINCT FROM v_registration.candidate_identity
             OR v_event.binding->'candidate'->>'publication_sha256'
                IS DISTINCT FROM v_registration.candidate_publication_sha256 THEN
            RAISE EXCEPTION 'executable protected release event binding is stale'
              USING ERRCODE = '55000';
          END IF;
          v_release := jsonb_build_object(
            'schema_version', 2,
            'binding', v_event.binding,
            'reporter_incarnation', v_registration.reporter_incarnation::text,
            'bootstrap_registration_epoch', v_event.bootstrap_registration_epoch,
            'protected_registration_epoch', v_event.protected_registration_epoch,
            'bootstrap_revoked', true,
            'protected_release_sha256', v_event.protected_release_sha256,
            'executable', true
          );
          RETURN jsonb_build_object(
            'schema_version', 2,
            'event_id', v_event.event_id,
            'event_kind', v_event.event_kind,
            'release', v_release
          );
        END
        $function$
        """
    )


def _install_ack_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.acknowledge_executable_protected_release_publication(
          p_agent_incarnation uuid,
          p_admission_event_id bigint,
          p_publication_payload jsonb,
          p_canonical_payload bytea,
          p_publication_digest text,
          p_manager_acknowledgement_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_last_event_id bigint;
          v_existing {SCHEMA}.executable_release_publication_events%ROWTYPE;
          v_existing_kind text;
          v_event record;
          v_release jsonb;
          v_updated bigint;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'executable protected release acknowledgement requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'executable protected release caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'executable protected release agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF p_agent_incarnation IS NULL OR p_admission_event_id IS NULL
             OR p_admission_event_id <= 0
             OR jsonb_typeof(p_publication_payload) IS DISTINCT FROM 'object'
             OR octet_length(p_publication_payload::text) > 8388608
             OR p_canonical_payload IS NULL
             OR octet_length(p_canonical_payload) > 8388608
             OR convert_from(p_canonical_payload, 'UTF8')::jsonb
                IS DISTINCT FROM p_publication_payload
             OR p_publication_digest IS NULL
             OR p_publication_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_canonical_payload), 'hex')
                IS DISTINCT FROM p_publication_digest
             OR p_manager_acknowledgement_digest IS NULL
             OR p_manager_acknowledgement_digest !~ '^[0-9a-f]{{64}}$'
             OR p_publication_payload->>'protected_release_sha256' !~ '^[0-9a-f]{{64}}$' THEN
            RAISE EXCEPTION 'executable protected release acknowledgement is invalid'
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
            RAISE EXCEPTION 'executable protected release agent incarnation is not registered'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.executable_release_publication_state
            (agent_incarnation, last_event_id)
          VALUES (p_agent_incarnation, 0)
          ON CONFLICT (agent_incarnation) DO NOTHING;
          SELECT last_event_id INTO v_last_event_id
            FROM {SCHEMA}.executable_release_publication_state
           WHERE agent_incarnation = p_agent_incarnation
           FOR UPDATE;

          SELECT * INTO v_existing
            FROM {SCHEMA}.executable_release_publication_events
           WHERE admission_event_id = p_admission_event_id
           FOR KEY SHARE;
          IF FOUND THEN
            SELECT event_kind INTO v_existing_kind
              FROM {SCHEMA}.executable_admission_events
             WHERE event_id = p_admission_event_id;
            IF v_existing.agent_incarnation IS DISTINCT FROM p_agent_incarnation
               OR v_existing.publication_payload IS DISTINCT FROM p_publication_payload
               OR v_existing.publication_canonical_payload
                  IS DISTINCT FROM p_canonical_payload
               OR v_existing.publication_digest IS DISTINCT FROM p_publication_digest
               OR v_existing.manager_acknowledgement_digest IS DISTINCT FROM
                  p_manager_acknowledgement_digest THEN
              RAISE EXCEPTION 'conflicting executable protected release publication replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN jsonb_build_object(
              'schema_version', 2,
              'event_id', v_existing.admission_event_id,
              'event_kind', v_existing_kind,
              'publication_digest', v_existing.publication_digest,
              'manager_acknowledgement_digest',
                v_existing.manager_acknowledgement_digest
            );
          END IF;

          SELECT e.event_id, e.event_kind, e.binding,
                 e.bootstrap_registration_epoch, e.protected_registration_epoch,
                 CASE
                   WHEN e.event_kind = 'withdrawn' THEN e.receipt->>'withdrawal_digest'
                   ELSE e.receipt->>'protected_release_sha256'
                 END AS protected_release_sha256
            INTO v_event
            FROM {SCHEMA}.executable_admission_events AS e
           WHERE e.agent_incarnation = p_agent_incarnation
             AND e.subject_id = v_registration.subject_id
             AND e.subject_incarnation = v_registration.subject_incarnation
             AND e.event_kind IN ('released', 'withdrawn', 'prepared-revoked')
             AND e.event_id > v_last_event_id
           ORDER BY e.event_id
           LIMIT 1
           FOR KEY SHARE;
          IF NOT FOUND OR v_event.event_id <> p_admission_event_id THEN
            RAISE EXCEPTION 'executable protected release publication must acknowledge the next event'
              USING ERRCODE = '55000';
          END IF;
          IF v_event.protected_release_sha256 IS NULL
             OR v_event.protected_release_sha256 !~ '^[0-9a-f]{{64}}$' THEN
            RAISE EXCEPTION 'executable protected release digest is invalid'
              USING ERRCODE = '55000';
          END IF;
          IF v_event.binding->>'subject_id' IS DISTINCT FROM v_registration.subject_id::text
             OR v_event.binding->>'subject_incarnation'
                IS DISTINCT FROM v_registration.subject_incarnation::text
             OR (v_event.binding->>'deployment_generation')::bigint
                IS DISTINCT FROM v_registration.deployment_generation
             OR v_event.binding->'candidate'->>'algorithm'
                IS DISTINCT FROM v_registration.candidate_identity_algorithm
             OR v_event.binding->'candidate'->>'identity'
                IS DISTINCT FROM v_registration.candidate_identity
             OR v_event.binding->'candidate'->>'publication_sha256'
                IS DISTINCT FROM v_registration.candidate_publication_sha256 THEN
            RAISE EXCEPTION 'executable protected release event binding is stale'
              USING ERRCODE = '55000';
          END IF;
          v_release := jsonb_build_object(
            'schema_version', 2,
            'binding', v_event.binding,
            'reporter_incarnation', v_registration.reporter_incarnation::text,
            'bootstrap_registration_epoch', v_event.bootstrap_registration_epoch,
            'protected_registration_epoch', v_event.protected_registration_epoch,
            'bootstrap_revoked', true,
            'protected_release_sha256', v_event.protected_release_sha256,
            'executable', true
          );
          IF v_release IS DISTINCT FROM p_publication_payload THEN
            RAISE EXCEPTION 'executable protected release publication payload changed'
              USING ERRCODE = '55000';
          END IF;
          IF convert_from(p_canonical_payload, 'UTF8') IS DISTINCT FROM
             {SCHEMA}.canonical_executable_publication_payload(v_release) THEN
            RAISE EXCEPTION 'executable protected release publication canonical payload changed'
              USING ERRCODE = '22023';
          END IF;

          INSERT INTO {SCHEMA}.executable_release_publication_events
            (agent_incarnation, admission_event_id, publication_payload,
             publication_canonical_payload, publication_digest,
             manager_acknowledgement_digest)
          VALUES
            (p_agent_incarnation, p_admission_event_id, p_publication_payload,
             p_canonical_payload, p_publication_digest,
             p_manager_acknowledgement_digest);

          UPDATE {SCHEMA}.executable_release_publication_state
             SET last_event_id = p_admission_event_id
           WHERE agent_incarnation = p_agent_incarnation
             AND last_event_id = v_last_event_id;
          GET DIAGNOSTICS v_updated = ROW_COUNT;
          IF v_updated <> 1 THEN
            RAISE EXCEPTION 'executable protected release publication compare-and-set failed'
              USING ERRCODE = '40001';
          END IF;

          RETURN jsonb_build_object(
            'schema_version', 2,
            'event_id', p_admission_event_id,
            'event_kind', v_event.event_kind,
            'publication_digest', p_publication_digest,
            'manager_acknowledgement_digest', p_manager_acknowledgement_digest
          );
        END
        $function$
        """
    )


def _install_observe_executable_intent(*, include_withdrawal: bool) -> None:
    _executor, quoted_executor = _role("capacity_guard_executor_role")
    _observer, quoted_observer = _role("capacity_guard_observer_role")
    withdrawal_declaration = (
        f"          v_withdrawal {SCHEMA}.executable_admission_events%ROWTYPE;\n"
        if include_withdrawal
        else ""
    )
    withdrawal_query = (
        f"""
          SELECT * INTO v_withdrawal FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'withdrawn'
           ORDER BY protected_registration_epoch DESC, event_id DESC
           LIMIT 1 FOR KEY SHARE;
        """
        if include_withdrawal
        else ""
    )
    withdrawal_field = (
        "            'withdrawal', v_withdrawal.receipt,\n" if include_withdrawal else ""
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
{withdrawal_declaration.rstrip()}
          v_prepared_revocation {SCHEMA}.executable_admission_events%ROWTYPE;
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'executable intent observation requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT executor_role_name INTO v_executor_role
            FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1;
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
{withdrawal_query.rstrip()}
          SELECT * INTO v_prepared_revocation
            FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = p_intent_id
             AND subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'prepared-revoked'
           FOR KEY SHARE;

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
{withdrawal_field.rstrip()}
            'prepared_revocation', v_prepared_revocation.receipt,
            'executable', true
          );
        END
        $function$
        """
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{OBSERVATION_FUNCTION} FROM PUBLIC")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{OBSERVATION_FUNCTION} "
        f"TO {quoted_executor}, {quoted_observer}"
    )


def upgrade() -> None:
    _agent, quoted_agent = _role()
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.executable_release_publication_state (
          agent_incarnation uuid PRIMARY KEY REFERENCES {SCHEMA}.agent_registrations,
          last_event_id bigint NOT NULL DEFAULT 0 CHECK (last_event_id >= 0)
        )
        """
    )
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.executable_release_publication_events (
          publication_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          agent_incarnation uuid NOT NULL REFERENCES {SCHEMA}.agent_registrations,
          admission_event_id bigint NOT NULL UNIQUE
            REFERENCES {SCHEMA}.executable_admission_events(event_id),
          publication_payload jsonb NOT NULL CHECK (
            jsonb_typeof(publication_payload) = 'object'
            AND octet_length(publication_payload::text) <= 8388608
          ),
          publication_canonical_payload bytea NOT NULL CHECK (
            octet_length(publication_canonical_payload) <= 8388608
          ),
          publication_digest text NOT NULL CHECK (publication_digest ~ '^[0-9a-f]{{64}}$'),
          manager_acknowledgement_digest text NOT NULL CHECK (
            manager_acknowledgement_digest ~ '^[0-9a-f]{{64}}$'
          ),
          created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER guard_executable_release_publication_events_update_guard
        BEFORE UPDATE OR DELETE ON {SCHEMA}.executable_release_publication_events
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER guard_executable_release_publication_events_truncate_guard
        BEFORE TRUNCATE ON {SCHEMA}.executable_release_publication_events
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    _install_read_function()
    _install_canonical_payload_function()
    _install_ack_function()
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{CANONICAL_FUNCTION} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{READ_FUNCTION} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{ACK_FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{READ_FUNCTION} TO {quoted_agent}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{ACK_FUNCTION} TO {quoted_agent}")
    _install_observe_executable_intent(include_withdrawal=True)


def downgrade() -> None:
    _agent, quoted_agent = _role()
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.executable_release_publication_events
          ) THEN
            RAISE EXCEPTION 'cannot downgrade guard_0019 with publication evidence';
          END IF;
        END $$;
        """
    )
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{ACK_FUNCTION} FROM {quoted_agent}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{READ_FUNCTION} FROM {quoted_agent}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{ACK_FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{CANONICAL_FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{READ_FUNCTION}")
    _install_observe_executable_intent(include_withdrawal=False)
    op.execute(
        f"DROP TRIGGER guard_executable_release_publication_events_truncate_guard "
        f"ON {SCHEMA}.executable_release_publication_events"
    )
    op.execute(
        f"DROP TRIGGER guard_executable_release_publication_events_update_guard "
        f"ON {SCHEMA}.executable_release_publication_events"
    )
    op.execute(f"DROP TABLE {SCHEMA}.executable_release_publication_events")
    op.execute(f"DROP TABLE {SCHEMA}.executable_release_publication_state")
