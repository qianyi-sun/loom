"""Expose fresh unused-bootstrap evidence without changing historical replay.

Revision ID: guard_0033
Revises: guard_0032

This executor-only observation is not a lease, registration, or runtime admission.
Its caller must commit and compose current manager/scheduler authority separately.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "guard_0033"
down_revision: str | Sequence[str] | None = "guard_0032"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCHEMA = "loom_capacity_guard"
_SIGNATURE = f"{_SCHEMA}.observe_current_executable_bootstrap(uuid,uuid,jsonb,bytea,text)"


def upgrade() -> None:
    config = op.get_context().config
    role = config.attributes.get("capacity_guard_executor_role") if config else None
    if not isinstance(role, str) or not role:
        raise RuntimeError("current bootstrap observation requires the validated executor role")
    quoted_executor = op.get_bind().dialect.identifier_preparer.quote(role)
    op.execute(f"""
        CREATE FUNCTION {_SCHEMA}.observe_current_executable_bootstrap(
          p_subject_id uuid, p_subject_incarnation uuid, p_payload jsonb,
          p_canonical_payload bytea, p_request_digest text
        ) RETURNS jsonb
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_incarnation uuid;
          v_intent_id uuid := (p_payload->'binding'->>'intent_id')::uuid;
          v_physical {_SCHEMA}.executable_admission_events%ROWTYPE;
          v_prepared {_SCHEMA}.executable_admission_events%ROWTYPE;
          v_bootstrap {_SCHEMA}.protected_executable_bootstrap_registrations%ROWTYPE;
          v_now timestamptz;
          v_expires timestamptz;
        BEGIN
          -- Reuse the exact physical-binding request schema, not bind's replay path.
          v_agent_incarnation := {_SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'bind', p_payload,
            p_canonical_payload, p_request_digest
          );
          -- Bound contention: never add an opposing blocking lock order to admission.
          PERFORM 1 FROM {_SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          SELECT * INTO v_physical FROM {_SCHEMA}.executable_admission_events
           WHERE operation_id = (p_payload->>'operation_id')::uuid
             AND event_kind = 'physical-bound';
          SELECT * INTO v_prepared FROM {_SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'prepared';
          -- Latest, not the historical bootstrap referenced by the prepared event.
          SELECT * INTO v_bootstrap FROM {_SCHEMA}.protected_executable_bootstrap_registrations
           WHERE intent_id = v_intent_id
           ORDER BY bootstrap_registration_epoch DESC LIMIT 1;
          v_now := pg_catalog.clock_timestamp();
          v_expires := (v_bootstrap.proposal_payload->>'expires_at')::timestamptz;
          IF v_physical.operation_id IS NULL OR v_prepared.operation_id IS NULL
             OR v_bootstrap.registration_id IS NULL
             OR v_physical.request_payload IS DISTINCT FROM p_payload
             OR v_physical.request_digest IS DISTINCT FROM p_request_digest
             OR v_physical.agent_incarnation IS DISTINCT FROM v_agent_incarnation
             OR v_prepared.agent_incarnation IS DISTINCT FROM v_agent_incarnation
             OR v_bootstrap.agent_incarnation IS DISTINCT FROM v_agent_incarnation
             OR v_prepared.binding IS DISTINCT FROM p_payload->'binding'
             OR v_bootstrap.binding IS DISTINCT FROM p_payload->'binding'
             OR v_prepared.bootstrap_registration_epoch IS DISTINCT FROM
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR v_bootstrap.bootstrap_registration_epoch IS DISTINCT FROM
                v_prepared.bootstrap_registration_epoch
             OR v_bootstrap.bootstrap_sha256 IS DISTINCT FROM v_prepared.bootstrap_sha256
             OR (v_expires > v_now) IS DISTINCT FROM true
             OR EXISTS (
               SELECT 1 FROM {_SCHEMA}.executable_admission_events
                WHERE intent_id = v_intent_id AND event_kind NOT IN ('prepared', 'physical-bound')
             ) THEN
            RAISE EXCEPTION 'current unused bootstrap requires exact live physical preparation'
              USING ERRCODE = '55000';
          END IF;
          RETURN jsonb_build_object(
            'schema_version', 2, 'physical_binding', p_payload,
            'agent_incarnation', v_agent_incarnation,
            'bootstrap_sha256', v_bootstrap.bootstrap_sha256,
            -- Snapshot age must never be refreshed by lock/read delay. The
            -- separate v_now above still checks expiry against the current clock.
            'observed_at', pg_catalog.transaction_timestamp(), 'bootstrap_expires_at', v_expires,
            'request_digest', p_request_digest,
            'observation_state', 'current-unused-bootstrap', 'executable', false
          );
        END
        $function$
    """)
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {_SIGNATURE} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {_SIGNATURE} TO {quoted_executor}")


def downgrade() -> None:
    # No durable events or runtime authority are created by this observation.
    op.execute(f"DROP FUNCTION {_SIGNATURE}")
