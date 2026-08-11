"""Monotonic disabled-agent reconfiguration and restart high-water read.

Revision ID: guard_0009
Revises: guard_0008
Create Date: 2026-08-11
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "guard_0009"
down_revision: str | Sequence[str] | None = "guard_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("agent reconfiguration migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def upgrade() -> None:
    quoted_agent = _agent_role()
    op.execute(
        f"DROP TRIGGER authority_state_append_only_row ON {SCHEMA}.authority_state"
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_authority_reconfiguration()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'protected authority state remains append-only for deletion'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.singleton_id IS DISTINCT FROM OLD.singleton_id
             OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
             OR NEW.environment_id IS DISTINCT FROM OLD.environment_id
             OR NEW.subject_id IS DISTINCT FROM OLD.subject_id
             OR NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
             OR NEW.authority_mode IS DISTINCT FROM OLD.authority_mode
             OR NEW.authority_incarnation IS DISTINCT FROM OLD.authority_incarnation
             OR NEW.reporter_high_water IS DISTINCT FROM OLD.reporter_high_water
             OR NEW.allocation_epoch IS DISTINCT FROM OLD.allocation_epoch THEN
            RAISE EXCEPTION 'protected authority immutable binding changed'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.configuration_generation <= OLD.configuration_generation
             OR NEW.deployment_generation < OLD.deployment_generation THEN
            RAISE EXCEPTION 'protected authority generation did not advance monotonically'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.updated_at <= OLD.updated_at THEN
            RAISE EXCEPTION 'protected authority update timestamp did not advance monotonically'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.deployment_generation = OLD.deployment_generation
             AND (NEW.reporter_incarnation IS DISTINCT FROM OLD.reporter_incarnation
                  OR NEW.candidate_digest IS DISTINCT FROM OLD.candidate_digest) THEN
            RAISE EXCEPTION 'capacity-only reconfiguration changed reporter or candidate'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.deployment_generation > OLD.deployment_generation
             AND NEW.reporter_incarnation IS NOT DISTINCT FROM OLD.reporter_incarnation THEN
            RAISE EXCEPTION 'replacement deployment did not rotate its reporter incarnation'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER authority_state_monotonic_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.authority_state
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.enforce_authority_reconfiguration()
        """
    )
    op.execute(
        f"DROP TRIGGER agent_registrations_append_only_row ON {SCHEMA}.agent_registrations"
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_agent_registration_reconfiguration()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'agent registration remains append-only for deletion'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.agent_incarnation IS DISTINCT FROM OLD.agent_incarnation
             OR NEW.singleton_id IS DISTINCT FROM OLD.singleton_id
             OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
             OR NEW.environment_id IS DISTINCT FROM OLD.environment_id
             OR NEW.subject_id IS DISTINCT FROM OLD.subject_id
             OR NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
             OR NEW.authority_incarnation IS DISTINCT FROM OLD.authority_incarnation
             OR NEW.authority_mode IS DISTINCT FROM OLD.authority_mode
             OR NEW.allocation_epoch IS DISTINCT FROM OLD.allocation_epoch
             OR NEW.registration_state IS DISTINCT FROM OLD.registration_state
             OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'agent registration immutable binding changed'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.configuration_generation <= OLD.configuration_generation
             OR NEW.deployment_generation < OLD.deployment_generation THEN
            RAISE EXCEPTION 'agent registration generation did not advance monotonically'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.deployment_generation = OLD.deployment_generation
             AND (NEW.reporter_incarnation IS DISTINCT FROM OLD.reporter_incarnation
                  OR NEW.candidate_digest IS DISTINCT FROM OLD.candidate_digest) THEN
            RAISE EXCEPTION 'capacity-only agent reconfiguration changed reporter or candidate'
              USING ERRCODE = '55000';
          END IF;
          IF NEW.deployment_generation > OLD.deployment_generation
             AND NEW.reporter_incarnation IS NOT DISTINCT FROM OLD.reporter_incarnation THEN
            RAISE EXCEPTION 'replacement agent did not rotate its reporter incarnation'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER agent_registrations_monotonic_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.agent_registrations
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.enforce_agent_registration_reconfiguration()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_agent_reporter_high_water(p_agent_incarnation uuid)
        RETURNS bigint
        LANGUAGE plpgsql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_role text;
          v_high_water bigint;
        BEGIN
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'capacity reporter state caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'capacity demand agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          SELECT s.high_water INTO v_high_water
            FROM {SCHEMA}.agent_reporter_state AS s
            JOIN {SCHEMA}.agent_registrations AS r
              ON r.agent_incarnation = s.agent_incarnation
            JOIN {SCHEMA}.authority_state AS f
              ON f.singleton_id = r.singleton_id
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
             AND r.registration_state = 'registered';
          IF v_high_water IS NULL THEN
            RAISE EXCEPTION 'capacity reporter state binding is unavailable'
              USING ERRCODE = '55000';
          END IF;
          RETURN v_high_water;
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}."
        "read_agent_reporter_high_water(uuid) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}."
        f"read_agent_reporter_high_water(uuid) TO {quoted_agent}"
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_agent_lifecycle_demand_observation(
          p_agent_incarnation uuid,
          p_sequence bigint
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_high_water bigint;
          v_payload jsonb;
        BEGIN
          v_high_water := {SCHEMA}.read_agent_reporter_high_water(p_agent_incarnation);
          IF p_sequence <= 0 OR p_sequence <> v_high_water THEN
            RAISE EXCEPTION 'capacity observation is not the current reporter high-water'
              USING ERRCODE = '55000';
          END IF;
          SELECT o.payload INTO v_payload
            FROM {SCHEMA}.demand_observations AS o
           WHERE o.agent_incarnation = p_agent_incarnation
             AND o.sequence = p_sequence;
          IF v_payload IS NULL THEN
            RAISE EXCEPTION 'capacity observation payload is unavailable'
              USING ERRCODE = '55000';
          END IF;
          RETURN v_payload;
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}."
        "read_agent_lifecycle_demand_observation(uuid, bigint) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}."
        f"read_agent_lifecycle_demand_observation(uuid, bigint) TO {quoted_agent}"
    )


def downgrade() -> None:
    op.execute(
        f"""
        DO $block$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.audit_events
             WHERE event_type IN (
               'authority_reconfigured.v1',
               'agent_reconfigured.v1'
             )
          ) THEN
            RAISE EXCEPTION 'cannot downgrade guard_0009 after agent reconfiguration';
          END IF;
        END
        $block$
        """
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.read_agent_lifecycle_demand_observation(uuid, bigint)"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.read_agent_reporter_high_water(uuid)")
    op.execute(
        f"DROP TRIGGER agent_registrations_monotonic_row ON {SCHEMA}.agent_registrations"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_agent_registration_reconfiguration()")
    op.execute(
        f"""
        CREATE TRIGGER agent_registrations_append_only_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.agent_registrations
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(f"DROP TRIGGER authority_state_monotonic_row ON {SCHEMA}.authority_state")
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_authority_reconfiguration()")
    op.execute(
        f"""
        CREATE TRIGGER authority_state_append_only_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.authority_state
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
