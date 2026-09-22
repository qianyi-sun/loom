"""Zero-executable prepared admission and worker bindings.

Revision ID: guard_0003
Revises: guard_0002
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0003"
down_revision: str | Sequence[str] | None = "guard_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
APPEND_ONLY_TABLES = (
    "prepared_admission_plans",
    "prepared_worker_shapes",
    "prepared_placement_allowances",
    "prepared_bootstrap_bindings",
    "prepared_worker_bindings",
)


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("prepared admission migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def _install_append_only_guards() -> None:
    for table in APPEND_ONLY_TABLES:
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_row
            BEFORE UPDATE OR DELETE ON {SCHEMA}.{table}
            FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_truncate
            BEFORE TRUNCATE ON {SCHEMA}.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )


def _install_agent_binding_guard() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.assert_inert_agent_binding(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text
        )
        RETURNS void
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_agent_role text;
        BEGIN
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'prepared admission caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'prepared admission agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR octet_length(p_payload::text) > 8388608
             OR octet_length(p_canonical_payload) > 8388608
             OR convert_from(p_canonical_payload, 'UTF8')::jsonb IS DISTINCT FROM p_payload
             OR p_payload_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_canonical_payload), 'hex') IS DISTINCT FROM p_payload_digest THEN
            RAISE EXCEPTION 'prepared admission payload is invalid or oversized'
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
            RAISE EXCEPTION 'prepared admission agent incarnation is not registered'
              USING ERRCODE = '55000';
          END IF;

          IF (p_payload->>'schema_version')::integer IS DISTINCT FROM 1
             OR p_payload->>'environment_id' IS DISTINCT FROM v_registration.environment_id
             OR (p_payload->>'subject_id')::uuid IS DISTINCT FROM v_registration.subject_id
             OR (p_payload->>'subject_incarnation')::uuid IS DISTINCT FROM v_registration.subject_incarnation
             OR (p_payload->>'authority_incarnation')::uuid IS DISTINCT FROM v_registration.authority_incarnation
             OR (p_payload->>'agent_incarnation')::uuid IS DISTINCT FROM v_registration.agent_incarnation
             OR (p_payload->>'reporter_incarnation')::uuid IS DISTINCT FROM v_registration.reporter_incarnation
             OR p_payload->>'authority_mode' IS DISTINCT FROM 'disabled'
             OR (p_payload->>'allocation_epoch')::bigint IS DISTINCT FROM 0
             OR (p_payload->>'reporter_high_water')::bigint IS DISTINCT FROM 0
             OR p_payload->>'candidate_digest' IS DISTINCT FROM v_registration.candidate_digest
             OR (p_payload->>'deployment_generation')::bigint IS DISTINCT FROM v_registration.deployment_generation
             OR (p_payload->>'configuration_generation')::bigint IS DISTINCT FROM v_registration.configuration_generation THEN
            RAISE EXCEPTION 'prepared admission payload differs from its registered fence'
              USING ERRCODE = '55000';
          END IF;
        END
        $function$
        """
    )


def _install_prepare_plan() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.prepare_inert_admission_plan(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_plan_id uuid;
          v_existing {SCHEMA}.prepared_admission_plans%ROWTYPE;
          v_shape jsonb;
          v_allowance jsonb;
          v_shape_slots bigint;
          v_shape_count integer := 0;
          v_allowance_count integer := 0;
          v_audit_payload jsonb;
        BEGIN
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          v_plan_id := (p_payload->>'plan_id')::uuid;
          SELECT * INTO v_existing
            FROM {SCHEMA}.prepared_admission_plans
           WHERE plan_id = v_plan_id
           FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest
               OR (SELECT count(*) FROM {SCHEMA}.prepared_worker_shapes
                    WHERE plan_id = v_plan_id)
                    IS DISTINCT FROM jsonb_array_length(p_payload->'worker_shapes')
               OR (SELECT count(*) FROM {SCHEMA}.prepared_placement_allowances
                    WHERE plan_id = v_plan_id)
                    IS DISTINCT FROM jsonb_array_length(p_payload->'placement_allowances')
               OR EXISTS (
                    SELECT 1 FROM {SCHEMA}.prepared_worker_shapes AS s
                     WHERE s.plan_id = v_plan_id
                       AND NOT EXISTS (
                         SELECT 1 FROM jsonb_array_elements(p_payload->'worker_shapes') AS e(value)
                          WHERE e.value = s.payload
                       )
                  )
               OR EXISTS (
                    SELECT 1 FROM {SCHEMA}.prepared_placement_allowances AS a
                     WHERE a.plan_id = v_plan_id
                       AND NOT EXISTS (
                         SELECT 1 FROM jsonb_array_elements(p_payload->'placement_allowances') AS e(value)
                          WHERE e.value = a.payload
                       )
                  )
               OR (SELECT count(*) FROM {SCHEMA}.audit_events
                    WHERE event_type = 'admission_plan_prepared.v1'
                      AND payload->>'plan_id' = v_plan_id::text
                      AND payload_digest = p_payload_digest) IS DISTINCT FROM 1 THEN
              RAISE EXCEPTION 'conflicting prepared admission replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          IF p_payload->>'plan_state' IS DISTINCT FROM 'prepared'
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR v_plan_id = (p_payload->>'admission_incarnation')::uuid
             OR v_plan_id = (p_payload->>'manager_authority_incarnation')::uuid
             OR v_plan_id = p_agent_incarnation
             OR (p_payload->>'admission_incarnation')::uuid = p_agent_incarnation
             OR (p_payload->>'manager_authority_incarnation')::uuid = p_agent_incarnation
             OR p_payload->>'pool_id' NOT IN ('oldlab', 'gb10')
             OR p_payload->>'profile_id' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR (p_payload->>'manager_allocation_epoch')::bigint <= 0
             OR (p_payload->>'pool_generation')::bigint <= 0
             OR (p_payload->>'profile_generation')::bigint <= 0
             OR (p_payload->>'protocol_generation')::bigint <= 0
             OR p_payload->>'manager_input_digest' !~ '^[0-9a-f]{{64}}$'
             OR p_payload->>'manager_allocation_digest' !~ '^[0-9a-f]{{64}}$'
             OR p_payload->>'profile_digest' !~ '^[0-9a-f]{{64}}$'
             OR p_payload->>'protocol_digest' !~ '^[0-9a-f]{{64}}$'
             OR (p_payload->>'lease_not_after')::timestamptz <= statement_timestamp()
             OR jsonb_typeof(p_payload->'worker_shapes') IS DISTINCT FROM 'array'
             OR jsonb_array_length(p_payload->'worker_shapes') NOT BETWEEN 1 AND 10000
             OR jsonb_typeof(p_payload->'placement_allowances') IS DISTINCT FROM 'array'
             OR jsonb_array_length(p_payload->'placement_allowances') > 10000 THEN
            RAISE EXCEPTION 'prepared admission plan is not inert, current, and bounded'
              USING ERRCODE = '22023';
          END IF;
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.prepared_admission_plans
             WHERE pool_id = p_payload->>'pool_id'
               AND manager_allocation_epoch = (p_payload->>'manager_allocation_epoch')::bigint
          ) THEN
            RAISE EXCEPTION 'conflicting prepared admission epoch'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.prepared_admission_plans
            (plan_id, agent_incarnation, admission_incarnation,
             manager_authority_incarnation, manager_writer_epoch,
             manager_allocation_epoch, manager_input_digest,
             manager_allocation_digest, pool_id, pool_generation, profile_id,
             profile_generation, profile_digest, protocol_generation,
             protocol_digest, lease_not_after, plan_state, executable,
             payload, payload_digest)
          VALUES
            (v_plan_id, p_agent_incarnation,
             (p_payload->>'admission_incarnation')::uuid,
             (p_payload->>'manager_authority_incarnation')::uuid,
             (p_payload->>'manager_writer_epoch')::bigint,
             (p_payload->>'manager_allocation_epoch')::bigint,
             p_payload->>'manager_input_digest',
             p_payload->>'manager_allocation_digest', p_payload->>'pool_id',
             (p_payload->>'pool_generation')::bigint, p_payload->>'profile_id',
             (p_payload->>'profile_generation')::bigint, p_payload->>'profile_digest',
             (p_payload->>'protocol_generation')::bigint, p_payload->>'protocol_digest',
             (p_payload->>'lease_not_after')::timestamptz, 'prepared', false,
             p_payload, p_payload_digest);

          FOR v_shape IN SELECT value FROM jsonb_array_elements(p_payload->'worker_shapes')
          LOOP
            IF jsonb_typeof(v_shape) IS DISTINCT FROM 'object'
               OR (v_shape->>'schema_version')::integer IS DISTINCT FROM 1
               OR v_shape->>'pool_id' IS DISTINCT FROM p_payload->>'pool_id'
               OR (v_shape->>'pool_generation')::bigint IS DISTINCT FROM
                  (p_payload->>'pool_generation')::bigint
               OR v_shape->>'profile_id' IS DISTINCT FROM p_payload->>'profile_id'
               OR (v_shape->>'profile_generation')::bigint IS DISTINCT FROM
                  (p_payload->>'profile_generation')::bigint
               OR v_shape->>'profile_digest' IS DISTINCT FROM p_payload->>'profile_digest'
               OR (v_shape->>'protocol_generation')::bigint IS DISTINCT FROM
                  (p_payload->>'protocol_generation')::bigint
               OR v_shape->>'protocol_digest' IS DISTINCT FROM p_payload->>'protocol_digest'
               OR v_shape->>'shape_state' IS DISTINCT FROM 'prepared'
               OR (v_shape->>'executable')::boolean IS DISTINCT FROM false
               OR v_shape->>'worker_shape_digest' !~ '^[0-9a-f]{{64}}$'
               OR jsonb_typeof(v_shape->'worker_shape') IS DISTINCT FROM 'object'
               OR (v_shape->'worker_shape'->>'concurrency_slots')::bigint <= 0
               OR (v_shape->>'bootstrap_registration_epoch')::bigint <= 0 THEN
              RAISE EXCEPTION 'prepared worker shape differs from its inert plan binding'
                USING ERRCODE = '55000';
            END IF;
            IF EXISTS (
              SELECT 1 FROM {SCHEMA}.prepared_worker_shapes AS prior
               WHERE prior.shape_instance_id = v_shape->>'shape_instance_id'
                 AND (
                   prior.submission_intent_id IS DISTINCT FROM
                     (v_shape->>'submission_intent_id')::uuid
                   OR prior.pool_id IS DISTINCT FROM v_shape->>'pool_id'
                   OR prior.payload IS DISTINCT FROM v_shape
                   OR prior.worker_shape_digest IS DISTINCT FROM
                     v_shape->>'worker_shape_digest'
                 )
            ) THEN
              RAISE EXCEPTION 'prepared worker shape identity was rebound'
                USING ERRCODE = '55000';
            END IF;
            INSERT INTO {SCHEMA}.prepared_worker_shapes
              (shape_instance_id, plan_id, submission_intent_id, pool_id,
               concurrency_slots, bootstrap_registration_epoch, shape_state,
               executable, payload, worker_shape_digest)
            VALUES
              (v_shape->>'shape_instance_id', v_plan_id,
               (v_shape->>'submission_intent_id')::uuid, v_shape->>'pool_id',
               (v_shape->'worker_shape'->>'concurrency_slots')::bigint,
               (v_shape->>'bootstrap_registration_epoch')::bigint,
               'prepared', false, v_shape, v_shape->>'worker_shape_digest');
            v_shape_count := v_shape_count + 1;
          END LOOP;

          FOR v_allowance IN
            SELECT value FROM jsonb_array_elements(p_payload->'placement_allowances')
          LOOP
            SELECT concurrency_slots INTO v_shape_slots
              FROM {SCHEMA}.prepared_worker_shapes
             WHERE plan_id = v_plan_id
               AND shape_instance_id = v_allowance->>'shape_instance_id'
               AND submission_intent_id = (v_allowance->>'submission_intent_id')::uuid
               AND pool_id = v_allowance->>'pool_id';
            IF NOT FOUND
               OR jsonb_typeof(v_allowance) IS DISTINCT FROM 'object'
               OR (v_allowance->>'schema_version')::integer IS DISTINCT FROM 1
               OR v_allowance->>'pool_id' IS DISTINCT FROM p_payload->>'pool_id'
               OR v_allowance->>'allowance_state' IS DISTINCT FROM 'prepared'
               OR (v_allowance->>'executable')::boolean IS DISTINCT FROM false
               OR (v_allowance->>'shape_slot_index')::bigint < 0
               OR (v_allowance->>'shape_slot_index')::bigint >= v_shape_slots
               OR NOT EXISTS (
                 SELECT 1 FROM {SCHEMA}.trial_attempts AS a
                  WHERE a.protected_attempt_id =
                        (v_allowance->>'protected_attempt_id')::uuid
                    AND a.execution_generation =
                        (v_allowance->>'execution_generation')::bigint
                    AND a.requirements_digest = v_allowance->>'requirements_digest'
                    AND a.claim_state = 'queued'
                    AND a.assigned_pool IS NULL
                    AND a.assignment_epoch IS NULL
               ) THEN
              RAISE EXCEPTION 'prepared placement allowance binding is invalid'
                USING ERRCODE = '55000';
            END IF;
            INSERT INTO {SCHEMA}.prepared_placement_allowances
              (allowance_id, plan_id, protected_attempt_id, execution_generation,
               requirements_digest, pool_id, shape_instance_id, shape_slot_index,
               submission_intent_id, allowance_state, executable, payload)
            VALUES
              ((v_allowance->>'allowance_id')::uuid, v_plan_id,
               (v_allowance->>'protected_attempt_id')::uuid,
               (v_allowance->>'execution_generation')::bigint,
               v_allowance->>'requirements_digest', v_allowance->>'pool_id',
               v_allowance->>'shape_instance_id',
               (v_allowance->>'shape_slot_index')::bigint,
               (v_allowance->>'submission_intent_id')::uuid,
               'prepared', false, v_allowance);
            v_allowance_count := v_allowance_count + 1;
          END LOOP;
          IF v_shape_count <> jsonb_array_length(p_payload->'worker_shapes')
             OR v_allowance_count <>
                jsonb_array_length(p_payload->'placement_allowances') THEN
            RAISE EXCEPTION 'prepared admission child record count is inconsistent'
              USING ERRCODE = '55000';
          END IF;

          v_audit_payload := jsonb_build_object(
            'schema_version', 1,
            'plan_id', v_plan_id,
            'admission_incarnation', p_payload->>'admission_incarnation',
            'manager_allocation_epoch', (p_payload->>'manager_allocation_epoch')::bigint,
            'pool_id', p_payload->>'pool_id',
            'worker_shape_count', v_shape_count,
            'allowance_count', v_allowance_count,
            'executable', false
          );
          INSERT INTO {SCHEMA}.audit_events (event_type, payload, payload_digest)
          VALUES ('admission_plan_prepared.v1', v_audit_payload, p_payload_digest);
          RETURN p_payload;
        END
        $function$
        """
    )


def _install_bootstrap_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.register_inert_bootstrap(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_bootstrap_id uuid;
          v_existing {SCHEMA}.prepared_bootstrap_bindings%ROWTYPE;
          v_audit_payload jsonb;
        BEGIN
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          v_bootstrap_id := (p_payload->>'bootstrap_id')::uuid;
          SELECT * INTO v_existing FROM {SCHEMA}.prepared_bootstrap_bindings
           WHERE bootstrap_id = v_bootstrap_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest
               OR (SELECT count(*) FROM {SCHEMA}.audit_events
                    WHERE event_type = 'bootstrap_prepared.v1'
                      AND payload->>'bootstrap_id' = v_bootstrap_id::text
                      AND payload_digest = p_payload_digest) IS DISTINCT FROM 1 THEN
              RAISE EXCEPTION 'conflicting prepared bootstrap replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          IF p_payload->>'bootstrap_state' IS DISTINCT FROM 'registered'
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR v_bootstrap_id = (p_payload->>'plan_id')::uuid
             OR v_bootstrap_id = (p_payload->>'admission_incarnation')::uuid
             OR v_bootstrap_id = (p_payload->>'submission_intent_id')::uuid
             OR v_bootstrap_id = p_agent_incarnation
             OR p_payload->>'bootstrap_digest' !~ '^[0-9a-f]{{64}}$'
             OR (p_payload->>'expires_at')::timestamptz <= statement_timestamp()
             OR NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.prepared_admission_plans AS p
                 JOIN {SCHEMA}.prepared_worker_shapes AS s
                   ON s.plan_id = p.plan_id
                  AND s.shape_instance_id = p_payload->>'shape_instance_id'
                  AND s.submission_intent_id =
                      (p_payload->>'submission_intent_id')::uuid
                  AND s.pool_id = p_payload->>'pool_id'
                WHERE p.plan_id = (p_payload->>'plan_id')::uuid
                  AND p.agent_incarnation = p_agent_incarnation
                  AND p.admission_incarnation =
                      (p_payload->>'admission_incarnation')::uuid
                  AND p.manager_allocation_epoch =
                      (p_payload->>'manager_allocation_epoch')::bigint
                  AND p.plan_state = 'prepared' AND p.executable = false
                  AND s.bootstrap_registration_epoch =
                      (p_payload->>'bootstrap_registration_epoch')::bigint
                  AND (p_payload->>'expires_at')::timestamptz <= p.lease_not_after
             ) THEN
            RAISE EXCEPTION 'prepared bootstrap differs from its inert plan binding'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.prepared_bootstrap_bindings
            (bootstrap_id, plan_id, agent_incarnation, admission_incarnation,
             manager_allocation_epoch, pool_id, shape_instance_id,
             submission_intent_id, bootstrap_registration_epoch,
             bootstrap_digest, expires_at, bootstrap_state, executable,
             payload, payload_digest)
          VALUES
            (v_bootstrap_id, (p_payload->>'plan_id')::uuid, p_agent_incarnation,
             (p_payload->>'admission_incarnation')::uuid,
             (p_payload->>'manager_allocation_epoch')::bigint,
             p_payload->>'pool_id', p_payload->>'shape_instance_id',
             (p_payload->>'submission_intent_id')::uuid,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             p_payload->>'bootstrap_digest',
             (p_payload->>'expires_at')::timestamptz, 'registered', false,
             p_payload, p_payload_digest);
          v_audit_payload := jsonb_build_object(
            'schema_version', 1, 'bootstrap_id', v_bootstrap_id,
            'plan_id', p_payload->>'plan_id',
            'submission_intent_id', p_payload->>'submission_intent_id',
            'bootstrap_registration_epoch',
              (p_payload->>'bootstrap_registration_epoch')::bigint,
            'executable', false
          );
          INSERT INTO {SCHEMA}.audit_events (event_type, payload, payload_digest)
          VALUES ('bootstrap_prepared.v1', v_audit_payload, p_payload_digest);
          RETURN p_payload;
        END
        $function$
        """
    )


def _install_worker_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.record_inert_worker(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_worker_id uuid;
          v_existing {SCHEMA}.prepared_worker_bindings%ROWTYPE;
          v_audit_payload jsonb;
        BEGIN
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          v_worker_id := (p_payload->>'worker_id')::uuid;
          SELECT * INTO v_existing FROM {SCHEMA}.prepared_worker_bindings
           WHERE worker_id = v_worker_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest
               OR (SELECT count(*) FROM {SCHEMA}.audit_events
                    WHERE event_type = 'worker_prepared.v1'
                      AND payload->>'worker_id' = v_worker_id::text
                      AND payload_digest = p_payload_digest) IS DISTINCT FROM 1 THEN
              RAISE EXCEPTION 'conflicting prepared worker replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          IF p_payload->>'worker_state' IS DISTINCT FROM 'prepared'
             OR (p_payload->>'claim_authorization_epoch')::bigint IS DISTINCT FROM 0
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR v_worker_id = (p_payload->>'worker_incarnation')::uuid
             OR v_worker_id = (p_payload->>'bootstrap_id')::uuid
             OR v_worker_id = (p_payload->>'plan_id')::uuid
             OR v_worker_id = (p_payload->>'admission_incarnation')::uuid
             OR v_worker_id = (p_payload->>'submission_intent_id')::uuid
             OR v_worker_id = p_agent_incarnation
             OR p_payload->>'ownership_evidence_digest' !~ '^[0-9a-f]{{64}}$'
             OR p_payload->>'worker_credential_digest' !~ '^[0-9a-f]{{64}}$'
             OR NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.prepared_bootstrap_bindings AS b
                 JOIN {SCHEMA}.prepared_worker_shapes AS s
                   ON s.plan_id = b.plan_id
                  AND s.shape_instance_id = b.shape_instance_id
                  AND s.submission_intent_id = b.submission_intent_id
                  AND s.pool_id = b.pool_id
                WHERE b.bootstrap_id = (p_payload->>'bootstrap_id')::uuid
                  AND b.plan_id = (p_payload->>'plan_id')::uuid
                  AND b.agent_incarnation = p_agent_incarnation
                  AND b.admission_incarnation =
                      (p_payload->>'admission_incarnation')::uuid
                  AND b.manager_allocation_epoch =
                      (p_payload->>'manager_allocation_epoch')::bigint
                  AND b.pool_id = p_payload->>'pool_id'
                  AND b.shape_instance_id = p_payload->>'shape_instance_id'
                  AND b.submission_intent_id =
                      (p_payload->>'submission_intent_id')::uuid
                  AND b.bootstrap_registration_epoch =
                      (p_payload->>'bootstrap_registration_epoch')::bigint
                  AND b.bootstrap_state = 'registered' AND b.executable = false
                  AND b.expires_at > statement_timestamp()
             ) THEN
            RAISE EXCEPTION 'prepared worker differs from its inert bootstrap binding'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.prepared_worker_bindings
            (worker_id, worker_incarnation, bootstrap_id, plan_id,
             agent_incarnation, admission_incarnation, manager_allocation_epoch,
             pool_id, shape_instance_id, submission_intent_id,
             bootstrap_registration_epoch, slurm_job_id,
             ownership_evidence_digest, worker_credential_digest,
             claim_authorization_epoch, worker_state, executable,
             payload, payload_digest)
          VALUES
            (v_worker_id, (p_payload->>'worker_incarnation')::uuid,
             (p_payload->>'bootstrap_id')::uuid, (p_payload->>'plan_id')::uuid,
             p_agent_incarnation, (p_payload->>'admission_incarnation')::uuid,
             (p_payload->>'manager_allocation_epoch')::bigint,
             p_payload->>'pool_id', p_payload->>'shape_instance_id',
             (p_payload->>'submission_intent_id')::uuid,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             p_payload->>'slurm_job_id', p_payload->>'ownership_evidence_digest',
             p_payload->>'worker_credential_digest', 0, 'prepared', false,
             p_payload, p_payload_digest);
          v_audit_payload := jsonb_build_object(
            'schema_version', 1, 'worker_id', v_worker_id,
            'worker_incarnation', p_payload->>'worker_incarnation',
            'bootstrap_id', p_payload->>'bootstrap_id',
            'plan_id', p_payload->>'plan_id',
            'slurm_job_id', p_payload->>'slurm_job_id',
            'claim_authorization_epoch', 0,
            'executable', false
          );
          INSERT INTO {SCHEMA}.audit_events (event_type, payload, payload_digest)
          VALUES ('worker_prepared.v1', v_audit_payload, p_payload_digest);
          RETURN p_payload;
        END
        $function$
        """
    )


def upgrade() -> None:
    quoted_agent = _agent_role()
    op.create_unique_constraint(
        "guard_attempt_execution_requirement_key",
        "trial_attempts",
        ["protected_attempt_id", "execution_generation", "requirements_digest"],
        schema=SCHEMA,
    )
    op.create_table(
        "prepared_admission_plans",
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("admission_incarnation", sa.Uuid(), nullable=False),
        sa.Column("manager_authority_incarnation", sa.Uuid(), nullable=False),
        sa.Column("manager_writer_epoch", sa.BigInteger(), nullable=False),
        sa.Column("manager_allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("manager_input_digest", sa.Text(), nullable=False),
        sa.Column("manager_allocation_digest", sa.Text(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("profile_id", sa.Text(), nullable=False),
        sa.Column("profile_generation", sa.BigInteger(), nullable=False),
        sa.Column("profile_digest", sa.Text(), nullable=False),
        sa.Column("protocol_generation", sa.BigInteger(), nullable=False),
        sa.Column("protocol_digest", sa.Text(), nullable=False),
        sa.Column("lease_not_after", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("plan_state", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("manager_writer_epoch >= 0", name="guard_plan_writer_epoch_check"),
        sa.CheckConstraint(
            "manager_allocation_epoch > 0 AND pool_generation > 0 "
            "AND profile_generation > 0 AND protocol_generation > 0",
            name="guard_plan_generations_check",
        ),
        sa.CheckConstraint("pool_id IN ('oldlab', 'gb10')", name="guard_plan_pool_check"),
        sa.CheckConstraint(
            "profile_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_plan_profile_id_check",
        ),
        sa.CheckConstraint(
            "plan_state = 'prepared' AND executable = false",
            name="guard_plan_inert_check",
        ),
        sa.CheckConstraint(
            "manager_input_digest ~ '^[0-9a-f]{64}$' "
            "AND manager_allocation_digest ~ '^[0-9a-f]{64}$' "
            "AND profile_digest ~ '^[0-9a-f]{64}$' "
            "AND protocol_digest ~ '^[0-9a-f]{64}$' "
            "AND payload_digest ~ '^[0-9a-f]{64}$'",
            name="guard_plan_digests_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 8388608",
            name="guard_plan_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("plan_id"),
        sa.UniqueConstraint("admission_incarnation", name="guard_plan_admission_key"),
        sa.UniqueConstraint(
            "pool_id", "manager_allocation_epoch", name="guard_plan_pool_epoch_key"
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "prepared_worker_shapes",
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("concurrency_slots", sa.BigInteger(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("shape_state", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("worker_shape_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "shape_instance_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_prepared_shape_id_check",
        ),
        sa.CheckConstraint("pool_id IN ('oldlab', 'gb10')", name="guard_shape_pool_check"),
        sa.CheckConstraint(
            "concurrency_slots > 0 AND bootstrap_registration_epoch > 0",
            name="guard_shape_generations_check",
        ),
        sa.CheckConstraint(
            "shape_state = 'prepared' AND executable = false",
            name="guard_shape_inert_check",
        ),
        sa.CheckConstraint(
            "worker_shape_digest ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(payload) = 'object' "
            "AND octet_length(payload::text) <= 8388608",
            name="guard_shape_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["plan_id"], [f"{SCHEMA}.prepared_admission_plans.plan_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("plan_id", "shape_instance_id"),
        sa.UniqueConstraint(
            "plan_id", "submission_intent_id", name="guard_shape_plan_intent_key"
        ),
        sa.UniqueConstraint(
            "plan_id",
            "shape_instance_id",
            "submission_intent_id",
            "pool_id",
            name="guard_shape_exact_binding_key",
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "prepared_placement_allowances",
        sa.Column("allowance_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("execution_generation", sa.BigInteger(), nullable=False),
        sa.Column("requirements_digest", sa.Text(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("shape_slot_index", sa.BigInteger(), nullable=False),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=False),
        sa.Column("allowance_state", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "execution_generation > 0 AND shape_slot_index >= 0",
            name="guard_allowance_generation_slot_check",
        ),
        sa.CheckConstraint("pool_id IN ('oldlab', 'gb10')", name="guard_allowance_pool_check"),
        sa.CheckConstraint(
            "requirements_digest ~ '^[0-9a-f]{64}$'",
            name="guard_allowance_requirements_check",
        ),
        sa.CheckConstraint(
            "allowance_state = 'prepared' AND executable = false",
            name="guard_allowance_inert_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 8388608",
            name="guard_allowance_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id", "execution_generation", "requirements_digest"],
            [
                f"{SCHEMA}.trial_attempts.protected_attempt_id",
                f"{SCHEMA}.trial_attempts.execution_generation",
                f"{SCHEMA}.trial_attempts.requirements_digest",
            ],
            ondelete="RESTRICT",
            name="guard_allowance_attempt_binding_fk",
        ),
        sa.ForeignKeyConstraint(
            ["plan_id", "shape_instance_id", "submission_intent_id", "pool_id"],
            [
                f"{SCHEMA}.prepared_worker_shapes.plan_id",
                f"{SCHEMA}.prepared_worker_shapes.shape_instance_id",
                f"{SCHEMA}.prepared_worker_shapes.submission_intent_id",
                f"{SCHEMA}.prepared_worker_shapes.pool_id",
            ],
            ondelete="RESTRICT",
            name="guard_allowance_shape_binding_fk",
        ),
        sa.PrimaryKeyConstraint("allowance_id"),
        sa.UniqueConstraint(
            "plan_id", "protected_attempt_id", name="guard_allowance_plan_attempt_key"
        ),
        sa.UniqueConstraint(
            "plan_id",
            "shape_instance_id",
            "shape_slot_index",
            name="guard_allowance_plan_slot_key",
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "prepared_bootstrap_bindings",
        sa.Column("bootstrap_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("admission_incarnation", sa.Uuid(), nullable=False),
        sa.Column("manager_allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("bootstrap_digest", sa.Text(), nullable=False),
        sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("bootstrap_state", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "manager_allocation_epoch > 0 AND bootstrap_registration_epoch > 0",
            name="guard_bootstrap_generations_check",
        ),
        sa.CheckConstraint("pool_id IN ('oldlab', 'gb10')", name="guard_bootstrap_pool_check"),
        sa.CheckConstraint(
            "bootstrap_state = 'registered' AND executable = false",
            name="guard_bootstrap_inert_check",
        ),
        sa.CheckConstraint(
            "bootstrap_digest ~ '^[0-9a-f]{64}$' AND payload_digest ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(payload) = 'object' "
            "AND octet_length(payload::text) <= 8388608",
            name="guard_bootstrap_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["plan_id", "shape_instance_id", "submission_intent_id", "pool_id"],
            [
                f"{SCHEMA}.prepared_worker_shapes.plan_id",
                f"{SCHEMA}.prepared_worker_shapes.shape_instance_id",
                f"{SCHEMA}.prepared_worker_shapes.submission_intent_id",
                f"{SCHEMA}.prepared_worker_shapes.pool_id",
            ],
            ondelete="RESTRICT",
            name="guard_bootstrap_shape_binding_fk",
        ),
        sa.PrimaryKeyConstraint("bootstrap_id"),
        sa.UniqueConstraint(
            "submission_intent_id",
            "bootstrap_registration_epoch",
            name="guard_bootstrap_intent_epoch_key",
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "prepared_worker_bindings",
        sa.Column("worker_id", sa.Uuid(), nullable=False),
        sa.Column("worker_incarnation", sa.Uuid(), nullable=False),
        sa.Column("bootstrap_id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("admission_incarnation", sa.Uuid(), nullable=False),
        sa.Column("manager_allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("slurm_job_id", sa.Text(), nullable=False),
        sa.Column("ownership_evidence_digest", sa.Text(), nullable=False),
        sa.Column("worker_credential_digest", sa.Text(), nullable=False),
        sa.Column("claim_authorization_epoch", sa.BigInteger(), nullable=False),
        sa.Column("worker_state", sa.Text(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "manager_allocation_epoch > 0 AND bootstrap_registration_epoch > 0 "
            "AND claim_authorization_epoch = 0",
            name="guard_worker_generations_check",
        ),
        sa.CheckConstraint("pool_id IN ('oldlab', 'gb10')", name="guard_worker_pool_check"),
        sa.CheckConstraint(
            "slurm_job_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_worker_job_id_check",
        ),
        sa.CheckConstraint(
            "worker_state = 'prepared' AND executable = false",
            name="guard_worker_inert_check",
        ),
        sa.CheckConstraint(
            "ownership_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND worker_credential_digest ~ '^[0-9a-f]{64}$' "
            "AND payload_digest ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(payload) = 'object' "
            "AND octet_length(payload::text) <= 8388608",
            name="guard_worker_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["bootstrap_id"],
            [f"{SCHEMA}.prepared_bootstrap_bindings.bootstrap_id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["plan_id", "shape_instance_id", "submission_intent_id", "pool_id"],
            [
                f"{SCHEMA}.prepared_worker_shapes.plan_id",
                f"{SCHEMA}.prepared_worker_shapes.shape_instance_id",
                f"{SCHEMA}.prepared_worker_shapes.submission_intent_id",
                f"{SCHEMA}.prepared_worker_shapes.pool_id",
            ],
            ondelete="RESTRICT",
            name="guard_worker_shape_binding_fk",
        ),
        sa.PrimaryKeyConstraint("worker_id"),
        sa.UniqueConstraint("worker_incarnation", name="guard_worker_incarnation_key"),
        sa.UniqueConstraint("bootstrap_id", name="guard_worker_bootstrap_key"),
        sa.UniqueConstraint("pool_id", "slurm_job_id", name="guard_worker_pool_job_key"),
        schema=SCHEMA,
    )
    _install_append_only_guards()
    _install_agent_binding_guard()
    _install_prepare_plan()
    _install_bootstrap_function()
    _install_worker_function()
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM PUBLIC")
    for function in (
        "prepare_inert_admission_plan",
        "register_inert_bootstrap",
        "record_inert_worker",
    ):
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{function}(uuid, jsonb, bytea, text) "
            f"TO {quoted_agent}"
        )


def downgrade() -> None:
    quoted_agent = _agent_role()
    for function in (
        "record_inert_worker",
        "register_inert_bootstrap",
        "prepare_inert_admission_plan",
    ):
        op.execute(
            f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{function}(uuid, jsonb, bytea, text) "
            f"FROM {quoted_agent}"
        )
        op.execute(f"DROP FUNCTION {SCHEMA}.{function}(uuid, jsonb, bytea, text)")
    op.execute(
        f"DROP FUNCTION {SCHEMA}.assert_inert_agent_binding(uuid, jsonb, bytea, text)"
    )
    op.drop_table("prepared_worker_bindings", schema=SCHEMA)
    op.drop_table("prepared_bootstrap_bindings", schema=SCHEMA)
    op.drop_table("prepared_placement_allowances", schema=SCHEMA)
    op.drop_table("prepared_worker_shapes", schema=SCHEMA)
    op.drop_table("prepared_admission_plans", schema=SCHEMA)
    op.drop_constraint(
        "guard_attempt_execution_requirement_key",
        "trial_attempts",
        schema=SCHEMA,
        type_="unique",
    )
