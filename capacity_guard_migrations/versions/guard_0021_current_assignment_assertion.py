"""Require acknowledgement evidence to remain the current runnable assignment.

Revision ID: guard_0021
Revises: guard_0020
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0021"
down_revision: str | Sequence[str] | None = "guard_0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = "assert_current_inert_assignment(uuid,jsonb,bytea,text)"
PLAN_FUNCTION = "assert_current_inert_admission_plan(uuid,uuid,uuid,bigint,text,text)"
ABANDON_FUNCTION = "abandon_inert_admission_plan(uuid,jsonb,bytea,text)"
TOMBSTONE_FUNCTION = (
    "tombstone_never_converged_admission_plan"
    "(uuid,jsonb,bytea,text,bytea,text,bytea,text,bytea,text)"
)
PREPARE_EXCLUSION_FUNCTION = "enforce_never_converged_admission_exclusion()"
PREPARE_EXCLUSION_TRIGGER = "prepared_admission_plans_never_converged_exclusion"
UNCLAIMED_PLAN_FUNCTION = "assert_inert_admission_plan_unclaimed(uuid)"
WITHDRAWAL_GUARD_FUNCTION = "enforce_inert_admission_plan_withdrawal_claim_safety()"
WITHDRAWAL_GUARD_TRIGGER = "attempt_lifecycle_events_claim_safe_withdrawal"
CAPTURE_FUNCTIONS = (
    "capture_demand_observation_v1_legacy(uuid,bigint,integer)",
    "capture_lifecycle_demand_observation_v2_queued(uuid,bigint,integer)",
)
CANDIDATE_DIGEST_PAYLOAD_CLAUSE = (
    "'candidate_digest', v_registration.candidate_digest,"
)
EXACT_CANDIDATE_PAYLOAD_CLAUSE = (
    "'candidate_digest', v_registration.candidate_digest,\n"
    "            'candidate_identity_algorithm', "
    "v_registration.candidate_identity_algorithm,\n"
    "            'candidate_identity', v_registration.candidate_identity,\n"
    "            'candidate_publication_sha256', "
    "v_registration.candidate_publication_sha256,"
)


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("current-assignment migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


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
            RAISE EXCEPTION 'current-assignment function clause not found: {function}';
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


def _patch_capture_candidate_provenance() -> None:
    for function in CAPTURE_FUNCTIONS:
        _replace_function_clause(
            function,
            CANDIDATE_DIGEST_PAYLOAD_CLAUSE,
            EXACT_CANDIDATE_PAYLOAD_CLAUSE,
        )


def _unpatch_capture_candidate_provenance() -> None:
    for function in CAPTURE_FUNCTIONS:
        _replace_function_clause(
            function,
            EXACT_CANDIDATE_PAYLOAD_CLAUSE,
            CANDIDATE_DIGEST_PAYLOAD_CLAUSE,
        )


def _install_current_assignment_assertion() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.assert_current_inert_assignment(
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
          v_assignment {SCHEMA}.attempt_lifecycle_events%ROWTYPE;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION
              'current protected assignment assertion requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          SELECT event.* INTO v_assignment
            FROM {SCHEMA}.attempt_lifecycle_heads AS head
            JOIN {SCHEMA}.attempt_lifecycle_events AS event
              ON event.transition_id = head.transition_id
             AND event.protected_attempt_id = head.protected_attempt_id
           WHERE head.protected_attempt_id =
                 (p_payload->>'protected_attempt_id')::uuid
             AND head.transition_id = (p_payload->>'transition_id')::uuid
             AND head.transition_sequence =
                 (p_payload->>'expected_transition_sequence')::bigint + 1
             AND head.lifecycle_state = 'assigned'
             AND head.executable = false
             AND event.execution_generation =
                 (p_payload->>'execution_generation')::bigint
             AND event.requirements_digest = p_payload->>'requirements_digest'
             AND event.transition_sequence = head.transition_sequence
             AND event.operation = 'assign'
             AND event.previous_state = 'pending-unassigned'
             AND event.lifecycle_state = 'assigned'
             AND event.allowance_id = (p_payload->>'allowance_id')::uuid
             AND event.plan_id = (p_payload->>'plan_id')::uuid
             AND event.admission_incarnation =
                 (p_payload->>'admission_incarnation')::uuid
             AND event.manager_allocation_epoch =
                 (p_payload->>'manager_allocation_epoch')::bigint
             AND event.pool_id = p_payload->>'pool_id'
             AND event.shape_instance_id = p_payload->>'shape_instance_id'
             AND event.submission_intent_id =
                 (p_payload->>'submission_intent_id')::uuid
             AND event.executable = false
             AND event.payload = p_payload
             AND event.payload_digest = p_payload_digest
           FOR UPDATE OF head FOR KEY SHARE OF event;
          IF NOT FOUND OR EXISTS (
            SELECT 1 FROM {SCHEMA}.executable_claim_leases
             WHERE protected_attempt_id = v_assignment.protected_attempt_id
          ) THEN
            RAISE EXCEPTION 'current protected assignment is not current and runnable'
              USING ERRCODE = '55000';
          END IF;

          PERFORM 1
            FROM {SCHEMA}.prepared_placement_allowances AS allowance
            JOIN {SCHEMA}.prepared_admission_plans AS plan
              ON plan.plan_id = allowance.plan_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.protected_attempt_id = allowance.protected_attempt_id
             AND attempt.execution_generation = allowance.execution_generation
             AND attempt.requirements_digest = allowance.requirements_digest
            JOIN public.trials AS trial ON trial.id = attempt.trial_id
           WHERE allowance.allowance_id = v_assignment.allowance_id
             AND allowance.plan_id = v_assignment.plan_id
             AND allowance.protected_attempt_id = v_assignment.protected_attempt_id
             AND allowance.execution_generation = v_assignment.execution_generation
             AND allowance.requirements_digest = v_assignment.requirements_digest
             AND allowance.pool_id = v_assignment.pool_id
             AND allowance.shape_instance_id = v_assignment.shape_instance_id
             AND allowance.submission_intent_id = v_assignment.submission_intent_id
             AND allowance.allowance_state = 'prepared'
             AND allowance.executable = false
             AND (allowance.payload->>'allowance_id')::uuid = allowance.allowance_id
             AND (allowance.payload->>'protected_attempt_id')::uuid =
                 allowance.protected_attempt_id
             AND (allowance.payload->>'execution_generation')::bigint =
                 allowance.execution_generation
             AND allowance.payload->>'requirements_digest' =
                 allowance.requirements_digest
             AND allowance.payload->>'pool_id' = allowance.pool_id
             AND allowance.payload->>'shape_instance_id' =
                 allowance.shape_instance_id
             AND (allowance.payload->>'shape_slot_index')::bigint =
                 allowance.shape_slot_index
             AND (allowance.payload->>'submission_intent_id')::uuid =
                 allowance.submission_intent_id
             AND allowance.payload->>'allowance_state' = 'prepared'
             AND (allowance.payload->>'executable')::boolean = false
             AND plan.agent_incarnation = p_agent_incarnation
             AND plan.admission_incarnation = v_assignment.admission_incarnation
             AND plan.manager_allocation_epoch =
                 v_assignment.manager_allocation_epoch
             AND plan.pool_id = v_assignment.pool_id
             AND plan.plan_state = 'prepared'
             AND plan.executable = false
             AND plan.lease_not_after > statement_timestamp()
             AND (plan.payload->>'plan_id')::uuid = plan.plan_id
             AND (plan.payload->>'agent_incarnation')::uuid =
                 plan.agent_incarnation
             AND (plan.payload->>'admission_incarnation')::uuid =
                 plan.admission_incarnation
             AND (plan.payload->>'manager_allocation_epoch')::bigint =
                 plan.manager_allocation_epoch
             AND plan.payload->>'pool_id' = plan.pool_id
             AND (plan.payload->>'lease_not_after')::timestamptz =
                 plan.lease_not_after
             AND plan.payload->>'plan_state' = 'prepared'
             AND (plan.payload->>'executable')::boolean = false
             AND NOT EXISTS (
               SELECT 1 FROM {SCHEMA}.abandoned_admission_plans AS abandoned
                WHERE abandoned.plan_id = plan.plan_id
             )
             AND EXISTS (
               SELECT 1
                 FROM jsonb_array_elements(
                        plan.payload->'placement_allowances'
                      ) AS item(value)
                WHERE item.value = allowance.payload
             )
             AND attempt.claim_state = 'queued'
             AND trial.state = 'queued'
             AND trial.cancellation_requested_at IS NULL
             AND (trial.next_attempt_at IS NULL
                  OR trial.next_attempt_at <= statement_timestamp())
             AND trial.worker_id IS NULL
             AND trial.autoscaler_pool_name IS NULL
           FOR UPDATE OF attempt, trial
           FOR KEY SHARE OF allowance, plan;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'current protected assignment is not current and runnable'
              USING ERRCODE = '55000';
          END IF;
          RETURN v_assignment.payload;
        END
        $function$
        """
    )


def _install_current_plan_assertion() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.assert_current_inert_admission_plan(
          p_agent_incarnation uuid,
          p_plan_id uuid,
          p_admission_incarnation uuid,
          p_manager_allocation_epoch bigint,
          p_pool_id text,
          p_prepared_plan_digest text
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION
              'current protected admission assertion requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'current protected admission authority changed'
              USING ERRCODE = '55000';
          END IF;
          PERFORM 1
            FROM {SCHEMA}.agent_registrations AS registration
            JOIN {SCHEMA}.authority_state AS authority
              ON authority.singleton_id = registration.singleton_id
             AND authority.environment_id = registration.environment_id
             AND authority.subject_id = registration.subject_id
             AND authority.subject_incarnation = registration.subject_incarnation
             AND authority.authority_incarnation = registration.authority_incarnation
             AND authority.reporter_incarnation = registration.reporter_incarnation
             AND authority.authority_mode = registration.authority_mode
             AND authority.allocation_epoch = registration.allocation_epoch
             AND authority.deployment_generation = registration.deployment_generation
             AND authority.configuration_generation =
                 registration.configuration_generation
             AND authority.candidate_digest = registration.candidate_digest
           WHERE registration.agent_incarnation = p_agent_incarnation
             AND registration.registration_state = 'registered'
             AND registration.authority_mode = 'disabled'
             AND registration.allocation_epoch = 0
           FOR KEY SHARE OF registration, authority;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'current protected admission authority changed'
              USING ERRCODE = '55000';
          END IF;

          PERFORM 1
            FROM {SCHEMA}.prepared_admission_plans AS plan
           WHERE plan.plan_id = p_plan_id
             AND plan.agent_incarnation = p_agent_incarnation
             AND plan.admission_incarnation = p_admission_incarnation
             AND plan.manager_allocation_epoch = p_manager_allocation_epoch
             AND plan.pool_id = p_pool_id
             AND plan.payload_digest = p_prepared_plan_digest
             AND plan.plan_state = 'prepared'
             AND plan.executable = false
             AND plan.lease_not_after > statement_timestamp()
             AND (plan.payload->>'plan_id')::uuid = plan.plan_id
             AND (plan.payload->>'agent_incarnation')::uuid = plan.agent_incarnation
             AND (plan.payload->>'admission_incarnation')::uuid =
                 plan.admission_incarnation
             AND (plan.payload->>'manager_allocation_epoch')::bigint =
                 plan.manager_allocation_epoch
             AND plan.payload->>'pool_id' = plan.pool_id
             AND (plan.payload->>'lease_not_after')::timestamptz =
                 plan.lease_not_after
             AND plan.payload->>'plan_state' = 'prepared'
             AND (plan.payload->>'executable')::boolean = false
             AND NOT EXISTS (
               SELECT 1 FROM {SCHEMA}.abandoned_admission_plans AS abandoned
                WHERE abandoned.plan_id = plan.plan_id
             )
           FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'current protected admission plan is not current and runnable'
              USING ERRCODE = '55000';
          END IF;
          RETURN true;
        END
        $function$
        """
    )


def _install_claim_safe_abandonment() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.assert_inert_admission_plan_unclaimed(
          p_plan_id uuid
        )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_claim_operation_id uuid;
          v_claim_found boolean := false;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION
              'protected admission claim safety requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;

          PERFORM head.protected_attempt_id
            FROM {SCHEMA}.prepared_placement_allowances AS allowance
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = allowance.protected_attempt_id
           WHERE allowance.plan_id = p_plan_id
           ORDER BY head.protected_attempt_id
           FOR UPDATE OF head;

          FOR v_claim_operation_id IN
            SELECT lease.operation_id
              FROM {SCHEMA}.prepared_placement_allowances AS allowance
              JOIN {SCHEMA}.executable_claim_leases AS lease
                ON lease.protected_attempt_id = allowance.protected_attempt_id
               AND lease.execution_generation = allowance.execution_generation
               AND lease.requirements_digest = allowance.requirements_digest
             WHERE allowance.plan_id = p_plan_id
             ORDER BY lease.protected_attempt_id, lease.operation_id
             FOR UPDATE OF lease
          LOOP
            v_claim_found := true;
          END LOOP;
          IF v_claim_found THEN
            RAISE EXCEPTION 'protected admission plan has an executable claim lease'
              USING ERRCODE = '55000';
          END IF;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_inert_admission_plan_withdrawal_claim_safety()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF NEW.operation = 'withdraw' AND NEW.plan_id IS NOT NULL THEN
            PERFORM {SCHEMA}.assert_inert_admission_plan_unclaimed(NEW.plan_id);
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {WITHDRAWAL_GUARD_TRIGGER}
        BEFORE INSERT ON {SCHEMA}.attempt_lifecycle_events
        FOR EACH ROW EXECUTE FUNCTION
          {SCHEMA}.enforce_inert_admission_plan_withdrawal_claim_safety()
        """
    )


def _create_abandonment_table() -> None:
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.abandoned_admission_plans (
          closure_id uuid PRIMARY KEY,
          proposal_id uuid NOT NULL UNIQUE,
          proposal_digest text NOT NULL,
          plan_id uuid NOT NULL UNIQUE
            REFERENCES {SCHEMA}.prepared_admission_plans(plan_id) ON DELETE RESTRICT,
          admission_incarnation uuid NOT NULL,
          agent_incarnation uuid NOT NULL
            REFERENCES {SCHEMA}.agent_registrations(agent_incarnation) ON DELETE RESTRICT,
          manager_authority_incarnation uuid NOT NULL,
          manager_writer_epoch bigint NOT NULL,
          manager_allocation_epoch bigint NOT NULL,
          manager_input_digest text NOT NULL,
          manager_allocation_digest text NOT NULL,
          pool_id text NOT NULL,
          close_reason text NOT NULL,
          abandonment_state text NOT NULL,
          executable boolean NOT NULL,
          payload jsonb NOT NULL,
          payload_digest text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT guard_abandoned_plan_state_check CHECK (
            abandonment_state = 'abandoned' AND executable = false
          ),
          CONSTRAINT guard_abandoned_plan_reason_check CHECK (
            close_reason IN ('expired', 'allocation-superseded', 'manager-closed')
          ),
          CONSTRAINT guard_abandoned_plan_generation_check CHECK (
            manager_writer_epoch >= 0 AND manager_allocation_epoch > 0
          ),
          CONSTRAINT guard_abandoned_plan_digest_check CHECK (
            proposal_digest ~ '^[0-9a-f]{{64}}$'
            AND manager_input_digest ~ '^[0-9a-f]{{64}}$'
            AND manager_allocation_digest ~ '^[0-9a-f]{{64}}$'
            AND payload_digest ~ '^[0-9a-f]{{64}}$'
          ),
          CONSTRAINT guard_abandoned_plan_pool_check CHECK (
            pool_id IN ('oldlab', 'gb10')
          ),
          CONSTRAINT guard_abandoned_plan_payload_check CHECK (
            jsonb_typeof(payload) = 'object'
            AND octet_length(payload::text) <= 8388608
          )
        )
        """
    )


def _install_closure_dispositions() -> None:
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.never_converged_admission_plans (
          closure_id uuid PRIMARY KEY,
          proposal_id uuid NOT NULL UNIQUE,
          plan_id uuid NOT NULL UNIQUE,
          admission_incarnation uuid NOT NULL,
          agent_incarnation uuid NOT NULL
            REFERENCES {SCHEMA}.agent_registrations(agent_incarnation) ON DELETE RESTRICT,
          registration_digest text NOT NULL,
          closure_digest text NOT NULL,
          proposal_digest text NOT NULL,
          close_reason text NOT NULL,
          disposition_state text NOT NULL,
          executable boolean NOT NULL,
          payload jsonb NOT NULL,
          payload_digest text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT guard_never_converged_state_check CHECK (
            disposition_state = 'never-converged' AND executable = false
          ),
          CONSTRAINT guard_never_converged_reason_check CHECK (
            close_reason IN ('expired', 'allocation-superseded', 'manager-closed')
          ),
          CONSTRAINT guard_never_converged_digest_check CHECK (
            registration_digest ~ '^[0-9a-f]{{64}}$'
            AND closure_digest ~ '^[0-9a-f]{{64}}$'
            AND proposal_digest ~ '^[0-9a-f]{{64}}$'
            AND payload_digest ~ '^[0-9a-f]{{64}}$'
          ),
          CONSTRAINT guard_never_converged_payload_check CHECK (
            jsonb_typeof(payload) = 'object'
            AND octet_length(payload::text) <= 8388608
          )
        )
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER never_converged_admission_plans_append_only_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.never_converged_admission_plans
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER never_converged_admission_plans_append_only_truncate
        BEFORE TRUNCATE ON {SCHEMA}.never_converged_admission_plans
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_never_converged_admission_exclusion()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.never_converged_admission_plans
             WHERE plan_id = NEW.plan_id
          ) THEN
            RAISE EXCEPTION
              'prepared admission plan was closed as never-converged'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {PREPARE_EXCLUSION_TRIGGER}
        BEFORE INSERT ON {SCHEMA}.prepared_admission_plans
        FOR EACH ROW EXECUTE FUNCTION
          {SCHEMA}.enforce_never_converged_admission_exclusion()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.tombstone_never_converged_admission_plan(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text,
          p_registration_canonical_payload bytea,
          p_registration_digest text,
          p_closure_canonical_payload bytea,
          p_closure_digest text,
          p_proposal_canonical_payload bytea,
          p_proposal_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_existing {SCHEMA}.never_converged_admission_plans%ROWTYPE;
          v_closure jsonb := p_payload->'closure';
          v_proposal jsonb := p_payload->'closure'->'proposal';
          v_binding jsonb := p_payload->'closure'->'proposal'->'shapes'->0->'binding';
          v_registration jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION
              'protected never-converged tombstone requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          v_registration := jsonb_build_object(
            'schema_version', p_payload->'schema_version',
            'environment_id', p_payload->'environment_id',
            'subject_id', p_payload->'subject_id',
            'subject_incarnation', p_payload->'subject_incarnation',
            'authority_incarnation', p_payload->'authority_incarnation',
            'agent_incarnation', p_payload->'agent_incarnation',
            'reporter_incarnation', p_payload->'reporter_incarnation',
            'authority_mode', p_payload->'authority_mode',
            'allocation_epoch', p_payload->'allocation_epoch',
            'reporter_high_water', p_payload->'reporter_high_water',
            'candidate_digest', p_payload->'candidate_digest',
            'candidate_identity_algorithm', p_payload->'candidate_identity_algorithm',
            'candidate_identity', p_payload->'candidate_identity',
            'candidate_publication_sha256', p_payload->'candidate_publication_sha256',
            'deployment_generation', p_payload->'deployment_generation',
            'configuration_generation', p_payload->'configuration_generation'
          );
          IF jsonb_typeof(v_closure) IS DISTINCT FROM 'object'
             OR jsonb_typeof(v_proposal) IS DISTINCT FROM 'object'
             OR jsonb_typeof(v_binding) IS DISTINCT FROM 'object'
             OR (SELECT count(*) FROM jsonb_object_keys(p_payload))
                  IS DISTINCT FROM 22::bigint
             OR p_payload->>'disposition_state' IS DISTINCT FROM 'never-converged'
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR p_payload->>'registration_digest' !~ '^[0-9a-f]{{64}}$'
             OR p_payload->>'closure_digest' !~ '^[0-9a-f]{{64}}$'
             OR p_payload->>'proposal_digest' !~ '^[0-9a-f]{{64}}$'
             OR octet_length(p_registration_canonical_payload) > 8388608
             OR octet_length(p_closure_canonical_payload) > 8388608
             OR octet_length(p_proposal_canonical_payload) > 8388608
             OR convert_from(p_registration_canonical_payload, 'UTF8')::jsonb
                  IS DISTINCT FROM v_registration
             OR convert_from(p_closure_canonical_payload, 'UTF8')::jsonb
                  IS DISTINCT FROM v_closure
             OR convert_from(p_proposal_canonical_payload, 'UTF8')::jsonb
                  IS DISTINCT FROM v_proposal
             OR convert_from(p_canonical_payload, 'UTF8') IS DISTINCT FROM
                  {SCHEMA}.canonical_executable_publication_payload(p_payload)
             OR convert_from(p_registration_canonical_payload, 'UTF8')
                  IS DISTINCT FROM
                  {SCHEMA}.canonical_executable_publication_payload(v_registration)
             OR convert_from(p_closure_canonical_payload, 'UTF8') IS DISTINCT FROM
                  {SCHEMA}.canonical_executable_publication_payload(v_closure)
             OR convert_from(p_proposal_canonical_payload, 'UTF8') IS DISTINCT FROM
                  {SCHEMA}.canonical_executable_publication_payload(v_proposal)
             OR encode(sha256(p_registration_canonical_payload), 'hex')
                  IS DISTINCT FROM p_registration_digest
             OR encode(sha256(p_closure_canonical_payload), 'hex')
                  IS DISTINCT FROM p_closure_digest
             OR encode(sha256(p_proposal_canonical_payload), 'hex')
                  IS DISTINCT FROM p_proposal_digest
             OR p_registration_digest
                  IS DISTINCT FROM p_payload->>'registration_digest'
             OR p_closure_digest IS DISTINCT FROM p_payload->>'closure_digest'
             OR p_proposal_digest IS DISTINCT FROM p_payload->>'proposal_digest'
             OR v_closure->>'close_reason' NOT IN
                  ('expired', 'allocation-superseded', 'manager-closed')
             OR (v_closure->>'closure_id')::uuid IS NULL
             OR (v_proposal->>'proposal_id')::uuid IS NULL
             OR (v_proposal->>'plan_id')::uuid IS NULL
             OR (v_proposal->>'admission_incarnation')::uuid IS NULL
             OR v_binding->>'subject_id' IS DISTINCT FROM p_payload->>'subject_id'
             OR v_binding->>'subject_incarnation'
                  IS DISTINCT FROM p_payload->>'subject_incarnation'
             OR v_proposal->>'reporter_incarnation'
                  IS DISTINCT FROM p_payload->>'reporter_incarnation'
             OR (v_binding->>'deployment_generation')::bigint
                  IS DISTINCT FROM (p_payload->>'deployment_generation')::bigint
             OR v_binding->'candidate'->>'algorithm'
                  IS DISTINCT FROM p_payload->>'candidate_identity_algorithm'
             OR v_binding->'candidate'->>'identity'
                  IS DISTINCT FROM p_payload->>'candidate_identity'
             OR v_binding->'candidate'->>'publication_sha256'
                  IS DISTINCT FROM p_payload->>'candidate_publication_sha256'
             OR EXISTS (
                  SELECT 1
                    FROM jsonb_array_elements(v_proposal->'shapes') AS shape(value)
                   WHERE jsonb_typeof(shape.value) IS DISTINCT FROM 'object'
                      OR jsonb_typeof(shape.value->'binding')
                           IS DISTINCT FROM 'object'
                      OR shape.value->'binding'->'execution'
                           IS DISTINCT FROM v_binding->'execution'
                      OR shape.value->'binding'->>'tranche_id'
                           IS DISTINCT FROM v_binding->>'tranche_id'
                      OR shape.value->'binding'->>'subject_id'
                           IS DISTINCT FROM v_binding->>'subject_id'
                      OR shape.value->'binding'->>'subject_incarnation'
                           IS DISTINCT FROM v_binding->>'subject_incarnation'
                      OR shape.value->'binding'->'candidate'
                           IS DISTINCT FROM v_binding->'candidate'
                      OR shape.value->'binding'->>'candidate_generation'
                           IS DISTINCT FROM v_binding->>'candidate_generation'
                      OR shape.value->'binding'->>'deployment_generation'
                           IS DISTINCT FROM v_binding->>'deployment_generation'
                      OR shape.value->'binding'->>'pool_id'
                           IS DISTINCT FROM v_binding->>'pool_id'
                      OR shape.value->'binding'->>'pool_generation'
                           IS DISTINCT FROM v_binding->>'pool_generation'
                      OR shape.value->'binding'->>'executor_id'
                           IS DISTINCT FROM v_binding->>'executor_id'
                      OR shape.value->'binding'->>'executor_incarnation'
                           IS DISTINCT FROM v_binding->>'executor_incarnation'
                      OR shape.value->'binding'->>'profile_id'
                           IS DISTINCT FROM v_binding->>'profile_id'
                      OR shape.value->'binding'->>'profile_generation'
                           IS DISTINCT FROM v_binding->>'profile_generation'
                      OR shape.value->'binding'->>'profile_digest'
                           IS DISTINCT FROM v_binding->>'profile_digest'
                      OR shape.value->>'protocol_generation'
                           IS DISTINCT FROM v_proposal->'shapes'->0->>'protocol_generation'
                      OR shape.value->>'protocol_digest'
                           IS DISTINCT FROM v_proposal->'shapes'->0->>'protocol_digest'
                      OR NOT EXISTS (
                           SELECT 1
                             FROM {SCHEMA}.protected_executable_bootstrap_registrations
                                  AS bootstrap
                            WHERE bootstrap.agent_incarnation = p_agent_incarnation
                              AND bootstrap.subject_id =
                                  (shape.value->'binding'->>'subject_id')::uuid
                              AND bootstrap.subject_incarnation =
                                  (shape.value->'binding'->>'subject_incarnation')::uuid
                              AND bootstrap.intent_id =
                                  (shape.value->'binding'->>'intent_id')::uuid
                              AND bootstrap.bootstrap_registration_epoch =
                                  (shape.value->>'bootstrap_registration_epoch')::bigint
                              AND bootstrap.protected_admission_sha256 =
                                  v_proposal->>'protected_admission_sha256'
                              AND bootstrap.binding = shape.value->'binding'
                         )
                )
             OR NOT EXISTS (
                  SELECT 1 FROM {SCHEMA}.audit_events AS audit
                   WHERE audit.event_type IN
                         ('agent_registered.v1', 'agent_reconfigured.v1')
                     AND audit.payload_digest = p_payload->>'registration_digest'
                     AND audit.payload = v_registration
                )
             OR (v_closure->>'close_reason' = 'expired'
                 AND (v_proposal->>'lease_not_after')::timestamptz
                     > statement_timestamp()) THEN
            RAISE EXCEPTION 'protected never-converged tombstone payload changed'
              USING ERRCODE = '55000';
          END IF;

          SELECT * INTO v_existing
            FROM {SCHEMA}.never_converged_admission_plans
           WHERE closure_id = (v_closure->>'closure_id')::uuid
              OR proposal_id = (v_proposal->>'proposal_id')::uuid
              OR plan_id = (v_proposal->>'plan_id')::uuid
           FOR UPDATE;
          IF FOUND THEN
            IF v_existing.closure_id IS DISTINCT FROM
                 (v_closure->>'closure_id')::uuid
               OR v_existing.proposal_id IS DISTINCT FROM
                  (v_proposal->>'proposal_id')::uuid
               OR v_existing.plan_id IS DISTINCT FROM (v_proposal->>'plan_id')::uuid
               OR v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest THEN
              RAISE EXCEPTION 'protected never-converged tombstone replay changed'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.prepared_admission_plans
             WHERE plan_id = (v_proposal->>'plan_id')::uuid
             FOR KEY SHARE
          ) THEN
            RAISE EXCEPTION 'prepared admission plan already exists'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.never_converged_admission_plans
            (closure_id, proposal_id, plan_id, admission_incarnation,
             agent_incarnation, registration_digest, closure_digest,
             proposal_digest, close_reason, disposition_state, executable,
             payload, payload_digest)
          VALUES
            ((v_closure->>'closure_id')::uuid,
             (v_proposal->>'proposal_id')::uuid,
             (v_proposal->>'plan_id')::uuid,
             (v_proposal->>'admission_incarnation')::uuid,
             p_agent_incarnation,
             p_payload->>'registration_digest',
             p_payload->>'closure_digest',
             p_payload->>'proposal_digest',
             v_closure->>'close_reason', 'never-converged', false,
             p_payload, p_payload_digest);
          RETURN p_payload;
        END
        $function$
        """
    )
    _install_claim_safe_abandonment()
    op.execute(
        f"""
        CREATE TRIGGER abandoned_admission_plans_append_only_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.abandoned_admission_plans
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER abandoned_admission_plans_append_only_truncate
        BEFORE TRUNCATE ON {SCHEMA}.abandoned_admission_plans
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.abandon_inert_admission_plan(
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
          v_existing {SCHEMA}.abandoned_admission_plans%ROWTYPE;
          v_plan {SCHEMA}.prepared_admission_plans%ROWTYPE;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION
              'protected admission abandonment requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          IF (p_payload->>'schema_version')::integer IS DISTINCT FROM 1
             OR (p_payload->>'closure_id')::uuid IS NULL
             OR (p_payload->>'proposal_id')::uuid IS NULL
             OR (p_payload->>'plan_id')::uuid IS NULL
             OR (p_payload->>'admission_incarnation')::uuid IS NULL
             OR (p_payload->>'manager_authority_incarnation')::uuid IS NULL
             OR (p_payload->>'manager_writer_epoch')::bigint < 0
             OR (p_payload->>'manager_allocation_epoch')::bigint <= 0
             OR p_payload->>'close_reason' NOT IN
                  ('expired', 'allocation-superseded', 'manager-closed')
             OR p_payload->>'abandonment_state' IS DISTINCT FROM 'abandoned'
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'protected admission abandonment payload is invalid'
              USING ERRCODE = '55000';
          END IF;

          SELECT * INTO v_existing
            FROM {SCHEMA}.abandoned_admission_plans
           WHERE closure_id = (p_payload->>'closure_id')::uuid
              OR proposal_id = (p_payload->>'proposal_id')::uuid
              OR plan_id = (p_payload->>'plan_id')::uuid
           FOR UPDATE;
          IF FOUND THEN
            IF v_existing.closure_id IS DISTINCT FROM
                 (p_payload->>'closure_id')::uuid
               OR v_existing.proposal_id IS DISTINCT FROM
                  (p_payload->>'proposal_id')::uuid
               OR v_existing.plan_id IS DISTINCT FROM (p_payload->>'plan_id')::uuid
               OR v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest THEN
              RAISE EXCEPTION 'protected admission abandonment replay changed'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          SELECT * INTO v_plan
            FROM {SCHEMA}.prepared_admission_plans
           WHERE plan_id = (p_payload->>'plan_id')::uuid
             AND agent_incarnation = p_agent_incarnation
             AND admission_incarnation =
                 (p_payload->>'admission_incarnation')::uuid
             AND manager_authority_incarnation =
                 (p_payload->>'manager_authority_incarnation')::uuid
             AND manager_writer_epoch =
                 (p_payload->>'manager_writer_epoch')::bigint
             AND manager_allocation_epoch =
                 (p_payload->>'manager_allocation_epoch')::bigint
             AND manager_input_digest = p_payload->>'manager_input_digest'
             AND manager_allocation_digest = p_payload->>'manager_allocation_digest'
             AND pool_id = p_payload->>'pool_id'
             AND plan_state = 'prepared'
             AND executable = false
           FOR UPDATE;
          IF NOT FOUND
             OR (p_payload->>'close_reason' = 'expired'
                 AND v_plan.lease_not_after > statement_timestamp()) THEN
            RAISE EXCEPTION 'protected admission plan is not safely abandonable'
              USING ERRCODE = '55000';
          END IF;

          PERFORM {SCHEMA}.assert_inert_admission_plan_unclaimed(v_plan.plan_id);
          IF EXISTS (
            SELECT 1
              FROM {SCHEMA}.prepared_placement_allowances AS allowance
              JOIN {SCHEMA}.attempt_lifecycle_heads AS head
                ON head.protected_attempt_id = allowance.protected_attempt_id
             WHERE allowance.plan_id = v_plan.plan_id
               AND head.lifecycle_state = 'assigned'
          ) THEN
            RAISE EXCEPTION 'protected admission plan is not safely abandonable'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.abandoned_admission_plans
            (closure_id, proposal_id, proposal_digest, plan_id, admission_incarnation,
             agent_incarnation, manager_authority_incarnation,
             manager_writer_epoch, manager_allocation_epoch,
             manager_input_digest, manager_allocation_digest, pool_id,
             close_reason, abandonment_state, executable, payload, payload_digest)
          VALUES
            ((p_payload->>'closure_id')::uuid,
             (p_payload->>'proposal_id')::uuid,
             p_payload->>'proposal_digest',
             (p_payload->>'plan_id')::uuid,
             (p_payload->>'admission_incarnation')::uuid,
             p_agent_incarnation,
             (p_payload->>'manager_authority_incarnation')::uuid,
             (p_payload->>'manager_writer_epoch')::bigint,
             (p_payload->>'manager_allocation_epoch')::bigint,
             p_payload->>'manager_input_digest',
             p_payload->>'manager_allocation_digest',
             p_payload->>'pool_id', p_payload->>'close_reason',
             'abandoned', false, p_payload, p_payload_digest);
          RETURN p_payload;
        END
        $function$
        """
    )


def upgrade() -> None:
    quoted_agent = _agent_role()
    _patch_capture_candidate_provenance()
    _create_abandonment_table()
    _install_closure_dispositions()
    _install_current_plan_assertion()
    _install_current_assignment_assertion()
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{WITHDRAWAL_GUARD_FUNCTION} "
        "FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{PREPARE_EXCLUSION_FUNCTION} "
        "FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{TOMBSTONE_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{UNCLAIMED_PLAN_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{ABANDON_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{PLAN_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC"
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{ABANDON_FUNCTION} TO {quoted_agent}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{TOMBSTONE_FUNCTION} TO {quoted_agent}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{PLAN_FUNCTION} TO {quoted_agent}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quoted_agent}")


def downgrade() -> None:
    op.execute(
        f"LOCK TABLE {SCHEMA}.abandoned_admission_plans, "
        f"{SCHEMA}.never_converged_admission_plans IN ACCESS EXCLUSIVE MODE"
    )
    if op.get_bind().execute(
        sa.text(f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.abandoned_admission_plans)")
    ).scalar_one():
        raise RuntimeError("cannot downgrade guard_0021 with abandonment evidence")
    if op.get_bind().execute(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.never_converged_admission_plans)"
        )
    ).scalar_one():
        raise RuntimeError("cannot downgrade guard_0021 with closure disposition evidence")
    quoted_agent = _agent_role()
    _unpatch_capture_candidate_provenance()
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} FROM {quoted_agent}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{PLAN_FUNCTION} FROM {quoted_agent}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{PLAN_FUNCTION}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{ABANDON_FUNCTION} FROM {quoted_agent}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{ABANDON_FUNCTION}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{TOMBSTONE_FUNCTION} FROM {quoted_agent}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{TOMBSTONE_FUNCTION}")
    op.execute(
        f"DROP TRIGGER {PREPARE_EXCLUSION_TRIGGER} ON {SCHEMA}.prepared_admission_plans"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{PREPARE_EXCLUSION_FUNCTION}")
    op.execute(
        f"DROP TRIGGER {WITHDRAWAL_GUARD_TRIGGER} ON {SCHEMA}.attempt_lifecycle_events"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{WITHDRAWAL_GUARD_FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{UNCLAIMED_PLAN_FUNCTION}")
    op.execute(f"DROP TABLE {SCHEMA}.abandoned_admission_plans")
    op.execute(f"DROP TABLE {SCHEMA}.never_converged_admission_plans")
