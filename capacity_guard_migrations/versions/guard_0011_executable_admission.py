"""Protected executable-v2 worker admission and release fence.

Revision ID: guard_0011
Revises: guard_0010
Create Date: 2026-08-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0011"
down_revision: str | Sequence[str] | None = "guard_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
EXTERNAL_FUNCTIONS = (
    "prepare_executable_worker(uuid,uuid,jsonb,bytea,text,text)",
    "bind_executable_slurm_job(uuid,uuid,jsonb,bytea,text)",
    "register_executable_worker(uuid,uuid,jsonb,bytea,text,text,text)",
    "begin_executable_worker_drain(uuid,uuid,jsonb,bytea,text)",
    "acknowledge_executable_release(uuid,uuid,jsonb,bytea,text,text)",
    "admit_executable_claim(uuid,uuid,jsonb,bytea,text)",
)


def _executor_role() -> tuple[str, str]:
    role = op.get_context().config.attributes.get("capacity_guard_executor_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("executable admission migration is missing the executor role")
    return role, op.get_bind().dialect.identifier_preparer.quote(role)


def _append_only(table: str) -> None:
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


def _install_claim_state_guard() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_executable_claim_state()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF TG_OP = 'DELETE'
             OR NEW.intent_id IS DISTINCT FROM OLD.intent_id
             OR NEW.subject_id IS DISTINCT FROM OLD.subject_id
             OR NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
             OR NEW.binding IS DISTINCT FROM OLD.binding
             OR NOT (
               (NEW.claim_high_water = OLD.claim_high_water + 1
                AND OLD.draining = false AND NEW.draining = false)
               OR
               (NEW.claim_high_water = OLD.claim_high_water
                AND OLD.draining = false AND NEW.draining = true)
             ) THEN
            RAISE EXCEPTION 'executable claim state transition is not monotonic'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER executable_claim_state_monotonic_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.executable_claim_state
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.enforce_executable_claim_state()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER executable_claim_state_no_truncate
        BEFORE TRUNCATE ON {SCHEMA}.executable_claim_state
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )


def _install_binding_guard() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.assert_executable_admission_binding(
          p_subject_id uuid,
          p_subject_incarnation uuid,
          p_operation text,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_request_digest text
        )
        RETURNS uuid
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_executor_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_allowed_fields text[];
          v_binding jsonb := p_payload->'binding';
          v_execution jsonb := p_payload->'binding'->'execution';
          v_candidate jsonb := p_payload->'binding'->'candidate';
          v_resources jsonb := p_payload->'binding'->'resources';
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'executable admission requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT executor_role_name INTO v_executor_role
            FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1;
          IF v_executor_role IS NULL OR session_user::text <> v_executor_role THEN
            RAISE EXCEPTION 'executable admission caller is not the bound executor role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'executable admission executor unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          v_allowed_fields := CASE p_operation
            WHEN 'prepare' THEN ARRAY[
              'binding', 'bootstrap_evidence_sha256',
              'bootstrap_registration_epoch', 'command_sequence',
              'executable', 'schema_version'
            ]
            WHEN 'bind' THEN ARRAY[
              'binding', 'bootstrap_registration_epoch', 'executable',
              'operation_id', 'ownership_evidence_sha256', 'schema_version',
              'slurm_job_id'
            ]
            WHEN 'register' THEN ARRAY[
              'binding', 'bootstrap_registration_epoch', 'executable',
              'operation_id', 'predecessor_worker_incarnation',
              'protected_registration_epoch', 'schema_version',
              'slurm_job_id', 'worker_credential_sha256', 'worker_id',
              'worker_incarnation'
            ]
            WHEN 'drain' THEN ARRAY[
              'binding', 'drain_epoch', 'executable',
              'expected_claim_high_water', 'operation_id', 'schema_version',
              'worker_id', 'worker_incarnation'
            ]
            WHEN 'release' THEN ARRAY[
              'binding', 'bootstrap_registration_epoch', 'executable',
              'expected_claim_high_water', 'operation_id',
              'protected_registration_epoch', 'release_epoch',
              'reporter_incarnation', 'schema_version'
            ]
            ELSE NULL
          END;
          IF v_allowed_fields IS NULL
             OR jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR octet_length(p_payload::text) > 8388608
             OR octet_length(p_canonical_payload) > 8388608
             OR convert_from(p_canonical_payload, 'UTF8')::jsonb IS DISTINCT FROM p_payload
             OR p_request_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_canonical_payload), 'hex') IS DISTINCT FROM p_request_digest
             OR NOT p_payload ?& v_allowed_fields
             OR p_payload - v_allowed_fields <> '{{}}'::jsonb
             OR jsonb_typeof(p_payload->'schema_version') IS DISTINCT FROM 'number'
             OR (p_payload->>'schema_version')::integer IS DISTINCT FROM 2
             OR jsonb_typeof(p_payload->'executable') IS DISTINCT FROM 'boolean'
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM true
             OR jsonb_typeof(v_binding) IS DISTINCT FROM 'object'
             OR NOT v_binding ?& ARRAY[
               'account_id', 'candidate', 'candidate_generation',
               'concurrency_slots', 'deployment_generation', 'execution',
               'executor_id', 'executor_incarnation', 'intent_id', 'node_ids',
               'old_shape_backing_id', 'pool_generation', 'pool_id',
               'profile_digest', 'profile_generation', 'profile_id', 'resources',
               'rollout_surge_slots', 'schema_version', 'shape_id',
               'shape_instance_id', 'subject_id', 'subject_incarnation',
               'tier_id', 'tranche_id'
             ]
             OR v_binding - ARRAY[
               'account_id', 'candidate', 'candidate_generation',
               'concurrency_slots', 'deployment_generation', 'execution',
               'executor_id', 'executor_incarnation', 'intent_id', 'node_ids',
               'old_shape_backing_id', 'pool_generation', 'pool_id',
               'profile_digest', 'profile_generation', 'profile_id', 'resources',
               'rollout_surge_slots', 'schema_version', 'shape_id',
               'shape_instance_id', 'subject_id', 'subject_incarnation',
               'tier_id', 'tranche_id'
             ] <> '{{}}'::jsonb
             OR jsonb_typeof(v_binding->'schema_version') IS DISTINCT FROM 'number'
             OR (v_binding->>'schema_version')::integer IS DISTINCT FROM 2
             OR jsonb_typeof(v_binding->'subject_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'subject_id')::uuid IS DISTINCT FROM p_subject_id
             OR jsonb_typeof(v_binding->'subject_incarnation') IS DISTINCT FROM 'string'
             OR (v_binding->>'subject_incarnation')::uuid
                IS DISTINCT FROM p_subject_incarnation THEN
            RAISE EXCEPTION 'executable admission request schema is invalid or oversized'
              USING ERRCODE = '22023';
          END IF;

          IF (p_operation = 'prepare' AND (
               jsonb_typeof(p_payload->'command_sequence') IS DISTINCT FROM 'number'
               OR (p_payload->>'command_sequence' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'command_sequence')::numeric > 9223372036854775807
               OR jsonb_typeof(p_payload->'bootstrap_registration_epoch')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'bootstrap_registration_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'bootstrap_registration_epoch')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'bootstrap_evidence_sha256')
                  IS DISTINCT FROM 'string'
               OR (p_payload->>'bootstrap_evidence_sha256' ~ '^[0-9a-f]{{64}}$')
                  IS DISTINCT FROM true
             ))
             OR (p_operation = 'bind' AND (
               jsonb_typeof(p_payload->'operation_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'operation_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'bootstrap_registration_epoch')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'bootstrap_registration_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'bootstrap_registration_epoch')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'slurm_job_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'slurm_job_id' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'ownership_evidence_sha256')
                  IS DISTINCT FROM 'string'
               OR (p_payload->>'ownership_evidence_sha256' ~ '^[0-9a-f]{{64}}$')
                  IS DISTINCT FROM true
             ))
             OR (p_operation = 'register' AND (
               jsonb_typeof(p_payload->'operation_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'operation_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'bootstrap_registration_epoch')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'bootstrap_registration_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'bootstrap_registration_epoch')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'protected_registration_epoch')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'protected_registration_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'protected_registration_epoch')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'slurm_job_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'slurm_job_id' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'worker_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'worker_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'worker_incarnation') IS DISTINCT FROM 'string'
               OR (p_payload->>'worker_incarnation' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'worker_credential_sha256')
                  IS DISTINCT FROM 'string'
               OR (p_payload->>'worker_credential_sha256' ~ '^[0-9a-f]{{64}}$')
                  IS DISTINCT FROM true
               OR (jsonb_typeof(p_payload->'predecessor_worker_incarnation')
                   IN ('null', 'string')) IS DISTINCT FROM true
               OR (jsonb_typeof(p_payload->'predecessor_worker_incarnation') = 'string'
                   AND (p_payload->>'predecessor_worker_incarnation' ~
                     '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                     IS DISTINCT FROM true)
             ))
             OR (p_operation = 'drain' AND (
               jsonb_typeof(p_payload->'operation_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'operation_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'worker_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'worker_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'worker_incarnation') IS DISTINCT FROM 'string'
               OR (p_payload->>'worker_incarnation' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'expected_claim_high_water')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'expected_claim_high_water' ~ '^[0-9]+$')
                  IS DISTINCT FROM true
               OR (p_payload->>'expected_claim_high_water')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'drain_epoch') IS DISTINCT FROM 'number'
               OR (p_payload->>'drain_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'drain_epoch')::numeric > 9223372036854775807
             ))
             OR (p_operation = 'release' AND (
               jsonb_typeof(p_payload->'operation_id') IS DISTINCT FROM 'string'
               OR (p_payload->>'operation_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'reporter_incarnation') IS DISTINCT FROM 'string'
               OR (p_payload->>'reporter_incarnation' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
               OR jsonb_typeof(p_payload->'bootstrap_registration_epoch')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'bootstrap_registration_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'bootstrap_registration_epoch')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'expected_claim_high_water')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'expected_claim_high_water' ~ '^[0-9]+$')
                  IS DISTINCT FROM true
               OR (p_payload->>'expected_claim_high_water')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'protected_registration_epoch')
                  IS DISTINCT FROM 'number'
               OR (p_payload->>'protected_registration_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'protected_registration_epoch')::numeric >
                  9223372036854775807
               OR jsonb_typeof(p_payload->'release_epoch') IS DISTINCT FROM 'number'
               OR (p_payload->>'release_epoch' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
               OR (p_payload->>'release_epoch')::numeric > 9223372036854775807
             )) THEN
            RAISE EXCEPTION 'executable admission operation schema is invalid'
              USING ERRCODE = '22023';
          END IF;

          IF jsonb_typeof(v_execution) IS DISTINCT FROM 'object'
             OR NOT v_execution ?& ARRAY[
               'allocation_epoch', 'authority_incarnation', 'configuration_epoch',
               'executable', 'executable_new_capacity_ceiling',
               'executable_new_capacity_rate_per_minute', 'execution_epoch',
               'execution_manifest_sha256', 'execution_state', 'schema_version',
               'trusted_fleet_release_sha256', 'writer_epoch'
             ]
             OR v_execution - ARRAY[
               'allocation_epoch', 'authority_incarnation', 'configuration_epoch',
               'executable', 'executable_new_capacity_ceiling',
               'executable_new_capacity_rate_per_minute', 'execution_epoch',
               'execution_manifest_sha256', 'execution_state', 'schema_version',
               'trusted_fleet_release_sha256', 'writer_epoch'
             ] <> '{{}}'::jsonb
             OR jsonb_typeof(v_execution->'schema_version') IS DISTINCT FROM 'number'
             OR (v_execution->>'schema_version')::integer IS DISTINCT FROM 2
             OR jsonb_typeof(v_execution->'executable') IS DISTINCT FROM 'boolean'
             OR (v_execution->>'executable')::boolean IS DISTINCT FROM true
             OR jsonb_typeof(v_execution->'authority_incarnation')
                IS DISTINCT FROM 'string'
             OR (v_execution->>'authority_incarnation' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_execution->'writer_epoch') IS DISTINCT FROM 'number'
             OR (v_execution->>'writer_epoch' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_execution->>'writer_epoch')::numeric > 9223372036854775807
             OR jsonb_typeof(v_execution->'configuration_epoch')
                IS DISTINCT FROM 'number'
             OR (v_execution->>'configuration_epoch' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_execution->>'configuration_epoch')::numeric > 9223372036854775807
             OR jsonb_typeof(v_execution->'allocation_epoch') IS DISTINCT FROM 'number'
             OR (v_execution->>'allocation_epoch' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_execution->>'allocation_epoch')::numeric > 9223372036854775807
             OR jsonb_typeof(v_execution->'execution_epoch') IS DISTINCT FROM 'number'
             OR (v_execution->>'execution_epoch' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_execution->>'execution_epoch')::numeric > 9223372036854775807
             OR jsonb_typeof(v_execution->'executable_new_capacity_ceiling')
                IS DISTINCT FROM 'number'
             OR (v_execution->>'executable_new_capacity_ceiling' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_execution->>'executable_new_capacity_ceiling')::numeric >
                9223372036854775807
             OR jsonb_typeof(v_execution->'executable_new_capacity_rate_per_minute')
                IS DISTINCT FROM 'number'
             OR (v_execution->>'executable_new_capacity_rate_per_minute' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_execution->>'executable_new_capacity_rate_per_minute')::numeric >
                9223372036854775807
             OR jsonb_typeof(v_execution->'execution_state') IS DISTINCT FROM 'string'
             OR v_execution->>'execution_state' IS DISTINCT FROM 'active'
             OR jsonb_typeof(v_execution->'execution_manifest_sha256')
                IS DISTINCT FROM 'string'
             OR (v_execution->>'execution_manifest_sha256' ~ '^[0-9a-f]{{64}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_execution->'trusted_fleet_release_sha256')
                IS DISTINCT FROM 'string'
             OR (v_execution->>'trusted_fleet_release_sha256' ~ '^[0-9a-f]{{64}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_candidate) IS DISTINCT FROM 'object'
             OR NOT v_candidate ?& ARRAY[
               'algorithm', 'identity', 'publication_sha256', 'schema_version'
             ]
             OR v_candidate - ARRAY[
               'algorithm', 'identity', 'publication_sha256', 'schema_version'
             ] <> '{{}}'::jsonb
             OR jsonb_typeof(v_candidate->'schema_version') IS DISTINCT FROM 'number'
             OR (v_candidate->>'schema_version')::integer IS DISTINCT FROM 2
             OR jsonb_typeof(v_candidate->'algorithm') IS DISTINCT FROM 'string'
             OR jsonb_typeof(v_candidate->'identity') IS DISTINCT FROM 'string'
             OR jsonb_typeof(v_candidate->'publication_sha256') IS DISTINCT FROM 'string'
             OR (v_candidate->>'publication_sha256' ~ '^[0-9a-f]{{64}}$')
                IS DISTINCT FROM true
             OR (v_candidate->>'algorithm' = 'git-sha1'
                 AND (v_candidate->>'identity' ~ '^[0-9a-f]{{40}}$')
                     IS DISTINCT FROM true)
             OR (v_candidate->>'algorithm' = 'source-sha256'
                 AND (v_candidate->>'identity' ~ '^[0-9a-f]{{64}}$')
                     IS DISTINCT FROM true)
             OR (v_candidate->>'algorithm' IN ('git-sha1', 'source-sha256'))
                IS DISTINCT FROM true
             OR jsonb_typeof(v_resources) IS DISTINCT FROM 'object'
             OR NOT v_resources ?& ARRAY[
               'cpu_millicores', 'generic', 'gpu_count', 'memory_bytes',
               'schema_version', 'slots'
             ]
             OR v_resources - ARRAY[
               'cpu_millicores', 'generic', 'gpu_count', 'memory_bytes',
               'schema_version', 'slots'
             ] <> '{{}}'::jsonb
             OR jsonb_typeof(v_resources->'schema_version') IS DISTINCT FROM 'number'
             OR (v_resources->>'schema_version')::integer IS DISTINCT FROM 1
             OR jsonb_typeof(v_resources->'slots') IS DISTINCT FROM 'number'
             OR (v_resources->>'slots' ~ '^[0-9]+$') IS DISTINCT FROM true
             OR (v_resources->>'slots')::numeric > 9223372036854775807
             OR jsonb_typeof(v_resources->'cpu_millicores') IS DISTINCT FROM 'number'
             OR (v_resources->>'cpu_millicores' ~ '^[0-9]+$')
                IS DISTINCT FROM true
             OR (v_resources->>'cpu_millicores')::numeric > 9223372036854775807
             OR jsonb_typeof(v_resources->'memory_bytes') IS DISTINCT FROM 'number'
             OR (v_resources->>'memory_bytes' ~ '^[0-9]+$') IS DISTINCT FROM true
             OR (v_resources->>'memory_bytes')::numeric > 9223372036854775807
             OR jsonb_typeof(v_resources->'gpu_count') IS DISTINCT FROM 'number'
             OR (v_resources->>'gpu_count' ~ '^[0-9]+$') IS DISTINCT FROM true
             OR (v_resources->>'gpu_count')::numeric > 9223372036854775807
             OR jsonb_typeof(v_resources->'generic') IS DISTINCT FROM 'object'
             OR EXISTS (
               SELECT 1 FROM jsonb_each(v_resources->'generic') AS resource(name, amount)
                WHERE (resource.name ~ '^[a-z][a-z0-9_.-]{{0,62}}$')
                      IS DISTINCT FROM true
                   OR jsonb_typeof(resource.amount) IS DISTINCT FROM 'number'
                   OR (resource.amount #>> '{{}}' ~ '^[0-9]+$') IS DISTINCT FROM true
                   OR (resource.amount #>> '{{}}')::numeric > 9223372036854775807
             )
             OR jsonb_typeof(v_binding->'tranche_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'tranche_id' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'intent_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'intent_id' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'executor_incarnation') IS DISTINCT FROM 'string'
             OR (v_binding->>'executor_incarnation' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'candidate_generation') IS DISTINCT FROM 'number'
             OR (v_binding->>'candidate_generation' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_binding->>'candidate_generation')::numeric > 9223372036854775807
             OR jsonb_typeof(v_binding->'deployment_generation') IS DISTINCT FROM 'number'
             OR (v_binding->>'deployment_generation' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_binding->>'deployment_generation')::numeric > 9223372036854775807
             OR jsonb_typeof(v_binding->'pool_generation') IS DISTINCT FROM 'number'
             OR (v_binding->>'pool_generation' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_binding->>'pool_generation')::numeric > 9223372036854775807
             OR jsonb_typeof(v_binding->'profile_generation') IS DISTINCT FROM 'number'
             OR (v_binding->>'profile_generation' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_binding->>'profile_generation')::numeric > 9223372036854775807
             OR jsonb_typeof(v_binding->'concurrency_slots') IS DISTINCT FROM 'number'
             OR (v_binding->>'concurrency_slots' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (v_binding->>'concurrency_slots')::numeric > 9223372036854775807
             OR jsonb_typeof(v_binding->'rollout_surge_slots') IS DISTINCT FROM 'number'
             OR (v_binding->>'rollout_surge_slots' ~ '^[0-9]+$')
                IS DISTINCT FROM true
             OR (v_binding->>'rollout_surge_slots')::numeric > 9223372036854775807
             OR (v_resources->>'slots')::bigint IS DISTINCT FROM
                (v_binding->>'concurrency_slots')::bigint
             OR jsonb_typeof(v_binding->'tier_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'tier_id' IN ('production', 'staging', 'development'))
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'account_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'account_id' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'pool_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'pool_id' IN ('oldlab', 'gb10')) IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'executor_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'executor_id' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'shape_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'shape_id' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'shape_instance_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'shape_instance_id' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'profile_id') IS DISTINCT FROM 'string'
             OR (v_binding->>'profile_id' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'profile_digest') IS DISTINCT FROM 'string'
             OR (v_binding->>'profile_digest' ~ '^[0-9a-f]{{64}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(v_binding->'node_ids') IS DISTINCT FROM 'array'
             OR jsonb_array_length(v_binding->'node_ids') < 1
             OR EXISTS (
               SELECT 1 FROM jsonb_array_elements(v_binding->'node_ids') AS node(id)
                WHERE jsonb_typeof(node.id) IS DISTINCT FROM 'string'
                   OR (node.id #>> '{{}}' ~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
                      IS DISTINCT FROM true
             )
             OR (SELECT count(*) FROM jsonb_array_elements_text(v_binding->'node_ids')) <>
                (SELECT count(DISTINCT node.id)
                   FROM jsonb_array_elements_text(v_binding->'node_ids') AS node(id))
             OR (v_binding->>'rollout_surge_slots')::bigint >
                (v_binding->>'concurrency_slots')::bigint
             OR ((v_binding->>'rollout_surge_slots')::bigint > 0) IS DISTINCT FROM
                (v_binding->>'old_shape_backing_id' IS NOT NULL)
             OR (jsonb_typeof(v_binding->'old_shape_backing_id') IN ('null', 'string'))
                IS DISTINCT FROM true
             OR ((v_binding->>'old_shape_backing_id') IS NOT NULL AND
                 v_binding->>'old_shape_backing_id' !~
                   '^[a-z0-9][a-z0-9_.-]{{0,127}}$')
             OR cardinality(ARRAY[
                  p_subject_id,
                  p_subject_incarnation,
                  (v_execution->>'authority_incarnation')::uuid,
                  (v_binding->>'tranche_id')::uuid,
                  (v_binding->>'intent_id')::uuid,
                  (v_binding->>'executor_incarnation')::uuid
                ]) IS DISTINCT FROM cardinality(ARRAY(
                  SELECT DISTINCT identity FROM unnest(ARRAY[
                    p_subject_id,
                    p_subject_incarnation,
                    (v_execution->>'authority_incarnation')::uuid,
                    (v_binding->>'tranche_id')::uuid,
                    (v_binding->>'intent_id')::uuid,
                    (v_binding->>'executor_incarnation')::uuid
                  ]) AS identity
                )) THEN
            RAISE EXCEPTION 'executable intent binding schema is invalid'
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
             AND f.deployment_generation = r.deployment_generation
             AND f.configuration_generation = r.configuration_generation
             AND f.candidate_digest = r.candidate_digest
           WHERE r.subject_id = p_subject_id
             AND r.subject_incarnation = p_subject_incarnation
             AND r.registration_state = 'registered'
           FOR KEY SHARE OF r, f;
          IF NOT FOUND
             OR v_binding->'candidate'->>'publication_sha256'
                IS DISTINCT FROM v_registration.candidate_digest
             OR (v_binding->>'deployment_generation')::bigint
                IS DISTINCT FROM v_registration.deployment_generation THEN
            RAISE EXCEPTION 'executable admission differs from its protected subject binding'
              USING ERRCODE = '55000';
          END IF;
          RETURN v_registration.agent_incarnation;
        END
        $function$
        """
    )


def _install_prepare() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.prepare_executable_worker(
          p_subject_id uuid,
          p_subject_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_request_digest text,
          p_bootstrap_sha256 text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_incarnation uuid;
          v_intent_id uuid;
          v_existing {SCHEMA}.executable_admission_events%ROWTYPE;
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          v_agent_incarnation := {SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'prepare', p_payload,
            p_canonical_payload, p_request_digest
          );
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE;
          v_intent_id := (p_payload->'binding'->>'intent_id')::uuid;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'prepared' FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest
               OR v_existing.bootstrap_sha256 IS DISTINCT FROM p_bootstrap_sha256 THEN
              RAISE EXCEPTION 'conflicting executable preparation'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          IF p_bootstrap_sha256 !~ '^[0-9a-f]{{64}}$'
             OR (p_payload->>'bootstrap_registration_epoch')::bigint <= 0
             OR p_payload->'binding'->'execution'->>'execution_state' <> 'active'
             OR (p_payload->'binding'->'execution'->>'executable_new_capacity_ceiling')::bigint <= 0
             OR EXISTS (SELECT 1 FROM {SCHEMA}.executable_admission_events
                         WHERE intent_id = v_intent_id AND event_kind = 'released') THEN
            RAISE EXCEPTION 'executable preparation is not current and executable'
              USING ERRCODE = '55000';
          END IF;
          SELECT count(*) + 1 INTO v_high_water
            FROM {SCHEMA}.executable_admission_events;
          v_receipt := jsonb_build_object(
            'schema_version', 2, 'subject_id', p_subject_id,
            'subject_incarnation', p_subject_incarnation,
            'intent_id', v_intent_id,
            'bootstrap_registration_epoch',
              (p_payload->>'bootstrap_registration_epoch')::bigint,
            'bootstrap_sha256', p_bootstrap_sha256,
            'request_digest', p_request_digest,
            'admission_digest', p_request_digest,
            'protected_high_water', v_high_water,
            'admission_state', 'prepared', 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_admission_events
            (operation_id, event_kind, agent_incarnation, subject_id,
             subject_incarnation, intent_id, bootstrap_registration_epoch,
             bootstrap_sha256, binding, request_payload, request_digest, receipt)
          VALUES
            (v_intent_id, 'prepared', v_agent_incarnation, p_subject_id,
             p_subject_incarnation, v_intent_id,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             p_bootstrap_sha256, p_payload->'binding', p_payload,
             p_request_digest, v_receipt);
          INSERT INTO {SCHEMA}.executable_claim_state
            (intent_id, subject_id, subject_incarnation, binding,
             claim_high_water, draining)
          VALUES
            (v_intent_id, p_subject_id, p_subject_incarnation,
             p_payload->'binding', 0, false);
          RETURN v_receipt;
        END
        $function$
        """
    )


def _install_physical_binding() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.bind_executable_slurm_job(
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
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          v_agent_incarnation := {SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'bind', p_payload,
            p_canonical_payload, p_request_digest
          );
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE operation_id = v_operation_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.event_kind <> 'physical-bound'
               OR v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
              RAISE EXCEPTION 'conflicting executable physical binding replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'prepared' FOR KEY SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'physical job requires prepared executable admission'
              USING ERRCODE = '55000';
          END IF;
          IF v_prepared.binding IS DISTINCT FROM p_payload->'binding'
             OR v_prepared.bootstrap_registration_epoch IS DISTINCT FROM
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR p_payload->>'slurm_job_id' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR p_payload->>'ownership_evidence_sha256' !~ '^[0-9a-f]{{64}}$'
             OR EXISTS (
               SELECT 1 FROM {SCHEMA}.executable_admission_events
                WHERE event_kind = 'physical-bound'
                  AND binding->>'pool_id' = p_payload->'binding'->>'pool_id'
                  AND physical_job_id = p_payload->>'slurm_job_id'
             )
             OR EXISTS (SELECT 1 FROM {SCHEMA}.executable_admission_events
                         WHERE intent_id = v_intent_id
                           AND event_kind IN ('physical-bound', 'released')) THEN
            RAISE EXCEPTION 'conflicting executable physical binding'
              USING ERRCODE = '55000';
          END IF;
          SELECT count(*) + 1 INTO v_high_water
            FROM {SCHEMA}.executable_admission_events;
          v_receipt := jsonb_build_object(
            'schema_version', 2, 'subject_id', p_subject_id,
            'subject_incarnation', p_subject_incarnation,
            'intent_id', v_intent_id,
            'bootstrap_registration_epoch',
              (p_payload->>'bootstrap_registration_epoch')::bigint,
            'slurm_job_id', p_payload->>'slurm_job_id',
            'ownership_evidence_sha256', p_payload->>'ownership_evidence_sha256',
            'request_digest', p_request_digest, 'binding_digest', p_request_digest,
            'protected_high_water', v_high_water,
            'binding_state', 'bound', 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_admission_events
            (operation_id, event_kind, agent_incarnation, subject_id,
             subject_incarnation, intent_id, bootstrap_registration_epoch,
             physical_job_id, ownership_evidence_sha256, binding,
             request_payload, request_digest, receipt)
          VALUES
            (v_operation_id, 'physical-bound', v_agent_incarnation, p_subject_id,
             p_subject_incarnation, v_intent_id,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             p_payload->>'slurm_job_id', p_payload->>'ownership_evidence_sha256',
             p_payload->'binding', p_payload, p_request_digest, v_receipt);
          RETURN v_receipt;
        END
        $function$
        """
    )


def _install_worker_registration() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.register_executable_worker(
          p_subject_id uuid,
          p_subject_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_request_digest text,
          p_bootstrap_capability text,
          p_predecessor_worker_credential text
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
          v_current {SCHEMA}.executable_admission_events%ROWTYPE;
          v_bootstrap_revoked boolean;
          v_predecessor_revoked boolean;
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          v_agent_incarnation := {SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'register', p_payload,
            p_canonical_payload, p_request_digest
          );
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE operation_id = v_operation_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.event_kind <> 'worker-registered'
               OR v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
              RAISE EXCEPTION 'conflicting worker registration'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          IF EXISTS (SELECT 1 FROM {SCHEMA}.executable_admission_events
                      WHERE intent_id = v_intent_id
                        AND event_kind IN ('draining', 'released')) THEN
            RAISE EXCEPTION 'protected release fence forbids delayed registration'
              USING ERRCODE = '55000';
          END IF;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'prepared' FOR KEY SHARE;
          SELECT * INTO v_physical FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'physical-bound' FOR KEY SHARE;
          IF v_prepared.operation_id IS NULL OR v_physical.operation_id IS NULL
             OR v_prepared.binding IS DISTINCT FROM p_payload->'binding'
             OR v_physical.binding IS DISTINCT FROM p_payload->'binding'
             OR v_prepared.bootstrap_registration_epoch IS DISTINCT FROM
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR v_physical.physical_job_id IS DISTINCT FROM p_payload->>'slurm_job_id'
             OR p_payload->>'worker_credential_sha256' !~ '^[0-9a-f]{{64}}$' THEN
            RAISE EXCEPTION 'worker registration requires exact physical binding'
              USING ERRCODE = '55000';
          END IF;
          SELECT * INTO v_current FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'worker-registered'
           ORDER BY protected_registration_epoch DESC, event_id DESC LIMIT 1
           FOR KEY SHARE;
          IF NOT FOUND THEN
            IF p_payload->>'predecessor_worker_incarnation' IS NOT NULL
               OR p_predecessor_worker_credential IS NOT NULL
               OR p_bootstrap_capability IS NULL
               OR octet_length(p_bootstrap_capability) NOT BETWEEN 1 AND 4096
               OR encode(sha256(convert_to(p_bootstrap_capability, 'UTF8')), 'hex')
                  IS DISTINCT FROM v_prepared.bootstrap_sha256 THEN
              RAISE EXCEPTION 'bootstrap capability exchange failed'
                USING ERRCODE = '42501';
            END IF;
            v_bootstrap_revoked := true;
            v_predecessor_revoked := false;
          ELSE
            IF p_bootstrap_capability IS NOT NULL
               OR p_predecessor_worker_credential IS NULL
               OR octet_length(p_predecessor_worker_credential) NOT BETWEEN 1 AND 4096
               OR (p_payload->>'predecessor_worker_incarnation')::uuid
                  IS DISTINCT FROM v_current.worker_incarnation
               OR encode(sha256(convert_to(p_predecessor_worker_credential, 'UTF8')), 'hex')
                  IS DISTINCT FROM v_current.worker_credential_sha256
               OR v_current.bootstrap_revoked IS DISTINCT FROM true
               OR (p_payload->>'protected_registration_epoch')::bigint <=
                  v_current.protected_registration_epoch THEN
              RAISE EXCEPTION 'requeue predecessor credential or epoch changed'
                USING ERRCODE = '42501';
            END IF;
            v_bootstrap_revoked := v_current.bootstrap_revoked;
            v_predecessor_revoked := true;
          END IF;
          IF (p_payload->>'protected_registration_epoch')::bigint <=
                v_prepared.bootstrap_registration_epoch THEN
            RAISE EXCEPTION 'worker registration epoch did not advance past bootstrap'
              USING ERRCODE = '55000';
          END IF;
          SELECT count(*) + 1 INTO v_high_water
            FROM {SCHEMA}.executable_admission_events;
          v_receipt := jsonb_build_object(
            'schema_version', 2, 'subject_id', p_subject_id,
            'subject_incarnation', p_subject_incarnation,
            'intent_id', v_intent_id,
            'worker_id', p_payload->>'worker_id',
            'worker_incarnation', p_payload->>'worker_incarnation',
            'predecessor_worker_incarnation',
              p_payload->>'predecessor_worker_incarnation',
            'protected_registration_epoch',
              (p_payload->>'protected_registration_epoch')::bigint,
            'request_digest', p_request_digest,
            'registration_digest', p_request_digest,
            'protected_high_water', v_high_water,
            'registration_state', 'registered', 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_admission_events
            (operation_id, event_kind, agent_incarnation, subject_id,
             subject_incarnation, intent_id, bootstrap_registration_epoch,
             protected_registration_epoch, physical_job_id, worker_id,
             worker_incarnation, predecessor_worker_incarnation,
             worker_credential_sha256, bootstrap_revoked,
             predecessor_credential_revoked, worker_credential_revoked,
             binding, request_payload,
             request_digest, receipt)
          VALUES
            (v_operation_id, 'worker-registered', v_agent_incarnation,
             p_subject_id, p_subject_incarnation, v_intent_id,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             (p_payload->>'protected_registration_epoch')::bigint,
             p_payload->>'slurm_job_id', (p_payload->>'worker_id')::uuid,
             (p_payload->>'worker_incarnation')::uuid,
             (p_payload->>'predecessor_worker_incarnation')::uuid,
             p_payload->>'worker_credential_sha256', v_bootstrap_revoked,
             v_predecessor_revoked, false, p_payload->'binding',
             p_payload, p_request_digest, v_receipt);
          RETURN v_receipt;
        END
        $function$
        """
    )


def _install_drain() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.begin_executable_worker_drain(
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
          v_current {SCHEMA}.executable_admission_events%ROWTYPE;
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
          v_claim_high_water bigint;
          v_live_claims bigint;
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          v_agent_incarnation := {SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'drain', p_payload,
            p_canonical_payload, p_request_digest
          );
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE operation_id = v_operation_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.event_kind <> 'draining'
               OR v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
              RAISE EXCEPTION 'conflicting executable drain replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          SELECT * INTO v_current FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'worker-registered'
           ORDER BY protected_registration_epoch DESC, event_id DESC LIMIT 1
           FOR KEY SHARE;
          SELECT * INTO v_state FROM {SCHEMA}.executable_claim_state
           WHERE intent_id = v_intent_id FOR UPDATE;
          SELECT count(*), count(*) FILTER (WHERE lease_state = 'live')
            INTO v_claim_high_water, v_live_claims
            FROM {SCHEMA}.executable_claim_leases
           WHERE intent_id = v_intent_id;
          IF v_current.operation_id IS NULL
             OR v_state.intent_id IS NULL
             OR v_current.binding IS DISTINCT FROM p_payload->'binding'
             OR v_current.worker_id IS DISTINCT FROM (p_payload->>'worker_id')::uuid
             OR v_current.worker_incarnation IS DISTINCT FROM
                (p_payload->>'worker_incarnation')::uuid
             OR v_claim_high_water IS DISTINCT FROM
                (p_payload->>'expected_claim_high_water')::bigint
             OR v_state.claim_high_water IS DISTINCT FROM v_claim_high_water
             OR v_state.draining
             OR EXISTS (
               SELECT 1 FROM {SCHEMA}.executable_admission_events
                WHERE intent_id = v_intent_id AND event_kind = 'draining'
                  AND drain_epoch >= (p_payload->>'drain_epoch')::bigint
             )
             OR EXISTS (SELECT 1 FROM {SCHEMA}.executable_admission_events
                         WHERE intent_id = v_intent_id AND event_kind = 'released') THEN
            RAISE EXCEPTION 'executable drain fence or claim high-water changed'
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
            'worker_id', p_payload->>'worker_id',
            'worker_incarnation', p_payload->>'worker_incarnation',
            'claim_high_water', v_claim_high_water,
            'live_claim_count', v_live_claims,
            'drain_epoch', (p_payload->>'drain_epoch')::bigint,
            'request_digest', p_request_digest, 'drain_digest', p_request_digest,
            'protected_high_water', v_high_water,
            'worker_state', 'draining', 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_admission_events
            (operation_id, event_kind, agent_incarnation, subject_id,
             subject_incarnation, intent_id, worker_id, worker_incarnation,
             claim_high_water, drain_epoch, binding, request_payload,
             request_digest, receipt)
          VALUES
            (v_operation_id, 'draining', v_agent_incarnation, p_subject_id,
             p_subject_incarnation, v_intent_id,
             (p_payload->>'worker_id')::uuid,
             (p_payload->>'worker_incarnation')::uuid,
             v_claim_high_water, (p_payload->>'drain_epoch')::bigint,
             p_payload->'binding', p_payload, p_request_digest, v_receipt);
          RETURN v_receipt;
        END
        $function$
        """
    )


def _install_release() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.acknowledge_executable_release(
          p_subject_id uuid,
          p_subject_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_request_digest text,
          p_current_worker_credential text
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
          v_current {SCHEMA}.executable_admission_events%ROWTYPE;
          v_drain {SCHEMA}.executable_admission_events%ROWTYPE;
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
          v_claim_high_water bigint;
          v_live_claims bigint;
          v_bootstrap_revoked boolean;
          v_worker_credentials_revoked boolean;
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          v_agent_incarnation := {SCHEMA}.assert_executable_admission_binding(
            p_subject_id, p_subject_incarnation, 'release', p_payload,
            p_canonical_payload, p_request_digest
          );
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1 FOR UPDATE;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE operation_id = v_operation_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.event_kind <> 'released'
               OR v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
              RAISE EXCEPTION 'conflicting executable release replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'released' FOR KEY SHARE;
          IF FOUND THEN
            RAISE EXCEPTION 'protected release fence conflicts with a prior release'
              USING ERRCODE = '55000';
          END IF;
          SELECT * INTO v_prepared FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'prepared' FOR KEY SHARE;
          SELECT * INTO v_current FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'worker-registered'
           ORDER BY protected_registration_epoch DESC, event_id DESC LIMIT 1
           FOR KEY SHARE;
          SELECT * INTO v_drain FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id AND event_kind = 'draining'
           ORDER BY drain_epoch DESC, event_id DESC LIMIT 1 FOR KEY SHARE;
          SELECT * INTO v_state FROM {SCHEMA}.executable_claim_state
           WHERE intent_id = v_intent_id FOR UPDATE;
          SELECT count(*), count(*) FILTER (WHERE lease_state = 'live')
            INTO v_claim_high_water, v_live_claims
            FROM {SCHEMA}.executable_claim_leases
           WHERE intent_id = v_intent_id;
          v_bootstrap_revoked := v_current.bootstrap_revoked IS TRUE;
          v_worker_credentials_revoked :=
            v_current.operation_id IS NOT NULL
            AND p_current_worker_credential IS NOT NULL
            AND octet_length(p_current_worker_credential) BETWEEN 1 AND 4096
            AND encode(
                  sha256(convert_to(p_current_worker_credential, 'UTF8')),
                  'hex'
                ) IS NOT DISTINCT FROM v_current.worker_credential_sha256;
          IF v_prepared.operation_id IS NULL
             OR v_state.intent_id IS NULL
             OR v_state.draining IS DISTINCT FROM true
             OR v_state.claim_high_water IS DISTINCT FROM v_claim_high_water
             OR v_prepared.binding IS DISTINCT FROM p_payload->'binding'
             OR v_prepared.bootstrap_registration_epoch IS DISTINCT FROM
                (p_payload->>'bootstrap_registration_epoch')::bigint
             OR v_current.operation_id IS NULL
             OR v_current.binding IS DISTINCT FROM p_payload->'binding'
             OR v_drain.operation_id IS NULL
             OR v_drain.worker_id IS DISTINCT FROM v_current.worker_id
             OR v_drain.worker_incarnation IS DISTINCT FROM v_current.worker_incarnation
             OR v_claim_high_water IS DISTINCT FROM
                (p_payload->>'expected_claim_high_water')::bigint
             OR v_live_claims <> 0
             OR (p_payload->>'protected_registration_epoch')::bigint IS DISTINCT FROM
                v_current.protected_registration_epoch
             OR v_current.protected_registration_epoch <=
                v_prepared.bootstrap_registration_epoch
             OR v_bootstrap_revoked IS DISTINCT FROM true
             OR v_worker_credentials_revoked IS DISTINCT FROM true
             OR (p_payload->>'reporter_incarnation')::uuid IS DISTINCT FROM (
                  SELECT reporter_incarnation FROM {SCHEMA}.agent_registrations
                   WHERE agent_incarnation = v_agent_incarnation
                ) THEN
            IF v_current.operation_id IS NULL
               OR (p_payload->>'protected_registration_epoch')::bigint IS DISTINCT FROM
                  v_current.protected_registration_epoch
               OR v_current.protected_registration_epoch <=
                  v_prepared.bootstrap_registration_epoch THEN
              RAISE EXCEPTION 'release requires a newer protected registration epoch with corresponding registration evidence'
                USING ERRCODE = '55000';
            END IF;
            RAISE EXCEPTION 'release requires zero live protected claims and revoked credentials'
              USING ERRCODE = '55000';
          END IF;
          SELECT count(*) + 1 INTO v_high_water
            FROM {SCHEMA}.executable_admission_events;
          v_receipt := jsonb_build_object(
            'schema_version', 2, 'binding', p_payload->'binding',
            'reporter_incarnation', p_payload->>'reporter_incarnation',
            'bootstrap_registration_epoch',
              (p_payload->>'bootstrap_registration_epoch')::bigint,
            'protected_registration_epoch',
              (p_payload->>'protected_registration_epoch')::bigint,
            'claim_high_water', v_claim_high_water, 'live_claim_count', 0,
            'release_epoch', (p_payload->>'release_epoch')::bigint,
            'bootstrap_revoked', v_bootstrap_revoked,
            'worker_credentials_revoked', v_worker_credentials_revoked,
            'request_digest', p_request_digest,
            'protected_release_sha256', p_request_digest,
            'protected_high_water', v_high_water,
            'release_state', 'acknowledged', 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_admission_events
            (operation_id, event_kind, agent_incarnation, subject_id,
             subject_incarnation, intent_id, bootstrap_registration_epoch,
             protected_registration_epoch, worker_id, worker_incarnation,
             worker_credential_sha256, claim_high_water, release_epoch,
             bootstrap_revoked, worker_credential_revoked,
             binding, request_payload, request_digest, receipt)
          VALUES
            (v_operation_id, 'released', v_agent_incarnation, p_subject_id,
             p_subject_incarnation, v_intent_id,
             (p_payload->>'bootstrap_registration_epoch')::bigint,
             (p_payload->>'protected_registration_epoch')::bigint,
             v_current.worker_id, v_current.worker_incarnation,
             v_current.worker_credential_sha256,
             v_claim_high_water, (p_payload->>'release_epoch')::bigint,
             v_bootstrap_revoked, v_worker_credentials_revoked,
             p_payload->'binding', p_payload, p_request_digest, v_receipt);
          RETURN v_receipt;
        END
        $function$
        """
    )


def _install_claim_admission() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.admit_executable_claim(
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
          v_executor_role text;
          v_operation_id uuid;
          v_current {SCHEMA}.executable_admission_events%ROWTYPE;
          v_state {SCHEMA}.executable_claim_state%ROWTYPE;
          v_existing {SCHEMA}.executable_claim_leases%ROWTYPE;
          v_receipt jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'executable claim admission requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT executor_role_name INTO v_executor_role
            FROM {SCHEMA}.executable_admission_authority WHERE singleton_id = 1;
          IF v_executor_role IS NULL OR session_user::text <> v_executor_role THEN
            RAISE EXCEPTION 'executable claim caller is not the bound executor role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'executable claim executor unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR octet_length(p_payload::text) > 8388608
             OR octet_length(p_canonical_payload) > 8388608
             OR convert_from(p_canonical_payload, 'UTF8')::jsonb IS DISTINCT FROM p_payload
             OR p_request_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_canonical_payload), 'hex') IS DISTINCT FROM p_request_digest
             OR NOT p_payload ?& ARRAY[
               'executable', 'execution_generation', 'expected_claim_high_water',
               'operation_id', 'protected_attempt_id', 'requirements_digest',
               'schema_version', 'worker_id', 'worker_incarnation'
             ]
             OR p_payload - ARRAY[
               'executable', 'execution_generation', 'expected_claim_high_water',
               'operation_id', 'protected_attempt_id', 'requirements_digest',
               'schema_version', 'worker_id', 'worker_incarnation'
             ] <> '{{}}'::jsonb
             OR jsonb_typeof(p_payload->'schema_version') IS DISTINCT FROM 'number'
             OR (p_payload->>'schema_version')::integer IS DISTINCT FROM 2
             OR jsonb_typeof(p_payload->'executable') IS DISTINCT FROM 'boolean'
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM true
             OR jsonb_typeof(p_payload->'operation_id') IS DISTINCT FROM 'string'
             OR (p_payload->>'operation_id' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(p_payload->'protected_attempt_id') IS DISTINCT FROM 'string'
             OR (p_payload->>'protected_attempt_id' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(p_payload->'worker_id') IS DISTINCT FROM 'string'
             OR (p_payload->>'worker_id' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(p_payload->'worker_incarnation') IS DISTINCT FROM 'string'
             OR (p_payload->>'worker_incarnation' ~
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                IS DISTINCT FROM true
             OR jsonb_typeof(p_payload->'execution_generation') IS DISTINCT FROM 'number'
             OR (p_payload->>'execution_generation' ~ '^[1-9][0-9]*$')
                IS DISTINCT FROM true
             OR (p_payload->>'execution_generation')::numeric > 9223372036854775807
             OR jsonb_typeof(p_payload->'expected_claim_high_water')
                IS DISTINCT FROM 'number'
             OR (p_payload->>'expected_claim_high_water' ~ '^[0-9]+$')
                IS DISTINCT FROM true
             OR (p_payload->>'expected_claim_high_water')::numeric >
                9223372036854775807
             OR jsonb_typeof(p_payload->'requirements_digest') IS DISTINCT FROM 'string'
             OR (p_payload->>'requirements_digest' ~ '^[0-9a-f]{{64}}$')
                IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'executable claim request schema is invalid'
              USING ERRCODE = '22023';
          END IF;
          v_operation_id := (p_payload->>'operation_id')::uuid;
          SELECT * INTO v_existing FROM {SCHEMA}.executable_claim_leases
           WHERE operation_id = v_operation_id FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.request_payload IS DISTINCT FROM p_payload
               OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
              RAISE EXCEPTION 'conflicting executable claim replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;
          SELECT * INTO v_current FROM {SCHEMA}.executable_admission_events
           WHERE subject_id = p_subject_id
             AND subject_incarnation = p_subject_incarnation
             AND event_kind = 'worker-registered'
             AND worker_id = (p_payload->>'worker_id')::uuid
             AND worker_incarnation = (p_payload->>'worker_incarnation')::uuid
           LIMIT 1
           FOR KEY SHARE;
          IF NOT FOUND OR EXISTS (
               SELECT 1 FROM {SCHEMA}.executable_admission_events
                WHERE intent_id = v_current.intent_id
                  AND event_kind = 'worker-registered'
                  AND protected_registration_epoch >
                      v_current.protected_registration_epoch
             ) OR EXISTS (
               SELECT 1 FROM {SCHEMA}.executable_admission_events
                WHERE intent_id = v_current.intent_id
                  AND event_kind IN ('draining', 'released')
             ) THEN
            RETURN NULL;
          END IF;
          SELECT * INTO v_state FROM {SCHEMA}.executable_claim_state
           WHERE intent_id = v_current.intent_id FOR UPDATE;
          IF NOT FOUND
             OR v_state.subject_id IS DISTINCT FROM p_subject_id
             OR v_state.subject_incarnation IS DISTINCT FROM p_subject_incarnation THEN
            RAISE EXCEPTION 'executable claim state binding changed'
              USING ERRCODE = '55000';
          END IF;
          IF v_state.draining THEN
            RETURN NULL;
          END IF;
          IF v_state.claim_high_water IS DISTINCT FROM
             (p_payload->>'expected_claim_high_water')::bigint THEN
            RAISE EXCEPTION 'executable claim high-water changed'
              USING ERRCODE = '40001';
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.trial_attempts AS attempt
            JOIN public.trials AS trial ON trial.id = attempt.trial_id
             WHERE attempt.protected_attempt_id =
                   (p_payload->>'protected_attempt_id')::uuid
               AND attempt.execution_generation =
                   (p_payload->>'execution_generation')::bigint
               AND attempt.requirements_digest = p_payload->>'requirements_digest'
               AND attempt.claim_state = 'queued'
               AND trial.state = 'queued'
               AND trial.cancellation_requested_at IS NULL
               AND (trial.next_attempt_at IS NULL
                    OR trial.next_attempt_at <= statement_timestamp())
               AND trial.worker_id IS NULL
          ) OR EXISTS (
            SELECT 1 FROM {SCHEMA}.executable_claim_leases
             WHERE protected_attempt_id = (p_payload->>'protected_attempt_id')::uuid
          ) THEN
            RAISE EXCEPTION 'executable claim differs from its protected attempt binding'
              USING ERRCODE = '55000';
          END IF;
          UPDATE {SCHEMA}.executable_claim_state
             SET claim_high_water = claim_high_water + 1
           WHERE intent_id = v_current.intent_id;
          v_receipt := jsonb_build_object(
            'schema_version', 2,
            'operation_id', v_operation_id,
            'protected_attempt_id', p_payload->>'protected_attempt_id',
            'execution_generation', (p_payload->>'execution_generation')::bigint,
            'requirements_digest', p_payload->>'requirements_digest',
            'intent_id', v_current.intent_id,
            'worker_id', v_current.worker_id,
            'worker_incarnation', v_current.worker_incarnation,
            'claim_high_water', v_state.claim_high_water + 1,
            'request_digest', p_request_digest,
            'lease_state', 'live', 'admitted', true, 'executable', true
          );
          INSERT INTO {SCHEMA}.executable_claim_leases
            (operation_id, intent_id, subject_id, subject_incarnation,
             protected_attempt_id, execution_generation, requirements_digest,
             worker_id, worker_incarnation, claim_high_water, lease_state,
             request_payload, request_digest, receipt, executable)
          VALUES
            (v_operation_id, v_current.intent_id, p_subject_id, p_subject_incarnation,
             (p_payload->>'protected_attempt_id')::uuid,
             (p_payload->>'execution_generation')::bigint,
             p_payload->>'requirements_digest', v_current.worker_id,
             v_current.worker_incarnation, v_state.claim_high_water + 1, 'live',
             p_payload, p_request_digest, v_receipt, true);
          RETURN v_receipt;
        END
        $function$
        """
    )


def upgrade() -> None:
    executor_role, quoted_executor = _executor_role()
    op.create_table(
        "executable_admission_authority",
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("executor_role_name", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="guard_exec_authority_singleton_check"),
        sa.CheckConstraint(
            "executor_role_name ~ '^[a-z][a-z0-9_]{0,62}$'",
            name="guard_exec_authority_role_check",
        ),
        sa.PrimaryKeyConstraint("singleton_id"),
        schema=SCHEMA,
    )
    op.get_bind().execute(
        sa.text(
            f"INSERT INTO {SCHEMA}.executable_admission_authority "
            "(singleton_id, executor_role_name) VALUES (1, :executor_role)"
        ),
        {"executor_role": executor_role},
    )
    op.create_table(
        "executable_claim_state",
        sa.Column("intent_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("binding", postgresql.JSONB(), nullable=False),
        sa.Column("claim_high_water", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("draining", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.CheckConstraint("claim_high_water >= 0", name="guard_exec_claim_state_high_water_check"),
        sa.CheckConstraint(
            "jsonb_typeof(binding) = 'object'",
            name="guard_exec_claim_state_binding_check",
        ),
        sa.PrimaryKeyConstraint("intent_id"),
        schema=SCHEMA,
    )
    op.create_table(
        "executable_admission_events",
        sa.Column("event_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("event_kind", sa.Text(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("intent_id", sa.Uuid(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=True),
        sa.Column("protected_registration_epoch", sa.BigInteger(), nullable=True),
        sa.Column("physical_job_id", sa.Text(), nullable=True),
        sa.Column("ownership_evidence_sha256", sa.Text(), nullable=True),
        sa.Column("worker_id", sa.Uuid(), nullable=True),
        sa.Column("worker_incarnation", sa.Uuid(), nullable=True),
        sa.Column("predecessor_worker_incarnation", sa.Uuid(), nullable=True),
        sa.Column("worker_credential_sha256", sa.Text(), nullable=True),
        sa.Column("bootstrap_revoked", sa.Boolean(), nullable=True),
        sa.Column("predecessor_credential_revoked", sa.Boolean(), nullable=True),
        sa.Column("worker_credential_revoked", sa.Boolean(), nullable=True),
        sa.Column("claim_high_water", sa.BigInteger(), nullable=True),
        sa.Column("drain_epoch", sa.BigInteger(), nullable=True),
        sa.Column("release_epoch", sa.BigInteger(), nullable=True),
        sa.Column("bootstrap_sha256", sa.Text(), nullable=True),
        sa.Column("binding", postgresql.JSONB(), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(), nullable=False),
        sa.Column("request_digest", sa.Text(), nullable=False),
        sa.Column("receipt", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "event_kind IN ('prepared', 'physical-bound', 'worker-registered', "
            "'draining', 'released')",
            name="guard_exec_event_kind_check",
        ),
        sa.CheckConstraint(
            "request_digest ~ '^[0-9a-f]{64}$' "
            "AND (bootstrap_sha256 IS NULL OR bootstrap_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (ownership_evidence_sha256 IS NULL OR "
            "ownership_evidence_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (worker_credential_sha256 IS NULL OR "
            "worker_credential_sha256 ~ '^[0-9a-f]{64}$')",
            name="guard_exec_event_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(binding) = 'object' "
            "AND jsonb_typeof(request_payload) = 'object' "
            "AND jsonb_typeof(receipt) = 'object' "
            "AND octet_length(request_payload::text) <= 8388608 "
            "AND octet_length(receipt::text) <= 8388608",
            name="guard_exec_event_payload_check",
        ),
        sa.CheckConstraint(
            "claim_high_water IS NULL OR claim_high_water >= 0",
            name="guard_exec_claim_high_water_check",
        ),
        sa.CheckConstraint(
            "((event_kind = 'prepared' "
            " AND bootstrap_registration_epoch > 0 AND bootstrap_sha256 IS NOT NULL "
            " AND protected_registration_epoch IS NULL AND physical_job_id IS NULL "
            " AND worker_id IS NULL AND worker_incarnation IS NULL "
            " AND claim_high_water IS NULL AND drain_epoch IS NULL AND release_epoch IS NULL "
            " AND bootstrap_revoked IS NULL AND predecessor_credential_revoked IS NULL "
            " AND worker_credential_revoked IS NULL) OR "
            "(event_kind = 'physical-bound' "
            " AND bootstrap_registration_epoch > 0 AND physical_job_id IS NOT NULL "
            " AND ownership_evidence_sha256 IS NOT NULL "
            " AND protected_registration_epoch IS NULL AND worker_id IS NULL "
            " AND worker_incarnation IS NULL AND claim_high_water IS NULL "
            " AND drain_epoch IS NULL AND release_epoch IS NULL "
            " AND bootstrap_sha256 IS NULL AND bootstrap_revoked IS NULL "
            " AND predecessor_credential_revoked IS NULL "
            " AND worker_credential_revoked IS NULL) OR "
            "(event_kind = 'worker-registered' "
            " AND bootstrap_registration_epoch > 0 AND protected_registration_epoch > 0 "
            " AND physical_job_id IS NOT NULL AND worker_id IS NOT NULL "
            " AND worker_incarnation IS NOT NULL AND worker_credential_sha256 IS NOT NULL "
            " AND bootstrap_revoked = true AND predecessor_credential_revoked IS NOT NULL "
            " AND worker_credential_revoked = false AND claim_high_water IS NULL "
            " AND drain_epoch IS NULL AND release_epoch IS NULL "
            " AND bootstrap_sha256 IS NULL) OR "
            "(event_kind = 'draining' "
            " AND worker_id IS NOT NULL AND worker_incarnation IS NOT NULL "
            " AND claim_high_water >= 0 AND drain_epoch > 0 "
            " AND bootstrap_registration_epoch IS NULL "
            " AND protected_registration_epoch IS NULL AND physical_job_id IS NULL "
            " AND worker_credential_sha256 IS NULL AND release_epoch IS NULL "
            " AND bootstrap_sha256 IS NULL AND bootstrap_revoked IS NULL "
            " AND predecessor_credential_revoked IS NULL "
            " AND worker_credential_revoked IS NULL) OR "
            "(event_kind = 'released' "
            " AND bootstrap_registration_epoch > 0 AND protected_registration_epoch > 0 "
            " AND worker_id IS NOT NULL AND worker_incarnation IS NOT NULL "
            " AND worker_credential_sha256 IS NOT NULL AND claim_high_water >= 0 "
            " AND release_epoch > 0 AND bootstrap_revoked = true "
            " AND worker_credential_revoked = true "
            " AND predecessor_credential_revoked IS NULL AND physical_job_id IS NULL "
            " AND drain_epoch IS NULL AND bootstrap_sha256 IS NULL)) IS TRUE",
            name="guard_exec_event_evidence_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint("operation_id", name="guard_exec_operation_key"),
        schema=SCHEMA,
    )
    op.create_index(
        "guard_exec_prepared_intent_key",
        "executable_admission_events",
        ["intent_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("event_kind = 'prepared'"),
    )
    op.create_index(
        "guard_exec_physical_intent_key",
        "executable_admission_events",
        ["intent_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("event_kind = 'physical-bound'"),
    )
    op.execute(
        f"CREATE UNIQUE INDEX guard_exec_physical_job_key "
        f"ON {SCHEMA}.executable_admission_events "
        "((binding->>'pool_id'), physical_job_id) "
        "WHERE event_kind = 'physical-bound'"
    )
    op.create_index(
        "guard_exec_worker_incarnation_key",
        "executable_admission_events",
        ["worker_incarnation"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("event_kind = 'worker-registered'"),
    )
    op.create_index(
        "guard_exec_drain_epoch_key",
        "executable_admission_events",
        ["intent_id", "drain_epoch"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("event_kind = 'draining'"),
    )
    op.create_index(
        "guard_exec_release_intent_key",
        "executable_admission_events",
        ["intent_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("event_kind = 'released'"),
    )
    op.create_table(
        "executable_claim_leases",
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("intent_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("execution_generation", sa.BigInteger(), nullable=False),
        sa.Column("requirements_digest", sa.Text(), nullable=False),
        sa.Column("worker_id", sa.Uuid(), nullable=False),
        sa.Column("worker_incarnation", sa.Uuid(), nullable=False),
        sa.Column("claim_high_water", sa.BigInteger(), nullable=False),
        sa.Column("lease_state", sa.Text(), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(), nullable=False),
        sa.Column("request_digest", sa.Text(), nullable=False),
        sa.Column("receipt", postgresql.JSONB(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "execution_generation > 0 AND claim_high_water > 0",
            name="guard_exec_claim_generation_check",
        ),
        sa.CheckConstraint(
            "requirements_digest ~ '^[0-9a-f]{64}$' AND request_digest ~ '^[0-9a-f]{64}$'",
            name="guard_exec_claim_digest_check",
        ),
        sa.CheckConstraint(
            "lease_state = 'live' AND executable = true",
            name="guard_exec_claim_live_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_payload) = 'object' AND jsonb_typeof(receipt) = 'object'",
            name="guard_exec_claim_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["intent_id"],
            [f"{SCHEMA}.executable_claim_state.intent_id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id", "execution_generation", "requirements_digest"],
            [
                f"{SCHEMA}.trial_attempts.protected_attempt_id",
                f"{SCHEMA}.trial_attempts.execution_generation",
                f"{SCHEMA}.trial_attempts.requirements_digest",
            ],
            ondelete="RESTRICT",
            name="guard_exec_claim_attempt_binding_fk",
        ),
        sa.PrimaryKeyConstraint("operation_id"),
        sa.UniqueConstraint(
            "intent_id", "claim_high_water", name="guard_exec_claim_high_water_key"
        ),
        sa.UniqueConstraint("protected_attempt_id", name="guard_exec_claim_attempt_key"),
        schema=SCHEMA,
    )
    _append_only("executable_admission_authority")
    _append_only("executable_admission_events")
    _append_only("executable_claim_leases")
    _install_claim_state_guard()
    _install_binding_guard()
    _install_prepare()
    _install_physical_binding()
    _install_worker_registration()
    _install_drain()
    _install_release()
    _install_claim_admission()
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SCHEMA {SCHEMA} FROM {quoted_executor}")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM {quoted_executor}")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM {quoted_executor}")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM {quoted_executor}")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {quoted_executor}")
    for function in EXTERNAL_FUNCTIONS:
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{function} TO {quoted_executor}")


def downgrade() -> None:
    _executor_role_name, quoted_executor = _executor_role()
    op.execute(
        f"""
        DO $block$
        BEGIN
          IF EXISTS (SELECT 1 FROM {SCHEMA}.executable_admission_events) THEN
            RAISE EXCEPTION 'cannot downgrade guard_0011 with executable admission evidence';
          END IF;
        END
        $block$
        """
    )
    for function in reversed(EXTERNAL_FUNCTIONS):
        op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{function} FROM {quoted_executor}")
        op.execute(f"DROP FUNCTION {SCHEMA}.{function}")
    op.execute(
        f"DROP FUNCTION {SCHEMA}.assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_executable_claim_state() CASCADE")
    op.drop_table("executable_claim_leases", schema=SCHEMA)
    op.drop_table("executable_admission_events", schema=SCHEMA)
    op.drop_table("executable_claim_state", schema=SCHEMA)
    op.drop_table("executable_admission_authority", schema=SCHEMA)
