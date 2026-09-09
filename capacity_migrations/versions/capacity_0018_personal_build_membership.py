"""Add the exact SQL identity prerequisite for typed build membership.

Revision ID: capacity_0018
Revises: capacity_0017

Typed build lifecycle events may be recorded with pending generation evidence.
Recreation and V4 executable admission remain interlocked.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0018"
down_revision: str | Sequence[str] | None = "capacity_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EXTENSION_SCHEMA = "capacity_build_extensions"


def _install_initial_build_guard() -> None:
    op.execute("""
    CREATE FUNCTION public.capacity_personal_build_json_exact(p_left jsonb,p_right jsonb)
    RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
      SELECT public.capacity_executable_canonical_jsonb_text(p_left)
        IS NOT DISTINCT FROM public.capacity_executable_canonical_jsonb_text(p_right)
    $$;
    REVOKE ALL ON FUNCTION public.capacity_personal_build_json_exact(jsonb,jsonb) FROM PUBLIC;
    CREATE FUNCTION public.capacity_personal_application_installation_matches(p_config jsonb,p_projection jsonb)
    RETURNS boolean LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
    DECLARE
      retained jsonb;
      profile jsonb;
    BEGIN
      SELECT to_jsonb(c) - ARRAY['id','subject_id','subject_incarnation','candidate_generation'] INTO retained
        FROM public.capacity_candidates c WHERE c.subject_id=(p_config ->> 'subject_id')::uuid
          AND c.subject_incarnation=(p_config ->> 'subject_incarnation')::uuid
          AND c.candidate_generation=(p_config ->> 'candidate_generation')::bigint;
      IF NOT public.capacity_personal_build_json_exact(retained,jsonb_build_object(
          'candidate_digest',p_projection -> 'candidate_sha256','candidate_identity_algorithm','source-sha256',
          'candidate_identity',p_projection -> 'candidate_sha256',
          'source_payload',jsonb_build_object('publication_sha256',p_projection -> 'candidate_publication_sha256'),
          'artifact_payload',jsonb_build_object('candidate_sha256',p_projection -> 'candidate_sha256'),
          'architecture_payload',jsonb_build_object('supported_architectures',p_projection -> 'supported_architectures','supported_pool_ids',p_projection -> 'supported_pool_ids'),
          'launcher_payload',jsonb_build_object('local_activation_sha256',p_projection -> 'local_activation_sha256'),
          'attestation_payload',jsonb_build_object('operation_id',p_projection -> 'operation_id','operation_epoch',p_projection -> 'operation_epoch',
            'protected_admission_sha256',p_projection -> 'protected_admission_sha256','capacity_agent_installation_sha256',p_projection -> 'capacity_agent_installation_sha256'),
          'protocol_payload',p_projection -> 'protocol_versions')) THEN RETURN false; END IF;
      SELECT to_jsonb(d) - ARRAY['id','subject_id','subject_incarnation','deployment_generation'] INTO retained
        FROM public.capacity_deployment_generations d WHERE d.subject_id=(p_config ->> 'subject_id')::uuid
          AND d.subject_incarnation=(p_config ->> 'subject_incarnation')::uuid
          AND d.deployment_generation=(p_config ->> 'deployment_generation')::bigint;
      IF NOT public.capacity_personal_build_json_exact(retained,jsonb_build_object(
          'candidate_digest',p_projection -> 'candidate_sha256','required_profiles',p_config -> 'profiles',
          'readiness_state','ready','lifecycle_state','active','cutover_payload',jsonb_build_object(
            'local_activation_sha256',p_projection -> 'local_activation_sha256','candidate_publication_sha256',p_projection -> 'candidate_publication_sha256',
            'protected_admission_sha256',p_projection -> 'protected_admission_sha256','capacity_agent_installation_sha256',p_projection -> 'capacity_agent_installation_sha256'))) THEN RETURN false; END IF;
      IF (SELECT count(*) FROM public.capacity_worker_profiles p WHERE p.subject_id=(p_config ->> 'subject_id')::uuid
          AND p.subject_incarnation=(p_config ->> 'subject_incarnation')::uuid
          AND p.deployment_generation=(p_config ->> 'deployment_generation')::bigint) <> jsonb_array_length(p_config -> 'profiles') THEN RETURN false; END IF;
      FOR profile IN SELECT value FROM jsonb_array_elements(p_config -> 'profiles') LOOP
        SELECT to_jsonb(p) - ARRAY['id','subject_id','subject_incarnation','deployment_generation'] INTO retained
          FROM public.capacity_worker_profiles p WHERE p.subject_id=(p_config ->> 'subject_id')::uuid
            AND p.subject_incarnation=(p_config ->> 'subject_incarnation')::uuid
            AND p.deployment_generation=(p_config ->> 'deployment_generation')::bigint AND p.pool_id=profile ->> 'pool_id';
        IF NOT public.capacity_personal_build_json_exact(retained,jsonb_build_object('pool_id',profile -> 'pool_id','pool_generation',profile -> 'pool_generation',
            'profile_generation',profile -> 'profile_generation','profile_digest',profile -> 'profile_digest',
            'shape_catalog',profile -> 'worker_shapes','narrowing_constraints',jsonb_build_object('eligible_resource_domains',profile -> 'eligible_resource_domains'))) THEN RETURN false; END IF;
      END LOOP;
      RETURN true;
    END $$;
    REVOKE ALL ON FUNCTION public.capacity_personal_application_installation_matches(jsonb,jsonb) FROM PUBLIC;
    CREATE FUNCTION public.capacity_personal_build_initial_insert_guard()
    RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
    DECLARE
      authority_record record;
      epoch_record record;
      previous_record record;
      prior_subject record;
      old_member jsonb;
      old_config jsonb;
      old_ack jsonb;
      old_projection jsonb;
      base_origin jsonb;
      base_config jsonb;
      base_projection jsonb;
      base_installation jsonb;
      base_reference jsonb;
      base_epoch record;
      mutation_kind text;
      member_purpose text;
      expected_projection jsonb;
      origin_projection jsonb;
      expected_candidate jsonb;
      expected_deployment jsonb;
      service_candidate_generation bigint;
      preparation jsonb;
      fleet jsonb;
      policy jsonb;
      template jsonb;
      owner_policy jsonb;
      projection jsonb;
      member jsonb;
      config jsonb;
      ack jsonb;
      expected_config jsonb;
      expected_execution jsonb;
      candidate jsonb;
      profile jsonb;
      retained jsonb;
      runtime_digest text;
      template_digest text;
      derived_account_id text;
      owner_hex text;
      calculated_head text;
    BEGIN
      -- JSONB equality considers 2.0 equal to 2. Every numeric field in these
      -- typed build contracts is a strict nonnegative integer, including nested
      -- execution, profile/resource and acknowledgement quantities.
      IF EXISTS (
        WITH RECURSIVE documents(value) AS (
          VALUES (NEW.request_payload),(NEW.result_payload)
          UNION ALL
          SELECT child.value FROM documents d CROSS JOIN LATERAL (
            SELECT value FROM jsonb_each(CASE WHEN jsonb_typeof(d.value)='object' THEN d.value ELSE '{}'::jsonb END)
            UNION ALL
            SELECT value FROM jsonb_array_elements(CASE WHEN jsonb_typeof(d.value)='array' THEN d.value ELSE '[]'::jsonb END)
          ) child
        )
        SELECT 1 FROM documents WHERE
          (jsonb_typeof(value)='number' AND value::text !~ '^(0|[1-9][0-9]*)$')
          OR (jsonb_typeof(value)='object' AND value ? 'schema_version'
            AND (value -> 'schema_version')::text !~ '^[1-9][0-9]*$')
      ) THEN
        RAISE EXCEPTION 'typed membership number is not an integer' USING ERRCODE = '23514';
      END IF;
      -- The existing application trigger remains unchanged and handles V1 only.
      -- Fail closed on every unsupported version/purpose/lifecycle operation.
      mutation_kind := NEW.request_payload #>> '{command,projection,operation_kind}';
      member_purpose := NEW.request_payload #>> '{command,purpose}';
      IF NEW.request_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
         OR member_purpose IS NULL OR member_purpose NOT IN ('personal-build-worker','personal-application')
         OR mutation_kind IS NULL OR mutation_kind NOT IN ('create','update','capacity','destroy') THEN
        RAISE EXCEPTION 'typed membership lifecycle is not admitted' USING ERRCODE = '23514';
      END IF;
      SELECT a.* INTO authority_record FROM public.capacity_authority_state a
        WHERE a.singleton_id = 1 FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'typed membership authority is unavailable' USING ERRCODE = '23514';
      END IF;
      SELECT e.* INTO epoch_record FROM public.capacity_execution_epochs e
        WHERE e.execution_epoch = NEW.execution_epoch FOR SHARE;
      IF NOT FOUND
         OR authority_record.execution_state IS DISTINCT FROM 'active'
         OR epoch_record.state IS DISTINCT FROM 'active'
         OR authority_record.execution_epoch IS DISTINCT FROM NEW.execution_epoch
         OR authority_record.authority_incarnation IS DISTINCT FROM NEW.authority_incarnation
         OR epoch_record.authority_incarnation IS DISTINCT FROM NEW.authority_incarnation
         OR authority_record.writer_epoch IS DISTINCT FROM NEW.writer_epoch
         OR epoch_record.current_writer_epoch IS DISTINCT FROM NEW.writer_epoch
         OR authority_record.execution_manifest_sha256 IS DISTINCT FROM NEW.execution_manifest_sha256
         OR epoch_record.execution_manifest_sha256 IS DISTINCT FROM NEW.execution_manifest_sha256
         OR authority_record.executable_new_capacity_ceiling IS DISTINCT FROM epoch_record.effective_ceiling THEN
        RAISE EXCEPTION 'typed membership execution authority changed' USING ERRCODE = '23514';
      END IF;
      preparation := epoch_record.manifest_payload;
      policy := preparation -> 'personal_membership';
      template := preparation -> 'personal_builds';
      IF preparation -> 'schema_version' IS DISTINCT FROM '4'::jsonb
         OR policy -> 'namespace_id' IS DISTINCT FROM to_jsonb(NEW.namespace_id::text)
         OR policy ->> 'management_principal_id' IS DISTINCT FROM NEW.actor
         OR encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(preparation),'UTF8')),'hex')
              IS DISTINCT FROM NEW.execution_manifest_sha256 THEN
        RAISE EXCEPTION 'typed membership preparation changed' USING ERRCODE = '23514';
      END IF;
      SELECT g.payload INTO fleet FROM public.capacity_config_generations g
        WHERE g.scope = 'fleet' AND g.scope_generation = epoch_record.fleet_generation
          AND g.digest = epoch_record.fleet_digest FOR SHARE;
      IF NOT FOUND
         OR encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(fleet),'UTF8')),'hex')
              IS DISTINCT FROM epoch_record.fleet_digest
         OR fleet ->> 'fleet_digest' IS DISTINCT FROM
              encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(fleet - 'fleet_digest'),'UTF8')),'hex')
         OR policy ->> 'development_template_sha256' IS DISTINCT FROM
              encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(fleet -> 'development_subject_template'),'UTF8')),'hex')
         OR preparation -> 'fleet_generation' IS DISTINCT FROM to_jsonb(epoch_record.fleet_generation)
         OR preparation ->> 'fleet_digest' IS DISTINCT FROM epoch_record.fleet_digest
         OR preparation -> 'configuration_epoch' IS DISTINCT FROM to_jsonb(epoch_record.configuration_epoch)
         OR preparation ->> 'trusted_fleet_release_sha256' IS DISTINCT FROM epoch_record.trusted_fleet_release_sha256
         OR epoch_record.effective_ceiling > epoch_record.requested_ceiling
         OR epoch_record.effective_rate_per_minute > epoch_record.requested_rate_per_minute THEN
        RAISE EXCEPTION 'typed membership fleet authority changed' USING ERRCODE = '23514';
      END IF;
      expected_execution := jsonb_build_object(
        'schema_version',2,'executable',true,'execution_state','active',
        'execution_epoch',NEW.execution_epoch,'execution_manifest_sha256',NEW.execution_manifest_sha256,
        'writer_epoch',NEW.writer_epoch,'authority_incarnation',NEW.authority_incarnation::text,
        'configuration_epoch',epoch_record.configuration_epoch,
        'trusted_fleet_release_sha256',epoch_record.trusted_fleet_release_sha256,
        'executable_new_capacity_ceiling',epoch_record.effective_ceiling,
        'executable_new_capacity_rate_per_minute',epoch_record.effective_rate_per_minute);
      projection := NEW.request_payload #> '{command,projection}';
      IF jsonb_typeof(projection -> 'candidate_generation') IS DISTINCT FROM 'number'
         OR coalesce((projection ->> 'candidate_generation') ~ '^[1-9][0-9]*$',false) IS NOT TRUE
         OR (projection ->> 'candidate_generation')::numeric > 9223372036854775807 THEN
        RAISE EXCEPTION 'typed build candidate generation is invalid' USING ERRCODE = '23514';
      END IF;
      service_candidate_generation := (projection ->> 'candidate_generation')::bigint;
      IF member_purpose='personal-application' THEN
        template := fleet -> 'development_subject_template';
      END IF;
      member := NEW.result_payload -> 'member';
      config := member -> 'configuration';
      ack := NEW.request_payload #> '{command,acknowledgement}';
      owner_hex := replace(NEW.owner_id::text,'-','');
      derived_account_id := 'dev-owner-' || owner_hex;
      SELECT value INTO owner_policy FROM jsonb_array_elements(fleet -> 'account_policies')
        WHERE value ->> 'account_id' = fleet #>> '{development_subject_template,owner_account_template_id}'
          AND value ->> 'kind' = 'owner_template';
      IF NOT FOUND THEN
        RAISE EXCEPTION 'typed membership owner template is missing' USING ERRCODE = '23514';
      END IF;
      owner_policy := owner_policy || jsonb_build_object('account_id',derived_account_id,'kind','owner','owner_id',NEW.owner_id::text);
      expected_config := jsonb_build_object(
        'schema_version',1,'subject_id',NEW.subject_id::text,'subject_incarnation',NEW.subject_incarnation::text,
        'display_name','dev-build-' || owner_hex,'account_id',derived_account_id,'tier_id','development',
        'min_slots',0,'max_slots',CASE WHEN mutation_kind='destroy' THEN '0'::jsonb ELSE projection -> 'max_slots' END,'rollout_surge_slots',0,
        'max_pending_slots',template -> 'max_pending_slots_per_subject',
        'max_pending_jobs',template -> 'max_pending_jobs_per_subject',
        'submission_rate_per_minute',owner_policy -> 'submission_rate_per_minute',
        'lifecycle_state',CASE WHEN mutation_kind='destroy' THEN 'disabled' ELSE 'active' END,
        'candidate_generation',service_candidate_generation,'deployment_generation',NEW.deployment_generation,
        'configuration_generation',NEW.configuration_generation,
        'demand_reporter_incarnation',NEW.reporter_incarnation::text,'profiles',template -> 'profiles');
      expected_projection := jsonb_build_object(
        'schema_version',1,'owner_id',NEW.owner_id::text,'subject_incarnation',NEW.subject_incarnation::text,
        'operation_kind',mutation_kind,'operation_id',NEW.operation_id::text,'operation_epoch',NEW.configuration_generation,
        'configuration_generation',NEW.configuration_generation,'candidate_generation',service_candidate_generation,'deployment_generation',NEW.deployment_generation,
        'demand_reporter_incarnation',NEW.reporter_incarnation::text,
        'demand_reporter_token_sha256',projection -> 'demand_reporter_token_sha256','max_slots',projection -> 'max_slots');
      candidate := template -> 'runtime_candidate';
      IF member_purpose='personal-application' THEN
        expected_projection := expected_projection || jsonb_build_object(
          'expected_configuration_epoch',epoch_record.configuration_epoch,'subject_id',NEW.subject_id::text,
          'environment_name',projection -> 'environment_name','min_slots',projection -> 'min_slots',
          'candidate_sha256',projection -> 'candidate_sha256','candidate_publication_sha256',projection -> 'candidate_publication_sha256',
          'local_activation_sha256',projection -> 'local_activation_sha256','protected_admission_sha256',projection -> 'protected_admission_sha256',
          'capacity_agent_installation_sha256',projection -> 'capacity_agent_installation_sha256',
          'supported_architectures','["arm64","x86_64"]'::jsonb,'supported_pool_ids','["gb10","oldlab"]'::jsonb,
          'protocol_versions',projection -> 'protocol_versions');
        expected_config := expected_config || jsonb_build_object(
          'display_name','dev-' || (projection ->> 'environment_name'),
          'min_slots',CASE WHEN mutation_kind='destroy' THEN '0'::jsonb ELSE projection -> 'min_slots' END,
          'rollout_surge_slots',template -> 'rollout_surge_slots');
        candidate := jsonb_build_object('schema_version',2,'algorithm','source-sha256',
          'identity',projection -> 'candidate_sha256','publication_sha256',projection -> 'candidate_publication_sha256');
        IF service_candidate_generation<>NEW.deployment_generation
           OR jsonb_typeof(projection -> 'environment_name') IS DISTINCT FROM 'string'
           OR coalesce((projection ->> 'environment_name') ~ '^[a-z]([-a-z0-9]{0,18}[a-z0-9])?$',false) IS NOT TRUE
           OR projection ->> 'environment_name' IN ('dev','development','staging','production','prod','local','loom','shared')
           OR jsonb_typeof(projection -> 'min_slots') IS DISTINCT FROM 'number'
           OR coalesce((projection ->> 'min_slots') ~ '^(0|[1-9][0-9]*)$',false) IS NOT TRUE
           OR (projection ->> 'min_slots')::numeric > (projection ->> 'max_slots')::numeric
           OR EXISTS (SELECT 1 FROM unnest(ARRAY['candidate_sha256','candidate_publication_sha256','local_activation_sha256',
                'protected_admission_sha256','capacity_agent_installation_sha256']) field
                WHERE jsonb_typeof(projection -> field) IS DISTINCT FROM 'string'
                  OR coalesce((projection ->> field) ~ '^[0-9a-f]{64}$',false) IS NOT TRUE)
           OR projection ->> 'candidate_sha256'=repeat('0',64)
           OR projection ->> 'candidate_publication_sha256'=repeat('0',64)
           OR jsonb_typeof(projection -> 'protocol_versions') IS DISTINCT FROM 'object'
           OR projection #>> '{protocol_versions,capacity-agent}' IS DISTINCT FROM 'v1'
           OR projection #>> '{protocol_versions,claim-guard}' IS DISTINCT FROM 'v1'
           OR projection #>> '{protocol_versions,control-plane-worker}' IS DISTINCT FROM 'v1'
           OR EXISTS (SELECT 1 FROM jsonb_each(projection -> 'protocol_versions') p
                WHERE p.key !~ '^[a-z0-9][a-z0-9_.-]{0,127}$' OR jsonb_typeof(p.value) IS DISTINCT FROM 'string'
                  OR (p.value #>> '{}') !~ '^[a-z0-9][a-z0-9_.-]{0,127}$')
           OR ack -> 'protected_admission_sha256' IS DISTINCT FROM projection -> 'protected_admission_sha256' THEN
          RAISE EXCEPTION 'typed application source or installation projection changed' USING ERRCODE = '23514';
        END IF;
      END IF;
      IF (member_purpose='personal-build-worker' AND NEW.subject_id IS DISTINCT FROM public.capacity_personal_build_subject_id(NEW.namespace_id,NEW.owner_id))
         OR NEW.subject_id = '00000000-0000-0000-0000-000000000000'::uuid
         OR NEW.owner_id = '00000000-0000-0000-0000-000000000000'::uuid
         OR NEW.subject_incarnation = '00000000-0000-0000-0000-000000000000'::uuid
         OR NEW.reporter_incarnation = '00000000-0000-0000-0000-000000000000'::uuid
         OR NEW.operation_id = '00000000-0000-0000-0000-000000000000'::uuid
         OR NEW.idempotency_key = '00000000-0000-0000-0000-000000000000'::uuid
         OR NEW.request_payload IS DISTINCT FROM jsonb_build_object(
              'schema_version',2,'execution',expected_execution,'namespace_id',NEW.namespace_id::text,
              'expected_revision',NEW.revision-1,'command',jsonb_build_object(
                'schema_version',2,'purpose',member_purpose,'projection',projection,'acknowledgement',ack))
         OR projection IS DISTINCT FROM expected_projection
         OR coalesce((projection ->> 'demand_reporter_token_sha256') ~ '^[0-9a-f]{64}$',false) IS NOT TRUE
         OR jsonb_typeof(projection -> 'demand_reporter_token_sha256') IS DISTINCT FROM 'string'
         OR projection ->> 'demand_reporter_token_sha256' = repeat('0',64)
         OR coalesce((projection ->> 'max_slots') ~ '^(0|[1-9][0-9]*)$',false) IS NOT TRUE
         OR jsonb_typeof(projection -> 'max_slots') IS DISTINCT FROM 'number'
         OR (projection ->> 'max_slots')::numeric > (template ->> 'max_slots_per_subject')::numeric
         OR config IS DISTINCT FROM expected_config
         OR member IS DISTINCT FROM jsonb_build_object('schema_version',1,'purpose',member_purpose,
              'revision',NEW.revision,'owner_id',NEW.owner_id::text,'configuration',expected_config,
              'acknowledgement',ack,'reincarnation',NULL)
         OR ack IS DISTINCT FROM jsonb_build_object('schema_version',2,'subject_id',NEW.subject_id::text,
              'subject_incarnation',NEW.subject_incarnation::text,'configuration_generation',NEW.configuration_generation,
              'deployment_generation',NEW.deployment_generation,'candidate',candidate,'reporter_incarnation',NEW.reporter_incarnation::text,
              'protected_admission_sha256',ack -> 'protected_admission_sha256',
              'legacy_writer_high_water',ack -> 'legacy_writer_high_water','acknowledgement_sha256',ack -> 'acknowledgement_sha256')
         OR coalesce((ack ->> 'protected_admission_sha256') ~ '^[0-9a-f]{64}$',false) IS NOT TRUE
         OR coalesce((ack ->> 'acknowledgement_sha256') ~ '^[0-9a-f]{64}$',false) IS NOT TRUE
         OR jsonb_typeof(ack -> 'protected_admission_sha256') IS DISTINCT FROM 'string'
         OR jsonb_typeof(ack -> 'acknowledgement_sha256') IS DISTINCT FROM 'string'
         OR coalesce((ack ->> 'legacy_writer_high_water') ~ '^(0|[1-9][0-9]*)$',false) IS NOT TRUE
         OR jsonb_typeof(ack -> 'legacy_writer_high_water') IS DISTINCT FROM 'number'
         OR (ack ->> 'legacy_writer_high_water')::numeric > 9223372036854775807
         OR NEW.result_payload IS DISTINCT FROM jsonb_build_object('schema_version',2,'revision',NEW.revision,
              'head_sha256',NEW.head_sha256,'member',member,'replayed',false) THEN
        RAISE EXCEPTION 'typed build event projection is not exact' USING ERRCODE = '23514';
      END IF;
      SELECT e.* INTO previous_record FROM public.capacity_personal_membership_events e
        WHERE e.execution_epoch = NEW.execution_epoch ORDER BY e.revision DESC LIMIT 1 FOR SHARE;
      IF (NOT FOUND AND (NEW.revision <> 1 OR NEW.previous_sha256 <> repeat('0',64)))
         OR (FOUND AND (NEW.revision <> previous_record.revision+1 OR NEW.previous_sha256 <> previous_record.head_sha256)) THEN
        RAISE EXCEPTION 'typed membership revision is not consecutive' USING ERRCODE = '23514';
      END IF;
      calculated_head := encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(jsonb_build_object(
        'actor',NEW.actor,'execution_epoch',NEW.execution_epoch,'idempotency_key',NEW.idempotency_key::text,
        'operation_id',NEW.operation_id::text,'previous_sha256',NEW.previous_sha256,'request_digest',NEW.request_digest,
        'request_payload',NEW.request_payload,'result_member',member,'revision',NEW.revision)),'UTF8')),'hex');
      IF NEW.head_sha256 IS DISTINCT FROM calculated_head
         OR NEW.request_digest IS DISTINCT FROM encode(sha256(convert_to(
              public.capacity_executable_canonical_jsonb_text(NEW.request_payload),'UTF8')),'hex') THEN
        RAISE EXCEPTION 'typed membership event digest changed' USING ERRCODE = '23514';
      END IF;
      -- A predecessor is either this epoch's event or its explicitly pinned
      -- managed base, never the highest historical/prepared epoch.
      SELECT value INTO base_origin FROM jsonb_array_elements(coalesce(preparation -> 'managed_application_origins','[]'::jsonb))
        WHERE value #>> '{configuration,subject_id}'=NEW.subject_id::text;
      IF FOUND THEN
        base_config := base_origin -> 'configuration';
        base_projection := base_origin -> 'base_projection';
        base_installation := base_origin -> 'installation_projection';
        IF member_purpose<>'personal-application' OR mutation_kind='create'
           OR (SELECT count(*) FROM jsonb_array_elements(preparation -> 'managed_application_origins') value
                WHERE value #>> '{configuration,subject_id}'=NEW.subject_id::text)<>1
           OR NOT coalesce(policy -> 'managed_base_subject_ids' @> jsonb_build_array(NEW.subject_id::text),false)
           OR base_config ->> 'subject_incarnation' IS DISTINCT FROM NEW.subject_incarnation::text
           OR base_config ->> 'account_id' IS DISTINCT FROM derived_account_id
           OR base_config ->> 'display_name' IS DISTINCT FROM expected_config ->> 'display_name'
           OR base_projection ->> 'owner_id' IS DISTINCT FROM NEW.owner_id::text
           OR base_projection ->> 'subject_id' IS DISTINCT FROM NEW.subject_id::text
           OR base_projection ->> 'subject_incarnation' IS DISTINCT FROM NEW.subject_incarnation::text
           OR base_projection ->> 'environment_name' IS DISTINCT FROM projection ->> 'environment_name'
           OR base_installation ->> 'operation_kind' NOT IN ('create','update')
           OR (base_installation ->> 'expected_configuration_epoch')::bigint > (base_projection ->> 'expected_configuration_epoch')::bigint
           OR (base_projection ->> 'expected_configuration_epoch')::bigint > epoch_record.configuration_epoch
           OR NOT public.capacity_personal_build_json_exact(
                base_projection - ARRAY['expected_configuration_epoch','operation_kind','operation_id','operation_epoch','configuration_generation','min_slots','max_slots'],
                base_installation - ARRAY['expected_configuration_epoch','operation_kind','operation_id','operation_epoch','configuration_generation','min_slots','max_slots'])
           OR (base_projection ->> 'operation_kind' IN ('create','update') AND NOT public.capacity_personal_build_json_exact(base_projection,base_installation))
           OR (base_projection ->> 'operation_kind' IN ('capacity','destroy') AND (
                (base_projection ->> 'configuration_generation')::bigint <= (base_installation ->> 'configuration_generation')::bigint
                OR base_projection -> 'operation_id'=base_installation -> 'operation_id'))
           OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(preparation -> 'subject_acknowledgements') value
                WHERE public.capacity_personal_build_json_exact(value,base_origin -> 'acknowledgement')) THEN
          RAISE EXCEPTION 'typed managed origin identity or installation changed' USING ERRCODE = '23514';
        END IF;
        SELECT c.* INTO base_epoch FROM public.capacity_configuration_epochs c
          WHERE c.configuration_epoch=epoch_record.configuration_epoch FOR SHARE;
        IF NOT FOUND OR base_epoch.fleet_generation<>epoch_record.fleet_generation OR base_epoch.fleet_digest<>epoch_record.fleet_digest
           OR base_epoch.canonical_digest IS DISTINCT FROM encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(jsonb_build_object(
                'schema_version',1,'configuration_epoch',epoch_record.configuration_epoch,
                'fleet',jsonb_build_object('schema_version',1,'scope','fleet','generation',epoch_record.fleet_generation,
                  'digest',epoch_record.fleet_digest,'subject_id',NULL,'subject_incarnation',NULL),
                'subjects',base_epoch.subject_generation_manifest)),'UTF8')),'hex') THEN
          RAISE EXCEPTION 'typed managed immutable configuration root changed' USING ERRCODE = '23514';
        END IF;
        SELECT value INTO base_reference FROM jsonb_array_elements(base_epoch.subject_generation_manifest)
          WHERE value ->> 'subject_id'=NEW.subject_id::text;
        IF NOT FOUND OR NOT public.capacity_personal_build_json_exact(base_reference,jsonb_build_object(
              'schema_version',1,'scope','subject','subject_id',NEW.subject_id::text,'subject_incarnation',NEW.subject_incarnation::text,
              'generation',base_config -> 'configuration_generation',
              'digest',encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(base_config),'UTF8')),'hex')))
           OR NOT EXISTS (SELECT 1 FROM public.capacity_config_generations g WHERE g.scope='subject' AND g.subject_id=NEW.subject_id
                AND g.subject_incarnation=NEW.subject_incarnation AND g.scope_generation=(base_config ->> 'configuration_generation')::bigint
                AND g.digest=base_reference ->> 'digest' AND public.capacity_personal_build_json_exact(g.payload,base_config))
           OR NOT public.capacity_personal_build_json_exact(base_config,expected_config || jsonb_build_object(
                'min_slots',CASE WHEN base_projection ->> 'operation_kind'='destroy' THEN '0'::jsonb ELSE base_projection -> 'min_slots' END,
                'max_slots',CASE WHEN base_projection ->> 'operation_kind'='destroy' THEN '0'::jsonb ELSE base_projection -> 'max_slots' END,
                'lifecycle_state',CASE WHEN base_projection ->> 'operation_kind'='destroy' THEN 'disabled' ELSE 'active' END,
                'configuration_generation',base_projection -> 'configuration_generation','candidate_generation',base_projection -> 'candidate_generation',
                'deployment_generation',base_projection -> 'deployment_generation','demand_reporter_incarnation',base_projection -> 'demand_reporter_incarnation'))
           OR NOT public.capacity_personal_application_installation_matches(base_config,base_installation) THEN
          RAISE EXCEPTION 'typed managed origin differs from immutable base or retained installation' USING ERRCODE = '23514';
        END IF;
      END IF;
      IF EXISTS (SELECT 1 FROM jsonb_array_elements(coalesce(preparation -> 'managed_application_origins','[]'::jsonb)) value WHERE
          NEW.operation_id::text IN (value #>> '{installation_projection,operation_id}',value #>> '{base_projection,operation_id}')
          OR (mutation_kind IN ('create','update') AND (
            NEW.reporter_incarnation::text=value #>> '{base_projection,demand_reporter_incarnation}'
            OR projection ->> 'demand_reporter_token_sha256'=value #>> '{base_projection,demand_reporter_token_sha256}'))) THEN
        RAISE EXCEPTION 'typed managed base operation or reporter identity was already used' USING ERRCODE = '23514';
      END IF;
      SELECT e.* INTO prior_subject FROM public.capacity_personal_membership_events e
        WHERE e.subject_id=NEW.subject_id AND e.execution_epoch=NEW.execution_epoch ORDER BY e.revision DESC LIMIT 1 FOR SHARE;
      IF FOUND THEN
        old_member := prior_subject.result_payload -> 'member';
        old_config := old_member -> 'configuration';
        old_ack := old_member -> 'acknowledgement';
        old_projection := prior_subject.request_payload #> '{command,projection}';
        IF prior_subject.owner_id<>NEW.owner_id OR prior_subject.request_payload #>> '{command,purpose}' IS DISTINCT FROM member_purpose THEN
          RAISE EXCEPTION 'typed membership predecessor purpose or owner changed' USING ERRCODE = '23514';
        END IF;
      ELSIF base_origin IS NOT NULL THEN
        old_config := base_config;
        old_projection := base_projection;
        old_ack := base_origin -> 'acknowledgement';
      END IF;
      IF mutation_kind='create' THEN
        IF old_config IS NOT NULL OR service_candidate_generation<>1 OR NEW.deployment_generation<>1
           OR EXISTS (SELECT 1 FROM public.capacity_personal_membership_events e WHERE e.subject_id=NEW.subject_id) THEN
          RAISE EXCEPTION 'typed build create cannot recreate retained membership' USING ERRCODE = '23514';
        END IF;
      ELSE
        IF old_config IS NULL OR old_config ->> 'subject_incarnation' IS DISTINCT FROM NEW.subject_incarnation::text
           OR (old_config ->> 'configuration_generation')::bigint >= NEW.configuration_generation
           OR old_config ->> 'display_name' IS DISTINCT FROM expected_config ->> 'display_name'
           OR old_config ->> 'lifecycle_state' IS DISTINCT FROM 'active' THEN
          RAISE EXCEPTION 'typed build lifecycle predecessor changed' USING ERRCODE = '23514';
        END IF;
        IF mutation_kind='update' THEN
          IF NEW.deployment_generation <= (old_config ->> 'deployment_generation')::bigint
             OR service_candidate_generation < (old_projection ->> 'candidate_generation')::bigint THEN
            RAISE EXCEPTION 'typed build update generations must advance' USING ERRCODE = '23514';
          END IF;
        ELSIF NEW.deployment_generation IS DISTINCT FROM (old_config ->> 'deployment_generation')::bigint
           OR NEW.reporter_incarnation::text IS DISTINCT FROM old_config ->> 'demand_reporter_incarnation'
           OR projection -> 'candidate_generation' IS DISTINCT FROM old_projection -> 'candidate_generation'
           OR projection -> 'demand_reporter_token_sha256' IS DISTINCT FROM old_projection -> 'demand_reporter_token_sha256'
           OR (ack - ARRAY['configuration_generation','acknowledgement_sha256'])
                IS DISTINCT FROM old_ack - ARRAY['configuration_generation','acknowledgement_sha256'] THEN
          RAISE EXCEPTION 'typed build non-deployment credentials must be retained' USING ERRCODE = '23514';
        END IF;
        IF member_purpose='personal-application' AND mutation_kind IN ('capacity','destroy')
           AND NOT public.capacity_personal_build_json_exact(
             projection - ARRAY['expected_configuration_epoch','operation_kind','operation_id','operation_epoch','configuration_generation','min_slots','max_slots'],
             old_projection - ARRAY['expected_configuration_epoch','operation_kind','operation_id','operation_epoch','configuration_generation','min_slots','max_slots']) THEN
          RAISE EXCEPTION 'typed application must retain complete installation evidence' USING ERRCODE = '23514';
        END IF;
      END IF;
      -- Newly issued reporters must be unused across both purposes and all epochs.
      IF mutation_kind IN ('create','update') AND EXISTS (
          SELECT 1 FROM public.capacity_personal_membership_events e WHERE e.reporter_incarnation=NEW.reporter_incarnation
          OR e.request_payload #>> '{command,projection,demand_reporter_token_sha256}' = projection ->> 'demand_reporter_token_sha256'
          OR e.request_payload #>> '{projection,demand_reporter_token_sha256}' = projection ->> 'demand_reporter_token_sha256') THEN
        RAISE EXCEPTION 'typed build reporter or credential was already used' USING ERRCODE = '23514';
      END IF;
      -- Do not steal a base/other-purpose identity, even from retained history.
      IF EXISTS (SELECT 1 FROM public.capacity_personal_membership_events e WHERE
            e.subject_id<>NEW.subject_id AND (e.subject_incarnation=NEW.subject_incarnation
              OR e.result_payload #>> '{member,configuration,display_name}' = expected_config ->> 'display_name'))
         OR EXISTS (SELECT 1 FROM public.capacity_personal_membership_events e WHERE e.subject_id=NEW.subject_id
              AND (e.subject_incarnation<>NEW.subject_incarnation OR e.owner_id<>NEW.owner_id
                OR coalesce(e.request_payload #>> '{command,purpose}','personal-application')<>member_purpose
                OR e.result_payload #>> '{member,configuration,display_name}' IS DISTINCT FROM expected_config ->> 'display_name'))
         OR EXISTS (SELECT 1 FROM public.capacity_config_generations g WHERE
              (g.subject_id = NEW.subject_id OR g.subject_incarnation = NEW.subject_incarnation)
              AND (base_origin IS NULL OR g.subject_id IS DISTINCT FROM NEW.subject_id OR g.subject_incarnation IS DISTINCT FROM NEW.subject_incarnation
                OR g.payload ->> 'account_id' IS DISTINCT FROM derived_account_id OR g.payload ->> 'display_name' IS DISTINCT FROM expected_config ->> 'display_name'))
         OR EXISTS (SELECT 1 FROM public.capacity_subjects s WHERE
              (s.subject_id = NEW.subject_id AND (s.subject_incarnation <> NEW.subject_incarnation
                OR (s.configuration_epoch <> epoch_record.configuration_epoch AND base_origin IS NULL)
                OR s.account_id<>derived_account_id OR s.display_name<>expected_config ->> 'display_name'))
              OR (s.subject_id <> NEW.subject_id AND (s.subject_incarnation = NEW.subject_incarnation
                OR s.display_name = expected_config ->> 'display_name')))
         OR (SELECT count(*) FROM (SELECT e.subject_id FROM public.capacity_personal_membership_events e
              WHERE e.execution_epoch=NEW.execution_epoch UNION SELECT value::uuid FROM
              jsonb_array_elements_text(policy -> 'managed_base_subject_ids') UNION SELECT NEW.subject_id) subjects)
              > (policy ->> 'max_subjects')::bigint
         OR (SELECT count(*) FROM public.capacity_subjects s WHERE s.configuration_epoch=epoch_record.configuration_epoch
              AND s.account_id=derived_account_id AND s.lifecycle_state <> 'disabled') > (owner_policy ->> 'max_live_subjects')::bigint
         OR (SELECT coalesce(sum(s.min_slots),0) FROM public.capacity_subjects s WHERE s.configuration_epoch=epoch_record.configuration_epoch
              AND s.account_id=derived_account_id AND s.lifecycle_state <> 'disabled') > (owner_policy ->> 'min_reservation_slots')::numeric THEN
        RAISE EXCEPTION 'typed build identity or membership bound changed' USING ERRCODE = '23514';
      END IF;
      -- Full materialization, including indexed fields, must agree with the event.
      SELECT to_jsonb(s) - ARRAY['id','configuration_epoch','payload'] INTO retained FROM public.capacity_subjects s
        WHERE s.configuration_epoch=epoch_record.configuration_epoch AND s.subject_id=NEW.subject_id
          AND public.capacity_personal_build_json_exact(s.payload,config);
      IF NOT public.capacity_personal_build_json_exact(retained,config - ARRAY['schema_version','profiles']) THEN
        RAISE EXCEPTION 'typed build subject materialization changed' USING ERRCODE = '23514';
      END IF;
      SELECT to_jsonb(a) - ARRAY['id','configuration_epoch','payload'] INTO retained FROM public.capacity_account_policies a
        WHERE a.configuration_epoch=epoch_record.configuration_epoch AND a.account_id=derived_account_id
          AND public.capacity_personal_build_json_exact(a.payload,owner_policy);
      IF NOT public.capacity_personal_build_json_exact(retained,(owner_policy - 'schema_version') || jsonb_build_object('max_builds',0,'max_artifact_bytes',0)) THEN
        RAISE EXCEPTION 'typed build owner materialization changed' USING ERRCODE = '23514';
      END IF;
      runtime_digest := CASE WHEN member_purpose='personal-application' THEN projection ->> 'candidate_sha256'
        ELSE encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(candidate),'UTF8')),'hex') END;
      template_digest := encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(template),'UTF8')),'hex');
      expected_candidate := jsonb_build_object('candidate_digest',runtime_digest,
          'candidate_identity_algorithm',candidate -> 'algorithm','candidate_identity',candidate -> 'identity',
          'source_payload',jsonb_build_object('publication_sha256',candidate -> 'publication_sha256'),
          'artifact_payload',jsonb_build_object('runtime_candidate',candidate),
          'architecture_payload','{"platform_pools":{"linux/amd64":"oldlab","linux/arm64":"gb10"}}'::jsonb,
          'launcher_payload',jsonb_build_object('purpose','personal-build-worker','trusted_fleet_release_sha256',epoch_record.trusted_fleet_release_sha256),
          'attestation_payload',jsonb_build_object('build_template_sha256',template_digest),
          'protocol_payload',(SELECT jsonb_object_agg(value ->> 'pool_id',jsonb_build_object(
            'generation',value -> 'protocol_generation','digest',value -> 'protocol_digest')) FROM jsonb_array_elements(config -> 'profiles')));
      expected_deployment := jsonb_build_object('candidate_digest',runtime_digest,'required_profiles',config -> 'profiles',
          'readiness_state','pending','lifecycle_state','active','cutover_payload',jsonb_build_object(
            'purpose','personal-build-worker','runtime_candidate',candidate,'build_template_sha256',template_digest,
            'protected_admission_sha256',ack -> 'protected_admission_sha256'));
      IF member_purpose='personal-application' THEN
        IF mutation_kind IN ('create','update') THEN
          origin_projection := projection;
        ELSE
          SELECT e.request_payload #> '{command,projection}' INTO origin_projection
            FROM public.capacity_personal_membership_events e WHERE e.execution_epoch=NEW.execution_epoch
              AND e.subject_id=NEW.subject_id AND e.subject_incarnation=NEW.subject_incarnation
              AND e.deployment_generation=NEW.deployment_generation
              AND e.request_payload #>> '{command,purpose}'='personal-application'
              AND e.request_payload #>> '{command,projection,operation_kind}' IN ('create','update')
            ORDER BY e.revision DESC LIMIT 1;
          IF NOT FOUND THEN
            IF base_origin IS NULL OR NEW.deployment_generation<>(base_config ->> 'deployment_generation')::bigint THEN
              RAISE EXCEPTION 'typed application installation origin is unavailable' USING ERRCODE = '23514';
            END IF;
            origin_projection := base_installation;
          END IF;
        END IF;
        expected_candidate := jsonb_build_object('candidate_digest',runtime_digest,
          'candidate_identity_algorithm','source-sha256','candidate_identity',runtime_digest,
          'source_payload',jsonb_build_object('publication_sha256',projection -> 'candidate_publication_sha256'),
          'artifact_payload',jsonb_build_object('candidate_sha256',runtime_digest),
          'architecture_payload',jsonb_build_object('supported_architectures',projection -> 'supported_architectures','supported_pool_ids',projection -> 'supported_pool_ids'),
          'launcher_payload',jsonb_build_object('local_activation_sha256',projection -> 'local_activation_sha256'),
          'attestation_payload',jsonb_build_object('operation_id',origin_projection -> 'operation_id',
            'operation_epoch',origin_projection -> 'operation_epoch','protected_admission_sha256',projection -> 'protected_admission_sha256',
            'capacity_agent_installation_sha256',projection -> 'capacity_agent_installation_sha256'),
          'protocol_payload',projection -> 'protocol_versions');
        expected_deployment := jsonb_build_object('candidate_digest',runtime_digest,'required_profiles',config -> 'profiles',
          'readiness_state','ready','lifecycle_state','active','cutover_payload',jsonb_build_object(
            'local_activation_sha256',projection -> 'local_activation_sha256','candidate_publication_sha256',projection -> 'candidate_publication_sha256',
            'protected_admission_sha256',projection -> 'protected_admission_sha256','capacity_agent_installation_sha256',projection -> 'capacity_agent_installation_sha256'));
      END IF;
      SELECT to_jsonb(c) - ARRAY['id','subject_id','subject_incarnation','candidate_generation'] INTO retained
        FROM public.capacity_candidates c WHERE c.subject_id=NEW.subject_id AND c.subject_incarnation=NEW.subject_incarnation
          AND c.candidate_generation=service_candidate_generation;
      IF NOT public.capacity_personal_build_json_exact(retained,expected_candidate) THEN
        RAISE EXCEPTION 'typed build candidate evidence changed' USING ERRCODE = '23514';
      END IF;
      SELECT to_jsonb(d) - ARRAY['id','subject_id','subject_incarnation','deployment_generation'] INTO retained
        FROM public.capacity_deployment_generations d WHERE d.subject_id=NEW.subject_id
          AND d.subject_incarnation=NEW.subject_incarnation AND d.deployment_generation=NEW.deployment_generation;
      IF NOT public.capacity_personal_build_json_exact(retained,expected_deployment) THEN
        RAISE EXCEPTION 'typed build deployment evidence changed' USING ERRCODE = '23514';
      END IF;
      IF (SELECT count(*) FROM public.capacity_worker_profiles p WHERE p.subject_id=NEW.subject_id
          AND p.subject_incarnation=NEW.subject_incarnation AND p.deployment_generation=NEW.deployment_generation) <> jsonb_array_length(config -> 'profiles') THEN
        RAISE EXCEPTION 'typed build profile set changed' USING ERRCODE = '23514';
      END IF;
      FOR profile IN SELECT value FROM jsonb_array_elements(config -> 'profiles') LOOP
        SELECT to_jsonb(p) - ARRAY['id','subject_id','subject_incarnation','deployment_generation'] INTO retained
          FROM public.capacity_worker_profiles p WHERE p.subject_id=NEW.subject_id AND p.subject_incarnation=NEW.subject_incarnation
            AND p.deployment_generation=NEW.deployment_generation AND p.pool_id=profile ->> 'pool_id';
        IF NOT public.capacity_personal_build_json_exact(retained,jsonb_build_object('pool_id',profile -> 'pool_id','pool_generation',profile -> 'pool_generation',
            'profile_generation',profile -> 'profile_generation','profile_digest',profile -> 'profile_digest',
            'shape_catalog',profile -> 'worker_shapes','narrowing_constraints',jsonb_build_object('eligible_resource_domains',profile -> 'eligible_resource_domains'))) THEN
          RAISE EXCEPTION 'typed build profile evidence changed' USING ERRCODE = '23514';
        END IF;
      END LOOP;
      IF NOT EXISTS (SELECT 1 FROM public.capacity_demand_reporters r WHERE r.subject_id=NEW.subject_id
          AND r.subject_incarnation=NEW.subject_incarnation AND r.reporter_incarnation=NEW.reporter_incarnation
          AND r.configuration_generation=NEW.configuration_generation AND r.deployment_generation=NEW.deployment_generation
          AND r.state='current' AND (mutation_kind IN ('capacity','destroy')
            OR (r.high_water=0 AND r.last_receipt_time IS NULL AND r.last_digest IS NULL))
          AND r.token_sha256=projection ->> 'demand_reporter_token_sha256')
         OR EXISTS (SELECT 1 FROM public.capacity_demand_reporters r WHERE r.reporter_incarnation=NEW.reporter_incarnation
              AND (r.subject_id<>NEW.subject_id OR r.subject_incarnation<>NEW.subject_incarnation)) THEN
        RAISE EXCEPTION 'typed build reporter evidence changed' USING ERRCODE = '23514';
      END IF;
      IF EXISTS (SELECT 1 FROM public.capacity_demand_reporters r WHERE r.subject_id=NEW.subject_id
          AND r.reporter_incarnation<>NEW.reporter_incarnation AND r.state='current')
         OR EXISTS (
           SELECT 1 FROM (
             SELECT DISTINCT ON (e.reporter_incarnation) e.* FROM public.capacity_personal_membership_events e
               WHERE e.subject_id=NEW.subject_id AND e.execution_epoch=NEW.execution_epoch
               ORDER BY e.reporter_incarnation,e.revision DESC
           ) h WHERE h.reporter_incarnation<>NEW.reporter_incarnation AND NOT EXISTS (
             SELECT 1 FROM public.capacity_demand_reporters r WHERE r.subject_id=h.subject_id
               AND r.subject_incarnation=h.subject_incarnation AND r.reporter_incarnation=h.reporter_incarnation
               AND r.state='fenced' AND r.configuration_generation=h.configuration_generation
               AND r.deployment_generation=h.deployment_generation
               AND r.token_sha256=h.request_payload #>> '{command,projection,demand_reporter_token_sha256}'
           )
         ) THEN
        RAISE EXCEPTION 'typed build retired reporter evidence changed' USING ERRCODE = '23514';
      END IF;
      IF base_origin IS NOT NULL AND base_config ->> 'demand_reporter_incarnation'<>NEW.reporter_incarnation::text THEN
        SELECT e.result_payload #> '{member,configuration}',e.request_payload #> '{command,projection}' INTO old_config,old_projection
          FROM public.capacity_personal_membership_events e WHERE e.execution_epoch=NEW.execution_epoch AND e.subject_id=NEW.subject_id
            AND e.reporter_incarnation=(base_config ->> 'demand_reporter_incarnation')::uuid ORDER BY e.revision DESC LIMIT 1;
        IF NOT FOUND THEN old_config := base_config; old_projection := base_projection; END IF;
        IF NOT EXISTS (SELECT 1 FROM public.capacity_demand_reporters r WHERE r.subject_id=NEW.subject_id AND r.subject_incarnation=NEW.subject_incarnation
            AND r.reporter_incarnation=(old_config ->> 'demand_reporter_incarnation')::uuid AND r.state='fenced'
            AND r.configuration_generation=(old_config ->> 'configuration_generation')::bigint
            AND r.deployment_generation=(old_config ->> 'deployment_generation')::bigint
            AND r.token_sha256=old_projection ->> 'demand_reporter_token_sha256') THEN
          RAISE EXCEPTION 'typed managed base retired reporter evidence changed' USING ERRCODE = '23514';
        END IF;
      END IF;
      RETURN NEW;
    END $$;
    REVOKE ALL ON FUNCTION public.capacity_personal_build_initial_insert_guard() FROM PUBLIC;
    DROP TRIGGER capacity_personal_membership_insert_guard ON public.capacity_personal_membership_events;
    CREATE TRIGGER capacity_personal_membership_insert_guard BEFORE INSERT ON public.capacity_personal_membership_events
      FOR EACH ROW WHEN (NEW.request_payload -> 'schema_version' = '1'::jsonb)
      EXECUTE FUNCTION public.capacity_personal_membership_insert_guard();
    CREATE TRIGGER capacity_personal_build_initial_insert_guard BEFORE INSERT ON public.capacity_personal_membership_events
      FOR EACH ROW WHEN (NEW.request_payload -> 'schema_version' IS DISTINCT FROM '1'::jsonb)
      EXECUTE FUNCTION public.capacity_personal_build_initial_insert_guard();
    """)


def _uuid_extension_schema() -> str:
    bind = op.get_bind()
    query = sa.text(
        "SELECT namespace.nspname FROM pg_catalog.pg_extension AS extension "
        "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = extension.extnamespace "
        "WHERE extension.extname = 'uuid-ossp'"
    )
    schema = bind.execute(query).scalar_one_or_none()
    if schema is not None:
        return str(schema)
    if bind.execute(sa.text("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :name)"), {"name": _EXTENSION_SCHEMA}).scalar_one():
        raise RuntimeError("build extension schema already exists without its UUID extension")
    # Only the newly created schema's privileges are changed. An existing shared
    # extension is reused where it is, without moving it or touching its ACLs.
    op.execute(f"CREATE SCHEMA {_EXTENSION_SCHEMA}")
    op.execute(f"REVOKE ALL ON SCHEMA {_EXTENSION_SCHEMA} FROM PUBLIC")
    op.execute(f'CREATE EXTENSION "uuid-ossp" WITH SCHEMA {_EXTENSION_SCHEMA}')
    resolved = bind.execute(query).scalar_one()
    if resolved != _EXTENSION_SCHEMA:
        raise RuntimeError("build UUID extension installation changed")
    return _EXTENSION_SCHEMA


def upgrade() -> None:
    schema = _uuid_extension_schema()
    qualified_schema = op.get_bind().dialect.identifier_preparer.quote_schema(schema)
    op.execute(
        f"""
        CREATE FUNCTION public.capacity_personal_build_subject_id(p_namespace uuid, p_owner uuid)
        RETURNS uuid LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $$
        BEGIN
          IF p_namespace IS NULL OR p_owner IS NULL
             OR p_namespace = '00000000-0000-0000-0000-000000000000'::uuid
             OR p_owner = '00000000-0000-0000-0000-000000000000'::uuid THEN
            RAISE EXCEPTION 'build membership identity must be nonzero'
              USING ERRCODE = '23514';
          END IF;
          RETURN {qualified_schema}.uuid_generate_v5(
            p_namespace, 'personal-build-worker:' || pg_catalog.replace(p_owner::text, '-', '')
          );
        END $$;
        REVOKE EXECUTE ON FUNCTION public.capacity_personal_build_subject_id(uuid,uuid) FROM PUBLIC;
        """
    )
    _install_initial_build_guard()


def downgrade() -> None:
    op.execute("LOCK TABLE public.capacity_personal_membership_events, public.capacity_execution_epochs IN SHARE ROW EXCLUSIVE MODE")
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM public.capacity_personal_membership_events "
        "WHERE request_payload -> 'schema_version' = '2'::jsonb "
        "OR result_payload -> 'member' ->> 'purpose' = 'personal-build-worker') "
        "OR EXISTS (SELECT 1 FROM public.capacity_execution_epochs "
        "WHERE manifest_payload -> 'schema_version' = '4'::jsonb)"
    )).scalar_one():
        raise RuntimeError("cannot downgrade capacity_0018 while typed membership history exists")
    op.execute("""
    DROP TRIGGER capacity_personal_build_initial_insert_guard ON public.capacity_personal_membership_events;
    DROP TRIGGER capacity_personal_membership_insert_guard ON public.capacity_personal_membership_events;
    CREATE TRIGGER capacity_personal_membership_insert_guard BEFORE INSERT ON public.capacity_personal_membership_events
      FOR EACH ROW EXECUTE FUNCTION public.capacity_personal_membership_insert_guard();
    DROP FUNCTION public.capacity_personal_build_initial_insert_guard();
    DROP FUNCTION public.capacity_personal_application_installation_matches(jsonb,jsonb);
    DROP FUNCTION public.capacity_personal_build_json_exact(jsonb,jsonb);
    DROP FUNCTION public.capacity_personal_build_subject_id(uuid,uuid);
    """)
    # Keep the standard extension and its schema: other database objects may
    # depend on them, including when the extension predated this migration.
