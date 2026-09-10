"""Add the exact SQL identity prerequisite for typed build membership.

Revision ID: capacity_0018
Revises: capacity_0017

Typed build lifecycle events may be recorded with pending generation evidence.
Recreation verifies durable release; V4 executable admission remains interlocked.
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


def _install_release_proof_helpers() -> None:
    # These invoker-only readers grant no mutation authority. The insertion guard
    # calls them under its existing shared authority lock and serializable write.
    op.execute("""
    -- Release witness keys include release_digest/released_at. Locale-sensitive
    -- ordering can invert those keys; wire hashes require ASCII key ordering.
    CREATE FUNCTION public.capacity_personal_release_json_text(p_value jsonb)
    RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $$
      SELECT CASE jsonb_typeof(p_value)
        WHEN 'object' THEN (SELECT '{' || coalesce(string_agg(to_jsonb(e.key)::text || ':' ||
          public.capacity_personal_release_json_text(e.value),',' ORDER BY e.key COLLATE "C"),'') || '}' FROM jsonb_each(p_value) e)
        WHEN 'array' THEN (SELECT '[' || coalesce(string_agg(public.capacity_personal_release_json_text(e.value),',' ORDER BY e.n),'') || ']'
          FROM jsonb_array_elements(p_value) WITH ORDINALITY e(value,n))
        ELSE p_value::text END
    $$;
    REVOKE ALL ON FUNCTION public.capacity_personal_release_json_text(jsonb) FROM PUBLIC;
    CREATE FUNCTION public.capacity_personal_release_json_digest(p_value jsonb)
    RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $$
      SELECT encode(sha256(convert_to(public.capacity_personal_release_json_text(p_value),'UTF8')),'hex')
    $$;
    REVOKE ALL ON FUNCTION public.capacity_personal_release_json_digest(jsonb) FROM PUBLIC;
    CREATE FUNCTION public.capacity_personal_release_timestamp(p_at timestamptz)
    RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $$
      SELECT to_char(p_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS') ||
        CASE WHEN extract(microseconds FROM p_at AT TIME ZONE 'UTC')::bigint % 1000000 = 0
          THEN '' ELSE '.' || to_char(p_at AT TIME ZONE 'UTC','US') END || '+00:00'
    $$;
    REVOKE ALL ON FUNCTION public.capacity_personal_release_timestamp(timestamptz) FROM PUBLIC;
    CREATE FUNCTION public.capacity_personal_predecessor_release_digest(p_subject uuid,p_incarnation uuid)
    RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
    DECLARE
      tranche record; shape record; legacy_intent record; release_record record;
      protected record; observation record; intent record; terminal record;
      witnesses jsonb := '[]'::jsonb; shapes jsonb; ownership jsonb;
      payload jsonb; binding jsonb; item jsonb; kind text; terminal_digest text;
    BEGIN
      IF p_subject IS NULL OR p_incarnation IS NULL THEN
        RAISE EXCEPTION 'predecessor release identity is absent' USING ERRCODE='23514';
      END IF;
      IF EXISTS (SELECT 1 FROM public.capacity_observed_commitments o
          WHERE (o.subject_id=p_subject AND o.subject_incarnation=p_incarnation)
            OR (o.binding_payload #>> '{observed_contract,subject_id}'=p_subject::text
              AND o.binding_payload #>> '{observed_contract,subject_incarnation}'=p_incarnation::text)) THEN
        RAISE EXCEPTION 'predecessor has unreleased observed commitments' USING ERRCODE='23514';
      END IF;
      FOR tranche IN SELECT * FROM public.capacity_reservation_tranches
          WHERE subject_id=p_subject AND subject_incarnation=p_incarnation ORDER BY id LOOP
        IF tranche.state IS DISTINCT FROM 'closed' OR tranche.closed_at IS NULL THEN
          RAISE EXCEPTION 'predecessor has unreleased legacy reservations' USING ERRCODE='23514';
        END IF;
        shapes := '[]'::jsonb;
        IF tranche.accepted_at IS NULL THEN
          IF tranche.closure_reason IS NULL OR tranche.closure_reason NOT IN ('proposal-expired','proposal-superseded')
             OR EXISTS (SELECT 1 FROM public.capacity_reservation_shapes WHERE tranche_id=tranche.id)
             OR EXISTS (SELECT 1 FROM public.capacity_submission_intents WHERE tranche_id=tranche.id) THEN
            RAISE EXCEPTION 'legacy predecessor unaccepted closure changed' USING ERRCODE='23514';
          END IF;
          kind := 'never-accepted-legacy';
        ELSE
          IF tranche.closure_reason IS DISTINCT FROM 'fully-released'
             OR NOT EXISTS (SELECT 1 FROM public.capacity_reservation_shapes WHERE tranche_id=tranche.id)
             OR EXISTS (SELECT 1 FROM public.capacity_submission_intents i WHERE i.tranche_id=tranche.id
               AND NOT EXISTS (SELECT 1 FROM public.capacity_reservation_shapes s WHERE s.tranche_id=tranche.id AND s.intent_id=i.id)) THEN
            RAISE EXCEPTION 'legacy predecessor released shape set changed' USING ERRCODE='23514';
          END IF;
          FOR shape IN SELECT * FROM public.capacity_reservation_shapes WHERE tranche_id=tranche.id ORDER BY shape_instance_id COLLATE "C" LOOP
            SELECT * INTO legacy_intent FROM public.capacity_submission_intents WHERE id=shape.intent_id AND tranche_id=tranche.id;
            IF NOT FOUND OR shape.state IS DISTINCT FROM 'released' OR shape.released_at IS NULL
               OR legacy_intent.state IS DISTINCT FROM 'closed' OR legacy_intent.shape_instance_id IS DISTINCT FROM shape.shape_instance_id
               OR legacy_intent.executor_id IS DISTINCT FROM tranche.executor_id
               OR legacy_intent.executor_incarnation IS DISTINCT FROM tranche.executor_incarnation THEN
              RAISE EXCEPTION 'legacy predecessor release witness changed' USING ERRCODE='23514';
            END IF;
            ownership := jsonb_build_object('schema_version',1,'executable',false,
              'authority_incarnation',tranche.authority_incarnation,'writer_epoch',tranche.writer_epoch,
              'configuration_epoch',tranche.configuration_epoch,'allocation_epoch',tranche.allocation_epoch,
              'tranche_id',tranche.id,'intent_id',shape.intent_id,'shape_instance_id',shape.shape_instance_id,
              'subject_id',p_subject,'subject_incarnation',p_incarnation,'account_id',tranche.account_id,
              'tier_id',tranche.tier_id,'candidate_generation',tranche.candidate_generation,
              'deployment_generation',tranche.deployment_generation,'pool_id',tranche.pool_id,'pool_generation',tranche.pool_generation,
              'shape_id',shape.shape_id,'profile_id',shape.profile_id,'profile_generation',shape.profile_generation,
              'profile_digest',shape.profile_digest,'concurrency_slots',shape.concurrency_slots,
              'resources',shape.resource_vector,'node_ids',shape.node_ids,'executor_id',tranche.executor_id,'executor_incarnation',tranche.executor_incarnation);
            IF public.capacity_personal_release_json_digest(ownership) IS DISTINCT FROM legacy_intent.ownership_metadata_sha256 THEN
              RAISE EXCEPTION 'legacy predecessor ownership changed' USING ERRCODE='23514';
            END IF;
            SELECT * INTO release_record FROM public.capacity_reservation_release_evidence WHERE shape_instance_id=shape.shape_instance_id;
            IF NOT FOUND THEN RAISE EXCEPTION 'legacy predecessor release witness is absent' USING ERRCODE='23514'; END IF;
            SELECT * INTO protected FROM public.capacity_protected_release_acknowledgements WHERE shape_instance_id=shape.shape_instance_id;
            IF NOT FOUND THEN RAISE EXCEPTION 'legacy predecessor protected witness is absent' USING ERRCODE='23514'; END IF;
            payload := (to_jsonb(release_record) - ARRAY['id','evidence_digest','received_at']) || jsonb_build_object('executable',false);
            IF public.capacity_personal_release_json_digest(payload) IS DISTINCT FROM release_record.evidence_digest
               OR shape.release_evidence_digest IS DISTINCT FROM release_record.evidence_digest
               OR release_record.tranche_id IS DISTINCT FROM tranche.id OR release_record.intent_id IS DISTINCT FROM legacy_intent.id
               OR release_record.executor_id IS DISTINCT FROM tranche.executor_id OR release_record.executor_incarnation IS DISTINCT FROM tranche.executor_incarnation
               OR release_record.bootstrap_revoked IS DISTINCT FROM true
               OR public.capacity_personal_release_json_digest((to_jsonb(protected) - ARRAY['id','idempotency_key','acknowledgement_digest','actor_id','received_at'])
                    || jsonb_build_object('schema_version',1)) IS DISTINCT FROM protected.acknowledgement_digest
               OR protected.tranche_id IS DISTINCT FROM tranche.id OR protected.intent_id IS DISTINCT FROM legacy_intent.id
               OR protected.authority_incarnation IS DISTINCT FROM tranche.authority_incarnation OR protected.writer_epoch IS DISTINCT FROM tranche.writer_epoch
               OR protected.configuration_epoch IS DISTINCT FROM tranche.configuration_epoch OR protected.allocation_epoch IS DISTINCT FROM tranche.allocation_epoch
               OR protected.subject_id IS DISTINCT FROM p_subject OR protected.subject_incarnation IS DISTINCT FROM p_incarnation
               OR protected.deployment_generation IS DISTINCT FROM tranche.deployment_generation
               OR protected.pool_id IS DISTINCT FROM tranche.pool_id OR protected.pool_generation IS DISTINCT FROM tranche.pool_generation
               OR protected.bootstrap_registration_epoch IS DISTINCT FROM coalesce(legacy_intent.bootstrap_registration_epoch,0)
               OR protected.protected_registration_epoch IS DISTINCT FROM release_record.protected_registration_epoch
               OR protected.protected_release_sha256 IS DISTINCT FROM release_record.protected_release_sha256
               OR protected.bootstrap_revoked IS DISTINCT FROM true OR protected.executable IS DISTINCT FROM false THEN
              RAISE EXCEPTION 'legacy predecessor release witness changed' USING ERRCODE='23514';
            END IF;
            SELECT * INTO observation FROM public.capacity_executor_observations
              WHERE executor_incarnation=tranche.executor_incarnation AND inventory_sequence=release_record.inventory_sequence;
            IF NOT FOUND OR observation.validity IS DISTINCT FROM 'valid' THEN
              RAISE EXCEPTION 'legacy predecessor inventory witness is absent' USING ERRCODE='23514';
            END IF;
            payload := observation.payload;
            IF public.capacity_personal_release_json_digest(payload) IS DISTINCT FROM observation.inventory_digest
               OR payload ->> 'executor_id' IS DISTINCT FROM tranche.executor_id
               OR payload ->> 'executor_incarnation' IS DISTINCT FROM tranche.executor_incarnation::text
               OR payload -> 'inventory_sequence' IS DISTINCT FROM to_jsonb(observation.inventory_sequence)
               OR payload ->> 'pool_id' IS DISTINCT FROM tranche.pool_id OR observation.pool_id IS DISTINCT FROM tranche.pool_id
               OR payload -> 'pool_generation' IS DISTINCT FROM to_jsonb(tranche.pool_generation) OR observation.pool_generation IS DISTINCT FROM tranche.pool_generation
               OR payload -> 'journal_sequence' IS DISTINCT FROM to_jsonb(observation.journal_sequence)
               OR payload ->> 'journal_digest' IS DISTINCT FROM observation.journal_digest THEN
              RAISE EXCEPTION 'legacy predecessor inventory witness changed' USING ERRCODE='23514';
            END IF;
            IF release_record.terminal_kind='unused' THEN
              IF release_record.terminal_identity IS DISTINCT FROM shape.shape_instance_id
                 OR release_record.terminal_evidence_sha256 IS DISTINCT FROM observation.inventory_digest
                 OR EXISTS (SELECT 1 FROM jsonb_array_elements(payload -> 'records') r WHERE
                   r #>> '{ownership_proof,metadata,intent_id}'=legacy_intent.id::text
                   OR r #>> '{ownership_proof,metadata,shape_instance_id}'=shape.shape_instance_id) THEN
                RAISE EXCEPTION 'legacy predecessor unused witness changed' USING ERRCODE='23514';
              END IF;
            ELSE
              SELECT value INTO item FROM jsonb_array_elements(payload -> 'records') WHERE value ->> 'physical_identity'=release_record.terminal_identity;
              IF NOT FOUND OR item ->> 'state' IS DISTINCT FROM 'terminal'
                 OR item ->> 'physical_kind' IS DISTINCT FROM release_record.terminal_kind
                 OR item ->> 'terminal_evidence_sha256' IS DISTINCT FROM release_record.terminal_evidence_sha256
                 OR item #> '{ownership_proof,metadata}' IS DISTINCT FROM ownership
                 OR (SELECT c.value ->> 'classification' FROM jsonb_array_elements(observation.classification_payload) WITH ORDINALITY c(value,n)
                    WHERE c.value ->> 'physical_identity'=release_record.terminal_identity ORDER BY c.n LIMIT 1) IS DISTINCT FROM 'authenticated' THEN
                RAISE EXCEPTION 'legacy predecessor physical witness changed' USING ERRCODE='23514';
              END IF;
            END IF;
            shapes := shapes || jsonb_build_array(jsonb_build_object('shape_instance_id',shape.shape_instance_id,
              'intent_id',legacy_intent.id,'ownership_digest',legacy_intent.ownership_metadata_sha256,
              'release_digest',release_record.evidence_digest,'protected_digest',protected.acknowledgement_digest,
              'inventory_digest',observation.inventory_digest,'released_at',public.capacity_personal_release_timestamp(shape.released_at)));
          END LOOP;
          kind := 'accepted-legacy';
        END IF;
        witnesses := witnesses || jsonb_build_array(jsonb_build_object('kind',kind,'tranche_id',tranche.id,
          'proposal_digest',tranche.proposal_digest,'closure_reason',tranche.closure_reason,
          'closed_at',public.capacity_personal_release_timestamp(tranche.closed_at),'shapes',shapes));
      END LOOP;
      FOR intent IN SELECT * FROM public.capacity_executable_intents
          WHERE subject_id=p_subject AND subject_incarnation=p_incarnation ORDER BY intent_id LOOP
        IF intent.state IS DISTINCT FROM 'released' OR intent.released_at IS NULL THEN
          RAISE EXCEPTION 'predecessor has unreleased executable intents' USING ERRCODE='23514';
        END IF;
        binding := intent.binding_payload;
        IF binding ->> 'subject_id' IS DISTINCT FROM p_subject::text OR binding ->> 'subject_incarnation' IS DISTINCT FROM p_incarnation::text
           OR binding ->> 'intent_id' IS DISTINCT FROM intent.intent_id::text OR binding ->> 'shape_instance_id' IS DISTINCT FROM intent.shape_instance_id
           OR binding #> '{execution,execution_epoch}' IS DISTINCT FROM to_jsonb(intent.execution_epoch)
           OR binding #>> '{execution,execution_manifest_sha256}' IS DISTINCT FROM intent.execution_manifest_sha256
           OR public.capacity_personal_release_json_digest(binding) IS DISTINCT FROM intent.binding_digest THEN
          RAISE EXCEPTION 'predecessor release binding changed' USING ERRCODE='23514';
        END IF;
        IF intent.accepted_at IS NULL THEN
          IF intent.bootstrap_registration_epoch IS NOT NULL OR intent.bootstrap_evidence_sha256 IS NOT NULL
             OR intent.permit_id IS NOT NULL OR intent.permit_consumed_at IS NOT NULL OR intent.inventory_sequence IS NOT NULL
             OR intent.terminal_kind IS NOT NULL OR intent.observed_state IS NOT NULL THEN
            RAISE EXCEPTION 'predecessor unaccepted release witness changed' USING ERRCODE='23514';
          END IF;
          item := jsonb_build_object('kind','never-accepted-executable','intent_id',intent.intent_id,
            'binding_digest',intent.binding_digest,'released_at',public.capacity_personal_release_timestamp(intent.released_at));
        ELSE
          SELECT * INTO protected FROM public.capacity_executable_protected_release_receipts
            WHERE intent_id=intent.intent_id ORDER BY protected_registration_epoch LIMIT 1;
          IF NOT FOUND THEN RAISE EXCEPTION 'predecessor protected release witness is absent' USING ERRCODE='23514'; END IF;
          IF protected.release_payload IS DISTINCT FROM jsonb_build_object('schema_version',2,'executable',true,'binding',binding,
                'reporter_incarnation',protected.reporter_incarnation,'bootstrap_registration_epoch',intent.bootstrap_registration_epoch,
                'protected_registration_epoch',protected.protected_registration_epoch,'bootstrap_revoked',true,'protected_release_sha256',protected.protected_release_sha256)
             OR protected.bootstrap_registration_epoch IS DISTINCT FROM intent.bootstrap_registration_epoch
             OR protected.execution_epoch IS DISTINCT FROM intent.execution_epoch OR protected.execution_manifest_sha256 IS DISTINCT FROM intent.execution_manifest_sha256
             OR public.capacity_personal_release_json_digest(protected.release_payload) IS DISTINCT FROM protected.acknowledgement_digest
             OR intent.inventory_sequence IS NULL OR intent.terminal_evidence_sha256 IS NULL THEN
            RAISE EXCEPTION 'predecessor protected release witness changed' USING ERRCODE='23514';
          END IF;
          terminal_digest := intent.terminal_evidence_sha256;
          IF intent.terminal_kind='unused' THEN
            IF intent.permit_consumed_at IS NOT NULL OR intent.terminal_identity IS DISTINCT FROM intent.shape_instance_id THEN
              RAISE EXCEPTION 'predecessor unused release witness changed' USING ERRCODE='23514';
            END IF;
          ELSE
            SELECT * INTO terminal FROM public.capacity_executable_terminal_inventory_evidence WHERE intent_id=intent.intent_id;
            IF NOT FOUND THEN RAISE EXCEPTION 'predecessor terminal release witness is absent' USING ERRCODE='23514'; END IF;
            payload := terminal.evidence_payload;
            item := payload -> 'record';
            IF public.capacity_personal_release_json_text(payload) IS DISTINCT FROM public.capacity_personal_release_json_text(jsonb_build_object(
                 'schema_version',2,'executable',true,'binding',binding,
                 'inventory_execution',(binding -> 'execution') - ARRAY['allocation_epoch','executable'],
                 'inventory_sequence',terminal.inventory_sequence,'inventory_digest',terminal.inventory_digest,
                 'journal_sequence',terminal.journal_sequence,'journal_digest',terminal.journal_digest,
                 'record',item,'observed_at',payload -> 'observed_at'))
               OR public.capacity_personal_release_json_text(item) IS DISTINCT FROM public.capacity_personal_release_json_text(jsonb_build_object(
                 'schema_version',2,'physical_identity',terminal.physical_identity,'physical_kind',terminal.physical_kind,
                 'authority_scope','dedicated-loom-association','state','terminal','resources',binding -> 'resources','node_ids',binding -> 'node_ids',
                 'controller_evidence_sha256',terminal.controller_evidence_sha256,'ownership_proof',item -> 'ownership_proof',
                 'terminal_evidence_sha256',terminal.terminal_evidence_sha256))
               OR item #> '{ownership_proof,metadata,binding}' IS DISTINCT FROM binding
               OR (terminal.journal_sequence=0) IS DISTINCT FROM (terminal.journal_digest=repeat('0',64))
               OR payload -> 'binding' IS DISTINCT FROM binding
               OR payload -> 'inventory_sequence' IS DISTINCT FROM to_jsonb(intent.inventory_sequence)
               OR payload #>> '{record,physical_kind}' IS DISTINCT FROM intent.terminal_kind
               OR payload #>> '{record,physical_identity}' IS DISTINCT FROM intent.terminal_identity
               OR payload #>> '{record,terminal_evidence_sha256}' IS DISTINCT FROM intent.terminal_evidence_sha256
               OR terminal.subject_id IS DISTINCT FROM p_subject OR terminal.subject_incarnation IS DISTINCT FROM p_incarnation
               OR terminal.execution_epoch IS DISTINCT FROM intent.execution_epoch OR terminal.execution_manifest_sha256 IS DISTINCT FROM intent.execution_manifest_sha256
               OR terminal.executor_id IS DISTINCT FROM binding ->> 'executor_id' OR terminal.executor_incarnation::text IS DISTINCT FROM binding ->> 'executor_incarnation'
               OR terminal.pool_id IS DISTINCT FROM binding ->> 'pool_id' OR to_jsonb(terminal.pool_generation) IS DISTINCT FROM binding -> 'pool_generation'
               OR terminal.inventory_sequence IS DISTINCT FROM intent.inventory_sequence
               OR terminal.inventory_digest IS DISTINCT FROM payload ->> 'inventory_digest'
               OR to_jsonb(terminal.journal_sequence) IS DISTINCT FROM payload -> 'journal_sequence' OR terminal.journal_digest IS DISTINCT FROM payload ->> 'journal_digest'
               OR terminal.physical_kind IS DISTINCT FROM intent.terminal_kind OR terminal.physical_identity IS DISTINCT FROM intent.terminal_identity
               OR terminal.controller_evidence_sha256 IS DISTINCT FROM payload #>> '{record,controller_evidence_sha256}'
               OR terminal.terminal_evidence_sha256 IS DISTINCT FROM intent.terminal_evidence_sha256
               OR terminal.observed_at IS DISTINCT FROM (payload ->> 'observed_at')::timestamptz
               OR public.capacity_personal_release_json_digest(payload) IS DISTINCT FROM terminal.evidence_digest THEN
              RAISE EXCEPTION 'predecessor terminal release witness changed' USING ERRCODE='23514';
            END IF;
            terminal_digest := terminal.evidence_digest;
          END IF;
          item := jsonb_build_object('kind','accepted-executable','intent_id',intent.intent_id,'binding_digest',intent.binding_digest,
            'protected_witness',protected.acknowledgement_digest,'terminal_witness',terminal_digest,'inventory_sequence',intent.inventory_sequence,
            'terminal_kind',intent.terminal_kind,'terminal_identity',intent.terminal_identity,'released_at',public.capacity_personal_release_timestamp(intent.released_at));
        END IF;
        witnesses := witnesses || jsonb_build_array(item);
      END LOOP;
      RETURN public.capacity_personal_release_json_digest(jsonb_build_object('schema_version',1,
        'subject_id',p_subject,'subject_incarnation',p_incarnation,'witnesses',witnesses));
    END $$;
    REVOKE ALL ON FUNCTION public.capacity_personal_predecessor_release_digest(uuid,uuid) FROM PUBLIC;
    """)


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
      proof jsonb;
      origin_reference jsonb;
      first_config jsonb;
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
              'acknowledgement',ack,'reincarnation',member -> 'reincarnation')
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
        IF member_purpose<>'personal-application'
           OR (SELECT count(*) FROM jsonb_array_elements(preparation -> 'managed_application_origins') value
                WHERE value #>> '{configuration,subject_id}'=NEW.subject_id::text)<>1
           OR NOT coalesce(policy -> 'managed_base_subject_ids' @> jsonb_build_array(NEW.subject_id::text),false)
           OR base_config ->> 'account_id' IS DISTINCT FROM derived_account_id
           OR base_config ->> 'display_name' IS DISTINCT FROM expected_config ->> 'display_name'
           OR base_projection ->> 'owner_id' IS DISTINCT FROM NEW.owner_id::text
           OR base_projection ->> 'subject_id' IS DISTINCT FROM NEW.subject_id::text
           OR base_projection ->> 'subject_incarnation' IS DISTINCT FROM base_config ->> 'subject_incarnation'
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
              'schema_version',1,'scope','subject','subject_id',NEW.subject_id::text,'subject_incarnation',base_config -> 'subject_incarnation',
              'generation',base_config -> 'configuration_generation',
              'digest',encode(sha256(convert_to(public.capacity_executable_canonical_jsonb_text(base_config),'UTF8')),'hex')))
           OR NOT EXISTS (SELECT 1 FROM public.capacity_config_generations g WHERE g.scope='subject' AND g.subject_id=NEW.subject_id
                AND g.subject_incarnation=(base_config ->> 'subject_incarnation')::uuid AND g.scope_generation=(base_config ->> 'configuration_generation')::bigint
                AND g.digest=base_reference ->> 'digest' AND public.capacity_personal_build_json_exact(g.payload,base_config))
           OR NOT public.capacity_personal_build_json_exact(base_config,expected_config || jsonb_build_object(
                'subject_incarnation',base_config -> 'subject_incarnation',
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
        IF service_candidate_generation<>1 OR NEW.deployment_generation<>1 THEN
          RAISE EXCEPTION 'typed recreation deployment must start at one' USING ERRCODE='23514';
        END IF;
        proof := nullif(member -> 'reincarnation','null'::jsonb);
        IF old_config IS NULL THEN
          IF proof IS NOT NULL OR EXISTS (SELECT 1 FROM public.capacity_personal_membership_events e WHERE e.subject_id=NEW.subject_id) THEN
            RAISE EXCEPTION 'typed fresh create has retained membership' USING ERRCODE='23514';
          END IF;
        ELSE
          IF old_member IS NULL OR proof IS NULL OR old_config ->> 'lifecycle_state' IS DISTINCT FROM 'disabled'
             OR old_config -> 'min_slots' IS DISTINCT FROM '0'::jsonb OR old_config -> 'max_slots' IS DISTINCT FROM '0'::jsonb
             OR old_config ->> 'subject_incarnation'=NEW.subject_incarnation::text
             OR (old_config ->> 'configuration_generation')::bigint >= NEW.configuration_generation
             OR old_config ->> 'display_name' IS DISTINCT FROM config ->> 'display_name'
             OR EXISTS (SELECT 1 FROM public.capacity_personal_membership_events e WHERE e.subject_incarnation=NEW.subject_incarnation)
             OR EXISTS (SELECT 1 FROM public.capacity_config_generations g WHERE g.subject_incarnation=NEW.subject_incarnation)
             OR EXISTS (SELECT 1 FROM public.capacity_candidates c WHERE c.subject_incarnation=NEW.subject_incarnation
                  AND (c.subject_id<>NEW.subject_id OR c.candidate_generation<>service_candidate_generation))
             OR EXISTS (SELECT 1 FROM public.capacity_demand_reporters r WHERE r.subject_incarnation=NEW.subject_incarnation
                  AND (r.subject_id<>NEW.subject_id OR r.reporter_incarnation<>NEW.reporter_incarnation))
             OR EXISTS (SELECT 1 FROM public.capacity_subjects s WHERE s.subject_incarnation=NEW.subject_incarnation
                  AND (s.configuration_epoch<>epoch_record.configuration_epoch OR s.subject_id<>NEW.subject_id)) THEN
            RAISE EXCEPTION 'typed recreation predecessor or successor identity changed' USING ERRCODE='23514';
          END IF;
          origin_reference := coalesce(nullif(old_member #> '{reincarnation,origin}','null'::jsonb),base_reference);
          IF origin_reference IS NULL THEN
            SELECT e.result_payload #> '{member,configuration}' INTO first_config FROM public.capacity_personal_membership_events e
              WHERE e.execution_epoch=NEW.execution_epoch AND e.subject_id=NEW.subject_id ORDER BY e.revision LIMIT 1;
            origin_reference := jsonb_build_object('schema_version',1,'scope','subject','subject_id',NEW.subject_id,
              'subject_incarnation',first_config -> 'subject_incarnation','generation',first_config -> 'configuration_generation',
              'digest',public.capacity_membership_json_digest(first_config));
          END IF;
          IF NOT public.capacity_personal_build_json_exact(proof,jsonb_build_object('schema_version',1,'namespace_id',NEW.namespace_id,
              'execution_manifest_sha256',NEW.execution_manifest_sha256,'origin',origin_reference,'predecessor',old_config,
              'predecessor_revision',prior_subject.revision,'predecessor_head_sha256',prior_subject.head_sha256,
              'admission_revision',NEW.revision,'successor_incarnation',NEW.subject_incarnation,
              'release_set_sha256',public.capacity_personal_predecessor_release_digest(NEW.subject_id,(old_config ->> 'subject_incarnation')::uuid))) THEN
            RAISE EXCEPTION 'typed recreation release certificate changed' USING ERRCODE='23514';
          END IF;
        END IF;
      ELSE
        IF NOT public.capacity_personal_build_json_exact(member -> 'reincarnation',coalesce(old_member -> 'reincarnation','null'::jsonb)) THEN
          RAISE EXCEPTION 'typed recreation certificate must be retained' USING ERRCODE='23514';
        END IF;
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
              AND ((e.subject_incarnation<>NEW.subject_incarnation AND nullif(member -> 'reincarnation','null'::jsonb) IS NULL) OR e.owner_id<>NEW.owner_id
                OR coalesce(e.request_payload #>> '{command,purpose}','personal-application')<>member_purpose
                OR e.result_payload #>> '{member,configuration,display_name}' IS DISTINCT FROM expected_config ->> 'display_name'))
         OR EXISTS (SELECT 1 FROM public.capacity_config_generations g WHERE
              (g.subject_id = NEW.subject_id OR g.subject_incarnation = NEW.subject_incarnation)
              AND (base_origin IS NULL OR g.subject_id IS DISTINCT FROM NEW.subject_id OR g.subject_incarnation IS DISTINCT FROM (base_config ->> 'subject_incarnation')::uuid
                OR g.payload ->> 'account_id' IS DISTINCT FROM derived_account_id OR g.payload ->> 'display_name' IS DISTINCT FROM expected_config ->> 'display_name'))
         OR EXISTS (SELECT 1 FROM public.capacity_subjects s WHERE
              (s.subject_id = NEW.subject_id AND ((s.subject_incarnation <> NEW.subject_incarnation AND
                  (nullif(member -> 'reincarnation','null'::jsonb) IS NULL OR s.subject_incarnation::text IS DISTINCT FROM base_config ->> 'subject_incarnation'))
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
        IF NOT EXISTS (SELECT 1 FROM public.capacity_demand_reporters r WHERE r.subject_id=NEW.subject_id AND r.subject_incarnation=(old_config ->> 'subject_incarnation')::uuid
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
    _install_release_proof_helpers()
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
    DROP FUNCTION public.capacity_personal_predecessor_release_digest(uuid,uuid);
    DROP FUNCTION public.capacity_personal_release_timestamp(timestamptz);
    DROP FUNCTION public.capacity_personal_release_json_digest(jsonb);
    DROP FUNCTION public.capacity_personal_release_json_text(jsonb);
    DROP FUNCTION public.capacity_personal_application_installation_matches(jsonb,jsonb);
    DROP FUNCTION public.capacity_personal_build_json_exact(jsonb,jsonb);
    DROP FUNCTION public.capacity_personal_build_subject_id(uuid,uuid);
    """)
    # Keep the standard extension and its schema: other database objects may
    # depend on them, including when the extension predated this migration.
