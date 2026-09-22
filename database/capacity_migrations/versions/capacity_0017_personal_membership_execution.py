"""Bind executable SQL guards to sealed personal membership authority.

Revision ID: capacity_0017
Revises: capacity_0016

SQL authenticates the exact immutable event/certificate chain. Physical release
witness recomputation remains in Python's CAS seal and membership-history checks.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0017"
down_revision: str | Sequence[str] | None = "capacity_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_GUARDS = (
    "capacity_executable_bootstrap_ack_insert_guard",
    "capacity_executable_admission_proposal_insert_guard",
    "capacity_executable_admission_ack_insert_guard",
    "capacity_executable_intent_protected_bootstrap_guard",
    "capacity_executable_admission_closure_ack_insert_guard",
    "capacity_executable_protected_release_insert_guard",
)


def _replace_once(value: str, old: str, new: str) -> str:
    if value.count(old) != 1:
        raise RuntimeError("capacity_0017 executable guard migration drift")
    return value.replace(old, new, 1)


def _replace_block(value: str, pattern: str, new: str) -> str:
    matches = list(re.finditer(pattern, value, re.DOTALL))
    if len(matches) != 1:
        raise RuntimeError("capacity_0017 executable guard block migration drift")
    match = matches[0]
    return value[: match.start()] + new + value[match.end() :]


def _install_helpers() -> None:
    op.execute(
        """
        CREATE FUNCTION public.capacity_membership_json_digest(value jsonb)
        RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog
        AS $$ SELECT encode(sha256(convert_to(
          public.capacity_executable_canonical_jsonb_text(value), 'UTF8')), 'hex') $$;

        CREATE FUNCTION public.capacity_membership_event_prefix(p_epoch bigint, p_revision bigint)
        RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        DECLARE
          epoch_record record; base_record record; event_record record;
          members jsonb := '{}'::jsonb; origins jsonb := '{}'::jsonb;
          used uuid[] := ARRAY[]::uuid[]; reference jsonb; member jsonb;
          prior jsonb; subject jsonb; proof jsonb; origin jsonb; acknowledgement jsonb; projection jsonb;
          previous_head text := repeat('0', 64); next_revision bigint := 1;
          identity text; incarnation uuid; calculated text;
        BEGIN
          IF p_revision IS NULL OR p_revision < 0 THEN
            RAISE EXCEPTION 'membership revision is invalid' USING ERRCODE = '23514';
          END IF;
          SELECT * INTO STRICT epoch_record FROM public.capacity_execution_epochs
            WHERE execution_epoch = p_epoch;
          IF epoch_record.manifest_payload -> 'schema_version' IS DISTINCT FROM '3'::jsonb
             OR public.capacity_membership_json_digest(epoch_record.manifest_payload)
                  IS DISTINCT FROM epoch_record.execution_manifest_sha256 THEN
            RAISE EXCEPTION 'membership execution manifest changed' USING ERRCODE = '23514';
          END IF;
          SELECT * INTO STRICT base_record FROM public.capacity_configuration_epochs
            WHERE configuration_epoch = epoch_record.configuration_epoch;
          FOR reference IN SELECT * FROM jsonb_array_elements(base_record.subject_generation_manifest)
          LOOP
            origins := origins || jsonb_build_object(reference ->> 'subject_id', reference);
            used := array_append(used, (reference ->> 'subject_incarnation')::uuid);
          END LOOP;
          FOR event_record IN SELECT * FROM public.capacity_personal_membership_events
            WHERE execution_epoch = p_epoch AND revision <= p_revision ORDER BY revision
          LOOP
            member := event_record.result_payload -> 'member';
            subject := member -> 'configuration';
            acknowledgement := member -> 'acknowledgement';
            projection := event_record.request_payload -> 'projection';
            identity := event_record.subject_id::text;
            incarnation := (subject ->> 'subject_incarnation')::uuid;
            prior := members -> identity;
            origin := origins -> identity;
            calculated := public.capacity_membership_json_digest(jsonb_build_object(
              'actor', event_record.actor, 'execution_epoch', event_record.execution_epoch,
              'idempotency_key', event_record.idempotency_key::text,
              'operation_id', event_record.operation_id::text,
              'previous_sha256', event_record.previous_sha256,
              'request_digest', event_record.request_digest,
              'request_payload', event_record.request_payload,
              'result_member', member, 'revision', event_record.revision));
            IF event_record.revision IS DISTINCT FROM next_revision
               OR event_record.previous_sha256 IS DISTINCT FROM previous_head
               OR event_record.head_sha256 IS DISTINCT FROM calculated
               OR event_record.execution_manifest_sha256
                    IS DISTINCT FROM epoch_record.execution_manifest_sha256
               OR event_record.namespace_id::text IS DISTINCT FROM
                    epoch_record.manifest_payload -> 'personal_membership' ->> 'namespace_id'
               OR event_record.actor IS DISTINCT FROM epoch_record.manifest_payload
                    -> 'personal_membership' ->> 'management_principal_id'
               OR public.capacity_membership_json_digest(event_record.request_payload)
                    IS DISTINCT FROM event_record.request_digest
               OR event_record.request_payload -> 'expected_revision' IS DISTINCT FROM to_jsonb(event_record.revision - 1)
               OR event_record.request_payload ->> 'namespace_id' IS DISTINCT FROM event_record.namespace_id::text
               OR event_record.request_payload -> 'execution' -> 'execution_epoch' IS DISTINCT FROM to_jsonb(p_epoch)
               OR event_record.request_payload -> 'execution' ->> 'execution_manifest_sha256' IS DISTINCT FROM event_record.execution_manifest_sha256
               OR event_record.request_payload -> 'execution' ->> 'authority_incarnation' IS DISTINCT FROM event_record.authority_incarnation::text
               OR event_record.request_payload -> 'execution' -> 'writer_epoch' IS DISTINCT FROM to_jsonb(event_record.writer_epoch)
               OR event_record.request_payload -> 'execution' ->> 'execution_state' IS DISTINCT FROM 'active'
               OR projection ->> 'operation_id' IS DISTINCT FROM event_record.operation_id::text
               OR projection ->> 'subject_id' IS DISTINCT FROM identity
               OR projection ->> 'subject_incarnation' IS DISTINCT FROM incarnation::text
               OR projection ->> 'owner_id' IS DISTINCT FROM event_record.owner_id::text
               OR projection -> 'configuration_generation' IS DISTINCT FROM to_jsonb(event_record.configuration_generation)
               OR projection -> 'deployment_generation' IS DISTINCT FROM to_jsonb(event_record.deployment_generation)
               OR projection ->> 'demand_reporter_incarnation' IS DISTINCT FROM event_record.reporter_incarnation::text
               OR event_record.result_payload -> 'revision' IS DISTINCT FROM to_jsonb(event_record.revision)
               OR event_record.result_payload -> 'replayed' IS DISTINCT FROM 'false'::jsonb
               OR event_record.result_payload ->> 'head_sha256'
                    IS DISTINCT FROM event_record.head_sha256
               OR member ->> 'owner_id' IS DISTINCT FROM event_record.owner_id::text
               OR member -> 'revision' IS DISTINCT FROM to_jsonb(event_record.revision)
               OR subject ->> 'subject_id' IS DISTINCT FROM identity
               OR incarnation IS DISTINCT FROM event_record.subject_incarnation
               OR subject -> 'configuration_generation'
                    IS DISTINCT FROM to_jsonb(event_record.configuration_generation)
               OR subject -> 'deployment_generation'
                    IS DISTINCT FROM to_jsonb(event_record.deployment_generation)
               OR subject -> 'candidate_generation' IS DISTINCT FROM projection -> 'candidate_generation'
               OR subject ->> 'display_name' IS DISTINCT FROM 'dev-' || (projection ->> 'environment_name')
               OR subject ->> 'account_id' IS DISTINCT FROM 'dev-owner-' || replace(event_record.owner_id::text, '-', '')
               OR subject -> 'min_slots' IS DISTINCT FROM (CASE WHEN projection ->> 'operation_kind' = 'destroy' THEN '0'::jsonb ELSE projection -> 'min_slots' END)
               OR subject -> 'max_slots' IS DISTINCT FROM (CASE WHEN projection ->> 'operation_kind' = 'destroy' THEN '0'::jsonb ELSE projection -> 'max_slots' END)
               OR subject ->> 'lifecycle_state' IS DISTINCT FROM (CASE WHEN projection ->> 'operation_kind' = 'destroy' THEN 'disabled' ELSE 'active' END)
               OR subject ->> 'demand_reporter_incarnation'
                    IS DISTINCT FROM event_record.reporter_incarnation::text
               OR acknowledgement IS DISTINCT FROM event_record.request_payload -> 'acknowledgement'
               OR acknowledgement ->> 'subject_id' IS DISTINCT FROM identity
               OR acknowledgement ->> 'subject_incarnation' IS DISTINCT FROM incarnation::text
               OR acknowledgement -> 'configuration_generation'
                    IS DISTINCT FROM subject -> 'configuration_generation'
               OR acknowledgement -> 'deployment_generation'
                    IS DISTINCT FROM subject -> 'deployment_generation'
               OR acknowledgement ->> 'reporter_incarnation'
                    IS DISTINCT FROM subject ->> 'demand_reporter_incarnation'
               OR acknowledgement -> 'candidate' ->> 'algorithm' IS DISTINCT FROM 'source-sha256'
               OR acknowledgement -> 'candidate' -> 'identity' IS DISTINCT FROM projection -> 'candidate_sha256'
               OR acknowledgement -> 'candidate' -> 'publication_sha256' IS DISTINCT FROM projection -> 'candidate_publication_sha256'
               OR acknowledgement -> 'protected_admission_sha256' IS DISTINCT FROM projection -> 'protected_admission_sha256' THEN
              RAISE EXCEPTION 'membership event prefix changed' USING ERRCODE = '23514';
            END IF;
            IF origin IS NULL THEN
              origin := jsonb_build_object('schema_version', 1, 'scope', 'subject',
                'generation', subject -> 'configuration_generation',
                'digest', public.capacity_membership_json_digest(subject),
                'subject_id', identity, 'subject_incarnation', incarnation::text);
              origins := origins || jsonb_build_object(identity, origin);
            END IF;
            proof := nullif(member -> 'reincarnation', 'null'::jsonb);
            IF prior IS NULL THEN
              IF proof IS NOT NULL THEN
                RAISE EXCEPTION 'membership predecessor is absent' USING ERRCODE = '23514';
              END IF;
            ELSE
              IF member -> 'owner_id' IS DISTINCT FROM prior -> 'owner_id'
                 OR subject -> 'display_name' IS DISTINCT FROM prior -> 'configuration' -> 'display_name'
                 OR (subject ->> 'configuration_generation')::bigint
                      <= (prior -> 'configuration' ->> 'configuration_generation')::bigint THEN
                RAISE EXCEPTION 'membership historical identity changed' USING ERRCODE = '23514';
              END IF;
              IF subject -> 'subject_incarnation'
                   IS DISTINCT FROM prior -> 'configuration' -> 'subject_incarnation' THEN
                IF incarnation = ANY(used) OR proof IS NULL
                   OR proof -> 'origin' IS DISTINCT FROM origin
                   OR proof -> 'predecessor' IS DISTINCT FROM prior -> 'configuration'
                   OR proof -> 'predecessor_revision' IS DISTINCT FROM prior -> 'revision'
                   OR proof -> 'admission_revision' IS DISTINCT FROM to_jsonb(event_record.revision)
                   OR proof ->> 'successor_incarnation' IS DISTINCT FROM incarnation::text
                   OR proof ->> 'namespace_id' IS DISTINCT FROM event_record.namespace_id::text
                   OR proof ->> 'execution_manifest_sha256'
                        IS DISTINCT FROM event_record.execution_manifest_sha256
                   OR proof ->> 'release_set_sha256' IS NULL
                   OR proof ->> 'release_set_sha256' !~ '^[0-9a-f]{64}$'
                   OR proof ->> 'release_set_sha256' = repeat('0', 64)
                   OR proof ->> 'predecessor_head_sha256' IS DISTINCT FROM (
                        SELECT head_sha256 FROM public.capacity_personal_membership_events
                        WHERE execution_epoch = p_epoch AND revision = (prior ->> 'revision')::bigint)
                   OR prior -> 'configuration' ->> 'lifecycle_state' IS DISTINCT FROM 'disabled'
                   OR prior -> 'configuration' -> 'min_slots' IS DISTINCT FROM '0'::jsonb
                   OR prior -> 'configuration' -> 'max_slots' IS DISTINCT FROM '0'::jsonb
                   OR subject -> 'candidate_generation' IS DISTINCT FROM '1'::jsonb
                   OR subject -> 'deployment_generation' IS DISTINCT FROM '1'::jsonb
                   OR event_record.request_payload -> 'projection' ->> 'operation_kind'
                        IS DISTINCT FROM 'create' THEN
                  RAISE EXCEPTION 'membership reincarnation chain changed' USING ERRCODE = '23514';
                END IF;
              ELSIF proof IS DISTINCT FROM nullif(prior -> 'reincarnation', 'null'::jsonb)
                 OR prior -> 'configuration' ->> 'lifecycle_state' = 'disabled' THEN
                RAISE EXCEPTION 'membership recreation evidence changed' USING ERRCODE = '23514';
              END IF;
            END IF;
            members := members || jsonb_build_object(identity, member);
            used := array_append(used, incarnation);
            previous_head := event_record.head_sha256;
            next_revision := next_revision + 1;
          END LOOP;
          IF next_revision - 1 <> p_revision THEN
            RAISE EXCEPTION 'membership pinned revision is absent' USING ERRCODE = '23514';
          END IF;
          RETURN jsonb_build_object('members', members, 'head_sha256', previous_head);
        END $$;

        CREATE FUNCTION public.capacity_membership_pinned_subject(
          p_allocation bigint, p_subject uuid, p_incarnation uuid)
        RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        DECLARE
          allocation_record record; epoch_record record; base_record record;
          snapshot jsonb; prefix jsonb; expected_members jsonb; member jsonb;
          reference jsonb; subject jsonb; acknowledgement jsonb; delegated boolean;
        BEGIN
          PERFORM 1 FROM public.capacity_authority_state WHERE singleton_id = 1 FOR SHARE;
          SELECT * INTO STRICT allocation_record FROM public.capacity_allocation_epochs
            WHERE allocation_epoch = p_allocation;
          SELECT * INTO STRICT epoch_record FROM public.capacity_execution_epochs
            WHERE execution_epoch = allocation_record.execution_epoch;
          SELECT * INTO STRICT base_record FROM public.capacity_configuration_epochs
            WHERE configuration_epoch = epoch_record.configuration_epoch;
          delegated := epoch_record.manifest_payload -> 'schema_version' = '3'::jsonb;
          IF allocation_record.status IS DISTINCT FROM 'executable'
             OR delegated IS NULL
             OR NOT allocation_record.executable OR NOT allocation_record.sealed
             OR allocation_record.configuration_epoch IS DISTINCT FROM epoch_record.configuration_epoch
             OR allocation_record.execution_manifest_sha256 IS DISTINCT FROM epoch_record.execution_manifest_sha256
             OR public.capacity_membership_json_digest(epoch_record.manifest_payload)
                  IS DISTINCT FROM epoch_record.execution_manifest_sha256
             OR allocation_record.complete_payload -> 'execution' -> 'allocation_epoch'
                  IS DISTINCT FROM to_jsonb(p_allocation)
             OR allocation_record.complete_payload -> 'execution' -> 'execution_epoch'
                  IS DISTINCT FROM to_jsonb(epoch_record.execution_epoch)
             OR allocation_record.complete_payload -> 'execution' ->> 'execution_manifest_sha256'
                  IS DISTINCT FROM epoch_record.execution_manifest_sha256
             OR allocation_record.complete_payload -> 'execution' -> 'writer_epoch'
                  IS DISTINCT FROM to_jsonb(allocation_record.writer_epoch)
             OR allocation_record.complete_payload -> 'execution' -> 'configuration_epoch'
                  IS DISTINCT FROM to_jsonb(epoch_record.configuration_epoch)
             OR allocation_record.complete_payload -> 'execution' ->> 'authority_incarnation'
                  IS DISTINCT FROM epoch_record.authority_incarnation::text
             OR allocation_record.complete_payload -> 'execution' ->> 'trusted_fleet_release_sha256'
                  IS DISTINCT FROM epoch_record.trusted_fleet_release_sha256
             OR (allocation_record.complete_payload -> 'execution' ->> 'executable_new_capacity_ceiling')::bigint
                  > epoch_record.requested_ceiling
             OR (allocation_record.complete_payload -> 'execution' ->> 'executable_new_capacity_rate_per_minute')::bigint
                  > epoch_record.requested_rate_per_minute
             OR jsonb_array_length(allocation_record.complete_payload -> 'allocations')
                  IS DISTINCT FROM allocation_record.allocation_count
             OR public.capacity_membership_json_digest(allocation_record.complete_payload -> 'configuration')
                  IS DISTINCT FROM base_record.canonical_digest
             OR allocation_record.complete_payload -> 'configuration' -> 'subjects'
                  IS DISTINCT FROM base_record.subject_generation_manifest
             OR allocation_record.complete_payload -> 'configuration' -> 'fleet' -> 'generation'
                  IS DISTINCT FROM to_jsonb(epoch_record.fleet_generation)
             OR allocation_record.complete_payload -> 'configuration' -> 'fleet' ->> 'digest'
                  IS DISTINCT FROM epoch_record.fleet_digest
             OR allocation_record.complete_payload ->> 'input_digest' IS DISTINCT FROM allocation_record.input_digest
             OR (delegated AND allocation_record.complete_payload -> 'schema_version' IS DISTINCT FROM '3'::jsonb)
             OR (NOT delegated AND (epoch_record.manifest_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
                   OR allocation_record.complete_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
                   OR allocation_record.complete_payload ? 'membership')) THEN
            RAISE EXCEPTION 'membership allocation authority changed' USING ERRCODE = '23514';
          END IF;
          IF delegated THEN
            snapshot := allocation_record.complete_payload -> 'membership';
            IF snapshot -> 'schema_version' IS DISTINCT FROM '1'::jsonb
               OR snapshot -> 'namespace_id' IS DISTINCT FROM epoch_record.manifest_payload -> 'personal_membership' -> 'namespace_id'
               OR jsonb_typeof(snapshot -> 'revision') IS DISTINCT FROM 'number'
               OR snapshot ->> 'revision' !~ '^(0|[1-9][0-9]*)$'
               OR jsonb_typeof(snapshot -> 'members') IS DISTINCT FROM 'array'
               OR snapshot - ARRAY['schema_version','namespace_id','revision','head_sha256','members'] IS DISTINCT FROM '{}'::jsonb THEN
              RAISE EXCEPTION 'membership allocation snapshot changed' USING ERRCODE = '23514';
            END IF;
            prefix := public.capacity_membership_event_prefix(epoch_record.execution_epoch, (snapshot ->> 'revision')::bigint);
            SELECT coalesce(jsonb_agg(value ORDER BY (value ->> 'revision')::bigint), '[]'::jsonb)
              INTO expected_members FROM jsonb_each(prefix -> 'members');
            IF snapshot -> 'members' IS DISTINCT FROM expected_members
               OR snapshot -> 'head_sha256' IS DISTINCT FROM prefix -> 'head_sha256' THEN
              RAISE EXCEPTION 'membership allocation prefix changed' USING ERRCODE = '23514';
            END IF;
            member := prefix -> 'members' -> p_subject::text;
          END IF;
          SELECT value INTO reference FROM jsonb_array_elements(base_record.subject_generation_manifest)
            WHERE value ->> 'subject_id' = p_subject::text;
          IF member IS NOT NULL THEN
            subject := member -> 'configuration'; acknowledgement := member -> 'acknowledgement';
            IF reference IS NOT NULL AND NOT (epoch_record.manifest_payload -> 'personal_membership' -> 'managed_base_subject_ids' @> to_jsonb(ARRAY[p_subject::text])) THEN
              RAISE EXCEPTION 'membership static subject override' USING ERRCODE = '23514';
            END IF;
          ELSE
            SELECT payload INTO subject FROM public.capacity_config_generations
              WHERE scope = 'subject' AND subject_id = p_subject
                AND subject_incarnation = (reference ->> 'subject_incarnation')::uuid
                AND scope_generation = (reference ->> 'generation')::bigint
                AND digest = reference ->> 'digest';
            IF reference IS NULL OR subject IS NULL
               OR public.capacity_membership_json_digest(subject) IS DISTINCT FROM reference ->> 'digest' THEN
              RAISE EXCEPTION 'membership base reference changed' USING ERRCODE = '23514';
            END IF;
            SELECT value INTO acknowledgement FROM jsonb_array_elements(epoch_record.manifest_payload -> 'subject_acknowledgements')
              WHERE value ->> 'subject_id' = p_subject::text;
          END IF;
          IF subject ->> 'subject_id' IS DISTINCT FROM p_subject::text
             OR subject ->> 'subject_incarnation' IS DISTINCT FROM p_incarnation::text
             OR acknowledgement ->> 'subject_id' IS DISTINCT FROM p_subject::text
             OR acknowledgement ->> 'subject_incarnation' IS DISTINCT FROM p_incarnation::text
             OR acknowledgement -> 'configuration_generation' IS DISTINCT FROM subject -> 'configuration_generation'
             OR acknowledgement -> 'deployment_generation' IS DISTINCT FROM subject -> 'deployment_generation'
             OR acknowledgement -> 'reporter_incarnation' IS DISTINCT FROM subject -> 'demand_reporter_incarnation' THEN
            RAISE EXCEPTION 'membership pinned subject identity changed' USING ERRCODE = '23514';
          END IF;
          RETURN jsonb_build_object('configuration', subject, 'acknowledgement', acknowledgement,
            'delegated', delegated, 'execution_epoch', epoch_record.execution_epoch,
            'execution_manifest_sha256', epoch_record.execution_manifest_sha256,
            'configuration_epoch', epoch_record.configuration_epoch);
        END $$;

        CREATE FUNCTION public.capacity_membership_target_current(p_allocation bigint, p_subject uuid, p_incarnation uuid)
        RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        DECLARE pinned jsonb; prefix jsonb; latest_revision bigint; current_subject jsonb;
          current_ack jsonb; materialized record; member jsonb; projection jsonb;
          candidate_record record; reporter_record record; deployment_record record;
          profile jsonb; profile_record record; current_incarnation uuid;
        BEGIN
          pinned := public.capacity_membership_pinned_subject(p_allocation, p_subject, p_incarnation);
          current_subject := pinned -> 'configuration'; current_ack := pinned -> 'acknowledgement';
          IF (pinned ->> 'delegated')::boolean THEN
            SELECT coalesce(max(revision), 0) INTO latest_revision FROM public.capacity_personal_membership_events
              WHERE execution_epoch = (pinned ->> 'execution_epoch')::bigint;
            prefix := public.capacity_membership_event_prefix((pinned ->> 'execution_epoch')::bigint, latest_revision);
            member := prefix -> 'members' -> p_subject::text;
            IF member IS NOT NULL THEN
              current_subject := member -> 'configuration'; current_ack := member -> 'acknowledgement';
            END IF;
          END IF;
          SELECT * INTO STRICT materialized FROM public.capacity_subjects
            WHERE configuration_epoch = (pinned ->> 'configuration_epoch')::bigint AND subject_id = p_subject;
          IF materialized.payload IS DISTINCT FROM current_subject
             OR materialized.account_id IS DISTINCT FROM current_subject ->> 'account_id'
             OR materialized.display_name IS DISTINCT FROM current_subject ->> 'display_name'
             OR materialized.tier_id IS DISTINCT FROM current_subject ->> 'tier_id'
             OR to_jsonb(materialized.min_slots) IS DISTINCT FROM current_subject -> 'min_slots'
             OR to_jsonb(materialized.max_slots) IS DISTINCT FROM current_subject -> 'max_slots'
             OR to_jsonb(materialized.rollout_surge_slots) IS DISTINCT FROM current_subject -> 'rollout_surge_slots'
             OR to_jsonb(materialized.max_pending_slots) IS DISTINCT FROM current_subject -> 'max_pending_slots'
             OR to_jsonb(materialized.max_pending_jobs) IS DISTINCT FROM current_subject -> 'max_pending_jobs'
             OR to_jsonb(materialized.submission_rate_per_minute) IS DISTINCT FROM current_subject -> 'submission_rate_per_minute'
             OR to_jsonb(materialized.deployment_generation) IS DISTINCT FROM current_subject -> 'deployment_generation'
             OR to_jsonb(materialized.candidate_generation) IS DISTINCT FROM current_subject -> 'candidate_generation'
             OR materialized.subject_incarnation::text IS DISTINCT FROM current_subject ->> 'subject_incarnation'
             OR to_jsonb(materialized.configuration_generation) IS DISTINCT FROM current_subject -> 'configuration_generation'
             OR materialized.demand_reporter_incarnation::text IS DISTINCT FROM current_subject ->> 'demand_reporter_incarnation'
             OR materialized.lifecycle_state IS DISTINCT FROM current_subject ->> 'lifecycle_state' THEN
            RAISE EXCEPTION 'membership current materialization changed' USING ERRCODE = '23514';
          END IF;
          -- Delegate only the new V3 evidence policy; legacy V2 callers retain
          -- their existing current-reporter and operation-specific checks.
          IF (pinned ->> 'delegated')::boolean THEN
            current_incarnation := (current_subject ->> 'subject_incarnation')::uuid;
            SELECT * INTO candidate_record FROM public.capacity_candidates
              WHERE subject_id = p_subject AND subject_incarnation = current_incarnation
                AND candidate_generation = (current_subject ->> 'candidate_generation')::bigint;
            IF NOT FOUND
               OR candidate_record.candidate_identity_algorithm IS DISTINCT FROM current_ack -> 'candidate' ->> 'algorithm'
               OR candidate_record.candidate_identity IS DISTINCT FROM current_ack -> 'candidate' ->> 'identity'
               OR candidate_record.source_payload -> 'publication_sha256' IS DISTINCT FROM current_ack -> 'candidate' -> 'publication_sha256' THEN
              RAISE EXCEPTION 'membership current candidate evidence changed' USING ERRCODE = '23514';
            END IF;
            SELECT * INTO reporter_record FROM public.capacity_demand_reporters
              WHERE subject_id = p_subject AND subject_incarnation = current_incarnation
                AND reporter_incarnation = (current_ack ->> 'reporter_incarnation')::uuid;
            IF NOT FOUND OR reporter_record.state IS DISTINCT FROM 'current'
               OR to_jsonb(reporter_record.configuration_generation) IS DISTINCT FROM current_subject -> 'configuration_generation'
               OR to_jsonb(reporter_record.deployment_generation) IS DISTINCT FROM current_subject -> 'deployment_generation' THEN
              RAISE EXCEPTION 'membership current reporter evidence changed' USING ERRCODE = '23514';
            END IF;
            SELECT * INTO deployment_record FROM public.capacity_deployment_generations
              WHERE subject_id = p_subject AND subject_incarnation = current_incarnation
                AND deployment_generation = (current_subject ->> 'deployment_generation')::bigint;
            IF (NOT FOUND AND member IS NOT NULL)
               OR (FOUND AND (deployment_record.candidate_digest IS DISTINCT FROM candidate_record.candidate_digest
                 OR deployment_record.required_profiles IS DISTINCT FROM current_subject -> 'profiles'
                 OR deployment_record.readiness_state IS DISTINCT FROM 'ready'
                 OR deployment_record.lifecycle_state IS DISTINCT FROM 'active')) THEN
              RAISE EXCEPTION 'membership current deployment evidence changed' USING ERRCODE = '23514';
            END IF;
            FOR profile IN SELECT * FROM jsonb_array_elements(current_subject -> 'profiles')
            LOOP
              SELECT * INTO profile_record FROM public.capacity_worker_profiles
                WHERE subject_id = p_subject AND subject_incarnation = current_incarnation
                  AND deployment_generation = (current_subject ->> 'deployment_generation')::bigint
                  AND pool_id = profile ->> 'pool_id'
                  AND profile_generation = (profile ->> 'profile_generation')::bigint;
              IF NOT FOUND
                 OR to_jsonb(profile_record.pool_generation) IS DISTINCT FROM profile -> 'pool_generation'
                 OR profile_record.profile_digest IS DISTINCT FROM profile ->> 'profile_digest'
                 OR profile_record.shape_catalog IS DISTINCT FROM profile -> 'worker_shapes'
                 OR profile_record.narrowing_constraints IS DISTINCT FROM jsonb_build_object(
                      'eligible_resource_domains', profile -> 'eligible_resource_domains') THEN
                RAISE EXCEPTION 'membership current profile evidence changed' USING ERRCODE = '23514';
              END IF;
            END LOOP;
            IF member IS NOT NULL THEN
              -- The prefix above authenticates this exact event, including its
              -- request digest. Bind mutable retained installation evidence to it.
              SELECT request_payload -> 'projection' INTO STRICT projection
                FROM public.capacity_personal_membership_events
                WHERE execution_epoch = (pinned ->> 'execution_epoch')::bigint
                  AND revision = (member ->> 'revision')::bigint;
              IF candidate_record.candidate_digest IS DISTINCT FROM projection ->> 'candidate_sha256'
                 OR candidate_record.candidate_identity_algorithm IS DISTINCT FROM 'source-sha256'
                 OR candidate_record.candidate_identity IS DISTINCT FROM projection ->> 'candidate_sha256'
                 OR candidate_record.source_payload IS DISTINCT FROM jsonb_build_object('publication_sha256', projection -> 'candidate_publication_sha256')
                 OR candidate_record.artifact_payload IS DISTINCT FROM jsonb_build_object('candidate_sha256', projection -> 'candidate_sha256')
                 OR candidate_record.architecture_payload IS DISTINCT FROM jsonb_build_object(
                      'supported_architectures', projection -> 'supported_architectures',
                      'supported_pool_ids', projection -> 'supported_pool_ids')
                 OR candidate_record.launcher_payload IS DISTINCT FROM jsonb_build_object('local_activation_sha256', projection -> 'local_activation_sha256')
                 OR candidate_record.protocol_payload IS DISTINCT FROM projection -> 'protocol_versions'
                 OR reporter_record.token_sha256 IS DISTINCT FROM projection ->> 'demand_reporter_token_sha256'
                 OR deployment_record.cutover_payload IS DISTINCT FROM jsonb_build_object(
                      'local_activation_sha256', projection -> 'local_activation_sha256',
                      'candidate_publication_sha256', projection -> 'candidate_publication_sha256',
                      'protected_admission_sha256', projection -> 'protected_admission_sha256',
                      'capacity_agent_installation_sha256', projection -> 'capacity_agent_installation_sha256') THEN
                RAISE EXCEPTION 'membership current retained evidence changed' USING ERRCODE = '23514';
              END IF;
            END IF;
          END IF;
          RETURN current_subject = pinned -> 'configuration' AND current_ack = pinned -> 'acknowledgement'
            AND current_subject ->> 'lifecycle_state' = 'active'
            AND EXISTS (SELECT 1 FROM public.capacity_demand_reporters
              WHERE subject_id = p_subject AND subject_incarnation = p_incarnation
                AND reporter_incarnation = (current_ack ->> 'reporter_incarnation')::uuid
                AND state = 'current');
        END $$;

        CREATE FUNCTION public.capacity_membership_binding_target(binding jsonb)
        RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        DECLARE pinned jsonb; acknowledgement jsonb;
        BEGIN
          pinned := public.capacity_membership_pinned_subject((binding -> 'execution' ->> 'allocation_epoch')::bigint,
            (binding ->> 'subject_id')::uuid, (binding ->> 'subject_incarnation')::uuid);
          acknowledgement := pinned -> 'acknowledgement';
          IF pinned -> 'execution_epoch' IS DISTINCT FROM binding -> 'execution' -> 'execution_epoch'
             OR pinned -> 'configuration_epoch' IS DISTINCT FROM binding -> 'execution' -> 'configuration_epoch'
             OR pinned -> 'execution_manifest_sha256' IS DISTINCT FROM binding -> 'execution' -> 'execution_manifest_sha256'
             OR acknowledgement -> 'deployment_generation' IS DISTINCT FROM binding -> 'deployment_generation'
             OR acknowledgement -> 'candidate' IS DISTINCT FROM binding -> 'candidate' THEN
            RAISE EXCEPTION 'membership intent target binding changed' USING ERRCODE = '23514';
          END IF;
          RETURN pinned;
        END $$;

        CREATE FUNCTION public.capacity_membership_cleanup_reporter(binding jsonb, p_reporter uuid)
        RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        DECLARE pinned jsonb; reporter_record record; prefix jsonb; latest_revision bigint;
          member jsonb; acknowledged jsonb;
        BEGIN
          IF EXISTS (SELECT 1 FROM public.capacity_execution_epochs AS epoch
            WHERE epoch.execution_epoch = (binding -> 'execution' ->> 'execution_epoch')::bigint
              AND epoch.manifest_payload -> 'schema_version' = '2'::jsonb) THEN
            RETURN EXISTS (SELECT 1 FROM public.capacity_demand_reporters AS reporter
              WHERE reporter.subject_id = (binding ->> 'subject_id')::uuid
                AND reporter.subject_incarnation = (binding ->> 'subject_incarnation')::uuid
                AND reporter.reporter_incarnation = p_reporter AND reporter.state = 'current');
          END IF;
          pinned := public.capacity_membership_binding_target(binding);
          IF pinned -> 'acknowledgement' ->> 'reporter_incarnation' IS DISTINCT FROM p_reporter::text THEN
            RETURN false;
          END IF;
          SELECT * INTO reporter_record FROM public.capacity_demand_reporters
            WHERE subject_id = (binding ->> 'subject_id')::uuid
              AND subject_incarnation = (binding ->> 'subject_incarnation')::uuid
              AND reporter_incarnation = p_reporter;
          IF NOT FOUND OR reporter_record.state NOT IN ('current', 'fenced') THEN RETURN false; END IF;
          IF to_jsonb(reporter_record.deployment_generation) IS DISTINCT FROM pinned -> 'configuration' -> 'deployment_generation'
             OR reporter_record.configuration_generation < (pinned -> 'configuration' ->> 'configuration_generation')::bigint THEN
            RETURN false;
          END IF;
          SELECT coalesce(max(revision), 0) INTO latest_revision FROM public.capacity_personal_membership_events
            WHERE execution_epoch = (pinned ->> 'execution_epoch')::bigint;
          prefix := public.capacity_membership_event_prefix((pinned ->> 'execution_epoch')::bigint, latest_revision);
          -- Authenticate the reporter row's recorded generation, not only the
          -- allocation's old pin. Capacity changes may legitimately retain it.
          IF NOT EXISTS (
            SELECT 1 FROM (
              SELECT result_payload -> 'member' -> 'acknowledgement' AS acknowledgement
                FROM public.capacity_personal_membership_events
                WHERE execution_epoch = (pinned ->> 'execution_epoch')::bigint
                  AND revision <= latest_revision
              UNION ALL
              SELECT value FROM public.capacity_execution_epochs AS epoch,
                LATERAL jsonb_array_elements(epoch.manifest_payload -> 'subject_acknowledgements')
                WHERE epoch.execution_epoch = (pinned ->> 'execution_epoch')::bigint
            ) AS recorded
            WHERE recorded.acknowledgement ->> 'subject_id' = binding ->> 'subject_id'
              AND recorded.acknowledgement ->> 'subject_incarnation' = binding ->> 'subject_incarnation'
              AND recorded.acknowledgement -> 'configuration_generation' = to_jsonb(reporter_record.configuration_generation)
              AND recorded.acknowledgement -> 'deployment_generation' = to_jsonb(reporter_record.deployment_generation)
              AND recorded.acknowledgement ->> 'reporter_incarnation' = p_reporter::text
          ) THEN RETURN false; END IF;
          IF reporter_record.state = 'current' THEN RETURN true; END IF;
          IF NOT (pinned ->> 'delegated')::boolean OR reporter_record.token_sha256 IS NULL THEN RETURN false; END IF;
          member := prefix -> 'members' -> (binding ->> 'subject_id');
          IF member IS NULL THEN RETURN false; END IF;
          acknowledged := member -> 'configuration';
          RETURN (acknowledged ->> 'configuration_generation')::bigint
                   > (pinned -> 'configuration' ->> 'configuration_generation')::bigint
            AND acknowledged -> 'demand_reporter_incarnation' IS DISTINCT FROM pinned -> 'configuration' -> 'demand_reporter_incarnation';
        END $$;
        """
    )
    for signature in (
        "capacity_membership_json_digest(jsonb)",
        "capacity_membership_event_prefix(bigint,bigint)",
        "capacity_membership_pinned_subject(bigint,uuid,uuid)",
        "capacity_membership_target_current(bigint,uuid,uuid)",
        "capacity_membership_binding_target(jsonb)",
        "capacity_membership_cleanup_reporter(jsonb,uuid)",
    ):
        op.execute(f"REVOKE EXECUTE ON FUNCTION public.{signature} FROM PUBLIC")


def _patch_guards() -> None:
    for index, name in enumerate(_GUARDS):
        definition = (
            op.get_bind()
            .execute(
                sa.text("SELECT pg_get_functiondef(CAST(:name AS regprocedure))"),
                {"name": f"public.{name}()"},
            )
            .scalar_one()
        )
        # Preserve the exact installed functions for reversible upgrades. These
        # return trigger, cannot be called as ordinary SQL, and have no PUBLIC ACL.
        backup = f"capacity_0017_prior_guard_{index}"
        op.execute(
            _replace_once(definition, f"FUNCTION public.{name}()", f"FUNCTION public.{backup}()")
        )
        op.execute(f"REVOKE EXECUTE ON FUNCTION public.{backup}() FROM PUBLIC")
        if name == _GUARDS[0]:
            definition = _replace_once(
                definition,
                "AND reporter.state = 'current'",
                "AND public.capacity_membership_cleanup_reporter(intent_binding, NEW.reporter_incarnation)",
            )
            definition = _replace_block(
                definition,
                r"          PERFORM 1\n            FROM public.capacity_execution_epochs AS epoch,.*?RAISE EXCEPTION 'executable bootstrap acknowledgement protected admission changed'\n              USING ERRCODE = '23514';\n          END IF;",
                """          IF public.capacity_membership_binding_target(intent_binding) -> 'acknowledgement' ->> 'protected_admission_sha256'
               IS DISTINCT FROM NEW.protected_admission_sha256 THEN
            RAISE EXCEPTION 'executable bootstrap acknowledgement protected admission changed' USING ERRCODE = '23514';
          END IF;""",
            )
        elif name == _GUARDS[1]:
            definition = _replace_block(
                definition,
                r"          PERFORM 1\n            FROM public.capacity_execution_epochs AS epoch,.*?RAISE EXCEPTION 'executable admission protected authority changed'\n              USING ERRCODE = '23514';\n          END IF;",
                """          IF public.capacity_membership_binding_target(first_binding) -> 'acknowledgement' ->> 'protected_admission_sha256'
                 IS DISTINCT FROM NEW.protected_admission_sha256
             OR public.capacity_membership_binding_target(first_binding) -> 'acknowledgement' ->> 'reporter_incarnation'
                 IS DISTINCT FROM NEW.reporter_incarnation::text
             OR public.capacity_membership_target_current(NEW.allocation_epoch, NEW.subject_id, NEW.subject_incarnation) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'executable admission protected authority changed' USING ERRCODE = '23514';
          END IF;""",
            )
        elif name == _GUARDS[2]:
            definition = _replace_once(
                definition,
                "          RETURN NEW;",
                """          IF public.capacity_membership_target_current(NEW.allocation_epoch, NEW.subject_id, NEW.subject_incarnation) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'executable admission target superseded' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;""",
            )
        elif name == _GUARDS[3]:
            anchor = "          RETURN NEW;"
            definition = _replace_once(
                definition,
                anchor,
                """          IF NEW.state = 'launch-ready' AND OLD.state IS DISTINCT FROM NEW.state
             AND public.capacity_membership_target_current(NEW.allocation_epoch, NEW.subject_id, NEW.subject_incarnation) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'executable launch readiness target superseded' USING ERRCODE = '23514';
          END IF;
"""
                + anchor,
            )
        elif name == _GUARDS[4]:
            definition = _replace_once(
                definition,
                "AND reporter.state = 'current'",
                "AND public.capacity_membership_cleanup_reporter(proposal_record.proposal_payload -> 'shapes' -> 0 -> 'binding', NEW.reporter_incarnation)",
            )
            definition = _replace_once(
                definition,
                "OR (NEW.close_reason = 'allocation-superseded'\n                 AND EXISTS (",
                """OR (NEW.close_reason = 'allocation-superseded'
                 AND CASE WHEN EXISTS (SELECT 1 FROM public.capacity_execution_epochs AS epoch
                   WHERE epoch.execution_epoch = proposal_record.execution_epoch
                     AND epoch.manifest_payload -> 'schema_version' = '3'::jsonb)
                   THEN public.capacity_membership_target_current(proposal_record.allocation_epoch,
                     proposal_record.subject_id, proposal_record.subject_incarnation)
                   ELSE true END
                 AND EXISTS (""",
            )
        else:
            definition = _replace_once(
                definition,
                "AND reporter.state = 'current'",
                "AND public.capacity_membership_cleanup_reporter(intent_binding, NEW.reporter_incarnation)",
            )
            anchor = "          expected_release_payload := pg_catalog.jsonb_build_object("
            definition = _replace_once(
                definition,
                anchor,
                """          IF NOT EXISTS (SELECT 1 FROM public.capacity_executable_bootstrap_acknowledgements AS bootstrap
              WHERE bootstrap.intent_id = NEW.intent_id
                AND bootstrap.reporter_incarnation = NEW.reporter_incarnation
                AND bootstrap.bootstrap_registration_epoch = NEW.bootstrap_registration_epoch) THEN
            RAISE EXCEPTION 'protected release pinned bootstrap reporter changed' USING ERRCODE = '23514';
          END IF;
"""
                + anchor,
            )
        op.execute(definition)


def upgrade() -> None:
    op.execute("LOCK TABLE public.capacity_authority_state IN EXCLUSIVE MODE")
    _install_helpers()
    _patch_guards()
    op.execute(
        """
        CREATE FUNCTION public.capacity_membership_intent_increase_guard()
        RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
          -- This companion adds delegated authority checks only. Preserve the
          -- existing V2 state-machine guards and reject mixed V2/V3 below.
          IF EXISTS (
            SELECT 1 FROM public.capacity_execution_epochs AS epoch
            JOIN public.capacity_allocation_epochs AS allocation
              ON allocation.execution_epoch = epoch.execution_epoch
            WHERE epoch.execution_epoch = NEW.execution_epoch
              AND allocation.allocation_epoch = NEW.allocation_epoch
              AND epoch.manifest_payload -> 'schema_version' = '2'::jsonb
              AND allocation.complete_payload -> 'schema_version' = '2'::jsonb
          ) THEN RETURN NEW; END IF;
          IF TG_OP = 'INSERT' OR (NEW.state IS DISTINCT FROM OLD.state
             AND NEW.state IN ('accepted','launch-ready','permitted','submitting-unknown'))
             OR (NEW.state = 'permitted' AND ROW(NEW.permit_id, NEW.permit_epoch,
                   NEW.permit_digest, NEW.permit_payload, NEW.permit_expires_at)
                 IS DISTINCT FROM ROW(OLD.permit_id, OLD.permit_epoch,
                   OLD.permit_digest, OLD.permit_payload, OLD.permit_expires_at)) THEN
            PERFORM 1 FROM public.capacity_authority_state WHERE singleton_id = 1 FOR SHARE;
            PERFORM public.capacity_membership_binding_target(NEW.binding_payload);
            IF public.capacity_membership_target_current(NEW.allocation_epoch, NEW.subject_id, NEW.subject_incarnation) IS DISTINCT FROM true THEN
              RAISE EXCEPTION 'executable intent target superseded' USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NEW;
        END $$;
        REVOKE EXECUTE ON FUNCTION public.capacity_membership_intent_increase_guard() FROM PUBLIC;
        CREATE TRIGGER capacity_membership_intent_increase_guard BEFORE INSERT OR UPDATE
          ON public.capacity_executable_intents FOR EACH ROW
          EXECUTE FUNCTION public.capacity_membership_intent_increase_guard();
        """
    )


def downgrade() -> None:
    op.execute("LOCK TABLE public.capacity_authority_state IN EXCLUSIVE MODE")
    if (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM public.capacity_allocation_epochs "
                "WHERE complete_payload -> 'schema_version' = '3'::jsonb)"
            )
        )
        .scalar_one()
    ):
        raise RuntimeError("cannot downgrade capacity_0017 while delegated allocations exist")
    op.execute(
        "DROP TRIGGER capacity_membership_intent_increase_guard ON public.capacity_executable_intents"
    )
    op.execute("DROP FUNCTION public.capacity_membership_intent_increase_guard()")
    for index, name in reversed(tuple(enumerate(_GUARDS))):
        backup = f"capacity_0017_prior_guard_{index}"
        definition = (
            op.get_bind()
            .execute(
                sa.text("SELECT pg_get_functiondef(CAST(:name AS regprocedure))"),
                {"name": f"public.{backup}()"},
            )
            .scalar_one()
        )
        op.execute(
            _replace_once(definition, f"FUNCTION public.{backup}()", f"FUNCTION public.{name}()")
        )
        op.execute(f"DROP FUNCTION public.{backup}()")
    for signature in (
        "capacity_membership_cleanup_reporter(jsonb,uuid)",
        "capacity_membership_binding_target(jsonb)",
        "capacity_membership_target_current(bigint,uuid,uuid)",
        "capacity_membership_pinned_subject(bigint,uuid,uuid)",
        "capacity_membership_event_prefix(bigint,bigint)",
        "capacity_membership_json_digest(jsonb)",
    ):
        op.execute(f"DROP FUNCTION public.{signature}")
