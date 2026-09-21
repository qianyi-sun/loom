"""add batched executable tranches and protected admission evidence

Revision ID: capacity_0014
Revises: capacity_0013
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0014"
down_revision: str | Sequence[str] | None = "capacity_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EXECUTABLE_ADMISSION_WORK_BYTES = 8 * 1024 * 1024
_EXECUTABLE_ADMISSION_CLOSURE_ENVELOPE_BYTES = 142
_EXECUTABLE_ADMISSION_PROPOSAL_BYTES = (
    _EXECUTABLE_ADMISSION_WORK_BYTES
    - _EXECUTABLE_ADMISSION_CLOSURE_ENVELOPE_BYTES
)


def _append_only(table_name: str) -> None:
    op.execute(
        f"""
        CREATE TRIGGER {table_name}_append_only_guard
        BEFORE UPDATE OR DELETE ON public.{table_name}
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {table_name}_truncate_guard
        BEFORE TRUNCATE ON public.{table_name}
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.capacity_executable_receipt_append_only_guard()
        """
    )


def _revoke_public_execute(function_signature: str) -> None:
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION public.{function_signature} FROM PUBLIC"
    )


def _install_admission_payload_validators() -> None:
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_canonical_jsonb_text(
          p_value jsonb
        )
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        STRICT
        SET search_path = pg_catalog
        AS $$
          SELECT CASE pg_catalog.jsonb_typeof(p_value)
            WHEN 'object' THEN (
              SELECT '{' || COALESCE(
                       pg_catalog.string_agg(
                         pg_catalog.to_jsonb(entry.key)::text || ':' ||
                           public.capacity_executable_canonical_jsonb_text(entry.value),
                         ',' ORDER BY entry.key
                       ),
                       ''
                     ) || '}'
                FROM pg_catalog.jsonb_each(p_value) AS entry(key, value)
            )
            WHEN 'array' THEN (
              SELECT '[' || COALESCE(
                       pg_catalog.string_agg(
                         public.capacity_executable_canonical_jsonb_text(item.value),
                         ',' ORDER BY item.position
                       ),
                       ''
                     ) || ']'
                FROM pg_catalog.jsonb_array_elements(p_value)
                     WITH ORDINALITY AS item(value, position)
            )
            ELSE p_value::text
          END
        $$
        """
    )
    _revoke_public_execute("capacity_executable_canonical_jsonb_text(jsonb)")
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_admission_proposal_payload_is_exact(
          p_payload jsonb,
          p_tranche_id uuid,
          p_execution_epoch bigint,
          p_allocation_epoch bigint,
          p_subject_id uuid,
          p_subject_incarnation uuid,
          p_pool_id text,
          p_manager_input_digest text,
          p_manager_allocation_digest text,
          p_proposal_digest text,
          p_expires_at timestamptz
        )
        RETURNS boolean
        LANGUAGE plpgsql
        STABLE
        SET search_path = pg_catalog
        AS $$
        DECLARE
          allocation_record record;
          shape jsonb;
          allowance jsonb;
          expected_shape_id text;
          slot_key text;
          expected_lease timestamptz;
        BEGIN
          IF pg_catalog.jsonb_typeof(p_payload) IS DISTINCT FROM 'object' THEN
            RETURN false;
          END IF;
          IF NOT (
               p_payload ?& ARRAY[
                 'schema_version',
                 'proposal_id',
                 'plan_id',
                 'admission_incarnation',
                 'reporter_incarnation',
                 'protected_admission_sha256',
                 'manager_input_digest',
                 'manager_allocation_digest',
                 'lease_not_after',
                 'shapes',
                 'allowances',
                 'executable'
               ]
             )
             OR (SELECT count(*) FROM pg_catalog.jsonb_object_keys(p_payload)) <> 12
             OR p_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
             OR p_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb
             OR pg_catalog.jsonb_typeof(p_payload -> 'shapes') IS DISTINCT FROM 'array'
             OR pg_catalog.jsonb_array_length(p_payload -> 'shapes') < 1
             OR pg_catalog.jsonb_typeof(p_payload -> 'allowances')
                  IS DISTINCT FROM 'array' THEN
            RETURN false;
          END IF;

          SELECT allocation.*,
                 epoch.complete_payload AS manager_epoch_payload,
                 epoch.input_valid_until AS manager_input_valid_until
            INTO allocation_record
            FROM public.capacity_allocations AS allocation
            JOIN public.capacity_allocation_epochs AS epoch
              ON epoch.allocation_epoch = allocation.allocation_epoch
             AND epoch.execution_epoch = p_execution_epoch
             AND epoch.status = 'executable'
             AND epoch.executable
             AND epoch.sealed
             AND epoch.input_digest = p_manager_input_digest
             AND epoch.input_valid_until > pg_catalog.clock_timestamp()
           WHERE allocation.allocation_epoch = p_allocation_epoch
             AND allocation.subject_id = p_subject_id
             AND allocation.subject_incarnation = p_subject_incarnation
             AND allocation.pool_id = p_pool_id
             AND allocation.mode = 'executable'
             AND allocation.executable
             AND p_allocation_epoch = (
                   SELECT latest.allocation_epoch
                     FROM public.capacity_allocation_epochs AS latest
                    WHERE latest.execution_epoch = p_execution_epoch
                      AND latest.status = 'executable'
                      AND latest.executable
                      AND latest.sealed
                    ORDER BY latest.allocation_epoch DESC
                    LIMIT 1
                 );
          IF NOT FOUND
             OR pg_catalog.jsonb_typeof(allocation_record.allowances)
                  IS DISTINCT FROM 'array'
             OR pg_catalog.jsonb_typeof(allocation_record.witness)
                  IS DISTINCT FROM 'object'
             OR pg_catalog.jsonb_typeof(allocation_record.witness -> 'attempt_ids')
                  IS DISTINCT FROM 'array'
             OR pg_catalog.jsonb_typeof(
                  allocation_record.witness -> 'shape_instance_ids'
                ) IS DISTINCT FROM 'array'
             OR pg_catalog.jsonb_array_length(
                  allocation_record.witness -> 'attempt_ids'
                ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                  allocation_record.witness -> 'shape_instance_ids'
                ) THEN
            RETURN false;
          END IF;

          SELECT LEAST(
                   allocation_record.manager_input_valid_until,
                   bootstrap.expires_at
                 )
            INTO expected_lease
            FROM public.capacity_executable_intents AS intent
            JOIN public.capacity_executable_bootstrap_proposals AS bootstrap
              ON bootstrap.intent_id = intent.intent_id
           WHERE intent.tranche_id = p_tranche_id
           ORDER BY intent.launch_rank, bootstrap.proposal_epoch DESC
           LIMIT 1;
          IF expected_lease IS NULL
             OR p_payload ->> 'manager_input_digest'
                  IS DISTINCT FROM p_manager_input_digest
             OR (p_payload ->> 'lease_not_after')::timestamptz
                  IS DISTINCT FROM expected_lease
             OR p_expires_at IS DISTINCT FROM expected_lease
             OR p_payload ->> 'manager_allocation_digest'
                  IS DISTINCT FROM p_manager_allocation_digest
             OR p_payload ->> 'manager_allocation_digest'
                  IS DISTINCT FROM pg_catalog.encode(
                    pg_catalog.sha256(
                      pg_catalog.convert_to(
                        public.capacity_executable_canonical_jsonb_text(
                          allocation_record.manager_epoch_payload
                        ),
                        'UTF8'
                      )
                    ),
                    'hex'
                  )
             OR p_manager_allocation_digest IS DISTINCT FROM pg_catalog.encode(
                  pg_catalog.sha256(
                    pg_catalog.convert_to(
                      public.capacity_executable_canonical_jsonb_text(
                        allocation_record.manager_epoch_payload
                      ),
                      'UTF8'
                    )
                  ),
                  'hex'
                ) THEN
            RETURN false;
          END IF;

          IF pg_catalog.jsonb_array_length(p_payload -> 'shapes')
               IS DISTINCT FROM (
                 SELECT count(*)
                   FROM public.capacity_executable_intents AS intent
                  WHERE intent.tranche_id = p_tranche_id
               ) THEN
            RETURN false;
          END IF;
          FOR shape IN
            SELECT value
              FROM pg_catalog.jsonb_array_elements(p_payload -> 'shapes') AS item(value)
          LOOP
            IF pg_catalog.jsonb_typeof(shape) IS DISTINCT FROM 'object' THEN
              RETURN false;
            END IF;
            IF NOT (
                 shape ?& ARRAY[
                   'schema_version',
                   'binding',
                   'protocol_generation',
                   'protocol_digest',
                   'worker_shape',
                   'worker_shape_digest',
                   'bootstrap_registration_epoch'
                 ]
               )
               OR (SELECT count(*) FROM pg_catalog.jsonb_object_keys(shape)) <> 7
               OR shape -> 'schema_version' IS DISTINCT FROM '2'::jsonb
               OR pg_catalog.jsonb_typeof(shape -> 'binding') IS DISTINCT FROM 'object'
               OR (shape ->> 'protocol_generation' ~ '^[1-9][0-9]*$')
                    IS DISTINCT FROM true
               OR (shape ->> 'protocol_digest' ~ '^[0-9a-f]{64}$')
                    IS DISTINCT FROM true
               OR pg_catalog.jsonb_typeof(shape -> 'worker_shape')
                    IS DISTINCT FROM 'object'
               OR (shape ->> 'worker_shape_digest' ~ '^[0-9a-f]{64}$')
                    IS DISTINCT FROM true
               OR shape ->> 'worker_shape_digest' IS DISTINCT FROM pg_catalog.encode(
                    pg_catalog.sha256(
                      pg_catalog.convert_to(
                        public.capacity_executable_canonical_jsonb_text(
                          shape -> 'worker_shape'
                        ),
                        'UTF8'
                      )
                    ),
                    'hex'
                  )
               OR shape -> 'bootstrap_registration_epoch' IS DISTINCT FROM '1'::jsonb
               OR NOT EXISTS (
                    SELECT 1
                      FROM public.capacity_executable_intents AS intent
                     WHERE intent.tranche_id = p_tranche_id
                       AND intent.binding_payload = shape -> 'binding'
                  )
               OR NOT EXISTS (
                    SELECT 1
                      FROM public.capacity_worker_profiles AS profile
                      JOIN public.capacity_pools AS pool
                        ON pool.configuration_epoch = (
                             shape -> 'binding' -> 'execution'
                               ->> 'configuration_epoch'
                           )::bigint
                       AND pool.pool_id = p_pool_id
                       AND pool.pool_generation = (
                             shape -> 'binding' ->> 'pool_generation'
                           )::bigint
                       AND pool.protocol_generation = (
                             shape ->> 'protocol_generation'
                           )::bigint
                       AND pool.protocol_digest = shape ->> 'protocol_digest'
                      JOIN LATERAL pg_catalog.jsonb_array_elements(
                             profile.shape_catalog
                           ) AS catalog(value)
                        ON catalog.value = shape -> 'worker_shape'
                       AND catalog.value ->> 'shape_id'
                            = shape -> 'binding' ->> 'shape_id'
                     WHERE profile.subject_id = p_subject_id
                       AND profile.subject_incarnation = p_subject_incarnation
                       AND profile.deployment_generation = (
                             shape -> 'binding' ->> 'deployment_generation'
                           )::bigint
                       AND profile.pool_id = p_pool_id
                       AND profile.pool_generation = (
                             shape -> 'binding' ->> 'pool_generation'
                           )::bigint
                       AND profile.profile_generation = (
                             shape -> 'binding' ->> 'profile_generation'
                           )::bigint
                       AND profile.profile_digest
                            = shape -> 'binding' ->> 'profile_digest'
                  ) THEN
              RETURN false;
            END IF;
          END LOOP;

          IF pg_catalog.jsonb_array_length(p_payload -> 'allowances')
               IS DISTINCT FROM pg_catalog.jsonb_array_length(
                 allocation_record.allowances
               )
             OR (
                  SELECT count(DISTINCT item ->> 'allowance_id')
                    FROM pg_catalog.jsonb_array_elements(
                           p_payload -> 'allowances'
                         ) AS item
                ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                  p_payload -> 'allowances'
                )
             OR (
                  SELECT count(DISTINCT item ->> 'protected_attempt_id')
                    FROM pg_catalog.jsonb_array_elements(
                           p_payload -> 'allowances'
                         ) AS item
                ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                  p_payload -> 'allowances'
                )
             OR (
                  SELECT count(
                           DISTINCT (
                             item ->> 'shape_instance_id',
                             item ->> 'shape_slot_index'
                           )
                         )
                    FROM pg_catalog.jsonb_array_elements(
                           p_payload -> 'allowances'
                         ) AS item
                ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                  p_payload -> 'allowances'
                ) THEN
            RETURN false;
          END IF;
          FOR allowance IN
            SELECT value
              FROM pg_catalog.jsonb_array_elements(
                     p_payload -> 'allowances'
                   ) AS item(value)
          LOOP
            IF pg_catalog.jsonb_typeof(allowance) IS DISTINCT FROM 'object' THEN
              RETURN false;
            END IF;
            IF NOT (
                 allowance ?& ARRAY[
                   'schema_version',
                   'allowance_id',
                   'protected_attempt_id',
                   'shape_instance_id',
                   'shape_slot_index',
                   'submission_intent_id'
                 ]
               )
               OR (SELECT count(*) FROM pg_catalog.jsonb_object_keys(allowance)) <> 6
               OR allowance -> 'schema_version' IS DISTINCT FROM '2'::jsonb
               OR (allowance ->> 'allowance_id'
                    ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                    IS DISTINCT FROM true
               OR (allowance ->> 'protected_attempt_id'
                    ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                    IS DISTINCT FROM true
               OR pg_catalog.jsonb_typeof(allowance -> 'shape_slot_index')
                    IS DISTINCT FROM 'number'
               OR (allowance ->> 'shape_slot_index' ~ '^(0|[1-9][0-9]*)$')
                    IS DISTINCT FROM true
               OR (allowance ->> 'submission_intent_id'
                    ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                    IS DISTINCT FROM true THEN
              RETURN false;
            END IF;
            SELECT expected ->> 'shape_instance_id', matched_slot.value
              INTO expected_shape_id, slot_key
              FROM pg_catalog.jsonb_array_elements(
                     allocation_record.allowances
                   ) AS expected
              JOIN LATERAL (
                     SELECT slot.value
                       FROM pg_catalog.jsonb_array_elements_text(
                              allocation_record.witness -> 'attempt_ids'
                            ) WITH ORDINALITY AS attempt(value, position)
                       JOIN pg_catalog.jsonb_array_elements_text(
                              allocation_record.witness -> 'shape_instance_ids'
                            ) WITH ORDINALITY AS slot(value, position)
                         USING (position)
                      WHERE attempt.value = expected ->> 'attempt_id'
                   ) AS matched_slot ON true
             WHERE expected ->> 'attempt_id'
                    = allowance ->> 'protected_attempt_id'
               AND expected ->> 'pool_id' = p_pool_id;
            IF NOT FOUND
               OR expected_shape_id IS DISTINCT FROM allowance ->> 'shape_instance_id'
               OR (slot_key ~ (
                    '^' || expected_shape_id || '-slot-[0-9]{8}$'
                  )) IS DISTINCT FROM true
               OR pg_catalog.right(slot_key, 8)::bigint IS DISTINCT FROM (
                    allowance ->> 'shape_slot_index'
                  )::bigint
               OR NOT EXISTS (
                    SELECT 1
                      FROM public.capacity_executable_intents AS intent
                     WHERE intent.tranche_id = p_tranche_id
                       AND intent.shape_instance_id = expected_shape_id
                       AND intent.intent_id::text
                            = allowance ->> 'submission_intent_id'
                  ) THEN
              RETURN false;
            END IF;
          END LOOP;
          IF p_proposal_digest IS DISTINCT FROM pg_catalog.encode(
               pg_catalog.sha256(
                 pg_catalog.convert_to(
                   public.capacity_executable_canonical_jsonb_text(p_payload),
                   'UTF8'
                 )
               ),
               'hex'
             ) THEN
            RETURN false;
          END IF;
          RETURN true;
        END;
        $$
        """
    )
    _revoke_public_execute(
        "capacity_executable_admission_proposal_payload_is_exact("
        "jsonb,uuid,bigint,bigint,uuid,uuid,text,text,text,text,timestamptz)"
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_admission_ack_payload_is_exact(
          p_payload jsonb,
          p_proposal_payload jsonb
        )
        RETURNS boolean
        LANGUAGE plpgsql
        IMMUTABLE
        SET search_path = pg_catalog
        AS $$
        DECLARE
          assignment jsonb;
          anchor jsonb;
        BEGIN
          IF pg_catalog.jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR pg_catalog.jsonb_typeof(p_proposal_payload) IS DISTINCT FROM 'object' THEN
            RETURN false;
          END IF;
          IF NOT (
               p_payload ?& ARRAY[
                 'schema_version',
                 'execution',
                 'tranche_id',
                 'proposal_id',
                 'plan_id',
                 'admission_incarnation',
                 'subject_id',
                 'subject_incarnation',
                 'pool_id',
                 'reporter_incarnation',
                 'protected_admission_sha256',
                 'proposal_digest',
                 'prepared_plan_digest',
                 'assignment_count',
                 'assignments',
                 'executable'
               ]
             )
             OR (SELECT count(*) FROM pg_catalog.jsonb_object_keys(p_payload)) <> 16
             OR p_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
             OR p_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb
             OR pg_catalog.jsonb_typeof(p_payload -> 'assignments')
                  IS DISTINCT FROM 'array'
             OR (p_payload ->> 'assignment_count' ~ '^(0|[1-9][0-9]*)$')
                  IS DISTINCT FROM true
             OR (p_payload ->> 'assignment_count')::bigint
                  IS DISTINCT FROM pg_catalog.jsonb_array_length(
                    p_payload -> 'assignments'
                  )
             OR (p_payload ->> 'prepared_plan_digest' ~ '^[0-9a-f]{64}$')
                  IS DISTINCT FROM true THEN
            RETURN false;
          END IF;
          anchor := p_proposal_payload -> 'shapes' -> 0 -> 'binding';
          IF p_payload -> 'execution' IS DISTINCT FROM anchor -> 'execution'
             OR p_payload ->> 'tranche_id' IS DISTINCT FROM anchor ->> 'tranche_id'
             OR p_payload ->> 'proposal_id'
                  IS DISTINCT FROM p_proposal_payload ->> 'proposal_id'
             OR p_payload ->> 'plan_id'
                  IS DISTINCT FROM p_proposal_payload ->> 'plan_id'
             OR p_payload ->> 'admission_incarnation'
                  IS DISTINCT FROM p_proposal_payload ->> 'admission_incarnation'
             OR p_payload ->> 'subject_id' IS DISTINCT FROM anchor ->> 'subject_id'
             OR p_payload ->> 'subject_incarnation'
                  IS DISTINCT FROM anchor ->> 'subject_incarnation'
             OR p_payload ->> 'pool_id' IS DISTINCT FROM anchor ->> 'pool_id'
             OR p_payload ->> 'reporter_incarnation'
                  IS DISTINCT FROM p_proposal_payload ->> 'reporter_incarnation'
             OR p_payload ->> 'protected_admission_sha256'
                  IS DISTINCT FROM p_proposal_payload ->> 'protected_admission_sha256'
             OR pg_catalog.jsonb_array_length(p_payload -> 'assignments')
                  IS DISTINCT FROM pg_catalog.jsonb_array_length(
                    p_proposal_payload -> 'allowances'
                  ) THEN
            RETURN false;
          END IF;
          IF (
               SELECT count(DISTINCT item ->> 'transition_id')
                 FROM pg_catalog.jsonb_array_elements(
                        p_payload -> 'assignments'
                      ) AS item
             ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
               p_payload -> 'assignments'
             )
             OR (
                  SELECT count(DISTINCT item ->> 'allowance_id')
                    FROM pg_catalog.jsonb_array_elements(
                           p_payload -> 'assignments'
                         ) AS item
                ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                  p_payload -> 'assignments'
                )
             OR (
                  SELECT count(DISTINCT item ->> 'protected_attempt_id')
                    FROM pg_catalog.jsonb_array_elements(
                           p_payload -> 'assignments'
                         ) AS item
                ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                  p_payload -> 'assignments'
                )
             OR (
                  SELECT count(
                           DISTINCT (
                             item ->> 'shape_instance_id',
                             item ->> 'shape_slot_index'
                           )
                         )
                    FROM pg_catalog.jsonb_array_elements(
                           p_payload -> 'assignments'
                         ) AS item
                ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                  p_payload -> 'assignments'
             ) THEN
            RETURN false;
          END IF;
          FOR assignment IN
            SELECT value
              FROM pg_catalog.jsonb_array_elements(
                     p_payload -> 'assignments'
                   ) AS item(value)
          LOOP
            IF pg_catalog.jsonb_typeof(assignment) IS DISTINCT FROM 'object'
               OR NOT (
                    assignment ?& ARRAY[
                      'schema_version',
                      'transition_id',
                      'allowance_id',
                      'protected_attempt_id',
                      'execution_generation',
                      'requirements_digest',
                      'shape_instance_id',
                      'shape_slot_index',
                      'submission_intent_id',
                      'lifecycle_sequence'
                    ]
                  )
               OR (SELECT count(*) FROM pg_catalog.jsonb_object_keys(assignment)) <> 10
               OR assignment -> 'schema_version' IS DISTINCT FROM '2'::jsonb
               OR (assignment ->> 'transition_id'
                    ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                    IS DISTINCT FROM true
               OR (assignment ->> 'execution_generation' ~ '^[1-9][0-9]*$')
                    IS DISTINCT FROM true
               OR (assignment ->> 'requirements_digest' ~ '^[0-9a-f]{64}$')
                    IS DISTINCT FROM true
               OR (assignment ->> 'lifecycle_sequence' ~ '^[1-9][0-9]*$')
                    IS DISTINCT FROM true
               OR NOT EXISTS (
                    SELECT 1
                      FROM pg_catalog.jsonb_array_elements(
                             p_proposal_payload -> 'allowances'
                           ) AS allowance
                     WHERE assignment ->> 'allowance_id'
                            = allowance ->> 'allowance_id'
                       AND assignment ->> 'protected_attempt_id'
                            = allowance ->> 'protected_attempt_id'
                       AND assignment ->> 'shape_instance_id'
                            = allowance ->> 'shape_instance_id'
                       AND assignment -> 'shape_slot_index'
                            = allowance -> 'shape_slot_index'
                       AND assignment ->> 'submission_intent_id'
                            = allowance ->> 'submission_intent_id'
                  ) THEN
              RETURN false;
            END IF;
          END LOOP;
          IF EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(
                     p_proposal_payload -> 'allowances'
                   ) AS allowance
             WHERE NOT EXISTS (
               SELECT 1
                 FROM pg_catalog.jsonb_array_elements(
                        p_payload -> 'assignments'
                      ) AS covered_assignment(value)
                WHERE covered_assignment.value ->> 'allowance_id'
                        = allowance ->> 'allowance_id'
                  AND covered_assignment.value ->> 'protected_attempt_id'
                        = allowance ->> 'protected_attempt_id'
                  AND covered_assignment.value ->> 'shape_instance_id'
                        = allowance ->> 'shape_instance_id'
                  AND covered_assignment.value -> 'shape_slot_index'
                        = allowance -> 'shape_slot_index'
                  AND covered_assignment.value ->> 'submission_intent_id'
                        = allowance ->> 'submission_intent_id'
             )
          ) THEN
            RETURN false;
          END IF;
          RETURN true;
        END;
        $$
        """
    )
    _revoke_public_execute(
        "capacity_executable_admission_ack_payload_is_exact(jsonb,jsonb)"
    )


def _install_intent_admission_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          IF OLD.state = 'accepted'
             AND NEW.state IN ('bootstrap-acknowledged', 'launch-ready') THEN
            PERFORM 1
              FROM public.capacity_executable_bootstrap_acknowledgements AS acknowledgement
             WHERE acknowledgement.intent_id = NEW.intent_id
               AND acknowledgement.execution_epoch = NEW.execution_epoch
               AND acknowledgement.execution_manifest_sha256
                    = NEW.execution_manifest_sha256
               AND acknowledgement.bootstrap_registration_epoch
                    = NEW.bootstrap_registration_epoch
               AND acknowledgement.bootstrap_evidence_sha256
                    = NEW.bootstrap_evidence_sha256;
            IF NOT FOUND THEN
              RAISE EXCEPTION
                'executable intent bootstrap acknowledgement is required'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          IF (OLD.state = 'bootstrap-acknowledged' AND NEW.state = 'launch-ready')
             OR (OLD.state = 'accepted' AND NEW.state = 'launch-ready') THEN
            PERFORM 1
              FROM public.capacity_executable_admission_acknowledgements AS acknowledgement
              JOIN public.capacity_executable_admission_proposals AS proposal
                ON proposal.proposal_id = acknowledgement.proposal_id
               AND proposal.proposal_digest = acknowledgement.proposal_digest
              JOIN public.capacity_authority_state AS authority
                ON authority.singleton_id = 1
               AND authority.authority_incarnation = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'authority_incarnation'
                   )::uuid
               AND authority.writer_epoch = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'writer_epoch'
                   )::bigint
               AND authority.execution_epoch = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'execution_epoch'
                   )::bigint
               AND authority.execution_epoch = acknowledgement.execution_epoch
               AND authority.execution_manifest_sha256 = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'execution_manifest_sha256'
                   )
               AND authority.execution_manifest_sha256
                    = acknowledgement.execution_manifest_sha256
               AND authority.execution_state = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'execution_state'
                   )
               AND authority.executable_new_capacity_ceiling = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'executable_new_capacity_ceiling'
                   )::bigint
              JOIN public.capacity_execution_epochs AS epoch
                ON epoch.execution_epoch = authority.execution_epoch
               AND epoch.execution_manifest_sha256
                    = authority.execution_manifest_sha256
               AND epoch.configuration_epoch = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'configuration_epoch'
                   )::bigint
               AND epoch.state = authority.execution_state
               AND epoch.effective_ceiling
                    = authority.executable_new_capacity_ceiling
               AND epoch.effective_rate_per_minute = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'executable_new_capacity_rate_per_minute'
                   )::bigint
               AND epoch.trusted_fleet_release_sha256 = (
                     acknowledgement.acknowledgement_payload
                       -> 'execution' ->> 'trusted_fleet_release_sha256'
                   )
              JOIN public.capacity_demand_reporters AS reporter
                ON reporter.subject_id = acknowledgement.subject_id
               AND reporter.subject_incarnation = acknowledgement.subject_incarnation
               AND reporter.reporter_incarnation = acknowledgement.reporter_incarnation
               AND reporter.state = 'current'
              JOIN public.capacity_allocation_epochs AS allocation
                ON allocation.allocation_epoch = acknowledgement.allocation_epoch
               AND allocation.execution_epoch = acknowledgement.execution_epoch
               AND allocation.execution_manifest_sha256
                    = acknowledgement.execution_manifest_sha256
               AND allocation.status = 'executable'
               AND allocation.executable
               AND allocation.sealed
               AND allocation.input_valid_until > pg_catalog.clock_timestamp()
             WHERE acknowledgement.tranche_id = NEW.tranche_id
               AND acknowledgement.execution_epoch = NEW.execution_epoch
               AND acknowledgement.execution_manifest_sha256
                    = NEW.execution_manifest_sha256
               AND acknowledgement.allocation_epoch = NEW.allocation_epoch
               AND acknowledgement.subject_id = NEW.subject_id
               AND acknowledgement.subject_incarnation = NEW.subject_incarnation
               AND acknowledgement.pool_id = NEW.pool_id
               AND acknowledgement.acknowledgement_payload
                     -> 'execution' -> 'executable' = 'true'::jsonb
               AND (acknowledgement.acknowledgement_payload
                      -> 'execution' ->> 'allocation_epoch')::bigint
                    = acknowledgement.allocation_epoch
               AND acknowledgement.allocation_epoch = (
                     SELECT latest.allocation_epoch
                       FROM public.capacity_allocation_epochs AS latest
                      WHERE latest.status = 'executable'
                        AND latest.executable
                        AND latest.sealed
                        AND latest.execution_epoch = epoch.execution_epoch
                        AND latest.execution_manifest_sha256
                             = epoch.execution_manifest_sha256
                      ORDER BY latest.allocation_epoch DESC
                      LIMIT 1
                   )
               AND pg_catalog.jsonb_typeof(proposal.proposal_payload -> 'shapes')
                    = 'array'
               AND EXISTS (
                     SELECT 1
                       FROM pg_catalog.jsonb_array_elements(
                              proposal.proposal_payload -> 'shapes'
                            ) AS shape
                      WHERE shape -> 'binding' = NEW.binding_payload
                        AND (shape ->> 'bootstrap_registration_epoch')::bigint
                             = NEW.bootstrap_registration_epoch
                   )
               AND pg_catalog.jsonb_array_length(
                     proposal.proposal_payload -> 'shapes'
                   ) = (
                     SELECT count(*)
                       FROM public.capacity_executable_intents AS covered_intent
                      WHERE covered_intent.tranche_id = NEW.tranche_id
                   )
               AND NOT EXISTS (
                     SELECT 1
                       FROM public.capacity_executable_intents AS covered_intent
                      WHERE covered_intent.tranche_id = NEW.tranche_id
                        AND NOT EXISTS (
                              SELECT 1
                                FROM pg_catalog.jsonb_array_elements(
                                       proposal.proposal_payload -> 'shapes'
                                     ) AS shape
                               WHERE shape -> 'binding'
                                     = covered_intent.binding_payload
                            )
                   )
               AND NOT EXISTS (
                     SELECT 1
                       FROM pg_catalog.jsonb_array_elements(
                              proposal.proposal_payload -> 'shapes'
                            ) AS shape
                      WHERE NOT EXISTS (
                              SELECT 1
                                FROM public.capacity_executable_intents
                                       AS covered_intent
                               WHERE covered_intent.tranche_id = NEW.tranche_id
                                 AND covered_intent.binding_payload
                                      = shape -> 'binding'
                            )
                   )
               AND public.capacity_executable_admission_proposal_payload_is_exact(
                     proposal.proposal_payload,
                     proposal.tranche_id,
                     proposal.execution_epoch,
                     proposal.allocation_epoch,
                     proposal.subject_id,
                     proposal.subject_incarnation,
                     proposal.pool_id,
                     proposal.manager_input_digest,
                     proposal.manager_allocation_digest,
                     proposal.proposal_digest,
                     proposal.expires_at
                   )
               AND public.capacity_executable_admission_ack_payload_is_exact(
                     acknowledgement.acknowledgement_payload,
                     proposal.proposal_payload
                   )
               AND proposal.expires_at > pg_catalog.clock_timestamp();
            IF NOT FOUND THEN
              RAISE EXCEPTION
                'executable intent launch readiness requires protected admission acknowledgement'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _revoke_public_execute(
        "capacity_executable_intent_protected_bootstrap_guard()"
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_intent_protected_bootstrap_guard
        BEFORE UPDATE ON public.capacity_executable_intents
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()
        """
    )


def _extend_intent_state_machine(*, downgrade: bool = False) -> None:
    replacements = (
        (
            "(OLD.state = 'accepted' AND NEW.state IN "
            "('launch-ready','closing','released','quarantined')) OR",
            "(OLD.state = 'accepted' AND NEW.state IN "
            "('bootstrap-acknowledged','launch-ready','closing','released',"
            "'quarantined')) OR\n"
            "            (OLD.state = 'bootstrap-acknowledged' AND NEW.state IN "
            "('launch-ready','closing','released','quarantined')) OR",
        ),
        (
            "OR (NEW.bootstrap_registration_epoch IS NULL)\n"
            "                <> (NEW.launch_ready_at IS NULL) THEN",
            "OR (NEW.state = 'bootstrap-acknowledged'\n"
            "                AND NEW.launch_ready_at IS NOT NULL)\n"
            "             OR (NEW.state IN ('closing','released')\n"
            "                AND NEW.bootstrap_registration_epoch IS NULL\n"
            "                AND NEW.launch_ready_at IS NOT NULL)\n"
            "             OR (NEW.state NOT IN "
            "('bootstrap-acknowledged','closing','released','quarantined')\n"
            "                AND (NEW.bootstrap_registration_epoch IS NULL)\n"
            "                    <> (NEW.launch_ready_at IS NULL)) THEN",
        ),
        (
            "IF OLD.state = 'accepted' AND NEW.state = 'launch-ready'",
            "IF OLD.state = 'accepted' AND NEW.state = 'bootstrap-acknowledged'\n"
            "             AND bootstrap_changed\n"
            "             AND NEW.bootstrap_registration_epoch IS NOT NULL\n"
            "             AND NEW.launch_ready_at IS NULL\n"
            "             AND NOT accepted_changed AND NOT permit_changed\n"
            "             AND NOT consumption_changed AND NOT inventory_changed\n"
            "             AND NOT release_changed THEN\n"
            "            RETURN NEW;\n"
            "          END IF;\n"
            "          IF OLD.state = 'bootstrap-acknowledged'\n"
            "             AND NEW.state = 'launch-ready'\n"
            "             AND bootstrap_changed\n"
            "             AND NEW.bootstrap_registration_epoch\n"
            "                  IS NOT DISTINCT FROM OLD.bootstrap_registration_epoch\n"
            "             AND NEW.bootstrap_evidence_sha256\n"
            "                  IS NOT DISTINCT FROM OLD.bootstrap_evidence_sha256\n"
            "             AND NEW.launch_ready_at IS NOT NULL\n"
            "             AND NOT accepted_changed AND NOT permit_changed\n"
            "             AND NOT consumption_changed AND NOT inventory_changed\n"
            "             AND NOT release_changed THEN\n"
            "            RETURN NEW;\n"
            "          END IF;\n"
            "          IF OLD.state = 'accepted' AND NEW.state = 'launch-ready'",
        ),
        (
            "IF OLD.state IN ('accepted','launch-ready','permitted')\n"
            "             AND NEW.state = 'closing'",
            "IF OLD.state IN "
            "('accepted','bootstrap-acknowledged','launch-ready','permitted')\n"
            "             AND NEW.state = 'closing'",
        ),
    )
    if downgrade:
        replacements = tuple((new, old) for old, new in reversed(replacements))
    values = ",\n".join(
        f"($old{index}${old}$old{index}$, $new{index}${new}$new{index}$)"
        for index, (old, new) in enumerate(replacements)
    )
    op.execute(
        f"""
        DO $migration$
        DECLARE
          definition text;
          prior text;
          replacement record;
        BEGIN
          SELECT pg_catalog.pg_get_functiondef(
                   'public.capacity_executable_intent_guard()'::regprocedure
                 )
            INTO definition;
          FOR replacement IN
            SELECT * FROM (VALUES {values}) AS changes(old_text, new_text)
          LOOP
            prior := definition;
            definition := pg_catalog.replace(
              definition,
              replacement.old_text,
              replacement.new_text
            );
            IF definition = prior THEN
              RAISE EXCEPTION
                'capacity executable intent state-machine migration drift';
            END IF;
          END LOOP;
          EXECUTE definition;
        END;
        $migration$;
        """
    )


def upgrade() -> None:
    op.execute(
        "LOCK TABLE public.capacity_executable_intents, "
        "public.capacity_executable_bootstrap_acknowledgements, "
        "public.capacity_executable_bootstrap_proposals IN ACCESS EXCLUSIVE MODE"
    )
    bind = op.get_bind()
    if bind.execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_executable_intents "
            "WHERE state IN ('launch-ready','permitted'))"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot upgrade capacity_0014 with bootstrap-only launch authority"
        )
    if bind.execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_executable_intents AS intent "
            "JOIN public.capacity_allocation_epochs AS allocation "
            "ON allocation.allocation_epoch = intent.allocation_epoch "
            "WHERE (SELECT count(*) "
            "FROM pg_catalog.jsonb_array_elements("
            "allocation.complete_payload -> 'hypothetical_launch_rank') AS rank "
            "WHERE rank ->> 'subject_id' = intent.subject_id::text "
            "AND rank ->> 'pool_id' = intent.pool_id) > 1)"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot upgrade capacity_0014 with unbatchable legacy intent scope"
        )
    op.execute(
        "DROP TRIGGER capacity_executable_intent_protected_bootstrap_guard "
        "ON public.capacity_executable_intents"
    )
    op.execute("DROP FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()")
    op.drop_constraint(
        "capacity_executable_tranche_identity_key",
        "capacity_executable_intents",
        schema="public",
        type_="unique",
    )
    op.drop_constraint(
        "capacity_executable_intent_state_check",
        "capacity_executable_intents",
        schema="public",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        schema="public",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        "execution_epoch > 0 AND prepared_writer_epoch > 0 "
        "AND current_writer_epoch > 0 AND configuration_epoch > 0 "
        "AND fleet_generation > 0 AND oldlab_pool_generation > 0 "
        "AND gb10_pool_generation > 0 AND requested_ceiling > 0 "
        "AND effective_ceiling >= 0 AND effective_ceiling <= requested_ceiling "
        "AND requested_rate_per_minute > 0 AND effective_rate_per_minute >= 0 "
        "AND effective_rate_per_minute <= requested_rate_per_minute",
        schema="public",
    )

    op.create_table(
        "capacity_executable_tranches",
        sa.Column("tranche_id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("configuration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("executor_id", sa.Text(), nullable=False),
        sa.Column("executor_incarnation", sa.UUID(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("subject_id", sa.UUID(), nullable=False),
        sa.Column("subject_incarnation", sa.UUID(), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("proposal_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND configuration_epoch > 0 "
            "AND allocation_epoch > 0 AND pool_generation > 0",
            name="capacity_executable_tranche_quantity_check",
        ),
        sa.CheckConstraint(
            "pool_id IN ('oldlab','gb10') "
            "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND proposal_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_tranche_binding_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(proposal_payload) = 'object' "
            "AND octet_length(proposal_payload::text) <= 8388608",
            name="capacity_executable_tranche_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["execution_epoch", "execution_manifest_sha256"],
            [
                "public.capacity_execution_epochs.execution_epoch",
                "public.capacity_execution_epochs.execution_manifest_sha256",
            ],
            name="capacity_executable_tranche_execution_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["allocation_epoch", "execution_epoch", "execution_manifest_sha256"],
            [
                "public.capacity_allocation_epochs.allocation_epoch",
                "public.capacity_allocation_epochs.execution_epoch",
                "public.capacity_allocation_epochs.execution_manifest_sha256",
            ],
            name="capacity_executable_tranche_allocation_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "execution_epoch",
                "execution_manifest_sha256",
                "executor_id",
                "executor_incarnation",
                "pool_id",
                "pool_generation",
            ],
            [
                "public.capacity_execution_executors.execution_epoch",
                "public.capacity_execution_executors.execution_manifest_sha256",
                "public.capacity_execution_executors.executor_id",
                "public.capacity_execution_executors.executor_incarnation",
                "public.capacity_execution_executors.pool_id",
                "public.capacity_execution_executors.pool_generation",
            ],
            name="capacity_executable_tranche_executor_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("tranche_id"),
        schema="public",
    )
    op.execute(
        """
        INSERT INTO public.capacity_executable_tranches
          (tranche_id, execution_epoch, execution_manifest_sha256,
           configuration_epoch, allocation_epoch, executor_id,
           executor_incarnation, pool_id, pool_generation, subject_id,
           subject_incarnation, proposal_digest, proposal_payload, created_at)
        SELECT tranche_id, execution_epoch, execution_manifest_sha256,
               configuration_epoch, allocation_epoch, executor_id,
               executor_incarnation, pool_id, pool_generation, subject_id,
               subject_incarnation, proposal_digest, proposal_payload, created_at
          FROM public.capacity_executable_intents
        """
    )
    _append_only("capacity_executable_tranches")
    op.create_unique_constraint(
        "capacity_executable_tranche_intent_key",
        "capacity_executable_intents",
        ["tranche_id", "intent_id"],
        schema="public",
    )
    op.create_foreign_key(
        "capacity_executable_intent_tranche_fkey",
        "capacity_executable_intents",
        "capacity_executable_tranches",
        ["tranche_id"],
        ["tranche_id"],
        source_schema="public",
        referent_schema="public",
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "capacity_executable_intent_state_check",
        "capacity_executable_intents",
        "state IN ('proposed','accepted','bootstrap-acknowledged','launch-ready',"
        "'permitted','submitting-unknown','bound','observed','terminal','closing',"
        "'released','quarantined')",
        schema="public",
    )
    _extend_intent_state_machine()
    _install_admission_payload_validators()

    op.create_table(
        "capacity_executable_admission_proposals",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("proposal_id", sa.UUID(), nullable=False),
        sa.Column("plan_id", sa.UUID(), nullable=False),
        sa.Column("admission_incarnation", sa.UUID(), nullable=False),
        sa.Column("tranche_id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("subject_id", sa.UUID(), nullable=False),
        sa.Column("subject_incarnation", sa.UUID(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("reporter_incarnation", sa.UUID(), nullable=False),
        sa.Column("protected_admission_sha256", sa.Text(), nullable=False),
        sa.Column("manager_input_digest", sa.Text(), nullable=False),
        sa.Column("manager_allocation_digest", sa.Text(), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("proposal_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND allocation_epoch > 0 "
            "AND pool_id IN ('oldlab','gb10')",
            name="capacity_executable_admission_proposal_quantity_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
            "AND manager_input_digest ~ '^[0-9a-f]{64}$' "
            "AND manager_allocation_digest ~ '^[0-9a-f]{64}$' "
            "AND proposal_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_admission_proposal_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(proposal_payload) = 'object' "
            "AND octet_length(convert_to("
            "public.capacity_executable_canonical_jsonb_text(proposal_payload),"
            "'UTF8')) <= "
            f"{_EXECUTABLE_ADMISSION_PROPOSAL_BYTES}",
            name="capacity_executable_admission_proposal_payload_check",
        ),
        sa.CheckConstraint(
            "expires_at > created_at AND expires_at <= created_at + interval '10 minutes'",
            name="capacity_executable_admission_proposal_expiry_check",
        ),
        sa.ForeignKeyConstraint(
            ["tranche_id"],
            ["public.capacity_executable_tranches.tranche_id"],
            name="capacity_executable_admission_proposal_tranche_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "proposal_id",
            "proposal_digest",
            name="capacity_executable_admission_proposal_exact_key",
        ),
        sa.UniqueConstraint(
            "execution_epoch",
            "allocation_epoch",
            "subject_id",
            "subject_incarnation",
            "pool_id",
            name="capacity_executable_admission_proposal_scope_key",
        ),
        sa.UniqueConstraint("plan_id", name="capacity_executable_admission_plan_key"),
        sa.UniqueConstraint(
            "admission_incarnation",
            name="capacity_executable_admission_incarnation_key",
        ),
        schema="public",
    )
    op.create_table(
        "capacity_executable_admission_acknowledgements",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("proposal_id", sa.UUID(), nullable=False),
        sa.Column("plan_id", sa.UUID(), nullable=False),
        sa.Column("admission_incarnation", sa.UUID(), nullable=False),
        sa.Column("tranche_id", sa.UUID(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("allocation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("subject_id", sa.UUID(), nullable=False),
        sa.Column("subject_incarnation", sa.UUID(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("reporter_incarnation", sa.UUID(), nullable=False),
        sa.Column("protected_admission_sha256", sa.Text(), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("prepared_plan_digest", sa.Text(), nullable=False),
        sa.Column("acknowledgement_digest", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=False),
        sa.Column(
            "acknowledgement_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND allocation_epoch > 0 "
            "AND pool_id IN ('oldlab','gb10')",
            name="capacity_executable_admission_ack_quantity_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
            "AND proposal_digest ~ '^[0-9a-f]{64}$' "
            "AND prepared_plan_digest ~ '^[0-9a-f]{64}$' "
            "AND acknowledgement_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_admission_ack_digest_check",
        ),
        sa.CheckConstraint(
            "actor_id <> '' AND octet_length(actor_id) <= 256 "
            "AND jsonb_typeof(acknowledgement_payload) = 'object' "
            "AND octet_length(acknowledgement_payload::text) <= 8388608",
            name="capacity_executable_admission_ack_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["proposal_id", "proposal_digest"],
            [
                "public.capacity_executable_admission_proposals.proposal_id",
                "public.capacity_executable_admission_proposals.proposal_digest",
            ],
            name="capacity_executable_admission_ack_proposal_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="capacity_executable_admission_ack_idempotency_key",
        ),
        sa.UniqueConstraint(
            "proposal_id",
            name="capacity_executable_admission_ack_proposal_key",
        ),
        schema="public",
    )
    op.create_table(
        "capacity_executable_admission_closure_acknowledgements",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("closure_id", sa.UUID(), nullable=False),
        sa.Column("proposal_id", sa.UUID(), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("plan_id", sa.UUID(), nullable=False),
        sa.Column("admission_incarnation", sa.UUID(), nullable=False),
        sa.Column("subject_id", sa.UUID(), nullable=False),
        sa.Column("subject_incarnation", sa.UUID(), nullable=False),
        sa.Column("reporter_incarnation", sa.UUID(), nullable=False),
        sa.Column("protected_admission_sha256", sa.Text(), nullable=False),
        sa.Column("close_reason", sa.Text(), nullable=False),
        sa.Column("disposition_kind", sa.Text(), nullable=False),
        sa.Column("disposition_digest", sa.Text(), nullable=False),
        sa.Column("acknowledgement_digest", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=False),
        sa.Column(
            "acknowledgement_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "close_reason IN ('expired','allocation-superseded','manager-closed')",
            name="capacity_executable_admission_closure_ack_reason_check",
        ),
        sa.CheckConstraint(
            "protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
            "AND proposal_digest ~ '^[0-9a-f]{64}$' "
            "AND disposition_digest ~ '^[0-9a-f]{64}$' "
            "AND acknowledgement_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executable_admission_closure_ack_digest_check",
        ),
        sa.CheckConstraint(
            "disposition_kind IN ('abandoned','never-converged')",
            name="capacity_executable_admission_closure_ack_disposition_check",
        ),
        sa.CheckConstraint(
            "actor_id <> '' AND octet_length(actor_id) <= 256 "
            "AND jsonb_typeof(acknowledgement_payload) = 'object' "
            "AND octet_length(acknowledgement_payload::text) <= 8388608",
            name="capacity_executable_admission_closure_ack_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["proposal_id", "proposal_digest"],
            [
                "public.capacity_executable_admission_proposals.proposal_id",
                "public.capacity_executable_admission_proposals.proposal_digest",
            ],
            name="capacity_executable_admission_closure_ack_proposal_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="capacity_executable_admission_closure_ack_idempotency_key",
        ),
        sa.UniqueConstraint(
            "closure_id",
            name="capacity_executable_admission_closure_ack_closure_key",
        ),
        sa.UniqueConstraint(
            "proposal_id",
            name="capacity_executable_admission_closure_ack_proposal_key",
        ),
        schema="public",
    )
    for table_name in (
        "capacity_executable_admission_proposals",
        "capacity_executable_admission_acknowledgements",
        "capacity_executable_admission_closure_acknowledgements",
    ):
        _append_only(table_name)
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_admission_proposal_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          tranche_record record;
          first_binding jsonb;
        BEGIN
          SELECT tranche.* INTO tranche_record
            FROM public.capacity_executable_tranches AS tranche
           WHERE tranche.tranche_id = NEW.tranche_id
           FOR SHARE;
          first_binding := NEW.proposal_payload -> 'shapes' -> 0 -> 'binding';
          IF NOT FOUND
             OR tranche_record.execution_epoch IS DISTINCT FROM NEW.execution_epoch
             OR tranche_record.execution_manifest_sha256
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR tranche_record.allocation_epoch IS DISTINCT FROM NEW.allocation_epoch
             OR tranche_record.subject_id IS DISTINCT FROM NEW.subject_id
             OR tranche_record.subject_incarnation IS DISTINCT FROM NEW.subject_incarnation
             OR tranche_record.pool_id IS DISTINCT FROM NEW.pool_id
             OR NEW.proposal_payload ->> 'proposal_id' IS DISTINCT FROM NEW.proposal_id::text
             OR NEW.proposal_payload ->> 'plan_id' IS DISTINCT FROM NEW.plan_id::text
             OR NEW.proposal_payload ->> 'admission_incarnation'
                  IS DISTINCT FROM NEW.admission_incarnation::text
             OR NEW.proposal_payload ->> 'reporter_incarnation'
                  IS DISTINCT FROM NEW.reporter_incarnation::text
             OR NEW.proposal_payload ->> 'protected_admission_sha256'
                  IS DISTINCT FROM NEW.protected_admission_sha256
             OR NEW.proposal_payload ->> 'manager_input_digest'
                  IS DISTINCT FROM NEW.manager_input_digest
             OR NEW.proposal_payload ->> 'manager_allocation_digest'
                  IS DISTINCT FROM NEW.manager_allocation_digest
             OR (NEW.proposal_payload ->> 'lease_not_after')::timestamptz
                  IS DISTINCT FROM NEW.expires_at
             OR NEW.proposal_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb
             OR pg_catalog.jsonb_typeof(NEW.proposal_payload -> 'shapes')
                  IS DISTINCT FROM 'array'
             OR pg_catalog.jsonb_array_length(NEW.proposal_payload -> 'shapes') < 1
             OR pg_catalog.jsonb_typeof(NEW.proposal_payload -> 'allowances')
                  IS DISTINCT FROM 'array'
             OR first_binding ->> 'tranche_id' IS DISTINCT FROM NEW.tranche_id::text
             OR (first_binding -> 'execution' ->> 'execution_epoch')::bigint
                  IS DISTINCT FROM NEW.execution_epoch
             OR first_binding -> 'execution' ->> 'execution_manifest_sha256'
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR (first_binding -> 'execution' ->> 'allocation_epoch')::bigint
                  IS DISTINCT FROM NEW.allocation_epoch
             OR first_binding ->> 'subject_id' IS DISTINCT FROM NEW.subject_id::text
             OR first_binding ->> 'subject_incarnation'
                  IS DISTINCT FROM NEW.subject_incarnation::text
             OR first_binding ->> 'pool_id' IS DISTINCT FROM NEW.pool_id THEN
            RAISE EXCEPTION 'executable admission proposal binding changed'
              USING ERRCODE = '23514';
          END IF;
          PERFORM 1
            FROM public.capacity_demand_reporters AS reporter
           WHERE reporter.subject_id = NEW.subject_id
             AND reporter.subject_incarnation = NEW.subject_incarnation
             AND reporter.reporter_incarnation = NEW.reporter_incarnation
             AND reporter.state = 'current'
           FOR SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'executable admission proposal reporter changed'
              USING ERRCODE = '23514';
          END IF;
          PERFORM 1
            FROM public.capacity_execution_epochs AS epoch,
                 LATERAL pg_catalog.jsonb_array_elements(
                   epoch.manifest_payload -> 'subject_acknowledgements'
                 ) AS acknowledgement
           WHERE epoch.execution_epoch = NEW.execution_epoch
             AND epoch.execution_manifest_sha256 = NEW.execution_manifest_sha256
             AND (acknowledgement ->> 'subject_id')::uuid = NEW.subject_id
             AND (acknowledgement ->> 'subject_incarnation')::uuid
                  = NEW.subject_incarnation
             AND (acknowledgement ->> 'reporter_incarnation')::uuid
                  = NEW.reporter_incarnation
             AND acknowledgement ->> 'protected_admission_sha256'
                  = NEW.protected_admission_sha256
           FOR SHARE OF epoch;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'executable admission protected authority changed'
              USING ERRCODE = '23514';
          END IF;
          PERFORM 1
            FROM public.capacity_executable_intents AS intent
           WHERE intent.tranche_id = NEW.tranche_id
           ORDER BY intent.launch_rank, intent.intent_id
           FOR SHARE;
          IF NOT FOUND
             OR pg_catalog.jsonb_array_length(NEW.proposal_payload -> 'shapes')
                  IS DISTINCT FROM (
                    SELECT count(*)
                      FROM public.capacity_executable_intents AS intent
                     WHERE intent.tranche_id = NEW.tranche_id
                  )
             OR EXISTS (
                  SELECT 1
                    FROM public.capacity_executable_intents AS intent
                   WHERE intent.tranche_id = NEW.tranche_id
                     AND NOT EXISTS (
                           SELECT 1
                             FROM pg_catalog.jsonb_array_elements(
                                    NEW.proposal_payload -> 'shapes'
                                  ) AS shape
                            WHERE shape -> 'binding' = intent.binding_payload
                         )
                )
             OR EXISTS (
                  SELECT 1
                    FROM pg_catalog.jsonb_array_elements(
                           NEW.proposal_payload -> 'shapes'
                         ) AS shape
                   WHERE NOT EXISTS (
                           SELECT 1
                             FROM public.capacity_executable_intents AS intent
                            WHERE intent.tranche_id = NEW.tranche_id
                              AND intent.binding_payload = shape -> 'binding'
                         )
                ) THEN
            RAISE EXCEPTION 'executable admission proposal intent set changed'
              USING ERRCODE = '23514';
          END IF;
          IF public.capacity_executable_admission_proposal_payload_is_exact(
               NEW.proposal_payload,
               NEW.tranche_id,
               NEW.execution_epoch,
               NEW.allocation_epoch,
               NEW.subject_id,
               NEW.subject_incarnation,
               NEW.pool_id,
               NEW.manager_input_digest,
               NEW.manager_allocation_digest,
               NEW.proposal_digest,
               NEW.expires_at
             ) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'executable admission proposal payload is not exact'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _revoke_public_execute(
        "capacity_executable_admission_proposal_insert_guard()"
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_admission_proposal_insert_guard
        BEFORE INSERT ON public.capacity_executable_admission_proposals
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_admission_proposal_insert_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_admission_ack_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          authority_record record;
          epoch_record record;
          allocation_record record;
          proposal_record record;
          execution_payload jsonb;
        BEGIN
          execution_payload := NEW.acknowledgement_payload -> 'execution';
          SELECT authority.* INTO authority_record
            FROM public.capacity_authority_state AS authority
           WHERE authority.singleton_id = 1
           FOR SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'executable admission acknowledgement execution fence changed'
              USING ERRCODE = '23514';
          END IF;
          SELECT epoch.* INTO epoch_record
            FROM public.capacity_execution_epochs AS epoch
           WHERE epoch.execution_epoch = authority_record.execution_epoch
           FOR SHARE;
          IF NOT FOUND
             OR authority_record.authority_incarnation
                  IS DISTINCT FROM (execution_payload ->> 'authority_incarnation')::uuid
             OR authority_record.writer_epoch
                  IS DISTINCT FROM (execution_payload ->> 'writer_epoch')::bigint
             OR (execution_payload ->> 'execution_epoch')::bigint
                  IS DISTINCT FROM NEW.execution_epoch
             OR authority_record.execution_epoch IS DISTINCT FROM NEW.execution_epoch
             OR execution_payload ->> 'execution_manifest_sha256'
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR authority_record.execution_state
                  IS DISTINCT FROM execution_payload ->> 'execution_state'
             OR authority_record.execution_manifest_sha256
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR authority_record.executable_new_capacity_ceiling
                  IS DISTINCT FROM (
                    execution_payload ->> 'executable_new_capacity_ceiling'
                  )::bigint
             OR epoch_record.configuration_epoch
                  IS DISTINCT FROM (execution_payload ->> 'configuration_epoch')::bigint
             OR epoch_record.execution_epoch IS DISTINCT FROM NEW.execution_epoch
             OR epoch_record.execution_manifest_sha256
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR epoch_record.state IS DISTINCT FROM execution_payload ->> 'execution_state'
             OR epoch_record.effective_ceiling
                  IS DISTINCT FROM (
                    execution_payload ->> 'executable_new_capacity_ceiling'
                  )::bigint
             OR epoch_record.effective_rate_per_minute
                  IS DISTINCT FROM (
                    execution_payload ->> 'executable_new_capacity_rate_per_minute'
                  )::bigint
             OR epoch_record.trusted_fleet_release_sha256
                  IS DISTINCT FROM execution_payload ->> 'trusted_fleet_release_sha256'
             OR execution_payload -> 'executable' IS DISTINCT FROM 'true'::jsonb THEN
            RAISE EXCEPTION 'executable admission acknowledgement execution fence changed'
              USING ERRCODE = '23514';
          END IF;
          SELECT allocation.* INTO allocation_record
            FROM public.capacity_allocation_epochs AS allocation
           WHERE allocation.status = 'executable'
             AND allocation.executable
             AND allocation.sealed
             AND allocation.execution_epoch = epoch_record.execution_epoch
             AND allocation.execution_manifest_sha256
                  = epoch_record.execution_manifest_sha256
           ORDER BY allocation.allocation_epoch DESC
           LIMIT 1
           FOR SHARE;
          IF NOT FOUND
             OR allocation_record.allocation_epoch IS DISTINCT FROM NEW.allocation_epoch
             OR (execution_payload ->> 'allocation_epoch')::bigint
                  IS DISTINCT FROM NEW.allocation_epoch THEN
            RAISE EXCEPTION 'executable admission acknowledgement allocation changed'
              USING ERRCODE = '23514';
          END IF;
          IF allocation_record.input_valid_until IS NULL
             OR allocation_record.input_valid_until <= pg_catalog.clock_timestamp() THEN
            RAISE EXCEPTION
              'executable admission acknowledgement allocation changed or expired'
              USING ERRCODE = '23514';
          END IF;
          PERFORM 1
            FROM public.capacity_demand_reporters AS reporter
           WHERE reporter.subject_id = NEW.subject_id
             AND reporter.subject_incarnation = NEW.subject_incarnation
             AND reporter.reporter_incarnation = NEW.reporter_incarnation
             AND reporter.state = 'current'
           FOR SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'executable admission acknowledgement reporter changed'
              USING ERRCODE = '23514';
          END IF;
          PERFORM 1
            FROM public.capacity_executable_intents AS intent
           WHERE intent.tranche_id = NEW.tranche_id
           ORDER BY intent.launch_rank, intent.intent_id
           FOR SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'executable admission acknowledgement intent set changed'
              USING ERRCODE = '23514';
          END IF;
          SELECT proposal.* INTO proposal_record
            FROM public.capacity_executable_admission_proposals AS proposal
           WHERE proposal.proposal_id = NEW.proposal_id
             AND proposal.proposal_digest = NEW.proposal_digest
           FOR UPDATE;
          IF NOT FOUND
             OR proposal_record.expires_at <= pg_catalog.clock_timestamp()
             OR proposal_record.plan_id IS DISTINCT FROM NEW.plan_id
             OR proposal_record.admission_incarnation
                  IS DISTINCT FROM NEW.admission_incarnation
             OR proposal_record.tranche_id IS DISTINCT FROM NEW.tranche_id
             OR proposal_record.execution_epoch IS DISTINCT FROM NEW.execution_epoch
             OR proposal_record.execution_manifest_sha256
                  IS DISTINCT FROM NEW.execution_manifest_sha256
             OR proposal_record.allocation_epoch IS DISTINCT FROM NEW.allocation_epoch
             OR proposal_record.subject_id IS DISTINCT FROM NEW.subject_id
             OR proposal_record.subject_incarnation IS DISTINCT FROM NEW.subject_incarnation
             OR proposal_record.pool_id IS DISTINCT FROM NEW.pool_id
             OR proposal_record.reporter_incarnation IS DISTINCT FROM NEW.reporter_incarnation
             OR proposal_record.protected_admission_sha256
                  IS DISTINCT FROM NEW.protected_admission_sha256
             OR NEW.acknowledgement_payload ->> 'proposal_id'
                  IS DISTINCT FROM NEW.proposal_id::text
             OR NEW.acknowledgement_payload ->> 'plan_id'
                  IS DISTINCT FROM NEW.plan_id::text
             OR NEW.acknowledgement_payload ->> 'admission_incarnation'
                  IS DISTINCT FROM NEW.admission_incarnation::text
             OR NEW.acknowledgement_payload ->> 'tranche_id'
                  IS DISTINCT FROM NEW.tranche_id::text
             OR NEW.acknowledgement_payload ->> 'subject_id'
                  IS DISTINCT FROM NEW.subject_id::text
             OR NEW.acknowledgement_payload ->> 'subject_incarnation'
                  IS DISTINCT FROM NEW.subject_incarnation::text
             OR NEW.acknowledgement_payload ->> 'pool_id' IS DISTINCT FROM NEW.pool_id
             OR NEW.acknowledgement_payload ->> 'reporter_incarnation'
                  IS DISTINCT FROM NEW.reporter_incarnation::text
             OR NEW.acknowledgement_payload ->> 'protected_admission_sha256'
                  IS DISTINCT FROM NEW.protected_admission_sha256
             OR NEW.acknowledgement_payload ->> 'proposal_digest'
                  IS DISTINCT FROM NEW.proposal_digest
             OR NEW.acknowledgement_payload ->> 'prepared_plan_digest'
                  IS DISTINCT FROM NEW.prepared_plan_digest
             OR (NEW.acknowledgement_payload ->> 'assignment_count')::bigint
                  IS DISTINCT FROM pg_catalog.jsonb_array_length(
                    NEW.acknowledgement_payload -> 'assignments'
                  )
             OR NEW.acknowledgement_payload -> 'executable'
                  IS DISTINCT FROM 'true'::jsonb THEN
            RAISE EXCEPTION 'executable admission acknowledgement binding changed'
              USING ERRCODE = '23514';
          END IF;
          IF EXISTS (
               SELECT 1
                 FROM public.capacity_executable_admission_closure_acknowledgements
                      AS closed
                WHERE closed.proposal_id = NEW.proposal_id
             ) THEN
            RAISE EXCEPTION 'executable admission acknowledgement binding changed'
              USING ERRCODE = '23514';
          END IF;
          IF public.capacity_executable_admission_proposal_payload_is_exact(
               proposal_record.proposal_payload,
               proposal_record.tranche_id,
               proposal_record.execution_epoch,
               proposal_record.allocation_epoch,
               proposal_record.subject_id,
               proposal_record.subject_incarnation,
               proposal_record.pool_id,
               proposal_record.manager_input_digest,
               proposal_record.manager_allocation_digest,
               proposal_record.proposal_digest,
               proposal_record.expires_at
             ) IS DISTINCT FROM true THEN
            RAISE EXCEPTION
              'executable admission acknowledgement proposal payload is not exact'
              USING ERRCODE = '23514';
          END IF;
          IF public.capacity_executable_admission_ack_payload_is_exact(
               NEW.acknowledgement_payload,
               proposal_record.proposal_payload
             ) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'executable admission acknowledgement payload is not exact'
              USING ERRCODE = '23514';
          END IF;
          IF pg_catalog.jsonb_array_length(
               proposal_record.proposal_payload -> 'shapes'
             ) IS DISTINCT FROM (
               SELECT count(*)
                 FROM public.capacity_executable_intents AS intent
                WHERE intent.tranche_id = NEW.tranche_id
             )
             OR EXISTS (
                  SELECT 1
                    FROM public.capacity_executable_intents AS intent
                   WHERE intent.tranche_id = NEW.tranche_id
                     AND NOT EXISTS (
                           SELECT 1
                             FROM pg_catalog.jsonb_array_elements(
                                    proposal_record.proposal_payload -> 'shapes'
                                  ) AS shape
                            WHERE shape -> 'binding' = intent.binding_payload
                         )
                )
             OR EXISTS (
                  SELECT 1
                    FROM pg_catalog.jsonb_array_elements(
                           proposal_record.proposal_payload -> 'shapes'
                         ) AS shape
                   WHERE NOT EXISTS (
                           SELECT 1
                             FROM public.capacity_executable_intents AS intent
                            WHERE intent.tranche_id = NEW.tranche_id
                              AND intent.binding_payload = shape -> 'binding'
                         )
                ) THEN
            RAISE EXCEPTION 'executable admission acknowledgement intent set changed'
              USING ERRCODE = '23514';
          END IF;
          IF EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(
                     NEW.acknowledgement_payload -> 'assignments'
                   ) AS assignment
             WHERE pg_catalog.jsonb_typeof(assignment) IS DISTINCT FROM 'object'
                OR NOT (
                     assignment ?& ARRAY[
                       'schema_version',
                       'transition_id',
                       'allowance_id',
                       'protected_attempt_id',
                       'execution_generation',
                       'requirements_digest',
                       'shape_instance_id',
                       'shape_slot_index',
                       'submission_intent_id',
                       'lifecycle_sequence'
                     ]
                   )
                OR (
                     SELECT count(*)
                       FROM pg_catalog.jsonb_object_keys(assignment)
                   ) IS DISTINCT FROM 10::bigint
                OR assignment -> 'schema_version' IS DISTINCT FROM '2'::jsonb
                OR (assignment ->> 'transition_id'
                     ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                     IS DISTINCT FROM true
                OR (assignment ->> 'allowance_id'
                     ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                     IS DISTINCT FROM true
                OR (assignment ->> 'protected_attempt_id'
                     ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                     IS DISTINCT FROM true
                OR (assignment ->> 'submission_intent_id'
                     ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
                     IS DISTINCT FROM true
                OR (assignment ->> 'execution_generation' ~ '^[1-9][0-9]*$')
                     IS DISTINCT FROM true
                OR (assignment ->> 'requirements_digest' ~ '^[0-9a-f]{64}$')
                     IS DISTINCT FROM true
                OR pg_catalog.jsonb_typeof(assignment -> 'shape_slot_index')
                     IS DISTINCT FROM 'number'
                OR (assignment ->> 'shape_slot_index' ~ '^(0|[1-9][0-9]*)$')
                     IS DISTINCT FROM true
                OR (assignment ->> 'lifecycle_sequence' ~ '^[1-9][0-9]*$')
                     IS DISTINCT FROM true
          ) THEN
            RAISE EXCEPTION
              'executable admission acknowledgement local assignment facts changed'
              USING ERRCODE = '23514';
          END IF;
          IF EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(
                     proposal_record.proposal_payload -> 'allowances'
                   ) AS allowance
             WHERE NOT EXISTS (
               SELECT 1
                 FROM pg_catalog.jsonb_array_elements(
                        NEW.acknowledgement_payload -> 'assignments'
                      ) AS assignment
                WHERE assignment ->> 'allowance_id' = allowance ->> 'allowance_id'
                  AND assignment ->> 'protected_attempt_id'
                      = allowance ->> 'protected_attempt_id'
                  AND assignment ->> 'shape_instance_id'
                      = allowance ->> 'shape_instance_id'
                  AND (assignment ->> 'shape_slot_index')::bigint
                      = (allowance ->> 'shape_slot_index')::bigint
                  AND assignment ->> 'submission_intent_id'
                      = allowance ->> 'submission_intent_id'
             )
          ) OR pg_catalog.jsonb_array_length(
                 proposal_record.proposal_payload -> 'allowances'
               ) IS DISTINCT FROM pg_catalog.jsonb_array_length(
                 NEW.acknowledgement_payload -> 'assignments'
               ) THEN
            RAISE EXCEPTION 'executable admission acknowledgement is incomplete'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _revoke_public_execute("capacity_executable_admission_ack_insert_guard()")
    op.execute(
        """
        CREATE TRIGGER capacity_executable_admission_ack_insert_guard
        BEFORE INSERT ON public.capacity_executable_admission_acknowledgements
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_admission_ack_insert_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_admission_closure_ack_insert_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          proposal_record record;
          expected_hex text;
          expected_closure_id uuid;
        BEGIN
          SELECT proposal.* INTO proposal_record
            FROM public.capacity_executable_admission_proposals AS proposal
           WHERE proposal.proposal_id = NEW.proposal_id
             AND proposal.proposal_digest = NEW.proposal_digest
           FOR UPDATE;
          expected_hex := pg_catalog.encode(
            pg_catalog.sha256(
              pg_catalog.convert_to(
                'admission-closure:' || NEW.proposal_id::text || ':' ||
                NEW.proposal_digest || ':' || NEW.close_reason,
                'UTF8'
              )
            ),
            'hex'
          );
          expected_closure_id := (
            pg_catalog.substr(expected_hex, 1, 8) || '-' ||
            pg_catalog.substr(expected_hex, 9, 4) || '-' ||
            pg_catalog.substr(expected_hex, 13, 4) || '-' ||
            pg_catalog.substr(expected_hex, 17, 4) || '-' ||
            pg_catalog.substr(expected_hex, 21, 12)
          )::uuid;
          IF NOT FOUND
             OR NEW.closure_id IS DISTINCT FROM expected_closure_id
             OR proposal_record.plan_id IS DISTINCT FROM NEW.plan_id
             OR proposal_record.admission_incarnation
                  IS DISTINCT FROM NEW.admission_incarnation
             OR proposal_record.subject_id IS DISTINCT FROM NEW.subject_id
             OR proposal_record.subject_incarnation
                  IS DISTINCT FROM NEW.subject_incarnation
             OR proposal_record.reporter_incarnation
                  IS DISTINCT FROM NEW.reporter_incarnation
             OR proposal_record.protected_admission_sha256
                  IS DISTINCT FROM NEW.protected_admission_sha256
             OR NOT (
                  NEW.acknowledgement_payload ?& ARRAY[
                    'schema_version',
                    'closure_id',
                    'proposal_id',
                    'proposal_digest',
                    'plan_id',
                    'admission_incarnation',
                    'subject_id',
                    'subject_incarnation',
                    'reporter_incarnation',
                    'protected_admission_sha256',
                    'close_reason',
                    'disposition_kind',
                    'disposition_digest',
                    'executable'
                  ]
                )
             OR (
                  SELECT count(*)
                    FROM pg_catalog.jsonb_object_keys(NEW.acknowledgement_payload)
                ) IS DISTINCT FROM 14::bigint
             OR NEW.acknowledgement_payload -> 'schema_version'
                  IS DISTINCT FROM '2'::jsonb
             OR NEW.acknowledgement_payload ->> 'closure_id'
                  IS DISTINCT FROM NEW.closure_id::text
             OR NEW.acknowledgement_payload ->> 'proposal_id'
                  IS DISTINCT FROM NEW.proposal_id::text
             OR NEW.acknowledgement_payload ->> 'proposal_digest'
                  IS DISTINCT FROM NEW.proposal_digest
             OR NEW.acknowledgement_payload ->> 'plan_id'
                  IS DISTINCT FROM NEW.plan_id::text
             OR NEW.acknowledgement_payload ->> 'admission_incarnation'
                  IS DISTINCT FROM NEW.admission_incarnation::text
             OR NEW.acknowledgement_payload ->> 'subject_id'
                  IS DISTINCT FROM NEW.subject_id::text
             OR NEW.acknowledgement_payload ->> 'subject_incarnation'
                  IS DISTINCT FROM NEW.subject_incarnation::text
             OR NEW.acknowledgement_payload ->> 'reporter_incarnation'
                  IS DISTINCT FROM NEW.reporter_incarnation::text
             OR NEW.acknowledgement_payload ->> 'protected_admission_sha256'
                  IS DISTINCT FROM NEW.protected_admission_sha256
             OR NEW.acknowledgement_payload ->> 'close_reason'
                  IS DISTINCT FROM NEW.close_reason
             OR NEW.acknowledgement_payload ->> 'disposition_kind'
                  IS DISTINCT FROM NEW.disposition_kind
             OR NEW.acknowledgement_payload ->> 'disposition_digest'
                  IS DISTINCT FROM NEW.disposition_digest
             OR NEW.acknowledgement_payload -> 'executable'
                  IS DISTINCT FROM 'false'::jsonb
             OR pg_catalog.encode(
                  pg_catalog.sha256(
                    pg_catalog.convert_to(
                      public.capacity_executable_canonical_jsonb_text(
                        NEW.acknowledgement_payload
                      ),
                      'UTF8'
                    )
                  ),
                  'hex'
                ) IS DISTINCT FROM NEW.acknowledgement_digest
             OR (NEW.close_reason = 'expired'
                 AND proposal_record.expires_at > pg_catalog.clock_timestamp())
             OR (NEW.close_reason = 'manager-closed'
                 AND NOT EXISTS (
                   SELECT 1
                     FROM public.capacity_executable_intents AS intent
                    WHERE intent.tranche_id = proposal_record.tranche_id
                      AND intent.state IN ('closing', 'released')
                 ))
             OR (NEW.close_reason = 'allocation-superseded'
                 AND EXISTS (
                   SELECT 1
                     FROM public.capacity_authority_state AS authority
                     JOIN public.capacity_execution_epochs AS epoch
                       ON epoch.execution_epoch = authority.execution_epoch
                     JOIN LATERAL (
                       SELECT allocation.*
                         FROM public.capacity_allocation_epochs AS allocation
                        WHERE allocation.status = 'executable'
                          AND allocation.executable
                          AND allocation.sealed
                          AND allocation.execution_epoch = epoch.execution_epoch
                          AND allocation.execution_manifest_sha256
                              = epoch.execution_manifest_sha256
                        ORDER BY allocation.allocation_epoch DESC
                        LIMIT 1
                     ) AS latest ON true
                    WHERE authority.singleton_id = 1
                      AND proposal_record.execution_epoch = epoch.execution_epoch
                      AND proposal_record.execution_manifest_sha256
                          = epoch.execution_manifest_sha256
                      AND proposal_record.allocation_epoch = latest.allocation_epoch
                      AND latest.input_valid_until > pg_catalog.clock_timestamp()
                 ))
             OR NOT EXISTS (
                  SELECT 1
                    FROM public.capacity_demand_reporters AS reporter
                   WHERE reporter.subject_id = NEW.subject_id
                     AND reporter.subject_incarnation = NEW.subject_incarnation
                     AND reporter.reporter_incarnation = NEW.reporter_incarnation
                     AND reporter.state = 'current'
                )
             OR EXISTS (
                  SELECT 1
                    FROM public.capacity_executable_admission_proposals AS prior
                   WHERE prior.subject_id = NEW.subject_id
                     AND prior.subject_incarnation = NEW.subject_incarnation
                     AND prior.reporter_incarnation = NEW.reporter_incarnation
                     AND (prior.created_at, prior.id)
                         < (proposal_record.created_at, proposal_record.id)
                     AND NOT EXISTS (
                       SELECT 1
                         FROM public.capacity_executable_admission_acknowledgements
                              AS admitted_prior
                        WHERE admitted_prior.proposal_id = prior.proposal_id
                     )
                     AND NOT EXISTS (
                       SELECT 1
                         FROM public.capacity_executable_admission_closure_acknowledgements
                              AS closed_prior
                        WHERE closed_prior.proposal_id = prior.proposal_id
                     )
                )
             OR EXISTS (
                  SELECT 1
                    FROM public.capacity_executable_admission_acknowledgements AS admitted
                   WHERE admitted.proposal_id = NEW.proposal_id
                ) THEN
            RAISE EXCEPTION 'executable admission closure acknowledgement binding changed'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _revoke_public_execute(
        "capacity_executable_admission_closure_ack_insert_guard()"
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_admission_closure_ack_insert_guard
        BEFORE INSERT ON public.capacity_executable_admission_closure_acknowledgements
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_admission_closure_ack_insert_guard()
        """
    )
    _install_intent_admission_guard()


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.capacity_executable_admission_closure_acknowledgements, "
        "public.capacity_executable_admission_acknowledgements, "
        "public.capacity_executable_admission_proposals, "
        "public.capacity_executable_intents IN ACCESS EXCLUSIVE MODE"
    )
    bind = op.get_bind()
    if bind.execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_executable_admission_closure_acknowledgements"
            ") OR EXISTS ("
            "SELECT 1 FROM public.capacity_executable_admission_acknowledgements"
            ") OR EXISTS ("
            "SELECT 1 FROM public.capacity_executable_admission_proposals)"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade capacity_0014 while protected admission evidence exists"
        )
    if bind.execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_execution_epochs "
            "WHERE requested_ceiling <> 1) OR EXISTS ("
            "SELECT 1 FROM public.capacity_executable_intents "
            "WHERE state = 'bootstrap-acknowledged' "
            "OR (state IN ('closing', 'released', 'quarantined') "
            "AND bootstrap_registration_epoch IS NOT NULL "
            "AND bootstrap_evidence_sha256 IS NOT NULL "
            "AND launch_ready_at IS NULL)) OR EXISTS ("
            "SELECT 1 FROM public.capacity_executable_intents "
            "GROUP BY tranche_id HAVING count(*) > 1)"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade capacity_0014 with capacity_0014-only executable history"
        )

    _extend_intent_state_machine(downgrade=True)
    op.execute(
        "DROP TRIGGER capacity_executable_intent_protected_bootstrap_guard "
        "ON public.capacity_executable_intents"
    )
    op.execute("DROP FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()")
    op.execute(
        "DROP TRIGGER capacity_executable_admission_ack_insert_guard "
        "ON public.capacity_executable_admission_acknowledgements"
    )
    op.execute("DROP FUNCTION public.capacity_executable_admission_ack_insert_guard()")
    op.execute(
        "DROP TRIGGER capacity_executable_admission_closure_ack_insert_guard "
        "ON public.capacity_executable_admission_closure_acknowledgements"
    )
    op.execute(
        "DROP FUNCTION public.capacity_executable_admission_closure_ack_insert_guard()"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_admission_proposal_insert_guard "
        "ON public.capacity_executable_admission_proposals"
    )
    op.execute("DROP FUNCTION public.capacity_executable_admission_proposal_insert_guard()")
    for table_name in (
        "capacity_executable_admission_closure_acknowledgements",
        "capacity_executable_admission_acknowledgements",
        "capacity_executable_admission_proposals",
    ):
        op.execute(f"DROP TRIGGER {table_name}_truncate_guard ON public.{table_name}")
        op.execute(f"DROP TRIGGER {table_name}_append_only_guard ON public.{table_name}")
    op.drop_table(
        "capacity_executable_admission_closure_acknowledgements",
        schema="public",
    )
    op.drop_table("capacity_executable_admission_acknowledgements", schema="public")
    op.drop_table("capacity_executable_admission_proposals", schema="public")
    op.execute(
        "DROP FUNCTION "
        "public.capacity_executable_admission_ack_payload_is_exact(jsonb,jsonb)"
    )
    op.execute(
        "DROP FUNCTION "
        "public.capacity_executable_admission_proposal_payload_is_exact("
        "jsonb,uuid,bigint,bigint,uuid,uuid,text,text,text,text,timestamptz)"
    )
    op.execute(
        "DROP FUNCTION public.capacity_executable_canonical_jsonb_text(jsonb)"
    )

    op.drop_constraint(
        "capacity_executable_intent_tranche_fkey",
        "capacity_executable_intents",
        schema="public",
        type_="foreignkey",
    )
    op.drop_constraint(
        "capacity_executable_tranche_intent_key",
        "capacity_executable_intents",
        schema="public",
        type_="unique",
    )
    op.drop_constraint(
        "capacity_executable_intent_state_check",
        "capacity_executable_intents",
        schema="public",
        type_="check",
    )
    op.drop_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        schema="public",
        type_="check",
    )
    op.execute(
        "DROP TRIGGER capacity_executable_tranches_truncate_guard "
        "ON public.capacity_executable_tranches"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_tranches_append_only_guard "
        "ON public.capacity_executable_tranches"
    )
    op.drop_table("capacity_executable_tranches", schema="public")
    op.create_unique_constraint(
        "capacity_executable_tranche_identity_key",
        "capacity_executable_intents",
        ["tranche_id"],
        schema="public",
    )
    op.create_check_constraint(
        "capacity_executable_intent_state_check",
        "capacity_executable_intents",
        "state IN ('proposed','accepted','launch-ready','permitted',"
        "'submitting-unknown','bound','observed','terminal','closing','released',"
        "'quarantined')",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_execution_epoch_quantity_check",
        "capacity_execution_epochs",
        "execution_epoch > 0 AND prepared_writer_epoch > 0 "
        "AND current_writer_epoch > 0 AND configuration_epoch > 0 "
        "AND fleet_generation > 0 AND oldlab_pool_generation > 0 "
        "AND gb10_pool_generation > 0 AND requested_ceiling = 1 "
        "AND effective_ceiling >= 0 AND effective_ceiling <= requested_ceiling "
        "AND requested_rate_per_minute > 0 AND effective_rate_per_minute >= 0 "
        "AND effective_rate_per_minute <= requested_rate_per_minute",
        schema="public",
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          IF OLD.state = 'accepted' AND NEW.state = 'launch-ready' THEN
            PERFORM 1
              FROM public.capacity_executable_bootstrap_acknowledgements AS acknowledgement
             WHERE acknowledgement.intent_id = NEW.intent_id
               AND acknowledgement.execution_epoch = NEW.execution_epoch
               AND acknowledgement.execution_manifest_sha256
                    = NEW.execution_manifest_sha256
               AND acknowledgement.bootstrap_registration_epoch
                    = NEW.bootstrap_registration_epoch
               AND acknowledgement.bootstrap_evidence_sha256
                    = NEW.bootstrap_evidence_sha256;
            IF NOT FOUND THEN
              RAISE EXCEPTION
                'executable intent launch readiness requires protected bootstrap acknowledgement'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _revoke_public_execute(
        "capacity_executable_intent_protected_bootstrap_guard()"
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_intent_protected_bootstrap_guard
        BEFORE UPDATE ON public.capacity_executable_intents
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_intent_protected_bootstrap_guard()
        """
    )
