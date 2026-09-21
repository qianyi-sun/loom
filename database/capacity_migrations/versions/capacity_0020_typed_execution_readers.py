"""Dispatch same-epoch typed application execution through purpose-aware SQL.

Revision ID: capacity_0020
Revises: capacity_0019

Build execution and inherited source graphs remain explicitly interlocked.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0020"
down_revision: str | Sequence[str] | None = "capacity_0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_READERS = {
    "event_prefix": "bigint,bigint",
    "pinned_subject": "bigint,uuid,uuid",
    "target_current": "bigint,uuid,uuid",
    "cleanup_reporter": "jsonb,uuid",
}
_GUARDS = (
    "capacity_executable_admission_closure_ack_insert_guard",
    "capacity_personal_build_initial_insert_guard",
)


def _once(value: str, old: str, new: str) -> str:
    if value.count(old) != 1:
        raise RuntimeError(f"capacity_0020 SQL reader drift: {old[:100]}")
    return value.replace(old, new, 1)


def _definition(name: str, signature: str) -> str:
    return str(op.get_bind().scalar(sa.text(
        "SELECT pg_get_functiondef(CAST(:name AS regprocedure))"
    ), {"name": f"public.{name}({signature})"}))


def _rename(value: str, old: str, new: str) -> str:
    return _once(value, f"FUNCTION public.{old}(", f"FUNCTION public.{new}(")


def _prefix(definition: str) -> str:
    definition = _once(definition,
        "identity text; incarnation uuid; calculated text;",
        """identity text; incarnation uuid; calculated text;
          old_config jsonb; old_projection jsonb; old_ack jsonb; base_origin jsonb;
          fresh_reporter boolean; mutation_kind text;""")
    definition = _once(definition,
        "epoch_record.manifest_payload -> 'schema_version' IS DISTINCT FROM '3'::jsonb",
        "epoch_record.manifest_payload -> 'schema_version' IS DISTINCT FROM '4'::jsonb")
    definition = _once(definition,
        "          SELECT * INTO STRICT base_record FROM public.capacity_configuration_epochs",
        """          IF nullif(epoch_record.manifest_payload -> 'retired_source', 'null'::jsonb) IS NOT NULL
             OR epoch_record.manifest_payload -> 'managed_build_origins' IS DISTINCT FROM '[]'::jsonb THEN
            RAISE EXCEPTION 'typed execution source graph is not yet connected' USING ERRCODE='23514';
          END IF;
          SELECT * INTO STRICT base_record FROM public.capacity_configuration_epochs""")
    definition = _once(definition,
        "projection := event_record.request_payload -> 'projection';",
        "projection := event_record.request_payload #> '{command,projection}';")
    definition = _once(definition,
        "            IF event_record.revision IS DISTINCT FROM next_revision",
        """            IF event_record.request_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
               OR event_record.result_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb
               OR event_record.request_payload #> '{command,schema_version}' IS DISTINCT FROM '2'::jsonb
               OR member -> 'schema_version' IS DISTINCT FROM '1'::jsonb
               OR (member ->> 'purpose' IN ('personal-application','personal-build-worker')) IS NOT TRUE
               OR member -> 'purpose' IS DISTINCT FROM event_record.request_payload #> '{command,purpose}'
               OR (prior IS NOT NULL AND member -> 'purpose' IS DISTINCT FROM prior -> 'purpose')
               OR event_record.authority_incarnation IS DISTINCT FROM epoch_record.authority_incarnation
               OR event_record.writer_epoch IS DISTINCT FROM epoch_record.prepared_writer_epoch
               OR event_record.request_payload #> '{execution,configuration_epoch}' IS DISTINCT FROM to_jsonb(epoch_record.configuration_epoch)
               OR event_record.request_payload #>> '{execution,trusted_fleet_release_sha256}' IS DISTINCT FROM epoch_record.trusted_fleet_release_sha256
               OR event_record.revision IS DISTINCT FROM next_revision""")
    definition = _once(definition,
        "OR projection ->> 'subject_id' IS DISTINCT FROM identity",
        """OR (member ->> 'purpose' = 'personal-application' AND projection ->> 'subject_id' IS DISTINCT FROM identity)
               OR (member ->> 'purpose' = 'personal-build-worker' AND
                   public.capacity_personal_build_subject_id(event_record.namespace_id, event_record.owner_id)::text IS DISTINCT FROM identity)""")
    definition = _once(definition,
        "OR subject ->> 'display_name' IS DISTINCT FROM 'dev-' || (projection ->> 'environment_name')",
        """OR subject ->> 'display_name' IS DISTINCT FROM (CASE WHEN member ->> 'purpose' = 'personal-build-worker'
                    THEN 'dev-build-' || replace(event_record.owner_id::text,'-','')
                    ELSE 'dev-' || (projection ->> 'environment_name') END)""")
    definition = _once(definition,
        "CASE WHEN projection ->> 'operation_kind' = 'destroy' THEN '0'::jsonb ELSE projection -> 'min_slots' END",
        "CASE WHEN member ->> 'purpose' = 'personal-build-worker' OR projection ->> 'operation_kind' = 'destroy' THEN '0'::jsonb ELSE projection -> 'min_slots' END")
    definition = _once(definition,
        "acknowledgement IS DISTINCT FROM event_record.request_payload -> 'acknowledgement'",
        "acknowledgement IS DISTINCT FROM event_record.request_payload #> '{command,acknowledgement}'")
    definition = _once(definition,
        "OR acknowledgement -> 'candidate' ->> 'algorithm' IS DISTINCT FROM 'source-sha256'",
        "OR (member ->> 'purpose' = 'personal-application' AND (acknowledgement -> 'candidate' ->> 'algorithm' IS DISTINCT FROM 'source-sha256'")
    definition = _once(definition,
        "OR acknowledgement -> 'protected_admission_sha256' IS DISTINCT FROM projection -> 'protected_admission_sha256' THEN",
        """OR acknowledgement -> 'protected_admission_sha256' IS DISTINCT FROM projection -> 'protected_admission_sha256'))
               OR (member ->> 'purpose' = 'personal-build-worker' AND (
                    acknowledgement -> 'candidate' IS DISTINCT FROM epoch_record.manifest_payload #> '{personal_builds,runtime_candidate}'
                    OR subject -> 'profiles' IS DISTINCT FROM epoch_record.manifest_payload #> '{personal_builds,profiles}'
                    OR subject -> 'rollout_surge_slots' IS DISTINCT FROM '0'::jsonb
                    OR (subject ->> 'max_slots')::bigint > (epoch_record.manifest_payload #>> '{personal_builds,max_slots_per_subject}')::bigint
                    OR subject -> 'max_pending_slots' IS DISTINCT FROM epoch_record.manifest_payload #> '{personal_builds,max_pending_slots_per_subject}'
                    OR subject -> 'max_pending_jobs' IS DISTINCT FROM epoch_record.manifest_payload #> '{personal_builds,max_pending_jobs_per_subject}')) THEN""")
    definition = _once(definition,
        "event_record.request_payload -> 'projection' ->> 'operation_kind'",
        "event_record.request_payload #>> '{command,projection,operation_kind}'")
    definition = _once(definition,
        "            IF origin IS NULL THEN",
        """            old_config := prior -> 'configuration'; old_ack := prior -> 'acknowledgement';
            old_projection := NULL; base_origin := NULL;
            IF prior IS NOT NULL THEN
              SELECT request_payload #> '{command,projection}' INTO STRICT old_projection
                FROM public.capacity_personal_membership_events
                WHERE execution_epoch=p_epoch AND revision=(prior ->> 'revision')::bigint;
            ELSE
              SELECT value INTO base_origin FROM jsonb_array_elements(epoch_record.manifest_payload -> 'managed_application_origins')
                WHERE value #>> '{configuration,subject_id}'=identity;
              IF base_origin IS NOT NULL THEN
                IF member ->> 'purpose' IS DISTINCT FROM 'personal-application'
                   OR base_origin -> 'schema_version' IS DISTINCT FROM '1'::jsonb THEN
                  RAISE EXCEPTION 'typed lifecycle base purpose changed' USING ERRCODE='23514';
                END IF;
                old_config := base_origin -> 'configuration'; old_ack := base_origin -> 'acknowledgement';
                old_projection := base_origin -> 'base_projection';
              END IF;
            END IF;
            mutation_kind := projection ->> 'operation_kind';
            IF member ->> 'purpose'='personal-application'
               AND subject -> 'candidate_generation' IS DISTINCT FROM subject -> 'deployment_generation' THEN
              RAISE EXCEPTION 'typed application lifecycle candidate must match deployment generation' USING ERRCODE='23514';
            END IF;
            fresh_reporter := NOT EXISTS (
              SELECT 1 FROM public.capacity_personal_membership_events e
              WHERE e.execution_epoch=p_epoch AND e.revision<event_record.revision
                AND (e.reporter_incarnation=event_record.reporter_incarnation
                  OR e.request_payload #> '{command,projection,demand_reporter_token_sha256}'=projection -> 'demand_reporter_token_sha256'))
              AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(epoch_record.manifest_payload -> 'managed_application_origins')
                WHERE value #> '{base_projection,demand_reporter_incarnation}'=projection -> 'demand_reporter_incarnation'
                  OR value #> '{base_projection,demand_reporter_token_sha256}'=projection -> 'demand_reporter_token_sha256');
            IF mutation_kind IN ('create','update') AND NOT fresh_reporter THEN
              RAISE EXCEPTION 'typed lifecycle reporter must rotate' USING ERRCODE='23514';
            END IF;
            IF old_config IS NULL THEN
              IF mutation_kind IS DISTINCT FROM 'create' OR incarnation=ANY(used)
                 OR subject -> 'candidate_generation' IS DISTINCT FROM '1'::jsonb
                 OR subject -> 'deployment_generation' IS DISTINCT FROM '1'::jsonb THEN
                RAISE EXCEPTION 'typed lifecycle requires fresh initial service' USING ERRCODE='23514';
              END IF;
            ELSE
              IF old_projection -> 'owner_id' IS DISTINCT FROM projection -> 'owner_id'
                 OR old_config -> 'display_name' IS DISTINCT FROM subject -> 'display_name'
                 OR (old_config ->> 'configuration_generation')::bigint >= (subject ->> 'configuration_generation')::bigint THEN
                RAISE EXCEPTION 'typed lifecycle predecessor identity changed' USING ERRCODE='23514';
              END IF;
              IF mutation_kind <> 'create' THEN
                IF old_config ->> 'lifecycle_state' IS DISTINCT FROM 'active'
                   OR old_config -> 'subject_incarnation' IS DISTINCT FROM subject -> 'subject_incarnation' THEN
                  RAISE EXCEPTION 'typed lifecycle predecessor not active' USING ERRCODE='23514';
                END IF;
                IF mutation_kind='update' THEN
                  IF (subject ->> 'deployment_generation')::bigint <= (old_config ->> 'deployment_generation')::bigint
                     OR (subject ->> 'candidate_generation')::bigint < (old_config ->> 'candidate_generation')::bigint
                     OR (member ->> 'purpose'='personal-application' AND
                         (subject ->> 'candidate_generation')::bigint <= (old_config ->> 'candidate_generation')::bigint) THEN
                    RAISE EXCEPTION 'typed lifecycle update generations must advance' USING ERRCODE='23514';
                  END IF;
                ELSIF mutation_kind NOT IN ('capacity','destroy')
                   OR projection - ARRAY['expected_configuration_epoch','operation_kind','operation_id','operation_epoch','configuration_generation','min_slots','max_slots']
                     IS DISTINCT FROM old_projection - ARRAY['expected_configuration_epoch','operation_kind','operation_id','operation_epoch','configuration_generation','min_slots','max_slots']
                   OR acknowledgement - ARRAY['configuration_generation','acknowledgement_sha256']
                     IS DISTINCT FROM old_ack - ARRAY['configuration_generation','acknowledgement_sha256'] THEN
                  RAISE EXCEPTION 'typed lifecycle nondeployment evidence changed' USING ERRCODE='23514';
                END IF;
              ELSIF prior IS NULL OR old_config ->> 'lifecycle_state' IS DISTINCT FROM 'disabled'
                 OR old_config -> 'subject_incarnation' IS NOT DISTINCT FROM subject -> 'subject_incarnation' THEN
                RAISE EXCEPTION 'typed lifecycle managed base cannot be recreated without predecessor event' USING ERRCODE='23514';
              END IF;
            END IF;
            IF origin IS NULL THEN""")
    return definition


def _pinned(definition: str) -> str:
    definition = _once(definition,
        "delegated := epoch_record.manifest_payload -> 'schema_version' = '3'::jsonb;",
        """IF epoch_record.manifest_payload -> 'schema_version' IS DISTINCT FROM '4'::jsonb THEN
            RAISE EXCEPTION 'typed allocation manifest version changed' USING ERRCODE='23514';
          END IF;
          delegated := true;""")
    definition = _once(definition,
        "(delegated AND allocation_record.complete_payload -> 'schema_version' IS DISTINCT FROM '3'::jsonb)",
        "(delegated AND allocation_record.complete_payload -> 'schema_version' IS DISTINCT FROM '4'::jsonb)")
    definition = _once(definition,
        "snapshot -> 'schema_version' IS DISTINCT FROM '1'::jsonb",
        "snapshot -> 'schema_version' IS DISTINCT FROM '2'::jsonb")
    definition = _once(definition,
        "'delegated', delegated, 'execution_epoch', epoch_record.execution_epoch,",
        """'delegated', delegated, 'member', member,
            'purpose', CASE WHEN member ->> 'purpose' = 'personal-build-worker' THEN 'personal-build-worker' ELSE 'application-worker' END,
            'execution_epoch', epoch_record.execution_epoch,""")
    return definition


def _current(definition: str) -> str:
    definition = _once(definition,
        "profile jsonb; profile_record record; current_incarnation uuid;",
        "profile jsonb; profile_record record; current_incarnation uuid; installation jsonb; base_origin jsonb;")
    definition = _once(definition,
        "          current_subject := pinned -> 'configuration'; current_ack := pinned -> 'acknowledgement';",
        """          IF pinned ->> 'purpose' = 'personal-build-worker' THEN
            RAISE EXCEPTION 'typed build execution readiness is not yet connected' USING ERRCODE='23514';
          END IF;
          current_subject := pinned -> 'configuration'; current_ack := pinned -> 'acknowledgement';""")
    definition = _once(definition,
        "reporter_record.state IS DISTINCT FROM 'current'",
        "reporter_record.state NOT IN ('current','equivocal')")
    definition = _once(definition,
        "SELECT request_payload -> 'projection' INTO STRICT projection",
        "SELECT request_payload #> '{command,projection}' INTO STRICT projection")
    definition = _once(definition,
        "          RETURN current_subject = pinned -> 'configuration' AND current_ack = pinned -> 'acknowledgement'",
        """          SELECT value INTO base_origin
            FROM public.capacity_execution_epochs epoch,
              LATERAL jsonb_array_elements(epoch.manifest_payload -> 'managed_application_origins')
            WHERE epoch.execution_epoch = (pinned ->> 'execution_epoch')::bigint
              AND value #>> '{configuration,subject_id}' = p_subject::text;
          IF member IS NOT NULL OR base_origin IS NOT NULL THEN
            SELECT request_payload #> '{command,projection}' INTO installation
              FROM public.capacity_personal_membership_events
              WHERE execution_epoch = (pinned ->> 'execution_epoch')::bigint
                AND revision <= latest_revision AND subject_id = p_subject
                AND subject_incarnation = current_incarnation
                AND deployment_generation = (current_subject ->> 'deployment_generation')::bigint
                AND request_payload #>> '{command,purpose}' = 'personal-application'
                AND request_payload #>> '{command,projection,operation_kind}' IN ('create','update')
              ORDER BY revision DESC LIMIT 1;
            IF installation IS NULL AND base_origin #> '{configuration,subject_incarnation}' = current_subject -> 'subject_incarnation'
                AND base_origin #> '{configuration,deployment_generation}' = current_subject -> 'deployment_generation' THEN
              installation := base_origin -> 'installation_projection';
            END IF;
            IF member IS NULL THEN projection := base_origin -> 'base_projection'; END IF;
            IF installation IS NULL OR projection IS NULL
               OR public.capacity_personal_application_installation_matches(current_subject, installation) IS DISTINCT FROM true
               OR reporter_record.token_sha256 IS DISTINCT FROM projection ->> 'demand_reporter_token_sha256' THEN
              RAISE EXCEPTION 'typed application installation evidence changed' USING ERRCODE='23514';
            END IF;
          END IF;
          RETURN current_subject = pinned -> 'configuration' AND current_ack = pinned -> 'acknowledgement'""")
    return definition


def upgrade() -> None:
    op.execute("LOCK TABLE public.capacity_authority_state IN EXCLUSIVE MODE")
    for suffix, signature in _READERS.items():
        original = f"capacity_membership_{suffix}"
        backup = f"capacity_0020_legacy_{suffix}"
        typed = f"capacity_typed_membership_{suffix}"
        definition = _definition(original, signature)
        op.execute(_rename(definition, original, backup))
        op.execute(f"REVOKE ALL ON FUNCTION public.{backup}({signature}) FROM PUBLIC")
        definition = _rename(definition, original, typed)
        if suffix == "event_prefix":
            definition = _prefix(definition)
        elif suffix == "pinned_subject":
            definition = _pinned(definition)
        elif suffix == "target_current":
            definition = _current(definition)
        op.execute(definition)
        op.execute(f"REVOKE ALL ON FUNCTION public.{typed}({signature}) FROM PUBLIC")
        if suffix == "event_prefix":
            args, call, lookup, returns = "p_epoch bigint,p_revision bigint", "p_epoch,p_revision", "execution_epoch=p_epoch", "jsonb"
        elif suffix == "cleanup_reporter":
            args, call, lookup, returns = "binding jsonb,p_reporter uuid", "binding,p_reporter", "execution_epoch=(binding #>> '{execution,execution_epoch}')::bigint", "boolean"
        else:
            args, call = "p_allocation bigint,p_subject uuid,p_incarnation uuid", "p_allocation,p_subject,p_incarnation"
            lookup = "execution_epoch=(SELECT execution_epoch FROM public.capacity_allocation_epochs WHERE allocation_epoch=p_allocation)"
            returns = "jsonb" if suffix == "pinned_subject" else "boolean"
        op.execute(f"""
            CREATE OR REPLACE FUNCTION public.{original}({args})
            RETURNS {returns} LANGUAGE plpgsql SET search_path=pg_catalog AS $$
            BEGIN
              IF EXISTS (SELECT 1 FROM public.capacity_execution_epochs WHERE {lookup}
                  AND manifest_payload -> 'schema_version' = '4'::jsonb) THEN
                RETURN public.{typed}({call});
              END IF;
              RETURN public.{backup}({call});
            END $$;
        """)
    for index, original in enumerate(_GUARDS):
        definition = _definition(original, "")
        backup = f"capacity_0020_prior_guard_{index}"
        op.execute(_rename(definition, original, backup))
        op.execute(f"REVOKE ALL ON FUNCTION public.{backup}() FROM PUBLIC")
        if index == 0:
            definition = _once(definition,
                "epoch.manifest_payload -> 'schema_version' = '3'::jsonb",
                "epoch.manifest_payload -> 'schema_version' IN ('3'::jsonb,'4'::jsonb)")
        else:
            definition = _once(definition,
                "      service_candidate_generation := (projection ->> 'candidate_generation')::bigint;",
                """      service_candidate_generation := (projection ->> 'candidate_generation')::bigint;
      IF member_purpose='personal-application' AND service_candidate_generation IS DISTINCT FROM NEW.deployment_generation THEN
        RAISE EXCEPTION 'typed application lifecycle candidate must match deployment generation' USING ERRCODE='23514';
      END IF;""")
        op.execute(definition)


def downgrade() -> None:
    op.execute("LOCK TABLE public.capacity_authority_state IN EXCLUSIVE MODE")
    op.execute("LOCK TABLE public.capacity_executable_intents IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text("""
        SELECT EXISTS (SELECT 1 FROM public.capacity_executable_intents i
          JOIN public.capacity_execution_epochs e ON e.execution_epoch=i.execution_epoch
          WHERE e.manifest_payload -> 'schema_version' = '4'::jsonb)
    """)):
        raise RuntimeError("cannot downgrade capacity_0020 with retained typed intents")
    for index, original in enumerate(_GUARDS):
        backup = f"capacity_0020_prior_guard_{index}"
        op.execute(_rename(_definition(backup, ""), backup, original))
        op.execute(f"DROP FUNCTION public.{backup}()")
    for suffix, signature in reversed(tuple(_READERS.items())):
        original = f"capacity_membership_{suffix}"
        backup = f"capacity_0020_legacy_{suffix}"
        op.execute(_rename(_definition(backup, signature), backup, original))
        op.execute(f"DROP FUNCTION public.capacity_typed_membership_{suffix}({signature})")
        op.execute(f"DROP FUNCTION public.{backup}({signature})")
