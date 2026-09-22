"""Separate protected candidate source identity from publication digest.

Revision ID: guard_0016
Revises: guard_0015
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0016"
down_revision: str | Sequence[str] | None = "guard_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


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
            RAISE EXCEPTION 'candidate provenance function clause not found: {function}';
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


def upgrade() -> None:
    op.add_column(
        "agent_registrations",
        sa.Column("candidate_identity_algorithm", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "agent_registrations",
        sa.Column("candidate_identity", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "agent_registrations",
        sa.Column("candidate_publication_sha256", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.execute(
        f"""
        ALTER TABLE {SCHEMA}.agent_registrations
        DISABLE TRIGGER agent_registrations_monotonic_row
        """
    )
    op.execute(
        f"""
        UPDATE {SCHEMA}.agent_registrations
           SET candidate_identity_algorithm = 'source-sha256',
               candidate_identity = candidate_digest,
               candidate_publication_sha256 = candidate_digest
        """
    )
    op.execute(
        f"""
        ALTER TABLE {SCHEMA}.agent_registrations
        ENABLE TRIGGER agent_registrations_monotonic_row
        """
    )
    for column in (
        "candidate_identity_algorithm",
        "candidate_identity",
        "candidate_publication_sha256",
    ):
        op.alter_column("agent_registrations", column, nullable=False, schema=SCHEMA)
    op.execute(
        f"""
        ALTER TABLE {SCHEMA}.audit_events
        DISABLE TRIGGER audit_events_append_only_row
        """
    )
    op.execute(
        f"""
        UPDATE {SCHEMA}.audit_events AS event
           SET payload = event.payload || jsonb_build_object(
               'candidate_identity_algorithm', 'source-sha256',
               'candidate_identity', event.payload->>'candidate_digest',
               'candidate_publication_sha256', event.payload->>'candidate_digest'
           ),
               payload_digest = encode(sha256(convert_to(
               '{{"agent_incarnation":' ||
                 to_jsonb(event.payload->>'agent_incarnation')::text ||
               ',"allocation_epoch":' || (event.payload->>'allocation_epoch') ||
               ',"authority_incarnation":' ||
                 to_jsonb(event.payload->>'authority_incarnation')::text ||
               ',"authority_mode":' || to_jsonb(event.payload->>'authority_mode')::text ||
               ',"candidate_digest":' || to_jsonb(event.payload->>'candidate_digest')::text ||
               ',"candidate_identity":' ||
                 to_jsonb(event.payload->>'candidate_digest')::text ||
               ',"candidate_identity_algorithm":' ||
                 to_jsonb('source-sha256'::text)::text ||
               ',"candidate_publication_sha256":' ||
                 to_jsonb(event.payload->>'candidate_digest')::text ||
               ',"configuration_generation":' ||
                 (event.payload->>'configuration_generation') ||
               ',"deployment_generation":' || (event.payload->>'deployment_generation') ||
               ',"environment_id":' || to_jsonb(event.payload->>'environment_id')::text ||
               ',"reporter_high_water":' || (event.payload->>'reporter_high_water') ||
               ',"reporter_incarnation":' ||
                 to_jsonb(event.payload->>'reporter_incarnation')::text ||
               ',"schema_version":' || (event.payload->>'schema_version') ||
               ',"subject_id":' || to_jsonb(event.payload->>'subject_id')::text ||
               ',"subject_incarnation":' ||
                 to_jsonb(event.payload->>'subject_incarnation')::text ||
               '}}',
               'UTF8'
           )), 'hex')
         WHERE event.event_type IN ('agent_registered.v1', 'agent_reconfigured.v1')
           AND (
               NOT (event.payload ? 'candidate_identity_algorithm')
               OR NOT (event.payload ? 'candidate_identity')
               OR NOT (event.payload ? 'candidate_publication_sha256')
           )
        """
    )
    op.execute(
        f"""
        ALTER TABLE {SCHEMA}.audit_events
        ENABLE TRIGGER audit_events_append_only_row
        """
    )
    op.create_check_constraint(
        "agent_registrations_candidate_identity_check",
        "agent_registrations",
        "((candidate_identity_algorithm = 'git-sha1' "
        "AND candidate_identity ~ '^[0-9a-f]{40}$') OR "
        "(candidate_identity_algorithm = 'source-sha256' "
        "AND candidate_identity ~ '^[0-9a-f]{64}$')) "
        "AND candidate_publication_sha256 ~ '^[0-9a-f]{64}$'",
        schema=SCHEMA,
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        "v_binding->'candidate'->>'publication_sha256'\n"
        "                  IS DISTINCT FROM v_registration.candidate_digest",
        "v_candidate->>'algorithm'\n"
        "                  IS DISTINCT FROM v_registration.candidate_identity_algorithm\n"
        "               OR v_candidate->>'identity'\n"
        "                  IS DISTINCT FROM v_registration.candidate_identity\n"
        "               OR v_candidate->>'publication_sha256'\n"
        "                  IS DISTINCT FROM v_registration.candidate_publication_sha256",
    )
    _replace_function_clause(
        "assert_inert_agent_binding(uuid,jsonb,bytea,text)",
        "OR p_payload->>'candidate_digest' IS DISTINCT FROM v_registration.candidate_digest\n"
        "             OR (p_payload->>'deployment_generation')::bigint",
        "OR p_payload->>'candidate_digest' IS DISTINCT FROM v_registration.candidate_digest\n"
        "             OR p_payload->>'candidate_identity_algorithm'\n"
        "                IS DISTINCT FROM v_registration.candidate_identity_algorithm\n"
        "             OR p_payload->>'candidate_identity'\n"
        "                IS DISTINCT FROM v_registration.candidate_identity\n"
        "             OR p_payload->>'candidate_publication_sha256'\n"
        "                IS DISTINCT FROM v_registration.candidate_publication_sha256\n"
        "             OR (p_payload->>'deployment_generation')::bigint",
    )
    _replace_function_clause(
        "enforce_agent_registration_reconfiguration()",
        "OR NEW.candidate_digest IS DISTINCT FROM OLD.candidate_digest) THEN",
        "OR NEW.candidate_digest IS DISTINCT FROM OLD.candidate_digest\n"
        "                  OR NEW.candidate_identity_algorithm IS DISTINCT FROM "
        "OLD.candidate_identity_algorithm\n"
        "                  OR NEW.candidate_identity IS DISTINCT FROM OLD.candidate_identity\n"
        "                  OR NEW.candidate_publication_sha256 IS DISTINCT FROM "
        "OLD.candidate_publication_sha256) THEN",
    )
    _replace_function_clause(
        "acknowledge_inert_protected_release(uuid,jsonb,bytea,text)",
        "'candidate_digest', 'deployment_generation',",
        "'candidate_digest', 'candidate_identity_algorithm', 'candidate_identity', "
        "'candidate_publication_sha256', 'deployment_generation',",
    )
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        "IF (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 20",
        "IF (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 23",
    )
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'deployment_generation', 'configuration_generation', 'trial_id',",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'candidate_identity', 'candidate_identity_algorithm',\n"
        "                  'candidate_publication_sha256', 'deployment_generation',\n"
        "                  'configuration_generation', 'trial_id',",
    )
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_payload->>'candidate_digest')::text ||\n"
        '            \',"configuration_generation":',
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_payload->>'candidate_digest')::text ||\n"
        "            ',\"candidate_identity\":' ||\n"
        "              to_jsonb(p_payload->>'candidate_identity')::text ||\n"
        "            ',\"candidate_identity_algorithm\":' ||\n"
        "              to_jsonb(p_payload->>'candidate_identity_algorithm')::text ||\n"
        "            ',\"candidate_publication_sha256\":' ||\n"
        "              to_jsonb(p_payload->>'candidate_publication_sha256')::text ||\n"
        '            \',"configuration_generation":',
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_protected_payload)) <> 20",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_protected_payload)) <> 23",
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 34",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 37",
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'deployment_generation', 'configuration_generation', 'trial_id',",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'candidate_identity', 'candidate_identity_algorithm',\n"
        "                  'candidate_publication_sha256', 'deployment_generation',\n"
        "                  'configuration_generation', 'trial_id',",
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_protected_payload->>'candidate_digest')::text ||\n"
        '            \',"configuration_generation":',
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_protected_payload->>'candidate_digest')::text ||\n"
        "            ',\"candidate_identity\":' ||\n"
        "              to_jsonb(p_protected_payload->>'candidate_identity')::text ||\n"
        "            ',\"candidate_identity_algorithm\":' ||\n"
        "              to_jsonb(p_protected_payload->>'candidate_identity_algorithm')::text ||\n"
        "            ',\"candidate_publication_sha256\":' ||\n"
        "              to_jsonb(p_protected_payload->>'candidate_publication_sha256')::text ||\n"
        '            \',"configuration_generation":',
    )
    _replace_function_clause(
        "prepare_inert_legacy_compatibility(uuid,jsonb,bytea,text)",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'compatibility_incarnation', 'compatibility_not_after', "
        "'compatibility_state', 'configuration_generation', "
        "'cross_pool_placement_authority', 'deployment_generation', "
        "'environment_id', 'executable', 'fleet_migration_epoch', "
        "'global_allowance_authority', 'mutation_inventory_digest', "
        "'new_claim_authority', 'new_submission_authority', "
        "'new_worker_authority', 'preparation_id', 'proposed_authority_mode', "
        "'reporter_high_water', 'reporter_incarnation', 'scale_up_authority', "
        "'schema_version', 'subject_id', 'subject_incarnation', 'writer_cursors'",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'candidate_identity', 'candidate_identity_algorithm', "
        "'candidate_publication_sha256', 'compatibility_incarnation', "
        "'compatibility_not_after', 'compatibility_state', "
        "'configuration_generation', 'cross_pool_placement_authority', "
        "'deployment_generation', 'environment_id', 'executable', "
        "'fleet_migration_epoch', 'global_allowance_authority', "
        "'mutation_inventory_digest', 'new_claim_authority', "
        "'new_submission_authority', 'new_worker_authority', 'preparation_id', "
        "'proposed_authority_mode', 'reporter_high_water', "
        "'reporter_incarnation', 'scale_up_authority', 'schema_version', "
        "'subject_id', 'subject_incarnation', 'writer_cursors'",
    )
    _replace_function_clause(
        "freeze_inert_legacy_compatibility(uuid,jsonb,bytea,text)",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'compatibility_incarnation', 'configuration_generation', "
        "'cross_pool_placement_authority', 'deployment_generation', "
        "'environment_id', 'executable', 'fleet_migration_epoch', 'freeze_id', "
        "'freeze_state', 'global_allowance_authority', "
        "'mutation_inventory_digest', 'new_claim_authority', "
        "'new_submission_authority', 'new_worker_authority', "
        "'preparation_digest', 'preparation_id', 'reporter_high_water', "
        "'reporter_incarnation', 'scale_up_authority', 'schema_version', "
        "'subject_id', 'subject_incarnation', 'writer_cursors'",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'candidate_identity', 'candidate_identity_algorithm', "
        "'candidate_publication_sha256', 'compatibility_incarnation', "
        "'configuration_generation', 'cross_pool_placement_authority', "
        "'deployment_generation', 'environment_id', 'executable', "
        "'fleet_migration_epoch', 'freeze_id', 'freeze_state', "
        "'global_allowance_authority', 'mutation_inventory_digest', "
        "'new_claim_authority', 'new_submission_authority', "
        "'new_worker_authority', 'preparation_digest', 'preparation_id', "
        "'reporter_high_water', 'reporter_incarnation', 'scale_up_authority', "
        "'schema_version', 'subject_id', 'subject_incarnation', 'writer_cursors'",
    )


def downgrade() -> None:
    _replace_function_clause(
        "freeze_inert_legacy_compatibility(uuid,jsonb,bytea,text)",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'candidate_identity', 'candidate_identity_algorithm', "
        "'candidate_publication_sha256', 'compatibility_incarnation', "
        "'configuration_generation', 'cross_pool_placement_authority', "
        "'deployment_generation', 'environment_id', 'executable', "
        "'fleet_migration_epoch', 'freeze_id', 'freeze_state', "
        "'global_allowance_authority', 'mutation_inventory_digest', "
        "'new_claim_authority', 'new_submission_authority', "
        "'new_worker_authority', 'preparation_digest', 'preparation_id', "
        "'reporter_high_water', 'reporter_incarnation', 'scale_up_authority', "
        "'schema_version', 'subject_id', 'subject_incarnation', 'writer_cursors'",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'compatibility_incarnation', 'configuration_generation', "
        "'cross_pool_placement_authority', 'deployment_generation', "
        "'environment_id', 'executable', 'fleet_migration_epoch', 'freeze_id', "
        "'freeze_state', 'global_allowance_authority', "
        "'mutation_inventory_digest', 'new_claim_authority', "
        "'new_submission_authority', 'new_worker_authority', "
        "'preparation_digest', 'preparation_id', 'reporter_high_water', "
        "'reporter_incarnation', 'scale_up_authority', 'schema_version', "
        "'subject_id', 'subject_incarnation', 'writer_cursors'",
    )
    _replace_function_clause(
        "prepare_inert_legacy_compatibility(uuid,jsonb,bytea,text)",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'candidate_identity', 'candidate_identity_algorithm', "
        "'candidate_publication_sha256', 'compatibility_incarnation', "
        "'compatibility_not_after', 'compatibility_state', "
        "'configuration_generation', 'cross_pool_placement_authority', "
        "'deployment_generation', 'environment_id', 'executable', "
        "'fleet_migration_epoch', 'global_allowance_authority', "
        "'mutation_inventory_digest', 'new_claim_authority', "
        "'new_submission_authority', 'new_worker_authority', 'preparation_id', "
        "'proposed_authority_mode', 'reporter_high_water', "
        "'reporter_incarnation', 'scale_up_authority', 'schema_version', "
        "'subject_id', 'subject_incarnation', 'writer_cursors'",
        "'activation_epoch', 'agent_incarnation', 'allocation_epoch', "
        "'authority_incarnation', 'authority_mode', 'candidate_digest', "
        "'compatibility_incarnation', 'compatibility_not_after', "
        "'compatibility_state', 'configuration_generation', "
        "'cross_pool_placement_authority', 'deployment_generation', "
        "'environment_id', 'executable', 'fleet_migration_epoch', "
        "'global_allowance_authority', 'mutation_inventory_digest', "
        "'new_claim_authority', 'new_submission_authority', "
        "'new_worker_authority', 'preparation_id', 'proposed_authority_mode', "
        "'reporter_high_water', 'reporter_incarnation', 'scale_up_authority', "
        "'schema_version', 'subject_id', 'subject_incarnation', 'writer_cursors'",
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_protected_payload->>'candidate_digest')::text ||\n"
        "            ',\"candidate_identity\":' ||\n"
        "              to_jsonb(p_protected_payload->>'candidate_identity')::text ||\n"
        "            ',\"candidate_identity_algorithm\":' ||\n"
        "              to_jsonb(p_protected_payload->>'candidate_identity_algorithm')::text ||\n"
        "            ',\"candidate_publication_sha256\":' ||\n"
        "              to_jsonb(p_protected_payload->>'candidate_publication_sha256')::text ||\n"
        '            \',"configuration_generation":',
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_protected_payload->>'candidate_digest')::text ||\n"
        '            \',"configuration_generation":',
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'candidate_identity', 'candidate_identity_algorithm',\n"
        "                  'candidate_publication_sha256', 'deployment_generation',\n"
        "                  'configuration_generation', 'trial_id',",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'deployment_generation', 'configuration_generation', 'trial_id',",
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 37",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 34",
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_protected_payload)) <> 23",
        "OR (SELECT count(*) FROM jsonb_object_keys(p_protected_payload)) <> 20",
    )
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_payload->>'candidate_digest')::text ||\n"
        "            ',\"candidate_identity\":' ||\n"
        "              to_jsonb(p_payload->>'candidate_identity')::text ||\n"
        "            ',\"candidate_identity_algorithm\":' ||\n"
        "              to_jsonb(p_payload->>'candidate_identity_algorithm')::text ||\n"
        "            ',\"candidate_publication_sha256\":' ||\n"
        "              to_jsonb(p_payload->>'candidate_publication_sha256')::text ||\n"
        '            \',"configuration_generation":',
        ',"candidate_digest":\' ||\n'
        "              to_jsonb(p_payload->>'candidate_digest')::text ||\n"
        '            \',"configuration_generation":',
    )
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'candidate_identity', 'candidate_identity_algorithm',\n"
        "                  'candidate_publication_sha256', 'deployment_generation',\n"
        "                  'configuration_generation', 'trial_id',",
        "'allocation_epoch', 'reporter_high_water', 'candidate_digest',\n"
        "                  'deployment_generation', 'configuration_generation', 'trial_id',",
    )
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        "IF (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 23",
        "IF (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 20",
    )
    _replace_function_clause(
        "assert_executable_admission_binding(uuid,uuid,text,jsonb,bytea,text)",
        "v_candidate->>'algorithm'\n"
        "                  IS DISTINCT FROM v_registration.candidate_identity_algorithm\n"
        "               OR v_candidate->>'identity'\n"
        "                  IS DISTINCT FROM v_registration.candidate_identity\n"
        "               OR v_candidate->>'publication_sha256'\n"
        "                  IS DISTINCT FROM v_registration.candidate_publication_sha256",
        "v_binding->'candidate'->>'publication_sha256'\n"
        "                  IS DISTINCT FROM v_registration.candidate_digest",
    )
    _replace_function_clause(
        "enforce_agent_registration_reconfiguration()",
        "OR NEW.candidate_digest IS DISTINCT FROM OLD.candidate_digest\n"
        "                  OR NEW.candidate_identity_algorithm IS DISTINCT FROM "
        "OLD.candidate_identity_algorithm\n"
        "                  OR NEW.candidate_identity IS DISTINCT FROM OLD.candidate_identity\n"
        "                  OR NEW.candidate_publication_sha256 IS DISTINCT FROM "
        "OLD.candidate_publication_sha256) THEN",
        "OR NEW.candidate_digest IS DISTINCT FROM OLD.candidate_digest) THEN",
    )
    _replace_function_clause(
        "assert_inert_agent_binding(uuid,jsonb,bytea,text)",
        "OR p_payload->>'candidate_digest' IS DISTINCT FROM v_registration.candidate_digest\n"
        "             OR p_payload->>'candidate_identity_algorithm'\n"
        "                IS DISTINCT FROM v_registration.candidate_identity_algorithm\n"
        "             OR p_payload->>'candidate_identity'\n"
        "                IS DISTINCT FROM v_registration.candidate_identity\n"
        "             OR p_payload->>'candidate_publication_sha256'\n"
        "                IS DISTINCT FROM v_registration.candidate_publication_sha256\n"
        "             OR (p_payload->>'deployment_generation')::bigint",
        "OR p_payload->>'candidate_digest' IS DISTINCT FROM v_registration.candidate_digest\n"
        "             OR (p_payload->>'deployment_generation')::bigint",
    )
    _replace_function_clause(
        "acknowledge_inert_protected_release(uuid,jsonb,bytea,text)",
        "'candidate_digest', 'candidate_identity_algorithm', 'candidate_identity', "
        "'candidate_publication_sha256', 'deployment_generation',",
        "'candidate_digest', 'deployment_generation',",
    )
    op.drop_constraint(
        "agent_registrations_candidate_identity_check",
        "agent_registrations",
        schema=SCHEMA,
    )
    op.drop_column("agent_registrations", "candidate_publication_sha256", schema=SCHEMA)
    op.drop_column("agent_registrations", "candidate_identity", schema=SCHEMA)
    op.drop_column("agent_registrations", "candidate_identity_algorithm", schema=SCHEMA)
