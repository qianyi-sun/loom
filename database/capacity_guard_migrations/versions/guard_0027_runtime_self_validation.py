"""Make the protected staging runtime prove its own least privilege.

Revision ID: guard_0027
Revises: guard_0026
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "guard_0027"
down_revision: str | Sequence[str] | None = "guard_0026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = "current_protected_runtime_registration()"

_LEGACY_RUNTIME_MEMBERSHIP_CHECK = """IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'protected submission runtime unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;"""

_SELF_VALIDATING_RUNTIME_MEMBERSHIP_CHECK = f"""IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'protected submission runtime unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;

          SELECT role.rolcanlogin,
                 role.rolinherit,
                 role.rolsuper,
                 role.rolcreatedb,
                 role.rolcreaterole,
                 role.rolreplication,
                 role.rolbypassrls,
                 role.rolpassword IS NOT NULL AS has_password,
                 (
                   SELECT count(*)
                     FROM pg_catalog.pg_auth_members AS membership
                    WHERE membership.member = role.oid
                       OR membership.roleid = role.oid
                 ) AS role_memberships
            INTO v_runtime
            FROM pg_catalog.pg_roles AS role
           WHERE role.rolname = v_runtime_role;
          IF NOT FOUND
             OR v_runtime.rolcanlogin IS DISTINCT FROM true
             OR v_runtime.rolinherit IS DISTINCT FROM false
             OR v_runtime.rolsuper IS DISTINCT FROM false
             OR v_runtime.rolcreatedb IS DISTINCT FROM false
             OR v_runtime.rolcreaterole IS DISTINCT FROM false
             OR v_runtime.rolreplication IS DISTINCT FROM false
             OR v_runtime.rolbypassrls IS DISTINCT FROM false
             OR v_runtime.has_password IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'protected submission runtime role attributes drifted'
              USING ERRCODE = '42501';
          END IF;
          IF v_runtime.role_memberships IS DISTINCT FROM 0 THEN
            RAISE EXCEPTION 'protected submission runtime role memberships drifted'
              USING ERRCODE = '42501';
          END IF;
          IF pg_catalog.has_schema_privilege(
               session_user, '{SCHEMA}', 'USAGE'
             ) IS DISTINCT FROM true
             OR pg_catalog.has_schema_privilege(
               session_user, '{SCHEMA}', 'CREATE'
             ) IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'protected submission runtime schema privileges drifted'
              USING ERRCODE = '42501';
          END IF;

          SELECT count(*) INTO v_relation_privileges
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
           WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
             AND namespace.nspname !~ '^pg_toast'
             AND (
               (
                 relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                 AND (
                   pg_catalog.has_table_privilege(session_user, relation.oid, 'SELECT')
                   OR pg_catalog.has_table_privilege(session_user, relation.oid, 'INSERT')
                   OR pg_catalog.has_table_privilege(session_user, relation.oid, 'UPDATE')
                   OR pg_catalog.has_table_privilege(session_user, relation.oid, 'DELETE')
                   OR pg_catalog.has_table_privilege(session_user, relation.oid, 'TRUNCATE')
                   OR pg_catalog.has_table_privilege(session_user, relation.oid, 'REFERENCES')
                   OR pg_catalog.has_table_privilege(session_user, relation.oid, 'TRIGGER')
                 )
               )
               OR (
                 relation.relkind = 'S'
                 AND (
                   pg_catalog.has_sequence_privilege(session_user, relation.oid, 'USAGE')
                   OR pg_catalog.has_sequence_privilege(session_user, relation.oid, 'SELECT')
                   OR pg_catalog.has_sequence_privilege(session_user, relation.oid, 'UPDATE')
                 )
               )
             );
          IF v_relation_privileges IS DISTINCT FROM 0 THEN
            RAISE EXCEPTION 'protected submission runtime relation privileges drifted'
              USING ERRCODE = '42501';
          END IF;

          v_allowed_functions := ARRAY[
            '{SCHEMA}.current_protected_runtime_registration()'::regprocedure::oid,
            '{SCHEMA}.submit_protected_runtime_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text,jsonb,bytea,text)'::regprocedure::oid,
            '{SCHEMA}.publish_protected_runtime_trial_readiness(uuid,uuid,uuid)'::regprocedure::oid,
            '{SCHEMA}.register_staging_public_worker(text,jsonb)'::regprocedure::oid,
            '{SCHEMA}.assert_staging_worker_session(uuid,text)'::regprocedure::oid,
            '{SCHEMA}.claim_staging_assigned_trial(uuid,text,jsonb)'::regprocedure::oid,
            '{SCHEMA}.retry_staging_claimed_trial(uuid,text,jsonb)'::regprocedure::oid
          ];
          SELECT count(*),
                 COALESCE(bool_and(routine.oid = ANY(v_allowed_functions)), false)
            INTO v_function_privileges, v_only_allowed_functions
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = routine.pronamespace
           WHERE namespace.nspname = '{SCHEMA}'
             AND pg_catalog.has_function_privilege(
                   session_user, routine.oid, 'EXECUTE'
                 );
          IF v_function_privileges IS DISTINCT FROM
               pg_catalog.cardinality(v_allowed_functions)
             OR v_only_allowed_functions IS DISTINCT FROM true
             OR EXISTS (
               SELECT 1
                 FROM unnest(v_allowed_functions) AS required(function_oid)
                WHERE pg_catalog.has_function_privilege(
                        session_user, required.function_oid, 'EXECUTE'
                      ) IS DISTINCT FROM true
             ) THEN
            RAISE EXCEPTION 'protected submission runtime function privileges drifted'
              USING ERRCODE = '42501';
          END IF;"""

_LEGACY_DECLARATIONS = f"""v_runtime_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;"""

_SELF_VALIDATING_DECLARATIONS = f"""v_runtime_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_runtime record;
          v_relation_privileges bigint;
          v_allowed_functions oid[];
          v_function_privileges bigint;
          v_only_allowed_functions boolean;"""


def _replace_function_clause(old: str, new: str) -> None:
    escaped_old = old.replace("'", "''")
    escaped_new = new.replace("'", "''")
    op.execute(
        f"""
        DO $$
        DECLARE
          v_definition text;
        BEGIN
          SELECT pg_get_functiondef(
            '{SCHEMA}.{FUNCTION}'::regprocedure
          ) INTO v_definition;
          IF position('{escaped_old}' in v_definition) = 0 THEN
            RAISE EXCEPTION
              'protected runtime self-validation clause was not found';
          END IF;
          IF length(v_definition) - length(replace(v_definition, '{escaped_old}', ''))
               <> length('{escaped_old}') THEN
            RAISE EXCEPTION
              'protected runtime self-validation clause was ambiguous';
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


def upgrade() -> None:
    _replace_function_clause(_LEGACY_DECLARATIONS, _SELF_VALIDATING_DECLARATIONS)
    _replace_function_clause(
        _LEGACY_RUNTIME_MEMBERSHIP_CHECK,
        _SELF_VALIDATING_RUNTIME_MEMBERSHIP_CHECK,
    )


def downgrade() -> None:
    _replace_function_clause(
        _SELF_VALIDATING_RUNTIME_MEMBERSHIP_CHECK,
        _LEGACY_RUNTIME_MEMBERSHIP_CHECK,
    )
    _replace_function_clause(_SELF_VALIDATING_DECLARATIONS, _LEGACY_DECLARATIONS)
