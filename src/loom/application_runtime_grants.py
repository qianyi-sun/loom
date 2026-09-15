"""Ordinary application grants within a protected ownership transaction.

The administrator caller must already admit the exact trusted application shape,
serialize relevant DDL, retire old sessions and hold its exact object locks.
This helper does not authenticate a release, transfer ownership, seal credentials,
or authorize activation. Fresh isolated reference builds satisfy these premises
without adopting any existing database objects.
"""

import re

from psycopg import sql


def application_runtime_grants_ddl(
    *, owner_role: str, runtime_role: str, allow_password_on_nologin_runtime: bool = False
) -> sql.Composed:
    """Grant DML after sealed ownership; caller verifies any preserved runtime password.

    The optional mode never permits LOGIN or a password on the owner. Its caller
    must verify the original credential and admit overlapping password-only writes;
    this SQL helper cannot validate a plaintext password or admit a writer.
    """
    if type(allow_password_on_nologin_runtime) is not bool:
        raise ValueError("application runtime password mode is invalid")
    if owner_role == runtime_role or any(
        re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None
        for role in (owner_role, runtime_role)
    ):
        raise ValueError("application runtime grant identities are invalid")
    return sql.SQL(
        """
        DO $application_grants$
        DECLARE
          v_owner pg_catalog.oid := pg_catalog.to_regrole({owner})::pg_catalog.oid;
          v_runtime pg_catalog.oid := pg_catalog.to_regrole({runtime})::pg_catalog.oid;
          v_relation pg_catalog.record;
          v_search_path pg_catalog.text := pg_catalog.current_setting('search_path');
        BEGIN
          PERFORM pg_catalog.set_config('search_path','pg_catalog,pg_temp',true);
          IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'application runtime grants require READ COMMITTED'
              USING ERRCODE='55000';
          END IF;
          IF current_user <> session_user OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=current_user AND rolsuper
          ) THEN
            RAISE EXCEPTION 'application runtime grants require protected administrator authority'
              USING ERRCODE='42501';
          END IF;
          IF v_owner IS NULL OR v_runtime IS NULL OR (SELECT count(*) FROM pg_catalog.pg_authid
            WHERE oid IN (v_owner,v_runtime) AND NOT rolcanlogin AND NOT rolinherit
              AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
              AND NOT rolreplication AND NOT rolbypassrls
              AND (rolpassword IS NULL OR {allow_runtime_password} AND oid=v_runtime)) <> 2
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members
                       WHERE member IN (v_owner,v_runtime) OR roleid IN (v_owner,v_runtime))
            OR (SELECT datdba FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
               IS DISTINCT FROM v_owner
            OR (SELECT nspowner FROM pg_catalog.pg_namespace WHERE nspname='public')
               IS DISTINCT FROM v_owner
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                       JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname='public' AND (c.relowner<>v_owner
                         OR c.relkind NOT IN ('r','p','v','m','S','i','I','c')))
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc p
                       JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
                       WHERE n.nspname='public' AND p.proowner<>v_owner)
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_type t
                       JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
                       WHERE n.nspname='public' AND t.typowner<>v_owner) THEN
            RAISE EXCEPTION 'application runtime grants require exact sealed ownership'
              USING ERRCODE='55000';
          END IF;
          IF pg_catalog.has_schema_privilege(v_runtime, 'public', 'CREATE')
             OR pg_catalog.has_database_privilege(v_runtime,pg_catalog.current_database(),'CREATE')
             OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                         JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                         WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m')
                           AND (pg_catalog.has_table_privilege(v_runtime,c.oid,
                                                            'TRIGGER,TRUNCATE,REFERENCES')
                             OR pg_catalog.has_any_column_privilege(v_runtime,c.oid,'REFERENCES')))
             OR pg_catalog.has_table_privilege(v_runtime,'public.alembic_version','INSERT,UPDATE,DELETE')
             OR pg_catalog.has_any_column_privilege(v_runtime,'public.alembic_version','INSERT,UPDATE') THEN
            RAISE EXCEPTION 'application runtime grants found residual schema authority'
              USING ERRCODE='55000';
          END IF;
          EXECUTE pg_catalog.format('GRANT CONNECT ON DATABASE %I TO %I',pg_catalog.current_database(),{runtime});
          GRANT USAGE ON SCHEMA public TO {runtime_identifier};
          FOR v_relation IN SELECT c.relname,c.relkind FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S') ORDER BY c.relname
          LOOP
            IF v_relation.relkind='S' THEN
              EXECUTE pg_catalog.format('GRANT USAGE,SELECT ON SEQUENCE public.%I TO %I',
                                        v_relation.relname,{runtime});
            ELSIF v_relation.relkind='m' OR v_relation.relname='alembic_version' THEN
              EXECUTE pg_catalog.format('GRANT SELECT ON TABLE public.%I TO %I',
                                        v_relation.relname,{runtime});
            ELSE
              EXECUTE pg_catalog.format('GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.%I TO %I',
                                        v_relation.relname,{runtime});
            END IF;
          END LOOP;
          ALTER DEFAULT PRIVILEGES FOR ROLE {owner_identifier} IN SCHEMA public
            GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO {runtime_identifier};
          ALTER DEFAULT PRIVILEGES FOR ROLE {owner_identifier} IN SCHEMA public
            GRANT USAGE,SELECT ON SEQUENCES TO {runtime_identifier};
          PERFORM pg_catalog.set_config('search_path',v_search_path,true);
        END
        $application_grants$;
        """
    ).format(
        owner=sql.Literal(owner_role),
        runtime=sql.Literal(runtime_role),
        owner_identifier=sql.Identifier(owner_role),
        runtime_identifier=sql.Identifier(runtime_role),
        allow_runtime_password=sql.Literal(allow_password_on_nologin_runtime),
    )
