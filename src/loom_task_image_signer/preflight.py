"""Read-only startup checks for the dedicated signer's effective SQL boundary.

Pins are SHA-256 of exact function bodies in immutable migrations 0135/0138.
Changing the authority implementation requires a reviewed release and new pins,
not accepting a hash supplied by the database under inspection.
"""

from __future__ import annotations

import asyncio
import hashlib

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_TABLES = {
    "task_image_publication_state": "singleton_id",
    "task_image_publication_keys": "key_id",
    "task_image_publication_keysets": None,
    "task_image_publication_keyset_members": None,
}
_FUNCTIONS = {
    "task_image_publication_lock_state": "17605160159e26f7097ed830c217158e6779c8efb8eeaaa0cfa5dc9c43c5168e",
    "task_image_publication_preserve_state": "1bb4701b72dab3d7592138aab2ed7f8fda03b6cab6d8dcba04e336e6389512b9",
    "task_image_publication_preserve_key": "10b53c9e45c65d7972c17274320c5ec0cea62df992579e2b6d7c0585b1f40c14",
    "task_image_keyset_preserve_audit": "e63ff2f17cbda9c5c2bae12faece7bee860f8df3eb6df06611aca7027c55afad",
}
_TRIGGERS = {
    ("task_image_publication_state", "task_image_publication_state_preserve", 27, "task_image_publication_preserve_state"),
    ("task_image_publication_keys", "task_image_publication_keys_lock_state", 30, "task_image_publication_lock_state"),
    ("task_image_publication_keys", "task_image_publication_keys_preserve", 27, "task_image_publication_preserve_key"),
    ("task_image_publication_keysets", "task_image_keysets_preserve", 58, "task_image_keyset_preserve_audit"),
    ("task_image_publication_keyset_members", "task_image_keyset_members_preserve", 58, "task_image_keyset_preserve_audit"),
}


async def verify_signer_database_role(engine: AsyncEngine) -> None:
    """No grants/DDL/writes; fail closed before a production listener is opened.

    This verifies direct effective table/column access, role/ownership and the
    exact authority triggers. The trusted database administrator and release
    process remain part of the service's trust boundary.
    """
    async with asyncio.timeout(5), engine.connect() as connection:
        await connection.execution_options(isolation_level="READ COMMITTED")
        async with connection.begin():
            await connection.execute(text("SET LOCAL statement_timeout='3s'"))
            await connection.execute(text("SET LOCAL idle_in_transaction_session_timeout='3s'"))
            role_ok = await connection.scalar(text("""
                SELECT current_user=session_user AND NOT (rolsuper OR rolinherit OR rolcreaterole
                  OR rolcreatedb OR rolreplication OR rolbypassrls)
                  AND current_setting('session_replication_role')='origin'
                  AND NOT pg_catalog.has_parameter_privilege(current_user,'session_replication_role','SET')
                  AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=r.oid)
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datdba=r.oid)
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner=r.oid
                    OR (nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'
                      AND pg_catalog.has_schema_privilege(current_user,oid,'CREATE')))
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE proowner=r.oid)
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_proc p
                    JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
                    WHERE p.prosecdef AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
                      AND p.prorettype NOT IN ('pg_catalog.trigger'::regtype,'pg_catalog.event_trigger'::regtype)
                      AND pg_catalog.has_function_privilege(current_user,p.oid,'EXECUTE'))
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                    WHERE c.relkind='S' AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
                      AND (c.relowner=r.oid OR pg_catalog.has_sequence_privilege(current_user,c.oid,'SELECT,USAGE,UPDATE')))
                FROM pg_catalog.pg_roles r WHERE rolname=current_user
            """))
            if role_ok is not True:
                raise ValueError("signer database identity has administrative or inherited authority")
            rows = (await connection.execute(text("""
                SELECT n.nspname,c.relname,c.relrowsecurity,c.relforcerowsecurity,
                  c.relowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) AS owned,
                  pg_catalog.has_table_privilege(current_user,c.oid,'SELECT') AS readable,
                  pg_catalog.has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS writable,
                  pg_catalog.has_any_column_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,REFERENCES') AS any_column
                FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname NOT LIKE 'pg_%'
                  AND n.nspname <> 'information_schema' ORDER BY n.nspname,c.relname LIMIT 2049
            """))).mappings().all()
            if len(rows) > 2048:
                raise ValueError("signer database inventory exceeds preflight bound")
            admitted = set()
            for row in rows:
                expected = row["nspname"] == "public" and row["relname"] in _TABLES
                if row["owned"] or row["writable"]:
                    raise ValueError("signer database has table write/ownership privilege")
                if expected:
                    if not row["readable"] or row["relrowsecurity"] or row["relforcerowsecurity"]:
                        raise ValueError("signer authority read scope is missing or filtered")
                    admitted.add(row["relname"])
                elif row["readable"] or row["any_column"]:
                    raise ValueError("signer database can access an unrelated relation")
            if admitted != set(_TABLES):
                raise ValueError("signer authority tables are missing")
            columns = (await connection.execute(text("""
                SELECT c.relname,a.attname,
                  pg_catalog.has_column_privilege(current_user,c.oid,a.attnum,'UPDATE') AS updatable,
                  pg_catalog.has_column_privilege(current_user,c.oid,a.attnum,'INSERT,REFERENCES') AS other_write
                FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relname=ANY(:names)
                  AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum
            """), {"names": list(_TABLES)})).mappings().all()
            for column in columns:
                if column["other_write"] or column["updatable"] != (_TABLES[column["relname"]] == column["attname"]):
                    raise ValueError("signer column privileges differ from exact locking grants")
            triggers = (await connection.execute(text("""
                SELECT c.relname,t.tgname,t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,
                  t.tgqual IS NULL AS no_qual,t.tgnargs,p.proname,p.prosrc,p.proconfig,
                  p.prosecdef,p.proleakproof,p.pronargs,lang.lanname,pn.nspname AS function_schema
                FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
                JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
                JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace
                JOIN pg_catalog.pg_language lang ON lang.oid=p.prolang
                WHERE n.nspname='public' AND c.relname=ANY(:names) AND NOT t.tgisinternal LIMIT 17
            """), {"names": list(_TABLES)})).mappings().all()
            actual = {(row["relname"], row["tgname"], row["tgtype"], row["proname"]) for row in triggers}
            if actual != _TRIGGERS or len(triggers) != len(_TRIGGERS):
                raise ValueError("signer authority trigger set changed")
            for row in triggers:
                search_path = "search_path=pg_catalog" if row["proname"] == "task_image_keyset_preserve_audit" else "search_path=pg_catalog, public"
                if (
                    row["tgenabled"] != "O" or row["tgdeferrable"] or row["tginitdeferred"]
                    or not row["no_qual"] or row["tgnargs"] or row["pronargs"]
                    or row["prosecdef"] or row["proleakproof"] or row["lanname"] != "plpgsql"
                    or row["function_schema"] != "public" or row["proconfig"] != [search_path]
                    or hashlib.sha256(row["prosrc"].encode()).hexdigest() != _FUNCTIONS[row["proname"]]
                ):
                    raise ValueError("signer authority trigger implementation changed")
