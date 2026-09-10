"""Startup refuses effective signer authority or trigger drift on disposable DB."""

import importlib
import secrets
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.integration.test_task_image_signer_policy import database as database


def module():
    name = "loom_task_image_signer.preflight"
    assert importlib.util.find_spec(name) is not None, "signer privilege preflight missing"
    return importlib.import_module(name)


@asynccontextmanager
async def role_engine(database):
    role, password = "signer_" + uuid4().hex, secrets.token_hex(24)
    async with database[0].begin() as connection:
        await connection.execute(text(f"CREATE ROLE {role} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"))
        await connection.execute(text(f"GRANT USAGE ON SCHEMA public TO {role}"))
        await connection.execute(text(f"GRANT SELECT ON task_image_publication_state, task_image_publication_keys, task_image_publication_keysets, task_image_publication_keyset_members TO {role}"))
        await connection.execute(text(f"GRANT UPDATE(singleton_id) ON task_image_publication_state TO {role}"))
        await connection.execute(text(f"GRANT UPDATE(key_id) ON task_image_publication_keys TO {role}"))
    engine = create_async_engine(database[0].url.set(username=role, password=password))
    try:
        yield engine, role
    finally:
        await engine.dispose()
        async with database[0].begin() as connection:
            await connection.execute(text(f"DROP OWNED BY {role}"))
            await connection.execute(text(f"DROP ROLE {role}"))


async def test_exact_effective_column_grants_and_immutable_triggers_pass(database):
    m = module()
    async with role_engine(database) as (engine, _):
        await m.verify_signer_database_role(engine)


async def test_database_owner_or_superuser_is_not_a_signer_identity(database):
    with pytest.raises(ValueError):
        await module().verify_signer_database_role(database[0])


@pytest.mark.parametrize("change", [
    "missing-read", "missing-lock", "counter-write", "key-write", "delete", "truncate",
    "foreign-table-read", "foreign-column-write", "schema-create", "role-membership",
    "disabled-trigger", "replaced-function", "extra-trigger", "row-security",
])
async def test_effective_privilege_or_schema_drift_keeps_listener_closed(database, change):
    m = module()
    async with role_engine(database) as (engine, role):
        async with database[0].begin() as connection:
            statements = {
                "missing-read": f"REVOKE SELECT ON task_image_publication_keys FROM {role}",
                "missing-lock": f"REVOKE UPDATE(singleton_id) ON task_image_publication_state FROM {role}",
                "counter-write": f"GRANT UPDATE(keyset_version) ON task_image_publication_state TO {role}",
                "key-write": f"GRANT UPDATE(public_key) ON task_image_publication_keys TO {role}",
                "delete": f"GRANT DELETE ON task_image_publication_keys TO {role}",
                "truncate": f"GRANT TRUNCATE ON task_image_publication_state TO {role}",
                "foreign-table-read": f"GRANT SELECT ON trials TO {role}",
                "foreign-column-write": f"GRANT UPDATE(state) ON trials TO {role}",
                "schema-create": f"GRANT CREATE ON SCHEMA public TO {role}",
                "role-membership": f"GRANT pg_read_all_data TO {role}",
                "disabled-trigger": "ALTER TABLE task_image_publication_keys DISABLE TRIGGER task_image_publication_keys_preserve",
                "replaced-function": "CREATE OR REPLACE FUNCTION task_image_publication_preserve_key() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$BEGIN RETURN NEW; END$$",
                "extra-trigger": "CREATE TRIGGER extra_signer_trigger BEFORE UPDATE ON task_image_publication_keys FOR EACH ROW EXECUTE FUNCTION task_image_publication_preserve_key()",
                "row-security": "ALTER TABLE task_image_publication_keys ENABLE ROW LEVEL SECURITY",
            }
            await connection.execute(text(statements[change]))
        with pytest.raises(ValueError):
            await m.verify_signer_database_role(engine)
