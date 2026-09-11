"""Migrate only with an explicit, isolated management build-owner login."""

import os
import re

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from loom_capacity_build_guard.migration_privileges import verify_migration_privileges

config = context.config
schema = "loom_capacity_build_guard"
owner = config.attributes.get("build_guard_owner_role") or os.environ.get("LOOM_CAPACITY_BUILD_GUARD_OWNER_ROLE", "")
agent = config.attributes.get("build_guard_agent_role") or os.environ.get("LOOM_CAPACITY_BUILD_GUARD_AGENT_ROLE", "")
url = config.get_main_option("sqlalchemy.url") or os.environ.get("LOOM_CAPACITY_BUILD_GUARD_DB_URL", "")
if (not url or owner == agent or any(not isinstance(role, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None
    for role in (owner, agent))):
    raise RuntimeError("build guard requires explicit database and distinct canonical owner/agent roles")
if context.is_offline_mode():
    raise RuntimeError("build guard requires online owner-role verification")
config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
engine = engine_from_config(config.get_section(config.config_ini_section, {}), prefix="sqlalchemy.", poolclass=pool.NullPool)
try:
    with engine.begin() as connection:
        login = connection.scalar(text("SELECT session_user"))
        roles = {row["rolname"]: row for row in connection.execute(text("""
            SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole,
              rolreplication, rolbypassrls,
              ARRAY(SELECT pg_get_userbyid(m.roleid) FROM pg_auth_members m WHERE m.member = r.oid) AS memberships
            FROM pg_roles r WHERE rolname IN (:owner, :agent, :login)
        """), {"owner": owner, "agent": agent, "login": login}).mappings()}
        if len(roles) != 3 or any(any(row[flag] for flag in (
            "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")) for row in roles.values()):
            raise RuntimeError("build guard requires distinct least-privileged roles")
        if (roles[owner]["rolcanlogin"] or roles[owner]["rolinherit"] or roles[owner]["memberships"]
            or not roles[agent]["rolcanlogin"] or roles[agent]["rolinherit"] or roles[agent]["memberships"]
            or not roles[login]["rolcanlogin"] or set(roles[login]["memberships"]) != {owner}):
            raise RuntimeError("build guard requires least-privileged owner, agent and migrator role membership")
        quote = connection.dialect.identifier_preparer.quote
        connection.exec_driver_sql(f"SET ROLE {quote(owner)}")
        actual_owner = connection.scalar(text("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=:schema"), {"schema": schema})
        if actual_owner is not None and actual_owner != owner:
            raise RuntimeError("build guard schema has a foreign owner")
        verify_migration_privileges(connection, owner=owner, agent=agent)
        if actual_owner is None:
            connection.exec_driver_sql(f"CREATE SCHEMA {schema} AUTHORIZATION {quote(owner)}")
        connection.exec_driver_sql(f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC")
        config.attributes["build_guard_agent_role"] = agent
        context.configure(connection=connection, include_schemas=True,
            version_table="alembic_version", version_table_schema=schema)
        with context.begin_transaction():
            context.run_migrations()
        verify_migration_privileges(connection, owner=owner, agent=agent)
finally:
    engine.dispose()
