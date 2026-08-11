"""Alembic environment for the protected per-environment capacity schema."""

from __future__ import annotations

import os
import re
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

PROTECTED_SCHEMA = "loom_capacity_guard"
VERSION_TABLE = "capacity_guard_alembic_version"
_ROLE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,62}$")

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

db_url = os.environ.get("LOOM_CAPACITY_GUARD_DB_URL", "").strip()
owner_role = os.environ.get("LOOM_CAPACITY_GUARD_OWNER_ROLE", "").strip()
agent_role = os.environ.get("LOOM_CAPACITY_GUARD_AGENT_ROLE", "").strip()
if not db_url:
    raise RuntimeError(
        "LOOM_CAPACITY_GUARD_DB_URL must be set to run protected capacity migrations"
    )
if _ROLE_PATTERN.fullmatch(owner_role) is None:
    raise RuntimeError("LOOM_CAPACITY_GUARD_OWNER_ROLE must name an explicit canonical SQL role")
if _ROLE_PATTERN.fullmatch(agent_role) is None or agent_role == owner_role:
    raise RuntimeError("LOOM_CAPACITY_GUARD_AGENT_ROLE must name a distinct canonical SQL role")
# Alembic stores options in ConfigParser, where URL-encoded percent signs in
# credentials are interpolation markers unless doubled at this one boundary.
config.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))
target_metadata = None


def run_migrations_offline() -> None:
    raise RuntimeError("protected capacity migrations require an online owner-role verification")


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    try:
        with connectable.connect() as connection:
            preparer = connection.dialect.identifier_preparer
            quoted_owner = preparer.quote(owner_role)
            quoted_schema = preparer.quote(PROTECTED_SCHEMA)
            login = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls "
                        "FROM pg_roles WHERE rolname = session_user"
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                login is None
                or login["rolcanlogin"] is not True
                or any(
                    login[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity migration login must be a least-privileged role"
                )
            agent = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(rolname, :owner_role, 'MEMBER') AS owner_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "WHERE m.member = pg_roles.oid) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :agent_role"
                    ),
                    {"agent_role": agent_role, "owner_role": owner_role},
                )
                .mappings()
                .one_or_none()
            )
            if (
                agent is None
                or agent["rolcanlogin"] is not True
                or agent["rolinherit"] is not False
                or agent["owner_member"] is not False
                or agent["role_memberships"] != 0
                or agent["rolname"] == login["rolname"]
                or any(
                    agent[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity agent must be a distinct least-privileged "
                    "NOINHERIT login with no owner membership"
                )
            config.attributes["capacity_guard_agent_role"] = agent_role
            connection.exec_driver_sql(f"SET ROLE {quoted_owner}")
            role = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls "
                        "FROM pg_roles WHERE rolname = current_role"
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                role is None
                or role["rolname"] != owner_role
                or role["rolcanlogin"] is True
                or role["rolinherit"] is True
                or any(
                    role[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity migrations require the exact least-privileged "
                    "NOLOGIN NOINHERIT owner role"
                )
            required_trial_columns = (
                "id",
                "state",
                "requires_caps",
                "cancellation_requested_at",
                "next_attempt_at",
                "autoscaler_pool_name",
                "worker_id",
                "attempt_count",
                "submit_priority",
                "submitted_at",
            )
            missing_trial_columns = tuple(
                column
                for column in required_trial_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.trials', :column, 'SELECT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_trial_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privileges on "
                    "the demand-source columns of public.trials: "
                    + ", ".join(missing_trial_columns)
                )

            connection.exec_driver_sql(
                f"CREATE SCHEMA IF NOT EXISTS {quoted_schema} AUTHORIZATION {quoted_owner}"
            )
            actual_owner = connection.execute(
                text("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = :schema"),
                {"schema": PROTECTED_SCHEMA},
            ).scalar_one()
            if actual_owner != owner_role:
                raise RuntimeError("protected capacity schema has the wrong owner")
            connection.commit()

            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                include_schemas=True,
                version_table=VERSION_TABLE,
                version_table_schema=PROTECTED_SCHEMA,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
