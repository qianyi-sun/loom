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
executor_role = os.environ.get("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", "").strip()
observer_role = os.environ.get("LOOM_CAPACITY_GUARD_OBSERVER_ROLE", "").strip()
if not db_url:
    raise RuntimeError(
        "LOOM_CAPACITY_GUARD_DB_URL must be set to run protected capacity migrations"
    )
if _ROLE_PATTERN.fullmatch(owner_role) is None:
    raise RuntimeError("LOOM_CAPACITY_GUARD_OWNER_ROLE must name an explicit canonical SQL role")
if _ROLE_PATTERN.fullmatch(agent_role) is None or agent_role == owner_role:
    raise RuntimeError("LOOM_CAPACITY_GUARD_AGENT_ROLE must name a distinct canonical SQL role")
if _ROLE_PATTERN.fullmatch(executor_role) is None or executor_role in {owner_role, agent_role}:
    raise RuntimeError("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE must name a distinct canonical SQL role")
if _ROLE_PATTERN.fullmatch(observer_role) is None or observer_role in {
    owner_role,
    agent_role,
    executor_role,
}:
    raise RuntimeError("LOOM_CAPACITY_GUARD_OBSERVER_ROLE must name a distinct canonical SQL role")
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
            executor = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(rolname, :owner_role, 'MEMBER') AS owner_member, "
                        "pg_has_role(rolname, :agent_role, 'MEMBER') AS agent_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "WHERE m.member = pg_roles.oid) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :executor_role"
                    ),
                    {
                        "executor_role": executor_role,
                        "owner_role": owner_role,
                        "agent_role": agent_role,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                executor is None
                or executor["rolinherit"] is not False
                or executor["owner_member"] is not False
                or executor["agent_member"] is not False
                or executor["role_memberships"] != 0
                or executor["rolname"] in {login["rolname"], agent_role, owner_role}
                or any(
                    executor[field] is True
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
                    "protected capacity executor must be a distinct least-privileged "
                    "NOINHERIT role with no memberships"
                )
            observer = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(rolname, :owner_role, 'MEMBER') AS owner_member, "
                        "pg_has_role(rolname, :agent_role, 'MEMBER') AS agent_member, "
                        "pg_has_role(rolname, :executor_role, 'MEMBER') AS executor_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "WHERE m.member = pg_roles.oid) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :observer_role"
                    ),
                    {
                        "observer_role": observer_role,
                        "owner_role": owner_role,
                        "agent_role": agent_role,
                        "executor_role": executor_role,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                observer is None
                or observer["rolcanlogin"] is not True
                or observer["rolinherit"] is not False
                or observer["owner_member"] is not False
                or observer["agent_member"] is not False
                or observer["executor_member"] is not False
                or observer["role_memberships"] != 0
                or observer["rolname"] in {login["rolname"], owner_role, agent_role, executor_role}
                or any(
                    observer[field] is True
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
                    "protected capacity observer must be a distinct least-privileged "
                    "NOINHERIT login with no memberships"
                )
            config.attributes["capacity_guard_agent_role"] = agent_role
            config.attributes["capacity_guard_executor_role"] = executor_role
            config.attributes["capacity_guard_observer_role"] = observer_role
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
                "lifecycle_authority_id",
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
            required_trial_insert_columns = (
                "id",
                "team_id",
                "task_id",
                "config",
                "requires_caps",
                "state",
                "submit_priority",
                "batch_id",
                "idempotency_key",
                "sample_idx",
                "combination_idx",
                "provider_connection_id",
                "provider_model_id",
                "submitted_by_user_id",
                "usage_attributed_user_id",
                "usage_attributed_actor",
                "family_key",
            )
            missing_trial_insert_columns = tuple(
                column
                for column in required_trial_insert_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.trials', :column, 'INSERT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_trial_insert_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct INSERT privileges on "
                    "the bounded projection columns of public.trials: "
                    + ", ".join(missing_trial_insert_columns)
                )
            required_trial_update_columns = ("lifecycle_authority_id", "state")
            missing_trial_update_columns = tuple(
                column
                for column in required_trial_update_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.trials', :column, 'UPDATE')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_trial_update_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct UPDATE privileges on "
                    "public.trials: " + ", ".join(missing_trial_update_columns)
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, 'public.trials', 'id', 'REFERENCES')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct REFERENCES privilege on "
                    "public.trials.id"
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, "
                    "'public.data_lifecycle_authorities', 'id', 'SELECT')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privilege on "
                    "public.data_lifecycle_authorities.id"
                )
            required_lifecycle_insert_columns = (
                "environment",
                "namespace",
                "team_id",
                "data_class",
                "owner_kind",
                "owner_id",
                "created_at",
                "expires_at",
                "pinned",
                "state",
            )
            missing_lifecycle_insert_columns = tuple(
                column
                for column in required_lifecycle_insert_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.data_lifecycle_authorities', :column, 'INSERT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_lifecycle_insert_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct INSERT privileges on "
                    "public.data_lifecycle_authorities: "
                    + ", ".join(missing_lifecycle_insert_columns)
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, "
                    "'public.data_lifecycle_authorities', 'id', 'REFERENCES')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct REFERENCES privilege on "
                    "public.data_lifecycle_authorities.id"
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
