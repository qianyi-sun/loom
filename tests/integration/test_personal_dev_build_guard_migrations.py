"""The management build ledger has no application-runtime write authority."""

from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError


@pytest.fixture
def build_guard_database(isolated_migration_postgres_url):
    url = make_url(isolated_migration_postgres_url)
    suffix = uuid4().hex
    owner, migrator, agent = (f"build_{kind}_{suffix}" for kind in ("owner", "migrator", "agent"))
    engine = create_engine(url)
    quote = engine.dialect.identifier_preparer.quote
    password = "isolated-build-guard-test"
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"CREATE ROLE {quote(owner)} NOLOGIN NOINHERIT")
            for role in (migrator, agent):
                connection.exec_driver_sql(f"CREATE ROLE {quote(role)} LOGIN NOINHERIT PASSWORD '{password}'")
            connection.exec_driver_sql(f"GRANT {quote(owner)} TO {quote(migrator)}")
            connection.exec_driver_sql(f"GRANT CREATE ON DATABASE {quote(url.database)} TO {quote(owner)}")
            connection.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {quote(owner)}")
            connection.exec_driver_sql(f"GRANT REFERENCES ON public.personal_dev_build_platform_requests TO {quote(owner)}")
        root = Path(__file__).resolve().parents[2] / "capacity_build_guard_migrations"
        config = Config(str(root / "alembic.ini"))
        config.set_main_option("script_location", str(root))
        config.set_main_option("sqlalchemy.url", url.set(username=migrator, password=password).render_as_string(hide_password=False).replace("%", "%%"))
        config.attributes.update(build_guard_owner_role=owner, build_guard_agent_role=agent)
        yield config, engine, owner, agent, url.set(username=agent, password=password)
    finally:
        with engine.begin() as connection:
            for role in (agent, migrator, owner):
                connection.exec_driver_sql(f"DROP OWNED BY {quote(role)}")
            for role in (agent, migrator, owner):
                connection.exec_driver_sql(f"DROP ROLE {quote(role)}")
        engine.dispose()


def test_build_guard_is_private_owner_only_and_empty_rollback_is_reversible(build_guard_database):
    config, engine, owner, agent, agent_url = build_guard_database
    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) == "build_guard_0001"
        assert connection.scalar(text("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname='loom_capacity_build_guard'")) == owner
        assert connection.scalar(text("SELECT has_schema_privilege(:agent,'loom_capacity_build_guard','USAGE')"), {"agent": agent})
    runtime = create_engine(agent_url)
    try:
        with runtime.connect() as connection, pytest.raises(DBAPIError, match="permission denied"):
            connection.execute(text("INSERT INTO loom_capacity_build_guard.installations DEFAULT VALUES"))
    finally:
        runtime.dispose()
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.assignments")) == 0


def test_build_guard_refuses_privileged_agent(build_guard_database):
    config, engine, _owner, agent, _url = build_guard_database
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER ROLE {engine.dialect.identifier_preparer.quote(agent)} CREATEDB")
    with pytest.raises(RuntimeError, match="least-privileged"):
        command.upgrade(config, "head")
