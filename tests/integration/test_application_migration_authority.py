"""Run application Alembic through a transient login and a sealed owner."""

from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


@pytest.fixture
def application_migration_roles(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[Config, str, str, str]]:
    source = make_url(postgres_url)
    suffix = uuid4().hex
    database = f"app_migrate_{suffix}"
    owner = f"app_owner_{suffix}"
    migrator = f"app_migrator_{suffix}"
    # Exercise ConfigParser's interpolation boundary with a real credential.
    password = "owner-%-" + uuid4().hex
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    quote = admin.dialect.identifier_preparer.quote
    with admin.connect() as connection:
        expires = (
            connection.execute(text("SELECT current_timestamp + interval '15 minutes'"))
            .scalar_one()
            .isoformat()
        )
        connection.exec_driver_sql(
            f"CREATE ROLE {quote(owner)} NOLOGIN NOINHERIT NOSUPERUSER "
            "NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        connection.exec_driver_sql(f"CREATE ROLE {quote(owner + '_peer')} NOLOGIN")
        connection.execution_options(no_parameters=True).exec_driver_sql(
            f"CREATE ROLE {quote(migrator)} LOGIN NOINHERIT NOSUPERUSER "
            "NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS "
            f"PASSWORD '{password}' VALID UNTIL '{expires}'"
        )
        connection.exec_driver_sql(
            f"GRANT {quote(owner)} TO {quote(migrator)} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE"
        )
        connection.exec_driver_sql(f"CREATE DATABASE {quote(database)} OWNER {quote(owner)}")
    root = Path(__file__).resolve().parents[2]
    cfg = Config(str(root / "database/migrations/alembic.ini"))
    cfg.set_main_option("script_location", str(root / "database" / "migrations"))
    cfg.set_main_option(
        "sqlalchemy.url",
        source.set(database=database, username=migrator, password=password)
        .render_as_string(hide_password=False)
        .replace("%", "%%"),
    )
    monkeypatch.setenv("LOOM_DB_OWNER_ROLE", owner)
    try:
        yield (
            cfg,
            source.set(database=database).render_as_string(hide_password=False),
            owner,
            migrator,
        )
    finally:
        with admin.connect() as connection:
            connection.exec_driver_sql(f"DROP DATABASE {quote(database)} WITH (FORCE)")
            connection.exec_driver_sql(f"DROP ROLE {quote(migrator)}")
            connection.exec_driver_sql(f"DROP ROLE {quote(owner)}")
            connection.exec_driver_sql(f"DROP ROLE {quote(owner + '_peer')}")
        admin.dispose()


def test_application_migrations_create_objects_as_nologin_owner(
    application_migration_roles: tuple[Config, str, str, str],
) -> None:
    cfg, admin_url, owner, migrator = application_migration_roles
    command.upgrade(cfg, "head")
    engine = create_engine(admin_url)
    try:
        with engine.connect() as observer:
            relations = observer.execute(
                text(
                    "SELECT c.relname, pg_get_userbyid(c.relowner) FROM pg_class AS c "
                    "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'S')"
                )
            ).all()
            assert len(relations) > 50
            assert {row[1] for row in relations} == {owner}
            assert migrator not in {row[1] for row in relations}
            functions = (
                observer.execute(
                    text(
                        "SELECT pg_get_userbyid(p.proowner) FROM pg_proc AS p "
                        "JOIN pg_namespace AS n ON n.oid = p.pronamespace WHERE n.nspname = 'public'"
                    )
                )
                .scalars()
                .all()
            )
            assert functions
            assert set(functions) == {owner}
            assert observer.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        # A fresh migration connection must assume the same owner again.
        command.downgrade(cfg, "-1")
        command.upgrade(cfg, "head")
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "drift",
    [
        "owner_login",
        "owner_superuser",
        "migrator_superuser",
        "permanent_login",
        "unbounded_login",
        "admin_grant",
        "inherited_grant",
        "missing_grant",
        "wrong_database_owner",
        "incoming_migrator_grant",
        "incoming_owner_grant",
        "outgoing_owner_grant",
        "outgoing_migrator_grant",
    ],
)
def test_application_migration_refuses_unsealed_authority_before_ddl(
    application_migration_roles: tuple[Config, str, str, str], drift: str
) -> None:
    cfg, admin_url, owner, migrator = application_migration_roles
    engine = create_engine(admin_url)
    quote = engine.dialect.identifier_preparer.quote
    statements = {
        "owner_login": f"ALTER ROLE {quote(owner)} LOGIN",
        "owner_superuser": f"ALTER ROLE {quote(owner)} SUPERUSER",
        "migrator_superuser": f"ALTER ROLE {quote(migrator)} SUPERUSER",
        "permanent_login": f"ALTER ROLE {quote(migrator)} VALID UNTIL 'infinity'",
        "unbounded_login": f"ALTER ROLE {quote(migrator)} VALID UNTIL '2099-01-01'",
        "admin_grant": f"GRANT {quote(owner)} TO {quote(migrator)} WITH ADMIN TRUE",
        "inherited_grant": f"GRANT {quote(owner)} TO {quote(migrator)} WITH INHERIT TRUE",
        "missing_grant": f"REVOKE {quote(owner)} FROM {quote(migrator)}",
        "wrong_database_owner": f"ALTER DATABASE {quote(make_url(admin_url).database)} OWNER TO {quote(migrator)}",
        "incoming_migrator_grant": f"GRANT {quote(migrator)} TO {quote(owner + '_peer')}",
        "incoming_owner_grant": f"GRANT {quote(owner)} TO {quote(owner + '_peer')}",
        "outgoing_owner_grant": f"GRANT {quote(owner + '_peer')} TO {quote(owner)}",
        "outgoing_migrator_grant": f"GRANT {quote(owner + '_peer')} TO {quote(migrator)}",
    }
    try:
        with engine.begin() as admin:
            admin.exec_driver_sql(statements[drift])
        with pytest.raises(RuntimeError, match="application migration"):
            command.upgrade(cfg, "head")
        with engine.connect() as observer:
            assert (
                observer.execute(text("SELECT to_regclass('public.alembic_version')")).scalar_one()
                is None
            )
            assert (
                observer.execute(text("SELECT to_regclass('public.trials')")).scalar_one() is None
            )
    finally:
        engine.dispose()


def test_application_owner_mode_refuses_offline_sql(
    application_migration_roles: tuple[Config, str, str, str],
) -> None:
    cfg, _, _, _ = application_migration_roles
    with pytest.raises(RuntimeError, match=r"application migration.*online"):
        command.upgrade(cfg, "head", sql=True)


@pytest.mark.parametrize("owner", ["", " owner", "owner; RESET ROLE", "owner'", "x" * 64])
def test_application_owner_mode_refuses_invalid_identity(
    application_migration_roles: tuple[Config, str, str, str],
    monkeypatch: pytest.MonkeyPatch,
    owner: str,
) -> None:
    cfg, _, _, _ = application_migration_roles
    monkeypatch.setenv("LOOM_DB_OWNER_ROLE", owner)
    with pytest.raises(RuntimeError, match="application migration owner"):
        command.upgrade(cfg, "head")
