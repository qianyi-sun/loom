"""Real TLS PostgreSQL management bootstrap, with no execution credentials."""

from __future__ import annotations

import psycopg
import pytest
from sqlalchemy.engine import make_url

from loom import nebius_platform_bootstrap as bootstrap
from tests.integration.test_nebius_platform_bootstrap import platform_database as platform_database


def test_management_bootstrap_migrates_without_execution_roles_or_worker_tokens(platform_database, monkeypatch):
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    monkeypatch.setenv("LOOM_DB_SERVICE_PASSWORD", "management-test-password-" + "x" * 30)
    for name in ("LOOM_COLLECTOR_TOKEN", "LOOM_BATCH_RUNNER_TOKEN", "LOOM_DB_CONTROL_PLANE_PASSWORD",
                 "LOOM_DB_GATEWAY_PASSWORD", "LOOM_DB_ACTUATOR_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    for _ in range(2):
        bootstrap.bootstrap_management_database({"namespace": "loom-nebius-management"})
    with psycopg.connect(platform_database) as db:
        assert db.execute("SELECT count(*) FROM tokens WHERE type='worker'").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM execution_targets").fetchone() == (0,)
        assert db.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('loom_service', 'loom_control_plane', 'loom_gateway', 'loom_actuator')").fetchall() == [("loom_service",)]
        assert db.execute("SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname='loom_service'").fetchone() == (False,) * 5
        assert db.execute("SELECT version_num FROM alembic_version").fetchone() is not None
    service_url = make_url(platform_database).set(username="loom_service", password="management-test-password-" + "x" * 30).render_as_string(hide_password=False)
    with psycopg.connect(service_url, autocommit=True) as db:
        for table in ("users", "teams", "nebius_platform_budgets", "nebius_environments"):
            assert db.execute("SELECT has_table_privilege(current_user, %s, 'SELECT,INSERT,UPDATE,DELETE')", (table,)).fetchone() == (True,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.execute("CREATE ROLE forbidden_admin SUPERUSER")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.execute("CREATE TABLE public.forbidden_ddl (id integer)")
