"""Configured management admission verifies the real least-privileged DB role."""

from importlib import import_module

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.unit.test_personal_dev_build_admission_runtime import inputs, settings


@pytest.mark.parametrize("privileged", [False, True])
@pytest.mark.parametrize("mode", ["prepare-bind-only", "native-registration", "native-claims"])
async def test_runtime_accepts_only_private_agent_and_disposes_on_rejection(
    build_guard_database, owner_sessions, tmp_path, monkeypatch, privileged, mode
):
    module = import_module("loom_service.personal_dev_build_admission")
    _config, engine, _owner, _agent, agent_url = build_guard_database
    document, _database, _principals = inputs(tmp_path)
    document["mode"] = mode
    configured = settings(tmp_path, document)
    created = []
    disposed = []

    def isolated_engine(url, **kwargs):
        # Production DSN shape/TLS validation still runs. The test connection uses
        # the disposable fixture, whose PostgreSQL listener is not TLS-enabled.
        assert "sslmode=verify-full" in url
        assert kwargs["isolation_level"] == "SERIALIZABLE"
        instance = create_async_engine(
            (engine.url if privileged else agent_url).set(drivername="postgresql+psycopg"), **kwargs
        )
        created.append(instance)
        return instance

    from sqlalchemy.ext.asyncio import AsyncEngine

    real_dispose = AsyncEngine.dispose

    async def observed_dispose(instance, *args, **kwargs):
        disposed.append(instance)
        await real_dispose(instance, *args, **kwargs)

    monkeypatch.setattr(module, "create_async_engine", isolated_engine)
    monkeypatch.setattr(AsyncEngine, "dispose", observed_dispose)
    if privileged:
        with pytest.raises(RuntimeError, match=r"private|privilege|agent"):
            await module.build_personal_build_admission_runtime(configured)
        assert disposed == created
    else:
        runtime = await module.build_personal_build_admission_runtime(configured)
        assert runtime is not None
        assert runtime.mode == mode
        assert runtime.verifier.verify_bearer("Bearer executor-secret").pool_id == "oldlab"
        assert runtime.sessions.kw["bind"] is created[0]
        await runtime.aclose()
        assert disposed == created


@pytest.mark.parametrize("boundary", ["foreign-prepare", "foreign-bind", "foreign-register", "foreign-claim", "foreign-drain", "owner-login", "owner-createdb", "owner-membership"])
async def test_runtime_rejects_protected_authority_drift(
    build_guard_database, owner_sessions, tmp_path, monkeypatch, boundary
):
    module = import_module("loom_service.personal_dev_build_admission")
    _config, engine, owner, agent, agent_url = build_guard_database
    quote = engine.dialect.identifier_preparer.quote
    foreign = "foreign_" + agent
    with engine.begin() as connection:
        connection.exec_driver_sql(f"CREATE ROLE {quote(foreign)} LOGIN NOINHERIT")
        if boundary.startswith("foreign-"):
            signature = {"foreign-prepare": "prepare_worker(uuid,jsonb,bytea,text,text)",
                "foreign-bind": "bind_slurm_job(uuid,jsonb,bytea,text)",
                "foreign-register": "register_worker(uuid,jsonb,bytea,text,text)",
                "foreign-claim": "claim_platform(uuid,jsonb,bytea,text,text)",
                "foreign-drain": "begin_drain(uuid,jsonb,bytea,text)"}[boundary]
            connection.exec_driver_sql(f"GRANT USAGE ON SCHEMA loom_capacity_build_guard TO {quote(foreign)}")
            connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION loom_capacity_build_guard.{signature} TO {quote(foreign)}")
        elif boundary == "owner-login":
            connection.exec_driver_sql(f"ALTER ROLE {quote(owner)} LOGIN")
        elif boundary == "owner-createdb":
            connection.exec_driver_sql(f"ALTER ROLE {quote(owner)} CREATEDB")
        else:
            connection.exec_driver_sql(f"GRANT {quote(foreign)} TO {quote(owner)}")
    document, _, _ = inputs(tmp_path)
    document["mode"] = "native-claims"
    configured = settings(tmp_path, document)
    created = []

    def isolated_engine(url, **kwargs):
        instance = create_async_engine(agent_url.set(drivername="postgresql+psycopg"), **kwargs)
        created.append(instance)
        return instance

    monkeypatch.setattr(module, "create_async_engine", isolated_engine)
    try:
        with pytest.raises(RuntimeError, match=r"private|privilege|owner"):
            await module.build_personal_build_admission_runtime(configured)
    finally:
        for instance in created:
            await instance.dispose()
        with engine.begin() as connection:
            connection.exec_driver_sql(f"DROP OWNED BY {quote(foreign)}")
            connection.exec_driver_sql(f"DROP ROLE {quote(foreign)}")
