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
async def test_runtime_accepts_only_private_agent_and_disposes_on_rejection(
    build_guard_database, owner_sessions, tmp_path, monkeypatch, privileged
):
    module = import_module("loom_service.personal_dev_build_admission")
    _config, engine, _owner, _agent, agent_url = build_guard_database
    document, _database, _principals = inputs(tmp_path)
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
        assert runtime.verifier.verify_bearer("Bearer executor-secret").pool_id == "oldlab"
        assert runtime.sessions.kw["bind"] is created[0]
        await runtime.aclose()
        assert disposed == created
