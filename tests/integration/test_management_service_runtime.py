"""Real management lifespan/auth with no child database, storage or runtime."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text

from loom.db.schema import Team, TeamMembership, User
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password


async def test_management_lifespan_login_and_readiness_without_children(
    isolated_migration_postgres_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("LOOM_SVC_MINIO_ACCESS_KEY", "LOOM_SVC_MINIO_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    settings = LoomServiceSettings(
        _env_file=None, service_mode="management", auth_local_http=False,
        db_url=isolated_migration_postgres_url, public_base_url="https://manage.example.com",
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        assert not any(task.get_name() in {
            "loom-svc-batch-runner", "loom-svc-taskset-materializer", "loom-svc-taskset-gc",
        } for task in asyncio.all_tasks())
        for attr in ("minio_client", "http_client", "gateway_client", "pipeline_binding_resolver"):
            assert not hasattr(app.state, attr)
        async with app.state.session_factory() as session:
            team = Team(id=uuid4(), name="management-owners")
            user = User(
                id=uuid4(), username="alice", username_normalized="alice", status="active",
                password_hash=hash_password("management-owner-passphrase"),
            )
            session.add_all([team, user])
            await session.flush()
            session.add(TeamMembership(team_id=team.id, user_id=user.id, role="owner"))
            await session.commit()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="https://manage.example.com",
        ) as client:
            assert (await client.get("/api/v1/health")).status_code == 200
            ready = await client.get("/api/v1/health/ready")
            assert ready.status_code == 200
            assert ready.json() == {"status": "ready", "mode": "management", "postgres": "ready"}
            login = await client.post("/api/v1/auth/login", json={
                "username": "alice", "password": "management-owner-passphrase",
            })
            assert login.status_code == 200, login.text
            assert client.cookies.get("__Host-loom_session")
            me = await client.get("/api/v1/auth/me")
            assert me.status_code == 200
            assert str(user.id) in me.text
            for path in ("trials", "tasks", "batches", "tasksets", "pipeline/runs", "provider-connections"):
                assert (await client.get("/api/v1/" + path)).status_code == 404
                assert (await client.post("/api/v1/" + path, json={})).status_code == 404
            rejected = await client.post("/api/v1/auth/login", json={
                "username": "alice", "password": "management-owner-passphrase",
            }, headers={"Origin": "https://alice.example.com"})
            assert rejected.status_code == 403
    assert not hasattr(app.state, "session_factory")
    restarted = create_app(settings)
    async with restarted.router.lifespan_context(restarted), httpx.AsyncClient(
        transport=httpx.ASGITransport(app=restarted), base_url="https://manage.example.com",
        cookies=client.cookies,
    ) as resumed_client:
        me = await resumed_client.get("/api/v1/auth/me")
        assert me.status_code == 200
        assert str(user.id) in me.text


async def test_management_startup_rejects_schema_drift(isolated_migration_postgres_url: str) -> None:
    from sqlalchemy.ext.asyncio import create_async_engine

    from loom.db.schema_startup import SchemaNotAtHeadError

    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("UPDATE alembic_version SET version_num = '0153_nebius_native_cpu_plans'"))
        app = create_app(LoomServiceSettings(
            _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        ))
        with pytest.raises(SchemaNotAtHeadError):
            async with app.router.lifespan_context(app):
                pytest.fail("management served an unvalidated schema")
        assert not hasattr(app.state, "session_factory")
    finally:
        await engine.dispose()


async def test_management_readiness_reports_database_outage_without_details(
    isolated_migration_postgres_url: str,
) -> None:
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    url = make_url(isolated_migration_postgres_url)
    assert url.database is not None and url.database.startswith("loom_migration_")
    admin = create_async_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    quoted_database = admin.dialect.identifier_preparer.quote(url.database)
    app = create_app(LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
    ))
    try:
        async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="https://manage.example.com",
        ) as client:
            async with admin.connect() as connection:
                await connection.execute(text(f"ALTER DATABASE {quoted_database} ALLOW_CONNECTIONS false"))
            await app.state._owned_service_engine.dispose()
            response = await client.get("/api/v1/health/ready")
            assert response.status_code == 503
            assert response.json() == {"status": "not-ready", "mode": "management", "postgres": "not-ready"}
            assert (await client.get("/api/v1/health")).status_code == 200
            async with admin.connect() as connection:
                await connection.execute(text(f"ALTER DATABASE {quoted_database} ALLOW_CONNECTIONS true"))
            assert (await client.get("/api/v1/health/ready")).status_code == 200
    finally:
        async with admin.connect() as connection:
            await connection.execute(text(f"ALTER DATABASE {quoted_database} ALLOW_CONNECTIONS true"))
        await admin.dispose()
