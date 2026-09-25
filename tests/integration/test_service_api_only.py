"""API-only processes share records without owning the shared background jobs.

This tests the process capability, not installed per-origin session isolation or
independent deployment versions. Those require the new shared-instance binding.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from uuid import uuid4

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Team, TeamMembership, User
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password


async def test_four_apis_share_records_and_survive_instance_replacement(
    isolated_migration_postgres_url: str,
) -> None:
    team_id, foreign_team_id = uuid4(), uuid4()
    owner_ids = [uuid4() for _ in range(5)]
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with async_sessionmaker(engine)() as session:
            session.add_all([
                Team(id=team_id, name="shared-development"),
                Team(id=foreign_team_id, name="private-other-team"),
            ])
            password_hash = hash_password("shared-development-test-passphrase")
            session.add_all([
                User(id=owner_id, username=f"developer-{index}", username_normalized=f"developer-{index}",
                     status="active", password_hash=password_hash)
                for index, owner_id in enumerate(owner_ids)
            ])
            await session.flush()
            session.add_all([
                TeamMembership(team_id=team_id, user_id=owner_id, role="member")
                for owner_id in owner_ids
            ])
            await session.commit()
    finally:
        await engine.dispose()

    async with AsyncExitStack() as processes:
        apps, clients, lifecycles = [], [], []
        for index in range(5):
            if index == 4:
                await lifecycles[0].aclose()
                assert not hasattr(apps[0].state, "session_factory")
                assert apps[0].state.http_client.is_closed
                # Data and authentication are still usable through Bob's API.
                me = await clients[1].get("/api/v1/auth/me")
                assert me.status_code == 200
                assert str(owner_ids[1]) in me.text
                async with apps[1].state.session_factory() as session:
                    team = await session.get(Team, team_id)
                    assert team is not None
                    team.name = "retained-shared-development"
                    await session.commit()

            origin = f"https://developer-{index}.example.com"
            app = create_app(LoomServiceSettings(
                _env_file=None, service_mode="api_only", db_url=isolated_migration_postgres_url,
                public_base_url=origin, auth_local_http=False,
                minio_endpoint="https://offline-storage.invalid",
                minio_access_key="test-access", minio_secret_key="test-secret",
                control_plane_url="https://offline-cp.invalid", gateway_url="https://offline-gateway.invalid",
            ))
            lifecycle = await processes.enter_async_context(AsyncExitStack())
            await lifecycle.enter_async_context(app.router.lifespan_context(app))
            client = await lifecycle.enter_async_context(httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url=origin,
            ))
            apps.append(app)
            clients.append(client)
            lifecycles.append(lifecycle)
            assert (await client.get("/api/v1/tasks")).status_code == 401
            login = await client.post("/api/v1/auth/login", json={
                "username": f"developer-{index}", "password": "shared-development-test-passphrase",
            })
            assert login.status_code == 200, login.text
            assert client.cookies.get("__Host-loom_session")
            own_team = await client.get(f"/api/v1/teams/{team_id}")
            assert own_team.status_code == 200, own_team.text
            assert own_team.json()["name"] == (
                "shared-development" if index < 4 else "retained-shared-development"
            )
            assert (await client.get(f"/api/v1/teams/{foreign_team_id}")).status_code == 403
            assert (await client.get("/api/v1/environments")).status_code == 404
            assert not any(task.get_name().startswith("loom-svc-") for task in asyncio.all_tasks())

        for client in clients[1:]:
            response = await client.get(f"/api/v1/teams/{team_id}")
            assert response.status_code == 200
            assert response.json()["name"] == "retained-shared-development"
    assert all(not hasattr(app.state, "session_factory") for app in apps)
    assert all(app.state.http_client.is_closed and app.state.gateway_client.is_closed for app in apps)
