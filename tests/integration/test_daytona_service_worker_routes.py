from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    DaytonaSandbox,
    Task,
    Team,
    Token,
    Trial,
    Worker,
)
from loom_control_plane.routes import daytona_sandboxes


async def test_daytona_lifecycle_is_idempotent_and_usage_is_singleton(
    postgres_url: str,
) -> None:
    now = datetime.now(tz=UTC)
    team_id = uuid4()
    trial_id = uuid4()
    owner_worker_id = uuid4()
    cleanup_worker_id = uuid4()
    task_id = f"daytona-route/{uuid4()}"
    raw_token = f"daytona-worker-{uuid4().hex}"
    token_hash = hashlib.sha256(raw_token.encode()).digest()
    caps = [
        {
            "backend": "daytona",
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "network_policies": ["public"],
            "dynamic_network_policy": True,
            "mounted_fs": True,
            "resource_modes": ["auto"],
        }
    ]
    sync_engine = create_engine(postgres_url)
    sessions = sessionmaker(sync_engine)
    with sessions.begin() as session:
        session.execute(insert(Team).values(id=team_id, name=f"daytona-{team_id}"))
        session.execute(
            insert(Task).values(
                id=task_id,
                checksum="a" * 64,
                config={
                    "schema_version": "1",
                    "task": {"id": task_id, "name": task_id},
                    "environment": {
                        "os": "linux",
                        "docker_image": "registry.example/task@sha256:" + "b" * 64,
                    },
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                },
            )
        )
        for worker_id, hostname in (
            (owner_worker_id, "daytona-owner"),
            (cleanup_worker_id, "daytona-cleanup"),
        ):
            session.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname=hostname,
                    version="test",
                    capabilities=caps,
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
        session.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={"agent_name": "oracle", "agent_model": None},
                requires_caps={
                    "backend": "daytona",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                },
                state="claimed",
                worker_id=owner_worker_id,
                attempt_count=1,
            )
        )
        session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["worker:report"],
                team_id=None,
                issued_at=now,
                expires_at=None,
            )
        )

    async_engine = create_async_engine(postgres_url)
    app = FastAPI()
    app.include_router(daytona_sandboxes.router)
    app.state.session_factory = async_sessionmaker(async_engine, expire_on_commit=False)
    headers = {"Authorization": f"Bearer {raw_token}"}
    provider_scope = "c" * 64
    image = "registry.example/task@sha256:" + "b" * 64
    reservation = {
        "trial_id": str(trial_id),
        "team_id": str(team_id),
        "attempt_count": 1,
        "candidate_sha": "d" * 40,
        "provider_scope": provider_scope,
        "artifact_ref": image,
        "sandbox_name": f"loom-{trial_id.hex}-1-dddddddd",
        "deadline_at": (now + timedelta(hours=1)).isoformat(),
    }
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://control-plane",
        ) as client:
            reserve_url = f"/workers/{owner_worker_id}/daytona-sandboxes/reserve"
            first = await client.post(reserve_url, headers=headers, json=reservation)
            assert first.status_code == 200, first.text
            second = await client.post(reserve_url, headers=headers, json=reservation)
            assert second.status_code == 200, second.text
            assert second.json()["id"] == first.json()["id"]
            ledger_id = first.json()["id"]

            drifted = await client.post(
                reserve_url,
                headers=headers,
                json={
                    **reservation,
                    "artifact_ref": "registry.example/other@sha256:" + "e" * 64,
                },
            )
            assert drifted.status_code == 409

            started_at = now + timedelta(seconds=5)
            started = await client.post(
                f"/workers/{owner_worker_id}/daytona-sandboxes/{ledger_id}/started",
                headers=headers,
                json={"sandbox_id": "sandbox-route-test", "started_at": started_at.isoformat()},
            )
            assert started.status_code == 200, started.text

            with sessions.begin() as session:
                session.execute(update(Trial).where(Trial.id == trial_id).values(state="failed"))

            cleanup = await client.post(
                f"/workers/{cleanup_worker_id}/daytona-sandboxes/claim-cleanup",
                headers=headers,
                json={"provider_scope": provider_scope},
            )
            assert cleanup.status_code == 200, cleanup.text
            assert cleanup.json()["id"] == ledger_id

            stopped_at = started_at + timedelta(seconds=10)
            deleted_response = await client.post(
                f"/workers/{cleanup_worker_id}/daytona-sandboxes/{ledger_id}/deleted",
                headers=headers,
                json={"deleted": True, "stopped_at": stopped_at.isoformat()},
            )
            assert deleted_response.status_code == 200, deleted_response.text

            retry = await client.post(
                f"/workers/{owner_worker_id}/daytona-sandboxes/{ledger_id}/deleted",
                headers=headers,
                json={"deleted": True, "stopped_at": stopped_at.isoformat()},
            )
            assert retry.status_code == 200, retry.text
            assert retry.json()["state"] == "deleted"

        with sessions() as session:
            usage_count = session.scalar(
                text(
                    "SELECT count(*) FROM cloud_compute_records "
                    "WHERE trial_id=CAST(:trial_id AS uuid)"
                ),
                {"trial_id": trial_id},
            )
            row = session.scalar(select(DaytonaSandbox).where(DaytonaSandbox.id == UUID(ledger_id)))
            assert usage_count == 1
            assert row is not None and row.state == "deleted"
    finally:
        await async_engine.dispose()
        with sessions.begin() as session:
            session.execute(
                text("DELETE FROM cloud_compute_records WHERE trial_id=CAST(:trial_id AS uuid)"),
                {"trial_id": trial_id},
            )
            session.execute(delete(DaytonaSandbox).where(DaytonaSandbox.trial_id == trial_id))
            session.execute(delete(Trial).where(Trial.id == trial_id))
            session.execute(delete(Task).where(Task.id == task_id))
            session.execute(
                delete(Worker).where(Worker.id.in_([owner_worker_id, cleanup_worker_id]))
            )
            session.execute(delete(Token).where(Token.token_hash == token_hash))
            session.execute(delete(Team).where(Team.id == team_id))
        sync_engine.dispose()
