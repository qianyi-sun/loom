"""Team registration + admin approval API for issue #10."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "R" * 43


@pytest.fixture
async def registration_app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[FastAPI]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )
    try:
        yield app
    finally:
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        with sync_engine.begin() as conn:
            for table_name in (
                "admin_audit_events",
                "tokens",
                "team_quotas",
                "pending_team_registrations",
                "teams",
            ):
                exists = conn.execute(
                    text("SELECT to_regclass(:table_name)"),
                    {"table_name": table_name},
                ).scalar_one()
                if exists is not None:
                    conn.execute(text(f"DELETE FROM {table_name}"))
        sync_engine.dispose()


async def _post_registration(
    client: httpx.AsyncClient,
    *,
    name: str = "latent-reasoning",
    contact_email: str = "latent@example.com",
) -> httpx.Response:
    return await client.post(
        "/api/v1/teams/register",
        json={"name": name, "contact_email": contact_email},
    )


def _admin_headers(actor: str = "qianyi") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
        "X-Loom-Admin-Actor": actor,
    }


async def test_closed_registration_creates_pending_row_without_auth(
    registration_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=registration_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        response = await _post_registration(client)

    assert response.status_code == 202, response.text
    body = response.json()
    assert UUID(body["id"])
    assert body["name"] == "latent-reasoning"
    assert body["contact_email"] == "latent@example.com"
    assert body["status"] == "pending"
    assert "requested_at" in body
    assert "token" not in body


async def test_duplicate_pending_registration_names_conflict_case_insensitive(
    registration_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=registration_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        first = await _post_registration(client, name="Latent-Reasoning")
        second = await _post_registration(client, name="latent-reasoning")

    assert first.status_code == 202, first.text
    assert second.status_code == 409, second.text
    assert "already has an active registration" in second.json()["detail"]


async def test_open_registration_setting_fails_closed_until_challenge_hook(
    registration_app: FastAPI,
) -> None:
    registration_app.state.settings.team_registration_open = True
    transport = httpx.ASGITransport(app=registration_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        response = await _post_registration(client, name="open-mode-team")

    assert response.status_code == 501, response.text
    assert "challenge hook" in response.json()["detail"]


async def test_admin_can_list_approve_and_use_revealed_team_token(
    registration_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=registration_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        created = await _post_registration(client)
        registration_id = created.json()["id"]

        listed = await client.get(
            "/api/v1/admin/team-registrations?status=pending",
            headers=_admin_headers(),
        )
        approved = await client.post(
            f"/api/v1/admin/team-registrations/{registration_id}/approve",
            headers=_admin_headers(),
        )
        second_approve = await client.post(
            f"/api/v1/admin/team-registrations/{registration_id}/approve",
            headers=_admin_headers(),
        )

        assert approved.status_code == 200, approved.text
        approved_body = approved.json()
        team_token = approved_body["team_token"]
        team_id = approved_body["team"]["id"]
        team_detail = await client.get(
            f"/api/v1/teams/{team_id}",
            headers={"Authorization": f"Bearer {team_token}"},
        )

    assert listed.status_code == 200, listed.text
    assert [item["id"] for item in listed.json()["items"]] == [registration_id]
    assert approved_body["registration"]["status"] == "approved"
    assert approved_body["team"]["name"] == "latent-reasoning"
    assert team_token.startswith("loom_team_")
    assert len(approved_body["token_hash_prefix"]) == 8
    assert second_approve.status_code == 409, second_approve.text
    assert team_detail.status_code == 200, team_detail.text
    assert team_detail.json()["id"] == team_id
    assert team_detail.json()["quota"] is not None


async def test_admin_can_reject_pending_registration(
    registration_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=registration_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        created = await _post_registration(client, name="memory-team")
        registration_id = created.json()["id"]
        rejected = await client.post(
            f"/api/v1/admin/team-registrations/{registration_id}/reject",
            headers=_admin_headers("security-owner"),
            json={"reason": "duplicate offline request"},
        )
        listed = await client.get(
            "/api/v1/admin/team-registrations?status=rejected",
            headers=_admin_headers(),
        )

    assert rejected.status_code == 200, rejected.text
    assert rejected.json()["status"] == "rejected"
    assert rejected.json()["reviewed_by_actor"] == "security-owner"
    assert [item["id"] for item in listed.json()["items"]] == [registration_id]
