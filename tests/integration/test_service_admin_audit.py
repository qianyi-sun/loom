"""Admin audit events for issue #10 admin mutations."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "A" * 43


@pytest.fixture
async def audit_app(
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
                "team_invites",
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


def _admin_headers(actor: str | None = "security-owner") -> dict[str, str]:
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    if actor is not None:
        headers["X-Loom-Admin-Actor"] = actor
    return headers


async def _register_team(
    client: httpx.AsyncClient,
    *,
    name: str,
) -> str:
    created = await client.post(
        "/api/v1/teams/register",
        json={"name": name, "contact_email": f"{name}@example.com"},
    )
    assert created.status_code == 202, created.text
    return created.json()["id"]


async def test_registration_review_writes_safe_audit_events(
    audit_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=audit_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        approved_registration_id = await _register_team(
            client,
            name="latent-audit",
        )
        rejected_registration_id = await _register_team(
            client,
            name="memory-audit",
        )

        approved = await client.post(
            "/api/v1/admin/team-registrations/"
            f"{approved_registration_id}/approve",
            headers=_admin_headers("qianyi"),
        )
        rejected = await client.post(
            "/api/v1/admin/team-registrations/"
            f"{rejected_registration_id}/reject",
            headers=_admin_headers("hongjian"),
            json={"reason": "operator denied during audit test"},
        )
        audit = await client.get(
            "/api/v1/admin/audit-events",
            headers=_admin_headers(),
        )

    assert approved.status_code == 200, approved.text
    assert rejected.status_code == 200, rejected.text
    assert audit.status_code == 200, audit.text
    body = audit.json()
    assert body["next_cursor"] is None
    events = body["items"]
    by_action = {event["action"]: event for event in events}
    assert set(by_action) == {
        "team_registration.approve",
        "team_registration.reject",
    }

    approve_event = by_action["team_registration.approve"]
    approved_body = approved.json()
    raw_invite_code = approved_body["invite_code"]
    assert approve_event["actor"] == "qianyi"
    assert approve_event["target_type"] == "team_registration"
    assert approve_event["target_id"] == approved_registration_id
    assert approve_event["metadata"]["team_id"] == approved_body["team"]["id"]
    assert approve_event["metadata"]["invite_prefix"] == (
        approved_body["invite"]["code_prefix"]
    )
    assert raw_invite_code not in json.dumps(approve_event)

    reject_event = by_action["team_registration.reject"]
    assert reject_event["actor"] == "hongjian"
    assert reject_event["target_id"] == rejected_registration_id
    assert reject_event["metadata"] == {"reason_present": True}


async def test_registration_review_rolls_back_when_audit_write_fails(
    audit_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import loom_service.routes.team_registrations as registration_routes

    async def _raise_audit_error(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("audit insert failed")

    monkeypatch.setattr(
        registration_routes,
        "write_admin_audit_event",
        _raise_audit_error,
        raising=False,
    )

    transport = httpx.ASGITransport(
        app=audit_app,
        raise_app_exceptions=False,
    )
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        registration_id = await _register_team(client, name="rollback-audit")
        approved = await client.post(
            f"/api/v1/admin/team-registrations/{registration_id}/approve",
            headers=_admin_headers("qianyi"),
        )
        pending = await client.get(
            "/api/v1/admin/team-registrations?status=pending",
            headers=_admin_headers(),
        )

    assert approved.status_code == 500, approved.text
    assert [item["id"] for item in pending.json()["items"]] == [
        registration_id,
    ]


async def test_admin_token_mutations_require_actor_and_write_audit(
    audit_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=audit_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        registration_id = await _register_team(client, name="token-audit")
        approved = await client.post(
            f"/api/v1/admin/team-registrations/{registration_id}/approve",
            headers=_admin_headers("bootstrap-admin"),
        )
        assert approved.status_code == 200, approved.text
        team_id = UUID(approved.json()["team"]["id"])

        missing_actor = await client.post(
            "/api/v1/tokens",
            headers=_admin_headers(actor=None),
            json={
                "type": "team",
                "team_id": str(team_id),
                "scopes": ["read:own"],
                "expires_in_days": 7,
            },
        )
        minted = await client.post(
            "/api/v1/tokens",
            headers=_admin_headers("ops-admin"),
            json={
                "type": "team",
                "team_id": str(team_id),
                "scopes": ["read:own"],
                "expires_in_days": 7,
            },
        )
        assert minted.status_code == 201, minted.text
        raw_token = minted.json()["token"]
        token_prefix = minted.json()["token_hash_prefix"]
        revoked = await client.delete(
            f"/api/v1/tokens/{token_prefix}",
            headers=_admin_headers("ops-admin"),
        )
        audit = await client.get(
            "/api/v1/admin/audit-events?limit=10",
            headers=_admin_headers(),
        )

    assert missing_actor.status_code == 400, missing_actor.text
    assert "X-Loom-Admin-Actor" in missing_actor.json()["detail"]
    assert revoked.status_code == 204, revoked.text
    assert audit.status_code == 200, audit.text
    token_events = [
        event for event in audit.json()["items"]
        if event["target_type"] == "token"
    ]
    assert [event["action"] for event in token_events] == [
        "token.revoke",
        "token.create",
    ]
    for event in token_events:
        assert event["actor"] == "ops-admin"
        assert event["target_id"] == token_prefix
        assert event["metadata"]["token_hash_prefix"] == token_prefix
        assert event["metadata"]["token_type"] == "team"
        assert event["metadata"]["team_id"] == str(team_id)
        assert raw_token not in json.dumps(event)


async def test_admin_audit_endpoint_rejects_team_tokens(
    audit_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=audit_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        registration_id = await _register_team(client, name=f"team-{uuid4()}")
        approved = await client.post(
            f"/api/v1/admin/team-registrations/{registration_id}/approve",
            headers=_admin_headers("qianyi"),
        )
        token = await client.post(
            "/api/v1/tokens",
            headers=_admin_headers("qianyi"),
            json={
                "type": "team",
                "scopes": ["read:own"],
                "team_id": approved.json()["team"]["id"],
                "expires_in_days": 1,
            },
        )
        assert token.status_code == 201, token.text
        audit = await client.get(
            "/api/v1/admin/audit-events",
            headers={"Authorization": f"Bearer {token.json()['token']}"},
        )

    assert audit.status_code == 403, audit.text


async def test_admin_audit_endpoint_pages_with_cursor(
    audit_app: FastAPI,
) -> None:
    transport = httpx.ASGITransport(app=audit_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        first_registration_id = await _register_team(client, name="page-one")
        second_registration_id = await _register_team(client, name="page-two")
        first_approval = await client.post(
            f"/api/v1/admin/team-registrations/{first_registration_id}/approve",
            headers=_admin_headers("qianyi"),
        )
        second_approval = await client.post(
            f"/api/v1/admin/team-registrations/{second_registration_id}/approve",
            headers=_admin_headers("qianyi"),
        )
        first_page = await client.get(
            "/api/v1/admin/audit-events?limit=1",
            headers=_admin_headers(),
        )
        next_cursor = first_page.json()["next_cursor"]
        second_page = await client.get(
            f"/api/v1/admin/audit-events?limit=1&cursor={next_cursor}",
            headers=_admin_headers(),
        )

    assert first_approval.status_code == 200, first_approval.text
    assert second_approval.status_code == 200, second_approval.text
    assert first_page.status_code == 200, first_page.text
    assert second_page.status_code == 200, second_page.text
    assert len(first_page.json()["items"]) == 1
    assert len(second_page.json()["items"]) == 1
    assert first_page.json()["items"][0]["id"] != second_page.json()["items"][0]["id"]
    assert second_page.json()["next_cursor"] is None
