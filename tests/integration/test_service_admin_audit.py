"""Admin audit events for issue #10 admin mutations."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import AdminAuditEvent
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


async def _create_internal_team(
    client: httpx.AsyncClient,
    *,
    name: str,
    actor: str = "security-owner",
) -> str:
    created = await client.post(
        "/api/v1/admin/teams",
        headers=_admin_headers(actor),
        json={"name": name},
    )
    assert created.status_code == 201, created.text
    return created.json()["id"]


async def _approve_registration(
    client: httpx.AsyncClient,
    registration_id: str,
    *,
    team_id: str,
    actor: str = "security-owner",
    role: str = "member",
) -> httpx.Response:
    return await client.post(
        f"/api/v1/admin/team-registrations/{registration_id}/approve",
        headers=_admin_headers(actor),
        json={"team_id": team_id, "role": role},
    )


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
        team_id = await _create_internal_team(
            client,
            name="Audit Review Team",
            actor="qianyi",
        )

        approved = await _approve_registration(
            client,
            approved_registration_id,
            team_id=team_id,
            actor="qianyi",
        )
        rejected = await client.post(
            f"/api/v1/admin/team-registrations/{rejected_registration_id}/reject",
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
    events = [
        event
        for event in body["items"]
        if event["target_type"] == "team_registration"
        and event["target_id"]
        in {
            approved_registration_id,
            rejected_registration_id,
        }
    ]
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
    assert approve_event["metadata"]["team_name"] == "Audit Review Team"
    assert approve_event["metadata"]["invite_role"] == "member"
    assert approve_event["metadata"]["invite_prefix"] == (approved_body["invite"]["code_prefix"])
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
        team_id = await _create_internal_team(
            client,
            name="Rollback Audit Team",
            actor="qianyi",
        )
        approved = await _approve_registration(
            client,
            registration_id,
            team_id=team_id,
            actor="qianyi",
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
        team_id_raw = await _create_internal_team(
            client,
            name="Token Audit Team",
            actor="bootstrap-admin",
        )
        approved = await _approve_registration(
            client,
            registration_id,
            team_id=team_id_raw,
            actor="bootstrap-admin",
        )
        assert approved.status_code == 200, approved.text
        team_id = UUID(approved.json()["team"]["id"])

        missing_actor = await client.post(
            "/api/v1/tokens",
            headers=_admin_headers(actor=None),
            json={
                "name": "missing-actor-token",
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
                "name": "audited-token",
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
    token_events = [event for event in audit.json()["items"] if event["target_type"] == "token"]
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
        team_id = await _create_internal_team(
            client,
            name=f"Audit Team {uuid4()}",
            actor="qianyi",
        )
        approved = await _approve_registration(
            client,
            registration_id,
            team_id=team_id,
            actor="qianyi",
        )
        assert approved.status_code == 200, approved.text
        token = await client.post(
            "/api/v1/tokens",
            headers=_admin_headers("qianyi"),
            json={
                "name": "team-reader",
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


async def test_admin_audit_uuid_cursor_is_stable_and_rolling_compatible(
    audit_app: FastAPI,
    postgres_url: str,
) -> None:
    tied_at = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    expected: list[tuple[UUID, datetime]] = []
    for index in range(53):
        event_id = UUID(int=50_000 + index)
        created_at = tied_at if index < 27 else tied_at - timedelta(minutes=index - 26)
        expected.append((event_id, created_at))
        rows.append(
            {
                "id": event_id,
                "created_at": created_at,
                "actor": "cursor-test-admin",
                "action": "cursor.test",
                "target_type": "cursor_fixture",
                "target_id": f"row-{index:02d}",
                "request_id": f"request-{index:02d}",
                "source_ip_hash": None,
                "user_agent_hash": None,
                "event_metadata": {"fixture_index": index},
            }
        )

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(AdminAuditEvent), rows)
    sync_engine.dispose()

    expected_ids = [
        str(event_id)
        for event_id, _created_at in sorted(
            expected,
            key=lambda item: (item[1], item[0].int),
            reverse=True,
        )
    ]
    transport = httpx.ASGITransport(app=audit_app)
    ids: list[str] = []
    page_sizes: list[int] = []
    cursors: list[str | None] = []
    cursor: str | None = None
    first_page_last_created_at: datetime | None = None
    first_page_last_id: UUID | None = None

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        while True:
            params = {"limit": "17"}
            if cursor is not None:
                params["cursor"] = cursor
            response = await client.get(
                "/api/v1/admin/audit-events",
                params=params,
                headers=_admin_headers(),
            )
            assert response.status_code == 200, response.text
            body = response.json()
            page_ids = [item["id"] for item in body["items"]]
            ids.extend(page_ids)
            page_sizes.append(len(page_ids))
            cursors.append(body["next_cursor"])
            if len(page_sizes) == 1:
                first_page_last_id = UUID(page_ids[-1])
                first_page_last_created_at = datetime.fromisoformat(body["items"][-1]["created_at"])
            cursor = body["next_cursor"]
            if cursor is None:
                break
            assert len(cursors) < 10
            if cursor is not None:
                UUID(cursor)

        invalid = await client.get(
            "/api/v1/admin/audit-events",
            params={"cursor": "not-a-uuid-cursor"},
            headers=_admin_headers(),
        )

    assert page_sizes == [17, 17, 17, 2]
    assert cursors[-1] is None
    assert ids == expected_ids
    assert len(ids) == 53
    assert len(set(ids)) == 53
    assert first_page_last_id is not None
    assert first_page_last_created_at is not None
    assert UUID(cursors[0] or "") == first_page_last_id
    assert invalid.status_code == 400
    assert "invalid cursor" in invalid.json()["detail"]
