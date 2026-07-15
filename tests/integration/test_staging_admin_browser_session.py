"""Staging-only platform-admin browser-session bootstrap (#692)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import AdminAuditEvent, Team, TeamMembership, User, UserSession
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "B" * 43
SESSION_TTL_SEC = 900
AUDIT_ACTION = "auth.staging_admin_browser_session.create"


@pytest.fixture
async def staging_admin_session_app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, UUID, str]]:
    for key, value in {
        "LOOM_ENV": "staging",
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)

    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    async_engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        async_engine,
        expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )

    target_id = uuid4()
    target_username = f"browser-admin-{target_id.hex}"
    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    created_admin_team = False
    with session_local() as session:
        admin_team = (
            session.execute(
                select(Team).where(func.lower(Team.name) == "admin"),
            )
        ).scalar_one_or_none()
        if admin_team is None:
            admin_team = Team(name="admin")
            session.add(admin_team)
            session.flush()
            created_admin_team = True
        target = User(
            id=target_id,
            email=None,
            username=target_username,
            username_normalized=target_username,
            display_name="Staging browser smoke administrator",
            status="pending_setup",
            disabled_at=None,
            is_platform_admin=True,
            created_at=datetime.now(UTC),
        )
        session.add(target)
        session.flush()
        session.add(
            TeamMembership(
                team_id=admin_team.id,
                user_id=target.id,
                role="owner",
            ),
        )
        session.commit()
        admin_team_id = admin_team.id

    try:
        yield app, target_id, target_username
    finally:
        await async_engine.dispose()
        with session_local() as session:
            session.execute(
                delete(AdminAuditEvent).where(
                    AdminAuditEvent.action == AUDIT_ACTION,
                    AdminAuditEvent.target_id == str(target_id),
                ),
            )
            session.execute(
                delete(UserSession).where(UserSession.user_id == target_id),
            )
            session.execute(
                delete(TeamMembership).where(TeamMembership.user_id == target_id),
            )
            session.execute(delete(User).where(User.id == target_id))
            if created_admin_team:
                session.execute(delete(Team).where(Team.id == admin_team_id))
            session.commit()
        sync_engine.dispose()


def _headers(*, request_id: str = "staging-admin-browser-test") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
        "X-Loom-Admin-Actor": "staging-browser-smoke",
        "X-Request-ID": request_id,
    }


@pytest.mark.parametrize("target_status", ["active", "pending_setup"])
async def test_bootstrap_sets_fixed_secure_cookie_and_safe_audit(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
    postgres_url: str,
    target_status: str,
) -> None:
    app, target_id, target_username = staging_admin_session_app
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as connection:
        connection.execute(
            update(User).where(User.id == target_id).values(status=target_status),
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        response = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(),
            json={"username": target_username},
        )

        assert response.status_code == 204, response.text
        assert response.content == b""
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["pragma"] == "no-cache"
        cookies = response.headers.get_list("set-cookie")
        assert len(cookies) == 1
        cookie = cookies[0].lower()
        assert "loom_session=loom_session_staging_admin_" in cookie
        assert "httponly" in cookie
        assert "secure" in cookie
        assert "samesite=lax" in cookie
        assert f"max-age={SESSION_TTL_SEC}" in cookie

        me = await client.get("/api/v1/auth/me")
        assert me.status_code == 200, me.text
        body = me.json()
        assert body["user"]["id"] == str(target_id)
        assert body["user"]["username"] == target_username
        assert body["is_platform_admin"] is True
        assert body["current_team"]["role"] == "platform_admin"

        refresh = await client.post(
            "/api/v1/auth/refresh",
            headers={"X-Loom-CSRF": body["csrf_token"]},
        )
        assert refresh.status_code == 403
        assert refresh.json()["detail"] == ("staging admin browser sessions cannot be refreshed")

        refreshed_me = await client.get("/api/v1/auth/me")
        logout = await client.post(
            "/api/v1/auth/logout",
            headers={"X-Loom-CSRF": refreshed_me.json()["csrf_token"]},
        )
        assert logout.status_code == 204
        assert (await client.get("/api/v1/auth/me")).status_code == 401

    with sessionmaker(sync_engine)() as session:
        audit = (
            session.execute(
                select(AdminAuditEvent).where(
                    AdminAuditEvent.action == AUDIT_ACTION,
                    AdminAuditEvent.target_id == str(target_id),
                ),
            )
        ).scalar_one()
        assert audit.actor == "staging-browser-smoke"
        assert audit.request_id == "staging-admin-browser-test"
        assert audit.event_metadata == {
            "auth_source": "singleton_admin_bearer",
            "target_status": target_status,
            "target_username": target_username,
            "ttl_seconds": SESSION_TTL_SEC,
        }
        stored_session = (
            session.execute(
                select(UserSession).where(UserSession.user_id == target_id),
            )
        ).scalar_one()
        assert (stored_session.expires_at - stored_session.issued_at).total_seconds() == (
            SESSION_TTL_SEC
        )
        assert stored_session.revoked_at is not None
    sync_engine.dispose()


@pytest.mark.parametrize("runtime", ["development", "production", ""])
async def test_bootstrap_is_hidden_outside_staging_before_auth(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: str,
) -> None:
    app, _target_id, target_username = staging_admin_session_app
    monkeypatch.setenv("LOOM_ENV", runtime)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        response = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            json={"username": target_username},
        )
        malformed = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 404
    assert response.json() == {"detail": "not found"}
    assert "set-cookie" not in response.headers
    assert malformed.status_code == 404
    assert malformed.json() == {"detail": "not found"}


async def test_staging_cookie_is_rejected_if_runtime_changes(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _target_id, target_username = staging_admin_session_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        created = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(),
            json={"username": target_username},
        )
        assert created.status_code == 204
        assert (await client.get("/api/v1/auth/me")).status_code == 200

        monkeypatch.setenv("LOOM_ENV", "production")
        assert (await client.get("/api/v1/auth/me")).status_code == 401
        hidden = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(request_id="production-probe"),
            json={"username": target_username},
        )
        assert hidden.status_code == 404


@pytest.mark.parametrize(
    ("headers", "expected_detail"),
    [
        (
            {
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Request-ID": "missing-actor",
            },
            "X-Loom-Admin-Actor header is required",
        ),
        (
            {
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "staging-browser-smoke",
            },
            "X-Request-ID header is required",
        ),
        (
            {
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "staging-browser-smoke",
                "X-Request-ID": "Bearer loom_admin_secret-looking",
            },
            "X-Request-ID must not contain secret-looking material",
        ),
    ],
)
async def test_bootstrap_requires_secret_safe_audit_headers(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
    headers: dict[str, str],
    expected_detail: str,
) -> None:
    app, _target_id, target_username = staging_admin_session_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        response = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=headers,
            json={"username": target_username},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == expected_detail
    assert "set-cookie" not in response.headers


async def test_platform_admin_browser_session_cannot_bootstrap_another_session(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
) -> None:
    app, _target_id, target_username = staging_admin_session_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        created = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(),
            json={"username": target_username},
        )
        assert created.status_code == 204
        me = await client.get("/api/v1/auth/me")
        nested = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers={
                "X-Loom-CSRF": me.json()["csrf_token"],
                "X-Loom-Admin-Actor": "staging-browser-smoke",
                "X-Request-ID": "nested-browser-session",
            },
            json={"username": target_username},
        )

    assert nested.status_code == 403
    assert nested.json()["detail"] == "singleton admin bearer required"


async def test_bootstrap_rejects_non_contract_body_fields(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
) -> None:
    app, _target_id, target_username = staging_admin_session_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        response = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(request_id="extra-body-field"),
            json={"username": target_username, "token": "must-not-be-accepted"},
        )

    assert response.status_code == 422
    assert "set-cookie" not in response.headers


async def test_staging_cookie_loses_access_when_admin_membership_is_demoted(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
    postgres_url: str,
) -> None:
    app, target_id, target_username = staging_admin_session_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        created = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(request_id="demoted-membership"),
            json={"username": target_username},
        )
        assert created.status_code == 204
        assert (await client.get("/api/v1/auth/me")).status_code == 200

        sync_engine = create_engine(postgres_url)
        with sync_engine.begin() as connection:
            connection.execute(
                update(TeamMembership)
                .where(TeamMembership.user_id == target_id)
                .values(role="member"),
            )
        sync_engine.dispose()

        assert (await client.get("/api/v1/auth/me")).status_code == 401


async def test_session_and_audit_are_atomic_when_audit_write_fails(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, target_id, target_username = staging_admin_session_app

    async def fail_audit(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("audit unavailable")

    monkeypatch.setattr(
        "loom_service.routes.auth.write_admin_audit_event",
        fail_audit,
    )
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        response = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(request_id="audit-failure"),
            json={"username": target_username},
        )

    assert response.status_code == 500
    assert "set-cookie" not in response.headers
    sync_engine = create_engine(postgres_url)
    with sessionmaker(sync_engine)() as session:
        assert session.execute(
            select(UserSession).where(UserSession.user_id == target_id),
        ).scalar_one_or_none() is None
        assert session.execute(
            select(AdminAuditEvent).where(
                AdminAuditEvent.target_id == str(target_id),
                AdminAuditEvent.request_id == "audit-failure",
            ),
        ).scalar_one_or_none() is None
    sync_engine.dispose()


@pytest.mark.parametrize(
    ("changes", "drop_membership"),
    [
        ({"status": "disabled"}, False),
        ({"disabled_at": datetime.now(UTC)}, False),
        ({"is_platform_admin": False}, False),
        ({}, True),
    ],
)
async def test_bootstrap_never_repairs_ineligible_target(
    staging_admin_session_app: tuple[FastAPI, UUID, str],
    postgres_url: str,
    changes: dict[str, object],
    drop_membership: bool,
) -> None:
    app, target_id, target_username = staging_admin_session_app
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as connection:
        if changes:
            connection.execute(
                update(User).where(User.id == target_id).values(**changes),
            )
        if drop_membership:
            connection.execute(
                delete(TeamMembership).where(TeamMembership.user_id == target_id),
            )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://svc.example",
    ) as client:
        response = await client.post(
            "/api/v1/auth/staging-admin-browser-session",
            headers=_headers(),
            json={"username": target_username},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "eligible staging platform admin not found"
    assert "set-cookie" not in response.headers
    with sessionmaker(sync_engine)() as session:
        assert (
            session.execute(
                select(UserSession).where(UserSession.user_id == target_id),
            )
        ).scalar_one_or_none() is None
    sync_engine.dispose()
