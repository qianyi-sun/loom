from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    AccountActionToken,
    AdminAuditEvent,
    PasswordResetRequest,
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    User,
    UserRegistrationRequest,
    UserSession,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password

ADMIN_PASSWORD = "admin-passphrase-1"
ADA_PASSWORD = "ada-passphrase-1"
ADA_NEW_PASSWORD = "ada-new-passphrase-1"


def _ensure_admin_seed(session) -> UUID:  # type: ignore[no-untyped-def]
    admin_team = session.execute(
        select(Team).where(text("lower(name) = 'admin'")),
    ).scalar_one_or_none()
    if admin_team is None:
        admin_team = Team(name="admin")
        session.add(admin_team)
        session.flush()
    if session.get(TeamQuota, admin_team.id) is None:
        session.add(TeamQuota(team_id=admin_team.id))

    for username in ("Qianyi", "Hongjian"):
        normalized = username.lower()
        user = session.execute(
            select(User).where(User.username_normalized == normalized),
        ).scalar_one_or_none()
        if user is None:
            user = User(
                id=uuid4(),
                email=None,
                username=username,
                username_normalized=normalized,
                display_name=username,
                is_platform_admin=True,
                created_at=datetime.now(UTC),
                status="pending_setup",
            )
            session.add(user)
            session.flush()
        else:
            user.username = username
            user.display_name = username
            user.is_platform_admin = True
        membership = session.execute(
            select(TeamMembership).where(
                TeamMembership.team_id == admin_team.id,
                TeamMembership.user_id == user.id,
            ),
        ).scalar_one_or_none()
        if membership is None:
            session.add(
                TeamMembership(team_id=admin_team.id, user_id=user.id, role="owner"),
            )
        else:
            membership.role = "owner"
    return admin_team.id


def _extract_token(link: str) -> str:
    parsed = urlparse(link)
    values = parse_qs(parsed.query).get("token")
    assert values, link
    return values[0]


@pytest.fixture
async def username_auth_app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, UUID, str]]:
    for key, value in {
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
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as session:
        admin_team_id = _ensure_admin_seed(session)
        research_team_name = f"Research-{uuid4().hex[:8]}"
        research_team = Team(name=research_team_name)
        session.add(research_team)
        session.flush()
        session.add(TeamQuota(team_id=research_team.id))
        qianyi = session.execute(
            select(User).where(User.username_normalized == "qianyi"),
        ).scalar_one()
        qianyi.password_hash = hash_password(ADMIN_PASSWORD)
        qianyi.password_set_at = datetime.now(UTC)
        qianyi.status = "active"
        qianyi.disabled_at = None
        qianyi.is_platform_admin = True
        session.merge(
            TeamMembership(team_id=admin_team_id, user_id=qianyi.id, role="owner"),
        )
        session.commit()
        research_team_id = research_team.id

    try:
        yield app, research_team_id, research_team_name
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as session:
            for table in (
                AccountActionToken,
                PasswordResetRequest,
                UserRegistrationRequest,
                UserSession,
                Token,
                AdminAuditEvent,
                TeamMembership,
            ):
                session.execute(delete(table))
            session.execute(
                delete(User).where(User.username_normalized.not_in(["qianyi", "hongjian"])),
            )
            session.execute(delete(TeamQuota))
            session.execute(delete(Team).where(text("lower(name) <> 'admin'")))
            session.execute(
                update(User)
                .where(User.username_normalized.in_(["qianyi", "hongjian"]))
                .values(
                    password_hash=None,
                    password_set_at=None,
                    status="pending_setup",
                    disabled_at=None,
                ),
            )
            _ensure_admin_seed(session)
            session.commit()
        sync_engine.dispose()


async def _login(
    client: httpx.AsyncClient,
    *,
    username: str,
    password: str,
) -> dict[str, object]:
    response = await client.post(
        "/api/v1/auth/login",
        json={"username": username, "password": password},
    )
    assert response.status_code == 200, response.text
    return response.json()


async def _provision_ada(
    client: httpx.AsyncClient,
    *,
    team_id: UUID,
) -> tuple[str, dict[str, object]]:
    admin_me = await _login(client, username="Qianyi", password=ADMIN_PASSWORD)
    csrf = str(admin_me["csrf_token"])
    created = await client.post(
        "/api/v1/auth/registration-requests",
        json={"username": "Ada", "team_id": str(team_id)},
    )
    assert created.status_code == 202, created.text
    request_id = created.json()["id"]
    approved = await client.post(
        f"/api/v1/admin/registration-requests/{request_id}/approve",
        headers={"X-Loom-CSRF": csrf},
        json={"role": "member"},
    )
    assert approved.status_code == 200, approved.text
    setup_token = _extract_token(approved.json()["setup_link"])
    completed = await client.post(
        "/api/v1/auth/setup/complete",
        headers={"X-Loom-CSRF": csrf},
        json={
            "token": setup_token,
            "password": ADA_PASSWORD,
            "confirm_password": ADA_PASSWORD,
        },
    )
    assert completed.status_code == 200, completed.text
    await client.post("/api/v1/auth/logout", headers={"X-Loom-CSRF": csrf})
    return request_id, completed.json()


async def test_user_registers_sets_password_and_logs_in(
    username_auth_app: tuple[FastAPI, UUID, str],
) -> None:
    app, team_id, team_name = username_auth_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        admin_me = await _login(client, username="Qianyi", password=ADMIN_PASSWORD)
        csrf = str(admin_me["csrf_token"])

        teams = await client.get("/api/v1/auth/public-teams")
        assert teams.status_code == 200, teams.text
        team_items = teams.json()["items"]
        assert {"id": str(team_id), "name": team_name} in team_items
        assert all(item["name"] != "admin" for item in team_items)

        created = await client.post(
            "/api/v1/auth/registration-requests",
            json={"username": "Ada", "team_id": str(team_id)},
        )
        duplicate = await client.post(
            "/api/v1/auth/registration-requests",
            json={"username": "ada", "team_id": str(team_id)},
        )
        assert created.status_code == 202, created.text
        assert duplicate.status_code == 409, duplicate.text
        body = created.json()
        assert body["username"] == "Ada"
        assert body["status"] == "pending"
        assert "email" not in body

        listed = await client.get("/api/v1/admin/registration-requests")
        assert listed.status_code == 200, listed.text
        assert [item["id"] for item in listed.json()["items"]] == [body["id"]]

        approved = await client.post(
            f"/api/v1/admin/registration-requests/{body['id']}/approve",
            headers={"X-Loom-CSRF": csrf},
            json={"role": "member"},
        )
        assert approved.status_code == 200, approved.text
        approved_body = approved.json()
        assert approved_body["registration"]["status"] == "approved"
        assert approved_body["registration"]["reviewed_by_actor"] == "user:Qianyi"
        assert approved_body["setup_link"].startswith("http://svc/auth/setup?")
        assert "invite" not in approved_body

        setup_token = _extract_token(approved_body["setup_link"])
        lookup = await client.get("/api/v1/auth/setup/lookup", params={"token": setup_token})
        assert lookup.status_code == 200, lookup.text
        assert lookup.json()["username"] == "Ada"
        assert lookup.json()["team"]["name"] == team_name

        setup = await client.post(
            "/api/v1/auth/setup/complete",
            headers={"X-Loom-CSRF": csrf},
            json={
                "token": setup_token,
                "password": ADA_PASSWORD,
                "confirm_password": ADA_PASSWORD,
            },
        )
        assert setup.status_code == 200, setup.text

    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ada_client:
        me = await _login(ada_client, username="ada", password=ADA_PASSWORD)
        assert me["user"]["username"] == "Ada"
        assert me["user"]["email"] is None
        assert me["current_team"]["name"] == team_name
        assert me["current_team"]["role"] == "member"


async def test_password_reset_requires_admin_link_and_revokes_sessions_and_tokens(
    username_auth_app: tuple[FastAPI, UUID, str],
    postgres_url: str,
) -> None:
    app, team_id, _team_name = username_auth_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as setup_client:
        await _provision_ada(setup_client, team_id=team_id)

    old_session = httpx.AsyncClient(transport=transport, base_url="http://svc")
    admin_client = httpx.AsyncClient(transport=transport, base_url="http://svc")
    try:
        await _login(old_session, username="Ada", password=ADA_PASSWORD)

        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as session:
            ada = session.execute(
                select(User).where(User.username_normalized == "ada"),
            ).scalar_one()
            ada_id = ada.id
            api_token = Token(
                token_hash=b"x" * 32,
                name="Ada CLI",
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                created_by_user_id=ada.id,
                issued_at=datetime.now(UTC),
                expires_at=datetime.now(UTC) + timedelta(days=30),
            )
            session.add(api_token)
            session.commit()

        unknown = await old_session.post(
            "/api/v1/auth/password-reset-requests",
            json={"username": "unknown-user"},
        )
        requested = await old_session.post(
            "/api/v1/auth/password-reset-requests",
            json={"username": "Ada"},
        )
        assert unknown.status_code == 202, unknown.text
        assert requested.status_code == 202, requested.text
        assert unknown.json() == requested.json() == {"status": "pending"}

        admin_me = await _login(admin_client, username="Qianyi", password=ADMIN_PASSWORD)
        csrf = str(admin_me["csrf_token"])
        listed = await admin_client.get("/api/v1/admin/password-reset-requests")
        assert listed.status_code == 200, listed.text
        request_id = listed.json()["items"][0]["id"]
        approved = await admin_client.post(
            f"/api/v1/admin/password-reset-requests/{request_id}/approve",
            headers={"X-Loom-CSRF": csrf},
        )
        assert approved.status_code == 200, approved.text
        reset_token = _extract_token(approved.json()["reset_link"])

        completed = await admin_client.post(
            "/api/v1/auth/reset/complete",
            headers={"X-Loom-CSRF": csrf},
            json={
                "token": reset_token,
                "password": ADA_NEW_PASSWORD,
                "confirm_password": ADA_NEW_PASSWORD,
            },
        )
        assert completed.status_code == 200, completed.text

        old_me_after_reset = await old_session.get("/api/v1/auth/me")
        assert old_me_after_reset.status_code == 401, old_me_after_reset.text

        old_password = await old_session.post(
            "/api/v1/auth/login",
            json={"username": "Ada", "password": ADA_PASSWORD},
        )
        assert old_password.status_code == 401, old_password.text

        new_me = await _login(old_session, username="Ada", password=ADA_NEW_PASSWORD)
        assert new_me["user"]["username"] == "Ada"

        with sl() as session:
            revoked_at = session.execute(
                select(Token.revoked_at).where(Token.created_by_user_id == ada_id),
            ).scalar_one()
        sync_engine.dispose()
        assert revoked_at is not None
    finally:
        await old_session.aclose()
        await admin_client.aclose()


async def test_member_cannot_use_admin_account_routes(
    username_auth_app: tuple[FastAPI, UUID, str],
) -> None:
    app, team_id, _team_name = username_auth_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        await _provision_ada(client, team_id=team_id)
        ada_me = await _login(client, username="Ada", password=ADA_PASSWORD)
        csrf = str(ada_me["csrf_token"])
        pending = await client.post(
            "/api/v1/auth/registration-requests",
            json={"username": "Grace", "team_id": str(team_id)},
        )
        assert pending.status_code == 202, pending.text

        create_team = await client.post(
            "/api/v1/admin/teams",
            headers={"X-Loom-CSRF": csrf},
            json={"name": "should-not-work"},
        )
        approve = await client.post(
            f"/api/v1/admin/registration-requests/{pending.json()['id']}/approve",
            headers={"X-Loom-CSRF": csrf},
            json={"role": "member"},
        )

    assert create_team.status_code == 403, create_team.text
    assert approve.status_code == 403, approve.text
