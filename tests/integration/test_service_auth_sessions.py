"""Browser user sessions and CSRF protection (#326)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    Task,
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    Trial,
    User,
    UserSession,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "A" * 43


@pytest.fixture
async def auth_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, UUID, UUID, UUID, UUID]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN": "1",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine, expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )
    captured_cp_requests: list[dict[str, str]] = []

    def cp_handler(req: httpx.Request) -> httpx.Response:
        captured_cp_requests.append({
            "method": req.method,
            "url": str(req.url),
            "auth": req.headers.get("authorization") or "",
        })
        if req.url.path == "/trials" and req.method == "POST":
            return httpx.Response(
                201,
                json={
                    "trial_id": "00000000-0000-0000-0000-000000000001",
                    "state": "queued",
                },
            )
        return httpx.Response(404)

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(cp_handler), base_url="http://cp",
    )
    app.state.session_auth_test_cp_requests = captured_cp_requests

    team_a, team_b, team_c, team_d = uuid4(), uuid4(), uuid4(), uuid4()
    user_id = uuid4()
    admin_user_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name=f"Alpha-{team_a}"))
        s.execute(insert(Team).values(id=team_b, name=f"Beta-{team_b}"))
        s.execute(insert(Team).values(id=team_c, name=f"Gamma-{team_c}"))
        s.execute(insert(Team).values(id=team_d, name=f"Delta-{team_d}"))
        s.execute(insert(User).values(
            id=user_id,
            email="owner@example.com",
            username="owner",
            username_normalized="owner",
            display_name="Owner Example",
            is_platform_admin=False,
            status="active",
            created_at=datetime.now(UTC),
        ))
        s.execute(insert(User).values(
            id=admin_user_id,
            email="admin@example.com",
            username="admin",
            username_normalized="admin",
            display_name="Admin Example",
            is_platform_admin=True,
            status="active",
            created_at=datetime.now(UTC),
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_a, user_id=user_id, role="viewer",
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_b, user_id=user_id, role="member",
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_c, user_id=user_id, role="owner",
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_d, user_id=admin_user_id, role="owner",
        ))
        s.execute(
            insert(Task).values(
                id="local/task-1",
                checksum="x" * 64,
                config={
                    "schema_version": "1",
                    "task": {"id": "local/task-1", "name": "local/task-1"},
                    "environment": {"os": "linux", "docker_image": "alpine"},
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                    "steps": [{"name": "main"}],
                },
                source="local",
            ),
        )
        s.commit()
    try:
        yield app, team_a, team_b, team_c, team_d
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Task))
            s.execute(delete(UserSession))
            from loom.db.schema import LoginChallenge
            s.execute(delete(LoginChallenge))
            s.execute(delete(Token))
            s.execute(delete(TeamMembership))
            s.execute(delete(User))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _login(
    ac: httpx.AsyncClient, email: str = "owner@example.com",
) -> tuple[dict[str, object], list[str]]:
    start = await ac.post("/api/v1/auth/login/start", json={"email": email})
    assert start.status_code == 200, start.text
    login_token = start.json().get("login_token")
    assert isinstance(login_token, str)
    complete = await ac.post(
        "/api/v1/auth/login/complete", json={"token": login_token},
    )
    assert complete.status_code == 200, complete.text
    return complete.json(), complete.headers.get_list("set-cookie")


async def test_login_me_and_cookie_flags(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, team_a, team_b, team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, set_cookie = await _login(ac)
        assert body["user"]["email"] == "owner@example.com"
        assert body["current_team"]["id"] == str(team_a)
        assert body["current_team"]["role"] == "viewer"
        assert {t["id"] for t in body["teams"]} == {
            str(team_a), str(team_b), str(team_c),
        }
        assert isinstance(body["csrf_token"], str)

        me = await ac.get("/api/v1/auth/me")
        assert me.status_code == 200
        assert me.json()["current_team"]["id"] == str(team_a)
        assert isinstance(me.json()["csrf_token"], str)

    cookie_headers = "\n".join(set_cookie)
    assert "loom_session=" in cookie_headers
    assert "HttpOnly" in cookie_headers
    assert "samesite=lax" in cookie_headers.lower()
    assert "loom_csrf=" not in cookie_headers
    cookie_names = {cookie.name for cookie in ac.cookies.jar}
    assert "loom_session" in cookie_names
    assert "loom_csrf" not in cookie_names


async def test_refresh_rotates_session_cookie_and_returns_csrf_token(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, _team_a, _team_b, _team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _set_cookie = await _login(ac)
        csrf = str(body["csrf_token"])
        before_session = ac.cookies.get("loom_session")
        assert before_session is not None

        refreshed = await ac.post(
            "/api/v1/auth/refresh",
            headers={"X-Loom-CSRF": csrf},
        )
        assert refreshed.status_code == 200, refreshed.text
        refresh_body = refreshed.json()
        assert isinstance(refresh_body["csrf_token"], str)
        assert refresh_body["csrf_token"] != csrf
        after_session = ac.cookies.get("loom_session")
        assert after_session is not None
        assert after_session != before_session

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        headers={"Cookie": f"loom_session={before_session}"},
    ) as old_ac:
        old_me = await old_ac.get("/api/v1/auth/me")
        assert old_me.status_code == 401

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        headers={"Cookie": f"loom_session={after_session}"},
    ) as new_ac:
        me = await new_ac.get("/api/v1/auth/me")
        assert me.status_code == 200, me.text


async def test_unknown_email_login_start_does_not_disclose_user_existence(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, _team_a, _team_b, _team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/auth/login/start", json={"email": "missing@example.com"},
        )
    assert r.status_code == 200
    assert r.json() == {"status": "sent"}


async def test_team_switch_requires_csrf_and_membership(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, _team_a, team_b, _team_c, team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac)
        csrf = str(body["csrf_token"])

        missing_csrf = await ac.post(
            "/api/v1/auth/team", json={"team_id": str(team_b)},
        )
        assert missing_csrf.status_code == 403

        switched = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_b)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert switched.status_code == 200, switched.text
        assert switched.json()["current_team"]["id"] == str(team_b)
        assert switched.json()["current_team"]["role"] == "member"
        csrf = str(switched.json()["csrf_token"])

        forbidden = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_d)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert forbidden.status_code == 403


async def test_logout_revokes_session(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, _team_a, _team_b, _team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac)
        csrf = str(body["csrf_token"])
        logout = await ac.post(
            "/api/v1/auth/logout", headers={"X-Loom-CSRF": csrf},
        )
        assert logout.status_code == 204, logout.text
        me = await ac.get("/api/v1/auth/me")
        assert me.status_code == 401


async def test_viewer_and_member_cannot_mint_token_but_owner_can(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, _team_a, team_b, team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac)
        csrf = str(body["csrf_token"])

        viewer_post = await ac.post(
            "/api/v1/tokens",
            json={
                "name": "viewer-token",
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
            headers={"X-Loom-CSRF": csrf},
        )
        assert viewer_post.status_code == 403

        switched = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_b)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert switched.status_code == 200, switched.text
        csrf = str(switched.json()["csrf_token"])
        member_post = await ac.post(
            "/api/v1/tokens",
            json={
                "name": "member-token",
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
            headers={"X-Loom-CSRF": csrf},
        )
        assert member_post.status_code == 403, member_post.text

        owner_switch = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_c)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert owner_switch.status_code == 200, owner_switch.text
        assert owner_switch.json()["current_team"]["role"] == "owner"
        csrf = str(owner_switch.json()["csrf_token"])
        owner_post = await ac.post(
            "/api/v1/tokens",
            json={
                "name": "owner-token",
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
            headers={"X-Loom-CSRF": csrf},
        )
        assert owner_post.status_code == 201, owner_post.text
        assert owner_post.json()["token"].startswith("loom_api_")


async def test_viewer_cannot_submit_trial_but_member_can(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, _team_a, team_b, _team_c, _team_d = auth_setup
    captured = app.state.session_auth_test_cp_requests
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac)
        csrf = str(body["csrf_token"])

        viewer_post = await ac.post(
            "/api/v1/trials",
            json={"task_id": "local/task-1", "config": {"agent": {"name": "fake"}}},
            headers={"X-Loom-CSRF": csrf},
        )
        assert viewer_post.status_code == 403
        assert captured == []

        switched = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_b)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert switched.status_code == 200, switched.text
        csrf = str(switched.json()["csrf_token"])
        member_post = await ac.post(
            "/api/v1/trials",
            json={"task_id": "local/task-1", "config": {"agent": {"name": "fake"}}},
            headers={"X-Loom-CSRF": csrf},
        )
        assert member_post.status_code == 201, member_post.text
        assert captured[-1]["method"] == "POST"
        assert captured[-1]["url"].endswith("/trials")
        assert captured[-1]["auth"] == ""


async def test_legacy_team_token_cannot_submit_trial_forwarder(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
    postgres_url: str,
) -> None:
    app, _team_a, team_b, _team_c, _team_d = auth_setup
    captured = app.state.session_auth_test_cp_requests
    legacy_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Token).values(
            token_hash=hashlib.sha256(legacy_raw.encode()).digest(),
            type="team",
            scopes=["read:own", "submit"],
            team_id=team_b,
            issued_at=datetime.now(UTC),
            expires_at=None,
        ))
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/trials",
            json={"task_id": "local/task-1", "config": {"agent": {"name": "fake"}}},
            headers={"Authorization": f"Bearer {legacy_raw}"},
        )

    assert r.status_code == 403
    assert "legacy team token" in r.json()["detail"]
    assert captured == []


async def test_session_team_reads_are_scoped_to_current_team(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, team_a, team_b, _team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac)
        csrf = str(body["csrf_token"])

        own = await ac.get(f"/api/v1/teams/{team_a}")
        assert own.status_code == 200, own.text
        cross = await ac.get(f"/api/v1/teams/{team_b}")
        assert cross.status_code == 403

        switched = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_b)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert switched.status_code == 200, switched.text
        csrf = str(switched.json()["csrf_token"])
        now_own = await ac.get(f"/api/v1/teams/{team_b}")
        assert now_own.status_code == 200, now_own.text
        old_team = await ac.get(f"/api/v1/teams/{team_a}")
        assert old_team.status_code == 403


async def test_platform_admin_user_can_read_any_team(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],
) -> None:
    app, team_a, _team_b, _team_c, team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac, "admin@example.com")
        assert body["is_platform_admin"] is True
        assert body["role"] == "platform_admin"
        read_a = await ac.get(f"/api/v1/teams/{team_a}")
        read_c = await ac.get(f"/api/v1/teams/{team_d}")
        assert read_a.status_code == 200, read_a.text
        assert read_c.status_code == 200, read_c.text
