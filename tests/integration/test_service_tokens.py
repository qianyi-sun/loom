"""/api/v1/tokens lifecycle (GET/POST/DELETE) for team callers (Plan 17 Task 4)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Team, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def svc_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID]]:
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
        engine, expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url), timeout=10.0,
    )

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team",
            scopes=["read:own", "submit"],
            team_id=team_id,
            issued_at=datetime.now(UTC),
            expires_at=None,
        ))
        s.commit()
    try:
        yield app, raw, team_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            from loom.db.schema import TeamQuota
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_list_own_tokens(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tokens", headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["type"] == "team"
    assert "submit" in items[0]["scopes"]
    assert items[0]["revoked_at"] is None


async def test_mint_and_revoke(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 30,
            },
        )
        assert post.status_code == 201, post.text
        new_token = post.json()["token"]
        new_prefix = post.json()["token_hash_prefix"]
        assert new_token.startswith("loom_team_")
        assert len(new_prefix) == 8

        listed = await ac.get(
            "/api/v1/tokens", headers={"Authorization": f"Bearer {raw}"},
        )
        assert any(
            it["token_hash_prefix"] == new_prefix
            for it in listed.json()["items"]
        )

        revoke = await ac.delete(
            f"/api/v1/tokens/{new_prefix}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        assert revoke.status_code == 204

        after = await ac.get(
            "/api/v1/tokens", headers={"Authorization": f"Bearer {raw}"},
        )
        revoked_items = [
            it for it in after.json()["items"]
            if it["token_hash_prefix"] == new_prefix
        ]
        assert revoked_items
        assert revoked_items[0]["revoked_at"] is not None


async def test_post_rejects_admin_scope_from_team_caller(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "team",
                "scopes": ["admin:tokens"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 403
    assert "admin:tokens" in r.json()["detail"]


async def test_post_rejects_admin_type_from_team_caller(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "admin",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 403
    assert "admin" in r.json()["detail"]


async def test_unauthenticated_401(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, _raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/tokens")
    assert r.status_code == 401


async def test_revoke_invalid_prefix_400(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.delete(
            "/api/v1/tokens/short",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_revoke_unknown_prefix_404(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.delete(
            "/api/v1/tokens/deadbeef",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_revoke_rejects_non_hex_prefix(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        # 8 chars but contains non-hex.
        r = await ac.delete(
            "/api/v1/tokens/xyzzyabc",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_post_rejects_unknown_scope(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Audit M2: an unrecognized scope is rejected at request time
    rather than silently stored."""
    app, raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "team",
                "scopes": ["bogus:typo"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 400
    assert "unrecognized scopes" in r.json()["detail"]


async def test_revoke_other_team_token_returns_404(
    svc_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Audit H1+M1: a team caller cannot revoke (or even probe) a
    token belonging to another team. The lookup is filtered by
    team_id BEFORE the prefix scan, so a colliding prefix gets 404
    (not 403, not silent success)."""
    app, raw, _team_id = svc_setup

    # Seed a token in a different team.
    from datetime import UTC, datetime
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"
    other_hash = hashlib.sha256(other_raw.encode()).digest()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(
            id=other_team, name=f"other-{other_team}",
        ))
        s.execute(insert(Token).values(
            token_hash=other_hash, type="team",
            scopes=["read:own"], team_id=other_team,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        other_prefix = other_hash.hex()[:8]
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://svc",
        ) as ac:
            r = await ac.delete(
                f"/api/v1/tokens/{other_prefix}",
                headers={"Authorization": f"Bearer {raw}"},
            )
        # The other team's token is invisible to this caller; 404
        # rather than 403 (which would have leaked existence).
        assert r.status_code == 404
        # And the other team's token is still unrevoked.
        with sl() as s:
            from sqlalchemy import select as sa_select
            still = s.execute(
                sa_select(Token).where(Token.token_hash == other_hash),
            ).scalar_one()
        assert still.revoked_at is None
    finally:
        with sl() as s:
            s.execute(delete(Token).where(Token.token_hash == other_hash))
            from loom.db.schema import TeamQuota
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == other_team))
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        sync_engine.dispose()
