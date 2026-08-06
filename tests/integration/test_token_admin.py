import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token, WorkerPoolAutoscalerPolicy
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "C" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{RAW_ADMIN_TOKEN}\"\n"
        "created_at = \"2026-06-16T00:00:00Z\"\n"
        "version = 1\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _set_cp_env(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    *,
    environment: str = "dev-token-test",
) -> None:
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
        "LOOM_ENV": environment,
    }.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def legacy_admin_seed(postgres_url: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    raw = f"loom_admin_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="admin", scopes=["admin:tokens"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield raw
    finally:
        with session_factory() as s:
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
):
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url)
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        session.execute(
            delete(WorkerPoolAutoscalerPolicy).where(
                WorkerPoolAutoscalerPolicy.environment == "dev-token-test",
            ),
        )
        session.add(
            WorkerPoolAutoscalerPolicy(
                environment="dev-token-test",
                pool_name="dev-token-test",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=2,
                actuator_config={"external_runner": True},
            ),
        )
        session.commit()
    try:
        yield create_app(ControlPlaneSettings(_env_file=None))
    finally:
        with sessionmaker(engine)() as session:
            session.execute(
                delete(WorkerPoolAutoscalerPolicy).where(
                    WorkerPoolAutoscalerPolicy.environment == "dev-token-test",
                ),
            )
            session.commit()
        engine.dispose()


def test_issue_worker_token(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )
        assert r.status_code == 201
        body = r.json()
        assert body["token"].startswith("loom_w_")
        assert "token_hash_prefix" in body


def test_issue_worker_token_rejected_while_dev_policy_is_draining(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        session.execute(
            update(WorkerPoolAutoscalerPolicy)
            .where(WorkerPoolAutoscalerPolicy.environment == "dev-token-test")
            .values(max_slots=0),
        )
        session.commit()
    engine.dispose()

    with TestClient(app) as client:
        response = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 30},
        )

    assert response.status_code == 409
    assert "active development capacity policy" in response.json()["detail"]


def test_revoke_all_worker_tokens(app, postgres_url: str) -> None:  # type: ignore[no-untyped-def]
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        for _ in range(2):
            issued = client.post(
                "/admin/worker-tokens",
                headers=headers,
                json={"expires_in_days": 30},
            )
            assert issued.status_code == 201
        revoked = client.delete("/admin/worker-tokens", headers=headers)
    assert revoked.status_code == 200
    assert revoked.json()["revoked"] >= 2

    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        active = session.execute(
            select(Token).where(Token.type == "worker", Token.revoked_at.is_(None)),
        ).scalars()
        assert list(active) == []
    engine.dispose()


def test_issue_batch_runner_token(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    with TestClient(app) as client:
        r = client.post(
            "/admin/batch-runner-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )

    assert r.status_code == 201
    body = r.json()
    assert body["token"].startswith("loom_br_")
    assert "token_hash_prefix" in body

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        row = s.execute(
            select(Token).where(
                Token.token_hash == hashlib.sha256(
                    body["token"].encode(),
                ).digest(),
            ),
        ).scalar_one()
    engine.dispose()
    assert row.type == "worker"
    assert row.team_id is None
    assert row.scopes == ["submit:batch"]


def test_issue_family_orchestrator_token(
    app,  # type: ignore[no-untyped-def]
    postgres_url: str,
) -> None:
    with TestClient(app) as client:
        r = client.post(
            "/admin/family-orchestrator-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["token"].startswith("loom_fo_")

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        row = s.execute(
            select(Token).where(
                Token.token_hash == hashlib.sha256(body["token"].encode()).digest(),
            ),
        ).scalar_one()
    engine.dispose()
    assert row.type == "family_orchestrator"
    assert row.team_id is None
    assert row.scopes == ["family:evolve"]


def test_issue_worker_token_accepts_singleton_admin_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
) -> None:
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url, environment="test-token-admin")
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    admin_app = create_app(ControlPlaneSettings(_env_file=None))

    with TestClient(admin_app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 90},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["token"].startswith("loom_w_")
    assert "token_hash_prefix" in body


def test_revoke_token(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        prefix = r.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert r2.status_code == 200


def test_revoke_token_accepts_singleton_admin_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
) -> None:
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url, environment="test-token-admin")
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    admin_app = create_app(ControlPlaneSettings(_env_file=None))

    with TestClient(admin_app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        assert r.status_code == 201, r.text
        prefix = r.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )

    assert r2.status_code == 200, r2.text


def test_issue_without_admin_scope_rejected(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": "Bearer wrong"},
            json={"expires_in_days": 1},
        )
        assert r.status_code == 403


@pytest.mark.parametrize("bad_prefix", [
    "abc",  # too short
    "ZZZZ",  # non-hex (uppercase, and also non-hex letters)
    "abcg",  # 'g' not hex
    "ab--",  # punctuation
    "abc!",  # punctuation
    "a" * 65,  # too long
])
def test_revoke_rejects_invalid_prefix(  # type: ignore[no-untyped-def]
    app, bad_prefix,
):
    """Bug 3 regression: revoke prefix must be 4-64 hex chars. Anything
    else — including the empty / wildcard / punctuation that would feed
    straight into a SQL LIKE — must be rejected with 400 BEFORE the
    UPDATE runs."""
    with TestClient(app) as client:
        r = client.delete(
            f"/admin/worker-tokens/{bad_prefix}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert r.status_code == 400, r.text


def test_revoke_doesnt_affect_unrelated_tokens(  # type: ignore[no-untyped-def]
    app,
):
    """Bug 3 regression: a precise prefix must only revoke matching tokens
    (not all of them). We issue two worker tokens, revoke one by its
    prefix, then confirm the admin token is still usable."""
    with TestClient(app) as client:
        r1 = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        prefix_1 = r1.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix_1}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert r2.status_code == 200
        # admin token still works
        r3 = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"expires_in_days": 1},
        )
        assert r3.status_code == 201


def test_legacy_db_admin_token_rejected(app, legacy_admin_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {legacy_admin_seed}"},
            json={"expires_in_days": 1},
        )

    assert r.status_code == 403
