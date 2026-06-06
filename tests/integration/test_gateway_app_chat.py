"""POST /v1/chat/completions with LiteLLM stubbed."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import RateCard, Team, Token
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings


@pytest.fixture
def seed_data(postgres_url: str) -> tuple[UUID, str]:
    # postgres_url uses postgresql+psycopg://; create_engine handles sync over psycopg 3.
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_token.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=datetime.now(UTC),
            expires_at=None,
        ))
        s.execute(insert(RateCard).values(
            id="card-1", captured_at=datetime.now(UTC),
            table={
                "id": "card-1",
                "entries": [{
                    "provider": "anthropic",
                    "model": "claude-opus-4-7",
                    "input_per_mtok": 3.0,
                    "output_per_mtok": 15.0,
                    "cache_read_per_mtok": 0.3,
                    "cache_write_per_mtok": 3.75,
                }],
            },
        ))
        s.commit()
    yield team_id, raw_token
    # Cleanup so tests are isolated.
    with session_factory() as s:
        from sqlalchemy import delete
        s.execute(delete(Token))
        s.execute(delete(Team))
        s.execute(delete(RateCard))
        s.commit()
    engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    seed_data: tuple[UUID, str],
):
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "stub")
    settings = GatewaySettings(_env_file=None)
    a = create_app(settings)

    async def stub(**kwargs: Any) -> dict[str, Any]:
        return {
            "id": "stub",
            "model": kwargs.get("model"),
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "stubbed"},
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
            },
        }
    monkeypatch.setattr("loom_llm_gateway.litellm_wrapper.acompletion", stub)
    return a


def test_chat_returns_loom_event_payload(app, seed_data):  # type: ignore[no-untyped-def]
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["choices"][0]["message"]["content"] == "stubbed"
        assert "loom" in body
        assert body["loom"]["input_tokens"] == 10
        assert body["loom"]["output_tokens"] == 5
        assert body["loom"]["cost_usd"] > 0
        assert "rate_card_hash" in body["loom"]
        assert "gateway_request_id" in body["loom"]


def test_chat_rejects_missing_loom_block(app, seed_data):  # type: ignore[no-untyped-def]
    _, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert r.status_code == 400
        # detail is now a structured Pydantic error list — the loom field
        # missing shows up in one of the error locations.
        detail = r.json()["detail"]
        assert any("loom" in str(err.get("loc", [])).lower() for err in detail)


def test_chat_rejects_bad_token(app, seed_data):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer bogus"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(uuid4()),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 401


def test_chat_rejects_team_id_mismatch(app, seed_data):  # type: ignore[no-untyped-def]
    """Regression for Bug 1: a client-supplied loom.team_id that doesn't
    match the bearer token's team_id must be rejected. Otherwise team A
    can attribute spend to team B by lying in the body."""
    _, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(uuid4()),  # wrong team
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 403
        assert "team_id" in r.json()["detail"]


def test_chat_rejects_missing_model_with_400(app, seed_data):  # type: ignore[no-untyped-def]
    """Regression for Bug 3: missing required `model` field → 400, not 500."""
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 400


def test_chat_strips_reserved_body_kwargs(app, seed_data):  # type: ignore[no-untyped-def]
    """Regression for Bug 4: a body containing reserved kwargs (api_key,
    timeout) must not duplicate-shadow the route's explicit named args."""
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
                # Reserved keys that used to cause TypeError → 500.
                "api_key": "client-attempt-to-override",
                "timeout": 9999,
            },
        )
        assert r.status_code == 200, r.text


def test_chat_rejects_unsupported_provider(app, seed_data):  # type: ignore[no-untyped-def]
    """Regression for Bug 6: an unknown provider in model="X/Y" → 400 with
    a clear allowed-providers list, instead of silently passing api_key=None."""
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "custom-co/foo-bar",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 400
        assert "unsupported provider" in r.json()["detail"]


def test_chat_rejects_unknown_model_with_400(app, seed_data):  # type: ignore[no-untyped-def]
    """Spec: unknown rate-card lookup → 400 with structured detail."""
    _, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/gpt-99",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(seed_data[0]),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 400
        assert "no entry" in r.json()["detail"]
