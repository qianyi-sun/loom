"""GET /trials/{trial_id}/llm-calls (Plan 9 amendment A9.2)."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import LlmCall, Task, Team, TeamQuota, Token, Trial
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def llm_calls_seed(postgres_url: str) -> Iterator[dict]:
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    team_id = uuid4()
    trial_id = uuid4()
    raw = f"w_{uuid4().hex}"
    now = datetime.now(UTC)
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:report", "read:own"],
            team_id=None,
            issued_at=now, expires_at=None,
        ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=trial_id, team_id=team_id, task_id="t",
            config={}, requires_caps={}, state="running",
        ))
        # Two llm_calls rows for this trial
        s.execute(insert(LlmCall).values(
            team_id=team_id, trial_id=trial_id, step_id="main",
            dialect="anthropic", model="claude-opus-4-7",
            input_tokens=100, output_tokens=50,
            provider_extras={"cache_read_input_tokens": 20},
            cost_usd=Decimal("0.001"), rate_card_hash="abc",
        ))
        s.execute(insert(LlmCall).values(
            team_id=team_id, trial_id=trial_id, step_id="main",
            dialect="anthropic", model="claude-opus-4-7",
            input_tokens=80, output_tokens=40,
            provider_extras={},
            cost_usd=Decimal("0.0008"), rate_card_hash="abc",
        ))
        # One row for a DIFFERENT trial — must not appear in results
        other_trial = uuid4()
        s.execute(insert(Trial).values(
            id=other_trial, team_id=team_id, task_id="t",
            config={}, requires_caps={}, state="running",
        ))
        s.execute(insert(LlmCall).values(
            team_id=team_id, trial_id=other_trial, step_id="main",
            dialect="openai_chat", model="gpt-5",
            input_tokens=10, output_tokens=5,
            provider_extras={},
            cost_usd=Decimal("0.00005"), rate_card_hash="abc",
        ))
        s.commit()
    try:
        yield {"trial_id": trial_id, "token": raw, "team_id": team_id}
    finally:
        with session_local() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, llm_calls_seed: dict):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_llm_calls_returns_only_this_trials_rows(app, llm_calls_seed):  # type: ignore[no-untyped-def]
    trial_id = llm_calls_seed["trial_id"]
    token = llm_calls_seed["token"]
    with TestClient(app) as client:
        r = client.get(
            f"/trials/{trial_id}/llm-calls",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        items = r.json()["items"]
        # 2 rows for this trial — the other_trial's row is filtered out.
        assert len(items) == 2
        for item in items:
            assert item["trial_id"] == str(trial_id)
        assert items[0]["model"] == "claude-opus-4-7"
        assert items[0]["input_tokens"] == 100
        assert items[1]["input_tokens"] == 80


def test_llm_calls_404_for_unknown_trial(app, llm_calls_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.get(
            f"/trials/{uuid4()}/llm-calls",
            headers={"Authorization": f"Bearer {llm_calls_seed['token']}"},
        )
        assert r.status_code == 404


def test_llm_calls_rejects_unauthenticated(app, llm_calls_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.get(f"/trials/{llm_calls_seed['trial_id']}/llm-calls")
        assert r.status_code == 401
