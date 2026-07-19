"""#5 Slice 3a — `POST /trials/{trial_id}/events` route contract.

Workers POST batches of typed trajectory events; CP appends to
`trial_events`. UNIQUE (trial_id, seq) double-duties as the
idempotency key (retries return inserted=N reflecting just the
newly-landed rows). Worker fence: trial.worker_id must match.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    DataLifecycleAuthority,
    DataLifecycleGcItem,
    DataLifecycleGcRun,
    DataLifecycleObject,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    TrialEvent,
    Worker,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def seed(postgres_url: str) -> Iterator[tuple[UUID, UUID, UUID, str, str]]:
    """Same shape as the state_seed fixture in test_state_patch_fenced,
    but tokens carry the `worker:index` scope (what append_events
    requires) instead of `worker:report` (what state PATCH requires).
    """
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_a = uuid4()
    worker_b = uuid4()
    trial_id = uuid4()
    raw_a = f"wa_{uuid4().hex}"
    raw_b = f"wb_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        for raw in (raw_a, raw_b):
            s.execute(insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="worker", scopes=["worker:index"], team_id=None,
                issued_at=datetime.now(UTC), expires_at=None,
            ))
        for wid in (worker_a, worker_b):
            s.execute(insert(Worker).values(
                id=wid, hostname="h", version="v", capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC), status="active",
            ))
        s.execute(insert(Task).values(
            id="trial-events-task", checksum="0" * 64, config={},
        ))
        s.execute(insert(Trial).values(
            id=trial_id, team_id=team_id, task_id="trial-events-task",
            config={}, requires_caps={}, state="running",
            worker_id=worker_a,
        ))
        s.commit()
    try:
        yield trial_id, worker_a, worker_b, raw_a, raw_b
    finally:
        with session_factory() as s:
            s.execute(delete(TrialEvent))
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(DataLifecycleGcItem))
            s.execute(delete(DataLifecycleGcRun))
            s.execute(delete(DataLifecycleObject))
            s.execute(delete(DataLifecycleAuthority))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    seed: tuple[UUID, UUID, UUID, str, str],
) -> Any:
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def _event(
    seq: int, kind: str = "trial_start",
    source: str = "worker", payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "seq": seq,
        "kind": kind,
        "source": source,
        "schema_version": 1,
        "payload": payload or {"seq": seq, "kind": kind},
    }


def _count_rows(postgres_url: str, trial_id: UUID) -> int:
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as conn:
            return conn.execute(
                text("SELECT count(*) FROM trial_events WHERE trial_id=:t"),
                {"t": str(trial_id)},
            ).scalar_one()
    finally:
        engine.dispose()


def test_append_events_happy_path_inserts_all(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str], postgres_url: str,
) -> None:
    """A clean batch of 3 events from the trial's current owner inserts
    all 3 and reports inserted=3 / deduped=0."""
    trial_id, worker_a, _wb, raw_a, _rb = seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "events": [_event(0), _event(1), _event(2)],
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["submitted"] == 3
    assert body["inserted"] == 3
    assert body["deduped"] == 0
    assert _count_rows(postgres_url, trial_id) == 3


def test_append_events_idempotent_on_seq_retry(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str], postgres_url: str,
) -> None:
    """A retried batch (worker re-sends after a partial ack) gets
    inserted=0 for the dupes — UNIQUE (trial_id, seq) is the
    idempotency key, ON CONFLICT DO NOTHING absorbs the retry."""
    trial_id, worker_a, _wb, raw_a, _rb = seed
    with TestClient(app) as client:
        first = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "events": [_event(0), _event(1)],
            },
        )
        assert first.status_code == 200
        retry = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "events": [_event(0), _event(1), _event(2)],
            },
        )
    assert retry.status_code == 200
    body = retry.json()
    assert body["submitted"] == 3
    assert body["inserted"] == 1
    assert body["deduped"] == 2
    assert _count_rows(postgres_url, trial_id) == 3


def test_append_events_wrong_worker_returns_409(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str], postgres_url: str,
) -> None:
    """worker_b posts against trial owned by worker_a → 409, no rows
    written. Mirrors the trajectory_index fence pattern; reclaim that
    nulled the owner mid-stream surfaces here so workers stop writing
    to stale trials."""
    trial_id, _wa, worker_b, _ra, raw_b = seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_b}"},
            json={
                "worker_id": str(worker_b),
                "events": [_event(0)],
            },
        )
    assert r.status_code == 409
    assert "claim" in r.json()["detail"].lower()
    assert _count_rows(postgres_url, trial_id) == 0


def test_append_events_unknown_trial_returns_404(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str],
) -> None:
    _trial_id, worker_a, _wb, raw_a, _rb = seed
    nonexistent = uuid4()
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{nonexistent}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "events": [_event(0)],
            },
        )
    assert r.status_code == 404


def test_append_events_missing_worker_id_returns_400(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str],
) -> None:
    trial_id, _wa, _wb, raw_a, _rb = seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"events": [_event(0)]},
        )
    assert r.status_code == 400
    assert "worker_id" in r.json()["detail"]


def test_append_events_empty_batch_returns_400(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str],
) -> None:
    trial_id, worker_a, _wb, raw_a, _rb = seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "events": []},
        )
    assert r.status_code == 400
    assert "non-empty" in r.json()["detail"]


def test_append_events_batch_too_large_returns_413(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str],
) -> None:
    trial_id, worker_a, _wb, raw_a, _rb = seed
    huge = [_event(i) for i in range(501)]
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "events": huge},
        )
    assert r.status_code == 413


def test_append_events_per_event_payload_too_large_returns_413(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str],
) -> None:
    trial_id, worker_a, _wb, raw_a, _rb = seed
    big_payload = {"chunk": "x" * (300 * 1024)}  # > 256 KiB
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "events": [_event(0, payload=big_payload)],
            },
        )
    assert r.status_code == 413


def test_append_events_wrong_scope_returns_401(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    seed: tuple[UUID, UUID, UUID, str, str],
) -> None:
    """Tokens without `worker:index` scope are rejected — the route
    enforces the same scope the existing trajectory_index PATCH does."""
    trial_id, worker_a, _wb, _ra, _rb = seed
    raw_no_scope = f"ns_{uuid4().hex}"
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_no_scope.encode()).digest(),
            type="worker", scopes=["worker:report"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    engine.dispose()
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    app = create_app(ControlPlaneSettings(_env_file=None))
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_no_scope}"},
            json={"worker_id": str(worker_a), "events": [_event(0)]},
        )
    assert r.status_code == 401


def test_append_events_persists_kind_source_payload(
    app: Any, seed: tuple[UUID, UUID, UUID, str, str], postgres_url: str,
) -> None:
    """Round-trip: every per-event field lands on its column. Pins the
    on-wire ↔ schema mapping so future readers (Slice 3c) can rely
    on it."""
    trial_id, worker_a, _wb, raw_a, _rb = seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "events": [{
                    "seq": 42,
                    "kind": "llm_call",
                    "source": "worker",
                    "schema_version": 1,
                    "payload": {
                        "kind": "llm_call",
                        "seq": 42,
                        "model": "gpt-4o",
                        "input_tokens": 100,
                    },
                }],
            },
        )
    assert r.status_code == 200
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                select(
                    TrialEvent.seq,
                    TrialEvent.kind,
                    TrialEvent.source,
                    TrialEvent.schema_version,
                    TrialEvent.payload,
                    DataLifecycleAuthority.data_class,
                    DataLifecycleAuthority.owner_kind,
                    DataLifecycleAuthority.owner_id,
                    DataLifecycleAuthority.team_id,
                )
                .join(
                    DataLifecycleAuthority,
                    DataLifecycleAuthority.id == TrialEvent.lifecycle_authority_id,
                )
                .where(TrialEvent.trial_id == trial_id),
            ).one()
    finally:
        engine.dispose()
    assert row.seq == 42
    assert row.kind == "llm_call"
    assert row.source == "worker"
    assert row.schema_version == 1
    assert row.payload["model"] == "gpt-4o"
    assert row.payload["input_tokens"] == 100
    assert row.data_class == "event"
    assert row.owner_kind == "trial"
    assert row.owner_id == str(trial_id)
    assert row.team_id is not None
