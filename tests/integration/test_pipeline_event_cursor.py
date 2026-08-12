"""Polling cursor contracts for the public Pipeline event feed."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from loom.auth import AuthContext
from loom.pipeline.public_api import PipelineRunEventsResponseV1
from loom_service.dependencies import authed_session
from loom_service.routes import pipeline as pipeline_routes


class _Result:
    def __init__(self, values: object | list[object]) -> None:
        self.values = values if isinstance(values, list) else [values]

    def scalar_one_or_none(self) -> object | None:
        return self.values[0] if self.values else None

    def scalars(self) -> list[object]:
        return self.values


class _Session:
    def __init__(self, *results: _Result) -> None:
        self._results = list(results)
        self.statements: list[object] = []

    async def execute(self, statement: object) -> _Result:
        self.statements.append(statement)
        return self._results.pop(0)


def _context(team_id: UUID) -> AuthContext:
    return AuthContext(
        token_hash=b"x" * 32,
        type="user",
        scopes=["read:own"],
        team_id=team_id,
        expires_at=None,
        user_id=uuid4(),
        role="viewer",
        auth_kind="session",
    )


def _app(session: _Session, *, team_id: UUID) -> FastAPI:
    app = FastAPI()
    app.include_router(pipeline_routes.router, prefix="/api/v1")

    async def _session_override() -> tuple[_Session, AuthContext]:
        return session, _context(team_id)

    app.dependency_overrides[authed_session] = _session_override
    return app


def _run(*, state: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), state=state)


def _event(seq: int, *, payload: dict[str, object] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        seq=seq,
        stage_run_id=None,
        execution_attempt_id=None,
        event_type="stage_state_changed",
        payload_json=payload or {"state": "running"},
        created_at=datetime(2026, 8, 12, 12, seq, tzinfo=UTC),
    )


async def test_event_poll_advances_from_last_returned_sequence() -> None:
    team_id = uuid4()
    run = _run(state="running")
    session = _Session(_Result(run), _Result([_event(5), _event(6)]))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, team_id=team_id)),
        base_url="http://svc",
    ) as client:
        response = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/events",
            params={"after_seq": 4, "limit": 2},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert [event["seq"] for event in body["events"]] == [5, 6]
    assert body["next_after_seq"] == 6
    assert body["terminal"] is False
    assert body["retry_after_ms"] == 1000
    PipelineRunEventsResponseV1.model_validate_json(response.content)

    event_statement = session.statements[1]
    assert "pipeline_events.seq >" in str(event_statement)
    params = event_statement.compile().params.values()
    assert run.id in params
    assert 4 in params
    assert 2 in params


async def test_empty_terminal_poll_preserves_cursor_and_stops_retrying() -> None:
    team_id = uuid4()
    run = _run(state="finished")
    session = _Session(_Result(run), _Result([]))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, team_id=team_id)),
        base_url="http://svc",
    ) as client:
        response = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/events",
            params={"after_seq": 9},
        )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "events": [],
        "next_after_seq": 9,
        "terminal": True,
        "retry_after_ms": None,
    }
    PipelineRunEventsResponseV1.model_validate_json(response.content)


async def test_repeating_a_poll_is_immutable_and_safe() -> None:
    team_id = uuid4()
    run = _run(state="running")
    event = _event(3, payload={"state": "queued", "attempt": 1})
    session = _Session(
        _Result(run),
        _Result([event]),
        _Result(run),
        _Result([event]),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, team_id=team_id)),
        base_url="http://svc",
    ) as client:
        first = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/events",
            params={"after_seq": 2},
        )
        repeated = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/events",
            params={"after_seq": 2},
        )

    assert first.status_code == repeated.status_code == 200
    assert first.json() == repeated.json()
    assert first.json()["next_after_seq"] == 3


@pytest.mark.parametrize(
    ("params", "invalid_field"),
    [
        ({"after_seq": -1}, "after_seq"),
        ({"limit": 0}, "limit"),
        ({"limit": 501}, "limit"),
    ],
)
async def test_event_cursor_bounds_are_rejected_before_query(
    params: dict[str, int],
    invalid_field: str,
) -> None:
    session = _Session()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, team_id=uuid4())),
        base_url="http://svc",
    ) as client:
        response = await client.get(
            f"/api/v1/pipeline-runs/{uuid4()}/events",
            params=params,
        )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][-1] == invalid_field
    assert session.statements == []


def test_event_response_contract_rejects_sequence_gaps() -> None:
    body = {
        "events": [
            {
                "seq": event.seq,
                "stage_run_id": None,
                "execution_attempt_id": None,
                "event_type": event.event_type,
                "payload": event.payload_json,
                "created_at": event.created_at,
            }
            for event in (_event(7), _event(9))
        ],
        "next_after_seq": 9,
        "terminal": False,
        "retry_after_ms": 1000,
    }

    with pytest.raises(ValidationError, match="gap-free"):
        PipelineRunEventsResponseV1.model_validate(body)
