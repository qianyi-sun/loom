"""HTTP contracts for the public Pipeline run service routes."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from loom.auth import AuthContext
from loom_service.dependencies import authed_session
from loom_service.routes import pipeline as pipeline_routes


class _Result:
    def __init__(self, values: object | list[object]) -> None:
        self.values = values if isinstance(values, list) else [values]

    def scalar_one_or_none(self) -> object | None:
        return self.values[0] if self.values else None

    def scalars(self) -> list[object]:
        return self.values

    def all(self) -> list[object]:
        return self.values


class _Session:
    def __init__(self, *results: _Result, get_result: object | None = None) -> None:
        self._results = list(results)
        self.get_result = get_result
        self.statements: list[object] = []
        self.commits = 0
        self.rollbacks = 0

    async def execute(self, statement: object) -> _Result:
        self.statements.append(statement)
        return self._results.pop(0)

    async def get(self, _model: object, _key: object) -> object | None:
        return self.get_result

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _context(
    *,
    team_id: UUID | None = None,
    user_id: UUID | None = None,
    role: str = "member",
) -> AuthContext:
    return AuthContext(
        token_hash=b"x" * 32,
        type="user",
        scopes=["read:own", "submit"],
        team_id=team_id or uuid4(),
        expires_at=None,
        user_id=user_id or uuid4(),
        role=role,
        auth_kind="session",
    )


def _app(session: _Session, context: AuthContext) -> FastAPI:
    app = FastAPI()
    app.include_router(pipeline_routes.router, prefix="/api/v1")

    async def _session_override() -> tuple[_Session, AuthContext]:
        return session, context

    app.dependency_overrides[authed_session] = _session_override
    app.state.pipeline_cursor_signing_key = b"cursor-signing-key" * 2
    return app


def _budget() -> dict[str, object]:
    return {
        "max_provider_cost_usd": "1.000000",
        "max_gpu_seconds": 60,
        "max_wall_seconds": 300,
        "max_artifact_bytes": 1024,
        "max_stage_runs": 8,
        "max_attempts_total": 16,
    }


def _submit_body() -> dict[str, object]:
    return {
        "budget": _budget(),
        "display_name": "public smoke run",
        "inputs": {},
        "parameters": {"seed": 7},
        "recipe": "smoke@1",
    }


def _run(*, run_id: UUID | None = None, created_at: datetime | None = None) -> SimpleNamespace:
    created_by_user_id = uuid4()
    return SimpleNamespace(
        id=run_id or uuid4(),
        team_id=uuid4(),
        created_by_user_id=created_by_user_id,
        display_name="public smoke run",
        recipe_name="smoke",
        recipe_version=1,
        recipe_digest="a" * 64,
        graph_spec_digest="b" * 64,
        graph_spec_json={"must": "remain private"},
        control_binding_snapshots_digest="c" * 64,
        parameters_digest="d" * 64,
        request_digest="e" * 64,
        state="running",
        result=None,
        result_reason=None,
        retry_of_pipeline_run_id=None,
        retry_from_stage_run_id=None,
        created_at=created_at or datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
        started_at=datetime(2026, 8, 12, 12, 1, tzinfo=UTC),
        finished_at=None,
        cancellation_requested_at=None,
    )


async def test_submit_returns_created_and_commits_parsed_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _Session()
    context = _context()
    run_id = uuid4()
    create_run = AsyncMock(
        return_value=(
            {
                "pipeline_run_id": str(run_id),
                "request_digest": "f" * 64,
                "idempotent_replay": False,
            },
            False,
        )
    )
    monkeypatch.setattr(pipeline_routes, "create_public_run", create_run)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, context)),
        base_url="http://svc",
    ) as client:
        response = await client.post(
            "/api/v1/pipeline-runs",
            headers={"Idempotency-Key": "submit-smoke-0001"},
            json=_submit_body(),
        )

    assert response.status_code == 201, response.text
    assert response.json()["pipeline_run_id"] == str(run_id)
    assert "Idempotent-Replay" not in response.headers
    assert session.commits == 1
    assert session.rollbacks == 0
    call = create_run.await_args.kwargs
    assert call["team_id"] == context.team_id
    assert call["user_id"] == context.user_id
    assert call["idempotency_key"] == "submit-smoke-0001"
    assert call["request"].model_dump(mode="json") == _submit_body() | {"judge_profile_id": None}


async def test_submit_replay_is_200_and_explicitly_marked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _Session()
    body = {
        "pipeline_run_id": str(uuid4()),
        "request_digest": "f" * 64,
        "idempotent_replay": True,
    }
    monkeypatch.setattr(
        pipeline_routes,
        "create_public_run",
        AsyncMock(return_value=(body, True)),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, _context())),
        base_url="http://svc",
    ) as client:
        response = await client.post(
            "/api/v1/pipeline-runs",
            headers={"Idempotency-Key": "submit-smoke-replay"},
            json=_submit_body(),
        )

    assert response.status_code == 200, response.text
    assert response.headers["Idempotent-Replay"] == "true"
    assert response.json() == body
    assert session.commits == 1


async def test_submit_request_is_closed_and_requires_idempotency_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_run = AsyncMock()
    monkeypatch.setattr(pipeline_routes, "create_public_run", create_run)
    app = _app(_Session(), _context())

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://svc",
    ) as client:
        missing_header = await client.post(
            "/api/v1/pipeline-runs",
            json=_submit_body(),
        )
        body_with_extra = _submit_body() | {"internal_override": True}
        extra_field = await client.post(
            "/api/v1/pipeline-runs",
            headers={"Idempotency-Key": "submit-smoke-extra"},
            json=body_with_extra,
        )

    assert missing_header.status_code == 422
    assert extra_field.status_code == 422
    assert any(error["type"] == "extra_forbidden" for error in extra_field.json()["detail"])
    create_run.assert_not_awaited()


async def test_get_run_projects_stages_artifacts_and_budget_without_internal_fields() -> None:
    run = _run()
    stage_id = uuid4()
    artifact_id = uuid4()
    stage = SimpleNamespace(
        id=stage_id,
        pipeline_run_id=run.id,
        node_key="evaluate",
        shard_key="0001",
        node_kind="container",
        state="failed",
        domain_outcome="infra_error",
        reason_code="worker_lost",
        execution_spec_digest="1" * 64,
        resolved_input_bindings_digest="2" * 64,
        resource_profile_digest="3" * 64,
        request_renderer_digest=None,
        attempt_count=2,
        latest_checkpoint_artifact_id=None,
    )
    artifact = SimpleNamespace(
        id=artifact_id,
        name="result",
        artifact_type="loom.result.v1",
        content_hash="4" * 64,
        manifest_sha256="5" * 64,
        stored_size_bytes=256,
        file_count=1,
        safety_state="verified",
    )
    ledger = SimpleNamespace(
        provider_limit_microusd=1_000_000,
        provider_reserved_microusd=250_000,
        provider_settled_microusd=125_000,
        gpu_limit_seconds=60,
        gpu_reserved_seconds=20,
        gpu_settled_seconds=10,
        artifact_limit_bytes=1024,
        artifact_reserved_bytes=256,
        artifact_settled_bytes=128,
        stage_runs_created=1,
        attempts_created=2,
        terminal_cause=None,
    )
    session = _Session(
        _Result(run),
        _Result([(run.id, "evaluate", "failed", "infra_error", 1)]),
        _Result([stage]),
        _Result([artifact]),
        get_result=ledger,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, _context())),
        base_url="http://svc",
    ) as client:
        response = await client.get(f"/api/v1/pipeline-runs/{run.id}")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["id"] == str(run.id)
    assert body["stages"][0]["id"] == str(stage_id)
    assert body["stages"][0]["retry_allowed"] is False
    assert body["stages"][0]["retry_ineligible_reason"] == "run_not_retryable"
    assert body["artifacts"][0]["download_path"] == (
        f"/api/v1/pipeline-artifacts/{artifact_id}/download"
    )
    assert body["budget"]["max_provider_cost_usd"]["settled"] == 125_000
    assert "team_id" not in body
    assert "graph_spec_json" not in body


async def test_get_run_hides_restricted_artifacts_from_unrelated_members() -> None:
    run = _run()
    restricted = SimpleNamespace(
        id=uuid4(),
        name="authoring",
        artifact_type="terminalgen_corpus.v1",
        content_hash="4" * 64,
        manifest_sha256="5" * 64,
        stored_size_bytes=256,
        file_count=1,
        safety_state="verified_internal",
        access_class="authoring_restricted",
    )
    session = _Session(
        _Result(run),
        _Result([]),
        _Result([]),
        _Result([restricted]),
        get_result=None,
    )
    context = _context(team_id=run.team_id, user_id=uuid4(), role="member")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, context)),
        base_url="http://svc",
    ) as client:
        response = await client.get(f"/api/v1/pipeline-runs/{run.id}")

    assert response.status_code == 200, response.text
    assert response.json()["artifacts"] == []


async def test_list_run_pagination_returns_an_opaque_signed_cursor() -> None:
    first = _run(created_at=datetime(2026, 8, 12, 12, 2, tzinfo=UTC))
    second = _run(created_at=datetime(2026, 8, 12, 12, 1, tzinfo=UTC))
    session = _Session(_Result([first, second]), _Result([]), _Result([]))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, _context())),
        base_url="http://svc",
    ) as client:
        response = await client.get("/api/v1/pipeline-runs", params={"limit": 1})

    assert response.status_code == 200, response.text
    body = response.json()
    assert [item["id"] for item in body["items"]] == [str(first.id)]
    assert isinstance(body["next_cursor"], str)
    assert str(first.id) not in body["next_cursor"]


def _stage(run_id: UUID, *, shard_key: str, state: str = "succeeded") -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        pipeline_run_id=run_id,
        node_key="evaluate",
        shard_key=shard_key,
        node_kind="container",
        state=state,
        domain_outcome="accepted" if state == "succeeded" else None,
        reason_code=None,
        attempt_count=1,
        resource_profile_json={"resource_profile": "terminalgen-validate-none@1"},
    )


async def test_stage_run_page_is_bounded_and_returns_durable_progress() -> None:
    run = _run()
    run.graph_spec_json = {"nodes": [{"node_key": "evaluate", "needs": []}]}
    first = _stage(run.id, shard_key="slot-0001")
    second = _stage(run.id, shard_key="slot-0002")
    aggregate = [(run.id, "evaluate", "succeeded", "accepted", 2)]
    session = _Session(_Result(run), _Result([first, second]), _Result(aggregate))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, _context(team_id=run.team_id))),
        base_url="http://svc",
    ) as client:
        response = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/stages", params={"limit": 1}
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert [item["id"] for item in body["items"]] == [str(first.id)]
    assert isinstance(body["next_cursor"], str)
    assert "slot-0001" not in body["next_cursor"]
    assert body["progress"] == {
        "total_stage_runs": 2,
        "completed_stage_runs": 2,
        "states": {"succeeded": 2},
        "domain_outcomes": {"accepted": 2},
        "nodes": {
            "evaluate": {
                "total_stage_runs": 2,
                "completed_stage_runs": 2,
                "states": {"succeeded": 2},
                "domain_outcomes": {"accepted": 2},
            }
        },
    }


async def test_stage_run_cursor_is_signed_and_filter_bound() -> None:
    run = _run()
    run.graph_spec_json = {"nodes": [{"node_key": "evaluate", "needs": []}]}
    first = _stage(run.id, shard_key="slot-0001")
    second = _stage(run.id, shard_key="slot-0002")
    aggregate = [(run.id, "evaluate", "succeeded", "accepted", 2)]
    first_session = _Session(_Result(run), _Result([first, second]), _Result(aggregate))
    context = _context(team_id=run.team_id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(first_session, context)),
        base_url="http://svc",
    ) as client:
        first_response = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/stages", params={"limit": 1}
        )
    cursor = first_response.json()["next_cursor"]

    second_session = _Session(_Result(run), _Result([second]), _Result(aggregate))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(second_session, context)),
        base_url="http://svc",
    ) as client:
        second_response = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/stages",
            params={"limit": 1, "cursor": cursor},
        )
    assert second_response.status_code == 200, second_response.text
    assert [item["id"] for item in second_response.json()["items"]] == [str(second.id)]

    tampered_session = _Session(_Result(run))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(tampered_session, context)),
        base_url="http://svc",
    ) as client:
        tampered = await client.get(
            f"/api/v1/pipeline-runs/{run.id}/stages",
            params={"limit": 1, "cursor": cursor[:-1] + ("A" if cursor[-1] != "A" else "B")},
        )
    assert tampered.status_code == 422
    assert tampered.json()["detail"]["reason_code"] == "invalid_cursor"


def _artifact(run_id: UUID, *, created_at: datetime, access_class: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        pipeline_run_id=run_id,
        pipeline_stage_run_id=uuid4(),
        execution_attempt_id=uuid4(),
        name="corpus",
        artifact_type="terminalgen_corpus.v1",
        content_hash="4" * 64,
        manifest_sha256="5" * 64,
        stored_size_bytes=256,
        file_count=1,
        safety_state="verified_internal",
        visibility="team",
        share_status="pending_scan",
        access_class=access_class,
        producer_kind="pipeline",
        created_at=created_at,
    )


async def test_artifact_page_preserves_restricted_access_and_cursor_scope() -> None:
    run = _run()
    restricted = _artifact(
        run.id,
        created_at=datetime(2026, 8, 12, 12, 2, tzinfo=UTC),
        access_class="authoring_restricted",
    )
    member_context = _context(team_id=run.team_id, user_id=uuid4(), role="member")
    member_session = _Session(_Result(run), _Result([restricted]))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(member_session, member_context)),
        base_url="http://svc",
    ) as client:
        hidden = await client.get(f"/api/v1/pipeline-runs/{run.id}/artifacts")
    assert hidden.status_code == 200, hidden.text
    assert hidden.json() == {"items": [], "next_cursor": None}

    owner_context = _context(team_id=run.team_id, user_id=uuid4(), role="owner")
    owner_session = _Session(_Result(run), _Result([restricted]))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(owner_session, owner_context)),
        base_url="http://svc",
    ) as client:
        visible = await client.get(f"/api/v1/pipeline-runs/{run.id}/artifacts")
    assert visible.status_code == 200, visible.text
    assert [item["id"] for item in visible.json()["items"]] == [str(restricted.id)]
