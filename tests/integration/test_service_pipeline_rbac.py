"""Authorization and tenant-isolation contracts for public Pipeline routes."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI

from loom.auth import AuthContext
from loom_service.dependencies import authed_session
from loom_service.routes import pipeline as pipeline_routes


class _Result:
    def __init__(self, value: object | None = None) -> None:
        self.value = value

    def scalar_one_or_none(self) -> object | None:
        return self.value

    def scalars(self) -> list[object]:
        return [] if self.value is None else [self.value]


class _Session:
    def __init__(self, *results: _Result) -> None:
        self._results = list(results)
        self.statements: list[object] = []

    async def execute(self, statement: object) -> _Result:
        self.statements.append(statement)
        return self._results.pop(0)


def _context(
    *,
    scopes: list[str],
    team_id: UUID | None = None,
    user_id: UUID | None = None,
    role: str = "member",
) -> AuthContext:
    return AuthContext(
        token_hash=b"x" * 32,
        type="user",
        scopes=scopes,
        team_id=team_id,
        expires_at=None,
        user_id=user_id,
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
        "max_provider_cost_usd": "1",
        "max_gpu_seconds": 60,
        "max_wall_seconds": 300,
        "max_artifact_bytes": 1024,
        "max_stage_runs": 8,
        "max_attempts_total": 16,
    }


def _input_import_manifest() -> dict[str, object]:
    checkpoint_tree_sha256 = (
        "sha256:0749d74c81fc078f8354547f87812260fdb8c4c07a981934ec7d79df0cffd106"
    )
    return {
        "schema_version": "behavior.input-import.v1",
        "kind": "policy",
        "name": "rbac-fixture",
        "version": "1",
        "upstream": {
            "type": "artifact",
            "locator": "tests/rbac-fixture",
            "revision": "r1",
        },
        "compatibility": {
            "kind": "policy",
            "architecture": "pi_behavior_b1k_fast",
            "action_dim": 23,
            "state_dim": 23,
            "robot_action_dim": 25,
            "checkpoint_format": "openpi_checkpoint_directory_v1",
            "checkpoint_root": "payload/checkpoint",
            "checkpoint_tree_sha256": checkpoint_tree_sha256,
            "model_identifier": "pi0-behavior-r1",
            "vla_interface_version": "behavior_b1k_websocket_v1",
            "controller_adapter_version": "r1pro_25_to_pi23_v1",
        },
        "files": [
            {
                "path": "checkpoint/weights.bin",
                "sha256": "a" * 64,
                "size_bytes": 3,
                "media_type": "application/octet-stream",
            }
        ],
    }


async def test_read_scope_can_list_but_cannot_submit() -> None:
    session = _Session(_Result())
    context = _context(
        scopes=["read:own"],
        team_id=uuid4(),
        user_id=uuid4(),
        role="viewer",
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, context)),
        base_url="http://svc",
    ) as client:
        listed = await client.get("/api/v1/pipeline-runs")
        submitted = await client.post(
            "/api/v1/pipeline-runs",
            headers={"Idempotency-Key": "viewer-submit"},
            json={
                "budget": _budget(),
                "inputs": {},
                "parameters": {},
                "recipe": "smoke@1",
            },
        )

    assert listed.status_code == 200, listed.text
    assert listed.json() == {"items": [], "next_cursor": None}
    assert submitted.status_code == 403
    assert submitted.json()["detail"] == "missing required scope: submit"


async def test_pipeline_routes_require_an_active_team_selection() -> None:
    context = _context(scopes=["read:own"], team_id=None, user_id=uuid4())

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(_Session(), context)),
        base_url="http://svc",
    ) as client:
        response = await client.get("/api/v1/pipeline-runs")

    assert response.status_code == 403
    assert response.json()["detail"]["reason_code"] == "team_required"


async def test_cross_team_run_is_hidden_as_not_found() -> None:
    selected_team_id = uuid4()
    session = _Session(_Result())
    context = _context(
        scopes=["read:own"],
        team_id=selected_team_id,
        user_id=uuid4(),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(session, context)),
        base_url="http://svc",
    ) as client:
        response = await client.get(f"/api/v1/pipeline-runs/{uuid4()}")

    assert response.status_code == 404
    assert response.json()["detail"]["reason_code"] == "not_found"
    statement = cast(Any, session.statements[0])
    assert "pipeline_runs.team_id" in str(statement)
    assert selected_team_id in statement.compile().params.values()


async def test_non_owner_cannot_create_an_input_import() -> None:
    session = _Session(_Result("member"))
    context = _context(
        scopes=["read:own", "submit"],
        team_id=uuid4(),
        user_id=uuid4(),
        role="member",
    )
    app = _app(session, context)
    adapter = AsyncMock()
    app.state.pipeline_public_adapter = adapter

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://svc",
    ) as client:
        response = await client.post(
            "/api/v1/pipeline-input-imports",
            headers={"Idempotency-Key": "member-import"},
            json={
                "kind": "policy",
                "manifest": _input_import_manifest(),
                "recipe": "behavior-recovery@1",
            },
        )

    assert response.status_code == 403
    assert response.json()["detail"]["reason_code"] == "team_admin_required"
    adapter.create_import.assert_not_awaited()


async def test_team_owner_can_create_an_input_import() -> None:
    session = _Session(_Result("owner"))
    context = _context(
        scopes=["read:own", "submit"],
        team_id=uuid4(),
        user_id=uuid4(),
        role="owner",
    )
    app = _app(session, context)
    adapter = AsyncMock()
    adapter.create_import.return_value = {
        "input_import_id": str(uuid4()),
        "state": "uploading",
    }
    app.state.pipeline_public_adapter = adapter

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://svc",
    ) as client:
        response = await client.post(
            "/api/v1/pipeline-input-imports",
            headers={"Idempotency-Key": "owner-import"},
            json={
                "kind": "policy",
                "manifest": _input_import_manifest(),
                "recipe": "behavior-recovery@1",
            },
        )

    assert response.status_code == 200, response.text
    call = adapter.create_import.await_args.kwargs
    assert call["team_id"] == context.team_id
    assert call["user_id"] == context.user_id
    assert call["idempotency_key"] == "owner-import"
