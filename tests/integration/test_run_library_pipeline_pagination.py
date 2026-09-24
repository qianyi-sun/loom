"""Run Library walks Pipeline bundles beyond the former 200-artifact boundary."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom_service.dependencies import authed_session
from loom_service.routes.run_library import router
from tests.integration.test_pipeline_constraints import PipelineSeed, pipeline_seed  # noqa: F401


async def test_pipeline_artifacts_paginate_after_access_filters(
    pipeline_seed: PipelineSeed, postgres_url: str,  # noqa: F811
) -> None:
    seed = pipeline_seed
    attempt = uuid4()
    created = datetime(2026, 9, 23, tzinfo=UTC)
    expected = [UUID(int=number) for number in range(1, 206)]
    with seed.engine.begin() as conn:
        conn.execute(text("INSERT INTO execution_attempts (id, stage_run_id, attempt_number, state, queued_at) VALUES (:id, :stage, 1, 'queued', now())"), {"id": attempt, "stage": seed.subject_stage_id})
        # Equal timestamps exercise the UUID tie-breaker. Restricted rows occur
        # before every public page, so the page size must follow authorization.
        conn.execute(text("""INSERT INTO artifacts (id, artifact_type, name, team_id, content_hash, pipeline_run_id, pipeline_stage_run_id, execution_attempt_id, producer_kind, created_at, access_class)
            VALUES (:id, 'fixture.output.v1', :name, :team, :digest, :run, :stage, :attempt, 'container', :created, :access)"""), [
            {"id": item, "name": f"output-{item.int}", "team": seed.team_id, "digest": "sha256:" + "a" * 64, "run": seed.run_id, "stage": seed.subject_stage_id, "attempt": attempt, "created": created, "access": "team_runtime" if item.int <= 205 else "authoring_restricted"}
            for item in [*expected, *(UUID(int=number) for number in range(206, 411))]
        ])
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    ctx = AuthContext(token_hash=b"x" * 32, type="user", scopes=["read:own"], team_id=seed.team_id, expires_at=None, user_id=uuid4(), role="member")
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    async def auth_override():
        async with sessions() as session:
            yield session, ctx
    app.dependency_overrides[authed_session] = auth_override
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
            params = {"producer_kind": "pipeline", "pipeline_recipe": "pipeline-constraint-fixture@1", "limit": "100"}
            seen = []
            sizes = []
            cursors = []
            while True:
                response = await client.get("/api/v1/run-library/artifacts", params=params)
                assert response.status_code == 200, response.text
                body = response.json()
                sizes.append(len(body["items"]))
                seen.extend(item["id"] for item in body["items"])
                assert all(item["pipeline"]["run_id"] == str(seed.run_id) for item in body["items"])
                if not body["next_cursor"]:
                    break
                cursors.append(body["next_cursor"])
                params["cursor"] = body["next_cursor"]
            assert sizes == [100, 100, 5]
            assert seen == [str(item) for item in reversed(expected)]
            middle = await client.get("/api/v1/run-library/artifacts", params={**params, "cursor": cursors[0]})
            assert [item["id"] for item in middle.json()["items"]] == seen[100:200]
            empty = await client.get("/api/v1/run-library/artifacts", params={"producer_kind": "pipeline", "artifact_type": "no-match"})
            assert empty.json() == {"items": [], "next_cursor": None}
            invalid = await client.get("/api/v1/run-library/artifacts", params={"cursor": "invalid"})
            assert invalid.status_code == 400
    finally:
        await engine.dispose()
