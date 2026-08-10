from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import RunGraphSpecV1
from loom_pipeline_orchestrator.repository import PipelineRepository

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.com/loom/pipeline@sha256:" + "b" * 64


def container(node_key: str, *, needs: list[str] | None = None) -> dict[str, Any]:
    return {
        "node_kind": "container",
        "node_key": node_key,
        "image": IMAGE,
        "argv": ["python", "-m", f"pipeline.{node_key}"],
        "workdir": "/workspace",
        "resource_profile": "cpu_small@1",
        "network_profile": "none",
        "needs": needs or [],
        "inputs": [],
        "outputs": [],
        "request_renderer": None,
        "checkpoint": None,
        "fanout": None,
        "fanout_commit": None,
        "timeout_seconds": 60,
        "max_attempts": 3,
        "failure_policy": "fail_run",
    }


def graph_value(*, provider: str = "0.000010", stages: int = 8, attempts: int = 8) -> dict[str, Any]:
    return {
        "schema_version": "loom.run-graph.v1",
        "recipe": {"name": "orchestrator-fixture", "version": 1, "digest": DIGEST},
        "inputs": [],
        "parameters": {},
        "budget": {
            "max_provider_cost_usd": provider,
            "max_gpu_seconds": 300,
            "max_wall_seconds": 600,
            "max_artifact_bytes": 10_000,
            "max_stage_runs": stages,
            "max_attempts_total": attempts,
        },
        "nodes": [container("root")],
    }


@dataclass(frozen=True, slots=True)
class OrchestratorSeed:
    postgres_url: str
    team_id: UUID
    run_id: UUID
    repository: PipelineRepository
    sessions: async_sessionmaker[Any]


@pytest.fixture
async def orchestrator_seed(postgres_url: str) -> AsyncIterator[OrchestratorSeed]:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    run_id = uuid4()
    graph = RunGraphSpecV1.model_validate(graph_value())
    graph_json = graph.model_dump(mode="json", exclude_none=False)
    async with sessions() as session, session.begin():
        await session.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            {"id": team_id, "name": f"pipeline-orchestrator-{team_id}"},
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_runs (
                    id, team_id, submission_policy, recipe_name, recipe_version, recipe_digest,
                    graph_spec_json, graph_spec_digest, parameters_json, parameters_digest,
                    resolved_inputs_json, budget_json, request_digest, idempotency_key
                ) VALUES (
                    :id, :team_id, 'ordinary', 'orchestrator-fixture', 1, :recipe_digest,
                    CAST(:graph AS jsonb), :graph_digest, '{}'::jsonb, :parameters_digest,
                    '[]'::jsonb, CAST(:budget AS jsonb), :request_digest, :idempotency_key
                )
            """),
            {
                "id": run_id,
                "team_id": team_id,
                "recipe_digest": DIGEST,
                "graph": json.dumps(graph_json),
                "graph_digest": canonical_digest(graph),
                "parameters_digest": canonical_digest({}),
                "budget": json.dumps(graph_json["budget"]),
                "request_digest": canonical_digest({"run_id": str(run_id)}),
                "idempotency_key": f"orchestrator-{run_id}",
            },
        )
    seed = OrchestratorSeed(
        postgres_url=postgres_url,
        team_id=team_id,
        run_id=run_id,
        repository=PipelineRepository(sessions),
        sessions=sessions,
    )
    try:
        yield seed
    finally:
        async with sessions() as session, session.begin():
            await session.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
            await session.execute(
                text("DELETE FROM artifacts WHERE team_id=:id"), {"id": team_id}
            )
            await session.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        await engine.dispose()
