from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import ApiIdempotencyRecord, PipelineRun, PipelineStageRun
from loom.pipeline.keys import canonical_digest
from loom.pipeline.public_api import PipelineIdempotencyEndpoint, PipelineRunRetryRequestV1
from loom.pipeline.recipes import OfficialRecipeRegistration, OfficialRecipeRegistry
from loom.pipeline.spec import RecipeIdentityV1, RunGraphSpecV1
from loom_service.pipeline_api_service import PipelineApiError, create_retry_run

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.com/loom/pipeline@sha256:" + "b" * 64


def _budget(*, wall_seconds: int = 600) -> dict[str, object]:
    return {
        "max_provider_cost_usd": "0.000000",
        "max_gpu_seconds": 0,
        "max_wall_seconds": wall_seconds,
        "max_artifact_bytes": 10_000,
        "max_stage_runs": 4,
        "max_attempts_total": 4,
    }


def _factory(identity: RecipeIdentityV1, parameters: Mapping[str, Any]) -> RunGraphSpecV1:
    return RunGraphSpecV1.model_validate(
        {
            "schema_version": "loom.run-graph.v1",
            "recipe": identity.model_dump(mode="json"),
            "inputs": [],
            "parameters": dict(parameters),
            "budget": _budget(),
            "nodes": [
                {
                    "node_kind": "container",
                    "node_key": "root",
                    "image": IMAGE,
                    "argv": ["python", "-m", "pipeline.root"],
                    "workdir": "/workspace",
                    "resource_profile": "cpu_small@1",
                    "network_profile": "none",
                    "needs": [],
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
            ],
        }
    )


def _registry() -> OfficialRecipeRegistry:
    return OfficialRecipeRegistry(
        (
            OfficialRecipeRegistration(
                name="retry-fixture",
                version=1,
                submission_policy="ordinary",
                factory=_factory,
                parameter_contract_digest=DIGEST,
                source_lock_digest=DIGEST,
            ),
        )
    )


async def test_retry_creates_one_new_full_replay_and_lost_response_replays(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    user_id = uuid4()
    source_run_id = uuid4()
    source_stage_id = uuid4()
    registry = _registry()
    registration = registry.get("retry-fixture", 1)
    graph = registration.resolve({})
    graph_json = graph.model_dump(mode="json", exclude_none=False)
    frozen_bindings: list[dict[str, Any]] = []

    try:
        async with sessions() as session, session.begin():
            await session.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"pipeline-retry-{team_id}"},
            )
            await session.execute(
                text(
                    "INSERT INTO users "
                    "(id,username,username_normalized,status,is_platform_admin) "
                    "VALUES (:id,:username,:username,'active',false)"
                ),
                {"id": user_id, "username": f"retry-{user_id}"},
            )
            await session.execute(
                text(
                    """
                    INSERT INTO pipeline_runs (
                        id,team_id,created_by_user_id,submission_policy,
                        recipe_name,recipe_version,recipe_digest,
                        graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                        resolved_inputs_json,control_binding_snapshots_json,
                        control_binding_snapshots_digest,budget_json,request_digest,
                        idempotency_key,state,result,result_reason,finished_at
                    ) VALUES (
                        :id,:team_id,:user_id,'ordinary','retry-fixture',1,:recipe_digest,
                        CAST(:graph AS jsonb),:graph_digest,'{}'::jsonb,:parameters_digest,
                        '[]'::jsonb,'[]'::jsonb,:bindings_digest,CAST(:budget AS jsonb),
                        :request_digest,:idempotency_key,'finished','failed',
                        'selected_fail_run_stage_failed',clock_timestamp()
                    )
                    """
                ),
                {
                    "id": source_run_id,
                    "team_id": team_id,
                    "user_id": user_id,
                    "recipe_digest": registration.digest,
                    "graph": json.dumps(graph_json),
                    "graph_digest": canonical_digest(graph),
                    "parameters_digest": canonical_digest({}),
                    "bindings_digest": canonical_digest(frozen_bindings),
                    "budget": json.dumps(_budget()),
                    "request_digest": canonical_digest({"source": str(source_run_id)}),
                    "idempotency_key": f"source:{source_run_id}",
                },
            )
            await session.execute(
                text(
                    """
                    INSERT INTO pipeline_stage_runs (
                        id,pipeline_run_id,node_key,shard_key,node_kind,state,reason_code,
                        resource_profile_json,resource_profile_digest,failure_policy,
                        attempt_count,finished_at
                    ) VALUES (
                        :id,:run_id,'root','singleton','container','failed',
                        'provider_transient','{}'::jsonb,:digest,'fail_run',0,
                        clock_timestamp()
                    )
                    """
                ),
                {"id": source_stage_id, "run_id": source_run_id, "digest": DIGEST},
            )

        request = PipelineRunRetryRequestV1.model_validate(
            {"budget": _budget(wall_seconds=900), "display_name": "full replay"}
        )
        async with sessions() as session:
            stage = await session.get(PipelineStageRun, source_stage_id)
            assert stage is not None
            first, replay = await create_retry_run(
                session,
                team_id=team_id,
                user_id=user_id,
                stage=stage,
                idempotency_key="retry-lost-response",
                request=request,
                registry=registry,
            )
            await session.commit()
        assert replay is False

        async with sessions() as session:
            stage = await session.get(PipelineStageRun, source_stage_id)
            assert stage is not None
            second, replay = await create_retry_run(
                session,
                team_id=team_id,
                user_id=user_id,
                stage=stage,
                idempotency_key="retry-lost-response",
                request=request,
                registry=registry,
            )
            await session.commit()
        assert replay is True
        assert second == first

        retry_id = UUID(first["id"])
        async with sessions() as session:
            retry = await session.get(PipelineRun, retry_id)
            assert retry is not None
            assert retry.retry_of_pipeline_run_id == source_run_id
            assert retry.retry_from_stage_run_id == source_stage_id
            assert retry.graph_spec_json == graph_json
            assert retry.graph_spec_digest == canonical_digest(graph)
            assert retry.resolved_inputs_json == []
            assert retry.control_binding_snapshots_json == frozen_bindings
            assert retry.budget_json == _budget(wall_seconds=900)
            assert (
                await session.execute(
                    select(PipelineRun).where(PipelineRun.retry_of_pipeline_run_id == source_run_id)
                )
            ).scalars().all() == [retry]
            assert (
                await session.execute(
                    select(PipelineStageRun).where(PipelineStageRun.pipeline_run_id == retry_id)
                )
            ).scalars().all() == []
            record = (
                await session.execute(
                    select(ApiIdempotencyRecord).where(
                        ApiIdempotencyRecord.team_id == team_id,
                        ApiIdempotencyRecord.endpoint == "pipeline_stage_retry",
                        ApiIdempotencyRecord.idempotency_key == "retry-lost-response",
                    )
                )
            ).scalar_one()
            assert record.resource_id == retry_id
            assert record.response_status == 201
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                text("DELETE FROM pipeline_runs WHERE retry_of_pipeline_run_id=:source"),
                {"source": source_run_id},
            )
            await session.execute(
                text("DELETE FROM pipeline_runs WHERE id=:source"), {"source": source_run_id}
            )
            await session.execute(text("DELETE FROM users WHERE id=:id"), {"id": user_id})
            await session.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        await engine.dispose()


async def test_retry_same_key_changed_digest_is_durable_conflict(
    postgres_url: str,
) -> None:
    """A completed durable record rejects a changed digest before resource mutation."""

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    key = "retry-conflict"
    try:
        async with sessions() as session, session.begin():
            await session.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"pipeline-retry-conflict-{team_id}"},
            )
            await session.execute(
                text(
                    """
                    INSERT INTO api_idempotency_records (
                        team_id,endpoint,idempotency_key,request_digest,state,
                        response_status,response_json,completed_at,expires_at
                    ) VALUES (
                        :team_id,'pipeline_stage_retry',:key,:digest,'completed',
                        201,'{}'::jsonb,clock_timestamp(),clock_timestamp()+interval '30 days'
                    )
                    """
                ),
                {"team_id": team_id, "key": key, "digest": DIGEST},
            )
        from loom_service.pipeline_api_service import claim_idempotency

        async with sessions() as session:
            with pytest.raises(PipelineApiError, match="Idempotency key body differs") as exc:
                await claim_idempotency(
                    session,
                    team_id=team_id,
                    endpoint=PipelineIdempotencyEndpoint.PIPELINE_STAGE_RETRY,
                    key=key,
                    request_digest="sha256:" + "c" * 64,
                )
            assert exc.value.status_code == 409
            assert exc.value.reason_code == "idempotency_conflict"
            await session.rollback()
    finally:
        async with sessions() as session, session.begin():
            await session.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        await engine.dispose()
