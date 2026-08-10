from __future__ import annotations

from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import LlmCall
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.llm_calls import record_call


async def test_gateway_call_has_execution_attempt_and_null_trial(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id, run_id, stage_id, attempt_id = uuid4(), uuid4(), uuid4(), uuid4()
    digest = "sha256:" + "a" * 64
    async with sessions() as session, session.begin():
        await session.execute(
            text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
            {"id": team_id, "name": f"gateway-attribution-{team_id}"},
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_runs (
                    id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                    graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key
                ) VALUES (
                    :id,:team,'ordinary','gateway-attribution',1,:digest,
                    '{}'::jsonb,:digest,'{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,
                    :digest,:key
                )
            """),
            {
                "id": run_id,
                "team": team_id,
                "digest": digest,
                "key": f"gateway-{run_id}",
            },
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_stage_runs (
                    id,pipeline_run_id,node_key,shard_key,node_kind,state,
                    resource_profile_json,resource_profile_digest,failure_policy
                ) VALUES (:id,:run,'judge','singleton','container','blocked',
                          '{}'::jsonb,:digest,'fail_run')
            """),
            {"id": stage_id, "run": run_id, "digest": digest},
        )
        await session.execute(
            text("""
                INSERT INTO execution_attempts (
                    id,stage_run_id,attempt_number,state
                ) VALUES (:id,:stage,1,'fault_pending')
            """),
            {"id": attempt_id, "stage": stage_id},
        )
    async with sessions() as session:
        await record_call(
            session,
            team_id=team_id,
            execution_attempt_id=attempt_id,
            step_id="offline_judge",
            dialect="openai_responses",
            model="gpt-test",
            usage=TokenUsage(input_tokens=3, output_tokens=2),
            cost_usd=0.01,
            rate_card_hash="test",
        )
        row = (
            await session.execute(
                select(LlmCall).where(LlmCall.execution_attempt_id == attempt_id)
            )
        ).scalar_one()
        assert row.trial_id is None
        assert row.execution_attempt_id == attempt_id
        await session.delete(row)
        await session.commit()
    async with engine.begin() as connection:
        await connection.execute(
            text("DELETE FROM execution_attempts WHERE id=:id"), {"id": attempt_id}
        )
        await connection.execute(
            text("DELETE FROM pipeline_stage_runs WHERE id=:id"), {"id": stage_id}
        )
        await connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
        await connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
    await engine.dispose()
