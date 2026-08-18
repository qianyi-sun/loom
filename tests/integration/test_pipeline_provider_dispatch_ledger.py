from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.provider_dispatch import (
    ProviderDispatchError,
    mark_provider_dispatch_sent,
    release_provider_dispatch_unsent,
    reserve_provider_dispatch,
    settle_provider_dispatch,
    settle_stale_provider_dispatches,
)


async def test_pipeline_provider_dispatch_is_exactly_once_and_conservatively_settled(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    ids = {name: uuid4() for name in ("team", "run", "stage", "attempt", "worker", "connection")}
    binding_digest = "sha256:" + "b" * 64
    authorization_digest = "sha256:" + "c" * 64
    step_jwt_id = uuid4()
    execution_spec = {
        "container_node": {"network_profile": "gateway"},
        "control_binding_snapshots": [{"snapshot_sha256": binding_digest}],
    }
    execution_spec_digest = canonical_digest(execution_spec)
    binding_snapshot = {
        "schema_version": "loom.recipe-provider-binding.v2",
        "provider_connection_id": str(ids["connection"]),
        "provider": "openai",
        "model": "gpt-test",
        "wire_api": "responses",
    }
    authorization = {"schema_version": "loom.terminalgen-authoring-grant.v1"}
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            await session.execute(
                text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
                {"id": ids["team"], "name": f"provider-ledger-{ids['team']}"},
            )
            await session.execute(
                text("""
                    INSERT INTO provider_connections (
                        id,team_id,provider_type,display_name,base_url,upstream_host,
                        encrypted_api_key_ref,status,created_by
                    ) VALUES (
                        :id,:team,'openai-compatible',:name,'https://provider.invalid/v1',
                        'provider.invalid','loom://test/provider','valid','test'
                    )
                """),
                {
                    "id": ids["connection"],
                    "team": ids["team"],
                    "name": str(ids["connection"]),
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                        graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                        resolved_inputs_json,budget_json,request_digest,idempotency_key,state
                    ) VALUES (
                        :id,:team,'ordinary','terminalgen-authoring',1,:digest,
                        '{}'::jsonb,:digest,'{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,
                        :digest,:key,'running'
                    )
                """),
                {
                    "id": ids["run"],
                    "team": ids["team"],
                    "digest": "sha256:" + "a" * 64,
                    "key": f"provider-ledger-{ids['run']}",
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id,pipeline_run_id,node_key,shard_key,node_kind,state,
                        resolved_execution_spec_json,resolved_execution_spec_bytes,
                        execution_spec_digest,resource_profile_json,resource_profile_digest,
                        provider_connection_ref,resolved_input_bindings_json,
                        resolved_input_bindings_digest,failure_policy,attempt_count,
                        claimed_at,started_at
                    ) VALUES (
                        :id,:run,'generate_card_00','singleton','container','running',
                        CAST(:spec AS jsonb),:spec_bytes,:spec_digest,'{}'::jsonb,:digest,
                        :connection,'[]'::jsonb,:inputs_digest,'fail_run',1,:now,:now
                    )
                """),
                {
                    "id": ids["stage"],
                    "run": ids["run"],
                    "spec": json.dumps(execution_spec),
                    "spec_bytes": canonical_document(execution_spec),
                    "spec_digest": execution_spec_digest,
                    "digest": "sha256:" + "d" * 64,
                    "connection": ids["connection"],
                    "inputs_digest": canonical_digest([]),
                    "now": now,
                },
            )
            await session.execute(
                text("""
                    INSERT INTO workers (
                        id,hostname,version,capabilities,registered_at,last_seen_at,status
                    ) VALUES (:id,:hostname,'test','[]'::jsonb,:now,:now,'active')
                """),
                {
                    "id": ids["worker"],
                    "hostname": f"provider-ledger-{ids['worker']}",
                    "now": now,
                },
            )
            await session.execute(
                text("""
                    INSERT INTO execution_attempts (
                        id,stage_run_id,attempt_number,state,worker_id,claim_id,lease_epoch,
                        lease_token_digest,lease_expires_at,step_jwt_id,
                        execution_authorization_json,execution_authorization_bytes,
                        execution_authorization_digest,queued_at,claimed_at,started_at
                    ) VALUES (
                        :id,:stage,1,'running',:worker,:claim,1,:lease_digest,:expires,:jti,
                        CAST(:authorization AS jsonb),:authorization_bytes,
                        :authorization_digest,:now,:now,:now
                    )
                """),
                {
                    "id": ids["attempt"],
                    "stage": ids["stage"],
                    "worker": ids["worker"],
                    "claim": uuid4(),
                    "lease_digest": "sha256:" + "e" * 64,
                    "expires": now + timedelta(minutes=5),
                    "jti": step_jwt_id,
                    "authorization": json.dumps(authorization),
                    "authorization_bytes": canonical_document(authorization),
                    "authorization_digest": authorization_digest,
                    "now": now,
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_run_control_bindings (
                        id,pipeline_run_id,logical_name,kind,node_key,source_object_id,
                        source_version,snapshot_json,snapshot_bytes,snapshot_sha256,
                        provider_connection_id,provider_request_limit,
                        provider_cost_limit_microusd,per_call_timeout_seconds
                    ) VALUES (
                        :id,:run,'terminalgen_generate_card_00','provider',
                        'generate_card_00',:source,1,CAST(:snapshot AS jsonb),
                        :snapshot_bytes,:digest,:connection,2,10,30
                    )
                """),
                {
                    "id": uuid4(),
                    "run": ids["run"],
                    "source": uuid4(),
                    "snapshot": json.dumps(binding_snapshot),
                    "snapshot_bytes": canonical_document(binding_snapshot),
                    "digest": binding_digest,
                    "connection": ids["connection"],
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_budget_ledgers (
                        pipeline_run_id,provider_limit_microusd,gpu_limit_seconds,
                        artifact_limit_bytes,stage_run_limit,attempt_limit,wall_deadline_at
                    ) VALUES (:run,10,0,0,1,1,:deadline)
                """),
                {"run": ids["run"], "deadline": now + timedelta(hours=1)},
            )
            await session.execute(
                text("""
                    INSERT INTO execution_attempt_provider_budgets (
                        attempt_id,binding_snapshot_sha256,request_limit,
                        cost_limit_microusd,per_call_timeout_seconds
                    ) VALUES (:attempt,:binding,2,10,30)
                """),
                {"attempt": ids["attempt"], "binding": binding_digest},
            )

        ctx = AuthContext(
            token_hash=b"",
            type="step_session",
            scopes=["llm:call"],
            team_id=ids["team"],
            expires_at=now + timedelta(minutes=5),
            execution_attempt_id=ids["attempt"],
            step_id="generate_card_00",
            provider_connection_id=ids["connection"],
            provider_connection_id_bound=True,
            step_jwt_id=step_jwt_id,
            execution_attempt_lease_epoch=1,
            execution_spec_digest=execution_spec_digest,
            control_binding_snapshot_digest=binding_digest,
            execution_authorization_digest=authorization_digest,
        )
        unsent_request_id = uuid4()
        unsent_digest = canonical_digest({"ordinal": 0, "prompt": "secret-canary"})
        async with sessions() as session:
            unsent = await reserve_provider_dispatch(
                session,
                ctx=ctx,
                provider_request_id=unsent_request_id,
                request_digest=unsent_digest,
                provider_connection_id=ids["connection"],
                provider="openai",
                model="gpt-test",
                wire_api="responses",
            )
        async with sessions() as session:
            released = await release_provider_dispatch_unsent(
                session,
                ctx=ctx,
                dispatch_id=unsent.dispatch_id,
            )
        assert released.outcome == "not_dispatched"

        request_id = uuid4()
        request_digest = canonical_digest({"ordinal": 1, "prompt": "secret-canary"})
        async with sessions() as session:
            grant = await reserve_provider_dispatch(
                session,
                ctx=ctx,
                provider_request_id=request_id,
                request_digest=request_digest,
                provider_connection_id=ids["connection"],
                provider="openai",
                model="gpt-test",
                wire_api="responses",
            )
        assert grant.reserved_cost_microusd == 10
        async with sessions() as session:
            replay = await reserve_provider_dispatch(
                session,
                ctx=ctx,
                provider_request_id=request_id,
                request_digest=request_digest,
                provider_connection_id=ids["connection"],
                provider="openai",
                model="gpt-test",
                wire_api="responses",
            )
        assert replay == grant
        async with sessions() as session:
            with pytest.raises(ProviderDispatchError, match="request_id_conflict"):
                await reserve_provider_dispatch(
                    session,
                    ctx=ctx,
                    provider_request_id=request_id,
                    request_digest=canonical_digest({"ordinal": 999}),
                    provider_connection_id=ids["connection"],
                    provider="openai",
                    model="gpt-test",
                    wire_api="responses",
                )
        async with sessions() as session:
            await mark_provider_dispatch_sent(session, ctx=ctx, dispatch_id=grant.dispatch_id)
        response = {"id": "response-1", "usage": {"input_tokens": 3, "output_tokens": 2}}
        async with sessions() as session:
            settled = await settle_provider_dispatch(
                session,
                ctx=ctx,
                dispatch_id=grant.dispatch_id,
                outcome="succeeded",
                usage=TokenUsage(
                    input_tokens=3,
                    output_tokens=2,
                    provider_extras={"reasoning_tokens": 1, "raw_response": "secret-canary"},
                ),
                actual_cost_microusd=4,
                rate_card_hash="test-rate-card",
                request_params={"temperature": 0.1, "prompt": "secret-canary"},
                response_digest=canonical_digest(response),
            )
        assert settled.outcome == "succeeded"
        assert settled.actual_cost_microusd == 4

        async with sessions() as session:
            rows = (
                await session.execute(
                    text("""
                        SELECT d.state,d.outcome,d.actual_cost_microusd,d.response_digest,
                               b.requests_reserved,b.requests_settled,
                               b.cost_reserved_microusd,b.cost_settled_microusd,
                               l.provider_reserved_microusd,l.provider_settled_microusd,
                               c.provider_extras::text,c.request_params::text
                          FROM pipeline_provider_dispatches d
                          JOIN execution_attempt_provider_budgets b
                            ON b.attempt_id=d.execution_attempt_id
                          JOIN pipeline_budget_reservations r ON r.id=d.reservation_id
                          JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=r.pipeline_run_id
                          JOIN llm_calls c ON c.id=d.llm_call_id
                         WHERE d.id=:id
                    """),
                    {"id": grant.dispatch_id},
                )
            ).one()
        assert rows[:10] == (
            "settled",
            "succeeded",
            4,
            canonical_digest(response),
            0,
            1,
            0,
            4,
            0,
            4,
        )
        assert "secret-canary" not in rows[10]
        assert "secret-canary" not in rows[11]
        assert "raw_response" not in rows[10]
        assert "reasoning_tokens" in rows[10]

        stale_request_id = uuid4()
        async with sessions() as session:
            stale = await reserve_provider_dispatch(
                session,
                ctx=ctx,
                provider_request_id=stale_request_id,
                request_digest=canonical_digest({"ordinal": 2}),
                provider_connection_id=ids["connection"],
                provider="openai",
                model="gpt-test",
                wire_api="responses",
            )
        assert stale.reserved_cost_microusd == 6
        async with sessions() as session:
            await mark_provider_dispatch_sent(session, ctx=ctx, dispatch_id=stale.dispatch_id)
        async with sessions() as session:
            recovered = await settle_stale_provider_dispatches(
                session,
                stale_before=now + timedelta(hours=1),
            )
        assert recovered == 1
        async with sessions() as session:
            recovered_row = (
                await session.execute(
                    text("""
                        SELECT d.state,d.outcome,d.actual_cost_microusd,
                               d.reserved_cost_microusd,c.provider_extras::text
                        FROM pipeline_provider_dispatches d
                        JOIN llm_calls c ON c.id=d.llm_call_id
                        WHERE d.id=:id
                    """),
                    {"id": stale.dispatch_id},
                )
            ).one()
        assert recovered_row[:2] == ("settled", "uncertain")
        assert recovered_row.actual_cost_microusd == recovered_row.reserved_cost_microusd
        assert "gateway_crash_after_send" in recovered_row.provider_extras
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                text("DELETE FROM pipeline_provider_dispatches WHERE execution_attempt_id=:id"),
                {"id": ids["attempt"]},
            )
            await session.execute(
                text("DELETE FROM llm_calls WHERE execution_attempt_id=:id"),
                {"id": ids["attempt"]},
            )
            await session.execute(
                text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": ids["run"]}
            )
            await session.execute(
                text("DELETE FROM provider_connections WHERE id=:id"),
                {"id": ids["connection"]},
            )
            await session.execute(text("DELETE FROM workers WHERE id=:id"), {"id": ids["worker"]})
            await session.execute(text("DELETE FROM teams WHERE id=:id"), {"id": ids["team"]})
        await engine.dispose()
