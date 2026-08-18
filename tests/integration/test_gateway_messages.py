"""POST /v1/messages — Anthropic native passthrough (Plan 9 Task 7).

We use httpx.MockTransport on the Gateway's `app.state.upstream_client`
to intercept the outbound call to Anthropic and return a canned
Anthropic-shaped response. The route then records cost into llm_calls.
"""

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import LlmCall, RateCard, Team
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.gateway_db import (
    delete_gateway_trial,
    delete_team_and_quota,
    insert_gateway_trial,
)


@pytest.fixture
async def gateway_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID]]:
    """Build the gateway app + return (app, step_jwt, team_id, trial_id)
    with a seeded rate card + mocked Anthropic upstream."""
    for k, v in {
        "LOOM_GW_DB_URL": postgres_url,
        "LOOM_GW_ANTHROPIC_API_KEY": "test-anthropic-key",
    }.items():
        monkeypatch.setenv(k, v)
    settings = GatewaySettings(_env_file=None)
    app = create_app(settings)

    # Manually populate app.state (ASGITransport doesn't run lifespan)
    async_engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(async_engine, expire_on_commit=False)
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory,
        ttl_sec=settings.rate_card_cache_ttl_sec,
    )

    # Canned Anthropic response intercepted by MockTransport.
    canned_response = {
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-4-7",
        "content": [{"type": "text", "text": "hello"}],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_creation_input_tokens": 20,
            "cache_read_input_tokens": 30,
        },
    }

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/messages"
        assert request.headers["x-api-key"] == "test-anthropic-key"
        return httpx.Response(200, json=canned_response)

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )

    # Seed a rate card so the cost compute works.
    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(
            insert(RateCard).values(
                id="card-test",
                captured_at=datetime.now(UTC),
                table={
                    "id": "card-test",
                    "entries": [
                        {
                            "provider": "anthropic",
                            "model": "claude-opus-4-7",
                            "input_per_mtok": 15.0,
                            "output_per_mtok": 75.0,
                            "cache_read_per_mtok": 1.5,
                            "cache_write_per_mtok": 18.75,
                        }
                    ],
                },
            )
        )
        s.commit()

    team_id = uuid4()
    trial_id = uuid4()
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        task_id = insert_gateway_trial(s, team_id=team_id, trial_id=trial_id)
        s.commit()
    step_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="main",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )

    try:
        yield app, step_jwt, team_id, trial_id
    finally:
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            delete_gateway_trial(s, trial_id=trial_id, task_id=task_id)
            delete_team_and_quota(s, team_id)
            s.execute(delete(RateCard))
            s.commit()
        sync_engine.dispose()


async def test_messages_native_passthrough_records_llm_call(  # type: ignore[no-untyped-def]
    gateway_setup,
    postgres_url,
):
    app, step_jwt, team_id, trial_id = gateway_setup
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/messages",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={
                "model": "claude-opus-4-7",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        # Native Anthropic shape preserved (cache fields visible).
        assert body["usage"]["input_tokens"] == 100
        assert body["usage"]["cache_creation_input_tokens"] == 20
        assert body["content"][0]["text"] == "hello"

    # llm_calls row written with the JWT's trial_id + step_id.
    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "anthropic"
    assert row["model"] == "claude-opus-4-7"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 50
    # Cost = 100/1M*15 + 50/1M*75 + 30/1M*1.5 + 20/1M*18.75
    #      = 0.0015 + 0.00375 + 0.000045 + 0.000375 = 0.00567
    assert float(row["cost_usd"]) == pytest.approx(0.00567, abs=1e-6)


async def test_messages_rejects_pipeline_jwt_outside_fenced_responses_route(  # type: ignore[no-untyped-def]
    gateway_setup,
    postgres_url,
):
    app, _trial_jwt, team_id, _trial_id = gateway_setup
    run_id, stage_id, attempt_id, worker_id, claim_id, token_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    digest = "sha256:" + "a" * 64
    execution_spec = {
        "container_node": {
            "network_profile": "gateway",
            "timeout_seconds": 300,
        },
        "control_binding_snapshots": [],
    }
    execution_spec_digest = canonical_digest(execution_spec)
    execution_authorization = {"schema_version": "loom.terminalgen-authoring-grant.v1"}
    execution_authorization_digest = canonical_digest(execution_authorization)
    lease_token = "lease-" + "x" * 40
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text("""
                INSERT INTO workers (
                    id,hostname,version,capabilities,supported_work_kinds,
                    auth_token_hash,registered_at,last_seen_at,status
                ) VALUES (
                    :id,'gateway-attempt-worker','test','[]'::jsonb,
                    ARRAY['trial','execution_attempt']::text[],
                    :token,now(),now(),'active'
                )
            """),
            {"id": worker_id, "token": b"g" * 32},
        )
        connection.execute(
            text("""
                INSERT INTO pipeline_runs (
                    id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                    graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key,state,started_at
                ) VALUES (
                    :id,:team,'ordinary','messages-attribution',1,:digest,
                    '{}'::jsonb,:digest,'{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,
                    :digest,:key,'running',now()
                )
            """),
            {
                "id": run_id,
                "team": team_id,
                "digest": digest,
                "key": f"messages-{run_id}",
            },
        )
        connection.execute(
            text("""
                INSERT INTO pipeline_stage_runs (
                    id,pipeline_run_id,node_key,shard_key,node_kind,state,
                    resolved_execution_spec_json,resolved_execution_spec_bytes,
                    execution_spec_digest,resolved_input_bindings_json,
                    resolved_input_bindings_digest,resource_profile_json,
                    resource_profile_digest,failure_policy,ready_at,claimed_at,started_at
                ) VALUES (
                    :id,:run,'recovery_primitive','singleton','container','running',
                    CAST(:spec AS jsonb),:spec_bytes,:spec_digest,'[]'::jsonb,
                    :bindings_digest,'{}'::jsonb,:digest,'fail_run',now(),now(),now()
                )
            """),
            {
                "id": stage_id,
                "run": run_id,
                "digest": digest,
                "spec": json.dumps(execution_spec),
                "spec_bytes": canonical_document(execution_spec),
                "spec_digest": execution_spec_digest,
                "bindings_digest": canonical_digest([]),
            },
        )
        connection.execute(
            text("""
                INSERT INTO execution_attempts (
                    id,stage_run_id,attempt_number,state,worker_id,claim_id,
                        lease_epoch,lease_token_digest,lease_expires_at,step_jwt_id,
                        execution_authorization_json,execution_authorization_bytes,
                        execution_authorization_digest,
                        queued_at,claimed_at,started_at
                    ) VALUES (
                        :id,:stage,1,'running',:worker,:claim,3,:lease_digest,:expires,
                        :token_id,CAST(:authorization AS jsonb),:authorization_bytes,
                        :authorization_digest,now(),now(),now()
                )
            """),
            {
                "id": attempt_id,
                "stage": stage_id,
                "worker": worker_id,
                "claim": claim_id,
                "lease_digest": hashlib.sha256(lease_token.encode()).hexdigest(),
                "expires": datetime.now(UTC) + timedelta(minutes=10),
                    "token_id": token_id,
                    "authorization": json.dumps(execution_authorization),
                    "authorization_bytes": canonical_document(execution_authorization),
                    "authorization_digest": execution_authorization_digest,
            },
        )
    step_jwt = mint_step_jwt(
        team_id=team_id,
        execution_attempt_id=attempt_id,
        step_id="recovery_primitive",
        ttl_sec=600,
        signing_key=app.state.settings.step_jwt_signing_key.get_secret_value(),
        provider_connection_id=None,
        provider_connection_id_bound=True,
        step_jwt_id=token_id,
        execution_attempt_lease_epoch=3,
        execution_spec_digest=execution_spec_digest,
        control_binding_snapshot_digest=None,
        execution_authorization_digest=execution_authorization_digest,
    )
    try:
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
            response = await client.post(
                "/v1/messages",
                headers={"Authorization": f"Bearer {step_jwt}"},
                json={
                    "model": "claude-opus-4-7",
                    "max_tokens": 32,
                    "messages": [{"role": "user", "content": "recover"}],
                },
            )
        assert response.status_code == 403, response.text
        assert response.json() == {
            "detail": "execution-attempt tokens are restricted to the fenced Responses route"
        }
        with engine.connect() as connection:
            call_count = connection.execute(
                text("""
                    SELECT count(*)
                    FROM llm_calls WHERE execution_attempt_id=:attempt
                """),
                {"attempt": attempt_id},
            ).scalar_one()
        assert call_count == 0

        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE execution_attempts
                    SET cancellation_requested_at=now()
                    WHERE id=:id
                """),
                {"id": attempt_id},
            )
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
            cancelled = await client.post(
                "/v1/messages",
                headers={"Authorization": f"Bearer {step_jwt}"},
                json={
                    "model": "claude-opus-4-7",
                    "max_tokens": 32,
                    "messages": [{"role": "user", "content": "recover"}],
                },
            )
        assert cancelled.status_code == 403
        assert cancelled.json() == {"detail": "execution attempt dispatch forbidden"}
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM llm_calls WHERE execution_attempt_id=:id"),
                {"id": attempt_id},
            )
            connection.execute(
                text("DELETE FROM execution_attempts WHERE id=:id"), {"id": attempt_id}
            )
            connection.execute(
                text("DELETE FROM pipeline_stage_runs WHERE id=:id"), {"id": stage_id}
            )
            connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
            connection.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
        engine.dispose()


async def test_messages_rejects_non_step_token(  # type: ignore[no-untyped-def]
    gateway_setup,
    postgres_url,
):
    """A regular team-token bearer must be rejected — only step-scoped
    JWTs carry the trial_id/step_id needed for cost attribution."""
    app, _step_jwt, _team_id, _trial_id = gateway_setup
    raw = f"team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    from loom.db.schema import Token

    with session_local() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/messages",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "model": "claude-opus-4-7",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        # 401 (no llm:call scope) or 403 (step-scoped required); either is fine.
        assert r.status_code in (401, 403)


async def test_messages_redacts_upstream_error_detail(gateway_setup):  # type: ignore[no-untyped-def]
    app, step_jwt, _team_id, _trial_id = gateway_setup

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            text=(
                "bad x-api-key=test-anthropic-key "
                "trace=http://minio.internal/a?X-Amz-Signature=abc "
                "via http://loom-llm-gateway:9100"
            ),
        )

    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/messages",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={
                "model": "claude-opus-4-7",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )

    assert r.status_code == 403
    detail = r.json()["detail"]
    assert "test-anthropic-key" not in detail
    assert "minio.internal" not in detail
    assert "X-Amz-Signature=abc" not in detail
    assert "loom-llm-gateway" not in detail
    assert "[REDACTED" in detail


def _sse_block(event: str, data: dict) -> bytes:
    """Build one Anthropic-shaped SSE block."""
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _canned_anthropic_stream() -> bytes:
    """Sample Anthropic SSE stream with the same totals as the
    non-streaming canned response — 100 input, 20 cache_write, 30
    cache_read, 50 output (final cumulative count)."""
    return b"".join(
        [
            _sse_block(
                "message_start",
                {
                    "type": "message_start",
                    "message": {
                        "id": "msg_stream",
                        "type": "message",
                        "role": "assistant",
                        "model": "claude-opus-4-7",
                        "content": [],
                        "usage": {
                            "input_tokens": 100,
                            "cache_creation_input_tokens": 20,
                            "cache_read_input_tokens": 30,
                            "output_tokens": 1,
                        },
                    },
                },
            ),
            _sse_block(
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                },
            ),
            _sse_block(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "hello"},
                },
            ),
            _sse_block(
                "content_block_stop",
                {
                    "type": "content_block_stop",
                    "index": 0,
                },
            ),
            _sse_block(
                "message_delta",
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn"},
                    "usage": {"output_tokens": 50},
                },
            ),
            _sse_block("message_stop", {"type": "message_stop"}),
        ]
    )


async def test_messages_stream_passthrough_records_llm_call(  # type: ignore[no-untyped-def]
    gateway_setup,
    postgres_url,
):
    """Streaming requests get the upstream SSE bytes forwarded verbatim,
    and `_extract_stream_usage` recovers the same token totals the
    non-streaming path records from the final JSON body."""
    app, step_jwt, team_id, trial_id = gateway_setup

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/messages"
        assert request.headers["x-api-key"] == "test-anthropic-key"
        payload = json.loads(request.content)
        assert payload["stream"] is True
        return httpx.Response(
            200,
            content=_canned_anthropic_stream(),
            headers={"content-type": "text/event-stream"},
        )

    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        async with client.stream(
            "POST",
            "/v1/messages",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={
                "model": "claude-opus-4-7",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        ) as r:
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/event-stream")
            body_bytes = b""
            async for chunk in r.aiter_bytes():
                body_bytes += chunk
    assert body_bytes == _canned_anthropic_stream()

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "anthropic"
    assert row["model"] == "claude-opus-4-7"
    assert row["input_tokens"] == 100
    # Final cumulative output_tokens from message_delta (50, not initial 1).
    assert row["output_tokens"] == 50
    assert float(row["cost_usd"]) == pytest.approx(0.00567, abs=1e-6)


async def test_messages_stream_upstream_error_records_failed_audit_row(  # type: ignore[no-untyped-def]
    gateway_setup,
    postgres_url,
):
    """Upstream returns 429 (not an SSE stream); the route must surface
    the error to the client and write a failed-attempt audit row."""
    app, step_jwt, team_id, trial_id = gateway_setup

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": "rate_limited"})

    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/messages",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={
                "model": "claude-opus-4-7",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        )
    assert r.status_code == 429
    assert "rate_limited" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "anthropic"
    assert row["model"] == "claude-opus-4-7"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"] == {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_4xx",
        "_loom_failure_status_code": 429,
        "_loom_usage_status": "missing",
    }
    assert "messages" not in row["request_params"]


async def test_messages_rejects_missing_model(gateway_setup):  # type: ignore[no-untyped-def]
    app, step_jwt, _team_id, _trial_id = gateway_setup
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/messages",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert r.status_code == 400
