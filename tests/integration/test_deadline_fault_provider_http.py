"""Actual TCP Gateway -> fixture -> trusted PostgreSQL receipt join."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine, insert, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import mint_step_jwt
from loom.db.schema import ProviderConnection, TrialEvent
from loom.deadline_canary import CanaryBinding
from loom_cli.deadline_fault_provider import MODEL, create_fault_app
from loom_llm_gateway.app import create_app
from loom_llm_gateway.deadline_canary_receipts import resolve_receipt
from tests.integration.test_gateway_dispatch_deadline_http import _terminal_receipts
from tests.integration.test_issue_1748_deadline_canary import (
    _TEST_MASTER_KEY,
    _cleanup_gateway,
    _seed_gateway,
    _serve,
)


async def test_real_signed_receipt_join_hold_and_two_retry_completions(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _TEST_MASTER_KEY)
    monkeypatch.setenv("LOOM_GW_LLM_RETRY_MAX_ATTEMPTS", "1")
    provider_key, operator_key = "p" * 40, "o" * 40
    settings, team, trial, connection, task, ref = await _seed_gateway(
        postgres_url=postgres_url,
        base_url="http://127.0.0.1:1/v1",
        provider_key=provider_key,
    )
    binding = CanaryBinding(case="B", team_id=team, provider_connection_id=connection)
    provider = create_fault_app(
        binding,
        provider_key=SecretStr(provider_key),
        operator_key=SecretStr(operator_key),
    )
    db = create_engine(postgres_url)
    async_db = create_async_engine(postgres_url)
    factory = async_sessionmaker(async_db)
    first_attempt, second_attempt = uuid4(), uuid4()
    try:
        async with _serve(provider) as provider_url:
            with db.begin() as conn:
                conn.execute(
                    update(ProviderConnection)
                    .where(ProviderConnection.id == connection)
                    .values(base_url=provider_url + "/v1")
                )
            async with _serve(create_app(settings)) as gateway_url:
                async with httpx.AsyncClient(timeout=15) as client:
                    provider_auth = {"Authorization": "Bearer " + provider_key}
                    operator_auth = {"Authorization": "Bearer " + operator_key}
                    assert (
                        await client.get(provider_url + "/v1/models", headers=provider_auth)
                    ).status_code == 200
                    assert (
                        await client.post(
                            provider_url + "/v1/chat/completions",
                            headers=provider_auth,
                            json={"model": MODEL, "messages": []},
                        )
                    ).status_code == 200
                    assert (
                        await client.post(
                            provider_url + "/operator/arm",
                            headers=operator_auth,
                            json={"trial_id": str(trial), "step_id": "main"},
                        )
                    ).status_code == 200
                    second_deadline = None
                    grants = [uuid4(), uuid4()]
                    for ordinal in range(3):
                        if ordinal == 0:
                            deadline = datetime.now(UTC) + timedelta(seconds=10)
                        else:
                            if second_deadline is None:
                                second_deadline = datetime.now(UTC) + timedelta(seconds=10)
                            deadline = second_deadline
                        token = mint_step_jwt(
                            team_id=team,
                            trial_id=trial,
                            step_id="main",
                            ttl_sec=360,
                            signing_key=settings.step_jwt_signing_key.get_secret_value(),
                            provider_connection_id=connection,
                            attempt_deadline_wall_clock=deadline,
                            agent_attempt_id=first_attempt if ordinal == 0 else second_attempt,
                            step_jwt_id=grants[0 if ordinal == 0 else 1],
                        )
                        pending = asyncio.create_task(
                            client.post(
                                gateway_url + "/openai/v1/chat/completions",
                                headers={
                                    "Authorization": "Bearer " + token,
                                    "X-Request-ID": str(uuid4()),
                                },
                                json={
                                    "model": MODEL,
                                    "messages": [
                                        {"role": "user", "content": "private-canary-prompt"}
                                    ],
                                },
                            )
                        )
                        try:
                            async with asyncio.timeout(2):
                                while True:
                                    snapshot = (
                                        await client.get(
                                            provider_url + "/operator/evidence",
                                            headers=operator_auth,
                                        )
                                    ).json()
                                    waiting = [
                                        r
                                        for r in snapshot["requests"]
                                        if r["outcome"] == "waiting_for_join"
                                    ]
                                    if waiting:
                                        break
                                    await asyncio.sleep(0.01)
                            rid = UUID(waiting[0]["receipt_id"])
                            async with factory() as session:
                                with pytest.raises(ValueError, match="eligible"):
                                    await resolve_receipt(
                                        session,
                                        receipt_id=rid,
                                        team_id=uuid4(),
                                        trial_id=trial,
                                        provider_connection_id=connection,
                                        step_id="main",
                                    )
                                approval = await resolve_receipt(
                                    session,
                                    receipt_id=rid,
                                    team_id=team,
                                    trial_id=trial,
                                    provider_connection_id=connection,
                                    step_id="main",
                                    previous_attempt_id=first_attempt if ordinal else None,
                                )
                            approved = await client.post(
                                provider_url + "/operator/approve",
                                headers=operator_auth,
                                json=approval.model_dump(mode="json"),
                            )
                            assert approved.status_code == 200
                            response = await pending
                        finally:
                            if not pending.done():
                                pending.cancel()
                                await asyncio.gather(pending, return_exceptions=True)
                        if ordinal == 0:
                            assert response.status_code == 504
                            # This transport test seeds supervisor evidence explicitly;
                            # it is not a live worker or complete Case B acceptance.
                            with db.begin() as conn:
                                conn.execute(
                                    insert(TrialEvent).values(
                                        trial_id=trial,
                                        seq=1,
                                        kind="agent_timeout",
                                        source="worker",
                                        payload={
                                            "agent_attempt_id": str(first_attempt),
                                            "task_stopped": True,
                                            "configured_timeout_sec": 10,
                                        },
                                    )
                                )
                        else:
                            assert response.status_code == 200, response.text
                            parsed = json.loads(response.json()["choices"][0]["message"]["content"])
                            assert parsed["task_complete"] is True and parsed["commands"] == []
                    rows = await _terminal_receipts(postgres_url, trial)
                    assert len(rows) == 3
                    assert [r["agent_attempt_id"] for r in rows] == [
                        first_attempt,
                        second_attempt,
                        second_attempt,
                    ]
                    assert [r["gateway_outcome"] for r in rows] == [
                        "deadline",
                        "completed",
                        "completed",
                    ]
                    snapshot = (
                        await client.get(provider_url + "/operator/evidence", headers=operator_auth)
                    ).json()
                    assert snapshot["discovery_request_count"] == 2
                    assert snapshot["rejected_request_count"] == 0
                    assert snapshot["full_canary_passed"] is False
                    serialized = json.dumps(snapshot)
                    assert all(
                        secret not in serialized
                        for secret in (
                            provider_key,
                            operator_key,
                            token,
                            provider_url,
                            "private-canary-prompt",
                        )
                    )
                    await client.post(provider_url + "/operator/close", headers=operator_auth)
    finally:
        db.dispose()
        await async_db.dispose()
        _cleanup_gateway(
            postgres_url=postgres_url, team_id=team, trial_id=trial, task_id=task, secret_ref=ref
        )
