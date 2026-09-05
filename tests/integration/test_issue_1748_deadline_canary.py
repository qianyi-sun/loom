from __future__ import annotations

import asyncio
import base64
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from scripts.ops.issue_1748_deadline_canary import (
    FaultProviderConfig,
    FaultProviderState,
    build_local_transport_evidence,
    create_fault_provider_app,
    validate_local_transport_evidence,
)
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt, verify_step_jwt
from loom.db.schema import LlmCall, ProviderConnection, Secret, Team
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from tests.integration.gateway_db import (
    delete_gateway_trial,
    delete_team_and_quota,
    insert_gateway_trial,
)

_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()
_FIXTURE_CANDIDATE_SHA = "a" * 40
_FIXTURE_CANDIDATE_TREE = "b" * 40


@asynccontextmanager
async def _serve(app: object) -> AsyncIterator[str]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    host, port = sock.getsockname()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="error",
            access_log=False,
            lifespan="on",
        )
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    for _ in range(400):
        if server.started:
            break
        if task.done():
            await task
        await asyncio.sleep(0.005)
    else:
        raise AssertionError("test HTTP server did not start")
    try:
        yield f"http://{host}:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=10)


async def _seed_gateway(
    *,
    postgres_url: str,
    base_url: str,
) -> tuple[GatewaySettings, UUID, UUID, UUID, str, str]:
    settings = GatewaySettings(_env_file=None)
    team_id = uuid4()
    trial_id = uuid4()
    connection_id = uuid4()

    async_engine = create_async_engine(postgres_url)
    async_session_factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with async_session_factory() as session:
        ref = await LocalEncryptedSecretStore(session).put(
            namespace=f"team:{team_id}",
            value="local-placeholder-provider-key",
        )
        await session.commit()
    await async_engine.dispose()

    engine = create_engine(postgres_url)
    with engine.begin() as session:
        session.execute(insert(Team).values(id=team_id, name=f"deadline-{team_id}"))
        task_id = insert_gateway_trial(session, team_id=team_id, trial_id=trial_id)
        session.execute(
            insert(ProviderConnection).values(
                id=connection_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name=f"deadline-{connection_id}",
                base_url=base_url,
                upstream_host="127.0.0.1",
                resolved_egress_ips=["127.0.0.1"],
                encrypted_api_key_ref=ref,
                pricing_source="tokens-only",
                created_by="issue-1748-local-test",
                responses_api_supported=True,
                responses_api_probed_at=datetime.now(UTC),
            )
        )
    engine.dispose()
    return settings, team_id, trial_id, connection_id, task_id, ref


def _cleanup_gateway(
    *,
    postgres_url: str,
    team_id: UUID,
    trial_id: UUID,
    task_id: str,
    secret_ref: str,
) -> None:
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    with session_local() as session:
        session.execute(delete(LlmCall).where(LlmCall.trial_id == trial_id))
        delete_gateway_trial(session, trial_id=trial_id, task_id=task_id)
        session.execute(delete(ProviderConnection).where(ProviderConnection.team_id == team_id))
        session.execute(delete(Secret).where(Secret.ref == secret_ref))
        delete_team_and_quota(session, team_id)
        session.commit()
    engine.dispose()


@pytest.mark.parametrize("case", ["A", "B"])
async def test_real_http_gateway_deadline_canary_transport(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    case: str,
) -> None:
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _TEST_MASTER_KEY)
    monkeypatch.setenv("LOOM_GW_LLM_RETRY_MAX_ATTEMPTS", "1")

    repo_root = Path(__file__).resolve().parents[2]
    # Synthetic bindings isolate transport behavior. The manual CLI separately
    # binds evidence to the exact clean candidate checkout.
    candidate_sha = _FIXTURE_CANDIDATE_SHA
    candidate_tree = _FIXTURE_CANDIDATE_TREE
    trial_id = uuid4()
    config = FaultProviderConfig(
        case=case,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        trial_id=trial_id,
        step_id="main",
        nonce=uuid4().hex,
        deadline_budget_sec=0.25,
        hold_sec=0.75,
    )
    state = FaultProviderState(config)

    async with _serve(create_fault_provider_app(state)) as provider_origin:
        provider_base_url = f"{provider_origin}/{config.nonce}/v1"
        (
            settings,
            team_id,
            seeded_trial_id,
            connection_id,
            task_id,
            secret_ref,
        ) = await _seed_gateway(
            postgres_url=postgres_url,
            base_url=provider_base_url,
        )
        config = config.model_copy(update={"trial_id": seeded_trial_id})
        state.rebind(config)
        gateway = create_app(settings)
        try:
            async with _serve(gateway) as gateway_url:
                outcomes: list[dict[str, object]] = []
                deadline = datetime.now(UTC) + timedelta(seconds=0.25)
                first_jwt = mint_step_jwt(
                    team_id=team_id,
                    trial_id=seeded_trial_id,
                    step_id="main",
                    ttl_sec=360,
                    signing_key=settings.step_jwt_signing_key.get_secret_value(),
                    provider_connection_id=connection_id,
                    attempt_deadline_wall_clock=deadline,
                )
                first_context = verify_step_jwt(
                    first_jwt,
                    signing_key=settings.step_jwt_signing_key.get_secret_value(),
                )
                assert first_context.attempt_deadline_wall_clock is not None
                assert first_context.expires_at is not None
                async with httpx.AsyncClient(base_url=gateway_url, timeout=5) as client:
                    first_started_at = datetime.now(UTC)
                    first_count_before = len(state.snapshot()["requests"])
                    first = await client.post(
                        "/v1/responses",
                        headers={"Authorization": f"Bearer {first_jwt}"},
                        json={"model": "canary-model", "input": "local deadline canary"},
                    )
                    first_received_at = datetime.now(UTC)
                    first_count_after = len(state.snapshot()["requests"])
                    first_detail = first.json()["detail"]
                    outcomes.append(
                        {
                            "phase": "initial_deadline",
                            "case_attempt_ordinal": 1,
                            "http_status": first.status_code,
                            "detail_code": first_detail["code"],
                            "detail_reason": first_detail["reason"],
                            "request_started_at": first_started_at.isoformat(),
                            "response_received_at": first_received_at.isoformat(),
                            "signed_deadline_wall_clock": (
                                first_context.attempt_deadline_wall_clock.isoformat()
                            ),
                            "grant_expires_at": first_context.expires_at.isoformat(),
                            "provider_request_count_before": first_count_before,
                            "provider_request_count_after": first_count_after,
                        }
                    )
                    assert first.status_code == 504
                    assert first_count_after == 1

                    replay_count_before = len(state.snapshot()["requests"])
                    replay_started_at = datetime.now(UTC)
                    expired_replay = await client.post(
                        "/v1/responses",
                        headers={"Authorization": f"Bearer {first_jwt}"},
                        json={"model": "canary-model", "input": "expired replay"},
                    )
                    replay_received_at = datetime.now(UTC)
                    replay_count_after = len(state.snapshot()["requests"])
                    replay_detail = expired_replay.json()["detail"]
                    outcomes.append(
                        {
                            "phase": "expired_deadline_replay",
                            "case_attempt_ordinal": 1,
                            "http_status": expired_replay.status_code,
                            "detail_code": replay_detail["code"],
                            "detail_reason": replay_detail["reason"],
                            "request_started_at": replay_started_at.isoformat(),
                            "response_received_at": replay_received_at.isoformat(),
                            "signed_deadline_wall_clock": (
                                first_context.attempt_deadline_wall_clock.isoformat()
                            ),
                            "grant_expires_at": first_context.expires_at.isoformat(),
                            "provider_request_count_before": replay_count_before,
                            "provider_request_count_after": replay_count_after,
                        }
                    )
                    assert expired_replay.status_code == 504
                    assert replay_count_after == replay_count_before == 1

                    if case == "B":
                        fresh_deadline = datetime.now(UTC) + timedelta(seconds=2)
                        second_jwt = mint_step_jwt(
                            team_id=team_id,
                            trial_id=seeded_trial_id,
                            step_id="main",
                            ttl_sec=360,
                            signing_key=settings.step_jwt_signing_key.get_secret_value(),
                            provider_connection_id=connection_id,
                            attempt_deadline_wall_clock=fresh_deadline,
                        )
                        assert second_jwt != first_jwt
                        second_context = verify_step_jwt(
                            second_jwt,
                            signing_key=settings.step_jwt_signing_key.get_secret_value(),
                        )
                        assert second_context.attempt_deadline_wall_clock is not None
                        assert second_context.expires_at is not None
                        assert (
                            second_context.attempt_deadline_wall_clock
                            != first_context.attempt_deadline_wall_clock
                        )
                        second_count_before = len(state.snapshot()["requests"])
                        second_started_at = datetime.now(UTC)
                        second = await client.post(
                            "/v1/responses",
                            headers={"Authorization": f"Bearer {second_jwt}"},
                            json={"model": "canary-model", "input": "fresh deadline grant"},
                        )
                        second_received_at = datetime.now(UTC)
                        second_count_after = len(state.snapshot()["requests"])
                        outcomes.append(
                            {
                                "phase": "fresh_deadline_grant",
                                "case_attempt_ordinal": 2,
                                "http_status": second.status_code,
                                "detail_code": None,
                                "detail_reason": None,
                                "request_started_at": second_started_at.isoformat(),
                                "response_received_at": second_received_at.isoformat(),
                                "signed_deadline_wall_clock": (
                                    second_context.attempt_deadline_wall_clock.isoformat()
                                ),
                                "grant_expires_at": second_context.expires_at.isoformat(),
                                "provider_request_count_before": second_count_before,
                                "provider_request_count_after": second_count_after,
                            }
                        )
                        assert second.status_code == 200, second.text
                        assert second_count_after == second_count_before + 1 == 2

                await asyncio.sleep(config.hold_sec + 0.1)

            evidence = build_local_transport_evidence(
                config=config,
                provider_snapshot=state.snapshot(),
                gateway_outcomes=outcomes,
                harness_path=repo_root / "scripts/ops/issue_1748_deadline_canary.py",
                gateway_route="/v1/responses",
            )
            validate_local_transport_evidence(evidence)
            assert evidence["transport_assertions"]["provider_request_count"] == (
                1 if case == "A" else 2
            )
            assert evidence["transport_assertions"]["post_deadline_attempt_1_dispatch_count"] == 0
            assert evidence["transport_assertions"]["fresh_deadline_grant_completed"] is (
                case == "B"
            )
            assert evidence["full_canary_passed"] is False
        finally:
            _cleanup_gateway(
                postgres_url=postgres_url,
                team_id=team_id,
                trial_id=seeded_trial_id,
                task_id=task_id,
                secret_ref=secret_ref,
            )
