"""Gateway completion audit under the actual bootstrapped TLS database role."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest
from sqlalchemy import create_engine, insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom import nebius_platform_bootstrap as bootstrap
from loom.db.schema import RateCard, Task, Team, Token, Trial
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.test_nebius_platform_bootstrap import platform_database  # noqa: F401

pytestmark = pytest.mark.docker


async def test_gateway_completion_records_usage_with_restricted_tls_role(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = request.getfixturevalue("platform_database")
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: database_url)
    monkeypatch.setenv("LOOM_DB_URL", database_url)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + "f" * 64)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + "b" * 64)
    password = "gateway-test-password-" + "x" * 32
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", password)
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_NAMESPACE", "gateway-grants-test")
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    # A repeated migration/bootstrap must preserve these restricted grants.
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})

    team_id, trial_id = uuid4(), uuid4()
    task_id = "gateway-grants-" + uuid4().hex
    token = "loom_team_" + uuid4().hex
    issued_at = datetime.now(UTC)
    admin_engine = create_engine(make_url(database_url).set(drivername="postgresql+psycopg"))
    try:
        with admin_engine.begin() as db:
            db.execute(insert(Team).values(id=team_id, name="gateway-grants"))
            db.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(token.encode()).digest(),
                    type="team",
                    scopes=["llm:call"],
                    team_id=team_id,
                    issued_at=issued_at,
                )
            )
            db.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}, source="test"))
            db.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={},
                    state="running",
                    submitted_at=issued_at,
                )
            )
            db.execute(
                insert(RateCard).values(
                    id="gateway-grants",
                    captured_at=issued_at,
                    table={
                        "id": "gateway-grants",
                        "entries": [
                            {
                                "provider": "openai",
                                "model": "mock-model",
                                "input_per_mtok": 1,
                                "output_per_mtok": 2,
                                "cache_read_per_mtok": 0,
                                "cache_write_per_mtok": 0,
                            }
                        ],
                    },
                )
            )
    finally:
        admin_engine.dispose()

    gateway_url = make_url(database_url).set(username="loom_gateway", password=password)
    monkeypatch.setenv(
        "LOOM_GW_DB_URL",
        gateway_url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False),
    )
    monkeypatch.setenv("LOOM_GW_OPENAI_API_KEY", "mock-upstream-key")
    settings = GatewaySettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory, ttl_sec=settings.rate_card_cache_ttl_sec
    )
    upstream_calls = 0

    async def upstream(**kwargs: Any) -> dict[str, Any]:
        nonlocal upstream_calls
        upstream_calls += 1
        return {
            "id": "mock-response",
            "model": kwargs["model"],
            "choices": [
                {"message": {"role": "assistant", "content": "done"}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
        }

    monkeypatch.setattr("loom_llm_gateway.litellm_wrapper.acompletion", upstream)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://gateway.test",
        ) as client:
            authorities = None
            # First call lazily binds the pre-migration Trial; the second must
            # verify/reuse existing authorities without UPDATE or DELETE.
            for count in (1, 2):
                response = await client.post(
                    "/v1/chat/completions",
                    headers={"Authorization": "Bearer " + token},
                    json={
                        "model": "openai/mock-model",
                        "messages": [{"role": "user", "content": "hello"}],
                        "loom": {
                            "team_id": str(team_id),
                            "trial_id": str(trial_id),
                            "step_id": "main",
                        },
                    },
                )
                assert upstream_calls == count  # A 500 here is AFTER the provider returned.
                assert response.status_code == 200, response.text
                assert response.json()["choices"][0]["message"]["content"] == "done"
                assert response.json()["loom"]["input_tokens"] == 2
                with psycopg.connect(database_url) as db:
                    calls = db.execute(
                        "SELECT input_tokens, output_tokens, cost_usd, lifecycle_authority_id "
                        "FROM llm_calls WHERE trial_id=%s",
                        (trial_id,),
                    ).fetchall()
                    assert len(calls) == count
                    assert all(row[0:2] == (2, 1) and row[2] > 0 and row[3] for row in calls)
                    current = db.execute(
                        "SELECT * FROM data_lifecycle_authorities WHERE owner_id=%s ORDER BY data_class",
                        (str(trial_id),),
                    ).fetchall()
                    assert len(current) == 2
                    if authorities is not None:
                        assert current == authorities
                    authorities = current
                    assert db.execute(
                        "SELECT lifecycle_authority_id IS NOT NULL FROM trials WHERE id=%s",
                        (trial_id,),
                    ).fetchone() == (True,)
    finally:
        await engine.dispose()

    with psycopg.connect(gateway_url.render_as_string(hide_password=False)) as db:
        assert db.execute(
            "SELECT current_user, ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"
        ).fetchone() == ("loom_gateway", True)
        for statement in (
            "UPDATE data_lifecycle_authorities SET pinned=true",
            "UPDATE data_lifecycle_authorities SET expires_at=NULL",
            "UPDATE data_lifecycle_authorities SET state='deleted'",
            "DELETE FROM data_lifecycle_authorities",
            "UPDATE execution_admission_policies SET max_concurrent=1000",
            "DELETE FROM execution_budget_policies",
            "UPDATE tokens SET scopes=ARRAY['admin:tokens']",
            "DELETE FROM users",
            "CREATE ROLE gateway_escape",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                db.execute(statement)
            db.rollback()
