"""Successful provider responses survive pricing failures under the deployed DB role."""

from uuid import uuid4

import httpx
import psycopg
import pytest
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import GatewayDispatchReceipt, LlmCall, PriceCatalog, ProviderConnection
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.test_gateway_facade_openai import facade_setup as facade_setup
from tests.integration.test_nebius_gateway_privileges import gateway_database  # noqa: F401
from tests.integration.test_nebius_platform_bootstrap import platform_database  # noqa: F401

pytestmark = pytest.mark.docker


@pytest.fixture
def postgres_url(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    database_url, _ = request.getfixturevalue("gateway_database")
    # Match the shared facade fixture's scoped lifecycle cleanup.
    monkeypatch.setenv("LOOM_NAMESPACE", "loom")
    return make_url(database_url).set(drivername="postgresql+psycopg").render_as_string(
        hide_password=False,
    )


@pytest.mark.parametrize("catalog_access", [True, False], ids=["priced", "lookup-denied"])
async def test_catalog_response_and_usage_survive_restricted_role(
    facade_setup, request: pytest.FixtureRequest, catalog_access: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, jwt, _, trial_id, connection_id, captures = facade_setup
    upstream_body = captures["response"].json()
    upstream_body["usage"]["prompt_tokens_details"] = {"cached_tokens": 0}
    captures["response"] = httpx.Response(200, json=upstream_body)
    database_url, password = request.getfixturevalue("gateway_database")
    owners = app.state.session_factory
    catalog_id = "test:" + uuid4().hex
    async with owners() as session:
        session.add(PriceCatalog(
            id=catalog_id, name="Gateway regression", revision=1,
            prices={"gpt-4o": {"input_usd_per_1m": 5, "output_usd_per_1m": 15}},
        ))
        connection = await session.get(ProviderConnection, connection_id)
        connection.pricing_config = {"pricing_mode": "catalog", "catalog_id": catalog_id}
        await session.commit()

    if not catalog_access:
        with psycopg.connect(database_url) as db:
            db.execute("REVOKE SELECT ON price_catalogs FROM loom_gateway")
    gateway_url = make_url(database_url).set(
        drivername="postgresql+psycopg", username="loom_gateway", password=password,
    )
    engine = create_async_engine(gateway_url)
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory, ttl_sec=60,
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://gateway.test",
        ) as client:
            response = await client.post(
                "/openai/v1/chat/completions",
                headers={
                    "Authorization": "Bearer " + jwt,
                    "x-loom-provider-connection-id": str(connection_id),
                },
                json={"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]},
            )
        assert len(captures["requests"]) == 1
        assert response.status_code == 200, response.text
        assert response.json() == captures["response"].json()
        async with owners() as session:
            call = (await session.scalars(select(LlmCall).where(
                LlmCall.trial_id == trial_id,
            ))).one()
            assert (call.input_tokens, call.output_tokens) == (100, 50)
            receipt = (await session.scalars(select(GatewayDispatchReceipt).where(
                GatewayDispatchReceipt.trial_id == trial_id,
            ))).one()
            assert (receipt.provider_http_status, receipt.gateway_http_status) == (200, 200)
            assert receipt.gateway_outcome == "completed"
            if catalog_access:
                assert float(call.cost_usd) == pytest.approx(0.00125)
                assert call.provider_extras["_loom_price_basis"]["catalog_id"] == catalog_id
            else:
                assert call.provider_extras["_loom_cost_source"] == "unpriced"
                assert call.provider_extras["_loom_cost_confidence"] == "unavailable"
                assert call.provider_extras["_loom_unpriced_reason"] == "catalog_lookup_failed"
                assert "catalog_lookup_failed" in caplog.text
                assert "SELECT price_catalogs" not in caplog.text
        with psycopg.connect(gateway_url.set(drivername="postgresql").render_as_string(
            hide_password=False,
        )) as db:
            for statement in (
                "UPDATE price_catalogs SET revision=revision+1 WHERE false",
                "DELETE FROM price_catalogs WHERE false",
                "INSERT INTO price_catalogs (id,name) VALUES ('escape','escape')",
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    db.execute(statement)
                db.rollback()
    finally:
        await engine.dispose()
