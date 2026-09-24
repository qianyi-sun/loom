"""Provider pricing contract: exclusive modes, exact models, truthful estimates."""

import pytest
from pydantic import ValidationError

from loom.provider_pricing import ModelPrice, PricingConfig, calculate_price, parse_price_import


def test_models_are_independent_and_zero_is_a_price():
    config = PricingConfig(
        pricing_mode="custom",
        custom_pricing={
            "model-a": {"input_usd_per_1m": 0, "output_usd_per_1m": 0},
        },
    )
    assert config.price_for("model-a") is not None
    assert config.price_for("model-b") is None
    assert config.price_for("models/model-a") is None


@pytest.mark.parametrize("value", [-1, float("inf"), float("nan"), True, "bad"])
def test_invalid_price_rejected(value):
    with pytest.raises(ValidationError):
        ModelPrice(input_usd_per_1m=value, output_usd_per_1m=1)


def test_base_prices_required_and_cache_is_optional():
    with pytest.raises(ValidationError):
        ModelPrice(input_usd_per_1m=1)
    price = ModelPrice(input_usd_per_1m=1, output_usd_per_1m=2)
    assert price.cache_read_usd_per_1m is None
    assert calculate_price(price, input_tokens=100, output_tokens=10, cache_read_tokens=30) is None


def test_explicit_cache_overlap_avoids_double_billing():
    price = ModelPrice(
        input_usd_per_1m=10, output_usd_per_1m=20, cache_read_usd_per_1m=1, cache_write_usd_per_1m=5
    )
    assert (
        calculate_price(
            price,
            input_tokens=1000000,
            output_tokens=0,
            cache_read_tokens=400000,
            input_includes_cache=True,
        )
        == 6.4
    )
    assert (
        calculate_price(
            price,
            input_tokens=600000,
            output_tokens=0,
            cache_read_tokens=400000,
            input_includes_cache=False,
        )
        == 6.4
    )


@pytest.mark.parametrize("mode", ["usage_only", "catalog"])
def test_no_mixed_catalog_and_custom(mode):
    with pytest.raises(ValidationError):
        PricingConfig(
            pricing_mode=mode,
            catalog_id="supplier:yibuapi" if mode == "catalog" else None,
            custom_pricing={"a": {"input_usd_per_1m": 1, "output_usd_per_1m": 1}},
        )


def test_import_rejects_duplicates_and_invalid_units_atomically():
    with pytest.raises(ValueError, match="columns"):
        parse_price_import("model,price,currency\n", "csv")
    with pytest.raises(ValueError, match="duplicate"):
        parse_price_import("model,input_usd_per_1m,output_usd_per_1m\na,1,2\na,3,4", "csv")
    with pytest.raises(ValueError):
        parse_price_import(
            '[{"model":"a","input_usd_per_1m":1,"output_usd_per_1m":2,"currency":"CNY"}]', "json"
        )
    assert (
        parse_price_import("model,input_usd_per_1m,output_usd_per_1m\na,0,2", "csv")[
            "a"
        ].input_usd_per_1m
        == 0
    )


def test_supplier_group_and_missing_cache_are_not_guessed():
    from loom_service.price_catalogs import parse_supplier_prices

    payload = {
        "success": True,
        "group_ratio": {"default": 2},
        "data": [
            {
                "model_name": "a",
                "enable_groups": ["default"],
                "quota_type": 0,
                "model_ratio": 0.5,
                "completion_ratio": 3,
            },
            {
                "model_name": "b",
                "enable_groups": ["other"],
                "quota_type": 0,
                "model_ratio": 1,
                "completion_ratio": 2,
            },
        ],
    }
    parsed = parse_supplier_prices(payload, supplier="yibuapi")
    assert set(parsed.prices) == {"a"}
    assert parsed.prices["a"].input_usd_per_1m == 2
    assert parsed.prices["a"].cache_read_usd_per_1m is None
    payload["data"][0]["model_ratio"] = float("inf")
    with pytest.raises(ValueError):
        parse_supplier_prices(payload, supplier="yibuapi")


@pytest.mark.asyncio
async def test_custom_resolution_and_snapshot_do_not_fall_back():
    from types import SimpleNamespace

    from loom_llm_gateway.dialect import DIALECTS
    from loom_llm_gateway.provider_pricing import configured_cost

    row = SimpleNamespace(
        pricing_config={
            "pricing_mode": "custom",
            "custom_pricing": {
                "a": {"input_usd_per_1m": 10, "output_usd_per_1m": 20, "cache_read_usd_per_1m": 1},
            },
        }
    )
    usage = DIALECTS["openai_chat"].extract_tokens(
        {
            "usage": {
                "prompt_tokens": 1000000,
                "completion_tokens": 0,
                "prompt_tokens_details": {"cached_tokens": 400000},
            }
        }
    )
    estimate = await configured_cost(None, row, "a", usage)
    assert estimate.cost_usd == 6.4
    missing = await configured_cost(None, row, "b", usage)
    assert missing.source == "unpriced"
    assert missing.unpriced_reason == "missing_model_price"
    row.pricing_config["custom_pricing"]["a"]["input_usd_per_1m"] = 99
    assert estimate.provider_extras()["_loom_price_basis"]["prices"]["input_usd_per_1m"] == 10
    assert (await configured_cost(None, row, "a", usage)).rate_card_hash != estimate.rate_card_hash


@pytest.mark.asyncio
async def test_catalog_uses_exact_identity_or_declared_alias_and_owner_scope():
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from loom_llm_gateway.dialect import TokenUsage
    from loom_llm_gateway.provider_pricing import configured_cost

    team = uuid4()
    catalog = SimpleNamespace(
        id="team:test",
        team_id=team,
        supplier_id=None,
        revision=1,
        aliases={"alias-a": "a"},
        prices={"a": {"input_usd_per_1m": 1, "output_usd_per_1m": 2}},
        source_url=None,
        updated_at=datetime.now(UTC),
        source_metadata={},
    )
    session = SimpleNamespace(get=AsyncMock(return_value=catalog))
    row = SimpleNamespace(
        team_id=team, pricing_config={"pricing_mode": "catalog", "catalog_id": catalog.id}
    )
    usage = TokenUsage(1000000, 0)
    assert (await configured_cost(session, row, "alias-a", usage)).cost_usd == 1
    assert (await configured_cost(session, row, "models/a", usage)).source == "unpriced"
    assert (await configured_cost(session, row, "b", usage)).source == "unpriced"
    row.team_id = uuid4()
    assert (
        await configured_cost(session, row, "a", usage)
    ).unpriced_reason == "catalog_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,model,cost", [("custom", "a", 2), ("custom", "b", None), ("usage_only", "a", None)]
)
async def test_pre_run_budget_uses_same_model_price_and_preserves_unknown(mode, model, cost):
    from types import SimpleNamespace

    from loom_service.usage_accounting import estimate_pre_run_batch_budget

    config = {"pricing_mode": mode}
    if mode == "custom":
        config["custom_pricing"] = {"a": {"input_usd_per_1m": 2, "output_usd_per_1m": 4}}
    estimate = await estimate_pre_run_batch_budget(
        None,
        provider_connection=SimpleNamespace(pricing_config=config),
        provider_model_id=model,
        expected_trial_count=1,
        settings=SimpleNamespace(),
        budget_usd=1,
        budget_policy="hard",
    )
    assert estimate.pre_run_estimated_cost_usd == cost
    if cost is None:
        assert estimate.cost_estimate_confidence != "configured"


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect,response", [
    ("anthropic", {"usage": {"input_tokens": 10, "output_tokens": 2, "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 5, "cache_creation": {"ephemeral_1h_input_tokens": 5}}}),
    ("gemini", {"usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 2,
        "cachedContentTokenCount": 0, "thoughtsTokenCount": 20}}),
    ("gemini", {"usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 2,
        "cachedContentTokenCount": 0, "promptTokensDetails": [{"modality": "AUDIO", "tokenCount": 5}]}}),
])
async def test_unsupported_token_dimensions_are_not_silently_omitted(dialect, response):
    from types import SimpleNamespace

    from loom_llm_gateway.dialect import DIALECTS
    from loom_llm_gateway.provider_pricing import configured_cost

    row = SimpleNamespace(pricing_config={"pricing_mode": "custom", "custom_pricing": {
        "a": {"input_usd_per_1m": 1, "output_usd_per_1m": 2, "cache_write_usd_per_1m": 3},
    }})
    cost = await configured_cost(None, row, "a", DIALECTS[dialect].extract_tokens(response))
    assert cost.source == "unpriced"
    assert cost.unpriced_reason == "unsupported_billing_dimensions"
