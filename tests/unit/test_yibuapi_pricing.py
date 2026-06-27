from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.rate_card import RateCardTable
from loom_llm_gateway.routes._facade_common import compute_facade_cost_usd
from loom_llm_gateway.yibuapi_pricing import (
    DEFAULT_YIBUAPI_PRICING_URL,
    build_yibuapi_rate_card,
    normalize_yibuapi_model_name,
)


def test_build_yibuapi_rate_card_converts_ratio_fields_to_usd_per_mtok() -> None:
    payload = {
        "success": True,
        "pricing_version": "pricing-v1",
        "group_ratio": {"default": 1},
        "data": [
            {
                "model_name": "qwen3.6-35b-a3b",
                "quota_type": 0,
                "model_ratio": 0.36,
                "completion_ratio": 6,
                "cache_ratio": 0.25,
                "create_cache_ratio": 0.5,
                "pricing_version": "model-v1",
            },
        ],
    }

    card = build_yibuapi_rate_card(
        payload,
        source_url=DEFAULT_YIBUAPI_PRICING_URL,
        fetched_at=datetime(2026, 6, 27, tzinfo=UTC),
    )

    assert card["id"] == "yibuapi-pricing-v1"
    assert card["provider"] == "yibuapi"
    assert card["source_url"] == DEFAULT_YIBUAPI_PRICING_URL
    assert card["pricing_version"] == "pricing-v1"
    assert card["currency"] == "USD"
    assert card["last_checked_at"] == "2026-06-27T00:00:00+00:00"
    assert card["entries"] == [
        {
            "provider": "yibuapi",
            "model": "qwen3.6-35b-a3b",
            "input_per_mtok": 0.72,
            "output_per_mtok": 4.32,
            "cache_read_per_mtok": 0.18,
            "cache_write_per_mtok": 0.36,
            "currency": "USD",
            "source_url": DEFAULT_YIBUAPI_PRICING_URL,
            "pricing_version": "model-v1",
            "source_model": "qwen3.6-35b-a3b",
            "pricing_unit": "usd_per_1m_tokens",
        },
    ]


def test_build_yibuapi_rate_card_uses_group_ratio_and_top_level_version() -> None:
    payload = {
        "success": True,
        "pricing_version": "pricing-v2",
        "group_ratio": {"default": 1, "codex": 0.5},
        "data": [
            {
                "model_name": "gpt-4o-mini",
                "quota_type": 0,
                "model_ratio": 0.075,
                "completion_ratio": 4,
                "cache_ratio": 0.5,
            },
        ],
    }

    card = build_yibuapi_rate_card(
        payload,
        source_url=DEFAULT_YIBUAPI_PRICING_URL,
        fetched_at=datetime(2026, 6, 27, tzinfo=UTC),
        group="codex",
    )

    entry = card["entries"][0]
    assert entry["input_per_mtok"] == 0.075
    assert entry["output_per_mtok"] == 0.3
    assert entry["cache_read_per_mtok"] == 0.0375
    assert entry["cache_write_per_mtok"] == 0
    assert entry["pricing_version"] == "pricing-v2"


def test_build_yibuapi_rate_card_skips_non_token_or_dynamic_models() -> None:
    payload = {
        "success": True,
        "pricing_version": "pricing-v3",
        "data": [
            {
                "model_name": "fixed-price-api",
                "quota_type": 1,
                "model_ratio": 0,
                "model_price": 0.0005,
            },
            {
                "model_name": "dynamic-api",
                "quota_type": 0,
                "billing_mode": "tiered_expr",
                "billing_expr": "p=1",
                "model_ratio": 1,
                "completion_ratio": 1,
            },
        ],
    }

    card = build_yibuapi_rate_card(
        payload,
        source_url=DEFAULT_YIBUAPI_PRICING_URL,
        fetched_at=datetime(2026, 6, 27, tzinfo=UTC),
    )

    assert card["entries"] == []
    assert card["skipped_model_count"] == 2


def test_normalize_yibuapi_model_name_strips_common_provider_prefixes() -> None:
    assert normalize_yibuapi_model_name("yibuapi/qwen3.6-35b-a3b") == ("qwen3.6-35b-a3b")
    assert normalize_yibuapi_model_name("models/qwen3.6-35b-a3b") == ("qwen3.6-35b-a3b")
    assert normalize_yibuapi_model_name("qwen3.6-35b-a3b") == ("qwen3.6-35b-a3b")


class _Cache:
    def __init__(self, table: RateCardTable) -> None:
        self.table = table

    async def get(self) -> RateCardTable:
        return self.table


@pytest.mark.asyncio
async def test_facade_yibuapi_lookup_normalizes_model_prefix() -> None:
    table = RateCardTable(
        id="yibuapi-pricing-v1",
        captured_at=datetime(2026, 6, 27, tzinfo=UTC),
        provider="yibuapi",
        source_url=DEFAULT_YIBUAPI_PRICING_URL,
        pricing_version="pricing-v1",
        entries=[
            {
                "provider": "yibuapi",
                "model": "qwen3.6-35b-a3b",
                "input_per_mtok": 0.72,
                "output_per_mtok": 4.32,
                "cache_read_per_mtok": 0.0,
                "cache_write_per_mtok": 0.0,
            }
        ],
    )

    cost, rate_card_hash = await compute_facade_cost_usd(
        SimpleNamespace(
            pricing_source="rate-card",
            rate_card_provider="yibuapi",
            provider_type="openai-compatible",
        ),
        "yibuapi/qwen3.6-35b-a3b",
        TokenUsage(input_tokens=1_000_000, output_tokens=1_000_000),
        rate_card_cache=_Cache(table),
    )

    assert cost == pytest.approx(5.04)
    assert rate_card_hash != "facade:rate-card:missing"
