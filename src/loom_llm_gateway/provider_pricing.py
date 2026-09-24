"""One price resolver for gateway calls and pre-run budget estimates."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PriceCatalog, ProviderConnection
from loom.provider_pricing import ModelPrice, PricingConfig, calculate_price
from loom_llm_gateway.dialect import USAGE_STATUS_KEY, TokenUsage
from loom_llm_gateway.rate_card import CostEstimate


async def configured_cost(
    session: AsyncSession | None,
    row: ProviderConnection,
    model: str,
    usage: TokenUsage,
) -> CostEstimate:
    config = PricingConfig.model_validate(row.pricing_config)
    source = {"custom": "operator-supplied", "catalog": "rate-card", "usage_only": "tokens-only"}[
        config.pricing_mode
    ]

    def unknown(reason: str) -> CostEstimate:
        return CostEstimate(
            0,
            "facade:price-unknown",
            "unpriced",
            "unavailable",
            None,
            pricing_source=source,
            unpriced_reason=reason,
        )

    if config.pricing_mode == "usage_only":
        return CostEstimate(
            0, "facade:tokens-only", "tokens-only", "not_applicable", None, pricing_source=source
        )
    if usage.provider_extras.get("_loom_unsupported_billing"):
        return unknown("unsupported_billing_dimensions")
    if usage.provider_extras.get(USAGE_STATUS_KEY) in ("missing", "partial"):
        return unknown("incomplete_usage")
    if usage.provider_extras.get("_loom_cache_usage_known") is False:
        return unknown("missing_cache_usage")
    price: ModelPrice | None = None
    basis: dict[str, Any] = {
        "mode": config.pricing_mode,
        "model": model,
        "currency": "USD",
        "unit": "usd_per_1m_tokens",
    }
    if config.pricing_mode == "custom":
        price = config.price_for(model)
    elif session is not None:
        catalog = await session.get(PriceCatalog, config.catalog_id)
        if catalog is None or catalog.team_id not in (None, row.team_id):
            return unknown("catalog_unavailable")
        canonical = catalog.aliases.get(model, model)
        raw = catalog.prices.get(canonical)
        if raw is not None:
            try:
                price = ModelPrice.model_validate(raw)
            except ValidationError:
                return unknown("invalid_catalog_price")
        basis.update(
            catalog_id=catalog.id,
            supplier_id=catalog.supplier_id,
            revision=catalog.revision,
            source_model=canonical,
            source_url=catalog.source_url,
            updated_at=catalog.updated_at.isoformat() if catalog.updated_at else None,
            supplier_metadata=dict(catalog.source_metadata or {}),
        )
    if price is None:
        return unknown("missing_model_price")
    includes_cache = usage.provider_extras.get("_loom_input_includes_cache", False)
    cost = calculate_price(
        price,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cache_read_tokens=usage.cached_input_tokens,
        cache_write_tokens=usage.cache_write_tokens,
        input_includes_cache=bool(includes_cache),
    )
    if cost is None:
        return unknown("incomplete_billing_dimensions")
    basis["prices"] = price.model_dump(mode="json")
    basis["input_includes_cache"] = includes_cache
    digest = hashlib.sha256(json.dumps(basis, sort_keys=True).encode()).hexdigest()
    return CostEstimate(
        cost,
        digest,
        source,
        "configured",
        "USD",
        pricing_source=source,
        rate_card_provider=config.supplier_id,
        price_basis=basis,
    )
