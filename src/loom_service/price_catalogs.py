"""Public supplier catalogs and team-private prices; atomic publication."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PriceCatalog
from loom.provider_pricing import ModelPrice, validate_model_id

logger = logging.getLogger(__name__)


class CatalogPrices(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prices: dict[str, ModelPrice]
    aliases: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_identities(self) -> CatalogPrices:
        for model in self.prices:
            validate_model_id(model)
        for alias, model in self.aliases.items():
            validate_model_id(alias)
            if model not in self.prices or alias in self.prices:
                raise ValueError(
                    "aliases must point directly to a priced model and cannot shadow one"
                )
        return self


class SupplierPriceSnapshot(CatalogPrices):
    source_metadata: dict[str, Any] = Field(default_factory=dict)


def catalog_response(row: PriceCatalog) -> dict[str, Any]:
    return {
        "id": row.id,
        "team_id": row.team_id,
        "name": row.name,
        "supplier_id": row.supplier_id,
        "source_url": row.source_url,
        "currency": "USD",
        "unit": "usd_per_1m_tokens",
        "prices": row.prices,
        "aliases": row.aliases,
        "revision": row.revision,
        "source_metadata": row.source_metadata,
        "updated_at": row.updated_at,
        "checked_at": row.checked_at,
        "sync_error": row.sync_error,
        "stale": row.supplier_id is not None
        and (
            row.updated_at is None
            or datetime.now(UTC) - row.updated_at > timedelta(hours=25)
            or row.sync_error is not None
        ),
    }


SUPPLIER_SOURCES = {
    "yibuapi": "https://yibuapi.com/api/pricing",
    "az-gptplus5": "https://az.gptplus5.com/api/pricing",
}


def parse_supplier_prices(
    raw: Any, *, supplier: str, group: str = "default"
) -> SupplierPriceSnapshot:
    """Both initial suppliers publish New API token ratios (base USD 2/M).

    The selected group is explicit. Never use a cheapest/automatic group or
    infer a purchase group from the API protocol. Unsupported billing entries
    stay unpriced; malformed supported entries reject the entire update.
    """
    if not isinstance(raw, dict) or not isinstance(raw.get("data"), list):
        raise ValueError("invalid supplier response")
    if raw.get("success") is False:
        raise ValueError("supplier rejected price request")
    if raw.get("currency", "USD") != "USD":
        raise ValueError("supplier currency is unsupported; no automatic conversion")
    groups = raw.get("group_ratio")
    if not isinstance(groups, dict) or group not in groups:
        raise ValueError("selected supplier group has no published ratio")
    from math import isfinite

    def ratio(value: Any) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value < 0
        ):
            raise ValueError("supplier ratios must be finite nonnegative numbers")
        return float(value)

    group_ratio = ratio(groups[group])
    prices: dict[str, ModelPrice] = {}
    for entry in raw["data"]:
        if not isinstance(entry, dict):
            raise ValueError("invalid supplier entry")
        if (
            entry.get("quota_type") != 0
            or entry.get("billing_expr")
            or entry.get("billing_mode") == "tiered_expr"
        ):
            continue
        if not isinstance(entry.get("enable_groups"), list):
            raise ValueError("supplier entry has no valid group list")
        if group not in entry["enable_groups"]:
            continue
        model = validate_model_id(entry.get("model_name", ""))
        if model in prices:
            raise ValueError("duplicate supplier model")
        base = ratio(entry.get("model_ratio")) * 2 * group_ratio
        prices[model] = ModelPrice(
            input_usd_per_1m=base,
            output_usd_per_1m=base * ratio(entry.get("completion_ratio")),
            cache_read_usd_per_1m=base * ratio(entry["cache_ratio"])
            if entry.get("cache_ratio") is not None
            else None,
            cache_write_usd_per_1m=base * ratio(entry["create_cache_ratio"])
            if entry.get("create_cache_ratio") is not None
            else None,
        )
    if not prices:
        raise ValueError("supplier returned no usable token prices")
    return SupplierPriceSnapshot(
        prices=prices,
        source_metadata={
            "supplier": supplier,
            "group": group,
            "group_ratio": group_ratio,
            "pricing_version": str(raw.get("pricing_version") or ""),
            "priced_model_count": len(prices),
            "unpriced_or_other_group_count": len(raw["data"]) - len(prices),
        },
    )


async def fetch_supplier_prices(supplier: str) -> SupplierPriceSnapshot:
    async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
        async with client.stream("GET", SUPPLIER_SOURCES[supplier]) as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 5_000_000:
                    raise ValueError("supplier pricing response exceeds 5 MB")
    import json

    raw = json.loads(body)
    return parse_supplier_prices(raw, supplier=supplier)


async def sync_catalog(session: AsyncSession, row: PriceCatalog) -> None:
    row.checked_at = datetime.now(UTC)
    try:
        if row.supplier_id not in SUPPLIER_SOURCES:
            raise ValueError("supplier adapter unavailable")
        result = await fetch_supplier_prices(row.supplier_id)
    except (httpx.HTTPError, ValueError, TypeError, KeyError):
        # Never serialize upstream errors/response bodies into shared catalog metadata.
        row.sync_error = "Supplier update failed; last valid prices retained."
    else:
        row.prices = {key: value.model_dump(mode="json") for key, value in result.prices.items()}
        row.aliases = result.aliases
        row.source_metadata = result.source_metadata
        row.updated_at = row.checked_at
        row.sync_error = None
        row.revision += 1
    await session.flush()


async def run_loop(*, session_factory: Any) -> None:
    while True:
        try:
            async with session_factory() as session:
                rows = (
                    await session.scalars(
                        select(PriceCatalog)
                        .where(
                            PriceCatalog.supplier_id.is_not(None),
                            (PriceCatalog.checked_at.is_(None))
                            | (PriceCatalog.checked_at < datetime.now(UTC) - timedelta(hours=6)),
                        )
                        .with_for_update(skip_locked=True)
                    )
                ).all()
                for row in rows:
                    await sync_catalog(session, row)
                await session.commit()
        except Exception:
            logger.warning("Supplier catalog synchronization unavailable; retrying later")
        await asyncio.sleep(300)
