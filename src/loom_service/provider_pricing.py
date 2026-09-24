"""Provider pricing persistence boundary, including untouched legacy records."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PriceCatalog, ProviderConnection, ProviderConnectionShare
from loom.provider_pricing import PricingConfig

SUPPLIER_CATALOGS = {"yibuapi": "supplier:yibuapi", "az-gptplus5": "supplier:az-gptplus5"}
LEGACY_MODES = {"tokens-only": "usage_only", "rate-card": "catalog", "operator-supplied": "custom"}


def pricing_response(row: ProviderConnection) -> dict[str, Any]:
    if row.pricing_config is not None:
        return {
            **PricingConfig.model_validate(row.pricing_config).model_dump(),
            "legacy_pricing": None,
        }
    # Never turn an old wildcard price into an apparent exact-model table. Its
    # original behavior remains active until an explicit pricing edit.
    return {
        "pricing_mode": LEGACY_MODES.get(row.pricing_source, "usage_only"),
        "supplier_id": None,
        "catalog_id": None,
        "custom_pricing": None,
        "legacy_pricing": {
            "default_model_price": row.pricing_data,
            "catalog_namespace": row.rate_card_provider,
            "message": "Legacy pricing remains active until you explicitly replace pricing settings.",
        }
        if row.pricing_source != "tokens-only"
        else None,
    }


async def resolve_config(
    session: AsyncSession,
    team_id: UUID,
    changes: dict[str, Any],
    *,
    previous: dict[str, Any] | None = None,
) -> PricingConfig:
    """Omission retains a value, explicit null clears it, mode changes clear inactive fields."""
    data = dict(previous or {})
    data.update(changes)
    if previous is None and changes.get("pricing_mode") is None:
        supplier = changes.get("supplier_id")
        data["pricing_mode"] = "catalog" if supplier in SUPPLIER_CATALOGS else "usage_only"
        if data["pricing_mode"] == "catalog" and "catalog_id" not in changes:
            data["catalog_id"] = SUPPLIER_CATALOGS[str(supplier)]
    mode = data.get("pricing_mode")
    if mode != "custom":
        if changes.get("custom_pricing") is not None:
            raise HTTPException(400, "custom prices are only valid in custom mode")
        data["custom_pricing"] = None
    if mode != "catalog":
        if changes.get("catalog_id") is not None:
            raise HTTPException(400, "catalog is only valid in catalog mode")
        data["catalog_id"] = None
    try:
        config = PricingConfig.model_validate(data)
    except ValidationError as exc:
        raise HTTPException(400, str(exc)) from exc
    if config.catalog_id:
        catalog = await session.get(PriceCatalog, config.catalog_id)
        if catalog is None or catalog.team_id not in (None, team_id):
            raise HTTPException(404, "price catalog not found")
        if config.supplier_id and catalog.supplier_id and config.supplier_id != catalog.supplier_id:
            raise HTTPException(400, "catalog does not belong to the selected supplier")
    return config


async def ensure_catalog_share_compatible(
    session: AsyncSession, row: ProviderConnection, config: PricingConfig
) -> None:
    if config.catalog_id is None:
        return
    catalog = await session.get(PriceCatalog, config.catalog_id)
    if catalog is None or catalog.team_id is None:
        return
    shared = await session.scalar(
        select(ProviderConnectionShare.provider_connection_id)
        .where(
            ProviderConnectionShare.provider_connection_id == row.id,
        )
        .limit(1)
    )
    if shared is not None:
        raise HTTPException(
            400, "A team-private price catalog cannot be attached to a shared connection."
        )


def apply_config(row: ProviderConnection, config: PricingConfig) -> None:
    row.pricing_config = config.model_dump(mode="json")
    # Old binaries must not misinterpret model-specific data as uniform prices.
    # Keep old columns valid, while new code always resolves pricing_config first.
    row.pricing_source = "tokens-only"
    row.pricing_data = None
    row.rate_card_provider = None
