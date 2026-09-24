"""Shared Provider pricing contract (USD per million tokens).

Configuration modes are independent of historical accounting classifications.
No resolver may borrow prices from another model, connection or catalog.
"""

from __future__ import annotations

import csv
import io
import json
import math
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Price = Annotated[float, Field(ge=0, allow_inf_nan=False, strict=True)]


class ModelPrice(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    input_usd_per_1m: Price
    output_usd_per_1m: Price
    cache_read_usd_per_1m: Price | None = None
    cache_write_usd_per_1m: Price | None = None


class PricingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pricing_mode: Literal["usage_only", "catalog", "custom"] = "usage_only"
    supplier_id: str | None = None
    catalog_id: str | None = None
    custom_pricing: dict[str, ModelPrice] | None = None

    @field_validator("custom_pricing")
    @classmethod
    def valid_models(cls, value: dict[str, ModelPrice] | None) -> dict[str, ModelPrice] | None:
        if value is not None:
            for model in value:
                validate_model_id(model)
        return value

    @model_validator(mode="after")
    def exclusive(self) -> PricingConfig:
        if self.pricing_mode == "catalog":
            if not self.catalog_id or self.custom_pricing is not None:
                raise ValueError("catalog mode requires one catalog and no custom prices")
        elif self.pricing_mode == "custom":
            if self.catalog_id is not None or self.custom_pricing is None:
                raise ValueError("custom mode requires a model price table and no catalog")
        elif self.catalog_id is not None or self.custom_pricing is not None:
            raise ValueError("usage_only cannot contain monetary pricing configuration")
        return self

    def price_for(self, model: str) -> ModelPrice | None:
        return (self.custom_pricing or {}).get(model) if self.pricing_mode == "custom" else None


def validate_model_id(model: str) -> str:
    if (
        not isinstance(model, str)
        or not model
        or model != model.strip()
        or len(model) > 256
        or any(c.isspace() or ord(c) < 32 for c in model)
    ):
        raise ValueError("model must be an exact, nonempty model ID without whitespace")
    return model


def parse_price_import(content: str, format: Literal["csv", "json"]) -> dict[str, ModelPrice]:
    """Validate the entire import before returning anything to its caller.

    JSON is an array of model rows, allowing duplicate detection just like CSV.
    CSV header names match ModelPrice fields; empty cache cells mean unknown.
    """
    if len(content) > 2_000_000:
        raise ValueError("price import exceeds 2 MB")
    if format == "csv":
        reader = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames or []
        if len(headers) != len(set(headers)):
            raise ValueError("duplicate CSV headers")
        required = {"model", "input_usd_per_1m", "output_usd_per_1m"}
        allowed = {"model", *ModelPrice.model_fields}
        if not required.issubset(headers) or set(headers) - allowed:
            raise ValueError(
                "CSV requires model and base price columns; unknown columns are invalid"
            )
        rows: Any = list(reader)
    elif format == "json":
        rows = json.loads(content)
    else:
        raise ValueError("format must be csv or json")
    if not isinstance(rows, list) or len(rows) > 10000:
        raise ValueError("import must be an array of at most 10000 model rows")
    prices: dict[str, ModelPrice] = {}
    for index, raw in enumerate(rows, 1):
        if not isinstance(raw, dict):
            raise ValueError(f"row {index}: expected an object")
        data = dict(raw)
        model = data.pop("model", None)
        if not isinstance(model, str):
            raise ValueError(f"row {index}: model is required")
        validate_model_id(model)
        if model in prices:
            raise ValueError(f"row {index}: duplicate model {model}")
        if format == "csv":
            data = {key: None if value == "" else float(value) for key, value in data.items()}
        prices[model] = ModelPrice.model_validate(data)
    return prices


def calculate_price(
    price: ModelPrice,
    *,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0,
    input_includes_cache: bool = False,
) -> float | None:
    """Return unknown when a used billing dimension has no price.

    OpenAI/Google input totals include caches; Anthropic input excludes them.
    The dialect boundary supplies this fact explicitly. Never infer it from a
    model name or a supplier name.
    """
    counts = (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens)
    if any(n < 0 for n in counts):
        return None
    if cache_read_tokens and price.cache_read_usd_per_1m is None:
        return None
    if cache_write_tokens and price.cache_write_usd_per_1m is None:
        return None
    ordinary = (
        input_tokens - cache_read_tokens - cache_write_tokens
        if input_includes_cache
        else input_tokens
    )
    if ordinary < 0:
        return None
    cost = (
        ordinary * price.input_usd_per_1m
        + output_tokens * price.output_usd_per_1m
        + cache_read_tokens * (price.cache_read_usd_per_1m or 0)
        + cache_write_tokens * (price.cache_write_usd_per_1m or 0)
    ) / 1_000_000
    return cost if math.isfinite(cost) else None
