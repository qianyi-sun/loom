"""YibuAPI official pricing conversion helpers.

YibuAPI's public pricing endpoint exposes model "ratio" fields. The pricing
page's own JavaScript renders USD token prices as:

    input USD / 1M tokens = model_ratio * 2 * group_ratio
    output USD / 1M tokens = input_price * completion_ratio

Cache read/create ratios use the same input-price base. This module converts
that official payload into Loom's rate-card table shape, preserving source
metadata so the table used for a run remains auditable.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any

DEFAULT_YIBUAPI_PRICING_URL = "https://yibuapi.com/api/pricing"
YIBUAPI_RATE_CARD_PROVIDER = "yibuapi"

_COMMON_PREFIX_RE = re.compile(r"^(?:yibuapi|models?)/", re.IGNORECASE)


def normalize_yibuapi_model_name(model_name: str) -> str:
    """Return the SKU shape YibuAPI uses in `/api/pricing`."""

    value = str(model_name or "").strip()
    while _COMMON_PREFIX_RE.match(value):
        value = _COMMON_PREFIX_RE.sub("", value, count=1)
    return value


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rounded(value: float) -> float:
    return round(value, 12)


def _card_id(version: str | None, payload: dict[str, Any]) -> str:
    if version:
        return f"yibuapi-{version}"
    digest = hashlib.sha256(
        repr(payload.get("data", [])).encode(),
    ).hexdigest()[:12]
    return f"yibuapi-{digest}"


def build_yibuapi_rate_card(
    payload: dict[str, Any],
    *,
    source_url: str = DEFAULT_YIBUAPI_PRICING_URL,
    fetched_at: datetime,
    provider: str = YIBUAPI_RATE_CARD_PROVIDER,
    group: str = "default",
) -> dict[str, Any]:
    """Convert a YibuAPI `/api/pricing` response to a Loom rate card."""

    version = str(payload.get("pricing_version")) if payload.get("pricing_version") else None
    group_ratio_payload = payload.get("group_ratio")
    group_ratio = 1.0
    if isinstance(group_ratio_payload, dict):
        group_ratio = _number(group_ratio_payload.get(group)) or 1.0

    entries: list[dict[str, Any]] = []
    skipped = 0
    for raw in payload.get("data") or []:
        if not isinstance(raw, dict):
            skipped += 1
            continue
        if int(raw.get("quota_type", -1)) != 0:
            skipped += 1
            continue
        if raw.get("billing_mode") == "tiered_expr" or raw.get("billing_expr"):
            skipped += 1
            continue
        model = normalize_yibuapi_model_name(str(raw.get("model_name") or ""))
        model_ratio = _number(raw.get("model_ratio"))
        completion_ratio = _number(raw.get("completion_ratio"))
        if not model or model_ratio is None or completion_ratio is None:
            skipped += 1
            continue

        input_price = model_ratio * 2.0 * group_ratio
        output_price = input_price * completion_ratio
        cache_ratio = _number(raw.get("cache_ratio"))
        create_cache_ratio = _number(raw.get("create_cache_ratio"))
        entry_version = str(raw.get("pricing_version")) if raw.get("pricing_version") else version
        entries.append(
            {
                "provider": provider,
                "model": model,
                "input_per_mtok": _rounded(input_price),
                "output_per_mtok": _rounded(output_price),
                "cache_read_per_mtok": (
                    _rounded(input_price * cache_ratio) if cache_ratio is not None else 0
                ),
                "cache_write_per_mtok": (
                    _rounded(input_price * create_cache_ratio)
                    if create_cache_ratio is not None
                    else 0
                ),
                "currency": "USD",
                "source_url": source_url,
                "pricing_version": entry_version,
                "source_model": model,
                "pricing_unit": "usd_per_1m_tokens",
            }
        )

    return {
        "id": _card_id(version, payload),
        "provider": provider,
        "source_url": source_url,
        "pricing_version": version,
        "last_checked_at": fetched_at.isoformat(),
        "currency": "USD",
        "group": group,
        "group_ratio": group_ratio,
        "entry_count": len(entries),
        "skipped_model_count": skipped,
        "entries": entries,
    }
