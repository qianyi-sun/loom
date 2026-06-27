from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _parse_datetime(value: datetime | str) -> datetime | None:
    if isinstance(value, datetime):
        return value
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def format_local_datetime(
    value: datetime | str | None,
    *,
    fallback: str = "-",
    time_zone: str | None = None,
) -> str:
    if value is None:
        return fallback
    if isinstance(value, str) and not value.strip():
        return fallback
    parsed = _parse_datetime(value)
    if parsed is None:
        return str(value)

    tz = None
    if time_zone is not None:
        try:
            tz = ZoneInfo(time_zone)
        except ZoneInfoNotFoundError:
            tz = None
    local_dt = parsed.astimezone(tz)
    zone = local_dt.tzname() or local_dt.strftime("%z")
    suffix = f" {zone}" if zone else ""
    return local_dt.strftime("%Y-%m-%d %H:%M") + suffix
