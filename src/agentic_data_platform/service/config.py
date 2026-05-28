from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ServiceSettings:
    app_name: str
    environment: str
    database_url: str
    redis_url: str
    object_storage_endpoint: str
    object_storage_bucket: str


def load_service_settings(environ: Mapping[str, str] | None = None) -> ServiceSettings:
    values = os.environ if environ is None else environ
    return ServiceSettings(
        app_name=_get(values, "APP_NAME", "agentic-data-platform"),
        environment=_get(values, "APP_ENV", "dev"),
        database_url=_get(values, "DATABASE_URL", ""),
        redis_url=_get(values, "REDIS_URL", ""),
        object_storage_endpoint=_get(values, "OBJECT_STORAGE_ENDPOINT", ""),
        object_storage_bucket=_get(values, "OBJECT_STORAGE_BUCKET", ""),
    )


def _get(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) else default
