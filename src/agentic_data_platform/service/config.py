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
    object_storage_access_key: str
    object_storage_secret_key: str
    object_storage_region: str
    model_provider_base_url: str = ""
    model_provider_api_key: str = ""
    evaluator_provider_base_url: str = ""
    evaluator_provider_api_key: str = ""


def load_service_settings(environ: Mapping[str, str] | None = None) -> ServiceSettings:
    values = os.environ if environ is None else environ
    return ServiceSettings(
        app_name=_get(values, "APP_NAME", "agentic-data-platform"),
        environment=_get(values, "APP_ENV", "dev"),
        database_url=_get(values, "DATABASE_URL", ""),
        redis_url=_get(values, "REDIS_URL", ""),
        object_storage_endpoint=_get(values, "OBJECT_STORAGE_ENDPOINT", ""),
        object_storage_bucket=_get(values, "OBJECT_STORAGE_BUCKET", ""),
        object_storage_access_key=_get(values, "OBJECT_STORAGE_ACCESS_KEY", ""),
        object_storage_secret_key=_get(values, "OBJECT_STORAGE_SECRET_KEY", ""),
        object_storage_region=_get(values, "OBJECT_STORAGE_REGION", "us-east-1"),
        model_provider_base_url=_get(values, "MODEL_PROVIDER_BASE_URL", ""),
        model_provider_api_key=_get(values, "MODEL_PROVIDER_API_KEY", ""),
        evaluator_provider_base_url=_get(values, "EVALUATOR_PROVIDER_BASE_URL", ""),
        evaluator_provider_api_key=_get(values, "EVALUATOR_PROVIDER_API_KEY", ""),
    )


def _get(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) else default
