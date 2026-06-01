from __future__ import annotations

import os
from dataclasses import dataclass, field
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
    internal_auth_tokens: str = ""
    web_login_credentials: str = ""
    web_session_secret: str = ""
    web_session_ttl_seconds: int = 28800
    model_provider_models: str = ""
    sandbox_workspace_root: str = ".runtime/sandbox-workspaces"
    sandbox_host_workspace_root: str = ""
    harbor_task_upload_max_bytes: int = 10 * 1024 * 1024
    harbor_task_upload_max_files: int = 256
    harbor_task_upload_max_uncompressed_bytes: int = 50 * 1024 * 1024
    worker_subprocess_isolation_enabled: bool = False
    worker_subprocess_timeout_seconds: int = 7200
    worker_legacy_queue_claim_enabled: bool = False
    scheduler_global_max_active_runs: int = 1
    scheduler_backend_max_active_runs: dict[str, int] = field(default_factory=dict)
    scheduler_project_max_active_runs: dict[str, int] = field(default_factory=dict)


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
        internal_auth_tokens=_get(values, "INTERNAL_AUTH_TOKENS", ""),
        web_login_credentials=_get(values, "WEB_LOGIN_CREDENTIALS", ""),
        web_session_secret=_get(values, "WEB_SESSION_SECRET", ""),
        web_session_ttl_seconds=_get_int(values, "WEB_SESSION_TTL_SECONDS", 28800),
        model_provider_models=_get(values, "MODEL_PROVIDER_MODELS", ""),
        sandbox_workspace_root=_get(values, "SANDBOX_WORKSPACE_ROOT", ".runtime/sandbox-workspaces"),
        sandbox_host_workspace_root=_get(values, "SANDBOX_HOST_WORKSPACE_ROOT", ""),
        harbor_task_upload_max_bytes=_get_int(values, "HARBOR_TASK_UPLOAD_MAX_BYTES", 10 * 1024 * 1024),
        harbor_task_upload_max_files=_get_int(values, "HARBOR_TASK_UPLOAD_MAX_FILES", 256),
        harbor_task_upload_max_uncompressed_bytes=_get_int(
            values,
            "HARBOR_TASK_UPLOAD_MAX_UNCOMPRESSED_BYTES",
            50 * 1024 * 1024,
        ),
        worker_subprocess_isolation_enabled=_get_bool(values, "WORKER_SUBPROCESS_ISOLATION_ENABLED", False),
        worker_subprocess_timeout_seconds=_get_int(values, "WORKER_SUBPROCESS_TIMEOUT_SECONDS", 7200),
        worker_legacy_queue_claim_enabled=_get_bool(values, "WORKER_LEGACY_QUEUE_CLAIM_ENABLED", False),
        scheduler_global_max_active_runs=_get_int(values, "SCHEDULER_GLOBAL_MAX_ACTIVE_RUNS", 1),
        scheduler_backend_max_active_runs=_get_int_map(values, "SCHEDULER_BACKEND_MAX_ACTIVE_RUNS"),
        scheduler_project_max_active_runs=_get_int_map(values, "SCHEDULER_PROJECT_MAX_ACTIVE_RUNS"),
    )


def _get(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) else default


def _get_int(values: Mapping[str, str], key: str, default: int) -> int:
    value = _get(values, key, "")
    if not value:
        return default
    return int(value)


def _get_bool(values: Mapping[str, str], key: str, default: bool) -> bool:
    value = _get(values, key, "")
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _get_int_map(values: Mapping[str, str], key: str) -> dict[str, int]:
    raw_value = _get(values, key, "")
    if not raw_value:
        return {}

    parsed: dict[str, int] = {}
    for item in raw_value.split(","):
        if not item.strip():
            continue
        name, separator, value = item.partition("=")
        if not separator:
            raise ValueError(f"{key} entries must use name=value syntax")
        name = name.strip()
        if not name:
            raise ValueError(f"{key} entries must include a non-empty name")
        parsed[name] = int(value.strip())
    return parsed
