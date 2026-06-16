"""ControlPlaneSettings (spec §7.5)."""

from __future__ import annotations

from pathlib import Path

from pydantic import HttpUrl, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom.models.types import LogLevel


class ControlPlaneSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_CP_", env_file=".env", extra="forbid",
    )

    db_url: PostgresDsn
    minio_endpoint: str
    minio_access_key: SecretStr
    minio_secret_key: SecretStr
    minio_region: str = "us-east-1"
    llm_gateway_url: HttpUrl
    admin_secret_file: Path | None = None

    bind_host: str = "0.0.0.0"
    bind_port: int = 8080
    log_level: LogLevel = "info"
    metrics_port: int = 9090

    # Dev-only: when truthy, uvicorn runs in `--reload` mode watching
    # `/app/src` for changes. Set via `LOOM_CP_DEV_RELOAD=1` in the
    # dev compose; leave unset in production.
    dev_reload: bool = False

    # Worker liveness — claim is reclaimable once heartbeat is this stale.
    worker_heartbeat_expiry_sec: int = 15
    worker_reclaim_sweep_interval_sec: int = 30

    # Signed-URL TTL for artifact uploads.
    signed_url_expiry_sec: int = 3600

    # HS256 secret shared with the LLM Gateway. Used to sign step-scoped
    # JWTs (loom_step_...) that agents present as their bearer token.
    # The Gateway verifies with this same key; rotating it requires a
    # coordinated restart of both services.
    step_jwt_signing_key: SecretStr
