"""GatewaySettings — pydantic-settings v2 (spec §7.5)."""

from __future__ import annotations

from pydantic import PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom.models.types import LogLevel


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_GW_",
        env_file=".env",
        extra="forbid",
    )

    # Persistence (rate_cards live here per §4.7)
    db_url: PostgresDsn

    # Provider keys — only the Gateway has these (§7.4)
    anthropic_api_key: SecretStr | None = None
    openai_api_key: SecretStr | None = None
    together_api_key: SecretStr | None = None

    # Server
    bind_host: str = "0.0.0.0"
    bind_port: int = 9100
    log_level: LogLevel = "info"
    metrics_port: int = 9101

    # Rate-card cache TTL
    rate_card_cache_ttl_sec: int = 300

    # Request timeout to providers
    upstream_timeout_sec: float = 120.0
