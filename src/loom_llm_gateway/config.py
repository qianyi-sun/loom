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

    # Provider keys — only the Gateway has these (§7.4). The native-
    # dialect passthrough routes (Plan 9 A9.1, A9.3) inject these into
    # outbound calls; the OpenAI Chat path goes through LiteLLM and uses
    # them via env-var injection at app startup.
    anthropic_api_key: SecretStr | None = None
    openai_api_key: SecretStr | None = None
    google_api_key: SecretStr | None = None         # Gemini native passthrough
    together_api_key: SecretStr | None = None

    # HS256 secret shared with the Control Plane (Plan 9 amendments §6.1).
    # Verifies loom_step_... bearer tokens minted by CP's
    # POST /admin/step-tokens. Rotating requires coordinated restart.
    step_jwt_signing_key: SecretStr

    # Server
    bind_host: str = "0.0.0.0"
    bind_port: int = 9100
    log_level: LogLevel = "info"
    metrics_port: int = 9101

    # Rate-card cache TTL
    rate_card_cache_ttl_sec: int = 300

    # Request timeout to providers
    upstream_timeout_sec: float = 120.0
