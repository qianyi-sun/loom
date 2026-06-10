"""GatewaySettings — pydantic-settings v2 (spec §7.5)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from pydantic import PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom.models.types import LogLevel


@dataclass(frozen=True)
class LocalProviderConfig:
    """One operator-registered locally-served OpenAI-compatible LLM
    server. Surfaced through env vars `LOOM_GW_LOCAL_<NAME>_BASE_URL`
    and optional `LOOM_GW_LOCAL_<NAME>_API_KEY`. Consumed by chat route
    when `model="local/<name>/<model_id>"`. Gateway dispatch path is
    follow-up work; the config surface is here so operators can stage
    secrets ahead of the route-level support landing."""

    base_url: str
    api_key: str | None = None


def _parse_local_providers_from_env() -> dict[str, LocalProviderConfig]:
    """Scan os.environ for `LOOM_GW_LOCAL_<NAME>_BASE_URL` + optional
    `LOOM_GW_LOCAL_<NAME>_API_KEY` pairs. Name is lowercased for
    matching the same convention used by CLI config (TOML table keys
    are lowercase identifiers)."""
    base_prefix = "LOOM_GW_LOCAL_"
    base_suffix = "_BASE_URL"
    key_suffix = "_API_KEY"
    names: dict[str, dict[str, str]] = {}
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(base_prefix):
            continue
        if env_key.endswith(base_suffix):
            name = env_key[len(base_prefix):-len(base_suffix)].lower()
            names.setdefault(name, {})["base_url"] = env_val
        elif env_key.endswith(key_suffix):
            name = env_key[len(base_prefix):-len(key_suffix)].lower()
            names.setdefault(name, {})["api_key"] = env_val
    out: dict[str, LocalProviderConfig] = {}
    for name, parts in names.items():
        if "base_url" not in parts:
            continue  # api_key without base_url is incomplete — ignore
        out[name] = LocalProviderConfig(
            base_url=parts["base_url"],
            api_key=parts.get("api_key"),
        )
    return out


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

    # Dev-only: when truthy, uvicorn runs in `--reload` mode watching
    # `/app/src` for changes. Set via `LOOM_GW_DEV_RELOAD=1` in the
    # dev compose; leave unset in production.
    dev_reload: bool = False

    # Rate-card cache TTL
    rate_card_cache_ttl_sec: int = 300

    # Request timeout to providers
    upstream_timeout_sec: float = 120.0

    @property
    def local_providers(self) -> dict[str, LocalProviderConfig]:
        """Operator-registered locally-served OpenAI-compatible LLM
        servers, parsed from `LOOM_GW_LOCAL_<NAME>_BASE_URL` (+ optional
        `_API_KEY`) env vars. Property (not field) so we recompute
        whenever the env changes during a test session."""
        return _parse_local_providers_from_env()
