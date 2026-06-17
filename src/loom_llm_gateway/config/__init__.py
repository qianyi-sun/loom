"""GatewaySettings — re-export from codegen + LocalProviderConfig property.

Schema-driven fields live in `_generated.py`. The local-provider
env-var-scan property (multi-name pattern that doesn't fit the
schema) is layered on here via subclass.
"""
from __future__ import annotations

from loom_llm_gateway.config._generated import GatewaySettings as _BaseSettings
from loom_llm_gateway.config.local_providers import (
    LocalProviderConfig,
    parse_local_providers_from_env,
)


class GatewaySettings(_BaseSettings):
    @property
    def local_providers(self) -> dict[str, LocalProviderConfig]:
        return parse_local_providers_from_env()


__all__ = [
    "GatewaySettings",
    "LocalProviderConfig",
    "parse_local_providers_from_env",
]
