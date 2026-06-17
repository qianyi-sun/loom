"""LocalProviderConfig + env-scan helper (intentionally out of schema,
per spec §Out of scope: multi-name env-var pattern doesn't fit
one-name-one-entry)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class LocalProviderConfig:
    base_url: str
    api_key: str | None = None


def parse_local_providers_from_env() -> dict[str, LocalProviderConfig]:
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
            continue
        out[name] = LocalProviderConfig(
            base_url=parts["base_url"],
            api_key=parts.get("api_key"),
        )
    return out
