"""XDG-aware config for the `loom` CLI.

Lives at `$XDG_CONFIG_HOME/loom/config.toml` (default
`~/.config/loom/config.toml`). Holds:

- `tokens` — upstream LLM provider API keys (anthropic / openai /
  google)
- `server_url` — optional Control Plane URL for additive
  result-posting from `loom run`
- `local_providers` — registered locally-served OpenAI-compatible
  LLM servers (vLLM, ollama, llama.cpp, lm-studio). Each server has
  a name + base_url + optional api_key. The name disambiguates
  between multiple local servers; the base_url is the
  OpenAI-compatible endpoint (typically ends in `/v1`).

All fields are optional — a fresh install has no file and
`load_config()` returns an empty `LoomConfig`.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import tomli_w

CONFIG_FILENAME = "config.toml"


def _xdg_config_home() -> Path:
    val = os.environ.get("XDG_CONFIG_HOME")
    if val:
        return Path(val)
    return Path(os.environ.get("HOME", str(Path.home()))) / ".config"


def config_path() -> Path:
    return _xdg_config_home() / "loom" / CONFIG_FILENAME


@dataclass
class LocalProvider:
    """One registered locally-served OpenAI-compatible LLM server."""

    base_url: str
    api_key: str | None = None
    served_model_name: str | None = None


@dataclass
class LoomConfig:
    """Persisted CLI config. Mutable so callers can set fields then save."""

    tokens: dict[str, str] = field(default_factory=dict)
    server_url: str | None = None
    # Bearer token for `server_url` API calls (set by `loom auth login`).
    # Stored in plain TOML — operators with shared-machine concerns should
    # either rely on the default 0600 config file mode or use env-var-only
    # auth (LOOM_API_TOKEN, set per-shell, never persisted).
    auth_token: str | None = None
    local_providers: dict[str, LocalProvider] = field(default_factory=dict)

    def to_toml_dict(self) -> dict[str, object]:
        out: dict[str, object] = {}
        if self.tokens:
            out["tokens"] = dict(self.tokens)
        if self.server_url is not None:
            out["server_url"] = self.server_url
        if self.auth_token is not None:
            out["auth_token"] = self.auth_token
        if self.local_providers:
            local: dict[str, dict[str, str]] = {}
            for name, p in self.local_providers.items():
                entry: dict[str, str] = {"base_url": p.base_url}
                if p.api_key is not None:
                    entry["api_key"] = p.api_key
                if p.served_model_name is not None:
                    entry["served_model_name"] = p.served_model_name
                local[name] = entry
            out["local_providers"] = local
        return out


def load_config() -> LoomConfig:
    path = config_path()
    if not path.exists():
        return LoomConfig()
    raw = tomllib.loads(path.read_text())
    tokens_obj = raw.get("tokens", {})
    if not isinstance(tokens_obj, dict):
        raise ValueError(f"{path}: [tokens] must be a table")
    tokens = {str(k): str(v) for k, v in tokens_obj.items()}
    server_url = raw.get("server_url")
    if server_url is not None and not isinstance(server_url, str):
        raise ValueError(f"{path}: server_url must be a string")
    auth_token = raw.get("auth_token")
    if auth_token is not None and not isinstance(auth_token, str):
        raise ValueError(f"{path}: auth_token must be a string")
    local_raw = raw.get("local_providers", {})
    if not isinstance(local_raw, dict):
        raise ValueError(f"{path}: [local_providers] must be a table")
    local_providers: dict[str, LocalProvider] = {}
    for name, entry in local_raw.items():
        if not isinstance(entry, dict):
            raise ValueError(
                f"{path}: [local_providers.{name}] must be a table "
                f"with at minimum a base_url",
            )
        base_url = entry.get("base_url")
        if not isinstance(base_url, str) or not base_url:
            raise ValueError(
                f"{path}: [local_providers.{name}].base_url is required "
                f"(string, non-empty)",
            )
        api_key = entry.get("api_key")
        if api_key is not None and not isinstance(api_key, str):
            raise ValueError(
                f"{path}: [local_providers.{name}].api_key must be a string",
            )
        served_model_name = entry.get("served_model_name")
        if served_model_name is not None and not isinstance(served_model_name, str):
            raise ValueError(
                f"{path}: [local_providers.{name}].served_model_name must be a string",
            )
        local_providers[str(name)] = LocalProvider(
            base_url=base_url,
            api_key=api_key,
            served_model_name=served_model_name,
        )
    return LoomConfig(
        tokens=tokens,
        server_url=server_url,
        auth_token=auth_token,
        local_providers=local_providers,
    )


def save_config(cfg: LoomConfig) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        path.parent.chmod(0o700)
    path.write_text(tomli_w.dumps(cfg.to_toml_dict()))
    if os.name != "nt":
        path.chmod(0o600)


def set_local_provider(
    name: str,
    *,
    base_url: str,
    api_key: str | None = None,
    served_model_name: str | None = None,
) -> None:
    """Persist a `[local_providers.<name>]` entry to
    `~/.config/loom/config.toml`. Overwrites any existing entry with
    the same name."""
    cfg = load_config()
    cfg.local_providers[name] = LocalProvider(
        base_url=base_url,
        api_key=api_key,
        served_model_name=served_model_name,
    )
    save_config(cfg)


def unset_local_provider(name: str) -> None:
    """Remove a `[local_providers.<name>]` entry. Silent no-op if
    the entry doesn't exist."""
    cfg = load_config()
    if name in cfg.local_providers:
        del cfg.local_providers[name]
        save_config(cfg)
