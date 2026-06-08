"""XDG-aware config for the `loom` CLI.

Lives at `$XDG_CONFIG_HOME/loom/config.toml` (default `~/.config/loom/config.toml`).
Holds upstream LLM API tokens and optional `server_url` for additive
result-posting. All fields are optional — a fresh install has no file
and `load_config()` returns an empty `LoomConfig`.
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
class LoomConfig:
    """Persisted CLI config. Mutable so callers can set fields then save."""

    tokens: dict[str, str] = field(default_factory=dict)
    server_url: str | None = None

    def to_toml_dict(self) -> dict[str, object]:
        out: dict[str, object] = {}
        if self.tokens:
            out["tokens"] = dict(self.tokens)
        if self.server_url is not None:
            out["server_url"] = self.server_url
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
    return LoomConfig(tokens=tokens, server_url=server_url)


def save_config(cfg: LoomConfig) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(tomli_w.dumps(cfg.to_toml_dict()))
