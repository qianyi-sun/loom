"""`loom config set|show` handlers.

Supported keys for `set`:
  - `token.<provider>` — upstream LLM API key (provider in {anthropic, openai, google})
  - `server_url`       — optional Control Plane URL for additive result POSTing
"""

from __future__ import annotations

import argparse
import sys

from loom_cli.config import load_config, save_config

_VALID_PROVIDERS = frozenset({"anthropic", "openai", "google"})


def dispatch(args: argparse.Namespace) -> int:
    sub = args.config_cmd
    if sub == "set":
        return _set(args.key, args.value)
    if sub == "show":
        return _show()
    print(f"unknown config subcommand: {sub}", file=sys.stderr)
    return 2


def _set(key: str, value: str) -> int:
    cfg = load_config()
    if key.startswith("token."):
        provider = key.split(".", 1)[1]
        if provider not in _VALID_PROVIDERS:
            print(
                f"unknown key {key!r}: provider must be one of "
                f"{sorted(_VALID_PROVIDERS)}",
                file=sys.stderr,
            )
            return 2
        cfg.tokens[provider] = value
    elif key == "server_url":
        cfg.server_url = value
    else:
        print(
            f"unknown key {key!r}; supported: token.<provider>, server_url",
            file=sys.stderr,
        )
        return 2
    save_config(cfg)
    print(f"set {key}")
    return 0


def _redact(value: str) -> str:
    if len(value) <= 4:
        return "***"
    return f"{value[:2]}***{value[-2:]}"


def _show() -> int:
    cfg = load_config()
    if not cfg.tokens and cfg.server_url is None:
        print("(empty — run `loom config set token.<provider> <key>`)")
        return 0
    print("tokens:")
    for provider, val in sorted(cfg.tokens.items()):
        print(f"  {provider} = {_redact(val)}")
    if cfg.server_url is not None:
        print(f"server_url = {cfg.server_url}")
    return 0
