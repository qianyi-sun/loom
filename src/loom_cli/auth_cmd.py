"""`loom auth {login,status,whoami,logout}` — manage credentials for the
deployed Loom server the rest of the CLI talks to.

Per cluster-deploy.md §CLI surface. Works against any deployed Loom
(`loom service` or `loom cluster`) — the CLI doesn't care which mode
the server is in.

- `loom auth login` persists `(server_url, auth_token)` to
  $XDG_CONFIG_HOME/loom/config.toml.
- `loom auth status` prints what's stored + exits 0 if logged in,
  non-zero otherwise (CI-friendly).
- `loom auth whoami` asks the server which principal/scopes the
  stored token maps to.
- `loom auth logout` clears the token (preserves server_url so
  re-login is one flag less).

Argv hygiene: `--token` accepts ONLY `env:VAR | file:PATH | -`. See
loom_cli.secret_source for the rationale.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import cast

import httpx

from loom_cli.config import config_path, load_config, save_config
from loom_cli.secret_source import (
    SecretSourceError,
    resolve_secret_source,
    secret_source_argparse_type,
)
from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)


def _redact(token: str) -> str:
    """Show token prefix + suffix only (matches `loom config show`)."""
    if len(token) < 8:
        # Token is suspiciously short — show length but no chars to
        # avoid revealing structure.
        return f"<len={len(token)}>"
    return f"{token[:6]}***{token[-4:]}"


def _login(args: argparse.Namespace) -> int:
    try:
        token = resolve_secret_source(args.token, flag_name="--token")
    except SecretSourceError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    server_url: str = args.server.rstrip("/")
    if not (server_url.startswith("http://") or
            server_url.startswith("https://")):
        sys.stderr.write(
            f"error: --server must start with http:// or https://; "
            f"got {server_url!r}\n",
        )
        return 2

    cfg = load_config()
    cfg.server_url = server_url
    cfg.auth_token = token
    save_config(cfg)
    print(f"Logged in to {server_url}")
    print(f"Token stored in {config_path()} as {_redact(token)}")
    return 0


def _status(args: argparse.Namespace) -> int:
    cfg = load_config()
    server = cfg.server_url or "(none)"
    if cfg.auth_token:
        token_display = f"set ({_redact(cfg.auth_token)})"
        logged_in = cfg.server_url is not None
    else:
        token_display = "(none)"
        logged_in = False

    # Stable two-column output for human + CI parsing.
    print(f"Server:  {server}")
    print(f"Token:   {token_display}")
    print(f"Config:  {config_path()}")

    return 0 if logged_in else 1


def _format_scopes(scopes: object) -> str:
    if not isinstance(scopes, Sequence) or isinstance(scopes, str):
        return "(none)"
    rendered = sorted(str(scope) for scope in scopes)
    return ", ".join(rendered) if rendered else "(none)"


def _principal_label(data: dict[str, object]) -> str:
    auth_kind = str(data.get("auth_kind") or "unknown")
    principal_type = str(data.get("principal_type") or "unknown")
    if auth_kind in {"bearer", "token"} and principal_type == "team":
        return "team token"
    if auth_kind == "session" and principal_type == "user":
        return "browser session"
    return f"{principal_type} via {auth_kind}"


def _team_label(data: dict[str, object]) -> str:
    name = data.get("team_name")
    team_id = data.get("team_id")
    role = data.get("role")
    team = str(name or team_id or "(none)")
    if role:
        return f"{team} ({role})"
    return team


def _whoami(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    try:
        with authed_client(cfg) as c:
            data = assert_2xx(
                c.get("/api/v1/auth/whoami"),
                action="inspect authenticated principal",
            )
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {cfg.server_url}: {e}\n")
        return 2

    print(f"Server:    {cfg.server_url}")
    print(f"Principal: {_principal_label(data)}")
    print(f"Team:      {_team_label(data)}")
    print(f"Scopes:    {_format_scopes(data.get('scopes'))}")
    print(f"Token:     {data.get('token_prefix') or '(session)'}")
    expires_at = data.get("expires_at")
    if expires_at:
        print(f"Expires:   {expires_at}")
    return 0


def _logout(args: argparse.Namespace) -> int:
    cfg = load_config()
    if not cfg.auth_token:
        print("Already logged out — no token in config.")
        return 0
    cfg.auth_token = None
    save_config(cfg)
    print(f"Cleared token in {config_path()} (server URL preserved).")
    return 0


def dispatch(argv: list[str]) -> int:
    """Entry point invoked from `loom_cli.__main__` when `argv[0]` is
    'auth'. Returns the process exit code.
    """
    parser = argparse.ArgumentParser(
        prog="loom auth",
        description=(
            "Manage credentials for the deployed Loom server the CLI "
            "talks to. See `loom eval` + `loom providers` for what to "
            "do once logged in."
        ),
    )
    sub = parser.add_subparsers(dest="auth_cmd", required=True)

    p_login = sub.add_parser(
        "login",
        help="Save server URL + bearer token to ~/.config/loom/config.toml",
    )
    p_login.add_argument(
        "--server",
        required=True,
        help="Server URL (e.g. https://loom.example.com)",
    )
    p_login.add_argument(
        "--token",
        required=True,
        type=secret_source_argparse_type("--token"),
        help=(
            "Token source. ONE of: 'env:VAR' (read os.environ[VAR]), "
            "'file:PATH' (read file content), or '-' (read stdin until "
            "EOF). Literal values are rejected — argv leaks via shell "
            "history and `ps -ef`."
        ),
    )
    p_login.set_defaults(handler=_login)

    p_status = sub.add_parser(
        "status",
        help="Show stored server + token (redacted). Exit 0 if logged in.",
    )
    p_status.set_defaults(handler=_status)

    p_whoami = sub.add_parser(
        "whoami",
        help="Ask the server which principal, team, scopes, and token prefix are active.",
    )
    p_whoami.set_defaults(handler=_whoami)

    p_logout = sub.add_parser(
        "logout",
        help="Clear stored token (preserves server URL).",
    )
    p_logout.set_defaults(handler=_logout)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
