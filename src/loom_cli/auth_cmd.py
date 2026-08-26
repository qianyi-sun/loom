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
import json
import re
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
    CSRF_HEADER_NAME,
    SESSION_COOKIE_NAME,
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    assert_2xx_response,
    authed_client,
    persist_session_credentials_from_response,
    require_logged_in,
)


def _redact(token: str) -> str:
    """Show token prefix + suffix only (matches `loom config show`)."""
    if len(token) < 8:
        # Token is suspiciously short — show length but no chars to
        # avoid revealing structure.
        return f"<len={len(token)}>"
    return f"{token[:6]}***{token[-4:]}"


def _normalize_server_url(raw_server: str) -> str:
    server_url = raw_server.rstrip("/")
    if not (server_url.startswith("http://") or
            server_url.startswith("https://")):
        raise ValueError(
            f"--server must start with http:// or https://; got {server_url!r}",
        )
    return server_url


def _plain_client(server_url: str, *, timeout: float = 30.0) -> httpx.Client:
    return httpx.Client(base_url=server_url, timeout=timeout)


def _resolve_secret(value: str, *, flag_name: str) -> str | None:
    try:
        return resolve_secret_source(value, flag_name=flag_name)
    except SecretSourceError as e:
        sys.stderr.write(f"error: {e}\n")
        return None


def _save_bearer_login(*, server_url: str, token: str) -> None:
    cfg = load_config()
    cfg.server_url = server_url
    cfg.auth_token = token
    cfg.auth_session_cookie = None
    cfg.auth_csrf_token = None
    save_config(cfg)


def _save_session_login(
    *,
    server_url: str,
    session_cookie: str,
    csrf_token: str,
) -> None:
    cfg = load_config()
    cfg.server_url = server_url
    cfg.auth_token = None
    cfg.auth_session_cookie = session_cookie
    cfg.auth_csrf_token = csrf_token
    save_config(cfg)


def _login_with_token(args: argparse.Namespace, *, server_url: str) -> int:
    token = _resolve_secret(args.token, flag_name="--token")
    if token is None:
        return 2
    _save_bearer_login(server_url=server_url, token=token)
    print(f"Logged in to {server_url}")
    print(f"Token stored in {config_path()} as {_redact(token)}")
    return 0


def _login_with_password(args: argparse.Namespace, *, server_url: str) -> int:
    password = _resolve_secret(args.password, flag_name="--password")
    if password is None:
        return 2
    try:
        with _plain_client(server_url) as c:
            response = c.post(
                "/api/v1/auth/login",
                json={"username": args.username, "password": password},
            )
            assert_2xx_response(response, action="log in with username/password")
            data = response.json()
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {server_url}: {e}\n")
        return 2

    session_cookie = response.cookies.get(SESSION_COOKIE_NAME)
    csrf_token = data.get("csrf_token") if isinstance(data, dict) else None
    if not session_cookie or not isinstance(csrf_token, str) or not csrf_token:
        sys.stderr.write("error: login response did not include session credentials\n")
        return 1
    _save_session_login(
        server_url=server_url,
        session_cookie=session_cookie,
        csrf_token=csrf_token,
    )
    user = data.get("user") if isinstance(data, dict) else None
    username = user.get("username") if isinstance(user, dict) else args.username
    print(f"Logged in to {server_url} as {username}")
    print(f"Session stored in {config_path()} ({SESSION_COOKIE_NAME} + {CSRF_HEADER_NAME})")
    return 0


def _login(args: argparse.Namespace) -> int:
    try:
        server_url = _normalize_server_url(args.server)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    if args.token is not None:
        if args.username is not None or args.password is not None:
            sys.stderr.write("error: use either --token or --username/--password, not both\n")
            return 2
        return _login_with_token(args, server_url=server_url)
    if args.username is not None and args.password is not None:
        return _login_with_password(args, server_url=server_url)

    sys.stderr.write("error: provide --token or both --username and --password\n")
    return 2


def _register(args: argparse.Namespace) -> int:
    try:
        server_url = _normalize_server_url(args.server)
        with _plain_client(server_url) as c:
            data = assert_2xx(
                c.post(
                    "/api/v1/auth/registration-requests",
                    json={
                        "username": args.username,
                        "team_id": args.team_id,
                        "metadata": {},
                    },
                ),
                action="submit registration request",
            )
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {args.server}: {e}\n")
        return 2

    print("Registration request submitted")
    print(f"Username: {data.get('username') or args.username}")
    print(f"Status:   {data.get('status') or 'pending'}")
    return 0


def _teams(args: argparse.Namespace) -> int:
    try:
        server_url = _normalize_server_url(args.server)
        with _plain_client(server_url) as c:
            data = assert_2xx(
                c.get("/api/v1/auth/public-teams"),
                action="list registration teams",
            )
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {args.server}: {e}\n")
        return 2
    items = data.get("items") if isinstance(data, dict) else []
    if not isinstance(items, list) or not items:
        print("No public registration teams available.")
        return 0
    print("ID                                    Name")
    for item in items:
        if not isinstance(item, dict):
            continue
        print(f"{item.get('id')!s:36}  {item.get('name') or '(unnamed)'}")
    return 0


def _complete_password_action(
    args: argparse.Namespace,
    *,
    path: str,
    action: str,
    success_verb: str,
) -> int:
    token = _resolve_secret(args.token, flag_name="--token")
    password = _resolve_secret(args.password, flag_name="--password")
    confirm_password = _resolve_secret(
        args.confirm_password, flag_name="--confirm-password",
    )
    if token is None or password is None or confirm_password is None:
        return 2
    try:
        server_url = _normalize_server_url(args.server)
        with _plain_client(server_url) as c:
            data = assert_2xx(
                c.post(
                    path,
                    json={
                        "token": token,
                        "password": password,
                        "confirm_password": confirm_password,
                    },
                ),
                action=action,
            )
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {args.server}: {e}\n")
        return 2
    user = data.get("user") if isinstance(data, dict) else None
    username = user.get("username") if isinstance(user, dict) else "(unknown user)"
    print(f"Password {success_verb} {username}")
    return 0


def _setup_password(args: argparse.Namespace) -> int:
    return _complete_password_action(
        args,
        path="/api/v1/auth/setup/complete",
        action="set password",
        success_verb="set for",
    )


def _forgot_password(args: argparse.Namespace) -> int:
    try:
        server_url = _normalize_server_url(args.server)
        with _plain_client(server_url) as c:
            assert_2xx(
                c.post(
                    "/api/v1/auth/password-reset-requests",
                    json={"username": args.username},
                ),
                action="submit password reset request",
            )
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {args.server}: {e}\n")
        return 2
    print("Password reset request submitted")
    return 0


def _reset_password(args: argparse.Namespace) -> int:
    return _complete_password_action(
        args,
        path="/api/v1/auth/reset/complete",
        action="reset password",
        success_verb="reset for",
    )


def _status(args: argparse.Namespace) -> int:
    cfg = load_config()
    server = cfg.server_url or "(none)"
    if cfg.auth_token:
        token_display = f"set ({_redact(cfg.auth_token)})"
    else:
        token_display = "(none)"
    if cfg.auth_session_cookie:
        session_display = "set"
    else:
        session_display = "(none)"
    logged_in = (
        cfg.server_url is not None
        and (cfg.auth_token is not None or cfg.auth_session_cookie is not None)
    )

    # Stable two-column output for human + CI parsing.
    print(f"Server:  {server}")
    print(f"Token:   {token_display}")
    print(f"Session: {session_display}")
    print(f"Config:  {config_path()}")

    return 0 if logged_in else 1


def _format_scopes(scopes: object) -> str:
    if not isinstance(scopes, Sequence) or isinstance(scopes, str):
        return "(none)"
    rendered = sorted(str(scope) for scope in scopes)
    return ", ".join(rendered) if rendered else "(none)"


_SCOPE = re.compile(r"[a-z][a-z0-9_-]*(?::[a-z][a-z0-9_-]*)*")


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _validated_scope_list(scopes: object) -> list[str]:
    """Return only the bounded, non-secret scope strings from a whoami response."""
    if not isinstance(scopes, Sequence) or isinstance(scopes, str):
        return []
    return [
        scope
        for scope in scopes
        if isinstance(scope, str) and _SCOPE.fullmatch(scope) is not None
    ]


def _principal_label(data: dict[str, object]) -> str:
    auth_kind = str(data.get("auth_kind") or "unknown")
    principal_type = str(data.get("principal_type") or "unknown")
    credential_type = str(data.get("credential_type") or "")
    labels = {
        "browser_session": "browser session",
        "user_owned_api_token": "user-owned API token",
        "legacy_team_token": "legacy team token",
        "service_credential": "service credential",
        "admin_bearer_token": "admin bearer token",
        "worker_token": "worker token",
        "step_session": "step session",
    }
    if credential_type in labels:
        return labels[credential_type]
    if auth_kind in {"bearer", "token"} and principal_type == "team":
        if data.get("user_id"):
            return "user-owned API token"
        return "legacy team token"
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


def _user_label(data: dict[str, object]) -> str:
    username = data.get("username")
    user_id = data.get("user_id")
    if username and user_id:
        return f"{username} ({user_id})"
    return str(username or user_id or "(none)")


def _whoami(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    try:
        with authed_client(cfg) as c:
            response = c.get("/api/v1/auth/whoami")
            data = assert_2xx(response, action="inspect authenticated principal")
            persist_session_credentials_from_response(cfg, response, data=data)
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {cfg.server_url}: {e}\n")
        return 2

    if args.format == "json":
        record = {
            "auth_kind": _optional_string(data.get("auth_kind")),
            "credential_type": _optional_string(data.get("credential_type")),
            "expires_at": _optional_string(data.get("expires_at")),
            "principal_type": _optional_string(data.get("principal_type")),
            "role": _optional_string(data.get("role")),
            "scopes": sorted(set(_validated_scope_list(data.get("scopes")))),
            "server": cfg.server_url,
            "team_id": _optional_string(data.get("team_id")),
            "token_prefix": _optional_string(data.get("token_prefix")),
            "user_id": _optional_string(data.get("user_id")),
        }
        json.dump(record, sys.stdout, separators=(",", ":"), sort_keys=True)
        sys.stdout.write("\n")
        return 0

    print(f"Server:    {cfg.server_url}")
    print(f"Principal: {_principal_label(data)}")
    if data.get("username") or data.get("user_id"):
        print(f"User:      {_user_label(data)}")
    print(f"Team:      {_team_label(data)}")
    print(f"Scopes:    {_format_scopes(data.get('scopes'))}")
    print(f"Token:     {data.get('token_prefix') or '(session)'}")
    expires_at = data.get("expires_at")
    if expires_at:
        print(f"Expires:   {expires_at}")
    return 0


def _logout(args: argparse.Namespace) -> int:
    cfg = load_config()
    if not cfg.auth_token and not cfg.auth_session_cookie:
        print("Already logged out — no credentials in config.")
        return 0
    cfg.auth_token = None
    cfg.auth_session_cookie = None
    cfg.auth_csrf_token = None
    save_config(cfg)
    print(f"Cleared credentials in {config_path()} (server URL preserved).")
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
        help="Log in with username/password or save a bearer token.",
    )
    p_login.add_argument(
        "--server",
        required=True,
        help="Server URL (e.g. https://loom.example.com)",
    )
    p_login.add_argument(
        "--token",
        type=secret_source_argparse_type("--token"),
        help=(
            "Token source. ONE of: 'env:VAR' (read os.environ[VAR]), "
            "'file:PATH' (read file content), or '-' (read stdin until "
            "EOF). Literal values are rejected — argv leaks via shell "
            "history and `ps -ef`."
        ),
    )
    p_login.add_argument("--username", help="Username for account login.")
    p_login.add_argument(
        "--password",
        type=secret_source_argparse_type("--password"),
        help="Password source for account login: env:VAR, file:PATH, or -.",
    )
    p_login.set_defaults(handler=_login)

    p_register = sub.add_parser(
        "register",
        help="Submit a username/team registration request for admin approval.",
    )
    p_register.add_argument("--server", required=True, help="Server URL.")
    p_register.add_argument("--username", required=True, help="Requested username.")
    p_register.add_argument("--team-id", required=True, help="Existing team UUID.")
    p_register.set_defaults(handler=_register)

    p_teams = sub.add_parser(
        "teams",
        help="List teams available for registration requests.",
    )
    p_teams.add_argument("--server", required=True, help="Server URL.")
    p_teams.set_defaults(handler=_teams)

    p_setup = sub.add_parser(
        "setup-password",
        help="Complete an admin-approved setup link by setting a password.",
    )
    p_setup.add_argument("--server", required=True, help="Server URL.")
    p_setup.add_argument(
        "--token",
        required=True,
        type=secret_source_argparse_type("--token"),
        help="Setup token source: env:VAR, file:PATH, or -.",
    )
    p_setup.add_argument(
        "--password",
        required=True,
        type=secret_source_argparse_type("--password"),
        help="Password source: env:VAR, file:PATH, or -.",
    )
    p_setup.add_argument(
        "--confirm-password",
        required=True,
        type=secret_source_argparse_type("--confirm-password"),
        help="Confirmation password source: env:VAR, file:PATH, or -.",
    )
    p_setup.set_defaults(handler=_setup_password)

    p_forgot = sub.add_parser(
        "forgot-password",
        help="Submit a password reset request for admin approval.",
    )
    p_forgot.add_argument("--server", required=True, help="Server URL.")
    p_forgot.add_argument("--username", required=True, help="Username to reset.")
    p_forgot.set_defaults(handler=_forgot_password)

    p_reset = sub.add_parser(
        "reset-password",
        help="Complete an admin-approved password reset link.",
    )
    p_reset.add_argument("--server", required=True, help="Server URL.")
    p_reset.add_argument(
        "--token",
        required=True,
        type=secret_source_argparse_type("--token"),
        help="Reset token source: env:VAR, file:PATH, or -.",
    )
    p_reset.add_argument(
        "--password",
        required=True,
        type=secret_source_argparse_type("--password"),
        help="Password source: env:VAR, file:PATH, or -.",
    )
    p_reset.add_argument(
        "--confirm-password",
        required=True,
        type=secret_source_argparse_type("--confirm-password"),
        help="Confirmation password source: env:VAR, file:PATH, or -.",
    )
    p_reset.set_defaults(handler=_reset_password)

    p_status = sub.add_parser(
        "status",
        help="Show stored server + token (redacted). Exit 0 if logged in.",
    )
    p_status.set_defaults(handler=_status)

    p_whoami = sub.add_parser(
        "whoami",
        help="Ask the server which principal, team, scopes, and token prefix are active.",
    )
    p_whoami.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    p_whoami.set_defaults(handler=_whoami)

    p_logout = sub.add_parser(
        "logout",
        help="Clear stored token (preserves server URL).",
    )
    p_logout.set_defaults(handler=_logout)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
