"""Thin httpx wrapper for the deployed Loom server.

Picks up `(server_url, auth_token)` from the CLI's persisted config
(set by `loom auth login`). Used by `loom providers`, `loom eval`,
and future server-talking subcommands.

Why a separate module: every server-talking command needs the same
"load config + assert logged in + build authed httpx client" preamble.
Centralizing keeps the auth-required UX consistent (same error
message + exit code when not logged in).
"""

from __future__ import annotations

from typing import Any

import httpx

from loom.security.redaction import redact_mapping, redact_text
from loom_cli.config import LoomConfig, load_config, save_config

LOGIN_HINT = (
    "Run `loom auth login --server URL --username USER --password env:PASS`, "
    "or use an API token with "
    "`loom auth login --server URL --token env:LOOM_API_TOKEN`."
)
SESSION_COOKIE_NAME = "loom_session"
CSRF_HEADER_NAME = "X-Loom-CSRF"
_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


class NotLoggedInError(Exception):
    """The persisted config doesn't have a (server_url, auth_token)
    pair. Caller prints a helpful message + exits non-zero."""


def require_logged_in() -> LoomConfig:
    """Load the CLI config; raise NotLoggedInError if not logged in.

    Used as the preamble for every server-talking command.
    """
    cfg = load_config()
    has_bearer = cfg.auth_token is not None
    has_session = cfg.auth_session_cookie is not None
    if cfg.server_url is None or not (has_bearer or has_session):
        raise NotLoggedInError(
            f"not logged in. {LOGIN_HINT}",
        )
    return cfg


def authed_client(
    cfg: LoomConfig,
    *,
    timeout: float = 30.0,
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """httpx.Client preconfigured with the server URL as base_url and
    the auth token in the default Authorization header. Callers use
    `with authed_client(cfg) as c: c.get('/api/v1/...')`.

    Synchronous httpx (not AsyncClient) because the CLI is a one-shot
    script — async ergonomics aren't worth the complexity here.
    """
    assert cfg.server_url is not None  # require_logged_in guarantees
    kwargs: dict[str, Any] = {
        "base_url": cfg.server_url,
        "timeout": timeout,
    }
    if transport is not None:
        kwargs["transport"] = transport
    if cfg.auth_token is not None:
        return httpx.Client(
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            **kwargs,
        )
    assert cfg.auth_session_cookie is not None
    headers = {}
    if cfg.auth_csrf_token is not None:
        headers[CSRF_HEADER_NAME] = cfg.auth_csrf_token
    return _SessionAuthClient(
        cfg,
        cookies={SESSION_COOKIE_NAME: cfg.auth_session_cookie},
        headers=headers,
        **kwargs,
    )


def persist_session_credentials_from_response(
    cfg: LoomConfig,
    response: httpx.Response,
    *,
    data: dict[str, Any] | None = None,
) -> bool:
    """Persist rotated browser-session credentials returned by auth routes."""
    if cfg.auth_session_cookie is None:
        return False
    changed = False
    session_cookie = response.cookies.get(SESSION_COOKIE_NAME)
    if session_cookie and session_cookie != cfg.auth_session_cookie:
        cfg.auth_session_cookie = session_cookie
        changed = True
    body = data
    if body is None:
        try:
            parsed = response.json()
        except Exception:
            parsed = None
        body = parsed if isinstance(parsed, dict) else None
    csrf_token = body.get("csrf_token") if isinstance(body, dict) else None
    if isinstance(csrf_token, str) and csrf_token and csrf_token != cfg.auth_csrf_token:
        cfg.auth_csrf_token = csrf_token
        changed = True
    if changed:
        save_config(cfg)
    return changed


def _response_is_csrf_rejection(response: httpx.Response) -> bool:
    if response.status_code != 403:
        return False
    return "csrf" in _response_detail(response).lower()


class _SessionAuthClient(httpx.Client):
    """httpx client that keeps CLI browser-session CSRF state fresh."""

    def __init__(self, cfg: LoomConfig, *args: Any, **kwargs: Any) -> None:
        self._loom_cfg = cfg
        self._loom_csrf_refreshed = False
        super().__init__(*args, **kwargs)

    def request(self, method: str, url: httpx.URL | str, **kwargs: Any) -> httpx.Response:
        unsafe = method.upper() in _UNSAFE_METHODS
        if unsafe:
            self._refresh_session_csrf()
        response = super().request(method, url, **kwargs)
        if unsafe and _response_is_csrf_rejection(response):
            self._refresh_session_csrf(force=True)
            response = super().request(method, url, **kwargs)
        return response

    def _refresh_session_csrf(self, *, force: bool = False) -> None:
        if self._loom_csrf_refreshed and not force:
            return
        self._loom_csrf_refreshed = True
        response = super().request("GET", "/api/v1/auth/me")
        if response.status_code // 100 != 2:
            return
        try:
            data = response.json()
        except Exception:
            data = None
        body = data if isinstance(data, dict) else None
        if persist_session_credentials_from_response(
            self._loom_cfg,
            response,
            data=body,
        ):
            if self._loom_cfg.auth_session_cookie is not None:
                self.cookies.clear()
                self.cookies.set(SESSION_COOKIE_NAME, self._loom_cfg.auth_session_cookie)
            if self._loom_cfg.auth_csrf_token is not None:
                self.headers[CSRF_HEADER_NAME] = self._loom_cfg.auth_csrf_token


class HttpStatusError(Exception):
    """A non-2xx HTTP response. Handlers catch + return exit code 1.

    Raising (not exiting) lets handlers stay testable via the
    `rc = main(...)` pattern instead of forcing every test to use
    pytest.raises(SystemExit).
    """


def assert_2xx(
    response: httpx.Response, *, action: str,
) -> dict[str, Any]:
    """If response is 2xx, return the parsed JSON. Otherwise raise
    HttpStatusError with a printable message naming the action. The
    handler's outer try/except prints to stderr + returns 1."""
    assert_2xx_response(response, action=action)
    if response.status_code == 204:  # no content
        return {}
    return response.json()  # type: ignore[no-any-return]


def assert_2xx_response(
    response: httpx.Response, *, action: str,
) -> httpx.Response:
    """Return a successful response, otherwise raise a redacted CLI error."""
    if response.status_code // 100 != 2:
        detail = _response_detail(response)
        if _response_is_csrf_rejection(response):
            raise HttpStatusError(
                f"CSRF token rejected by server while trying to {action}: "
                "browser-session CSRF is missing, expired, or stale. "
                "Run `loom auth whoami` or retry the command to refresh the "
                f"session CSRF.\n  {detail}",
            )
        if response.status_code in {401, 403}:
            raise HttpStatusError(
                f"token rejected by server while trying to {action}: "
                "revoked, expired, or missing scope. "
                f"{LOGIN_HINT}\n"
                f"  {detail}",
            )
        raise HttpStatusError(
            f"failed to {action}: HTTP {response.status_code}\n"
            f"  {detail}",
        )
    return response


def _response_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
        detail = body.get("detail") if isinstance(body, dict) else body
    except Exception:
        detail = response.text or "(no body)"
    if isinstance(detail, str):
        return redact_text(detail)
    return str(redact_mapping(detail))
