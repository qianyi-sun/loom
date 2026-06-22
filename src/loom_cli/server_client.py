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
from loom_cli.config import LoomConfig, load_config

LOGIN_HINT = (
    "Create a team API token in the web UI, then run "
    "`export LOOM_API_TOKEN=loom_api_...` and "
    "`loom auth login --server URL --token env:LOOM_API_TOKEN`."
)


class NotLoggedInError(Exception):
    """The persisted config doesn't have a (server_url, auth_token)
    pair. Caller prints a helpful message + exits non-zero."""


def require_logged_in() -> LoomConfig:
    """Load the CLI config; raise NotLoggedInError if not logged in.

    Used as the preamble for every server-talking command.
    """
    cfg = load_config()
    if cfg.server_url is None or cfg.auth_token is None:
        raise NotLoggedInError(
            f"not logged in. {LOGIN_HINT}",
        )
    return cfg


def authed_client(cfg: LoomConfig, *, timeout: float = 30.0) -> httpx.Client:
    """httpx.Client preconfigured with the server URL as base_url and
    the auth token in the default Authorization header. Callers use
    `with authed_client(cfg) as c: c.get('/api/v1/...')`.

    Synchronous httpx (not AsyncClient) because the CLI is a one-shot
    script — async ergonomics aren't worth the complexity here.
    """
    assert cfg.server_url is not None  # require_logged_in guarantees
    assert cfg.auth_token is not None
    return httpx.Client(
        base_url=cfg.server_url,
        headers={"Authorization": f"Bearer {cfg.auth_token}"},
        timeout=timeout,
    )


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
