"""Runtime probe for OpenAI-Responses-API support at a given gateway URL.

Used by SubprocessAgent to decide whether the codex adapter's `wire_api`
should stay at "responses" (default; targets OpenAI's Responses endpoint)
or fall back to "chat" (targets OpenAI Chat Completions).

Background: some BYO OpenAI-compatible providers (yibuapi, and any other
provider that only proxies `/v1/chat/completions`) return 504 or 501 when
codex issues its Responses-flavored requests, causing codex to burn its
retry budget and the trial's entire agent-timeout window with zero
successful LLM calls (see #277). This probe surfaces the gap before
codex ever starts so we can switch to `/chat/completions` where the
provider is functional.

Fail-closed policy: any 5xx / timeout / connect-error is treated as
"unsupported" so codex falls back to /chat/completions. False negatives
here just cost one config knob at startup; false positives would let
codex hang for 40 minutes.

The result is cached per `base_url` at module scope so we probe each
gateway URL at most once per worker process lifetime.
"""

from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

_PROBE_CACHE: dict[str, bool] = {}
_PROBE_CACHE_LOCK = asyncio.Lock()

_PROBE_TIMEOUT_SEC = 5.0
# Interpretation table (see module docstring):
_SUPPORTED_STATUSES = frozenset({200, 400, 401})
_UNSUPPORTED_STATUSES = frozenset({404, 501})


async def responses_api_supported(
    *,
    base_url: str,
    token: str,
    model: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> bool:
    """Return True iff `POST {base_url}/responses` appears to reach a
    real Responses-API implementation. Cached at module level per
    `base_url`. `transport` is a test seam — production callers omit
    it and get the default HTTPS transport.
    """
    normalised = base_url.rstrip("/")
    if normalised in _PROBE_CACHE:
        return _PROBE_CACHE[normalised]
    async with _PROBE_CACHE_LOCK:
        if normalised in _PROBE_CACHE:
            return _PROBE_CACHE[normalised]
        result = await _probe_once(
            base_url=normalised, token=token, model=model, transport=transport,
        )
        _PROBE_CACHE[normalised] = result
        return result


async def _probe_once(
    *,
    base_url: str,
    token: str,
    model: str,
    transport: httpx.AsyncBaseTransport | None,
) -> bool:
    url = f"{base_url}/responses"
    payload = {
        "model": model,
        # Deliberately minimal — the probe cares about routing shape,
        # not model behavior. `max_output_tokens=1` keeps the upstream
        # bill negligible if the endpoint IS real and honours the call.
        "input": "hi",
        "max_output_tokens": 1,
    }
    async with httpx.AsyncClient(
        timeout=_PROBE_TIMEOUT_SEC, transport=transport,
    ) as client:
        try:
            response = await client.post(
                url,
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
        except (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.RemoteProtocolError,
            httpx.TransportError,
        ) as exc:
            logger.info(
                "responses_api_probe transport error base_url=%s err=%s",
                base_url, type(exc).__name__,
            )
            return False
    if response.status_code in _SUPPORTED_STATUSES:
        return True
    if response.status_code in _UNSUPPORTED_STATUSES:
        return False
    if 500 <= response.status_code < 600:
        return False
    # Other 4xx (403 forbidden, 429 rate-limited, …) mean the endpoint
    # is reachable AND authoritative about our request; we assume the
    # provider implements Responses even though this particular request
    # was rejected.
    return True
