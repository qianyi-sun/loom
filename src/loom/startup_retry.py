"""Bounded retries for startup dependency probes.

These helpers are intentionally narrow: they retry transient dependency
resolution and transport failures during process startup, while preserving
immediate hard failures for deterministic configuration or schema errors.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

import httpx
from sqlalchemy.exc import DBAPIError

T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StartupRetryConfig:
    max_attempts: int = 12
    base_backoff_sec: float = 1.0
    max_backoff_sec: float = 10.0
    budget_sec: float = 120.0
    jitter_sec: float = 0.25


DEFAULT_STARTUP_RETRY_CONFIG = StartupRetryConfig()


_PERMANENT_DB_ERROR_FRAGMENTS = (
    "password authentication failed",
    "role \"",
    "does not exist",
    "database \"",
    "no pg_hba.conf entry",
    "permission denied",
    "invalid authorization specification",
)

_TRANSIENT_DB_ERROR_FRAGMENTS = (
    "temporary failure in name resolution",
    "name or service not known",
    "nodename nor servname provided",
    "could not translate host name",
    "connection refused",
    "could not connect",
    "connection timed out",
    "timeout expired",
    "server closed the connection unexpectedly",
    "the database system is starting up",
    "database system is starting up",
    "terminating connection",
    "connection not open",
    "connection reset",
)

_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


def is_retryable_startup_exception(exc: BaseException) -> bool:
    """Return true for startup dependency failures worth retrying."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_HTTP_STATUSES

    if isinstance(exc, (
        httpx.TimeoutException,
        httpx.NetworkError,
        httpx.RemoteProtocolError,
    )):
        return True

    if isinstance(exc, DBAPIError):
        return bool(exc.connection_invalidated) or _is_retryable_db_exception(exc)

    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True

    if isinstance(exc, OSError):
        return _contains_any(str(exc), _TRANSIENT_DB_ERROR_FRAGMENTS)

    return False


async def retry_startup_dependency(
    run: Callable[[], Awaitable[T]],
    *,
    operation_name: str,
    is_retryable: Callable[[BaseException], bool] = is_retryable_startup_exception,
    config: StartupRetryConfig = DEFAULT_STARTUP_RETRY_CONFIG,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    now: Callable[[], float] = time.monotonic,
) -> T:
    max_attempts = max(1, int(config.max_attempts))
    base_backoff = max(0.0, float(config.base_backoff_sec))
    max_backoff = max(0.0, float(config.max_backoff_sec))
    budget = max(0.0, float(config.budget_sec))
    jitter = max(0.0, float(config.jitter_sec))
    started = now()

    for attempt in range(1, max_attempts + 1):
        try:
            return await run()
        except Exception as exc:
            if attempt >= max_attempts or not is_retryable(exc):
                raise

            wait_for = _next_backoff(
                attempt=attempt,
                base=base_backoff,
                jitter=jitter,
                cap=max_backoff,
            )
            if (now() - started) + wait_for > budget:
                raise

            logger.warning(
                "startup_dependency_retry operation=%s attempt=%d/%d wait=%.2fs err=%r",
                operation_name,
                attempt,
                max_attempts,
                wait_for,
                exc,
            )
            await sleep(wait_for)

    raise RuntimeError("unreachable startup retry loop exit")  # pragma: no cover


def _is_retryable_db_exception(exc: BaseException) -> bool:
    message = str(exc).lower()
    if _contains_any(message, _PERMANENT_DB_ERROR_FRAGMENTS):
        return False
    if _contains_any(message, _TRANSIENT_DB_ERROR_FRAGMENTS):
        return True

    orig = getattr(exc, "orig", None)
    if orig is not None and orig is not exc:
        orig_message = str(orig).lower()
        if _contains_any(orig_message, _PERMANENT_DB_ERROR_FRAGMENTS):
            return False
        if _contains_any(orig_message, _TRANSIENT_DB_ERROR_FRAGMENTS):
            return True
        if isinstance(orig, (ConnectionError, TimeoutError)):
            return True
        if isinstance(orig, OSError):
            return _contains_any(orig_message, _TRANSIENT_DB_ERROR_FRAGMENTS)

    return False


def _contains_any(text: str, fragments: tuple[str, ...]) -> bool:
    return any(fragment in text for fragment in fragments)


def _next_backoff(
    *, attempt: int, base: float, jitter: float, cap: float,
) -> float:
    raw: float = base * (2 ** (attempt - 1))
    if jitter > 0:
        raw += random.uniform(-jitter, jitter)
    return float(max(0.0, min(raw, cap)))
