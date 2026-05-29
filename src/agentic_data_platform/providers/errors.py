from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ProviderErrorCode(str, Enum):
    RATE_LIMITED = "rate_limited"
    AUTH_FAILED = "auth_failed"
    TIMEOUT = "timeout"
    INVALID_REQUEST = "invalid_request"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ProviderBoundaryError(Exception):
    code: ProviderErrorCode
    message: str
    retryable: bool = False
    status_code: int | None = None

    def __str__(self) -> str:
        return self.message


def normalize_provider_error(exc: Exception) -> ProviderBoundaryError:
    status_code = _status_code(exc)
    message = str(exc) or exc.__class__.__name__
    lower_message = message.lower()

    if status_code == 429 or "429" in lower_message or "rate limit" in lower_message or "rate limited" in lower_message:
        return ProviderBoundaryError(
            code=ProviderErrorCode.RATE_LIMITED,
            message=message,
            retryable=True,
            status_code=status_code,
        )
    if status_code in {401, 403} or "401" in lower_message or "403" in lower_message or isinstance(exc, PermissionError):
        return ProviderBoundaryError(
            code=ProviderErrorCode.AUTH_FAILED,
            message=message,
            retryable=False,
            status_code=status_code,
        )
    if isinstance(exc, TimeoutError) or "timeout" in lower_message or "timed out" in lower_message:
        return ProviderBoundaryError(
            code=ProviderErrorCode.TIMEOUT,
            message=message,
            retryable=True,
            status_code=status_code,
        )
    if isinstance(exc, ValueError):
        return ProviderBoundaryError(
            code=ProviderErrorCode.INVALID_REQUEST,
            message=message,
            retryable=False,
            status_code=status_code,
        )
    if status_code is not None and status_code >= 500:
        return ProviderBoundaryError(
            code=ProviderErrorCode.UNAVAILABLE,
            message=message,
            retryable=True,
            status_code=status_code,
        )
    return ProviderBoundaryError(
        code=ProviderErrorCode.UNKNOWN,
        message=message,
        retryable=False,
        status_code=status_code,
    )


def _status_code(exc: Exception) -> int | None:
    value = getattr(exc, "status_code", None)
    if isinstance(value, int):
        return value
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None
