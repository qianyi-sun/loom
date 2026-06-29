"""Central redaction helpers for public Loom surfaces."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

ShareStatus = Literal["shared", "blocked"]


@dataclass(frozen=True)
class RedactionDecision:
    status: ShareStatus
    reason: str | None = None


_TOKEN_RE = re.compile(
    r"\bloom_(?:admin|api|invite|team|w|session|csrf|login)_"
    r"[A-Za-z0-9._~+/=-]+",
)
_OPENAI_STYLE_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{6,}\b")
_HUGGINGFACE_TOKEN_RE = re.compile(r"\bhf_[A-Za-z0-9]{20,}\b")
_BEARER_RE = re.compile(
    r"(?i)\b(Authorization\s*:\s*Bearer|Bearer)\s+"
    r"(?!\[REDACTED[^\]]*\])[^,;\s'\"]+",
)
_SECRET_REF_RE = re.compile(
    r"\b(?:loom|k8s-secret)://[A-Za-z0-9._~:/?#[\]@!$&'()*+,;=%-]+",
)
_SIGNED_URL_RE = re.compile(
    r"https?://[^,\s'\"]*(?:X-Amz-Signature|X-Amz-Credential|"
    r"X-Amz-Algorithm|X-Amz-Security-Token|Signature=)[^,\s'\"]*",
    re.IGNORECASE,
)
_SIGNED_URL_PARAM_RE = re.compile(
    r"(?i)\b(?:X-Amz-Signature|X-Amz-Credential|X-Amz-Security-Token|"
    r"Signature|AWSAccessKeyId)=([^&\s'\"]+)",
)
_INTERNAL_URL_RE = re.compile(
    r"https?://(?:loom-control-plane|loom-llm-gateway|loom-worker|"
    r"control-plane|llm-gateway|minio)(?:[.:][^/\s'\"]*)?[^,\s'\"]*",
    re.IGNORECASE,
)

_SENSITIVE_KEYS = {
    "authorization",
    "cookie",
    "set-cookie",
    "x-loom-csrf",
    "x-csrf-token",
    "csrf",
    "csrf_token",
    "api_key",
    "apikey",
    "access_key",
    "secret_key",
    "token",
    "auth_token",
    "invite_code",
    "provider_api_key",
    "password",
}


def redact_text(value: str, *, limit: int | None = None) -> str:
    """Redact secret-like material from text while preserving diagnostics."""

    out = value if limit is None else value[:limit]
    replacements = (
        (_SIGNED_URL_RE, "[REDACTED:signed-url]"),
        (_SIGNED_URL_PARAM_RE, lambda m: f"{m.group(0).split('=', 1)[0]}=[REDACTED]"),
        (_INTERNAL_URL_RE, "[REDACTED:internal-url]"),
        (_SECRET_REF_RE, "[REDACTED:secret-ref]"),
        (_BEARER_RE, lambda m: f"{m.group(1)} [REDACTED:bearer]"),
        (_TOKEN_RE, "[REDACTED:loom-token]"),
        (_OPENAI_STYLE_KEY_RE, "[REDACTED:api-key]"),
        (_HUGGINGFACE_TOKEN_RE, "[REDACTED:hf-token]"),
    )
    for pattern, replacement in replacements:
        out = pattern.sub(replacement, out)
    return out


def redact_mapping(value: Any) -> Any:
    """Recursively redact a JSON-like value without changing its shape."""

    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in _SENSITIVE_KEYS:
                out[key_text] = f"[REDACTED:{key_text}]"
            else:
                out[key_text] = redact_mapping(item)
        return out
    if isinstance(value, list):
        return [redact_mapping(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_mapping(item) for item in value)
    if isinstance(value, str):
        return redact_text(value)
    return value


def contains_secret_like_content(content: bytes | str) -> RedactionDecision:
    """Classify artifact content for org-wide sharing.

    This is intentionally conservative: a positive match blocks org sharing,
    while owner-team diagnostics can still retain the raw artifact.
    """

    text = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    if redact_text(text) != text:
        return RedactionDecision(
            status="blocked",
            reason="secret-like content detected",
        )
    return RedactionDecision(status="shared", reason=None)
