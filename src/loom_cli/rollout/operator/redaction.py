"""Fail-closed redaction for rollout-owned persisted text and JSON."""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from loom.security.redaction import is_sensitive_environment_key, redact_text

_KNOWN_SECRETS: ContextVar[tuple[str, ...]] = ContextVar(
    "rollout_known_secrets",
    default=(),
)
_REDACTION_MARKER_RE = re.compile(r"\[REDACTED(?::[^\]\r\n]*)?\]")
_PEM_BLOCK_RE = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]*(?:PRIVATE KEY|CERTIFICATE))-----.*?"
    r"-----END (?P=label)-----",
    re.DOTALL,
)
_UNTERMINATED_PEM_RE = re.compile(
    r"-----BEGIN [A-Z0-9 ]*(?:PRIVATE KEY|CERTIFICATE)-----.*\Z",
    re.DOTALL,
)
_CREDENTIAL_URL_RE = re.compile(
    r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/?#@:]*:[^\s/?#@]+@[^\s]+",
)
_SENSITIVE_TEXT_KEY = (
    r"(?:auth[_-]?token|token|api[_-]?key|"
    r"access[_-]?key|secret(?:[_-]?key)?|password|passwd|credential(?:s)?)"
)
_QUOTED_SENSITIVE_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![A-Za-z0-9_])[\"']?"
    rf"(?P<key>{_SENSITIVE_TEXT_KEY})[\"']?\s*(?:=|:)\s*)"
    r"(?P<quote>[\"'])(?P<value>(?:\\.|(?!(?P=quote))[^\r\n])*)(?P=quote)",
)
_BARE_SENSITIVE_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![A-Za-z0-9_])[\"']?"
    rf"(?P<key>{_SENSITIVE_TEXT_KEY})[\"']?\s*(?:=|:)\s*)"
    r"(?P<value>(?![\"']|\[REDACTED)[^\s,;}\]]+)",
)
_SENSITIVE_MAPPING_KEYS = {
    "authorization",
    "auth",
    "auth_token",
    "bearer",
    "cookie",
    "set-cookie",
    "x-loom-csrf",
    "x-csrf-token",
    "csrf",
    "csrf_token",
    "api_key",
    "apikey",
    "access_key",
    "secret",
    "secret_key",
    "token",
    "invite_code",
    "provider_api_key",
    "password",
    "passwd",
    "credential",
    "credentials",
}
_MAX_SECRET_SOURCE_BYTES = 1024 * 1024


def _normalized_secrets(values: Iterable[str]) -> tuple[str, ...]:
    # Short strings are too collision-prone to replace safely in diagnostics.
    expanded: set[str] = set()
    for value in values:
        if not isinstance(value, str) or len(value) < 4:
            continue
        expanded.add(value)
        escaped = json.dumps(value, ensure_ascii=True)[1:-1]
        if len(escaped) >= 4:
            expanded.add(escaped)
    return tuple(
        sorted(
            expanded,
            key=len,
            reverse=True,
        )
    )


def _replace_exact_outside_markers(value: str, secret: str) -> str:
    """Replace one exact value without rewriting an existing redaction marker."""
    parts: list[str] = []
    cursor = 0
    for match in _REDACTION_MARKER_RE.finditer(value):
        parts.append(value[cursor : match.start()].replace(secret, "[REDACTED:known-secret]"))
        parts.append(match.group(0))
        cursor = match.end()
    parts.append(value[cursor:].replace(secret, "[REDACTED:known-secret]"))
    return "".join(parts)


def _redact_quoted_assignment(match: re.Match[str]) -> str:
    key = match.group("key").lower()
    quote = match.group("quote")
    return f"{match.group('prefix')}{quote}[REDACTED:{key}]{quote}"


def _redact_bare_assignment(match: re.Match[str]) -> str:
    key = match.group("key").lower()
    return f"{match.group('prefix')}[REDACTED:{key}]"


def _is_sensitive_mapping_key(key: str) -> bool:
    return key.lower() in _SENSITIVE_MAPPING_KEYS or is_sensitive_environment_key(key)


def redact_rollout_text(
    value: str,
    *,
    known_secrets: Iterable[str] = (),
    limit: int | None = None,
) -> str:
    """Replace exact protected values, structural credentials, then Loom patterns."""
    out = value
    secrets = _normalized_secrets((*_KNOWN_SECRETS.get(), *known_secrets))
    for secret in secrets:
        out = _replace_exact_outside_markers(out, secret)
    out = _PEM_BLOCK_RE.sub("[REDACTED:pem]", out)
    out = _UNTERMINATED_PEM_RE.sub("[REDACTED:pem]", out)
    out = _CREDENTIAL_URL_RE.sub("[REDACTED:credential-url]", out)
    out = _QUOTED_SENSITIVE_VALUE_RE.sub(_redact_quoted_assignment, out)
    out = _BARE_SENSITIVE_VALUE_RE.sub(_redact_bare_assignment, out)
    out = redact_text(out)
    return out if limit is None else out[:limit]


def redact_rollout_mapping(
    value: Any,
    *,
    known_secrets: Iterable[str] = (),
) -> Any:
    """Recursively redact a JSON-compatible value without changing its shape."""
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            rendered_key = str(key)
            if _is_sensitive_mapping_key(rendered_key):
                redacted[rendered_key] = f"[REDACTED:{rendered_key}]"
            else:
                redacted[rendered_key] = redact_rollout_mapping(
                    item,
                    known_secrets=known_secrets,
                )
        return redacted
    if isinstance(value, list):
        return [redact_rollout_mapping(item, known_secrets=known_secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_rollout_mapping(item, known_secrets=known_secrets) for item in value)
    if isinstance(value, str):
        return redact_rollout_text(value, known_secrets=known_secrets)
    return value


@contextmanager
def rollout_redaction_scope(known_secrets: Iterable[str]) -> Iterator[None]:
    """Install request-local exact values for every central persistence boundary."""
    token = _KNOWN_SECRETS.set(_normalized_secrets((*_KNOWN_SECRETS.get(), *known_secrets)))
    try:
        yield
    finally:
        _KNOWN_SECRETS.reset(token)


def known_secrets_from_sources(
    sources: Iterable[str | None],
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Load configured secret values in memory and include their protected references."""
    env = os.environ if environ is None else environ
    values: list[str] = []
    for source in sources:
        if not source:
            continue
        values.append(source)
        if source.startswith("file:"):
            rendered_path = source.removeprefix("file:")
            values.append(rendered_path)
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            try:
                fd = os.open(Path(rendered_path), flags)
            except OSError:
                continue
            try:
                metadata = os.fstat(fd)
                mode = stat.S_IMODE(metadata.st_mode)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_size > _MAX_SECRET_SOURCE_BYTES
                    or metadata.st_nlink != 1
                    or mode & 0o027
                ):
                    continue
                payload = bytearray()
                while len(payload) <= _MAX_SECRET_SOURCE_BYTES:
                    chunk = os.read(fd, min(65536, _MAX_SECRET_SOURCE_BYTES + 1 - len(payload)))
                    if not chunk:
                        break
                    payload.extend(chunk)
                if len(payload) > _MAX_SECRET_SOURCE_BYTES:
                    continue
                final_metadata = os.fstat(fd)
                if (
                    final_metadata.st_dev,
                    final_metadata.st_ino,
                    final_metadata.st_size,
                    final_metadata.st_mtime_ns,
                ) != (
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_size,
                    metadata.st_mtime_ns,
                ):
                    continue
                raw = bytes(payload).decode("utf-8")
            except (OSError, UnicodeError):
                continue
            finally:
                os.close(fd)
            values.extend((raw, raw.strip()))
            values.extend(line for line in raw.splitlines() if line.strip())
        elif source.startswith("env:"):
            name = source.removeprefix("env:")
            if name and name in env:
                values.append(env[name])
    return _normalized_secrets(values)


__all__ = [
    "known_secrets_from_sources",
    "redact_rollout_mapping",
    "redact_rollout_text",
    "rollout_redaction_scope",
]
