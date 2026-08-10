"""BEHAVIOR's thin facade over the single Pipeline canonical JSON authority."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loom.integrations.behavior.errors import CanonicalDocumentError
from loom.pipeline.keys import (
    canonical_digest,
    canonical_document,
    canonical_identity,
    digest_bytes,
)

__all__ = [
    "canonical_digest",
    "canonical_document",
    "canonical_identity",
    "digest_bytes",
    "load_canonical_document",
]


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise CanonicalDocumentError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_non_finite(value: str) -> None:
    raise CanonicalDocumentError(f"non-finite JSON number is forbidden: {value}")


def load_canonical_document(path: Path, *, max_bytes: int = 67_108_864) -> Any:
    """Read one bounded RFC8785/JCS document with exactly one trailing LF."""

    with path.open("rb") as stream:
        encoded = stream.read(max_bytes + 1)
    if len(encoded) > max_bytes:
        raise CanonicalDocumentError(f"document exceeds {max_bytes} bytes")
    if encoded.startswith(b"\xef\xbb\xbf"):
        raise CanonicalDocumentError("UTF-8 BOM is forbidden")
    if b"\r" in encoded:
        raise CanonicalDocumentError("CR and CRLF are forbidden")
    if not encoded.endswith(b"\n") or encoded.endswith(b"\n\n"):
        raise CanonicalDocumentError("document must end in exactly one LF")
    try:
        value = json.loads(
            encoded,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalDocumentError(str(exc)) from exc
    try:
        expected = canonical_document(value)
    except ValueError as exc:
        raise CanonicalDocumentError(str(exc)) from exc
    if encoded != expected:
        raise CanonicalDocumentError("document is not canonical RFC8785/JCS+LF")
    return value
