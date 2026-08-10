"""Single RFC 8785/JCS implementation for Pipeline identities and documents."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any
from uuid import UUID, uuid5

import rfc8785
from pydantic import BaseModel

MAX_SAFE_INTEGER = 9_007_199_254_740_991


class CanonicalizationError(ValueError):
    """Raised when a value cannot be represented by the Pipeline JCS contract."""


def _plain_json(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _plain_json(value.model_dump(mode="python", exclude_none=False))
    if isinstance(value, Enum):
        return _plain_json(value.value)
    if isinstance(value, UUID):
        return str(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            raise CanonicalizationError("integer is outside the interoperable JSON range")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError("NaN and Infinity are forbidden")
        return value
    if isinstance(value, str):
        # UTF-8 encoding rejects lone surrogates.  Do this here so every caller
        # receives the same stable domain error rather than an encoder detail.
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise CanonicalizationError("lone Unicode surrogate is forbidden") from exc
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError("JSON object keys must be strings")
            key.encode("utf-8", errors="strict")
            result[key] = _plain_json(item)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, memoryview)):
        return [_plain_json(item) for item in value]
    raise CanonicalizationError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_identity(value: Any) -> bytes:
    """Return raw JCS bytes for an identity/idempotency/UUIDv5 preimage."""

    try:
        return rfc8785.dumps(_plain_json(value))
    except (rfc8785.CanonicalizationError, UnicodeError, ValueError) as exc:
        if isinstance(exc, CanonicalizationError):
            raise
        raise CanonicalizationError(str(exc)) from exc


def canonical_document(value: Any) -> bytes:
    """Return persisted JCS bytes, including the one required ASCII LF."""

    return canonical_identity(value) + b"\n"


def digest_bytes(value: bytes) -> str:
    """Return the normative lowercase SHA-256 identifier for exact bytes."""

    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def canonical_digest(value: Any, *, persisted: bool = True) -> str:
    """Digest either persisted JCS+LF bytes or a raw identity preimage."""

    encoded = canonical_document(value) if persisted else canonical_identity(value)
    return digest_bytes(encoded)


def canonical_uuid5(namespace: UUID, value: Any) -> UUID:
    """Create a UUIDv5 whose name is the raw (no-LF) JCS UTF-8 text."""

    return uuid5(namespace, canonical_identity(value).decode("utf-8"))
