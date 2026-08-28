"""Canonical secret-free receipts for bounded personal-dev isolation probes."""

from __future__ import annotations

import hashlib
import json

EXPECTED_HIDDEN_DENIAL_ERROR = (
    "error: expected hidden-resource denial was not observed\n"
)
EXPECTED_HIDDEN_DENIAL_PHASE_HEADER = "X-Loom-Personal-Dev-Hidden-Denial-Phase"

_REQUEST_BINDINGS = {
    "read": ("GET", "target_read"),
    "update": ("PUT", "target_update"),
    "destroy": ("DELETE", "target_destroy"),
}


def expected_hidden_denial_phase(operation: str) -> str:
    """Return the exact server-origin phase marker for one target request."""

    try:
        return _REQUEST_BINDINGS[operation][1]
    except KeyError as exc:
        raise ValueError("expected hidden-denial operation is invalid") from exc


def expected_hidden_denial_receipt(operation: str) -> bytes:
    """Return the canonical allowlisted receipt for one exact target request."""

    try:
        method, target_phase = _REQUEST_BINDINGS[operation]
    except KeyError as exc:
        raise ValueError("expected hidden-denial operation is invalid") from exc
    record = {
        "error_code": "resource_hidden",
        "http_method": method,
        "schema": "loom-personal-dev-expected-hidden-denial-v1",
        "status": 404,
        "target_phase": target_phase,
    }
    return (
        json.dumps(record, separators=(",", ":"), sort_keys=True).encode("ascii")
        + b"\n"
    )


def expected_hidden_denial_sha256(operation: str) -> str:
    """Return the SHA-256 bound by strict acceptance-result evidence."""

    return hashlib.sha256(expected_hidden_denial_receipt(operation)).hexdigest()


__all__ = [
    "EXPECTED_HIDDEN_DENIAL_ERROR",
    "EXPECTED_HIDDEN_DENIAL_PHASE_HEADER",
    "expected_hidden_denial_phase",
    "expected_hidden_denial_receipt",
    "expected_hidden_denial_sha256",
]
