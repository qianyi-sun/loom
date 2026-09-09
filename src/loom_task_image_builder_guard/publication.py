"""Closed nonsecret publication status wire; not readiness or execution authority.

All admitted strings are ASCII and integers are RFC 8785-safe, so sorted compact
stdlib JSON is byte-identical to RFC 8785 for this particular closed schema.
The guard binds request IDs and receipt consistency, not the candidate identity
set it cannot reconstruct. The supervisor must check that complete set itself.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from loom_task_image_builder_guard.errors import GuardError

MAX_PUBLICATION_STATUS_BYTES = 4096
_MAX_RECEIPT_BYTES = 2048
_MAX_SAFE_INTEGER = (1 << 53) - 1
_DIGEST = re.compile(r"[0-9a-f]{64}")
_BOUND_FIELDS = frozenset(
    {
        "operation_id",
        "materialization_id",
        "attempt_id",
        "lease_epoch",
        "snapshot_sha256",
        "candidate_set_sha256",
        "component_count",
    }
)
_STATUS_FIELDS = _BOUND_FIELDS | {"schema", "grant_id", "state"}
_RECEIPT_FIELDS = _BOUND_FIELDS | {
    "schema",
    "worker_generation",
    "publication_set_sha256",
    "completed_at",
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    if len(pairs) > 12 or len({key for key, _ in pairs}) != len(pairs):
        raise ValueError("invalid object")
    return dict(pairs)


def _uuid(value: object) -> None:
    if not isinstance(value, str):
        raise ValueError("invalid UUID")
    parsed = UUID(value)
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("invalid UUID")


def _integer(value: object, maximum: int = _MAX_SAFE_INTEGER) -> None:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError("invalid integer")


def _digest(value: object) -> None:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None or value == "0" * 64:
        raise ValueError("invalid digest")


@dataclass(frozen=True, slots=True)
class PublicationStatus:
    """Own canonical immutable bytes; revalidate adapter results at the service."""

    canonical_bytes: bytes

    def as_dict(self) -> dict[str, object]:
        """Return a new document, never an alias to checked mutable inputs."""
        return cast(dict[str, object], json.loads(self.canonical_bytes))


def parse_publication_status(
    payload: bytes,
    *,
    grant_id: UUID,
    operation_id: UUID,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
) -> PublicationStatus:
    """Revalidate exact canonical wire and fixed IDs without reflecting errors."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_PUBLICATION_STATUS_BYTES:
            raise ValueError("invalid bytes")
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs)
        if not isinstance(value, dict) or not isinstance(value.get("state"), str):
            raise ValueError("invalid object")
        state = value["state"]
        if state not in {"queued", "running", "failed", "completed"}:
            raise ValueError("invalid state")
        extra = (
            {"receipt"}
            if state == "completed"
            else {"failure_code"}
            if state == "failed"
            else set()
        )
        if (
            set(value) != _STATUS_FIELDS | extra
            or value["schema"] != "loom.task-image-publication-status/v1"
        ):
            raise ValueError("invalid schema")
        expected: dict[str, object] = {
            "grant_id": str(grant_id),
            "operation_id": str(operation_id),
            "materialization_id": str(materialization_id),
            "attempt_id": str(attempt_id),
            "lease_epoch": lease_epoch,
        }
        _integer(lease_epoch)
        for field in ("grant_id", "operation_id", "materialization_id", "attempt_id"):
            _uuid(value[field])
        for field, binding in expected.items():
            if value[field] != binding:
                raise ValueError("invalid request binding")
        _integer(value["lease_epoch"])
        _integer(value["component_count"], 128)
        _digest(value["snapshot_sha256"])
        _digest(value["candidate_set_sha256"])
        if state == "failed" and (
            not isinstance(value["failure_code"], str)
            or value["failure_code"]
            not in {"integrity", "authority_lost", "verification_failed", "deadline"}
        ):
            raise ValueError("invalid failure")
        if state == "completed":
            receipt = value["receipt"]
            if not isinstance(receipt, dict) or set(receipt) != _RECEIPT_FIELDS:
                raise ValueError("invalid receipt")
            if receipt["schema"] != "loom.task-image-publication-receipt/v1":
                raise ValueError("invalid receipt schema")
            for field in _BOUND_FIELDS:
                # Strict type comparison rejects bool/float equality to integers.
                if type(receipt[field]) is not type(value[field]) or receipt[field] != value[field]:
                    raise ValueError("invalid receipt binding")
            _integer(receipt["worker_generation"])
            _digest(receipt["publication_set_sha256"])
            completed = receipt["completed_at"]
            if not isinstance(completed, str) or not completed.isascii() or len(completed) != 20:
                raise ValueError("invalid completion time")
            parsed = datetime.strptime(completed, "%Y-%m-%dT%H:%M:%SZ")
            if parsed.isoformat(timespec="seconds") + "Z" != completed:
                raise ValueError("invalid completion time")
            if len(_canonical(receipt)) > _MAX_RECEIPT_BYTES:
                raise ValueError("receipt too large")
        if _canonical(value) != payload:
            raise ValueError("noncanonical status")
        return PublicationStatus(payload)
    except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
        raise GuardError("authority_publication_invalid") from None
