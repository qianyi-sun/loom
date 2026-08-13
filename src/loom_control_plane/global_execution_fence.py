"""Fail-closed coexistence fence for legacy capacity scale-up writers.

The executable capacity manager owns any non-shadow execution epoch.  Legacy
autoscalers may continue their drain-safe paths, but may create capacity only
when the authenticated manager witness proves the manager remains in its
inert, zero-ceiling shadow state.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

_MAX_WITNESS_BYTES = 64 * 1024
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_STATES = frozenset({"shadow", "prepared", "active", "drain-only"})
_FIELDS = frozenset(
    {
        "authority",
        "pool_id",
        "execution_epoch",
        "execution_state",
        "executable_new_capacity_ceiling",
        "expires_at",
        "authenticated",
        "canonical_digest",
    }
)


class GlobalExecutionFenceError(ValueError):
    """The manager state cannot safely coexist with a legacy scale-up writer."""


def _exact_identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise GlobalExecutionFenceError(f"global execution witness {field} is invalid")
    return value


def _quantity(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise GlobalExecutionFenceError(f"global execution witness {field} is invalid")
    return value


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise GlobalExecutionFenceError("global execution witness expiry is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GlobalExecutionFenceError("global execution witness expiry is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise GlobalExecutionFenceError("global execution witness expiry is invalid")
    return parsed.astimezone(UTC)


def _canonical_digest(value: Mapping[str, object]) -> str:
    payload = {key: item for key, item in value.items() if key != "canonical_digest"}
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class GlobalExecutionWitness:
    """A bounded manager state witness supplied over authenticated transport."""

    authority: str
    pool_id: str
    execution_epoch: int
    execution_state: str
    executable_new_capacity_ceiling: int
    expires_at: datetime
    authenticated: bool
    canonical_digest: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> GlobalExecutionWitness:
        if set(value) != _FIELDS:
            raise GlobalExecutionFenceError("global execution witness fields are invalid")
        digest = value["canonical_digest"]
        if not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None:
            raise GlobalExecutionFenceError("global execution witness digest is invalid")
        if digest != _canonical_digest(value):
            raise GlobalExecutionFenceError("global execution witness digest does not match")
        state = value["execution_state"]
        if not isinstance(state, str) or state not in _STATES:
            raise GlobalExecutionFenceError("global execution witness state is invalid")
        authenticated = value["authenticated"]
        if type(authenticated) is not bool:
            raise GlobalExecutionFenceError("global execution witness authentication is invalid")
        return cls(
            authority=_exact_identifier(value["authority"], "authority"),
            pool_id=_exact_identifier(value["pool_id"], "pool"),
            execution_epoch=_quantity(value["execution_epoch"], "epoch"),
            execution_state=state,
            executable_new_capacity_ceiling=_quantity(
                value["executable_new_capacity_ceiling"],
                "ceiling",
            ),
            expires_at=_timestamp(value["expires_at"]),
            authenticated=authenticated,
            canonical_digest=digest,
        )


def load_global_execution_witness(path: Path | None) -> GlobalExecutionWitness | None:
    """Read one bounded manager witness without treating absent evidence as safe."""

    if path is None:
        return None
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise GlobalExecutionFenceError("global execution witness is unavailable") from exc
    if not raw or len(raw) > _MAX_WITNESS_BYTES:
        raise GlobalExecutionFenceError("global execution witness is unavailable")
    try:
        decoded = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GlobalExecutionFenceError("global execution witness is invalid") from exc
    if not isinstance(decoded, dict):
        raise GlobalExecutionFenceError("global execution witness is invalid")
    return GlobalExecutionWitness.from_mapping(cast(Mapping[str, object], decoded))


def assert_legacy_scale_up_allowed(
    witness: GlobalExecutionWitness | None,
    *,
    expected_authority: str,
    expected_pool_id: str,
    now: datetime | None = None,
    required: bool,
) -> None:
    """Raise unless the exact manager scope is freshly authenticated shadow state."""

    if witness is None:
        if required:
            raise GlobalExecutionFenceError("global execution witness is unavailable")
        return
    now = now or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise GlobalExecutionFenceError("global execution fence clock is invalid")
    if not witness.authenticated:
        raise GlobalExecutionFenceError("global execution witness is not authenticated")
    if witness.authority != _exact_identifier(expected_authority, "authority"):
        raise GlobalExecutionFenceError("global execution witness authority does not match")
    if witness.pool_id != _exact_identifier(expected_pool_id, "pool"):
        raise GlobalExecutionFenceError("global execution witness pool does not match")
    if witness.expires_at <= now.astimezone(UTC):
        raise GlobalExecutionFenceError("global execution witness is stale")
    if witness.execution_state != "shadow":
        raise GlobalExecutionFenceError("global execution witness state forbids legacy scale-up")
    if witness.execution_epoch != 0:
        raise GlobalExecutionFenceError("global execution witness epoch is not shadow")
    if witness.executable_new_capacity_ceiling != 0:
        raise GlobalExecutionFenceError("global execution witness ceiling is not zero")


__all__ = [
    "GlobalExecutionFenceError",
    "GlobalExecutionWitness",
    "assert_legacy_scale_up_allowed",
    "load_global_execution_witness",
]
