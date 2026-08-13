"""Pinned-key, fail-closed coexistence fence for legacy capacity writers."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

_MAX_WITNESS_BYTES = 64 * 1024
_MAX_PUBLIC_KEY_BYTES = 32
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_STATES = frozenset({"shadow", "prepared", "active", "drain-only"})
_SIGNED_FIELDS = frozenset(
    {
        "authority",
        "pool_id",
        "execution_epoch",
        "execution_state",
        "executable_new_capacity_ceiling",
        "expires_at",
        "signing_key_id",
    }
)
_ENVELOPE_FIELDS = _SIGNED_FIELDS | {"canonical_digest", "signature_base64"}


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


def canonical_global_execution_witness_bytes(value: Mapping[str, object]) -> bytes:
    """Encode the manager-signed witness payload, never its signature."""

    payload = {key: value[key] for key in sorted(_SIGNED_FIELDS) if key in value}
    if set(payload) != _SIGNED_FIELDS:
        raise GlobalExecutionFenceError("global execution witness fields are invalid")
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _read_owner_only_bounded(path: Path, *, label: str, maximum_bytes: int) -> bytes:
    """Read a stable current-UID 0600 file through its parent descriptor."""

    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise GlobalExecutionFenceError(f"{label} is unavailable")
    try:
        before = path.lstat()
        parent_fd = os.open(
            path.parent,
            os.O_RDONLY
            | os.O_DIRECTORY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise GlobalExecutionFenceError(f"{label} is unavailable") from exc
    try:
        parent = os.fstat(parent_fd)
        if not stat.S_ISDIR(parent.st_mode):
            raise GlobalExecutionFenceError(f"{label} is unavailable")
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size <= 0
            or before.st_size > maximum_bytes
        ):
            raise GlobalExecutionFenceError(f"{label} metadata is unsafe")
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise GlobalExecutionFenceError(f"{label} is unavailable") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_size <= 0
                or opened.st_size > maximum_bytes
            ):
                raise GlobalExecutionFenceError(f"{label} metadata changed while opening")
            payload = bytearray()
            while len(payload) <= maximum_bytes:
                chunk = os.read(descriptor, min(4096, maximum_bytes + 1 - len(payload)))
                if not chunk:
                    break
                payload.extend(chunk)
            after = os.fstat(descriptor)
            if len(payload) != opened.st_size or (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ) != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns):
                raise GlobalExecutionFenceError(f"{label} changed during validation")
            try:
                current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise GlobalExecutionFenceError(f"{label} changed during validation") from exc
            if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            ):
                raise GlobalExecutionFenceError(f"{label} changed during validation")
            return bytes(payload)
        finally:
            os.close(descriptor)
    finally:
        os.close(parent_fd)


def _load_pinned_public_key(
    path: Path,
    *,
    expected_sha256: str,
) -> Ed25519PublicKey:
    if not isinstance(expected_sha256, str) or _DIGEST.fullmatch(expected_sha256) is None:
        raise GlobalExecutionFenceError("manager public key fingerprint is invalid")
    raw = _read_owner_only_bounded(
        path,
        label="manager public key",
        maximum_bytes=_MAX_PUBLIC_KEY_BYTES,
    )
    if len(raw) != _MAX_PUBLIC_KEY_BYTES or not hmac.compare_digest(
        hashlib.sha256(raw).hexdigest(), expected_sha256
    ):
        raise GlobalExecutionFenceError("manager public key does not match the pinned fingerprint")
    try:
        return Ed25519PublicKey.from_public_bytes(raw)
    except ValueError as exc:  # pragma: no cover - backend validation
        raise GlobalExecutionFenceError("manager public key is invalid") from exc


@dataclass(frozen=True, slots=True)
class GlobalExecutionWitness:
    """One manager-signed, physical-pool-bound execution state witness."""

    authority: str
    pool_id: str
    execution_epoch: int
    execution_state: str
    executable_new_capacity_ceiling: int
    expires_at: datetime
    signing_key_id: str
    canonical_digest: str

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, object],
        *,
        public_key: Ed25519PublicKey,
        expected_public_key_sha256: str,
    ) -> GlobalExecutionWitness:
        if set(value) != _ENVELOPE_FIELDS:
            raise GlobalExecutionFenceError("global execution witness fields are invalid")
        if not isinstance(public_key, Ed25519PublicKey):
            raise GlobalExecutionFenceError("manager public key is invalid")
        raw_public_key = public_key.public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        if not isinstance(expected_public_key_sha256, str) or not hmac.compare_digest(
            hashlib.sha256(raw_public_key).hexdigest(), expected_public_key_sha256
        ):
            raise GlobalExecutionFenceError(
                "manager public key does not match the pinned fingerprint"
            )
        digest = value["canonical_digest"]
        if not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None:
            raise GlobalExecutionFenceError("global execution witness digest is invalid")
        canonical = canonical_global_execution_witness_bytes(value)
        if not hmac.compare_digest(digest, hashlib.sha256(canonical).hexdigest()):
            raise GlobalExecutionFenceError("global execution witness digest does not match")
        signature = value["signature_base64"]
        if not isinstance(signature, str):
            raise GlobalExecutionFenceError("global execution witness signature is invalid")
        try:
            signature_bytes = base64.b64decode(signature, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise GlobalExecutionFenceError(
                "global execution witness signature is invalid"
            ) from exc
        if (
            len(signature_bytes) != 64
            or base64.b64encode(signature_bytes).decode("ascii") != signature
        ):
            raise GlobalExecutionFenceError("global execution witness signature is invalid")
        try:
            public_key.verify(signature_bytes, canonical)
        except InvalidSignature as exc:
            raise GlobalExecutionFenceError(
                "global execution witness signature does not match"
            ) from exc
        state = value["execution_state"]
        if not isinstance(state, str) or state not in _STATES:
            raise GlobalExecutionFenceError("global execution witness state is invalid")
        return cls(
            authority=_exact_identifier(value["authority"], "authority"),
            pool_id=_exact_identifier(value["pool_id"], "pool"),
            execution_epoch=_quantity(value["execution_epoch"], "epoch"),
            execution_state=state,
            executable_new_capacity_ceiling=_quantity(
                value["executable_new_capacity_ceiling"], "ceiling"
            ),
            expires_at=_timestamp(value["expires_at"]),
            signing_key_id=_exact_identifier(value["signing_key_id"], "signing key"),
            canonical_digest=digest,
        )


def load_global_execution_witness(
    path: Path | None,
    *,
    manager_public_key_path: Path | None,
    expected_manager_public_key_sha256: str | None,
) -> GlobalExecutionWitness | None:
    """Load a bounded envelope only after independently pinning its signer."""

    if (
        path is None
        and manager_public_key_path is None
        and expected_manager_public_key_sha256 is None
    ):
        return None
    if (
        path is None
        or manager_public_key_path is None
        or expected_manager_public_key_sha256 is None
    ):
        raise GlobalExecutionFenceError("global execution witness is unavailable")
    raw = _read_owner_only_bounded(
        path, label="global execution witness", maximum_bytes=_MAX_WITNESS_BYTES
    )
    public_key = _load_pinned_public_key(
        manager_public_key_path,
        expected_sha256=expected_manager_public_key_sha256,
    )
    try:
        decoded = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GlobalExecutionFenceError("global execution witness is invalid") from exc
    if not isinstance(decoded, dict):
        raise GlobalExecutionFenceError("global execution witness is invalid")
    return GlobalExecutionWitness.from_mapping(
        cast(Mapping[str, object], decoded),
        public_key=public_key,
        expected_public_key_sha256=expected_manager_public_key_sha256,
    )


def assert_legacy_scale_up_allowed(
    witness: GlobalExecutionWitness | None,
    *,
    expected_authority: str,
    expected_pool_id: str,
    now: datetime | None = None,
) -> None:
    """Raise unless the exact manager scope is fresh, pinned-key shadow state."""

    if witness is None:
        raise GlobalExecutionFenceError("global execution witness is unavailable")
    now = now or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise GlobalExecutionFenceError("global execution fence clock is invalid")
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
    "canonical_global_execution_witness_bytes",
    "load_global_execution_witness",
]
