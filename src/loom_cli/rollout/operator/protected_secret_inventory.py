"""Strict, private protected-capacity Secret checkpoint inventory."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_RESOURCE_VERSION_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_DATA_KEY_RE = re.compile(r"^[A-Za-z0-9._-]{1,253}$")
_MAX_SECRET_BYTES = 1024 * 1024
_MAX_INVENTORY_BYTES = 64 * 1024
_INVENTORY_FILENAME = "protected-capacity-secret-inventory.json"
_STAGING_SECRET_FILES = frozenset(
    {"loom-admin-secret.yaml", "loom-secrets.yaml", "loom-staging-tls.yaml"}
)
_ROOT_FIELDS = frozenset({"apiVersion", "data", "immutable", "kind", "metadata", "type"})
_METADATA_FIELDS = frozenset(
    {
        "annotations",
        "creationTimestamp",
        "labels",
        "managedFields",
        "name",
        "namespace",
        "resourceVersion",
        "uid",
    }
)


class SecretInventoryError(ValueError):
    """Secret-safe rejection of an incomplete or drifting inventory."""


@dataclass(frozen=True, slots=True)
class ProtectedSecretSpec:
    namespace: str
    name: str
    required: bool

    @property
    def filename(self) -> str:
        return f"protected-{self.namespace}-{self.name}.json"


PROTECTED_SECRET_SPECS = (
    ProtectedSecretSpec("loom-dev", "loom-capacity-manager", True),
    ProtectedSecretSpec("loom-staging", "loom-capacity-agent", False),
    ProtectedSecretSpec("loom-staging", "loom-protected-worker-runtime", False),
    ProtectedSecretSpec("loom-dev", "loom-capacity-execution-operator", False),
    ProtectedSecretSpec("loom-dev", "loom-capacity-executor-gb10", False),
    ProtectedSecretSpec("loom-dev", "loom-capacity-executor-oldlab", False),
)
_SPECS_BY_IDENTITY = {(item.namespace, item.name): item for item in PROTECTED_SECRET_SPECS}


@dataclass(frozen=True, slots=True)
class SecretInventory:
    inventory_payload: bytes
    exported_objects: Mapping[str, bytes]

    @property
    def inventory_sha256(self) -> str:
        return hashlib.sha256(self.inventory_payload).hexdigest()


@dataclass(frozen=True, slots=True)
class _SecretObservation:
    uid: str
    resource_version: str
    export_payload: bytes


def _strict_json(payload: bytes, *, max_bytes: int) -> dict[str, object]:
    if not payload or len(payload) > max_bytes:
        raise SecretInventoryError("protected Secret payload size is invalid")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise SecretInventoryError("protected Secret payload is not strict JSON") from exc
    if not isinstance(value, dict):
        raise SecretInventoryError("protected Secret payload is not a JSON object")
    return value


def _safe_string_map(value: object) -> bool:
    return isinstance(value, dict) and all(
        isinstance(key, str)
        and _DATA_KEY_RE.fullmatch(key) is not None
        and isinstance(item, str)
        and bool(item)
        for key, item in value.items()
    )


def _parse_secret(payload: bytes, *, namespace: str, name: str) -> _SecretObservation:
    value = _strict_json(payload, max_bytes=_MAX_SECRET_BYTES)
    metadata = value.get("metadata")
    data = value.get("data")
    if (
        not set(value) <= _ROOT_FIELDS
        or value.get("apiVersion") != "v1"
        or value.get("kind") != "Secret"
        or not isinstance(metadata, dict)
        or not set(metadata) <= _METADATA_FIELDS
        or metadata.get("namespace") != namespace
        or metadata.get("name") != name
        or not isinstance(metadata.get("uid"), str)
        or _UID_RE.fullmatch(metadata["uid"]) is None
        or not isinstance(metadata.get("resourceVersion"), str)
        or _RESOURCE_VERSION_RE.fullmatch(metadata["resourceVersion"]) is None
        or not _safe_string_map(data)
        or not data
        or ("immutable" in value and type(value["immutable"]) is not bool)
        or ("type" in value and (not isinstance(value["type"], str) or not value["type"]))
    ):
        raise SecretInventoryError("protected Secret identity or metadata is invalid")
    assert isinstance(data, dict)
    for encoded in data.values():
        assert isinstance(encoded, str)
        try:
            base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise SecretInventoryError("protected Secret data encoding is invalid") from exc
    exported: dict[str, object] = {
        "apiVersion": "v1",
        "data": dict(sorted(data.items())),
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": value.get("type", "Opaque"),
    }
    if "immutable" in value:
        exported["immutable"] = value["immutable"]
    export_payload = (json.dumps(exported, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return _SecretObservation(
        uid=metadata["uid"],
        resource_version=metadata["resourceVersion"],
        export_payload=export_payload,
    )


def canonical_secret_export(payload: bytes, *, namespace: str, name: str) -> bytes:
    """Validate one Kubernetes Secret response and strip unsafe metadata."""
    return _parse_secret(payload, namespace=namespace, name=name).export_payload


def _validate_persisted_export(payload: bytes, *, namespace: str, name: str) -> None:
    value = _strict_json(payload, max_bytes=_MAX_SECRET_BYTES)
    metadata = value.get("metadata")
    data = value.get("data")
    allowed = {"apiVersion", "data", "immutable", "kind", "metadata", "type"}
    if (
        not set(value) <= allowed
        or set(value) < {"apiVersion", "data", "kind", "metadata", "type"}
        or value.get("apiVersion") != "v1"
        or value.get("kind") != "Secret"
        or metadata != {"name": name, "namespace": namespace}
        or not _safe_string_map(data)
        or not data
        or not isinstance(value.get("type"), str)
        or not value["type"]
        or ("immutable" in value and type(value["immutable"]) is not bool)
    ):
        raise SecretInventoryError("persisted Secret identity or payload is invalid")
    assert isinstance(data, dict)
    try:
        for encoded in data.values():
            assert isinstance(encoded, str)
            base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise SecretInventoryError("persisted Secret data encoding is invalid") from exc
    canonical = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if canonical != payload:
        raise SecretInventoryError("persisted Secret payload is noncanonical")


def build_secret_inventory(
    observations: Mapping[tuple[str, str], tuple[bytes | None, bytes | None]],
) -> SecretInventory:
    """Bind two observations of the exact protected Secret allowlist."""
    if set(observations) != set(_SPECS_BY_IDENTITY):
        raise SecretInventoryError("protected Secret observation set is invalid")
    records: list[dict[str, object]] = []
    files: dict[str, bytes] = {}
    for spec in PROTECTED_SECRET_SPECS:
        first_raw, second_raw = observations[(spec.namespace, spec.name)]
        if (first_raw is None) != (second_raw is None):
            raise SecretInventoryError("protected Secret changed during acquisition")
        if first_raw is None:
            if spec.required:
                raise SecretInventoryError("required protected Secret is absent")
            records.append(
                {
                    "filename": None,
                    "name": spec.name,
                    "namespace": spec.namespace,
                    "present": False,
                    "sha256": None,
                }
            )
            continue
        assert second_raw is not None
        first = _parse_secret(first_raw, namespace=spec.namespace, name=spec.name)
        second = _parse_secret(second_raw, namespace=spec.namespace, name=spec.name)
        if first != second:
            raise SecretInventoryError("protected Secret changed during acquisition")
        files[spec.filename] = first.export_payload
        records.append(
            {
                "filename": spec.filename,
                "name": spec.name,
                "namespace": spec.namespace,
                "present": True,
                "sha256": hashlib.sha256(first.export_payload).hexdigest(),
            }
        )
    inventory_payload = (
        json.dumps(
            {"schema_version": 1, "secrets": records},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    return SecretInventory(
        inventory_payload=inventory_payload,
        exported_objects=MappingProxyType(files),
    )


def _read_private_file(path: Path, *, expected_owner_uid: int, max_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise SecretInventoryError("protected Secret inventory file is unavailable") from exc
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != expected_owner_uid
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > max_bytes
        ):
            raise SecretInventoryError("protected Secret inventory file metadata is invalid")
        payload = bytearray()
        while len(payload) <= max_bytes:
            chunk = os.read(fd, min(65536, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(fd)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_gid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if (
            len(payload) > max_bytes
            or len(payload) != before.st_size
            or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
        ):
            raise SecretInventoryError("protected Secret inventory file changed while read")
        return bytes(payload)
    finally:
        os.close(fd)


def inspect_secret_inventory(root: Path, *, expected_owner_uid: int) -> SecretInventory:
    """Revalidate exact private files and inventory digests without decoding data."""
    try:
        metadata = os.stat(root, follow_symlinks=False)
        filenames = {entry.name for entry in os.scandir(root)}
    except OSError as exc:
        raise SecretInventoryError("protected Secret inventory directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != expected_owner_uid
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_nlink != 2
    ):
        raise SecretInventoryError("protected Secret inventory directory metadata is invalid")
    inventory_payload = _read_private_file(
        root / _INVENTORY_FILENAME,
        expected_owner_uid=expected_owner_uid,
        max_bytes=_MAX_INVENTORY_BYTES,
    )
    value = _strict_json(inventory_payload, max_bytes=_MAX_INVENTORY_BYTES)
    records = value.get("secrets")
    if (
        set(value) != {"schema_version", "secrets"}
        or value["schema_version"] != 1
        or not isinstance(records, list)
    ):
        raise SecretInventoryError("protected Secret inventory schema is invalid")
    seen: set[tuple[str, str]] = set()
    expected_files = set(_STAGING_SECRET_FILES) | {_INVENTORY_FILENAME}
    exported: dict[str, bytes] = {}
    for filename in _STAGING_SECRET_FILES:
        name = filename.removesuffix(".yaml")
        payload = _read_private_file(
            root / filename,
            expected_owner_uid=expected_owner_uid,
            max_bytes=_MAX_SECRET_BYTES,
        )
        _validate_persisted_export(payload, namespace="loom-staging", name=name)
    for record in records:
        if not isinstance(record, dict) or set(record) != {
            "filename",
            "name",
            "namespace",
            "present",
            "sha256",
        }:
            raise SecretInventoryError("protected Secret inventory record is invalid")
        identity = (record.get("namespace"), record.get("name"))
        if not all(isinstance(item, str) for item in identity) or identity in seen:
            raise SecretInventoryError("protected Secret inventory has duplicate identity")
        seen.add(identity)  # type: ignore[arg-type]
        spec = _SPECS_BY_IDENTITY.get(identity)  # type: ignore[arg-type]
        if spec is None or type(record.get("present")) is not bool:
            raise SecretInventoryError("protected Secret inventory identity is invalid")
        if not record["present"]:
            if spec.required or record["filename"] is not None or record["sha256"] is not None:
                raise SecretInventoryError("protected Secret absence record is invalid")
            continue
        if (
            record["filename"] != spec.filename
            or not isinstance(record["sha256"], str)
            or _SHA256_RE.fullmatch(record["sha256"]) is None
        ):
            raise SecretInventoryError("protected Secret presence record is invalid")
        payload = _read_private_file(
            root / spec.filename,
            expected_owner_uid=expected_owner_uid,
            max_bytes=_MAX_SECRET_BYTES,
        )
        # The persisted export is intentionally metadata-minimal, so parse its
        # exact canonical JSON shape directly rather than requiring live UID/RV.
        _validate_persisted_export(payload, namespace=spec.namespace, name=spec.name)
        if hashlib.sha256(payload).hexdigest() != record["sha256"]:
            raise SecretInventoryError("protected Secret exported-object digest does not match")
        exported[spec.filename] = payload
        expected_files.add(spec.filename)
    if len(records) != len(PROTECTED_SECRET_SPECS) or seen != set(_SPECS_BY_IDENTITY):
        raise SecretInventoryError("protected Secret inventory identity set is invalid")
    if filenames != expected_files:
        raise SecretInventoryError("protected Secret inventory file set is invalid")
    return SecretInventory(inventory_payload, MappingProxyType(exported))


__all__ = [
    "PROTECTED_SECRET_SPECS",
    "SecretInventory",
    "SecretInventoryError",
    "build_secret_inventory",
    "canonical_secret_export",
    "inspect_secret_inventory",
]
