"""Bind one verified critical checkpoint to isolated restore evidence.

This module is the only authority that may translate a schema-v2 rollout
checkpoint into a reusable :class:`BackupLease`.  A completed manifest alone
is deliberately insufficient: the exact PostgreSQL dump and immutable object
inventory must also be proven restorable by an isolated rehearsal.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from loom_cli.cluster_backup_guard import (
    BackupTraversalLimits,
    backup_manifest_sha256,
    validate_backup_manifest,
)

from .backup import VerifiedBackup
from .backup_lease import BackupLease
from .rollout_checkpoint import ImmutableObjectInventory

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUEST_ID_RE = re.compile(r"^req-[a-z0-9][a-z0-9-]{7,63}$")
_EVIDENCE_ID_RE = re.compile(r"^restore-[a-z0-9][a-z0-9-]{7,63}$")
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_INVENTORY_BYTES = 16 * 1024 * 1024
_COMPONENTS = ("k8s_secrets", "object_inventory", "postgres")


class CheckpointLeaseError(RuntimeError):
    """Raised when checkpoint or restore authority is incomplete or drifted."""


def _read_private_regular_file(
    path: Path,
    *,
    expected_owner_uid: int,
    max_bytes: int,
    label: str,
) -> bytes:
    if not path.is_absolute() or ".." in path.parts or max_bytes <= 0:
        raise CheckpointLeaseError(f"{label} path or bound is invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise CheckpointLeaseError(
            f"{label} is unavailable through the no-follow boundary"
        ) from exc
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
            raise CheckpointLeaseError(f"{label} private-file authority is invalid")
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
        if len(payload) > max_bytes or any(
            getattr(before, field) != getattr(after, field) for field in stable_fields
        ):
            raise CheckpointLeaseError(f"{label} changed while it was read")
        return bytes(payload)
    finally:
        os.close(fd)


def _json_object(payload: bytes, *, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        loaded = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise CheckpointLeaseError(f"{label} is not strict JSON") from exc
    if not isinstance(loaded, dict):
        raise CheckpointLeaseError(f"{label} must be a JSON object")
    return loaded


def _component_hashes(manifest: Mapping[str, object]) -> dict[str, str]:
    components = manifest.get("components")
    if not isinstance(components, Mapping) or set(components) != set(_COMPONENTS):
        raise CheckpointLeaseError("checkpoint manifest component set is invalid")
    hashes: dict[str, str] = {}
    for name in _COMPONENTS:
        component = components.get(name)
        if not isinstance(component, Mapping):
            raise CheckpointLeaseError(f"checkpoint component {name} is invalid")
        digest = component.get("sha256")
        if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
            raise CheckpointLeaseError(f"checkpoint component {name} digest is invalid")
        hashes[name] = digest
    return hashes


@dataclass(frozen=True, slots=True)
class CriticalCheckpointEvidence:
    """Strictly parsed identity of one immutable schema-v2 checkpoint."""

    request_id: str
    manifest_path: Path
    manifest_sha256: str
    component_sha256: Mapping[str, str]
    environment: str
    namespace: str
    mutation_epoch: int
    db_snapshot_identity: str
    schema_revision: str
    object_inventory_root: str
    created_at: datetime

    def __post_init__(self) -> None:
        if (
            _REQUEST_ID_RE.fullmatch(self.request_id) is None
            or not self.manifest_path.is_absolute()
            or ".." in self.manifest_path.parts
            or _SHA256_RE.fullmatch(self.manifest_sha256) is None
            or self.environment != "staging"
            or not self.namespace
            or self.mutation_epoch < 0
            or not self.db_snapshot_identity.startswith("pgdump-sha256:")
            or _SHA256_RE.fullmatch(self.db_snapshot_identity.removeprefix("pgdump-sha256:"))
            is None
            or not self.schema_revision
            or _SHA256_RE.fullmatch(self.object_inventory_root) is None
            or self.created_at.tzinfo is None
            or self.created_at.utcoffset() is None
        ):
            raise ValueError("critical checkpoint evidence is invalid")
        if set(self.component_sha256) != set(_COMPONENTS) or any(
            _SHA256_RE.fullmatch(value) is None for value in self.component_sha256.values()
        ):
            raise ValueError("critical checkpoint component authority is invalid")

    @property
    def evidence_digest(self) -> str:
        payload = {
            "component_sha256": dict(self.component_sha256),
            "created_at": self.created_at.isoformat(),
            "db_snapshot_identity": self.db_snapshot_identity,
            "environment": self.environment,
            "manifest_path": str(self.manifest_path),
            "manifest_sha256": self.manifest_sha256,
            "mutation_epoch": self.mutation_epoch,
            "namespace": self.namespace,
            "object_inventory_root": self.object_inventory_root,
            "request_id": self.request_id,
            "schema_revision": self.schema_revision,
            "schema_version": 1,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class RestoreVerificationEvidence:
    """Isolated restore/rehearsal proof bound to the checkpoint identity."""

    verification_id: str
    request_id: str
    checkpoint_evidence_sha256: str
    manifest_sha256: str
    db_snapshot_identity: str
    object_inventory_root: str
    mutation_epoch: int
    schema_revision: str
    environment: str
    namespace: str
    report_sha256: str
    verified_at: datetime

    def __post_init__(self) -> None:
        if (
            _EVIDENCE_ID_RE.fullmatch(self.verification_id) is None
            or _REQUEST_ID_RE.fullmatch(self.request_id) is None
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.checkpoint_evidence_sha256,
                    self.manifest_sha256,
                    self.object_inventory_root,
                    self.report_sha256,
                )
            )
            or not self.db_snapshot_identity.startswith("pgdump-sha256:")
            or self.mutation_epoch < 0
            or not self.schema_revision
            or self.environment != "staging"
            or not self.namespace
            or self.verified_at.tzinfo is None
            or self.verified_at.utcoffset() is None
        ):
            raise ValueError("restore verification evidence is invalid")


def inspect_critical_checkpoint(
    backup: VerifiedBackup,
    *,
    request_id: str,
    environment: str,
    namespace: str,
    expected_owner_uid: int,
    now: datetime,
    limits: BackupTraversalLimits | None = None,
) -> CriticalCheckpointEvidence:
    """Validate and parse one critical checkpoint without trusting path text."""
    if _REQUEST_ID_RE.fullmatch(request_id) is None:
        raise CheckpointLeaseError("checkpoint request identity is invalid")
    problems = validate_backup_manifest(
        backup.manifest_path,
        environment=environment,
        namespace=namespace,
        now=now,
        expected_owner_uid=expected_owner_uid,
        require_private_files=True,
        enforce_freshness=True,
        limits=limits,
    )
    if problems:
        raise CheckpointLeaseError("checkpoint manifest failed strict validation")
    digest = backup_manifest_sha256(
        backup.manifest_path,
        expected_owner_uid=expected_owner_uid,
        require_private_file=True,
        limits=limits,
    )
    if digest != backup.manifest_sha256:
        raise CheckpointLeaseError("checkpoint manifest digest drifted")
    manifest = _json_object(
        _read_private_regular_file(
            backup.manifest_path,
            expected_owner_uid=expected_owner_uid,
            max_bytes=_MAX_MANIFEST_BYTES,
            label="checkpoint manifest",
        ),
        label="checkpoint manifest",
    )
    if manifest.get("schema_version") != 2:
        raise CheckpointLeaseError("rollout checkpoint must use schema version 2")
    component_sha256 = _component_hashes(manifest)
    root = backup.manifest_path.parent
    components = manifest["components"]
    assert isinstance(components, Mapping)
    expected_paths = {
        "k8s_secrets": root / "secrets",
        "object_inventory": root / "object-inventory.json",
        "postgres": root / "postgres" / "loom.dump",
    }
    for name, expected_path in expected_paths.items():
        component = components[name]
        assert isinstance(component, Mapping)
        if component.get("path") != str(expected_path):
            raise CheckpointLeaseError(f"checkpoint component {name} path is not canonical")
    inventory_payload = _read_private_regular_file(
        expected_paths["object_inventory"],
        expected_owner_uid=expected_owner_uid,
        max_bytes=_MAX_INVENTORY_BYTES,
        label="checkpoint object inventory",
    )
    if hashlib.sha256(inventory_payload).hexdigest() != component_sha256["object_inventory"]:
        raise CheckpointLeaseError("checkpoint object inventory digest drifted")
    inventory_record = _json_object(inventory_payload, label="checkpoint object inventory")
    recorded_root = inventory_record.pop("inventory_root", None)
    if not isinstance(recorded_root, str):
        raise CheckpointLeaseError("checkpoint object inventory root is missing")
    try:
        inventory = ImmutableObjectInventory.from_dict(inventory_record)
    except ValueError as exc:
        raise CheckpointLeaseError("checkpoint object inventory schema is invalid") from exc
    if (
        inventory.inventory_root != recorded_root
        or inventory.environment != environment
        or inventory.namespace != namespace
    ):
        raise CheckpointLeaseError("checkpoint object inventory identity drifted")
    created_at_raw = manifest.get("created_at")
    if not isinstance(created_at_raw, str):
        raise CheckpointLeaseError("checkpoint creation time is invalid")
    try:
        created_at = datetime.fromisoformat(created_at_raw)
    except ValueError as exc:
        raise CheckpointLeaseError("checkpoint creation time is invalid") from exc
    if created_at != inventory.created_at:
        raise CheckpointLeaseError("checkpoint and object inventory clocks drifted")
    # Re-run the bounded guard after parsing so a path mutation between the two
    # reads cannot be promoted into lease authority.
    if validate_backup_manifest(
        backup.manifest_path,
        environment=environment,
        namespace=namespace,
        now=now,
        expected_owner_uid=expected_owner_uid,
        require_private_files=True,
        enforce_freshness=True,
        limits=limits,
    ):
        raise CheckpointLeaseError("checkpoint changed during identity extraction")
    return CriticalCheckpointEvidence(
        request_id=request_id,
        manifest_path=backup.manifest_path,
        manifest_sha256=digest,
        component_sha256=component_sha256,
        environment=environment,
        namespace=namespace,
        mutation_epoch=inventory.mutation_epoch,
        db_snapshot_identity=f"pgdump-sha256:{component_sha256['postgres']}",
        schema_revision=inventory.schema_revision,
        object_inventory_root=inventory.inventory_root,
        created_at=created_at,
    )


def build_restore_verified_lease(
    checkpoint: CriticalCheckpointEvidence,
    restore: RestoreVerificationEvidence,
    *,
    expires_at: datetime,
) -> BackupLease:
    """Create a lease only when every restore proof field matches exactly."""
    if restore.verified_at < checkpoint.created_at or restore.verified_at >= expires_at:
        raise CheckpointLeaseError("restore verification freshness is invalid")
    checks = {
        restore.request_id == checkpoint.request_id,
        restore.checkpoint_evidence_sha256 == checkpoint.evidence_digest,
        restore.manifest_sha256 == checkpoint.manifest_sha256,
        restore.db_snapshot_identity == checkpoint.db_snapshot_identity,
        restore.object_inventory_root == checkpoint.object_inventory_root,
        restore.mutation_epoch == checkpoint.mutation_epoch,
        restore.schema_revision == checkpoint.schema_revision,
        restore.environment == checkpoint.environment,
        restore.namespace == checkpoint.namespace,
    }
    if checks != {True}:
        raise CheckpointLeaseError("restore verification does not match checkpoint authority")
    return BackupLease(
        lease_id=f"lease-{checkpoint.manifest_sha256[:24]}",
        source_request_id=checkpoint.request_id,
        manifest_sha256=checkpoint.manifest_sha256,
        component_sha256=checkpoint.component_sha256,
        environment=checkpoint.environment,
        namespace=checkpoint.namespace,
        mutation_epoch=checkpoint.mutation_epoch,
        db_snapshot_identity=checkpoint.db_snapshot_identity,
        schema_revision=checkpoint.schema_revision,
        object_inventory_root=checkpoint.object_inventory_root,
        created_at=checkpoint.created_at,
        expires_at=expires_at,
        restore_verified_at=restore.verified_at,
    )


__all__ = [
    "CheckpointLeaseError",
    "CriticalCheckpointEvidence",
    "RestoreVerificationEvidence",
    "build_restore_verified_lease",
    "inspect_critical_checkpoint",
]
