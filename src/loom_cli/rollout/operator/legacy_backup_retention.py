"""Digest-approved convergence of pre-rotation staging backup payloads."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

from loom_cli.cluster_backup_guard import (
    REQUIRED_BACKUP_COMPONENTS,
    ROLLOUT_CHECKPOINT_COMPONENTS,
)

from .backup import BackupCreator
from .backup_retirement import BackupPayloadRetirer
from .backup_rotation import BackupRetirementRecord
from .config import OperatorConfig
from .store import RequestStore

_BUNDLE_RE = re.compile(
    r"^(?P<timestamp>[0-9]{8}T[0-9]{6}Z)-(?P<request_id>[a-z0-9][a-z0-9-]{7,79})$"
)


class LegacyBackupRetentionError(RuntimeError):
    """Normalized legacy payload inventory or convergence failure."""


@dataclass(frozen=True, slots=True)
class LegacyBackupInventoryRecord:
    """Manifest-bound size evidence for one exact legacy payload root."""

    retirement: BackupRetirementRecord
    schema_version: int
    manifest_size_bytes: int
    payload_file_count: int
    payload_size_bytes: int
    component_names: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            self.schema_version not in {1, 2}
            or self.manifest_size_bytes <= 0
            or self.payload_file_count <= 0
            or self.payload_size_bytes <= 0
            or not self.component_names
            or tuple(sorted(self.component_names)) != self.component_names
            or len(set(self.component_names)) != len(self.component_names)
            or self.retirement.manifest_sha256 is None
        ):
            raise ValueError("legacy backup inventory record is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "component_names": list(self.component_names),
            "manifest_size_bytes": self.manifest_size_bytes,
            "payload_file_count": self.payload_file_count,
            "payload_size_bytes": self.payload_size_bytes,
            "retirement": self.retirement.to_dict(),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LegacyBackupInventoryRecord:
        expected = {
            "component_names",
            "manifest_size_bytes",
            "payload_file_count",
            "payload_size_bytes",
            "retirement",
            "schema_version",
        }
        if (
            set(data) != expected
            or not isinstance(data["retirement"], dict)
            or type(data["schema_version"]) is not int
            or type(data["manifest_size_bytes"]) is not int
            or type(data["payload_file_count"]) is not int
            or type(data["payload_size_bytes"]) is not int
            or not isinstance(data["component_names"], list)
            or not all(isinstance(name, str) for name in data["component_names"])
        ):
            raise ValueError("legacy backup inventory record schema is invalid")
        return cls(
            retirement=BackupRetirementRecord.from_dict(data["retirement"]),
            schema_version=data["schema_version"],
            manifest_size_bytes=data["manifest_size_bytes"],
            payload_file_count=data["payload_file_count"],
            payload_size_bytes=data["payload_size_bytes"],
            component_names=tuple(data["component_names"]),
        )


@dataclass(frozen=True, slots=True)
class LegacyBackupOpaqueEvidenceRecord:
    """Exact top-level evidence that retention must preserve untouched."""

    name: str
    kind: str
    device: int
    inode: int
    owner_uid: int
    owner_gid: int
    mode: int
    link_count: int
    size_bytes: int
    modified_ns: int
    changed_ns: int
    content_observation: str
    sha256: str | None

    def __post_init__(self) -> None:
        if (
            not self.name
            or self.name == "latest"
            or "/" in self.name
            or self.name != self.name.strip()
            or self.kind not in {"directory", "file"}
            or self.device < 0
            or self.inode <= 0
            or self.owner_uid < 0
            or self.owner_gid < 0
            or not 0 <= self.mode <= 0o7777
            or self.link_count <= 0
            or self.size_bytes < 0
            or self.modified_ns < 0
            or self.changed_ns < 0
            or self.content_observation not in {"metadata-only", "sha256"}
            or (self.content_observation == "sha256") != (self.sha256 is not None)
            or (self.kind == "directory" and self.content_observation != "metadata-only")
            or (
                self.sha256 is not None
                and (
                    len(self.sha256) != 64
                    or any(character not in "0123456789abcdef" for character in self.sha256)
                )
            )
        ):
            raise ValueError("legacy backup opaque evidence record is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "changed_ns": self.changed_ns,
            "content_observation": self.content_observation,
            "device": self.device,
            "inode": self.inode,
            "kind": self.kind,
            "link_count": self.link_count,
            "mode": self.mode,
            "modified_ns": self.modified_ns,
            "name": self.name,
            "owner_gid": self.owner_gid,
            "owner_uid": self.owner_uid,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LegacyBackupOpaqueEvidenceRecord:
        expected = {
            "changed_ns",
            "content_observation",
            "device",
            "inode",
            "kind",
            "link_count",
            "mode",
            "modified_ns",
            "name",
            "owner_gid",
            "owner_uid",
            "sha256",
            "size_bytes",
        }
        integer_fields = (
            "changed_ns",
            "device",
            "inode",
            "link_count",
            "mode",
            "modified_ns",
            "owner_gid",
            "owner_uid",
            "size_bytes",
        )
        if (
            set(data) != expected
            or not isinstance(data["name"], str)
            or not isinstance(data["kind"], str)
            or not isinstance(data["content_observation"], str)
            or not all(type(data[field]) is int for field in integer_fields)
            or (data["sha256"] is not None and not isinstance(data["sha256"], str))
        ):
            raise ValueError("legacy backup opaque evidence record schema is invalid")
        return cls(
            name=data["name"],
            kind=data["kind"],
            device=cast(int, data["device"]),
            inode=cast(int, data["inode"]),
            owner_uid=cast(int, data["owner_uid"]),
            owner_gid=cast(int, data["owner_gid"]),
            mode=cast(int, data["mode"]),
            link_count=cast(int, data["link_count"]),
            size_bytes=cast(int, data["size_bytes"]),
            modified_ns=cast(int, data["modified_ns"]),
            changed_ns=cast(int, data["changed_ns"]),
            content_observation=data["content_observation"],
            sha256=data["sha256"],
        )


@dataclass(frozen=True, slots=True)
class LegacyBackupRetentionPlan:
    """Immutable inventory authorizing only exact complete superseded roots."""

    backups_device: int
    backups_inode: int
    latest_bundle: str
    candidates: tuple[LegacyBackupInventoryRecord, ...]
    protected: tuple[LegacyBackupInventoryRecord, ...]
    opaque_evidence: tuple[LegacyBackupOpaqueEvidenceRecord, ...]
    environment: str = "staging"
    namespace: str = "loom-staging"

    def __post_init__(self) -> None:
        if (
            self.backups_device < 0
            or self.backups_inode <= 0
            or _BUNDLE_RE.fullmatch(self.latest_bundle) is None
            or self.environment != "staging"
            or not self.namespace
        ):
            raise ValueError("legacy backup retention plan authority is invalid")
        retirements = [record.retirement for record in (*self.candidates, *self.protected)]
        names = [record.bundle_name for record in retirements]
        payload_ids = [record.payload_id for record in retirements]
        if (
            any(name is None for name in names)
            or len(set(names)) != len(names)
            or len(set(payload_ids)) != len(payload_ids)
        ):
            raise ValueError("legacy backup retention plan has duplicate bundle authority")
        if self.latest_bundle not in names:
            raise ValueError("legacy backup retention plan does not preserve latest")
        opaque_names = tuple(record.name for record in self.opaque_evidence)
        if tuple(sorted(opaque_names)) != opaque_names or len(set(opaque_names)) != len(
            opaque_names
        ):
            raise ValueError("opaque legacy backup evidence must be unique and sorted")

    def to_dict(self) -> dict[str, object]:
        return {
            "backups_device": self.backups_device,
            "backups_inode": self.backups_inode,
            "candidates": [record.to_dict() for record in self.candidates],
            "environment": self.environment,
            "latest_bundle": self.latest_bundle,
            "namespace": self.namespace,
            "opaque_evidence": [record.to_dict() for record in self.opaque_evidence],
            "protected": [record.to_dict() for record in self.protected],
            "schema_version": 3,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LegacyBackupRetentionPlan:
        expected = {
            "backups_device",
            "backups_inode",
            "candidates",
            "environment",
            "latest_bundle",
            "namespace",
            "opaque_evidence",
            "protected",
            "schema_version",
        }
        if (
            set(data) != expected
            or data["schema_version"] != 3
            or type(data["backups_device"]) is not int
            or type(data["backups_inode"]) is not int
            or not isinstance(data["latest_bundle"], str)
            or not isinstance(data["environment"], str)
            or not isinstance(data["namespace"], str)
            or not isinstance(data["candidates"], list)
            or not isinstance(data["protected"], list)
            or not isinstance(data["opaque_evidence"], list)
            or not all(isinstance(item, dict) for item in data["candidates"])
            or not all(isinstance(item, dict) for item in data["protected"])
            or not all(isinstance(item, dict) for item in data["opaque_evidence"])
        ):
            raise ValueError("legacy backup retention plan schema is invalid")
        return cls(
            backups_device=data["backups_device"],
            backups_inode=data["backups_inode"],
            latest_bundle=data["latest_bundle"],
            candidates=tuple(
                LegacyBackupInventoryRecord.from_dict(item) for item in data["candidates"]
            ),
            protected=tuple(
                LegacyBackupInventoryRecord.from_dict(item) for item in data["protected"]
            ),
            opaque_evidence=tuple(
                LegacyBackupOpaqueEvidenceRecord.from_dict(item) for item in data["opaque_evidence"]
            ),
            environment=data["environment"],
            namespace=data["namespace"],
        )

    @property
    def evidence_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(slots=True)
class LegacyBackupRetention:
    """Inventory and converge legacy complete roots without broad deletion."""

    config: OperatorConfig
    service_uid: int
    store: RequestStore

    def _opaque_record(
        self,
        *,
        backups_fd: int,
        name: str,
        expected_metadata: os.stat_result,
    ) -> LegacyBackupOpaqueEvidenceRecord:
        if not name or name == "latest" or "/" in name:
            raise LegacyBackupRetentionError("legacy backup evidence name is invalid")
        if stat.S_ISDIR(expected_metadata.st_mode):
            kind = "directory"
        elif stat.S_ISREG(expected_metadata.st_mode) and expected_metadata.st_nlink == 1:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            kind = "file"
        else:
            raise LegacyBackupRetentionError("legacy backup evidence entry is unsafe")

        opened = os.stat(name, dir_fd=backups_fd, follow_symlinks=False)
        if _identity(opened) != _identity(expected_metadata):
            raise LegacyBackupRetentionError("legacy backup evidence changed during inventory")
        sha256 = None
        content_observation = "metadata-only"
        if kind == "file":
            try:
                descriptor = os.open(name, flags, dir_fd=backups_fd)
            except PermissionError:
                descriptor = None
            except OSError as exc:
                raise LegacyBackupRetentionError(
                    "legacy backup evidence could not be opened safely"
                ) from exc
            if descriptor is not None:
                try:
                    opened = os.fstat(descriptor)
                    if _identity(opened) != _identity(expected_metadata):
                        raise LegacyBackupRetentionError(
                            "legacy backup evidence changed during inventory"
                        )
                    payload = _read_bounded(descriptor, maximum_bytes=1024 * 1024)
                    if _identity(os.fstat(descriptor)) != _identity(opened):
                        raise LegacyBackupRetentionError(
                            "legacy backup evidence changed during inventory"
                        )
                    sha256 = hashlib.sha256(payload).hexdigest()
                    content_observation = "sha256"
                finally:
                    os.close(descriptor)
        if _identity(os.stat(name, dir_fd=backups_fd, follow_symlinks=False)) != _identity(
            expected_metadata
        ):
            raise LegacyBackupRetentionError("legacy backup evidence changed during inventory")
        return LegacyBackupOpaqueEvidenceRecord(
            name=name,
            kind=kind,
            device=opened.st_dev,
            inode=opened.st_ino,
            owner_uid=opened.st_uid,
            owner_gid=opened.st_gid,
            mode=stat.S_IMODE(opened.st_mode),
            link_count=opened.st_nlink,
            size_bytes=opened.st_size,
            modified_ns=opened.st_mtime_ns,
            changed_ns=opened.st_ctime_ns,
            content_observation=content_observation,
            sha256=sha256,
        )

    def _record(
        self,
        *,
        backups_fd: int,
        bundle_name: str,
        expected_bundle_metadata: os.stat_result,
    ) -> LegacyBackupInventoryRecord:
        matched = _BUNDLE_RE.fullmatch(bundle_name)
        if matched is None:
            raise LegacyBackupRetentionError("legacy backup bundle name is invalid")
        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            bundle_fd = os.open(bundle_name, directory_flags, dir_fd=backups_fd)
        except OSError as exc:
            raise LegacyBackupRetentionError(
                "legacy backup root could not be opened safely"
            ) from exc
        try:
            opened_bundle = os.fstat(bundle_fd)
            if _identity(opened_bundle) != _identity(expected_bundle_metadata):
                raise LegacyBackupRetentionError("legacy backup root changed during inventory")
            try:
                manifest_fd = os.open("backup-manifest.json", file_flags, dir_fd=bundle_fd)
            except OSError as exc:
                raise LegacyBackupRetentionError(
                    "legacy backup manifest could not be opened safely"
                ) from exc
            try:
                before = os.fstat(manifest_fd)
                if (
                    not stat.S_ISREG(before.st_mode)
                    or before.st_uid != self.service_uid
                    or stat.S_IMODE(before.st_mode) != 0o600
                    or before.st_nlink != 1
                    or before.st_size <= 0
                    or before.st_size > 1024 * 1024
                ):
                    raise LegacyBackupRetentionError("legacy backup manifest metadata is unsafe")
                payload = _read_bounded(manifest_fd, maximum_bytes=1024 * 1024)
                after = os.fstat(manifest_fd)
                if _identity(before) != _identity(after) or len(payload) != after.st_size:
                    raise LegacyBackupRetentionError(
                        "legacy backup manifest changed during inventory"
                    )
            finally:
                os.close(manifest_fd)
            if _identity(os.fstat(bundle_fd)) != _identity(opened_bundle):
                raise LegacyBackupRetentionError("legacy backup root changed during inventory")
        finally:
            os.close(bundle_fd)

        digest = hashlib.sha256(payload).hexdigest()
        try:
            manifest = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LegacyBackupRetentionError("legacy backup manifest is not valid JSON") from exc
        if not isinstance(manifest, dict):
            raise LegacyBackupRetentionError("legacy backup manifest must be an object")
        schema_version = manifest.get("schema_version")
        required_components = {
            1: REQUIRED_BACKUP_COMPONENTS,
            2: ROLLOUT_CHECKPOINT_COMPONENTS,
        }.get(schema_version if type(schema_version) is int else -1)
        if required_components is None:
            raise LegacyBackupRetentionError("legacy backup manifest schema is unsupported")
        if (
            manifest.get("environment") != "staging"
            or manifest.get("namespace") != self.config.namespace
        ):
            raise LegacyBackupRetentionError("legacy backup manifest scope does not match")
        created_at = manifest.get("created_at")
        if not isinstance(created_at, str):
            raise LegacyBackupRetentionError("legacy backup manifest timestamp is invalid")
        try:
            parsed_created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise LegacyBackupRetentionError("legacy backup manifest timestamp is invalid") from exc
        if parsed_created_at.tzinfo is None or parsed_created_at.utcoffset() is None:
            raise LegacyBackupRetentionError("legacy backup manifest timestamp is invalid")
        verification = manifest.get("verification")
        if (
            not isinstance(verification, dict)
            or verification.get("status") != "verified"
            or verification.get("required_components") != list(required_components)
        ):
            raise LegacyBackupRetentionError("legacy backup manifest verification is invalid")
        components = manifest.get("components")
        if not isinstance(components, dict) or set(components) != set(required_components):
            raise LegacyBackupRetentionError("legacy backup manifest components are invalid")
        bundle_root = self.config.rollout_root / "backups" / bundle_name
        payload_file_count = 0
        payload_size_bytes = 0
        for name in required_components:
            component = components.get(name)
            if not isinstance(component, dict):
                raise LegacyBackupRetentionError("legacy backup component is invalid")
            kind = component.get("kind")
            size_bytes = component.get("size_bytes")
            sha256 = component.get("sha256")
            component_path = component.get("path")
            if (
                kind not in {"file", "directory"}
                or type(size_bytes) is not int
                or size_bytes <= 0
                or not isinstance(sha256, str)
                or len(sha256) != 64
                or any(character not in "0123456789abcdef" for character in sha256)
                or not isinstance(component_path, str)
            ):
                raise LegacyBackupRetentionError("legacy backup component metadata is invalid")
            component_fs_path = Path(component_path)
            if (
                not component_fs_path.is_absolute()
                or ".." in component_fs_path.parts
                or component_fs_path == bundle_root
                or not component_fs_path.is_relative_to(bundle_root)
            ):
                raise LegacyBackupRetentionError("legacy backup component path is invalid")
            if kind == "directory":
                file_count = component.get("file_count")
                if type(file_count) is not int or file_count <= 0:
                    raise LegacyBackupRetentionError(
                        "legacy backup component file count is invalid"
                    )
            else:
                if "file_count" in component:
                    raise LegacyBackupRetentionError(
                        "legacy backup file component has unexpected file count"
                    )
                file_count = 1
            payload_file_count += file_count
            payload_size_bytes += size_bytes
        payload_id = (
            "payload-legacy-" + hashlib.sha256(f"{bundle_name}\0{digest}".encode()).hexdigest()[:16]
        )
        return LegacyBackupInventoryRecord(
            retirement=BackupRetirementRecord(
                payload_id=payload_id,
                request_id=matched.group("request_id"),
                bundle_name=bundle_name,
                reason="superseded",
                manifest_sha256=digest,
            ),
            schema_version=cast(int, schema_version),
            manifest_size_bytes=len(payload),
            payload_file_count=payload_file_count,
            payload_size_bytes=payload_size_bytes,
            component_names=tuple(sorted(components)),
        )

    def inventory(
        self, *, additionally_protected: frozenset[str] = frozenset()
    ) -> LegacyBackupRetentionPlan:
        if self.store.read_active() is not None:
            raise LegacyBackupRetentionError("active rollout blocks backup retention inventory")
        backups = self.config.rollout_root / "backups"
        metadata = backups.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if not stat.S_ISDIR(metadata.st_mode) or not (
            (metadata.st_uid == self.service_uid and mode == 0o700)
            or (metadata.st_uid != self.service_uid and mode == 0o770)
        ):
            raise LegacyBackupRetentionError("backup root metadata is unsafe")
        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            backups_fd = os.open(backups, directory_flags)
        except OSError as exc:
            raise LegacyBackupRetentionError("backup root could not be opened safely") from exc
        try:
            if _identity(os.fstat(backups_fd)) != _identity(metadata):
                raise LegacyBackupRetentionError("backup root changed during inventory")
            latest_metadata = os.stat("latest", dir_fd=backups_fd, follow_symlinks=False)
            if not stat.S_ISLNK(latest_metadata.st_mode):
                raise LegacyBackupRetentionError("latest backup pointer is not a symlink")
            latest_bundle = os.readlink("latest", dir_fd=backups_fd)
            if _BUNDLE_RE.fullmatch(latest_bundle) is None or "/" in latest_bundle:
                raise LegacyBackupRetentionError("latest backup pointer is unsafe")
            protected_names = {latest_bundle, *additionally_protected}
            candidates: list[LegacyBackupInventoryRecord] = []
            protected: list[LegacyBackupInventoryRecord] = []
            opaque: list[LegacyBackupOpaqueEvidenceRecord] = []
            observed_names: set[str] = set()
            with os.scandir(backups_fd) as entries:
                for entry in entries:
                    if entry.name == "latest":
                        continue
                    observed_names.add(entry.name)
                    entry_metadata = entry.stat(follow_symlinks=False)
                    if _BUNDLE_RE.fullmatch(entry.name) is None or not stat.S_ISDIR(
                        entry_metadata.st_mode
                    ):
                        opaque.append(
                            self._opaque_record(
                                backups_fd=backups_fd,
                                name=entry.name,
                                expected_metadata=entry_metadata,
                            )
                        )
                        continue
                    try:
                        probe_fd = os.open(entry.name, directory_flags, dir_fd=backups_fd)
                    except (FileNotFoundError, PermissionError):
                        opaque.append(
                            self._opaque_record(
                                backups_fd=backups_fd,
                                name=entry.name,
                                expected_metadata=entry_metadata,
                            )
                        )
                        continue
                    try:
                        manifest_metadata = os.stat(
                            "backup-manifest.json",
                            dir_fd=probe_fd,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        opaque.append(
                            self._opaque_record(
                                backups_fd=backups_fd,
                                name=entry.name,
                                expected_metadata=entry_metadata,
                            )
                        )
                        continue
                    finally:
                        os.close(probe_fd)
                    if not stat.S_ISREG(manifest_metadata.st_mode):
                        raise LegacyBackupRetentionError(
                            "legacy backup manifest metadata is unsafe"
                        )
                    if (
                        entry_metadata.st_uid != self.service_uid
                        or stat.S_IMODE(entry_metadata.st_mode) != 0o700
                    ):
                        raise LegacyBackupRetentionError(
                            "manifest-backed legacy backup root metadata is unsafe"
                        )
                    record = self._record(
                        backups_fd=backups_fd,
                        bundle_name=entry.name,
                        expected_bundle_metadata=entry_metadata,
                    )
                    (protected if entry.name in protected_names else candidates).append(record)
            if _identity(os.fstat(backups_fd)) != _identity(metadata):
                raise LegacyBackupRetentionError("backup root changed during inventory")
        finally:
            os.close(backups_fd)
        if not additionally_protected.issubset(observed_names):
            raise LegacyBackupRetentionError("explicitly protected backup root is missing")
        if latest_bundle not in {record.retirement.bundle_name for record in protected}:
            raise LegacyBackupRetentionError("latest backup manifest is unavailable")
        return LegacyBackupRetentionPlan(
            backups_device=metadata.st_dev,
            backups_inode=metadata.st_ino,
            latest_bundle=latest_bundle,
            candidates=tuple(
                sorted(candidates, key=lambda record: record.retirement.bundle_name or "")
            ),
            protected=tuple(
                sorted(protected, key=lambda record: record.retirement.bundle_name or "")
            ),
            opaque_evidence=tuple(sorted(opaque, key=lambda record: record.name)),
            namespace=self.config.namespace,
        )

    def apply(
        self,
        plan: LegacyBackupRetentionPlan,
        *,
        approved_inventory_digest: str,
    ) -> dict[str, object]:
        if approved_inventory_digest != plan.evidence_digest:
            raise LegacyBackupRetentionError("legacy backup inventory approval does not match")
        if self.store.read_active() is not None:
            raise LegacyBackupRetentionError("active rollout blocks backup retention apply")
        current = self.inventory(
            additionally_protected=frozenset(
                record.retirement.bundle_name
                for record in plan.protected
                if record.retirement.bundle_name is not None
            )
        )
        if (
            current.backups_device != plan.backups_device
            or current.backups_inode != plan.backups_inode
            or current.latest_bundle != plan.latest_bundle
            or current.protected != plan.protected
            or current.opaque_evidence != plan.opaque_evidence
        ):
            raise LegacyBackupRetentionError("legacy backup protected inventory drifted")
        planned = {record.retirement.payload_id: record for record in plan.candidates}
        present = {record.retirement.payload_id: record for record in current.candidates}
        if not set(present).issubset(planned) or any(
            planned[payload_id] != record for payload_id, record in present.items()
        ):
            raise LegacyBackupRetentionError("legacy backup candidate inventory drifted")
        retirer = BackupPayloadRetirer(
            creator=BackupCreator(self.config, service_uid=self.service_uid),
            store=self.store,
        )
        retired: list[str] = []
        for inventory_record in plan.candidates:
            record = inventory_record.retirement
            if record.payload_id in present:
                retirer(record)
                retired.append(record.payload_id)
            elif not self.store.has_backup_retirement_receipt(record.payload_id):
                raise LegacyBackupRetentionError(
                    "legacy backup payload disappeared without receipt"
                )
        return {
            "approved_inventory_digest": approved_inventory_digest,
            "environment": "staging",
            "latest_bundle": plan.latest_bundle,
            "namespace": self.config.namespace,
            "retired_payload_ids": retired,
            "schema_version": 1,
        }


__all__ = [
    "LegacyBackupInventoryRecord",
    "LegacyBackupOpaqueEvidenceRecord",
    "LegacyBackupRetention",
    "LegacyBackupRetentionError",
    "LegacyBackupRetentionPlan",
]


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_bounded(descriptor: int, *, maximum_bytes: int) -> bytes:
    payload = bytearray()
    while True:
        chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes - len(payload) + 1))
        if not chunk:
            return bytes(payload)
        payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise LegacyBackupRetentionError("legacy backup manifest exceeds size limit")
