"""Digest-approved convergence of pre-rotation staging backup payloads."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from loom_cli.cluster_backup_guard import backup_manifest_sha256

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
class LegacyBackupRetentionPlan:
    """Immutable inventory authorizing only exact complete superseded roots."""

    backups_device: int
    backups_inode: int
    latest_bundle: str
    candidates: tuple[BackupRetirementRecord, ...]
    protected: tuple[BackupRetirementRecord, ...]
    incomplete_bundles: tuple[str, ...]
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
        names = [record.bundle_name for record in (*self.candidates, *self.protected)]
        payload_ids = [record.payload_id for record in (*self.candidates, *self.protected)]
        if (
            any(name is None for name in names)
            or len(set(names)) != len(names)
            or len(set(payload_ids)) != len(payload_ids)
        ):
            raise ValueError("legacy backup retention plan has duplicate bundle authority")
        if self.latest_bundle not in names:
            raise ValueError("legacy backup retention plan does not preserve latest")
        if tuple(sorted(self.incomplete_bundles)) != self.incomplete_bundles:
            raise ValueError("incomplete legacy backup roots must be sorted")

    def to_dict(self) -> dict[str, object]:
        return {
            "backups_device": self.backups_device,
            "backups_inode": self.backups_inode,
            "candidates": [record.to_dict() for record in self.candidates],
            "environment": self.environment,
            "incomplete_bundles": list(self.incomplete_bundles),
            "latest_bundle": self.latest_bundle,
            "namespace": self.namespace,
            "protected": [record.to_dict() for record in self.protected],
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LegacyBackupRetentionPlan:
        expected = {
            "backups_device",
            "backups_inode",
            "candidates",
            "environment",
            "incomplete_bundles",
            "latest_bundle",
            "namespace",
            "protected",
            "schema_version",
        }
        if (
            set(data) != expected
            or data["schema_version"] != 1
            or type(data["backups_device"]) is not int
            or type(data["backups_inode"]) is not int
            or not isinstance(data["latest_bundle"], str)
            or not isinstance(data["environment"], str)
            or not isinstance(data["namespace"], str)
            or not isinstance(data["candidates"], list)
            or not isinstance(data["protected"], list)
            or not isinstance(data["incomplete_bundles"], list)
            or not all(isinstance(item, dict) for item in data["candidates"])
            or not all(isinstance(item, dict) for item in data["protected"])
            or not all(isinstance(item, str) for item in data["incomplete_bundles"])
        ):
            raise ValueError("legacy backup retention plan schema is invalid")
        return cls(
            backups_device=data["backups_device"],
            backups_inode=data["backups_inode"],
            latest_bundle=data["latest_bundle"],
            candidates=tuple(BackupRetirementRecord.from_dict(item) for item in data["candidates"]),
            protected=tuple(BackupRetirementRecord.from_dict(item) for item in data["protected"]),
            incomplete_bundles=tuple(data["incomplete_bundles"]),
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

    def _record(self, bundle_name: str, manifest_path: Path) -> BackupRetirementRecord:
        matched = _BUNDLE_RE.fullmatch(bundle_name)
        if matched is None:
            raise LegacyBackupRetentionError("legacy backup bundle name is invalid")
        digest = backup_manifest_sha256(
            manifest_path,
            expected_owner_uid=self.service_uid,
            require_private_file=True,
        )
        payload_id = (
            "payload-legacy-" + hashlib.sha256(f"{bundle_name}\0{digest}".encode()).hexdigest()[:16]
        )
        return BackupRetirementRecord(
            payload_id=payload_id,
            request_id=matched.group("request_id"),
            bundle_name=bundle_name,
            reason="superseded",
            manifest_sha256=digest,
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
        latest_path = backups / "latest"
        latest_metadata = latest_path.lstat()
        if not stat.S_ISLNK(latest_metadata.st_mode):
            raise LegacyBackupRetentionError("latest backup pointer is not a symlink")
        latest_bundle = os.readlink(latest_path)
        if _BUNDLE_RE.fullmatch(latest_bundle) is None or "/" in latest_bundle:
            raise LegacyBackupRetentionError("latest backup pointer is unsafe")
        protected_names = {latest_bundle, *additionally_protected}
        candidates: list[BackupRetirementRecord] = []
        protected: list[BackupRetirementRecord] = []
        incomplete: list[str] = []
        observed_names: set[str] = set()
        with os.scandir(backups) as entries:
            for entry in entries:
                if entry.name == "latest":
                    continue
                observed_names.add(entry.name)
                entry_metadata = entry.stat(follow_symlinks=False)
                if (
                    not stat.S_ISDIR(entry_metadata.st_mode)
                    or entry_metadata.st_uid != self.service_uid
                    or stat.S_IMODE(entry_metadata.st_mode) != 0o700
                ):
                    raise LegacyBackupRetentionError("legacy backup root contains an unsafe entry")
                if _BUNDLE_RE.fullmatch(entry.name) is None:
                    incomplete.append(entry.name)
                    continue
                manifest = backups / entry.name / "backup-manifest.json"
                try:
                    manifest.lstat()
                except FileNotFoundError:
                    incomplete.append(entry.name)
                    continue
                record = self._record(entry.name, manifest)
                (protected if entry.name in protected_names else candidates).append(record)
        if not additionally_protected.issubset(observed_names):
            raise LegacyBackupRetentionError("explicitly protected backup root is missing")
        if latest_bundle not in {record.bundle_name for record in protected}:
            raise LegacyBackupRetentionError("latest backup manifest is unavailable")
        return LegacyBackupRetentionPlan(
            backups_device=metadata.st_dev,
            backups_inode=metadata.st_ino,
            latest_bundle=latest_bundle,
            candidates=tuple(sorted(candidates, key=lambda record: record.bundle_name or "")),
            protected=tuple(sorted(protected, key=lambda record: record.bundle_name or "")),
            incomplete_bundles=tuple(sorted(incomplete)),
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
                record.bundle_name for record in plan.protected if record.bundle_name is not None
            )
        )
        if (
            current.backups_device != plan.backups_device
            or current.backups_inode != plan.backups_inode
            or current.latest_bundle != plan.latest_bundle
            or current.protected != plan.protected
            or current.incomplete_bundles != plan.incomplete_bundles
        ):
            raise LegacyBackupRetentionError("legacy backup protected inventory drifted")
        planned = {record.payload_id: record for record in plan.candidates}
        present = {record.payload_id: record for record in current.candidates}
        if not set(present).issubset(planned) or any(
            planned[payload_id] != record for payload_id, record in present.items()
        ):
            raise LegacyBackupRetentionError("legacy backup candidate inventory drifted")
        retirer = BackupPayloadRetirer(
            creator=BackupCreator(self.config, service_uid=self.service_uid),
            store=self.store,
        )
        retired: list[str] = []
        for record in plan.candidates:
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
    "LegacyBackupRetention",
    "LegacyBackupRetentionError",
    "LegacyBackupRetentionPlan",
]
