"""Digest-approved convergence of exact backup-rotation retirements."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from .backup_retirement import BackupPayloadRetirer
from .backup_rotation import (
    BackupRetirementRecord,
    BackupRotationState,
    acknowledge_retirement,
)
from .config import OperatorConfig
from .store import RequestStore

_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600


class InstalledBackupRetentionError(RuntimeError):
    """Raised when exact rotation retirement cannot proceed safely."""


@dataclass(frozen=True, slots=True)
class BackupRotationRetentionPlan:
    """Immutable authority for the exact currently queued retirements."""

    rotation_generation: int
    rotation_sha256: str
    active_payload_id: str | None
    active_bundle_name: str | None
    latest_bundle: str | None
    retirements: tuple[BackupRetirementRecord, ...]
    environment: str = "staging"
    namespace: str = "loom-staging"

    def __post_init__(self) -> None:
        payload_ids = tuple(record.payload_id for record in self.retirements)
        if (
            self.rotation_generation < 0
            or len(self.rotation_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.rotation_sha256)
            or self.environment != "staging"
            or self.namespace != "loom-staging"
            or not self.retirements
            or len(set(payload_ids)) != len(payload_ids)
            or self.active_payload_id in set(payload_ids)
            or (self.active_payload_id is None) != (self.active_bundle_name is None)
            or any(
                value is not None and (not value or "/" in value or value in {".", ".."})
                for value in (self.active_bundle_name, self.latest_bundle)
            )
            or self.latest_bundle
            not in {
                None,
                self.active_bundle_name,
                *(record.bundle_name for record in self.retirements),
            }
        ):
            raise ValueError("backup rotation retention plan is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "active_payload_id": self.active_payload_id,
            "active_bundle_name": self.active_bundle_name,
            "environment": self.environment,
            "namespace": self.namespace,
            "latest_bundle": self.latest_bundle,
            "retirements": [record.to_dict() for record in self.retirements],
            "rotation_generation": self.rotation_generation,
            "rotation_sha256": self.rotation_sha256,
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> BackupRotationRetentionPlan:
        expected = {
            "active_payload_id",
            "active_bundle_name",
            "environment",
            "namespace",
            "latest_bundle",
            "retirements",
            "rotation_generation",
            "rotation_sha256",
            "schema_version",
        }
        if (
            set(data) != expected
            or data["schema_version"] != 1
            or type(data["rotation_generation"]) is not int
            or not isinstance(data["rotation_sha256"], str)
            or not isinstance(data["environment"], str)
            or not isinstance(data["namespace"], str)
            or (
                data["active_payload_id"] is not None
                and not isinstance(data["active_payload_id"], str)
            )
            or (
                data["active_bundle_name"] is not None
                and not isinstance(data["active_bundle_name"], str)
            )
            or (data["latest_bundle"] is not None and not isinstance(data["latest_bundle"], str))
            or not isinstance(data["retirements"], list)
            or not all(isinstance(item, dict) for item in data["retirements"])
        ):
            raise ValueError("backup rotation retention plan schema is invalid")
        return cls(
            rotation_generation=data["rotation_generation"],
            rotation_sha256=data["rotation_sha256"],
            active_payload_id=data["active_payload_id"],
            active_bundle_name=data["active_bundle_name"],
            latest_bundle=data["latest_bundle"],
            retirements=tuple(
                BackupRetirementRecord.from_dict(item) for item in data["retirements"]
            ),
            environment=data["environment"],
            namespace=data["namespace"],
        )

    @property
    def plan_digest(self) -> str:
        return hashlib.sha256(_json_bytes(self.to_dict())).hexdigest()


@dataclass(slots=True)
class InstalledBackupRetentionService:
    """Claim, retire, and acknowledge only exact persisted rotation records."""

    config: OperatorConfig
    service_uid: int
    store: RequestStore
    retirer: BackupPayloadRetirer

    def __post_init__(self) -> None:
        if (
            self.service_uid < 1
            or self.config.source_mode != "sealed-cumulative"
            or self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
        ):
            raise ValueError("installed backup retention authority is invalid")

    @property
    def evidence_root(self) -> Path:
        return self.config.state_root / "backup-retention-maintenance"

    def inventory(self) -> BackupRotationRetentionPlan:
        if self.store.read_active() is not None:
            raise InstalledBackupRetentionError("active rollout blocks backup retirement")
        state = self.store.read_backup_rotation()
        if state.candidate is not None:
            raise InstalledBackupRetentionError("backup candidate blocks backup retirement")
        if not state.retirements:
            raise InstalledBackupRetentionError("backup rotation has no queued retirements")
        referenced = self.store.referenced_backup_payload_ids()
        if referenced & {record.payload_id for record in state.retirements}:
            raise InstalledBackupRetentionError("referenced backup payload blocks retirement")
        plan = BackupRotationRetentionPlan(
            rotation_generation=state.generation,
            rotation_sha256=state.evidence_digest,
            active_payload_id=(None if state.active is None else state.active.payload_id),
            active_bundle_name=(None if state.active is None else state.active.bundle_name),
            latest_bundle=self._read_latest_bundle(state),
            retirements=tuple(
                self.store.resolve_backup_retirement(record) for record in state.retirements
            ),
            namespace=self.config.namespace,
        )
        _publish_exact(self._plan_path(plan.plan_digest), plan.to_dict(), self.service_uid)
        return plan

    def load_claim(self, approved_plan_digest: str) -> BackupRotationRetentionPlan:
        if len(approved_plan_digest) != 64 or any(
            character not in "0123456789abcdef" for character in approved_plan_digest
        ):
            raise InstalledBackupRetentionError("backup retention approval is invalid")
        try:
            plan = BackupRotationRetentionPlan.from_dict(
                _read_exact(self._plan_path(approved_plan_digest), self.service_uid)
            )
        except (FileNotFoundError, ValueError) as exc:
            raise InstalledBackupRetentionError("backup retention approval is unavailable") from exc
        if plan.plan_digest != approved_plan_digest:
            raise InstalledBackupRetentionError("backup retention claim digest drifted")
        self._validate_current(plan)
        return plan

    def apply(self, plan: BackupRotationRetentionPlan) -> dict[str, object]:
        applied_path = self.evidence_root / f"{plan.plan_digest}.applied.json"
        try:
            existing = _read_exact(applied_path, self.service_uid)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            self._validate_current(plan)
            return existing
        retired: list[str] = []
        retained: list[str] = []
        for planned in plan.retirements:
            if planned.bundle_name == plan.latest_bundle:
                retained.append(planned.payload_id)
                continue
            state = self.store.read_backup_rotation()
            self._validate_current(plan, state=state)
            present = tuple(
                record for record in state.retirements if record.payload_id == planned.payload_id
            )
            if not present:
                if not self.store.has_backup_retirement_receipt(planned.payload_id):
                    raise InstalledBackupRetentionError(
                        "backup retirement disappeared without an exact receipt"
                    )
                continue
            if len(present) != 1 or self.store.resolve_backup_retirement(present[0]) != planned:
                raise InstalledBackupRetentionError("backup retirement identity drifted")
            self.retirer(planned)
            current = self.store.read_backup_rotation()
            self._validate_current(plan, state=current)
            result = acknowledge_retirement(current, payload_id=planned.payload_id)
            self.store.replace_backup_rotation(
                result.state,
                expected_generation=current.generation,
            )
            retired.append(planned.payload_id)
        final = self.store.read_backup_rotation()
        self._validate_current(plan, state=final)
        protected_latest = {
            record.payload_id
            for record in plan.retirements
            if record.bundle_name == plan.latest_bundle
        }
        if any(
            record.payload_id
            in {planned.payload_id for planned in plan.retirements} - protected_latest
            for record in final.retirements
        ):
            raise InstalledBackupRetentionError("backup retirements remain after convergence")
        result_document: dict[str, object] = {
            "approved_plan_sha256": plan.plan_digest,
            "environment": "staging",
            "final_rotation_generation": final.generation,
            "final_rotation_sha256": final.evidence_digest,
            "namespace": self.config.namespace,
            "retired_payload_ids": retired,
            "retained_payload_ids": retained,
            "schema_version": 1,
        }
        _publish_exact(
            applied_path,
            result_document,
            self.service_uid,
        )
        return result_document

    def _validate_current(
        self,
        plan: BackupRotationRetentionPlan,
        *,
        state: BackupRotationState | None = None,
    ) -> None:
        if self.store.read_active() is not None:
            raise InstalledBackupRetentionError("active rollout blocks backup retirement")
        current = state or self.store.read_backup_rotation()
        active_id = None if current.active is None else current.active.payload_id
        active_bundle = None if current.active is None else current.active.bundle_name
        planned = {record.payload_id: record for record in plan.retirements}
        observed = {
            record.payload_id: self.store.resolve_backup_retirement(record)
            for record in current.retirements
        }
        if (
            current.candidate is not None
            or active_id != plan.active_payload_id
            or active_bundle != plan.active_bundle_name
            or self._read_latest_bundle(current) != plan.latest_bundle
            or not set(observed).issubset(planned)
            or any(planned[payload_id] != record for payload_id, record in observed.items())
            or self.store.referenced_backup_payload_ids() & set(planned)
        ):
            raise InstalledBackupRetentionError("backup rotation authority drifted")
        if (
            current.generation == plan.rotation_generation
            and current.evidence_digest != plan.rotation_sha256
        ):
            raise InstalledBackupRetentionError("backup rotation digest drifted")
        protected_latest = {
            record.payload_id
            for record in plan.retirements
            if record.bundle_name == plan.latest_bundle
        }
        missing = set(planned) - set(observed) - protected_latest
        if any(not self.store.has_backup_retirement_receipt(payload_id) for payload_id in missing):
            raise InstalledBackupRetentionError("backup retirement receipt is missing")

    def _read_latest_bundle(self, state: BackupRotationState) -> str | None:
        latest = self.config.rollout_root / "backups" / "latest"
        try:
            metadata = latest.lstat()
        except FileNotFoundError:
            return None
        if (
            not stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or metadata.st_nlink != 1
        ):
            raise InstalledBackupRetentionError("latest backup pointer is unsafe")
        target = os.readlink(latest)
        observed = latest.lstat()
        if (observed.st_dev, observed.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise InstalledBackupRetentionError("latest backup pointer changed during read")
        known = {
            *(record.bundle_name for record in state.retirements),
            None if state.active is None else state.active.bundle_name,
        }
        if not target or "/" in target or target not in known:
            raise InstalledBackupRetentionError("latest backup pointer is outside rotation")
        return target

    def _plan_path(self, digest: str) -> Path:
        return self.evidence_root / f"{digest}.plan.json"


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _ensure_private_directory(path: Path, service_uid: int) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=_PRIVATE_DIRECTORY_MODE)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise InstalledBackupRetentionError("backup retention evidence directory is unsafe")


def _publish_exact(path: Path, value: dict[str, object], service_uid: int) -> None:
    _ensure_private_directory(path.parent, service_uid)
    payload = _json_bytes(value)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
        )
    except FileExistsError as exc:
        if _read_exact(path, service_uid) != value:
            raise InstalledBackupRetentionError("backup retention evidence drifted") from exc
        return
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != service_uid
            or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
            or metadata.st_nlink != 1
        ):
            raise InstalledBackupRetentionError("backup retention evidence file is unsafe")
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _read_exact(path: Path, service_uid: int) -> dict[str, object]:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
        or metadata.st_nlink != 1
        or metadata.st_size > 1024 * 1024
    ):
        raise InstalledBackupRetentionError("backup retention evidence file is unsafe")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise InstalledBackupRetentionError("backup retention evidence changed during open")
        chunks: list[bytes] = []
        remaining = 1024 * 1024 + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
    finally:
        os.close(descriptor)
    if len(payload) > 1024 * 1024:
        raise InstalledBackupRetentionError("backup retention evidence is oversized")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InstalledBackupRetentionError("backup retention evidence is invalid") from exc
    if not isinstance(value, dict):
        raise InstalledBackupRetentionError("backup retention evidence is invalid")
    return value


__all__ = [
    "BackupRotationRetentionPlan",
    "InstalledBackupRetentionError",
    "InstalledBackupRetentionService",
]
