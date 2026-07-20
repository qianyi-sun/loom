"""Exact, evidence-first retirement of bounded staging backup payloads."""

from __future__ import annotations

from dataclasses import dataclass

from .backup import BackupCreator
from .backup_rotation import BackupRetirementRecord
from .store import RequestStore


@dataclass(slots=True)
class BackupPayloadRetirer:
    """Retire one persisted rotation record without broad path discovery."""

    creator: BackupCreator
    store: RequestStore

    def __call__(self, record: BackupRetirementRecord) -> None:
        record = self.store.resolve_backup_retirement(record)
        if record.bundle_name is None:
            raise RuntimeError("backup retirement bundle authority is unresolved")
        backups_root = self.creator.config.rollout_root / "backups"
        manifest_path = backups_root / record.bundle_name / "backup-manifest.json"
        if record.manifest_sha256 is None:
            self.store.publish_backup_retirement_evidence(record, manifest_path=None)
            self.creator.cleanup_incomplete(
                record.request_id,
                bundle_name=record.bundle_name,
            )
        else:
            self.store.publish_backup_retirement_evidence(
                record,
                manifest_path=manifest_path,
            )
            self.creator.retire_payload(
                record.request_id,
                bundle_name=record.bundle_name,
                expected_manifest_sha256=record.manifest_sha256,
            )
        self.store.publish_backup_retirement_receipt(record.payload_id)


__all__ = ["BackupPayloadRetirer"]
