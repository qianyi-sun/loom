"""Shared traversal limits for broker-created staging backups."""

from __future__ import annotations

from loom_cli.cluster_backup_guard import BackupTraversalLimits

from .config import OperatorConfig

BACKUP_MAX_TOTAL_BYTES = 16 * 1024**4
BACKUP_NON_MINIO_FILE_ALLOWANCE = 4
BACKUP_NON_MINIO_ENTRY_ALLOWANCE = 6


def operator_backup_traversal_limits(
    config: OperatorConfig,
    *,
    max_total_bytes: int = BACKUP_MAX_TOTAL_BYTES,
) -> BackupTraversalLimits:
    """Return the one reviewed traversal policy used by writer and worker."""
    return BackupTraversalLimits(
        max_files=config.backup_max_objects + BACKUP_NON_MINIO_FILE_ALLOWANCE,
        max_entries=config.backup_max_entries,
        max_total_bytes=max_total_bytes,
    )


__all__ = [
    "BACKUP_MAX_TOTAL_BYTES",
    "BACKUP_NON_MINIO_ENTRY_ALLOWANCE",
    "BACKUP_NON_MINIO_FILE_ALLOWANCE",
    "operator_backup_traversal_limits",
]
