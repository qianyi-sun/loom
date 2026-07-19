"""Derive preflight backup authority from exact live inventory and rotation state."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from loom_cli.rollout.preflight_runtime_sources import BackupAdmissionAuthority

from .backup_lease import BackupLease
from .backup_rotation import BackupRotationState
from .rollout_checkpoint import ImmutableObjectInventory


class BackupAuthorityStore(Protocol):
    def read_backup_rotation(self) -> BackupRotationState: ...


InventorySource = Callable[[datetime], ImmutableObjectInventory]


def build_installed_backup_authority(
    store: BackupAuthorityStore,
    inventory_source: InventorySource,
    *,
    mutation_epoch: int,
    now: datetime,
) -> BackupAdmissionAuthority:
    """Bind either the active restore-verified lease or a fresh checkpoint sentinel.

    The current immutable-object inventory is always read first.  This prevents
    an old active lease from suppressing an inventory/schema/epoch drift: the
    shared lease check receives the active lease identity as its expectation,
    while the attestation bindings still carry the current epoch and the check
    reports every mismatch before a request is published.
    """
    if mutation_epoch < 0 or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("installed backup authority input is invalid")
    inventory = inventory_source(now)
    if (
        inventory.environment != "staging"
        or inventory.namespace != "loom-staging"
        or inventory.mutation_epoch != mutation_epoch
    ):
        raise ValueError("installed backup inventory drifts from mutation authority")
    state = store.read_backup_rotation()
    active = state.active
    if active is None or active.lease is None:
        return BackupAdmissionAuthority.fresh(
            schema_revision=inventory.schema_revision,
            object_inventory_root=inventory.inventory_root,
        )
    lease = active.lease

    def current_lease() -> BackupLease | None:
        observed = store.read_backup_rotation().active
        if (
            observed is None
            or observed.payload_id != active.payload_id
            or observed.request_id != active.request_id
            or observed.lease is None
            or observed.lease.evidence_digest != lease.evidence_digest
        ):
            return None
        return observed.lease

    return BackupAdmissionAuthority(
        lease_source=current_lease,
        expected_lease_digest=lease.evidence_digest,
        source_request_id=lease.source_request_id,
        db_snapshot_identity=lease.db_snapshot_identity,
        schema_revision=lease.schema_revision,
        object_inventory_root=lease.object_inventory_root,
        manifest_sha256=lease.manifest_sha256,
        component_sha256=lease.component_sha256,
    )


__all__ = ["BackupAuthorityStore", "InventorySource", "build_installed_backup_authority"]
