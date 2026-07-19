from __future__ import annotations

from datetime import UTC, datetime, timedelta

from loom_cli.rollout.operator.backup_lease import BackupLease
from loom_cli.rollout.operator.backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRotationState,
)
from loom_cli.rollout.operator.installed_backup_authority import (
    build_installed_backup_authority,
)
from loom_cli.rollout.operator.rollout_checkpoint import build_immutable_inventory


class _Store:
    def __init__(self, state: BackupRotationState) -> None:
        self.state = state

    def read_backup_rotation(self) -> BackupRotationState:
        return self.state


def _inventory(now: datetime, *, epoch: int = 7):
    return build_immutable_inventory(
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=epoch,
        schema_revision="0066_staging_data_lifecycle",
        created_at=now,
        objects=(),
    )


def _lease(now: datetime) -> BackupLease:
    return BackupLease(
        lease_id="lease-current01",
        source_request_id="req-current01",
        manifest_sha256="1" * 64,
        component_sha256={"postgres": "2" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="pgdump-sha256:" + "3" * 64,
        schema_revision="0066_staging_data_lifecycle",
        object_inventory_root="4" * 64,
        created_at=now - timedelta(minutes=10),
        expires_at=now + timedelta(hours=1),
        restore_verified_at=now - timedelta(minutes=5),
    )


def test_missing_active_lease_requires_fresh_checkpoint() -> None:
    now = datetime(2026, 7, 19, 12, tzinfo=UTC)
    authority = build_installed_backup_authority(
        _Store(BackupRotationState()),
        lambda created_at: _inventory(created_at),
        mutation_epoch=7,
        now=now,
    )

    assert authority.lease_source() is None
    assert authority.source_request_id == "fresh-checkpoint"
    assert authority.schema_revision == "0066_staging_data_lifecycle"
    assert authority.object_inventory_root == _inventory(now).inventory_root


def test_active_lease_is_bound_and_disappearance_fails_closed() -> None:
    now = datetime(2026, 7, 19, 12, tzinfo=UTC)
    lease = _lease(now)
    record = BackupPayloadRecord(
        payload_id="payload-current01",
        request_id=lease.source_request_id,
        phase=BackupPayloadPhase.ACTIVE,
        created_at=lease.created_at,
        manifest_sha256=lease.manifest_sha256,
        lease=lease,
    )
    store = _Store(BackupRotationState(generation=3, active=record))

    authority = build_installed_backup_authority(
        store,
        lambda created_at: _inventory(created_at),
        mutation_epoch=7,
        now=now,
    )

    assert authority.lease_source() == lease
    assert authority.expected_lease_digest == lease.evidence_digest
    store.state = BackupRotationState(generation=4)
    assert authority.lease_source() is None


def test_inventory_epoch_drift_is_rejected_before_request() -> None:
    now = datetime(2026, 7, 19, 12, tzinfo=UTC)
    try:
        build_installed_backup_authority(
            _Store(BackupRotationState()),
            lambda created_at: _inventory(created_at, epoch=8),
            mutation_epoch=7,
            now=now,
        )
    except ValueError as exc:
        assert str(exc) == "installed backup inventory drifts from mutation authority"
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("epoch drift was accepted")
