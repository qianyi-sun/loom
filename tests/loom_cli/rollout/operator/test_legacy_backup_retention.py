from __future__ import annotations

import os
from pathlib import Path

import pytest

from loom_cli.rollout.operator.legacy_backup_retention import (
    LegacyBackupRetention,
    LegacyBackupRetentionError,
    LegacyBackupRetentionPlan,
)
from loom_cli.rollout.operator.model import ActivePointer
from loom_cli.rollout.operator.store import RequestStore
from tests.loom_cli.rollout.operator.test_backup import make_config


def _root(tmp_path: Path) -> tuple[LegacyBackupRetention, Path, Path, Path]:
    config = make_config(tmp_path)
    backups = config.rollout_root / "backups"
    backups.mkdir(mode=0o700)
    old = backups / "20260718T200000Z-req-oldbackup0"
    latest = backups / "20260719T200000Z-req-newbackup0"
    incomplete = backups / "20260719T210000Z-req-incomplete0"
    for bundle in (old, latest, incomplete):
        (bundle / "postgres").mkdir(parents=True, mode=0o700)
        bundle.chmod(0o700)
    shared = old / "postgres" / "dump.bin"
    shared.write_bytes(b"shared-payload")
    shared.chmod(0o600)
    os.link(shared, latest / "postgres" / "dump.bin")
    for bundle in (old, latest):
        manifest = bundle / "backup-manifest.json"
        manifest.write_bytes((bundle.name + "\n").encode())
        manifest.chmod(0o600)
    (incomplete / "postgres" / "partial.bin").write_bytes(b"partial")
    (incomplete / "postgres" / "partial.bin").chmod(0o600)
    (backups / "latest").symlink_to(latest.name)
    store = RequestStore(tmp_path / "request-store")
    return (
        LegacyBackupRetention(config=config, service_uid=os.geteuid(), store=store),
        old,
        latest,
        incomplete,
    )


def test_inventory_and_digest_approved_apply_preserve_latest_and_incomplete(
    tmp_path: Path,
) -> None:
    retention, old, latest, incomplete = _root(tmp_path)

    plan = retention.inventory()
    reconstructed = LegacyBackupRetentionPlan.from_dict(plan.to_dict())
    report = retention.apply(plan, approved_inventory_digest=plan.evidence_digest)
    retried = retention.apply(plan, approved_inventory_digest=plan.evidence_digest)

    assert reconstructed == plan
    assert len(plan.candidates) == 1
    assert len(plan.protected) == 1
    assert plan.incomplete_bundles == (incomplete.name,)
    assert not old.exists()
    assert latest.is_dir()
    assert incomplete.is_dir()
    assert report["retired_payload_ids"] == [plan.candidates[0].payload_id]
    assert retried["retired_payload_ids"] == []
    assert retention.store.has_backup_retirement_receipt(plan.candidates[0].payload_id)


def test_apply_rejects_digest_drift_and_active_rollout(tmp_path: Path) -> None:
    retention, _old, _latest, _incomplete = _root(tmp_path)
    plan = retention.inventory()

    with pytest.raises(LegacyBackupRetentionError, match="approval"):
        retention.apply(plan, approved_inventory_digest="0" * 64)

    retention.store.set_active(
        ActivePointer(
            request_id="req-active0000",
            attempt_number=1,
            unit_name="loom-staging-rollout-active.service",
            status="running",
        )
    )
    with pytest.raises(LegacyBackupRetentionError, match="active rollout"):
        retention.inventory()


def test_inventory_fails_closed_on_unsafe_unknown_entry(tmp_path: Path) -> None:
    retention, old, _latest, _incomplete = _root(tmp_path)
    unsafe = old.parent / "unknown-symlink"
    unsafe.symlink_to(old.name)

    with pytest.raises(LegacyBackupRetentionError, match="unsafe entry"):
        retention.inventory()
