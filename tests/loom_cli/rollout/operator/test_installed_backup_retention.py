from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator.backup import BackupCreator
from loom_cli.rollout.operator.backup_retirement import BackupPayloadRetirer
from loom_cli.rollout.operator.backup_rotation import (
    BackupRetirementRecord,
    BackupRotationState,
    begin_candidate,
)
from loom_cli.rollout.operator.installed_backup_retention import (
    InstalledBackupRetentionError,
    InstalledBackupRetentionService,
)
from loom_cli.rollout.operator.store import RequestStore
from tests.loom_cli.rollout.operator.test_backup import make_config


def _service(tmp_path: Path) -> tuple[InstalledBackupRetentionService, tuple[Path, Path]]:
    config = replace(
        make_config(tmp_path),
        source_mode="sealed-cumulative",
        source_commit_sha="a" * 40,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    store = RequestStore(config.state_root)
    backups = config.rollout_root / "backups"
    backups.mkdir(mode=0o700)
    records: list[BackupRetirementRecord] = []
    roots: list[Path] = []
    for index in (1, 2):
        request_id = f"req-failed000{index}"
        bundle_name = f"20260719T1{index}0000Z-{request_id}"
        root = backups / bundle_name
        root.mkdir(mode=0o700)
        partial = root / "partial.bin"
        partial.write_bytes(f"partial-{index}".encode())
        partial.chmod(0o600)
        roots.append(root)
        records.append(
            BackupRetirementRecord(
                payload_id=f"payload-failed0{index}",
                request_id=request_id,
                bundle_name=bundle_name,
                reason="failed",
                manifest_sha256=None,
            )
        )
    state = BackupRotationState(generation=1, retirements=tuple(records))
    store.replace_backup_rotation(state, expected_generation=0)
    creator = BackupCreator(config, service_uid=os.geteuid())
    return (
        InstalledBackupRetentionService(
            config=config,
            service_uid=os.geteuid(),
            store=store,
            retirer=BackupPayloadRetirer(creator=creator, store=store),
        ),
        (roots[0], roots[1]),
    )


def test_digest_approved_rotation_retirement_preserves_compact_evidence(
    tmp_path: Path,
) -> None:
    service, roots = _service(tmp_path)

    plan = service.inventory()
    loaded = service.load_claim(plan.plan_digest)
    result = service.apply(loaded)
    retried = service.apply(loaded)

    assert loaded == plan
    assert result["retired_payload_ids"] == ["payload-failed01", "payload-failed02"]
    assert retried == result
    assert all(not root.exists() for root in roots)
    assert service.store.read_backup_rotation().payload_count == 0
    for payload_id in ("payload-failed01", "payload-failed02"):
        assert service.store.has_backup_retirement_receipt(payload_id)
        evidence = service.store.backup_retirements_root / f"{payload_id}.json"
        receipt = service.store.backup_retirements_root / f"{payload_id}.deleted.json"
        assert evidence.is_file()
        assert receipt.is_file()


def test_rotation_retirement_rejects_unapproved_or_drifted_claim(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    plan = service.inventory()

    with pytest.raises(InstalledBackupRetentionError, match="approval"):
        service.load_claim("0" * 64)

    current = service.store.read_backup_rotation()
    drifted = BackupRotationState(
        generation=current.generation + 1,
        retirements=current.retirements[:-1],
    )
    service.store.replace_backup_rotation(drifted, expected_generation=current.generation)

    with pytest.raises(InstalledBackupRetentionError, match="receipt"):
        service.load_claim(plan.plan_digest)


def test_rotation_retirement_rejects_candidate_before_inventory(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    current = service.store.read_backup_rotation()
    reduced = BackupRotationState(
        generation=current.generation + 1,
        retirements=current.retirements[:1],
    )
    service.store.replace_backup_rotation(reduced, expected_generation=current.generation)
    reserved = begin_candidate(
        reduced,
        payload_id="payload-candidate1",
        request_id="req-candidate1",
        bundle_name="20260719T140000Z-req-candidate1",
        created_at=datetime(2026, 7, 19, 14, tzinfo=UTC),
    )
    service.store.replace_backup_rotation(
        reserved.state,
        expected_generation=reduced.generation,
    )

    with pytest.raises(InstalledBackupRetentionError, match="candidate"):
        service.inventory()
