from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from loom_cli.cluster_backup_guard import write_backup_manifest
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
        minio = bundle / "minio"
        minio.mkdir(mode=0o700)
        (minio / "artifact.bin").write_bytes(b"artifact-payload")
        (minio / "artifact.bin").chmod(0o600)
        secrets = bundle / "k8s-secrets.yaml"
        secrets.write_bytes(b"sealed-secret-payload")
        secrets.chmod(0o600)
        write_backup_manifest(
            environment="staging",
            namespace=config.namespace,
            output_path=bundle / "backup-manifest.json",
            components={
                "postgres": bundle / "postgres",
                "minio": minio,
                "k8s_secrets": secrets,
            },
        )
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
    assert plan.candidates[0].payload_file_count == 3
    assert plan.candidates[0].payload_size_bytes == 51
    assert plan.candidates[0].component_names == ("k8s_secrets", "minio", "postgres")
    assert tuple(record.name for record in plan.opaque_evidence) == (incomplete.name,)
    assert not old.exists()
    assert latest.is_dir()
    assert incomplete.is_dir()
    payload_id = plan.candidates[0].retirement.payload_id
    assert report["retired_payload_ids"] == [payload_id]
    assert retried["retired_payload_ids"] == []
    assert retention.store.has_backup_retirement_receipt(payload_id)


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

    with pytest.raises(LegacyBackupRetentionError, match="evidence entry is unsafe"):
        retention.inventory()


def test_inventory_preserves_noncanonical_evidence_files_and_directories(
    tmp_path: Path,
) -> None:
    retention, _old, _latest, incomplete = _root(tmp_path)
    backups = incomplete.parent
    log = backups / "backup-refresh-20260710T184534Z.log"
    log.write_bytes(b"historical evidence\n")
    log.chmod(0o670)
    evidence = backups / "route-cutover-20260706T210056Z"
    evidence.mkdir(mode=0o770)

    plan = retention.inventory()
    records = {record.name: record for record in plan.opaque_evidence}

    assert set(records) == {incomplete.name, log.name, evidence.name}
    assert records[log.name].kind == "file"
    assert records[log.name].content_observation == "sha256"
    assert records[log.name].sha256 is not None
    assert records[evidence.name].kind == "directory"
    assert records[evidence.name].content_observation == "metadata-only"
    assert records[evidence.name].sha256 is None


def test_inventory_preserves_unreadable_evidence_by_stable_metadata(tmp_path: Path) -> None:
    retention, _old, _latest, incomplete = _root(tmp_path)
    evidence = incomplete.parent / ".chown-test"
    evidence.write_bytes(b"unreadable historical evidence")
    evidence.chmod(0o000)

    try:
        record = next(
            item for item in retention.inventory().opaque_evidence if item.name == evidence.name
        )
    finally:
        evidence.chmod(0o600)

    assert record.kind == "file"
    assert record.content_observation == "metadata-only"
    assert record.sha256 is None


def test_apply_rejects_opaque_evidence_drift(tmp_path: Path) -> None:
    retention, _old, _latest, _incomplete = _root(tmp_path)
    log = retention.config.rollout_root / "backups" / "backup-refresh.log"
    log.write_bytes(b"before\n")
    log.chmod(0o600)
    plan = retention.inventory()
    log.write_bytes(b"after\n")

    with pytest.raises(LegacyBackupRetentionError, match="protected inventory drifted"):
        retention.apply(plan, approved_inventory_digest=plan.evidence_digest)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("scope", "scope"),
        ("schema", "schema"),
        ("verification", "verification"),
        ("component-path", "component path"),
        ("component-size", "component metadata"),
    ],
)
def test_inventory_rejects_manifest_authority_drift(
    tmp_path: Path,
    mutation: str,
    expected: str,
) -> None:
    retention, old, _latest, _incomplete = _root(tmp_path)
    manifest_path = old / "backup-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if mutation == "scope":
        manifest["environment"] = "production"
    elif mutation == "schema":
        manifest["schema_version"] = 99
    elif mutation == "verification":
        manifest["verification"]["status"] = "pending"
    elif mutation == "component-path":
        manifest["components"]["postgres"]["path"] = str(tmp_path / "outside")
    else:
        manifest["components"]["postgres"]["size_bytes"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_path.chmod(0o600)

    with pytest.raises(LegacyBackupRetentionError, match=expected):
        retention.inventory()


def test_inventory_rejects_linked_manifest(tmp_path: Path) -> None:
    retention, old, _latest, _incomplete = _root(tmp_path)
    manifest = old / "backup-manifest.json"
    linked = tmp_path / "linked-manifest.json"
    os.link(manifest, linked)

    with pytest.raises(LegacyBackupRetentionError, match="metadata is unsafe"):
        retention.inventory()
