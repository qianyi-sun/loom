from __future__ import annotations

import hashlib
import os
from pathlib import Path

from loom_cli.rollout.operator.backup import BackupCreator
from loom_cli.rollout.operator.backup_retirement import BackupPayloadRetirer
from loom_cli.rollout.operator.backup_rotation import BackupRetirementRecord
from loom_cli.rollout.operator.store import RequestStore
from tests.loom_cli.rollout.operator.test_backup import make_config


def _payload(
    tmp_path: Path, *, with_manifest: bool
) -> tuple[BackupPayloadRetirer, Path, str | None]:
    config = make_config(tmp_path)
    bundle = config.rollout_root / "backups" / "20260719T210000Z-req-retire000"
    (bundle / "postgres").mkdir(parents=True, mode=0o700)
    bundle.parent.chmod(0o700)
    bundle.chmod(0o700)
    payload = bundle / "postgres" / "dump.bin"
    payload.write_bytes(b"payload")
    payload.chmod(0o600)
    digest = None
    if with_manifest:
        manifest = bundle / "backup-manifest.json"
        manifest.write_bytes(b"{}\n")
        manifest.chmod(0o600)
        digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    store = RequestStore(tmp_path / "request-store")
    return (
        BackupPayloadRetirer(
            creator=BackupCreator(config, service_uid=os.geteuid()),
            store=store,
        ),
        bundle,
        digest,
    )


def test_manifest_payload_retirement_persists_compact_evidence_and_receipt(
    tmp_path: Path,
) -> None:
    retirer, bundle, digest = _payload(tmp_path, with_manifest=True)
    assert digest is not None
    record = BackupRetirementRecord(
        payload_id="payload-retire000",
        request_id="req-retire000",
        bundle_name=bundle.name,
        reason="superseded",
        manifest_sha256=digest,
    )

    retirer(record)
    retirer(record)

    assert not bundle.exists()
    root = retirer.store.backup_retirements_root
    assert (root / "payload-retire000.json").is_file()
    assert (root / "payload-retire000.deleted.json").is_file()


def test_incomplete_payload_retirement_is_exact_and_idempotent(tmp_path: Path) -> None:
    retirer, bundle, digest = _payload(tmp_path, with_manifest=False)
    assert digest is None
    record = BackupRetirementRecord(
        payload_id="payload-retire001",
        request_id="req-retire000",
        bundle_name=bundle.name,
        reason="failed",
    )

    retirer(record)
    retirer(record)

    assert not bundle.exists()
    assert (retirer.store.backup_retirements_root / "payload-retire001.json").is_file()
