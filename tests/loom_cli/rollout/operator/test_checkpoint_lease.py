from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_cli.cluster_backup_guard import backup_manifest_sha256, write_backup_manifest
from loom_cli.rollout.operator.backup import VerifiedBackup
from loom_cli.rollout.operator.checkpoint_lease import (
    CheckpointLeaseError,
    RestoreVerificationEvidence,
    build_restore_verified_lease,
    inspect_critical_checkpoint,
)
from loom_cli.rollout.operator.rollout_checkpoint import (
    ImmutableObjectReference,
    build_immutable_inventory,
)

NOW = datetime(2026, 7, 19, 20, tzinfo=UTC)


def _private_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    path.write_bytes(payload)
    path.chmod(0o600)


def _checkpoint(tmp_path: Path, *, schema_version: int = 2) -> VerifiedBackup:
    root = tmp_path / "20260719T200000Z-req-checkpoint1"
    root.mkdir(mode=0o700)
    postgres = root / "postgres" / "loom.dump"
    secrets = root / "secrets"
    secrets.mkdir(mode=0o700)
    _private_file(postgres, b"PGDMP\x00critical-snapshot")
    _private_file(secrets / "loom-db-auth.json", b'{"kind":"Secret"}\n')
    inventory = build_immutable_inventory(
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=17,
        schema_revision="0067",
        created_at=NOW,
        objects=(
            ImmutableObjectReference(
                bucket="loom-staging-artifacts",
                object_key="catalog/pinned.json",
                version_id="version-1",
                content_sha256="a" * 64,
                size_bytes=42,
                data_class="catalog",
                authoritative_source="catalog-registry-v1",
            ),
        ),
    )
    object_inventory = root / "object-inventory.json"
    _private_file(
        object_inventory,
        (
            json.dumps(
                {**inventory.to_dict(), "inventory_root": inventory.inventory_root},
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode(),
    )
    components = {
        "k8s_secrets": secrets,
        "postgres": postgres,
    }
    if schema_version == 2:
        components["object_inventory"] = object_inventory
    else:
        minio = root / "minio"
        minio.mkdir(mode=0o700)
        _private_file(minio / "object.bin", b"payload")
        components["minio"] = minio
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
        now=NOW,
        schema_version=schema_version,
    )
    manifest_path.chmod(0o600)
    return VerifiedBackup(
        manifest_path=manifest_path,
        manifest_sha256=backup_manifest_sha256(
            manifest_path,
            expected_owner_uid=os.geteuid(),
        ),
    )


def _restore(checkpoint, **changes: object) -> RestoreVerificationEvidence:
    values: dict[str, object] = {
        "verification_id": "restore-checkpoint1",
        "request_id": checkpoint.request_id,
        "checkpoint_evidence_sha256": checkpoint.evidence_digest,
        "manifest_sha256": checkpoint.manifest_sha256,
        "db_snapshot_identity": checkpoint.db_snapshot_identity,
        "object_inventory_root": checkpoint.object_inventory_root,
        "mutation_epoch": checkpoint.mutation_epoch,
        "schema_revision": checkpoint.schema_revision,
        "environment": checkpoint.environment,
        "namespace": checkpoint.namespace,
        "report_sha256": "f" * 64,
        "verified_at": NOW + timedelta(minutes=5),
    }
    values.update(changes)
    return RestoreVerificationEvidence(**values)  # type: ignore[arg-type]


def test_schema_v2_checkpoint_requires_isolated_restore_before_lease(tmp_path: Path) -> None:
    backup = _checkpoint(tmp_path)

    checkpoint = inspect_critical_checkpoint(
        backup,
        request_id="req-checkpoint1",
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.geteuid(),
        now=NOW + timedelta(minutes=1),
    )
    lease = build_restore_verified_lease(
        checkpoint,
        _restore(checkpoint),
        expires_at=NOW + timedelta(hours=6),
    )

    assert checkpoint.mutation_epoch == 17
    assert checkpoint.schema_revision == "0067"
    assert checkpoint.db_snapshot_identity == (
        "pgdump-sha256:" + checkpoint.component_sha256["postgres"]
    )
    assert lease.source_request_id == "req-checkpoint1"
    assert lease.restore_verified_at == NOW + timedelta(minutes=5)
    assert lease.component_sha256 == checkpoint.component_sha256


def test_full_minio_dr_manifest_cannot_become_rollout_lease(tmp_path: Path) -> None:
    backup = _checkpoint(tmp_path, schema_version=1)

    with pytest.raises(CheckpointLeaseError, match="schema version 2"):
        inspect_critical_checkpoint(
            backup,
            request_id="req-checkpoint1",
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.geteuid(),
            now=NOW + timedelta(minutes=1),
        )


def test_checkpoint_rejects_noncanonical_inventory_path(tmp_path: Path) -> None:
    backup = _checkpoint(tmp_path)
    manifest = json.loads(backup.manifest_path.read_text(encoding="utf-8"))
    inventory = backup.manifest_path.parent / "alternate-inventory.json"
    original = backup.manifest_path.parent / "object-inventory.json"
    _private_file(inventory, original.read_bytes())
    manifest["components"]["object_inventory"]["path"] = str(inventory)
    manifest["components"]["object_inventory"]["sha256"] = hashlib.sha256(
        inventory.read_bytes()
    ).hexdigest()
    backup.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    backup.manifest_path.chmod(0o600)
    changed = VerifiedBackup(
        manifest_path=backup.manifest_path,
        manifest_sha256=backup_manifest_sha256(
            backup.manifest_path,
            expected_owner_uid=os.geteuid(),
        ),
    )

    with pytest.raises(CheckpointLeaseError, match="path is not canonical"):
        inspect_critical_checkpoint(
            changed,
            request_id="req-checkpoint1",
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.geteuid(),
            now=NOW + timedelta(minutes=1),
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"mutation_epoch": 18},
        {"manifest_sha256": "0" * 64},
        {"object_inventory_root": "1" * 64},
        {"checkpoint_evidence_sha256": "2" * 64},
        {"environment": "staging-other"},
        {"schema_revision": "0068"},
    ],
)
def test_restore_evidence_must_match_every_checkpoint_field(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    checkpoint = inspect_critical_checkpoint(
        _checkpoint(tmp_path),
        request_id="req-checkpoint1",
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.geteuid(),
        now=NOW + timedelta(minutes=1),
    )

    with pytest.raises((CheckpointLeaseError, ValueError), match="restore"):
        build_restore_verified_lease(
            checkpoint,
            _restore(checkpoint, **changes),
            expires_at=NOW + timedelta(hours=6),
        )


def test_restore_evidence_must_be_fresh_and_precede_expiry(tmp_path: Path) -> None:
    checkpoint = inspect_critical_checkpoint(
        _checkpoint(tmp_path),
        request_id="req-checkpoint1",
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.geteuid(),
        now=NOW + timedelta(minutes=1),
    )

    with pytest.raises(CheckpointLeaseError, match="freshness"):
        build_restore_verified_lease(
            checkpoint,
            _restore(checkpoint, verified_at=NOW - timedelta(seconds=1)),
            expires_at=NOW + timedelta(hours=6),
        )
