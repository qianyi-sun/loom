from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from loom_cli.cluster_backup_guard import backup_manifest_sha256, write_backup_manifest
from loom_cli.rollout.operator.backup import VerifiedBackup
from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.checkpoint_lease import (
    CheckpointLeaseError,
    RestoreVerificationEvidence,
    build_restore_verified_lease,
    inspect_critical_checkpoint,
)
from loom_cli.rollout.operator.protected_secret_inventory import (
    PROTECTED_SECRET_SPECS,
    build_secret_inventory,
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


def _checkpoint(tmp_path: Path, *, schema_version: int = 3) -> VerifiedBackup:
    root = tmp_path / "20260719T200000Z-req-checkpoint1"
    root.mkdir(mode=0o700)
    postgres = root / "postgres" / "loom.dump"
    secrets = root / "secrets"
    secrets.mkdir(mode=0o700)
    _private_file(postgres, b"PGDMP\x00critical-snapshot")
    for name in ("loom-admin-secret", "loom-secrets", "loom-staging-tls"):
        _private_file(
            secrets / f"{name}.yaml",
            (
                json.dumps(
                    {
                        "apiVersion": "v1",
                        "data": {"token": "c2Vuc2l0aXZl"},
                        "kind": "Secret",
                        "metadata": {"name": name, "namespace": "loom-staging"},
                        "type": "Opaque",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode(),
        )
    observations = {}
    for spec in PROTECTED_SECRET_SPECS:
        present = spec.required
        payload = (
            json.dumps(
                {
                    "apiVersion": "v1",
                    "data": {"token": "c2Vuc2l0aXZl"},
                    "kind": "Secret",
                    "metadata": {
                        "name": spec.name,
                        "namespace": spec.namespace,
                        "resourceVersion": "7",
                        "uid": "11111111-1111-4111-8111-111111111111",
                    },
                    "type": "Opaque",
                }
            ).encode()
            if present
            else None
        )
        observations[(spec.namespace, spec.name)] = (payload, payload)
    secret_inventory = build_secret_inventory(observations)
    for filename, payload in secret_inventory.exported_objects.items():
        _private_file(secrets / filename, payload)
    _private_file(
        secrets / "protected-capacity-secret-inventory.json",
        secret_inventory.inventory_payload,
    )
    inventory = build_immutable_inventory(
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=17,
        schema_revision="0067_global_capacity",
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
    if schema_version in {2, 3}:
        components["object_inventory"] = object_inventory
        if schema_version == 3:
            authority = DatabaseAuthorityEvidence(
                public_schema_revision="0067_global_capacity",
                capacity_guard_schema_revision="guard_0028",
                configuration_epoch=9,
                configuration_digest="9" * 64,
                authority_incarnation=UUID("00000000-0000-4000-8000-0000000000aa"),
                writer_epoch=4,
                execution_state="shadow",
                execution_epoch=0,
                execution_manifest_sha256=None,
                executable_new_capacity_ceiling=0,
                increase_freeze=True,
            )
            authority_path = root / "database-authority.json"
            _private_file(authority_path, authority.payload)
            components["database_authority"] = authority_path
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
        "checkpoint_schema_version": 3,
        "component_sha256": checkpoint.component_sha256,
        "database_authority_digest": checkpoint.database_authority_digest,
        "public_schema_revision": checkpoint.public_schema_revision,
        "capacity_guard_schema_revision": checkpoint.capacity_guard_schema_revision,
        "manager_configuration_epoch": checkpoint.manager_configuration_epoch,
        "manager_configuration_digest": checkpoint.manager_configuration_digest,
        "manager_authority_incarnation": checkpoint.manager_authority_incarnation,
        "manager_writer_epoch": checkpoint.manager_writer_epoch,
        "manager_execution_state": checkpoint.manager_execution_state,
        "manager_execution_epoch": checkpoint.manager_execution_epoch,
        "manager_execution_manifest_sha256": checkpoint.manager_execution_manifest_sha256,
        "manager_executable_new_capacity_ceiling": (
            checkpoint.manager_executable_new_capacity_ceiling
        ),
        "manager_increase_freeze": checkpoint.manager_increase_freeze,
    }
    values.update(changes)
    return RestoreVerificationEvidence(**values)  # type: ignore[arg-type]


def test_schema_v3_checkpoint_carries_typed_database_authority(tmp_path: Path) -> None:
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
    assert checkpoint.schema_revision == "0067_global_capacity"
    assert checkpoint.db_snapshot_identity == (
        "pgdump-sha256:" + checkpoint.component_sha256["postgres"]
    )
    assert checkpoint.database_authority_digest == checkpoint.component_sha256["database_authority"]
    assert checkpoint.public_schema_revision == "0067_global_capacity"
    assert checkpoint.capacity_guard_schema_revision == "guard_0028"
    assert checkpoint.manager_configuration_epoch == 9
    assert checkpoint.manager_configuration_digest == "9" * 64
    assert checkpoint.manager_authority_incarnation == UUID("00000000-0000-4000-8000-0000000000aa")
    assert checkpoint.manager_writer_epoch == 4
    assert lease.source_request_id == "req-checkpoint1"
    assert lease.restore_verified_at == NOW + timedelta(minutes=5)
    assert lease.component_sha256 == checkpoint.component_sha256
    assert lease.checkpoint_schema_version == 3
    assert lease.database_authority_digest == checkpoint.database_authority_digest
    assert lease.manager_configuration_epoch == checkpoint.manager_configuration_epoch


def test_restore_report_digest_is_bound_into_lease(tmp_path: Path) -> None:
    checkpoint = inspect_critical_checkpoint(
        _checkpoint(tmp_path),
        request_id="req-checkpoint1",
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.geteuid(),
        now=NOW + timedelta(minutes=1),
    )
    restore = _restore(checkpoint)
    lease = build_restore_verified_lease(checkpoint, restore, expires_at=NOW + timedelta(hours=6))
    assert lease.restore_report_sha256 == restore.report_sha256
    alternate = build_restore_verified_lease(
        checkpoint,
        replace(restore, report_sha256="e" * 64),
        expires_at=NOW + timedelta(hours=6),
    )
    assert alternate.restore_report_sha256 != lease.restore_report_sha256


def test_restore_evidence_schema_three_round_trip_rejects_mixed_fields(tmp_path: Path) -> None:
    checkpoint = inspect_critical_checkpoint(
        _checkpoint(tmp_path),
        request_id="req-checkpoint1",
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.geteuid(),
        now=NOW + timedelta(minutes=1),
    )
    evidence = _restore(checkpoint)

    record = evidence.to_dict()
    assert record["schema_version"] == 2
    assert record["checkpoint_schema_version"] == 3
    assert RestoreVerificationEvidence.from_dict(record) == evidence

    record.pop("database_authority_digest")
    with pytest.raises(ValueError, match="schema"):
        RestoreVerificationEvidence.from_dict(record)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("capacity_guard_schema_revision", "guard_0028"),
        ("manager_execution_manifest_sha256", "f" * 64),
    ],
)
def test_historical_restore_constructor_rejects_schema_three_only_fields(
    field: str,
    value: str,
) -> None:
    historical = RestoreVerificationEvidence(
        verification_id="restore-checkpoint1",
        request_id="req-checkpoint1",
        checkpoint_evidence_sha256="a" * 64,
        manifest_sha256="b" * 64,
        db_snapshot_identity="pgdump-sha256:" + "c" * 64,
        object_inventory_root="d" * 64,
        mutation_epoch=17,
        schema_revision="0067_global_capacity",
        environment="staging",
        namespace="loom-staging",
        report_sha256="e" * 64,
        verified_at=NOW + timedelta(minutes=5),
    )

    with pytest.raises(ValueError, match="historical restore evidence"):
        replace(historical, **{field: value})


def test_checkpoint_rejects_database_authority_digest_outside_component_binding(
    tmp_path: Path,
) -> None:
    checkpoint = inspect_critical_checkpoint(
        _checkpoint(tmp_path),
        request_id="req-checkpoint1",
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.geteuid(),
        now=NOW + timedelta(minutes=1),
    )

    with pytest.raises(ValueError, match="component authority"):
        replace(
            checkpoint,
            component_sha256={
                **checkpoint.component_sha256,
                "database_authority": "0" * 64,
            },
        )


def test_full_minio_dr_manifest_cannot_become_rollout_lease(tmp_path: Path) -> None:
    backup = _checkpoint(tmp_path, schema_version=1)

    with pytest.raises(CheckpointLeaseError, match="schema version 3"):
        inspect_critical_checkpoint(
            backup,
            request_id="req-checkpoint1",
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.geteuid(),
            now=NOW + timedelta(minutes=1),
        )


def test_historical_schema_v2_manifest_cannot_issue_new_rollout_authority(tmp_path: Path) -> None:
    backup = _checkpoint(tmp_path, schema_version=2)

    with pytest.raises(CheckpointLeaseError, match="schema version 3"):
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
        {"schema_revision": "0069"},
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
