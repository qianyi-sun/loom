from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.operator.backup_lease import BackupLease, evaluate_backup_lease

NOW = datetime(2026, 7, 19, 18, tzinfo=UTC)


def _lease() -> BackupLease:
    return BackupLease(
        lease_id="lease-12345678",
        source_request_id="req-12345678",
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "b" * 64, "authority": "c" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="lsn:0/16B6C50",
        schema_revision="0066",
        object_inventory_root="d" * 64,
        created_at=NOW - timedelta(minutes=20),
        restore_verified_at=NOW - timedelta(minutes=10),
        expires_at=NOW + timedelta(hours=2),
    )


def _evaluate(lease: BackupLease, **overrides):
    values = {
        "now": NOW,
        "environment": "staging",
        "namespace": "loom-staging",
        "mutation_epoch": 7,
        "db_snapshot_identity": "lsn:0/16B6C50",
        "schema_revision": "0066",
        "object_inventory_root": "d" * 64,
        "manifest_sha256": "a" * 64,
        "component_sha256": {"postgres": "b" * 64, "authority": "c" * 64},
    }
    values.update(overrides)
    return evaluate_backup_lease(lease, **values)


def test_exact_verified_unchanged_epoch_lease_is_eligible() -> None:
    result = _evaluate(_lease())

    assert result.eligible
    assert result.blockers == ()
    assert result.lease_digest == _lease().evidence_digest


def test_lease_collects_every_provenance_and_freshness_blocker() -> None:
    result = _evaluate(
        _lease(),
        now=NOW + timedelta(hours=3),
        environment="prod",
        namespace="other",
        mutation_epoch=8,
        db_snapshot_identity="other",
        schema_revision="0067",
        object_inventory_root="e" * 64,
        manifest_sha256="f" * 64,
        component_sha256={"postgres": "0" * 64},
    )

    assert result.eligible is False
    assert result.blockers == (
        "components",
        "db-snapshot",
        "environment",
        "freshness",
        "manifest",
        "mutation-epoch",
        "namespace",
        "object-inventory",
        "schema-revision",
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"manifest_sha256": "invalid"},
        {"component_sha256": {}},
        {"environment": "prod"},
        {"mutation_epoch": -1},
        {"restore_verified_at": NOW + timedelta(hours=3)},
    ],
)
def test_lease_constructor_rejects_incomplete_or_cross_environment_authority(changes) -> None:
    values = {
        "lease_id": "lease-12345678",
        "source_request_id": "req-12345678",
        "manifest_sha256": "a" * 64,
        "component_sha256": {"postgres": "b" * 64},
        "environment": "staging",
        "namespace": "loom-staging",
        "mutation_epoch": 7,
        "db_snapshot_identity": "lsn:0/16B6C50",
        "schema_revision": "0066",
        "object_inventory_root": "d" * 64,
        "created_at": NOW - timedelta(minutes=20),
        "restore_verified_at": NOW - timedelta(minutes=10),
        "expires_at": NOW + timedelta(hours=2),
    }
    values.update(changes)

    with pytest.raises(ValueError, match="backup lease"):
        BackupLease(**values)
