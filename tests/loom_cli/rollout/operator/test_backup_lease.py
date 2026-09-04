from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from loom_cli.rollout.operator.backup_lease import (
    BackupLease,
    component_set_digest,
    evaluate_backup_lease,
)

NOW = datetime(2026, 7, 19, 18, tzinfo=UTC)
AUTHORITY_DIGEST = "9450f793871aaaf62362a281605b0af15c5b8ca2859fa279616624fbcee1a03e"


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


def _schema_three_lease() -> BackupLease:
    return BackupLease(
        lease_id="lease-schema-three",
        source_request_id="req-schema-three",
        manifest_sha256="a" * 64,
        component_sha256={
            "database_authority": AUTHORITY_DIGEST,
            "k8s_secrets": "c" * 64,
            "object_inventory": "d" * 64,
            "postgres": "e" * 64,
        },
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        schema_revision="0066",
        object_inventory_root="f" * 64,
        created_at=NOW - timedelta(minutes=20),
        restore_verified_at=NOW - timedelta(minutes=10),
        expires_at=NOW + timedelta(hours=2),
        checkpoint_schema_version=3,
        database_authority_digest=AUTHORITY_DIGEST,
        public_schema_revision="0066",
        capacity_guard_schema_revision="guard_0027",
        manager_configuration_epoch=9,
        manager_configuration_digest="1" * 64,
        manager_authority_incarnation=UUID("00000000-0000-4000-8000-0000000000aa"),
        manager_writer_epoch=4,
        manager_execution_state="shadow",
        manager_execution_epoch=0,
        manager_execution_manifest_sha256=None,
        manager_executable_new_capacity_ceiling=0,
        manager_increase_freeze=True,
        restore_report_sha256="f" * 64,
    )


def _evaluate(lease: BackupLease, **overrides):
    values = {
        "now": NOW,
        "source_request_id": "req-12345678",
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


def test_lease_round_trips_complete_restore_authority() -> None:
    lease = _lease()

    assert BackupLease.from_dict(lease.to_dict()) == lease
    assert len(lease.evidence_digest) == 64


def test_new_lease_round_trips_schema_three_database_authority() -> None:
    lease = _schema_three_lease()

    record = lease.to_dict()

    assert record["schema_version"] == 2
    assert record["checkpoint_schema_version"] == 3
    assert record["database_authority_digest"] == lease.component_sha256["database_authority"]
    assert record["manager_authority_incarnation"] == ("00000000-0000-4000-8000-0000000000aa")
    assert BackupLease.from_dict(record) == lease
    assert replace(lease, restore_report_sha256="0" * 64).evidence_digest != lease.evidence_digest


def test_lease_reader_preserves_historical_schema_and_rejects_mixed_fields() -> None:
    historical = _lease().to_dict()
    assert historical["schema_version"] == 1
    assert BackupLease.from_dict(historical) == _lease()

    mixed_old = {**historical, "database_authority_digest": "b" * 64}
    with pytest.raises(ValueError, match="schema"):
        BackupLease.from_dict(mixed_old)

    mixed_new = _schema_three_lease().to_dict()
    mixed_new.pop("manager_writer_epoch")
    with pytest.raises(ValueError, match="schema"):
        BackupLease.from_dict(mixed_new)

    invalid_report = _schema_three_lease().to_dict()
    invalid_report["restore_report_sha256"] = 7
    with pytest.raises(ValueError, match="schema"):
        BackupLease.from_dict(invalid_report)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("capacity_guard_schema_revision", "guard_0027"),
        ("manager_execution_manifest_sha256", "f" * 64),
    ],
)
def test_historical_lease_constructor_rejects_schema_three_only_fields(
    field: str,
    value: str,
) -> None:
    with pytest.raises(ValueError, match="historical backup lease"):
        replace(_lease(), **{field: value})


def test_schema_three_lease_requires_exact_four_component_authority() -> None:
    lease = _schema_three_lease()

    with pytest.raises(ValueError, match="schema-3"):
        replace(lease, component_sha256={"database_authority": AUTHORITY_DIGEST})


def test_lease_collects_every_provenance_and_freshness_blocker() -> None:
    result = _evaluate(
        _lease(),
        now=NOW + timedelta(hours=3),
        source_request_id="req-87654321",
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
        "source-request",
    )


def test_component_set_digest_is_order_independent_and_strict() -> None:
    assert component_set_digest({"postgres": "a" * 64, "authority": "b" * 64}) == (
        component_set_digest({"authority": "b" * 64, "postgres": "a" * 64})
    )

    with pytest.raises(ValueError, match="component hashes"):
        component_set_digest({"../postgres": "a" * 64})


@pytest.mark.parametrize(
    "changes",
    [
        {"manifest_sha256": "invalid"},
        {"component_sha256": {}},
        {"environment": ""},
        {"lease_id": "invalid"},
        {"source_request_id": "invalid"},
        {"namespace": "Invalid"},
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


@pytest.mark.parametrize(
    "changes",
    [
        {"source_request_id": "invalid"},
        {"environment": ""},
        {"namespace": "Invalid"},
        {"mutation_epoch": -1},
        {"manifest_sha256": "invalid"},
        {"component_sha256": {"../postgres": "b" * 64}},
    ],
)
def test_evaluation_rejects_invalid_expectation_authority(changes) -> None:
    with pytest.raises(ValueError, match="backup lease"):
        _evaluate(_lease(), **changes)
