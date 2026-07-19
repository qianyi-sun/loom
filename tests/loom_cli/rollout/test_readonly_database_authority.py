from __future__ import annotations

from collections.abc import Mapping

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.operator.rollout_checkpoint import ImmutableObjectReference
from loom_cli.rollout.readonly_database_authority import probe_readonly_database


class Query:
    def __init__(
        self,
        *,
        revision: str = "0065",
        inventory: tuple[Mapping[str, object], ...] = (),
    ) -> None:
        self.revision = revision
        self.inventory = inventory
        self.calls: list[str] = []

    def __call__(self, sql: str) -> tuple[Mapping[str, object], ...]:
        self.calls.append(sql)
        if "pg_catalog.pg_roles" in sql:
            return (
                {
                    "role_name": "loom_rollout_readonly",
                    "transaction_read_only": "on",
                    "rolcanlogin": True,
                    "rolsuper": False,
                    "rolinherit": False,
                    "rolcreaterole": False,
                    "rolcreatedb": False,
                    "rolreplication": False,
                    "rolbypassrls": False,
                    "can_connect": True,
                    "can_create_temp": False,
                    "can_create_public": False,
                    "write_table_privileges": 0,
                    "can_select_baseline": True,
                },
            )
        if "alembic_version" in sql:
            return ({"schema_revision": self.revision},)
        if "provider_models_cache" in sql:
            return ({"teams": 2, "users": 3, "agents": 4, "tasks": 5, "provider_models": 6},)
        if "staging_mutation_epochs" in sql:
            return ({"environment": "staging", "namespace": "loom-staging", "epoch": 9},)
        if "FROM data_lifecycle_objects" in sql:
            return self.inventory
        if "staging_lifecycle_capacity" in sql:
            capacity = StagingCapacity(10, 20, 80, 90)
            return (
                {
                    "environment": "staging",
                    "namespace": "loom-staging",
                    "object_count": 10,
                    "bytes_used": 20,
                    "disk_free_percent": 80,
                    "inode_free_percent": 90,
                    "policy_sha256": staging_capacity_policy_digest(),
                    "evidence_sha256": capacity.evidence_digest,
                    "source": "exact-object-inventory-v1",
                    "observed_at_epoch": 1_721_390_400,
                },
            )
        raise AssertionError(sql)


def test_legacy_database_uses_schema_bound_bootstrap_epoch() -> None:
    query = Query(revision="0065")

    evidence = probe_readonly_database(query)

    assert evidence.schema_revision == "0065"
    assert evidence.mutation_epoch == 0
    assert evidence.epoch_authority == "legacy-pre-0066"
    assert evidence.capacity is None
    assert all("staging_mutation_epochs" not in sql for sql in query.calls)
    assert all("staging_lifecycle_capacity" not in sql for sql in query.calls)


def test_current_database_requires_exact_epoch_and_capacity() -> None:
    query = Query(revision="0067")

    evidence = probe_readonly_database(query)

    assert evidence.mutation_epoch == 9
    assert evidence.epoch_authority == "staging-mutation-epoch-v1"
    assert evidence.baseline_counts["tasks"] == 5
    assert evidence.capacity is not None
    assert evidence.capacity["bytes_used"] == 20
    assert len(evidence.evidence_sha256) == 64


def test_current_database_binds_sorted_immutable_inventory_in_same_snapshot() -> None:
    row = {
        "authoritative_source": "catalog:sha256:" + "f" * 64,
        "bucket": "loom-staging-artifacts",
        "content_sha256": "a" * 64,
        "data_class": "catalog",
        "object_key": "catalog/exact.json",
        "size_bytes": 42,
        "version_id": "v1",
    }
    query = Query(revision="0066", inventory=(row,))

    evidence = probe_readonly_database(query)

    assert evidence.immutable_objects == (ImmutableObjectReference.from_dict(row),)
    assert any("FROM data_lifecycle_objects" in sql for sql in query.calls)


def test_current_database_rejects_unclassified_immutable_inventory() -> None:
    query = Query(
        revision="0066",
        inventory=(
            {
                "authoritative_source": "",
                "bucket": "loom-staging-artifacts",
                "content_sha256": "a" * 64,
                "data_class": "artifact",
                "object_key": "runs/unclassified",
                "size_bytes": 42,
                "version_id": "v1",
            },
        ),
    )

    with pytest.raises(ValueError, match="immutable inventory is invalid"):
        probe_readonly_database(query)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("transaction_read_only", "off"),
        ("role_name", "loom"),
        ("rolsuper", True),
        ("rolinherit", True),
        ("rolbypassrls", True),
        ("can_create_temp", True),
        ("can_create_public", True),
        ("write_table_privileges", 1),
        ("can_select_baseline", False),
    ),
)
def test_database_role_drift_is_rejected(field: str, value: object) -> None:
    query = Query()
    original = query.__call__

    def drifted(sql: str) -> tuple[Mapping[str, object], ...]:
        rows = original(sql)
        if "pg_catalog.pg_roles" not in sql:
            return rows
        row = dict(rows[0])
        row[field] = value
        return (row,)

    with pytest.raises(ValueError, match="role authority drifted"):
        probe_readonly_database(drifted)


def test_new_schema_cannot_fall_back_when_epoch_is_missing() -> None:
    query = Query(revision="0066")
    original = query.__call__

    def missing(sql: str) -> tuple[Mapping[str, object], ...]:
        if "staging_mutation_epochs" in sql:
            return ()
        return original(sql)

    with pytest.raises(ValueError, match="epoch evidence is incomplete"):
        probe_readonly_database(missing)


def test_capacity_is_required_from_revision_0067() -> None:
    query = Query(revision="0067")
    original = query.__call__

    def missing(sql: str) -> tuple[Mapping[str, object], ...]:
        if "staging_lifecycle_capacity" in sql:
            return ()
        return original(sql)

    with pytest.raises(ValueError, match="capacity evidence is incomplete"):
        probe_readonly_database(missing)
