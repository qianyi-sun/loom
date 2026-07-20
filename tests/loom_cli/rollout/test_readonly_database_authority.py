from __future__ import annotations

import re
from collections.abc import Mapping

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout import readonly_database_authority
from loom_cli.rollout.operator.rollout_checkpoint import ImmutableObjectReference
from loom_cli.rollout.readonly_database_authority import (
    probe_readonly_database,
    probe_readonly_database_baseline,
    probe_readonly_mutation_epoch,
)


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
            match = re.search(r"LIMIT (\d+) OFFSET (\d+)\Z", sql)
            if match is None:
                raise AssertionError("immutable inventory query is not bounded")
            limit, offset = (int(value) for value in match.groups())
            return self.inventory[offset : offset + limit]
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


def test_capacity_query_floors_fractional_seconds_for_exact_identity() -> None:
    assert (
        "floor(extract(epoch FROM observed_at))::bigint"
        in readonly_database_authority._CAPACITY_SQL
    )


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


def test_current_database_binds_unversioned_legacy_pinned_inventory() -> None:
    row = {
        "authoritative_source": None,
        "authority_id": "11111111-1111-4111-8111-111111111111",
        "bucket": "loom-staging-artifacts",
        "content_sha256": "a" * 64,
        "data_class": "catalog",
        "object_key": "catalog/exact.json",
        "owner_id": "catalog:legacy",
        "owner_kind": "system",
        "size_bytes": 42,
        "version_id": None,
    }
    query = Query(revision="0066", inventory=(row,))

    evidence = probe_readonly_database(query)

    assert evidence.immutable_objects[0].version_id == "content-sha256:" + "a" * 64
    assert evidence.immutable_objects[0].authoritative_source.startswith(
        "lifecycle-authority:sha256:"
    )


def test_current_database_sorts_after_legacy_version_normalization() -> None:
    def row(content_sha256: str, authority_id: str) -> dict[str, object]:
        return {
            "authoritative_source": None,
            "authority_id": authority_id,
            "bucket": "loom-staging-artifacts",
            "content_sha256": content_sha256,
            "data_class": "catalog",
            "object_key": "catalog/exact.json",
            "owner_id": f"catalog:{authority_id}",
            "owner_kind": "system",
            "size_bytes": 42,
            "version_id": None,
        }

    query = Query(
        revision="0066",
        inventory=(
            row("b" * 64, "22222222-2222-4222-8222-222222222222"),
            row("a" * 64, "11111111-1111-4111-8111-111111111111"),
        ),
    )

    evidence = probe_readonly_database(query)

    assert [item.version_id for item in evidence.immutable_objects] == [
        "content-sha256:" + "a" * 64,
        "content-sha256:" + "b" * 64,
    ]


def test_current_database_rejects_unversioned_inventory_without_exact_digest() -> None:
    row = {
        "authoritative_source": None,
        "authority_id": "11111111-1111-4111-8111-111111111111",
        "bucket": "loom-staging-artifacts",
        "content_sha256": None,
        "data_class": "catalog",
        "object_key": "catalog/exact.json",
        "owner_id": "catalog:legacy",
        "owner_kind": "system",
        "size_bytes": 42,
        "version_id": None,
    }

    with pytest.raises(ValueError, match="immutable inventory is invalid"):
        probe_readonly_database(Query(revision="0066", inventory=(row,)))


def test_immutable_inventory_query_binds_legacy_authority_identity() -> None:
    assert "auth.id::text AS authority_id" in readonly_database_authority._INVENTORY_SQL
    assert "auth.owner_kind, auth.owner_id" in readonly_database_authority._INVENTORY_SQL


def test_current_database_pages_immutable_inventory_in_same_snapshot() -> None:
    inventory = tuple(
        {
            "authoritative_source": f"catalog:sha256:{index:064x}",
            "bucket": "loom-staging-artifacts",
            "content_sha256": f"{index:064x}",
            "data_class": "catalog",
            "object_key": f"catalog/{index:04d}.json",
            "size_bytes": index,
            "version_id": "v1",
        }
        for index in range(1_939)
    )
    query = Query(revision="0066", inventory=inventory)

    evidence = probe_readonly_database(query)

    assert len(evidence.immutable_objects) == 1_939
    inventory_calls = [sql for sql in query.calls if "FROM data_lifecycle_objects" in sql]
    assert len(inventory_calls) == 2
    assert inventory_calls[0].endswith("LIMIT 1024 OFFSET 0")
    assert inventory_calls[1].endswith("LIMIT 1024 OFFSET 1024")


def test_current_database_rejects_inventory_at_admission_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inventory = tuple(
        {
            "authoritative_source": f"catalog:sha256:{index:064x}",
            "bucket": "loom-staging-artifacts",
            "content_sha256": f"{index:064x}",
            "data_class": "catalog",
            "object_key": f"catalog/{index:04d}.json",
            "size_bytes": index,
            "version_id": "v1",
        }
        for index in range(2)
    )
    query = Query(revision="0066", inventory=inventory)
    monkeypatch.setattr(readonly_database_authority, "STAGING_ADMISSION_OBJECT_LIMIT", 2)

    with pytest.raises(ValueError, match="immutable inventory exceeds policy"):
        probe_readonly_database(query)


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


def test_checkpoint_inventory_drift_does_not_mask_database_baseline() -> None:
    query = Query(
        revision="0067",
        inventory=(
            {
                "authoritative_source": "",
                "bucket": "loom-staging-artifacts",
                "content_sha256": "a" * 64,
                "data_class": "catalog",
                "object_key": "catalog/unversioned.json",
                "size_bytes": 42,
                "version_id": "",
            },
        ),
    )

    evidence = probe_readonly_database_baseline(query)

    assert evidence.mutation_epoch == 9
    assert evidence.baseline_counts["tasks"] == 5
    assert evidence.immutable_objects == ()
    assert all("FROM data_lifecycle_objects" not in sql for sql in query.calls)


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


def test_missing_capacity_remains_explicit_without_masking_database_baseline() -> None:
    query = Query(revision="0067")
    original = query.__call__

    def missing(sql: str) -> tuple[Mapping[str, object], ...]:
        if "staging_lifecycle_capacity" in sql:
            return ()
        return original(sql)

    evidence = probe_readonly_database(missing)

    assert evidence.schema_revision == "0067"
    assert evidence.mutation_epoch == 9
    assert evidence.baseline_counts["tasks"] == 5
    assert evidence.capacity is None
    assert any("FROM data_lifecycle_objects" in sql for sql in query.calls)


def test_duplicate_capacity_authority_is_rejected() -> None:
    query = Query(revision="0067")
    original = query.__call__

    def duplicated(sql: str) -> tuple[Mapping[str, object], ...]:
        rows = original(sql)
        if "staging_lifecycle_capacity" in sql:
            return rows + rows
        return rows

    with pytest.raises(ValueError, match="capacity evidence is incomplete"):
        probe_readonly_database(duplicated)


def test_epoch_probe_does_not_let_missing_capacity_mask_the_preflight_dag() -> None:
    query = Query(revision="0067")
    original = query.__call__

    def missing(sql: str) -> tuple[Mapping[str, object], ...]:
        if "staging_lifecycle_capacity" in sql:
            return ()
        return original(sql)

    evidence = probe_readonly_mutation_epoch(missing)

    assert evidence.schema_revision == "0067"
    assert evidence.mutation_epoch == 9
    assert evidence.epoch_authority == "staging-mutation-epoch-v1"
    assert len(evidence.evidence_sha256) == 64
    assert all("staging_lifecycle_capacity" not in sql for sql in query.calls)
    assert all("AS provider_models" not in sql for sql in query.calls)
