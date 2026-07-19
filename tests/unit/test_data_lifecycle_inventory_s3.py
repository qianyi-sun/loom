from __future__ import annotations

from uuid import uuid4

import pytest

from loom.data_lifecycle_gc import GcScope, RegisteredObject
from loom.data_lifecycle_inventory_s3 import (
    ReconcilingLifecycleInventory,
    S3ObservedObjectInventory,
)
from loom.data_lifecycle_inventory_sql import LifecycleInventorySnapshot

SCOPE = GcScope(environment="staging", namespace="loom-staging")


class _Paginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages


class _Client:
    def __init__(self) -> None:
        self.current = _Paginator([{"Contents": [{"Key": "a", "Size": 1}]}])
        self.versions = _Paginator(
            [
                {
                    "Versions": [{"Key": "b", "VersionId": "v1", "Size": 2}],
                    "DeleteMarkers": [{"Key": "old", "VersionId": "d1"}],
                }
            ]
        )

    def get_bucket_versioning(self, **kwargs):
        return {"Status": "Enabled"} if kwargs["Bucket"] == "versioned" else {}

    def get_paginator(self, name: str):
        return self.versions if name == "list_object_versions" else self.current


def test_inventory_lists_current_versions_and_delete_markers_exactly() -> None:
    client = _Client()
    observed = S3ObservedObjectInventory(client).load(buckets=["plain", "versioned", "plain"])

    assert [item.identity for item in observed] == [
        ("plain", "a", ""),
        ("versioned", "b", "v1"),
        ("versioned", "old", "d1"),
    ]
    assert client.current.calls == [{"Bucket": "plain"}]
    assert client.versions.calls == [{"Bucket": "versioned"}]


def test_reconciliation_binds_both_orphan_directions_and_size_drift() -> None:
    authority_id = uuid4()
    registered = (
        RegisteredObject(
            id=uuid4(),
            authority_id=authority_id,
            environment="staging",
            namespace="loom-staging",
            bucket="plain",
            object_key="a",
            version_id=None,
            content_sha256="a" * 64,
            size_bytes=9,
            state="active",
        ),
        RegisteredObject(
            id=uuid4(),
            authority_id=authority_id,
            environment="staging",
            namespace="loom-staging",
            bucket="plain",
            object_key="missing",
            version_id=None,
            content_sha256="b" * 64,
            size_bytes=1,
            state="active",
        ),
    )

    class Database:
        def load(self, *, scope):
            assert scope == SCOPE
            return LifecycleInventorySnapshot(
                scope=SCOPE,
                mutation_epoch=1,
                authorities=(),
                objects=registered,
                unclassified_rows=(),
            )

    snapshot = ReconcilingLifecycleInventory(
        Database(),  # type: ignore[arg-type]
        S3ObservedObjectInventory(_Client()),
        buckets=["plain"],
    ).load(scope=SCOPE)

    assert snapshot.reconciliation.registered_missing == (("plain", "missing", ""),)
    assert snapshot.reconciliation.observed_unregistered == ()
    assert snapshot.reconciliation.registered_size_drift == (("plain", "a", ""),)
    assert any("missing" in blocker for blocker in snapshot.blockers)
    assert any("size drifted" in blocker for blocker in snapshot.blockers)


def test_registered_bucket_outside_allowlist_fails_closed() -> None:
    item = RegisteredObject(
        id=uuid4(),
        authority_id=uuid4(),
        environment="staging",
        namespace="loom-staging",
        bucket="other",
        object_key="x",
        version_id=None,
        content_sha256="c" * 64,
        size_bytes=1,
        state="active",
    )

    class Database:
        def load(self, *, scope):
            return LifecycleInventorySnapshot(scope, 1, (), (item,), ())

    with pytest.raises(RuntimeError, match="non-inventoried buckets"):
        ReconcilingLifecycleInventory(
            Database(),  # type: ignore[arg-type]
            S3ObservedObjectInventory(_Client()),
            buckets=["plain"],
        ).load(scope=SCOPE)
