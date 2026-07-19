"""Version-aware, read-only object-store reconciliation for lifecycle GC."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import replace
from typing import Any, Protocol

from loom.data_lifecycle_gc import GcScope, ObservedObject, reconcile_object_inventory
from loom.data_lifecycle_inventory_sql import LifecycleInventorySnapshot


class LifecycleInventoryLoader(Protocol):
    def load(self, *, scope: GcScope) -> LifecycleInventorySnapshot: ...


def _validated_buckets(values: Iterable[str]) -> tuple[str, ...]:
    buckets = tuple(sorted(set(values)))
    if not buckets or any(not value or value != value.strip() for value in buckets):
        raise ValueError("lifecycle inventory buckets must be non-empty and normalized")
    return buckets


class S3ObservedObjectInventory:
    """Enumerate exact object identities without granting deletion authority."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def load(self, *, buckets: Sequence[str]) -> tuple[ObservedObject, ...]:
        observed: list[ObservedObject] = []
        for bucket in _validated_buckets(buckets):
            versioning = self._client.get_bucket_versioning(Bucket=bucket).get("Status")
            if versioning in {"Enabled", "Suspended"}:
                observed.extend(self._load_versions(bucket))
            elif versioning in {None, ""}:
                observed.extend(self._load_current(bucket))
            else:
                raise RuntimeError(f"bucket {bucket} returned unknown versioning status")
        identities = [item.identity for item in observed]
        if len(identities) != len(set(identities)):
            raise RuntimeError("object inventory returned duplicate identities")
        return tuple(sorted(observed, key=lambda item: item.identity))

    def _load_current(self, bucket: str) -> list[ObservedObject]:
        paginator = self._client.get_paginator("list_objects_v2")
        result: list[ObservedObject] = []
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Contents", ()):  # current objects only
                result.append(
                    ObservedObject(
                        bucket=bucket,
                        object_key=str(item["Key"]),
                        version_id=None,
                        size_bytes=int(item["Size"]),
                    )
                )
        return result

    def _load_versions(self, bucket: str) -> list[ObservedObject]:
        paginator = self._client.get_paginator("list_object_versions")
        result: list[ObservedObject] = []
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Versions", ()):
                result.append(
                    ObservedObject(
                        bucket=bucket,
                        object_key=str(item["Key"]),
                        version_id=str(item["VersionId"]),
                        size_bytes=int(item["Size"]),
                    )
                )
            for marker in page.get("DeleteMarkers", ()):
                result.append(
                    ObservedObject(
                        bucket=bucket,
                        object_key=str(marker["Key"]),
                        version_id=str(marker["VersionId"]),
                        size_bytes=0,
                    )
                )
        return result


class ReconcilingLifecycleInventory:
    """Attach exact two-way object reconciliation to the SQL snapshot."""

    def __init__(
        self,
        database: LifecycleInventoryLoader,
        objects: S3ObservedObjectInventory,
        *,
        buckets: Sequence[str],
    ) -> None:
        self._database = database
        self._objects = objects
        self._buckets = _validated_buckets(buckets)

    def load(self, *, scope: GcScope) -> LifecycleInventorySnapshot:
        snapshot = self._database.load(scope=scope)
        observed = self._objects.load(buckets=self._buckets)
        registered_buckets = {item.bucket for item in snapshot.objects}
        unknown_registered = registered_buckets - set(self._buckets)
        if unknown_registered:
            raise RuntimeError(
                "registered lifecycle objects use non-inventoried buckets: "
                + ",".join(sorted(unknown_registered))
            )
        return replace(
            snapshot,
            reconciliation=reconcile_object_inventory(
                registered=snapshot.objects,
                observed=observed,
            ),
        )


__all__ = ["ReconcilingLifecycleInventory", "S3ObservedObjectInventory"]
