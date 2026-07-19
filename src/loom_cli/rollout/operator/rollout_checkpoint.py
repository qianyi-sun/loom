"""Critical rollout checkpoint authority, separate from asynchronous object DR."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMMUTABLE_CLASSES = frozenset({"benchmark", "catalog", "system"})


@dataclass(frozen=True, slots=True)
class ImmutableObjectReference:
    """Exact immutable object version referenced by a rollout checkpoint."""

    bucket: str
    object_key: str
    version_id: str
    content_sha256: str
    size_bytes: int
    data_class: str
    authoritative_source: str

    def __post_init__(self) -> None:
        if (
            not self.bucket
            or not self.object_key
            or self.object_key.startswith("/")
            or ".." in self.object_key.split("/")
            or not self.version_id
            or _SHA256_RE.fullmatch(self.content_sha256) is None
            or self.size_bytes < 0
            or self.data_class not in _IMMUTABLE_CLASSES
            or not self.authoritative_source
            or self.authoritative_source != self.authoritative_source.strip()
        ):
            raise ValueError("immutable object reference is invalid")

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.bucket, self.object_key, self.version_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "authoritative_source": self.authoritative_source,
            "bucket": self.bucket,
            "content_sha256": self.content_sha256,
            "data_class": self.data_class,
            "object_key": self.object_key,
            "size_bytes": self.size_bytes,
            "version_id": self.version_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ImmutableObjectReference:
        expected = {
            "authoritative_source",
            "bucket",
            "content_sha256",
            "data_class",
            "object_key",
            "size_bytes",
            "version_id",
        }
        if set(data) != expected or type(data["size_bytes"]) is not int:
            raise ValueError("immutable object reference schema is invalid")
        strings = {key: data[key] for key in expected - {"size_bytes"}}
        if not all(isinstance(value, str) for value in strings.values()):
            raise ValueError("immutable object reference schema is invalid")
        return cls(size_bytes=data["size_bytes"], **strings)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class ImmutableObjectInventory:
    """Content-addressed immutable references; no object payload is copied."""

    environment: str
    namespace: str
    mutation_epoch: int
    schema_revision: str
    created_at: datetime
    objects: tuple[ImmutableObjectReference, ...]

    def __post_init__(self) -> None:
        if (
            self.environment != "staging"
            or not self.namespace
            or self.namespace != self.namespace.strip()
            or self.mutation_epoch < 0
            or not self.schema_revision
            or self.schema_revision != self.schema_revision.strip()
            or self.created_at.tzinfo is None
            or self.created_at.utcoffset() is None
        ):
            raise ValueError("immutable object inventory identity is invalid")
        identities = [item.identity for item in self.objects]
        if identities != sorted(identities) or len(identities) != len(set(identities)):
            raise ValueError("immutable object inventory must be sorted and unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "created_at": self.created_at.isoformat(),
            "environment": self.environment,
            "mutation_epoch": self.mutation_epoch,
            "namespace": self.namespace,
            "objects": [item.to_dict() for item in self.objects],
            "schema_revision": self.schema_revision,
            "schema_version": 1,
        }

    @property
    def inventory_root(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ImmutableObjectInventory:
        expected = {
            "created_at",
            "environment",
            "mutation_epoch",
            "namespace",
            "objects",
            "schema_revision",
            "schema_version",
        }
        if (
            set(data) != expected
            or data["schema_version"] != 1
            or type(data["mutation_epoch"]) is not int
            or not isinstance(data["objects"], list)
            or not all(isinstance(item, dict) for item in data["objects"])
        ):
            raise ValueError("immutable object inventory schema is invalid")
        try:
            created_at = datetime.fromisoformat(str(data["created_at"]))
        except ValueError as exc:
            raise ValueError("immutable object inventory timestamp is invalid") from exc
        return cls(
            environment=str(data["environment"]),
            namespace=str(data["namespace"]),
            mutation_epoch=data["mutation_epoch"],
            schema_revision=str(data["schema_revision"]),
            created_at=created_at,
            objects=tuple(ImmutableObjectReference.from_dict(item) for item in data["objects"]),
        )


def build_immutable_inventory(
    *,
    environment: str,
    namespace: str,
    mutation_epoch: int,
    schema_revision: str,
    created_at: datetime,
    objects: Sequence[ImmutableObjectReference],
) -> ImmutableObjectInventory:
    return ImmutableObjectInventory(
        environment=environment,
        namespace=namespace,
        mutation_epoch=mutation_epoch,
        schema_revision=schema_revision,
        created_at=created_at,
        objects=tuple(sorted(objects, key=lambda item: item.identity)),
    )


__all__ = [
    "ImmutableObjectInventory",
    "ImmutableObjectReference",
    "build_immutable_inventory",
]
