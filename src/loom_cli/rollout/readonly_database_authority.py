"""Cross-version SELECT-only PostgreSQL authority for Tier 2 preflight."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.operator.rollout_checkpoint import ImmutableObjectReference

_REVISION_RE = re.compile(r"^[0-9]{4}$")
_ROLE = "loom_rollout_readonly"
_LEGACY_LAST_REVISION = 65
_EPOCH_FIRST_REVISION = 66
_CAPACITY_FIRST_REVISION = 67

_AUTHORITY_SQL = """
SELECT current_user AS role_name,
       current_setting('transaction_read_only') AS transaction_read_only,
       rolcanlogin, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
       rolreplication, rolbypassrls,
       has_database_privilege(current_user, current_database(), 'CONNECT') AS can_connect,
       has_database_privilege(current_user, current_database(), 'TEMP') AS can_create_temp,
       has_schema_privilege(current_user, 'public', 'CREATE') AS can_create_public,
       (
         SELECT count(*)
         FROM information_schema.role_table_grants
         WHERE grantee = current_user
           AND privilege_type IN
             ('INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')
       ) AS write_table_privileges,
       has_table_privilege(current_user, 'public.teams', 'SELECT')
       AND has_table_privilege(current_user, 'public.users', 'SELECT')
       AND has_table_privilege(current_user, 'public.agents', 'SELECT')
       AND has_table_privilege(current_user, 'public.tasks', 'SELECT')
       AND has_table_privilege(current_user, 'public.provider_models_cache', 'SELECT')
         AS can_select_baseline
FROM pg_catalog.pg_roles
WHERE rolname = current_user
""".strip()

_REVISION_SQL = "SELECT version_num AS schema_revision FROM alembic_version"

_BASELINE_SQL = """
SELECT
  (SELECT count(*) FROM teams) AS teams,
  (SELECT count(*) FROM users) AS users,
  (SELECT count(*) FROM agents) AS agents,
  (SELECT count(*) FROM tasks) AS tasks,
  (SELECT count(*) FROM provider_models_cache) AS provider_models
""".strip()

_EPOCH_SQL = """
SELECT environment, namespace, epoch
FROM staging_mutation_epochs
WHERE environment = 'staging' AND namespace = 'loom-staging'
""".strip()

_CAPACITY_SQL = """
SELECT environment, namespace, object_count, bytes_used, disk_free_percent,
       inode_free_percent, policy_sha256, evidence_sha256, source,
       extract(epoch FROM observed_at)::bigint AS observed_at_epoch
FROM staging_lifecycle_capacity
WHERE environment = 'staging' AND namespace = 'loom-staging'
""".strip()

_INVENTORY_SQL = """
SELECT obj.bucket, obj.object_key, obj.version_id, obj.content_sha256,
       obj.size_bytes, auth.data_class,
       auth.metadata ->> 'authoritative_source' AS authoritative_source
FROM data_lifecycle_objects AS obj
JOIN data_lifecycle_authorities AS auth ON auth.id = obj.authority_id
WHERE obj.environment = 'staging'
  AND obj.namespace = 'loom-staging'
  AND obj.state = 'active'
  AND auth.environment = obj.environment
  AND auth.namespace = obj.namespace
  AND auth.state = 'active'
  AND auth.pinned
  AND auth.data_class IN ('benchmark', 'catalog', 'system')
ORDER BY obj.bucket, obj.object_key, obj.version_id
""".strip()

DatabaseQuery = Callable[[str], tuple[Mapping[str, object], ...]]


@dataclass(frozen=True, slots=True)
class ReadonlyDatabaseEvidence:
    """Secret-free evidence from one repeatable-read, read-only transaction."""

    schema_revision: str
    mutation_epoch: int
    epoch_authority: str
    baseline_counts: Mapping[str, int]
    capacity: Mapping[str, object] | None
    evidence_sha256: str
    immutable_objects: tuple[ImmutableObjectReference, ...] = ()

    def __post_init__(self) -> None:
        counts = dict(self.baseline_counts)
        capacity = None if self.capacity is None else dict(self.capacity)
        identities = [item.identity for item in self.immutable_objects]
        if (
            _REVISION_RE.fullmatch(self.schema_revision) is None
            or self.mutation_epoch < 0
            or self.epoch_authority not in {"legacy-pre-0066", "staging-mutation-epoch-v1"}
            or set(counts) != {"agents", "provider_models", "tasks", "teams", "users"}
            or any(type(value) is not int or value < 0 for value in counts.values())
            or len(self.evidence_sha256) != 64
            or identities != sorted(identities)
            or len(identities) != len(set(identities))
        ):
            raise ValueError("readonly database evidence is invalid")
        if int(self.schema_revision) < _EPOCH_FIRST_REVISION:
            if self.mutation_epoch != 0 or self.epoch_authority != "legacy-pre-0066":
                raise ValueError("legacy database epoch authority is invalid")
            if self.immutable_objects:
                raise ValueError("legacy database immutable object authority is invalid")
        elif self.epoch_authority != "staging-mutation-epoch-v1":
            raise ValueError("database epoch authority is invalid")
        if (int(self.schema_revision) >= _CAPACITY_FIRST_REVISION) != (capacity is not None):
            raise ValueError("database capacity authority is incomplete")
        if capacity is not None:
            expected_capacity = {
                "environment",
                "namespace",
                "object_count",
                "bytes_used",
                "disk_free_percent",
                "inode_free_percent",
                "policy_sha256",
                "evidence_sha256",
                "source",
                "observed_at_epoch",
            }
            if (
                set(capacity) != expected_capacity
                or capacity["environment"] != "staging"
                or capacity["namespace"] != "loom-staging"
                or capacity["source"] != "exact-object-inventory-v1"
            ):
                raise ValueError("database capacity authority is incomplete")
            capacity_ints = {
                name: _integer(capacity[name], label=f"capacity-{name}")
                for name in (
                    "object_count",
                    "bytes_used",
                    "disk_free_percent",
                    "inode_free_percent",
                    "observed_at_epoch",
                )
            }
            capacity_value = StagingCapacity(
                object_count=capacity_ints["object_count"],
                bytes_used=capacity_ints["bytes_used"],
                disk_free_percent=capacity_ints["disk_free_percent"],
                inode_free_percent=capacity_ints["inode_free_percent"],
            )
            if (
                capacity["policy_sha256"] != staging_capacity_policy_digest()
                or capacity["evidence_sha256"] != capacity_value.evidence_digest
            ):
                raise ValueError("database capacity authority is incomplete")
        object.__setattr__(self, "baseline_counts", MappingProxyType(counts))
        object.__setattr__(
            self,
            "capacity",
            None if capacity is None else MappingProxyType(capacity),
        )


def _one(query: DatabaseQuery, sql: str, *, label: str) -> Mapping[str, object]:
    rows = query(sql)
    if len(rows) != 1:
        raise ValueError(f"readonly database {label} evidence is incomplete")
    return rows[0]


def _integer(value: object, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"readonly database {label} evidence is invalid")
    return value


def probe_readonly_database(query: DatabaseQuery) -> ReadonlyDatabaseEvidence:
    """Probe current staging without assuming migrations 0066/0067 already exist.

    ``query`` must execute every statement in the same PostgreSQL
    REPEATABLE READ, READ ONLY transaction. The first statement proves that
    database-enforced boundary before any application table is inspected.
    """

    authority = _one(query, _AUTHORITY_SQL, label="role")
    expected_authority = {
        "role_name": _ROLE,
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
    }
    if dict(authority) != expected_authority:
        raise ValueError("readonly database role authority drifted")

    revision_row = _one(query, _REVISION_SQL, label="revision")
    if set(revision_row) != {"schema_revision"}:
        raise ValueError("readonly database revision evidence is invalid")
    revision = revision_row["schema_revision"]
    if not isinstance(revision, str) or _REVISION_RE.fullmatch(revision) is None:
        raise ValueError("readonly database revision evidence is invalid")
    revision_number = int(revision)

    baseline = _one(query, _BASELINE_SQL, label="baseline")
    if set(baseline) != {"agents", "provider_models", "tasks", "teams", "users"}:
        raise ValueError("readonly database baseline evidence is invalid")
    counts = {name: _integer(value, label=name) for name, value in baseline.items()}

    if revision_number < _EPOCH_FIRST_REVISION:
        epoch = 0
        epoch_authority = "legacy-pre-0066"
    else:
        epoch_row = _one(query, _EPOCH_SQL, label="epoch")
        if (
            set(epoch_row) != {"environment", "namespace", "epoch"}
            or epoch_row["environment"] != "staging"
            or epoch_row["namespace"] != "loom-staging"
        ):
            raise ValueError("readonly database epoch evidence is invalid")
        epoch = _integer(epoch_row["epoch"], label="epoch")
        epoch_authority = "staging-mutation-epoch-v1"

    capacity: dict[str, object] | None = None
    if revision_number >= _CAPACITY_FIRST_REVISION:
        capacity_row = _one(query, _CAPACITY_SQL, label="capacity")
        expected = {
            "environment",
            "namespace",
            "object_count",
            "bytes_used",
            "disk_free_percent",
            "inode_free_percent",
            "policy_sha256",
            "evidence_sha256",
            "source",
            "observed_at_epoch",
        }
        if (
            set(capacity_row) != expected
            or capacity_row["environment"] != "staging"
            or capacity_row["namespace"] != "loom-staging"
            or capacity_row["source"] != "exact-object-inventory-v1"
            or not isinstance(capacity_row["policy_sha256"], str)
            or len(capacity_row["policy_sha256"]) != 64
            or not isinstance(capacity_row["evidence_sha256"], str)
            or len(capacity_row["evidence_sha256"]) != 64
        ):
            raise ValueError("readonly database capacity evidence is invalid")
        capacity = dict(capacity_row)
        capacity_values = {
            name: _integer(capacity[name], label=f"capacity-{name}")
            for name in (
                "object_count",
                "bytes_used",
                "disk_free_percent",
                "inode_free_percent",
                "observed_at_epoch",
            )
        }
        try:
            capacity_value = StagingCapacity(
                object_count=capacity_values["object_count"],
                bytes_used=capacity_values["bytes_used"],
                disk_free_percent=capacity_values["disk_free_percent"],
                inode_free_percent=capacity_values["inode_free_percent"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("readonly database capacity evidence is invalid") from exc
        if (
            capacity["policy_sha256"] != staging_capacity_policy_digest()
            or capacity["evidence_sha256"] != capacity_value.evidence_digest
        ):
            raise ValueError("readonly database capacity evidence is invalid")

    immutable_objects: tuple[ImmutableObjectReference, ...] = ()
    if revision_number >= _EPOCH_FIRST_REVISION:
        inventory_rows = query(_INVENTORY_SQL)
        if len(inventory_rows) > 1024:
            raise ValueError("readonly database immutable inventory is too large")
        try:
            immutable_objects = tuple(
                ImmutableObjectReference.from_dict(dict(row)) for row in inventory_rows
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("readonly database immutable inventory is invalid") from exc
        identities = [item.identity for item in immutable_objects]
        if identities != sorted(identities) or len(identities) != len(set(identities)):
            raise ValueError("readonly database immutable inventory is invalid")

    canonical = {
        "authority": expected_authority,
        "baseline_counts": counts,
        "capacity": capacity,
        "epoch_authority": epoch_authority,
        "immutable_objects": [item.to_dict() for item in immutable_objects],
        "mutation_epoch": epoch,
        "schema_revision": revision,
        "version": "v1",
    }
    evidence_sha256 = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ReadonlyDatabaseEvidence(
        schema_revision=revision,
        mutation_epoch=epoch,
        epoch_authority=epoch_authority,
        baseline_counts=counts,
        capacity=capacity,
        evidence_sha256=evidence_sha256,
        immutable_objects=immutable_objects,
    )


__all__ = [
    "DatabaseQuery",
    "ReadonlyDatabaseEvidence",
    "probe_readonly_database",
]
