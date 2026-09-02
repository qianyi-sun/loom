"""Cross-version SELECT-only PostgreSQL authority for Tier 2 preflight."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast
from uuid import UUID

from loom.data_lifecycle import (
    STAGING_ADMISSION_OBJECT_LIMIT,
    StagingCapacity,
    staging_capacity_policy_digest,
)
from loom_cli.rollout.operator.rollout_checkpoint import ImmutableObjectReference

_REVISION_RE = re.compile(r"^[0-9]{4}$")
_ROLE = "loom_rollout_readonly"
_LEGACY_LAST_REVISION = 68
_EPOCH_FIRST_REVISION = 69
_CAPACITY_FIRST_REVISION = 70
_INVENTORY_PAGE_SIZE = 1024

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
       AND has_table_privilege(current_user, 'public.team_memberships', 'SELECT')
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
       floor(extract(epoch FROM observed_at))::bigint AS observed_at_epoch
FROM staging_lifecycle_capacity
WHERE environment = 'staging' AND namespace = 'loom-staging'
""".strip()

_INVENTORY_SQL = """
SELECT obj.bucket, obj.object_key, obj.version_id, obj.content_sha256,
       obj.size_bytes, auth.data_class,
       auth.metadata ->> 'authoritative_source' AS authoritative_source,
       auth.id::text AS authority_id, auth.owner_kind, auth.owner_id
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

_SMOKE_USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


@dataclass(frozen=True, slots=True)
class ReadonlySmokeAuthorityEvidence:
    """Secret-free readiness for one configured admin-on-behalf identity."""

    mutation_epoch: int
    team_exists: bool
    team_active: bool
    team_submissions_enabled: bool
    user_exists: bool
    user_active: bool
    membership_present: bool
    evidence_sha256: str

    def __post_init__(self) -> None:
        flags = (
            self.team_exists,
            self.team_active,
            self.team_submissions_enabled,
            self.user_exists,
            self.user_active,
            self.membership_present,
        )
        if (
            self.mutation_epoch < 0
            or any(type(value) is not bool for value in flags)
            or (self.team_active and not self.team_exists)
            or (self.team_submissions_enabled and not self.team_exists)
            or (self.user_active and not self.user_exists)
            or (self.membership_present and not (self.team_exists and self.user_exists))
            or re.fullmatch(r"[0-9a-f]{64}", self.evidence_sha256) is None
        ):
            raise ValueError("readonly smoke authority evidence is invalid")

    @property
    def ready(self) -> bool:
        return all(
            (
                self.team_exists,
                self.team_active,
                self.team_submissions_enabled,
                self.user_exists,
                self.user_active,
                self.membership_present,
            )
        )


@dataclass(frozen=True, slots=True)
class ReadonlyMutationEpochEvidence:
    """Minimal readonly identity needed before the concurrent preflight DAG."""

    schema_revision: str
    mutation_epoch: int
    epoch_authority: str
    evidence_sha256: str

    def __post_init__(self) -> None:
        if (
            _REVISION_RE.fullmatch(self.schema_revision) is None
            or self.mutation_epoch < 0
            or self.epoch_authority not in {"legacy-pre-0069", "staging-mutation-epoch-v1"}
            or len(self.evidence_sha256) != 64
        ):
            raise ValueError("readonly mutation epoch evidence is invalid")
        if int(self.schema_revision) < _EPOCH_FIRST_REVISION:
            if self.mutation_epoch != 0 or self.epoch_authority != "legacy-pre-0069":
                raise ValueError("legacy database epoch authority is invalid")
        elif self.epoch_authority != "staging-mutation-epoch-v1":
            raise ValueError("database epoch authority is invalid")


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
            or self.epoch_authority not in {"legacy-pre-0069", "staging-mutation-epoch-v1"}
            or set(counts) != {"agents", "provider_models", "tasks", "teams", "users"}
            or any(type(value) is not int or value < 0 for value in counts.values())
            or len(self.evidence_sha256) != 64
            or identities != sorted(identities)
            or len(identities) != len(set(identities))
        ):
            raise ValueError("readonly database evidence is invalid")
        if int(self.schema_revision) < _EPOCH_FIRST_REVISION:
            if self.mutation_epoch != 0 or self.epoch_authority != "legacy-pre-0069":
                raise ValueError("legacy database epoch authority is invalid")
            if self.immutable_objects:
                raise ValueError("legacy database immutable object authority is invalid")
        elif self.epoch_authority != "staging-mutation-epoch-v1":
            raise ValueError("database epoch authority is invalid")
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


def _immutable_inventory_rows(query: DatabaseQuery) -> tuple[Mapping[str, object], ...]:
    """Read a policy-bounded inventory without relaxing per-query row limits."""

    rows: list[Mapping[str, object]] = []
    offset = 0
    while True:
        page = query(f"{_INVENTORY_SQL}\nLIMIT {_INVENTORY_PAGE_SIZE} OFFSET {offset}")
        if len(page) > _INVENTORY_PAGE_SIZE:
            raise ValueError("readonly database immutable inventory page is invalid")
        rows.extend(page)
        if len(rows) >= STAGING_ADMISSION_OBJECT_LIMIT:
            raise ValueError("readonly database immutable inventory exceeds policy")
        if len(page) < _INVENTORY_PAGE_SIZE:
            return tuple(rows)
        offset += _INVENTORY_PAGE_SIZE


def _immutable_reference(row: Mapping[str, object]) -> ImmutableObjectReference:
    """Normalize an exact legacy pinned identity without inventing mutable state.

    MinIO returns no version ID when bucket versioning is disabled.  The
    lifecycle registry nevertheless records a verified content digest, so that
    digest is the exact immutable version authority.  Legacy pinned rows also
    predate the optional ``authoritative_source`` metadata; bind those rows to
    the immutable lifecycle authority identity instead of treating an absent
    display field as an unreadable checkpoint.  Restore verification still has
    to prove the exact object bytes before a lease can be promoted.
    """
    value = dict(row)
    expected = {
        "authoritative_source",
        "bucket",
        "content_sha256",
        "data_class",
        "object_key",
        "size_bytes",
        "version_id",
    }
    legacy_binding = {"authority_id", "owner_kind", "owner_id"}
    if frozenset(value) not in {frozenset(expected), frozenset(expected | legacy_binding)}:
        raise ValueError("readonly database immutable inventory schema is invalid")
    content_sha256 = value.get("content_sha256")
    if not isinstance(content_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", content_sha256) is None:
        raise ValueError("readonly database immutable inventory digest is invalid")
    if value.get("version_id") is None:
        value["version_id"] = f"content-sha256:{content_sha256}"
    if value.get("authoritative_source") is None:
        binding = {name: value.get(name) for name in sorted(legacy_binding)}
        if any(not isinstance(item, str) or not item for item in binding.values()):
            raise ValueError("readonly database immutable source authority is invalid")
        binding_sha256 = hashlib.sha256(
            json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        value["authoritative_source"] = f"lifecycle-authority:sha256:{binding_sha256}"
    for name in legacy_binding:
        value.pop(name, None)
    return ImmutableObjectReference.from_dict(value)


def _expected_role_authority() -> dict[str, object]:
    return {
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


def probe_readonly_mutation_epoch(query: DatabaseQuery) -> ReadonlyMutationEpochEvidence:
    """Read the protected epoch without requiring later baseline/capacity rows.

    This is the identity prerequisite for the concurrent DAG.  The complete
    database baseline deliberately calls this same implementation before it
    evaluates capacity and immutable-object authority, so a missing capacity
    row is reported as its own earliest-stage blocker instead of masking every
    independent preflight check.
    """

    authority = _one(query, _AUTHORITY_SQL, label="role")
    expected_authority = _expected_role_authority()
    if dict(authority) != expected_authority:
        raise ValueError("readonly database role authority drifted")

    revision_row = _one(query, _REVISION_SQL, label="revision")
    if set(revision_row) != {"schema_revision"}:
        raise ValueError("readonly database revision evidence is invalid")
    revision = revision_row["schema_revision"]
    if not isinstance(revision, str) or _REVISION_RE.fullmatch(revision) is None:
        raise ValueError("readonly database revision evidence is invalid")
    revision_number = int(revision)

    if revision_number < _EPOCH_FIRST_REVISION:
        epoch = 0
        epoch_authority = "legacy-pre-0069"
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

    canonical = {
        "authority": expected_authority,
        "epoch_authority": epoch_authority,
        "mutation_epoch": epoch,
        "schema_revision": revision,
        "version": "v1",
    }
    return ReadonlyMutationEpochEvidence(
        schema_revision=revision,
        mutation_epoch=epoch,
        epoch_authority=epoch_authority,
        evidence_sha256=hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


def probe_readonly_smoke_authority(
    query: DatabaseQuery,
    *,
    represented_username: str,
    team_id: str,
) -> ReadonlySmokeAuthorityEvidence:
    """Prove one on-behalf username/team pair through the SELECT-only role."""

    normalized_username = represented_username.casefold()
    try:
        parsed_team_id = UUID(team_id)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("readonly smoke authority binding is invalid") from exc
    canonical_team_id = str(parsed_team_id)
    if (
        _SMOKE_USERNAME_RE.fullmatch(represented_username) is None
        or parsed_team_id.version != 4
        or canonical_team_id != team_id
    ):
        raise ValueError("readonly smoke authority binding is invalid")

    epoch = probe_readonly_mutation_epoch(query).mutation_epoch
    sql = f"""
WITH target_team AS (
  SELECT id, disabled_at, submissions_paused_at
  FROM teams
  WHERE id = '{canonical_team_id}'::uuid
), target_user AS (
  SELECT id, status, disabled_at
  FROM users
  WHERE username_normalized = '{normalized_username}'
)
SELECT
  EXISTS(SELECT 1 FROM target_team) AS team_exists,
  COALESCE((SELECT disabled_at IS NULL FROM target_team), FALSE) AS team_active,
  COALESCE(
    (SELECT submissions_paused_at IS NULL FROM target_team),
    FALSE
  ) AS team_submissions_enabled,
  EXISTS(SELECT 1 FROM target_user) AS user_exists,
  COALESCE(
    (SELECT status = 'active' AND disabled_at IS NULL FROM target_user),
    FALSE
  ) AS user_active,
  EXISTS(
    SELECT 1
    FROM team_memberships AS membership
    JOIN target_team ON target_team.id = membership.team_id
    JOIN target_user ON target_user.id = membership.user_id
  ) AS membership_present
""".strip()
    row = dict(_one(query, sql, label="smoke authority"))
    expected = {
        "team_exists",
        "team_active",
        "team_submissions_enabled",
        "user_exists",
        "user_active",
        "membership_present",
    }
    if set(row) != expected or any(type(value) is not bool for value in row.values()):
        raise ValueError("readonly database smoke authority evidence is invalid")
    flags = cast(dict[str, bool], row)
    binding_sha256 = hashlib.sha256(
        f"{normalized_username}\0{canonical_team_id}".encode()
    ).hexdigest()
    canonical = {
        "binding_sha256": binding_sha256,
        "mutation_epoch": epoch,
        **row,
        "version": "v1",
    }
    return ReadonlySmokeAuthorityEvidence(
        mutation_epoch=epoch,
        team_exists=flags["team_exists"],
        team_active=flags["team_active"],
        team_submissions_enabled=flags["team_submissions_enabled"],
        user_exists=flags["user_exists"],
        user_active=flags["user_active"],
        membership_present=flags["membership_present"],
        evidence_sha256=hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


def _probe_readonly_database(
    query: DatabaseQuery,
    *,
    include_immutable_inventory: bool,
) -> ReadonlyDatabaseEvidence:
    """Probe current staging without assuming migrations 0069/0070 already exist.

    ``query`` must execute every statement in the same PostgreSQL
    REPEATABLE READ, READ ONLY transaction. The first statements reuse the
    mutation-epoch probe and prove the database-enforced boundary before any
    application table is inspected.
    """

    epoch_evidence = probe_readonly_mutation_epoch(query)
    revision = epoch_evidence.schema_revision
    revision_number = int(revision)
    epoch = epoch_evidence.mutation_epoch
    epoch_authority = epoch_evidence.epoch_authority
    expected_authority = _expected_role_authority()

    baseline = _one(query, _BASELINE_SQL, label="baseline")
    if set(baseline) != {"agents", "provider_models", "tasks", "teams", "users"}:
        raise ValueError("readonly database baseline evidence is invalid")
    counts = {name: _integer(value, label=name) for name, value in baseline.items()}

    capacity: dict[str, object] | None = None
    if revision_number >= _CAPACITY_FIRST_REVISION:
        capacity_rows = query(_CAPACITY_SQL)
        if len(capacity_rows) > 1:
            raise ValueError("readonly database capacity evidence is incomplete")
        capacity_row = capacity_rows[0] if capacity_rows else None
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
        if capacity_row is not None and (
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
        if capacity_row is not None:
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
    if include_immutable_inventory and revision_number >= _EPOCH_FIRST_REVISION:
        inventory_rows = _immutable_inventory_rows(query)
        try:
            immutable_objects = tuple(
                sorted(
                    (_immutable_reference(row) for row in inventory_rows),
                    key=lambda item: item.identity,
                )
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


def probe_readonly_database(query: DatabaseQuery) -> ReadonlyDatabaseEvidence:
    """Return complete database and immutable checkpoint evidence."""

    return _probe_readonly_database(query, include_immutable_inventory=True)


def probe_readonly_database_baseline(query: DatabaseQuery) -> ReadonlyDatabaseEvidence:
    """Return independent Tier 2 baseline evidence without checkpoint authority."""

    return _probe_readonly_database(query, include_immutable_inventory=False)


__all__ = [
    "DatabaseQuery",
    "ReadonlyDatabaseEvidence",
    "ReadonlyMutationEpochEvidence",
    "ReadonlySmokeAuthorityEvidence",
    "probe_readonly_database",
    "probe_readonly_database_baseline",
    "probe_readonly_mutation_epoch",
    "probe_readonly_smoke_authority",
]
