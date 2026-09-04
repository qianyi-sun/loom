"""Typed, non-secret database authority for schema-3 checkpoints."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    FleetManifestV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.fleet_state import (
    validate_fleet_manifest_digests,
    validate_profile_narrowing,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REVISION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_GUARD_REVISION_RE = re.compile(r"^guard_[0-9]{4}$")
_MAX_OBSERVATION_BYTES = 4 * 1024 * 1024
_QUERY_TIMEOUT_SECONDS = 30.0

_DATABASE_AUTHORITY_SQL = r"""
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT CASE
  WHEN to_regclass('loom_capacity_guard.capacity_guard_alembic_version') IS NULL
  THEN 'false' ELSE 'true'
END AS guard_table_present \gset
\if :guard_table_present
SELECT COALESCE(jsonb_agg(version_num ORDER BY version_num), '[]'::jsonb)::text
  AS guard_revisions
FROM loom_capacity_guard.capacity_guard_alembic_version \gset
\else
\set guard_revisions '[]'
\endif
WITH latest AS (
  SELECT *
  FROM public.capacity_configuration_epochs
  WHERE configuration_epoch = (
    SELECT max(configuration_epoch) FROM public.capacity_configuration_epochs
  )
), configuration AS (
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'configuration_epoch', configuration_epoch,
    'fleet_generation', fleet_generation,
    'fleet_digest', fleet_digest,
    'subject_generation_manifest', subject_generation_manifest,
    'canonical_digest', canonical_digest
  )), '[]'::jsonb) AS rows
  FROM latest
), generations AS (
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'scope', scope,
    'subject_id', subject_id,
    'subject_incarnation', subject_incarnation,
    'scope_generation', scope_generation,
    'digest', digest,
    'payload', payload,
    'state', state
  ) ORDER BY scope, subject_id NULLS FIRST), '[]'::jsonb) AS rows
  FROM public.capacity_config_generations
  WHERE state = 'active'
), authority AS (
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'singleton_id', singleton_id,
    'schema_version', schema_version,
    'recovery_state', recovery_state,
    'authority_incarnation', authority_incarnation,
    'writer_epoch', writer_epoch,
    'execution_state', execution_state,
    'execution_epoch', execution_epoch,
    'execution_manifest_sha256', execution_manifest_sha256,
    'executable_new_capacity_ceiling', executable_new_capacity_ceiling,
    'increase_freeze', increase_freeze
  )), '[]'::jsonb) AS rows
  FROM public.capacity_authority_state
)
SELECT jsonb_build_object(
  'public_revisions', (SELECT COALESCE(jsonb_agg(version_num ORDER BY version_num), '[]'::jsonb) FROM public.alembic_version),
  'guard_table_present', :'guard_table_present'::boolean,
  'guard_revisions', :'guard_revisions'::jsonb,
  'configuration', configuration.rows,
  'generations', generations.rows,
  'authority', authority.rows
)
FROM configuration, generations, authority;
COMMIT;
"""


class DatabaseAuthorityError(ValueError):
    """Secret-safe rejection of database checkpoint authority."""


class DatabaseAuthorityRunner(Protocol):
    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
        timeout_seconds: float,
    ) -> bytes: ...


@dataclass(frozen=True, slots=True)
class DatabaseAuthorityEvidence:
    public_schema_revision: str
    capacity_guard_schema_revision: str | None
    configuration_epoch: int
    configuration_digest: str
    authority_incarnation: UUID
    writer_epoch: int
    execution_state: str
    execution_epoch: int
    execution_manifest_sha256: None
    executable_new_capacity_ceiling: int
    increase_freeze: bool

    def __post_init__(self) -> None:
        if (
            _REVISION_RE.fullmatch(self.public_schema_revision) is None
            or (
                self.capacity_guard_schema_revision is not None
                and _GUARD_REVISION_RE.fullmatch(self.capacity_guard_schema_revision) is None
            )
            or type(self.configuration_epoch) is not int
            or self.configuration_epoch < 1
            or _SHA256_RE.fullmatch(self.configuration_digest) is None
            or type(self.writer_epoch) is not int
            or self.writer_epoch < 0
            or self.execution_state != "shadow"
            or type(self.execution_epoch) is not int
            or self.execution_epoch != 0
            or self.execution_manifest_sha256 is not None
            or type(self.executable_new_capacity_ceiling) is not int
            or self.executable_new_capacity_ceiling != 0
            or self.increase_freeze is not True
        ):
            raise ValueError("database authority evidence is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "authority_incarnation": str(self.authority_incarnation),
            "capacity_guard_schema_revision": self.capacity_guard_schema_revision,
            "configuration_digest": self.configuration_digest,
            "configuration_epoch": self.configuration_epoch,
            "executable_new_capacity_ceiling": self.executable_new_capacity_ceiling,
            "execution_epoch": self.execution_epoch,
            "execution_manifest_sha256": self.execution_manifest_sha256,
            "execution_state": self.execution_state,
            "increase_freeze": self.increase_freeze,
            "public_schema_revision": self.public_schema_revision,
            "schema_version": 1,
            "writer_epoch": self.writer_epoch,
        }

    @property
    def payload(self) -> bytes:
        return (json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")) + "\n").encode()

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> DatabaseAuthorityEvidence:
        expected = {
            "authority_incarnation",
            "capacity_guard_schema_revision",
            "configuration_digest",
            "configuration_epoch",
            "executable_new_capacity_ceiling",
            "execution_epoch",
            "execution_manifest_sha256",
            "execution_state",
            "increase_freeze",
            "public_schema_revision",
            "schema_version",
            "writer_epoch",
        }
        if set(value) != expected or value.get("schema_version") != 1:
            raise DatabaseAuthorityError("database authority evidence schema is invalid")
        try:
            authority_incarnation = UUID(str(value["authority_incarnation"]))
        except (TypeError, ValueError) as exc:
            raise DatabaseAuthorityError("database authority evidence identity is invalid") from exc
        public_revision = value["public_schema_revision"]
        configuration_digest = value["configuration_digest"]
        if not isinstance(public_revision, str) or not isinstance(configuration_digest, str):
            raise DatabaseAuthorityError("database authority evidence fields are invalid")
        guard_revision = value["capacity_guard_schema_revision"]
        if guard_revision is not None and not isinstance(guard_revision, str):
            raise DatabaseAuthorityError("database authority evidence fields are invalid")
        try:
            return cls(
                public_schema_revision=public_revision,
                capacity_guard_schema_revision=guard_revision,
                configuration_epoch=value["configuration_epoch"],  # type: ignore[arg-type]
                configuration_digest=configuration_digest,
                authority_incarnation=authority_incarnation,
                writer_epoch=value["writer_epoch"],  # type: ignore[arg-type]
                execution_state=value["execution_state"],  # type: ignore[arg-type]
                execution_epoch=value["execution_epoch"],  # type: ignore[arg-type]
                execution_manifest_sha256=value["execution_manifest_sha256"],  # type: ignore[arg-type]
                executable_new_capacity_ceiling=value["executable_new_capacity_ceiling"],  # type: ignore[arg-type]
                increase_freeze=value["increase_freeze"],  # type: ignore[arg-type]
            )
        except (TypeError, ValueError) as exc:
            raise DatabaseAuthorityError("database authority evidence fields are invalid") from exc


def _strict_json(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > _MAX_OBSERVATION_BYTES:
        raise DatabaseAuthorityError("database authority observation size is invalid")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise DatabaseAuthorityError("database authority observation is not strict JSON") from exc
    if not isinstance(value, dict):
        raise DatabaseAuthorityError("database authority observation is not an object")
    return value


def _one_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise DatabaseAuthorityError(f"database authority {label} is not exact")
    return MappingProxyType(value[0])


def _parse_generations(
    rows: object,
    *,
    snapshot: ConfigurationSnapshotV1,
) -> FleetManifestV1:
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise DatabaseAuthorityError("database authority generation rows are invalid")
    expected_refs = (snapshot.fleet, *snapshot.subjects)
    if len(rows) != len(expected_refs):
        raise DatabaseAuthorityError("database authority generation set is incomplete")
    by_binding: dict[tuple[str, str | None], Mapping[str, object]] = {}
    for row in rows:
        assert isinstance(row, dict)
        expected_fields = {
            "digest",
            "payload",
            "scope",
            "scope_generation",
            "state",
            "subject_id",
            "subject_incarnation",
        }
        if set(row) != expected_fields or row.get("state") != "active":
            raise DatabaseAuthorityError("database authority generation row is invalid")
        subject_id = row.get("subject_id")
        binding = (str(row.get("scope")), None if subject_id is None else str(subject_id))
        if binding in by_binding:
            raise DatabaseAuthorityError("database authority generation identity is duplicate")
        by_binding[binding] = row
    fleet: FleetManifestV1 | None = None
    subjects: list[SubjectConfigurationV1] = []
    for reference in expected_refs:
        binding = (
            reference.scope,
            None if reference.subject_id is None else str(reference.subject_id),
        )
        row = by_binding.get(binding)
        if (
            row is None
            or row.get("scope_generation") != reference.generation
            or row.get("digest") != reference.digest
            or row.get("subject_incarnation")
            != (
                None
                if reference.subject_incarnation is None
                else str(reference.subject_incarnation)
            )
            or not isinstance(row.get("payload"), dict)
        ):
            raise DatabaseAuthorityError("database authority generation binding is inconsistent")
        try:
            if reference.scope == "fleet":
                fleet = FleetManifestV1.model_validate_json(json.dumps(row["payload"]))
                validate_fleet_manifest_digests(fleet)
                observed_digest = canonical_digest(fleet)
                if fleet.fleet_generation != reference.generation:
                    raise DatabaseAuthorityError(
                        "database authority fleet generation binding is inconsistent"
                    )
            else:
                subject = SubjectConfigurationV1.model_validate_json(json.dumps(row["payload"]))
                if (
                    subject.configuration_generation != reference.generation
                    or subject.subject_id != reference.subject_id
                    or subject.subject_incarnation != reference.subject_incarnation
                ):
                    raise DatabaseAuthorityError(
                        "database authority subject generation binding is inconsistent"
                    )
                subjects.append(subject)
                observed_digest = canonical_digest(subject)
        except ValueError as exc:
            raise DatabaseAuthorityError(
                "database authority generation payload is invalid"
            ) from exc
        if observed_digest != reference.digest:
            raise DatabaseAuthorityError("database authority generation payload is noncanonical")
    if fleet is None:
        raise DatabaseAuthorityError("database authority fleet generation is missing")
    try:
        for subject in subjects:
            for profile in subject.profiles:
                validate_profile_narrowing(fleet, profile)
    except ValueError as exc:
        raise DatabaseAuthorityError("database authority configuration is contradictory") from exc
    return fleet


def parse_database_authority_observation(payload: bytes) -> DatabaseAuthorityEvidence:
    """Validate one same-transaction database observation into typed evidence."""
    value = _strict_json(payload)
    expected_root = {
        "authority",
        "configuration",
        "generations",
        "guard_revisions",
        "guard_table_present",
        "public_revisions",
    }
    if set(value) != expected_root:
        raise DatabaseAuthorityError("database authority observation fields are invalid")
    public = value["public_revisions"]
    if (
        not isinstance(public, list)
        or len(public) != 1
        or not isinstance(public[0], str)
        or _REVISION_RE.fullmatch(public[0]) is None
    ):
        raise DatabaseAuthorityError("public database revision is not exact")
    guard_present = value["guard_table_present"]
    guard = value["guard_revisions"]
    if type(guard_present) is not bool or not isinstance(guard, list):
        raise DatabaseAuthorityError("capacity guard revision authority is invalid")
    if guard_present:
        if (
            len(guard) != 1
            or not isinstance(guard[0], str)
            or _GUARD_REVISION_RE.fullmatch(guard[0]) is None
        ):
            raise DatabaseAuthorityError("capacity guard revision is not exact")
        guard_revision: str | None = guard[0]
    else:
        if guard:
            raise DatabaseAuthorityError("absent capacity guard revision is contradictory")
        guard_revision = None

    configuration = _one_mapping(value["configuration"], label="configuration row")
    config_fields = {
        "canonical_digest",
        "configuration_epoch",
        "fleet_digest",
        "fleet_generation",
        "subject_generation_manifest",
    }
    if (
        set(configuration) != config_fields
        or type(configuration["configuration_epoch"]) is not int
        or type(configuration["fleet_generation"]) is not int
        or not isinstance(configuration["fleet_digest"], str)
        or not isinstance(configuration["canonical_digest"], str)
        or not isinstance(configuration["subject_generation_manifest"], list)
    ):
        raise DatabaseAuthorityError("database authority configuration row is invalid")
    try:
        subject_refs = tuple(
            ConfigurationGenerationRefV1.model_validate_json(json.dumps(item))
            for item in configuration["subject_generation_manifest"]
        )
        snapshot = ConfigurationSnapshotV1(
            configuration_epoch=configuration["configuration_epoch"],
            fleet=ConfigurationGenerationRefV1(
                scope="fleet",
                generation=configuration["fleet_generation"],
                digest=configuration["fleet_digest"],
            ),
            subjects=subject_refs,
        )
    except ValueError as exc:
        raise DatabaseAuthorityError(
            "database authority configuration row is inconsistent"
        ) from exc
    if canonical_digest(snapshot) != configuration["canonical_digest"]:
        raise DatabaseAuthorityError("database authority configuration digest is noncanonical")
    fleet = _parse_generations(value["generations"], snapshot=snapshot)

    authority = _one_mapping(value["authority"], label="authority row")
    authority_fields = {
        "authority_incarnation",
        "executable_new_capacity_ceiling",
        "execution_epoch",
        "execution_manifest_sha256",
        "execution_state",
        "increase_freeze",
        "recovery_state",
        "schema_version",
        "singleton_id",
        "writer_epoch",
    }
    if (
        set(authority) != authority_fields
        or type(authority.get("singleton_id")) is not int
        or authority.get("singleton_id") != 1
        or type(authority.get("schema_version")) is not int
        or authority.get("schema_version") != 1
        or authority.get("recovery_state") != "shadow"
    ):
        raise DatabaseAuthorityError("database authority row is invalid")
    evidence_record = {
        "authority_incarnation": authority["authority_incarnation"],
        "capacity_guard_schema_revision": guard_revision,
        "configuration_digest": configuration["canonical_digest"],
        "configuration_epoch": configuration["configuration_epoch"],
        "executable_new_capacity_ceiling": authority["executable_new_capacity_ceiling"],
        "execution_epoch": authority["execution_epoch"],
        "execution_manifest_sha256": authority["execution_manifest_sha256"],
        "execution_state": authority["execution_state"],
        "increase_freeze": authority["increase_freeze"],
        "public_schema_revision": public[0],
        "schema_version": 1,
        "writer_epoch": authority["writer_epoch"],
    }
    evidence = DatabaseAuthorityEvidence.from_dict(evidence_record)
    if evidence.authority_incarnation != fleet.authority_incarnation:
        raise DatabaseAuthorityError("database authority incarnation binding is inconsistent")
    return evidence


def capture_database_authority(
    runner: DatabaseAuthorityRunner,
    *,
    env: Mapping[str, str],
    namespace: str,
) -> DatabaseAuthorityEvidence:
    """Capture one strict authority observation without putting credentials in argv."""
    if namespace != "loom-staging":
        raise DatabaseAuthorityError("database authority namespace is invalid")
    try:
        payload = runner.capture_stdout_with_input(
            (
                "kubectl",
                "--namespace",
                namespace,
                "exec",
                "--stdin=true",
                "service/loom-postgres-rw",
                "--",
                "psql",
                "--no-psqlrc",
                "--quiet",
                "--tuples-only",
                "--no-align",
                "--set=ON_ERROR_STOP=1",
                "--username=postgres",
                "--dbname=loom",
                "--file=-",
            ),
            env=env,
            input_payload=_DATABASE_AUTHORITY_SQL.encode("utf-8"),
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        return parse_database_authority_observation(payload.strip())
    except DatabaseAuthorityError:
        raise
    except Exception:
        raise DatabaseAuthorityError("database authority capture failed") from None


__all__ = [
    "DatabaseAuthorityError",
    "DatabaseAuthorityEvidence",
    "capture_database_authority",
    "parse_database_authority_observation",
]
