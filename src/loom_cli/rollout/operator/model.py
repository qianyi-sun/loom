"""Strict data contracts for the protected staging rollout operator."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Literal, cast, get_args

APPROVED_REMOTE_URL = "https://github.com/qianyi-sun/loom.git"
APPROVED_FETCH_REF = "refs/heads/dev"
PINNED_TARGET_REF = "origin/dev"

SchemaVersion = Literal[1]
CandidateSourceMode = Literal["merged-dev", "sealed-cumulative"]
RequestCommand = Literal["start"]
RequestStatus = Literal["pending", "preview"]
ActiveStatus = Literal["pending", "running"]
RequestEventType = Literal[
    "requested",
    "preview",
    "backup_started",
    "backup_failed",
    "backup_cleanup_done",
    "backup_cleanup_failed",
    "backup_cleanup_started",
    "envelope_published",
    "launch_pending",
    "launch_failed",
    "attempt_pending",
    "attempt_running",
    "attempt_done",
    "attempt_failed",
    "cancel_requested",
    "cancel_failed",
    "cancelled",
]
EventStatus = Literal["pending", "preview", "running", "done", "failed", "cancelled"]
BackupPublicReason = Literal[
    "backup_failed",
    "backup_precondition_failed",
    "backup_capacity_exhausted",
    "backup_config_invalid",
    "backup_postgres_failed",
    "backup_minio_failed",
    "backup_transport_failed",
    "backup_object_limit_exceeded",
    "backup_object_inventory_failed",
    "backup_secrets_failed",
    "backup_manifest_failed",
    "backup_cleanup_failed",
    "backup_retirement_failed",
]
BACKUP_PUBLIC_REASONS: frozenset[str] = frozenset(get_args(BackupPublicReason))

_SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")
_USERNAME_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,63}$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{12} len=[1-9][0-9]*$")
_UNIT_RE = re.compile(r"^[A-Za-z0-9_.@:-]{1,255}$")

_REQUEST_COMMANDS = frozenset({"start"})
_REQUEST_STATUSES = frozenset({"pending", "preview"})
_ACTIVE_STATUSES = frozenset({"pending", "running"})
_REQUEST_EVENTS = frozenset(
    {
        "requested",
        "preview",
        "backup_started",
        "backup_failed",
        "backup_cleanup_done",
        "backup_cleanup_failed",
        "backup_cleanup_started",
        "envelope_published",
        "launch_pending",
        "launch_failed",
        "attempt_pending",
        "attempt_running",
        "attempt_done",
        "attempt_failed",
        "cancel_requested",
        "cancel_failed",
        "cancelled",
    }
)
_EVENT_STATUSES = frozenset({"pending", "preview", "running", "done", "failed", "cancelled"})


def validate_safe_identifier(value: object, field_name: str) -> str:
    """Return a path-safe request/rollout identifier or fail closed."""
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match ^[a-z0-9][a-z0-9-]{{7,79}}$")
    return value


def _require_exact_keys(
    data: Mapping[str, object],
    expected: set[str],
    object_name: str,
) -> None:
    actual = set(data)
    unknown = sorted(actual - expected)
    missing = sorted(expected - actual)
    if unknown:
        raise ValueError(f"{object_name} has unknown keys: {unknown}")
    if missing:
        raise ValueError(f"{object_name} is missing keys: {missing}")


def _require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field_name} must be a JSON object")
    return cast(dict[str, object], value)


def _require_schema(value: object) -> SchemaVersion:
    if type(value) is not int or value != 1:
        raise ValueError("schema_version must be 1")
    return 1


def _require_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_username(value: object, field_name: str) -> str:
    username = _require_string(value, field_name)
    if _USERNAME_RE.fullmatch(username) is None:
        raise ValueError(f"{field_name} must be a safe OS username")
    return username


def _require_nonnegative_int(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _require_positive_int(value: object, field_name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _require_literal(
    value: object,
    allowed: frozenset[str],
    field_name: str,
    description: str,
) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"unknown {description} {value!r}")
    return value


def _require_sha(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be 40 lowercase hexadecimal characters")
    return value


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be 64 lowercase hexadecimal characters")
    return value


def _require_absolute_path(value: object, field_name: str) -> str:
    rendered = _require_string(value, field_name)
    path = Path(rendered)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field_name} must be an absolute protected path")
    return rendered


def _require_file_source(value: object, field_name: str) -> str:
    rendered = _require_string(value, field_name)
    if not rendered.startswith("file:"):
        raise ValueError(f"{field_name} must be an absolute file: source")
    _require_absolute_path(rendered.removeprefix("file:"), field_name)
    return rendered


def _require_fingerprint(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _FINGERPRINT_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match sha256:<12-hex> len=<positive-integer>")
    return value


def _require_optional_string(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field_name)


def _require_optional_positive_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    return _require_positive_int(value, field_name)


@dataclass(frozen=True, slots=True)
class CallerIdentity:
    username: str
    uid: int
    schema_version: SchemaVersion = 1

    def __post_init__(self) -> None:
        _require_username(self.username, "username")
        _require_nonnegative_int(self.uid, "uid")
        _require_schema(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return {
            "username": self.username,
            "uid": self.uid,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> CallerIdentity:
        expected = {"username", "uid", "schema_version"}
        _require_exact_keys(data, expected, "caller identity")
        return cls(
            username=_require_username(data["username"], "username"),
            uid=_require_nonnegative_int(data["uid"], "uid"),
            schema_version=_require_schema(data["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class CandidateBinding:
    remote_url: str
    target_ref: str
    resolved_sha: str
    image_tag: str
    fetched_at: str
    schema_version: SchemaVersion = 1
    source_mode: CandidateSourceMode = "merged-dev"
    resolved_tree: str | None = None
    approved_base_sha: str | None = None

    def __post_init__(self) -> None:
        if self.remote_url != APPROVED_REMOTE_URL:
            raise ValueError("remote_url is not approved")
        if self.target_ref != PINNED_TARGET_REF:
            raise ValueError(f"target_ref must be {PINNED_TARGET_REF}")
        sha = _require_sha(self.resolved_sha, "resolved_sha")
        if self.image_tag != f"staging-{sha[:7]}":
            raise ValueError("image_tag must be staging-<resolved_sha[:7]>")
        _require_string(self.fetched_at, "fetched_at")
        if self.source_mode not in {"merged-dev", "sealed-cumulative"}:
            raise ValueError("source_mode is invalid")
        if self.source_mode == "sealed-cumulative":
            if self.resolved_tree is None or self.approved_base_sha is None:
                raise ValueError("sealed candidate source binding is incomplete")
            _require_sha(self.resolved_tree, "resolved_tree")
            _require_sha(self.approved_base_sha, "approved_base_sha")
        else:
            # A merged-dev candidate carries the resolved git tree (a derivable
            # identity the Tier-1 artifact builders require) but never an
            # approved base sha (the sealed-cumulative approval anchor).
            if self.approved_base_sha is not None:
                raise ValueError("merged candidate must not carry an approved base sha")
            if self.resolved_tree is not None:
                _require_sha(self.resolved_tree, "resolved_tree")
        _require_schema(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        value: dict[str, object] = {
            "remote_url": self.remote_url,
            "target_ref": self.target_ref,
            "resolved_sha": self.resolved_sha,
            "image_tag": self.image_tag,
            "fetched_at": self.fetched_at,
            "schema_version": self.schema_version,
        }
        if self.source_mode == "sealed-cumulative":
            value.update(
                {
                    "source_mode": self.source_mode,
                    "resolved_tree": self.resolved_tree,
                    "approved_base_sha": self.approved_base_sha,
                }
            )
        elif self.resolved_tree is not None:
            value.update(
                {
                    "source_mode": self.source_mode,
                    "resolved_tree": self.resolved_tree,
                }
            )
        return value

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> CandidateBinding:
        expected = {
            "remote_url",
            "target_ref",
            "resolved_sha",
            "image_tag",
            "fetched_at",
            "schema_version",
        }
        sealed = data.get("source_mode") == "sealed-cumulative"
        merged_with_tree = not sealed and "resolved_tree" in data
        if sealed:
            expected.update({"source_mode", "resolved_tree", "approved_base_sha"})
        elif merged_with_tree:
            expected.update({"source_mode", "resolved_tree"})
        _require_exact_keys(data, expected, "candidate binding")
        return cls(
            remote_url=_require_string(data["remote_url"], "remote_url"),
            target_ref=_require_string(data["target_ref"], "target_ref"),
            resolved_sha=_require_sha(data["resolved_sha"], "resolved_sha"),
            image_tag=_require_string(data["image_tag"], "image_tag"),
            fetched_at=_require_string(data["fetched_at"], "fetched_at"),
            schema_version=_require_schema(data["schema_version"]),
            source_mode="sealed-cumulative" if sealed else "merged-dev",
            resolved_tree=(
                _require_sha(data["resolved_tree"], "resolved_tree")
                if sealed or merged_with_tree
                else None
            ),
            approved_base_sha=(
                _require_sha(data["approved_base_sha"], "approved_base_sha") if sealed else None
            ),
        )


@dataclass(frozen=True, slots=True)
class PreflightRequest:
    """Immutable Tier 0-2 admission identity created before backup I/O."""

    request_id: str
    rollout_id: str
    caller: CallerIdentity
    candidate: CandidateBinding
    candidate_tree: str
    requested_at: str
    runner_config_sha256: str
    preflight_assessment_sha256: str
    preflight_registry_sha256: str
    preflight_coverage_sha256: str
    mutation_epoch: int
    environment: str
    namespace: str
    command: RequestCommand = "start"
    status: RequestStatus = "pending"
    schema_version: SchemaVersion = 1

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        validate_safe_identifier(self.rollout_id, "rollout_id")
        if not isinstance(self.caller, CallerIdentity):
            raise ValueError("caller must be a CallerIdentity")
        if not isinstance(self.candidate, CandidateBinding):
            raise ValueError("candidate must be a CandidateBinding")
        _require_sha(self.candidate_tree, "candidate_tree")
        if (
            self.candidate.resolved_tree is not None
            and self.candidate.resolved_tree != self.candidate_tree
        ):
            raise ValueError("candidate_tree does not match sealed candidate")
        _require_string(self.requested_at, "requested_at")
        _require_sha256(self.runner_config_sha256, "runner_config_sha256")
        _require_sha256(self.preflight_assessment_sha256, "preflight_assessment_sha256")
        _require_sha256(self.preflight_registry_sha256, "preflight_registry_sha256")
        _require_sha256(self.preflight_coverage_sha256, "preflight_coverage_sha256")
        _require_nonnegative_int(self.mutation_epoch, "mutation_epoch")
        if self.environment != "staging":
            raise ValueError("preflight request environment must be staging")
        namespace = _require_string(self.namespace, "namespace")
        if namespace != namespace.strip():
            raise ValueError("namespace must not contain surrounding whitespace")
        _require_literal(self.command, _REQUEST_COMMANDS, "command", "request command")
        _require_literal(self.status, _REQUEST_STATUSES, "status", "request status")
        _require_schema(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "caller": self.caller.to_dict(),
            "candidate": self.candidate.to_dict(),
            "candidate_tree": self.candidate_tree,
            "requested_at": self.requested_at,
            "runner_config_sha256": self.runner_config_sha256,
            "preflight_assessment_sha256": self.preflight_assessment_sha256,
            "preflight_registry_sha256": self.preflight_registry_sha256,
            "preflight_coverage_sha256": self.preflight_coverage_sha256,
            "mutation_epoch": self.mutation_epoch,
            "environment": self.environment,
            "namespace": self.namespace,
            "command": self.command,
            "status": self.status,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> PreflightRequest:
        expected = {
            "request_id",
            "rollout_id",
            "caller",
            "candidate",
            "candidate_tree",
            "requested_at",
            "runner_config_sha256",
            "preflight_assessment_sha256",
            "preflight_registry_sha256",
            "preflight_coverage_sha256",
            "mutation_epoch",
            "environment",
            "namespace",
            "command",
            "status",
            "schema_version",
        }
        _require_exact_keys(data, expected, "preflight request")
        command = _require_literal(data["command"], _REQUEST_COMMANDS, "command", "request command")
        status = _require_literal(data["status"], _REQUEST_STATUSES, "status", "request status")
        return cls(
            request_id=validate_safe_identifier(data["request_id"], "request_id"),
            rollout_id=validate_safe_identifier(data["rollout_id"], "rollout_id"),
            caller=CallerIdentity.from_dict(_require_mapping(data["caller"], "caller")),
            candidate=CandidateBinding.from_dict(_require_mapping(data["candidate"], "candidate")),
            candidate_tree=_require_sha(data["candidate_tree"], "candidate_tree"),
            requested_at=_require_string(data["requested_at"], "requested_at"),
            runner_config_sha256=_require_sha256(
                data["runner_config_sha256"], "runner_config_sha256"
            ),
            preflight_assessment_sha256=_require_sha256(
                data["preflight_assessment_sha256"], "preflight_assessment_sha256"
            ),
            preflight_registry_sha256=_require_sha256(
                data["preflight_registry_sha256"], "preflight_registry_sha256"
            ),
            preflight_coverage_sha256=_require_sha256(
                data["preflight_coverage_sha256"], "preflight_coverage_sha256"
            ),
            mutation_epoch=_require_nonnegative_int(data["mutation_epoch"], "mutation_epoch"),
            environment=_require_string(data["environment"], "environment"),
            namespace=_require_string(data["namespace"], "namespace"),
            command=cast(RequestCommand, command),
            status=cast(RequestStatus, status),
            schema_version=_require_schema(data["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class RolloutRequest:
    """Immutable request created before any backup is attempted."""

    request_id: str
    rollout_id: str
    caller: CallerIdentity
    candidate: CandidateBinding
    requested_at: str
    runner_config_sha256: str
    preflight_attestation_sha256: str
    preflight_registry_sha256: str
    preflight_coverage_sha256: str
    command: RequestCommand = "start"
    status: RequestStatus = "pending"
    schema_version: SchemaVersion = 1

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        validate_safe_identifier(self.rollout_id, "rollout_id")
        if not isinstance(self.caller, CallerIdentity):
            raise ValueError("caller must be a CallerIdentity")
        if not isinstance(self.candidate, CandidateBinding):
            raise ValueError("candidate must be a CandidateBinding")
        _require_string(self.requested_at, "requested_at")
        _require_sha256(self.runner_config_sha256, "runner_config_sha256")
        _require_sha256(self.preflight_attestation_sha256, "preflight_attestation_sha256")
        _require_sha256(self.preflight_registry_sha256, "preflight_registry_sha256")
        _require_sha256(self.preflight_coverage_sha256, "preflight_coverage_sha256")
        _require_literal(self.command, _REQUEST_COMMANDS, "command", "request command")
        _require_literal(self.status, _REQUEST_STATUSES, "status", "request status")
        _require_schema(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "caller": self.caller.to_dict(),
            "candidate": self.candidate.to_dict(),
            "requested_at": self.requested_at,
            "runner_config_sha256": self.runner_config_sha256,
            "preflight_attestation_sha256": self.preflight_attestation_sha256,
            "preflight_registry_sha256": self.preflight_registry_sha256,
            "preflight_coverage_sha256": self.preflight_coverage_sha256,
            "command": self.command,
            "status": self.status,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> RolloutRequest:
        expected = {
            "request_id",
            "rollout_id",
            "caller",
            "candidate",
            "requested_at",
            "runner_config_sha256",
            "preflight_attestation_sha256",
            "preflight_registry_sha256",
            "preflight_coverage_sha256",
            "command",
            "status",
            "schema_version",
        }
        _require_exact_keys(data, expected, "rollout request")
        command = _require_literal(data["command"], _REQUEST_COMMANDS, "command", "request command")
        status = _require_literal(data["status"], _REQUEST_STATUSES, "status", "request status")
        return cls(
            request_id=validate_safe_identifier(data["request_id"], "request_id"),
            rollout_id=validate_safe_identifier(data["rollout_id"], "rollout_id"),
            caller=CallerIdentity.from_dict(_require_mapping(data["caller"], "caller")),
            candidate=CandidateBinding.from_dict(_require_mapping(data["candidate"], "candidate")),
            requested_at=_require_string(data["requested_at"], "requested_at"),
            runner_config_sha256=_require_sha256(
                data["runner_config_sha256"], "runner_config_sha256"
            ),
            preflight_attestation_sha256=_require_sha256(
                data["preflight_attestation_sha256"], "preflight_attestation_sha256"
            ),
            preflight_registry_sha256=_require_sha256(
                data["preflight_registry_sha256"], "preflight_registry_sha256"
            ),
            preflight_coverage_sha256=_require_sha256(
                data["preflight_coverage_sha256"], "preflight_coverage_sha256"
            ),
            command=cast(RequestCommand, command),
            status=cast(RequestStatus, status),
            schema_version=_require_schema(data["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class AttemptIdentity:
    attempt_number: int
    operator: str
    uid: int
    resume: bool
    schema_version: SchemaVersion = 1

    def __post_init__(self) -> None:
        _require_positive_int(self.attempt_number, "attempt_number")
        _require_username(self.operator, "operator")
        _require_nonnegative_int(self.uid, "uid")
        _require_bool(self.resume, "resume")
        _require_schema(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return {
            "attempt_number": self.attempt_number,
            "operator": self.operator,
            "uid": self.uid,
            "resume": self.resume,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> AttemptIdentity:
        expected = {"attempt_number", "operator", "uid", "resume", "schema_version"}
        _require_exact_keys(data, expected, "attempt identity")
        return cls(
            attempt_number=_require_positive_int(data["attempt_number"], "attempt_number"),
            operator=_require_username(data["operator"], "operator"),
            uid=_require_nonnegative_int(data["uid"], "uid"),
            resume=_require_bool(data["resume"], "resume"),
            schema_version=_require_schema(data["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class ActivePointer:
    request_id: str
    attempt_number: int
    unit_name: str
    status: ActiveStatus
    schema_version: SchemaVersion = 1

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        _require_positive_int(self.attempt_number, "attempt_number")
        if not isinstance(self.unit_name, str) or _UNIT_RE.fullmatch(self.unit_name) is None:
            raise ValueError("unit_name contains unsafe characters")
        _require_literal(self.status, _ACTIVE_STATUSES, "status", "active status")
        _require_schema(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "attempt_number": self.attempt_number,
            "unit_name": self.unit_name,
            "status": self.status,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ActivePointer:
        expected = {"request_id", "attempt_number", "unit_name", "status", "schema_version"}
        _require_exact_keys(data, expected, "active pointer")
        status = _require_literal(data["status"], _ACTIVE_STATUSES, "status", "active status")
        return cls(
            request_id=validate_safe_identifier(data["request_id"], "request_id"),
            attempt_number=_require_positive_int(data["attempt_number"], "attempt_number"),
            unit_name=_require_string(data["unit_name"], "unit_name"),
            status=cast(ActiveStatus, status),
            schema_version=_require_schema(data["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class DriverEnvelope:
    schema_version: SchemaVersion
    request_id: str
    rollout_id: str
    initiating_operator: str
    initiating_uid: int
    attempt_number: int
    attempt_operator: str
    attempt_uid: int
    remote_url: str
    target_ref: str
    resolved_sha: str
    image_tag: str
    fetched_at: str
    backup_manifest_path: str
    backup_manifest_sha256: str
    runner_config_sha256: str
    preflight_attestation_sha256: str
    preflight_registry_sha256: str
    preflight_coverage_sha256: str
    cluster_name: str
    namespace: str
    environment: str
    cp_url: str
    cluster_config_path: str
    rollout_root: str
    admin_token_source: str
    worker_token_source: str
    service_token_source: str
    expect_admin_token_fingerprint: str
    smoke_on_behalf_username: str
    smoke_on_behalf_team_id: str
    scope: str
    gb10_prep_concurrency: int
    resume: bool
    source_mode: CandidateSourceMode = "merged-dev"
    resolved_tree: str | None = None
    approved_base_sha: str | None = None

    def __post_init__(self) -> None:
        _require_schema(self.schema_version)
        validate_safe_identifier(self.request_id, "request_id")
        validate_safe_identifier(self.rollout_id, "rollout_id")
        _require_username(self.initiating_operator, "initiating_operator")
        _require_nonnegative_int(self.initiating_uid, "initiating_uid")
        _require_positive_int(self.attempt_number, "attempt_number")
        _require_username(self.attempt_operator, "attempt_operator")
        _require_nonnegative_int(self.attempt_uid, "attempt_uid")
        if self.remote_url != APPROVED_REMOTE_URL:
            raise ValueError("remote_url is not approved")
        if self.target_ref != PINNED_TARGET_REF:
            raise ValueError(f"target_ref must be {PINNED_TARGET_REF}")
        sha = _require_sha(self.resolved_sha, "resolved_sha")
        if self.image_tag != f"staging-{sha[:7]}":
            raise ValueError("image_tag must be staging-<resolved_sha[:7]>")
        if self.source_mode not in {"merged-dev", "sealed-cumulative"}:
            raise ValueError("source_mode is invalid")
        if self.source_mode == "sealed-cumulative":
            if self.resolved_tree is None or self.approved_base_sha is None:
                raise ValueError("sealed envelope source binding is incomplete")
            _require_sha(self.resolved_tree, "resolved_tree")
            _require_sha(self.approved_base_sha, "approved_base_sha")
        else:
            # A merged-dev envelope mirrors its CandidateBinding: it carries the
            # resolved git tree (a derivable identity the Tier-1 artifact
            # builders require) but never an approved base sha (the
            # sealed-cumulative approval anchor).
            if self.approved_base_sha is not None:
                raise ValueError("merged envelope must not carry an approved base sha")
            if self.resolved_tree is not None:
                _require_sha(self.resolved_tree, "resolved_tree")
        _require_string(self.fetched_at, "fetched_at")
        _require_absolute_path(self.backup_manifest_path, "backup_manifest_path")
        _require_sha256(self.backup_manifest_sha256, "backup_manifest_sha256")
        _require_sha256(self.runner_config_sha256, "runner_config_sha256")
        _require_sha256(self.preflight_attestation_sha256, "preflight_attestation_sha256")
        _require_sha256(self.preflight_registry_sha256, "preflight_registry_sha256")
        _require_sha256(self.preflight_coverage_sha256, "preflight_coverage_sha256")
        if self.cluster_name != "loom-staging":
            raise ValueError("cluster_name must be loom-staging")
        if self.namespace != "loom-staging":
            raise ValueError("namespace must be loom-staging")
        if self.environment != "staging":
            raise ValueError("environment must be staging")
        if self.cp_url != "http://127.0.0.1:18081":
            raise ValueError("cp_url must be http://127.0.0.1:18081")
        _require_absolute_path(self.cluster_config_path, "cluster_config_path")
        _require_absolute_path(self.rollout_root, "rollout_root")
        _require_file_source(self.admin_token_source, "admin_token_source")
        _require_file_source(self.worker_token_source, "worker_token_source")
        _require_file_source(self.service_token_source, "service_token_source")
        _require_fingerprint(self.expect_admin_token_fingerprint, "expect_admin_token_fingerprint")
        _require_username(self.smoke_on_behalf_username, "smoke_on_behalf_username")
        _require_string(self.smoke_on_behalf_team_id, "smoke_on_behalf_team_id")
        if self.scope != "current-gb10":
            raise ValueError("scope must be current-gb10")
        concurrency = _require_positive_int(self.gb10_prep_concurrency, "gb10_prep_concurrency")
        if concurrency > 15:
            raise ValueError("gb10_prep_concurrency must be between 1 and 15")
        resume = _require_bool(self.resume, "resume")
        if self.attempt_number == 1 and resume:
            raise ValueError("resume must be false for attempt 1")
        if self.attempt_number > 1 and not resume:
            raise ValueError("resume must be true after attempt 1")

    def rollout_inputs(self) -> dict[str, object]:
        value: dict[str, object] = {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "initiating_operator": self.initiating_operator,
            "initiating_uid": self.initiating_uid,
            "remote_url": self.remote_url,
            "target_ref": self.target_ref,
            "resolved_sha": self.resolved_sha,
            "image_tag": self.image_tag,
            "backup_manifest_path": self.backup_manifest_path,
            "backup_manifest_sha256": self.backup_manifest_sha256,
            "runner_config_sha256": self.runner_config_sha256,
            "preflight_attestation_sha256": self.preflight_attestation_sha256,
            "preflight_registry_sha256": self.preflight_registry_sha256,
            "preflight_coverage_sha256": self.preflight_coverage_sha256,
        }
        if self.source_mode == "sealed-cumulative":
            value.update(
                {
                    "source_mode": self.source_mode,
                    "resolved_tree": self.resolved_tree,
                    "approved_base_sha": self.approved_base_sha,
                }
            )
        return value

    def to_dict(self) -> dict[str, object]:
        value = {field.name: getattr(self, field.name) for field in fields(self)}
        if self.source_mode == "merged-dev":
            for key in ("source_mode", "resolved_tree", "approved_base_sha"):
                value.pop(key)
        return value

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> DriverEnvelope:
        expected = {field.name for field in fields(cls)}
        sealed = data.get("source_mode") == "sealed-cumulative"
        if not sealed:
            expected.difference_update({"source_mode", "resolved_tree", "approved_base_sha"})
        _require_exact_keys(data, expected, "driver envelope")
        return cls(
            schema_version=_require_schema(data["schema_version"]),
            request_id=validate_safe_identifier(data["request_id"], "request_id"),
            rollout_id=validate_safe_identifier(data["rollout_id"], "rollout_id"),
            initiating_operator=_require_username(
                data["initiating_operator"], "initiating_operator"
            ),
            initiating_uid=_require_nonnegative_int(data["initiating_uid"], "initiating_uid"),
            attempt_number=_require_positive_int(data["attempt_number"], "attempt_number"),
            attempt_operator=_require_username(data["attempt_operator"], "attempt_operator"),
            attempt_uid=_require_nonnegative_int(data["attempt_uid"], "attempt_uid"),
            remote_url=_require_string(data["remote_url"], "remote_url"),
            target_ref=_require_string(data["target_ref"], "target_ref"),
            resolved_sha=_require_sha(data["resolved_sha"], "resolved_sha"),
            image_tag=_require_string(data["image_tag"], "image_tag"),
            fetched_at=_require_string(data["fetched_at"], "fetched_at"),
            backup_manifest_path=_require_absolute_path(
                data["backup_manifest_path"], "backup_manifest_path"
            ),
            backup_manifest_sha256=_require_sha256(
                data["backup_manifest_sha256"], "backup_manifest_sha256"
            ),
            runner_config_sha256=_require_sha256(
                data["runner_config_sha256"], "runner_config_sha256"
            ),
            preflight_attestation_sha256=_require_sha256(
                data["preflight_attestation_sha256"], "preflight_attestation_sha256"
            ),
            preflight_registry_sha256=_require_sha256(
                data["preflight_registry_sha256"], "preflight_registry_sha256"
            ),
            preflight_coverage_sha256=_require_sha256(
                data["preflight_coverage_sha256"], "preflight_coverage_sha256"
            ),
            cluster_name=_require_string(data["cluster_name"], "cluster_name"),
            namespace=_require_string(data["namespace"], "namespace"),
            environment=_require_string(data["environment"], "environment"),
            cp_url=_require_string(data["cp_url"], "cp_url"),
            cluster_config_path=_require_absolute_path(
                data["cluster_config_path"], "cluster_config_path"
            ),
            rollout_root=_require_absolute_path(data["rollout_root"], "rollout_root"),
            admin_token_source=_require_file_source(
                data["admin_token_source"], "admin_token_source"
            ),
            worker_token_source=_require_file_source(
                data["worker_token_source"], "worker_token_source"
            ),
            service_token_source=_require_file_source(
                data["service_token_source"], "service_token_source"
            ),
            expect_admin_token_fingerprint=_require_fingerprint(
                data["expect_admin_token_fingerprint"],
                "expect_admin_token_fingerprint",
            ),
            smoke_on_behalf_username=_require_username(
                data["smoke_on_behalf_username"], "smoke_on_behalf_username"
            ),
            smoke_on_behalf_team_id=_require_string(
                data["smoke_on_behalf_team_id"], "smoke_on_behalf_team_id"
            ),
            scope=_require_string(data["scope"], "scope"),
            gb10_prep_concurrency=_require_positive_int(
                data["gb10_prep_concurrency"], "gb10_prep_concurrency"
            ),
            resume=_require_bool(data["resume"], "resume"),
            source_mode="sealed-cumulative" if sealed else "merged-dev",
            resolved_tree=(
                _require_sha(data["resolved_tree"], "resolved_tree") if sealed else None
            ),
            approved_base_sha=(
                _require_sha(data["approved_base_sha"], "approved_base_sha") if sealed else None
            ),
        )


def driver_envelope_bytes(envelope: DriverEnvelope) -> bytes:
    """Return the exact immutable store representation for one driver envelope."""
    return (json.dumps(envelope.to_dict(), sort_keys=True, separators=(",", ":")) + "\n").encode()


def driver_envelope_sha256(envelope: DriverEnvelope) -> str:
    return hashlib.sha256(driver_envelope_bytes(envelope)).hexdigest()


@dataclass(frozen=True, slots=True)
class RequestEvent:
    request_id: str
    event: RequestEventType
    occurred_at: str
    operator: str
    operator_uid: int
    attempt_number: int | None = None
    unit_name: str | None = None
    status: EventStatus | None = None
    reason: str | None = None
    current_step: str | None = None
    schema_version: SchemaVersion = 1

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        _require_literal(self.event, _REQUEST_EVENTS, "event", "request event")
        _require_string(self.occurred_at, "occurred_at")
        _require_username(self.operator, "operator")
        _require_nonnegative_int(self.operator_uid, "operator_uid")
        _require_optional_positive_int(self.attempt_number, "attempt_number")
        if self.unit_name is not None and _UNIT_RE.fullmatch(self.unit_name) is None:
            raise ValueError("unit_name contains unsafe characters")
        if self.status is not None:
            _require_literal(self.status, _EVENT_STATUSES, "status", "event status")
        if self.reason is not None:
            reason = _require_string(self.reason, "reason")
            if len(reason) > 500 or any(ord(char) < 32 and char not in "\t" for char in reason):
                raise ValueError("reason must be at most 500 characters without control bytes")
            if (
                self.event
                in {
                    "backup_failed",
                    "backup_cleanup_started",
                    "backup_cleanup_done",
                    "backup_cleanup_failed",
                }
                and reason not in BACKUP_PUBLIC_REASONS
            ):
                raise ValueError("backup event reason is not an approved public token")
        if (
            self.current_step is not None
            and len(_require_string(self.current_step, "current_step")) > 128
        ):
            raise ValueError("current_step must be at most 128 characters")
        _require_schema(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "event": self.event,
            "occurred_at": self.occurred_at,
            "operator": self.operator,
            "operator_uid": self.operator_uid,
            "attempt_number": self.attempt_number,
            "unit_name": self.unit_name,
            "status": self.status,
            "reason": self.reason,
            "current_step": self.current_step,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> RequestEvent:
        expected = {
            "request_id",
            "event",
            "occurred_at",
            "operator",
            "operator_uid",
            "attempt_number",
            "unit_name",
            "status",
            "reason",
            "current_step",
            "schema_version",
        }
        _require_exact_keys(data, expected, "request event")
        event = _require_literal(data["event"], _REQUEST_EVENTS, "event", "request event")
        status_value = data["status"]
        status: str | None = None
        if status_value is not None:
            status = _require_literal(status_value, _EVENT_STATUSES, "status", "event status")
        return cls(
            request_id=validate_safe_identifier(data["request_id"], "request_id"),
            event=cast(RequestEventType, event),
            occurred_at=_require_string(data["occurred_at"], "occurred_at"),
            operator=_require_username(data["operator"], "operator"),
            operator_uid=_require_nonnegative_int(data["operator_uid"], "operator_uid"),
            attempt_number=_require_optional_positive_int(data["attempt_number"], "attempt_number"),
            unit_name=_require_optional_string(data["unit_name"], "unit_name"),
            status=cast(EventStatus | None, status),
            reason=_require_optional_string(data["reason"], "reason"),
            current_step=_require_optional_string(data["current_step"], "current_step"),
            schema_version=_require_schema(data["schema_version"]),
        )
