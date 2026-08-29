"""Canonical owner evidence for personal-development acceptance."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.dev_instance import derive_identity
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS, PERSONAL_DEV_PLATFORMS
from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlan,
    PersonalDevControlPlaneProfile,
    PersonalDevOperationalPlan,
    PersonalDevTrustedRelease,
)
from loom.personal_dev_expected_denial import expected_hidden_denial_sha256
from loom.personal_dev_minio_backup import (
    load_personal_dev_minio_manifest,
    validate_personal_dev_minio_payload_root,
)

_DIGEST = re.compile(r"[0-9a-f]{64}")
_EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
_GIT_IDENTITY = re.compile(r"[0-9a-f]{40}")
_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
_MAX_SOURCE_BYTES = 16 * 1024 * 1024
_LAUNCHER_SOURCE_FILES = (
    "src/loom_capacity_executor/bootstrap_handoff.py",
    "src/loom_capacity_executor/runtime.py",
    "src/loom_capacity_executor/trusted_launcher.py",
)
_SCANNER_SOURCE_FILE = "src/loom/personal_dev_builder_tools.py"
_SCANNER_ARGV = (
    "image",
    "--input",
    "<verified-oci-archive>",
    "--format",
    "json",
    "--scanners",
    "vuln,secret",
    "--severity",
    "HIGH,CRITICAL",
    "--exit-code",
    "1",
    "--no-progress",
    "--offline-scan",
    "--skip-db-update",
    "--skip-java-db-update",
    "--cache-dir",
    "<release-bound-cache>",
)
_MANAGEMENT_SECRET_KEYS = (
    "admin-secrets.toml",
    "capacity-lifecycle-ca.pem",
    "capacity-lifecycle-certificate.pem",
    "capacity-lifecycle-private-key.pem",
    "capacity-lifecycle-token",
    "capacity-reporter-ca.pem",
    "capacity-reporter-certificate.pem",
    "capacity-reporter-private-key.pem",
    "config.json",
    "dev-instance-database-admin-url",
    "minio-access-key",
    "minio-secret-key",
    "postgres-database",
    "postgres-password",
    "postgres-user",
    "secret-store-master-key",
    "svc-db-url",
)


class PersonalDevAcceptanceEvidenceError(ValueError):
    """Personal-development acceptance evidence is invalid."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _SourceBinding(_StrictModel):
    commit: str
    tree: str

    @field_validator("commit", "tree")
    @classmethod
    def _git_identity(cls, value: str) -> str:
        if _GIT_IDENTITY.fullmatch(value) is None:
            raise ValueError("source identity is invalid")
        return value


class _PostgresRestore(_StrictModel):
    dump_sha256: str
    image: str
    source_schema_head: str
    restored_schema_head: str
    source_state_sha256: str
    restored_state_sha256: str

    @field_validator("dump_sha256", "source_state_sha256", "restored_state_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("Postgres evidence digest is invalid")
        return value

    @field_validator("source_schema_head", "restored_schema_head")
    @classmethod
    def _schema_head(cls, value: str) -> str:
        if re.fullmatch(r"[0-9]{4}", value) is None:
            raise ValueError("Postgres schema head is invalid")
        return value

    @model_validator(mode="after")
    def _restored_state_matches(self) -> _PostgresRestore:
        if (
            self.source_schema_head != self.restored_schema_head
            or self.source_state_sha256 != self.restored_state_sha256
        ):
            raise ValueError("Postgres restore does not match the source snapshot")
        return self


class _MinioRestoreEmpty(_StrictModel):
    backup_manifest_sha256: str
    image: str
    source_object_count: Literal[0]
    restored_object_count: Literal[0]
    restored_manifest_sha256: str

    @field_validator("backup_manifest_sha256", "restored_manifest_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("MinIO evidence digest is invalid")
        return value

    @model_validator(mode="after")
    def _restored_state_matches(self) -> _MinioRestoreEmpty:
        if (
            self.source_object_count != self.restored_object_count
            or self.backup_manifest_sha256 != self.restored_manifest_sha256
        ):
            raise ValueError("MinIO restore does not match the source snapshot")
        return self


class _MinioRestoreRetained(_StrictModel):
    backup_manifest_sha256: str
    image: str
    source_object_count: int = Field(gt=0, le=10_000)
    restored_object_count: int = Field(gt=0, le=10_000)
    restored_manifest_sha256: str
    retained_payload_inventory_sha256: str
    retained_payload_count: int = Field(gt=0, le=10_000)
    retained_payload_bytes: int = Field(ge=0, le=1024 * 1024 * 1024 * 1024)

    @field_validator(
        "backup_manifest_sha256",
        "restored_manifest_sha256",
        "retained_payload_inventory_sha256",
    )
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("MinIO evidence digest is invalid")
        return value

    @model_validator(mode="after")
    def _restored_state_matches(self) -> _MinioRestoreRetained:
        if (
            self.source_object_count != self.restored_object_count
            or self.backup_manifest_sha256 != self.restored_manifest_sha256
            or self.retained_payload_count > self.source_object_count
        ):
            raise ValueError("MinIO restore does not match the source snapshot")
        return self


class _SecretBoundary(_StrictModel):
    key_inventory_sha256: str
    values_included: Literal[False]

    @field_validator("key_inventory_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("Secret inventory digest is invalid")
        return value


class _StorageBoundary(_StrictModel):
    postgres_pvc: Literal["data-loom-dev-postgres-0"]
    minio_pvc: Literal["data-loom-dev-minio-0"]
    storage_class: Literal["longhorn"]


class _ManagerBoundary(_StrictModel):
    executable_new_capacity_ceiling: Literal[0]
    personal_worker_count: Literal[0]


class _CleanupBoundary(_StrictModel):
    isolated_postgres_absent: Literal[True]
    isolated_minio_absent: Literal[True]
    isolated_network_absent: Literal[True]


class PersonalDevBackupRestoreEvidence(_StrictModel):
    schema_name: Literal[
        "loom-personal-dev-backup-restore-evidence-v1",
        "loom-personal-dev-backup-restore-evidence-v2",
    ] = Field(alias="schema")
    source: _SourceBinding
    release_sha256: str
    namespace: Literal["loom-dev"]
    started_at: str
    completed_at: str
    postgres: _PostgresRestore
    minio: _MinioRestoreEmpty | _MinioRestoreRetained
    secrets: _SecretBoundary
    storage: _StorageBoundary
    manager: _ManagerBoundary
    cleanup: _CleanupBoundary

    @field_validator("release_sha256")
    @classmethod
    def _release_digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("release digest is invalid")
        return value

    @field_validator("started_at", "completed_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        if _TIMESTAMP.fullmatch(value) is None:
            raise ValueError("evidence timestamp is invalid")
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        return value

    @model_validator(mode="after")
    def _time_order(self) -> PersonalDevBackupRestoreEvidence:
        started = datetime.strptime(self.started_at, "%Y-%m-%dT%H:%M:%SZ")
        completed = datetime.strptime(self.completed_at, "%Y-%m-%dT%H:%M:%SZ")
        if completed < started:
            raise ValueError("backup/restore evidence time order is invalid")
        if (
            self.schema_name == "loom-personal-dev-backup-restore-evidence-v1"
            and not isinstance(self.minio, _MinioRestoreEmpty)
        ) or (
            self.schema_name == "loom-personal-dev-backup-restore-evidence-v2"
            and not isinstance(self.minio, _MinioRestoreRetained)
        ):
            raise ValueError("MinIO evidence schema variant is invalid")
        return self


def _validated_digest(value: str) -> str:
    if _DIGEST.fullmatch(value) is None or value == "0" * 64:
        raise ValueError("evidence digest is invalid")
    return value


def _validated_nonempty_evidence_digest(value: str) -> str:
    validated = _validated_digest(value)
    if hmac.compare_digest(validated, _EMPTY_SHA256):
        raise ValueError("nonempty evidence digest is invalid")
    return validated


def _validated_uuid(value: str) -> str:
    try:
        parsed = UUID(value)
    except ValueError:
        raise ValueError("evidence UUID is invalid") from None
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("evidence UUID is invalid")
    return value


class _AcceptanceResultIdentity(_StrictModel):
    environment: str
    namespace: str
    database: str
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str
    route_host: str
    worker_control_plane_host: str
    worker_gateway_host: str
    route_path: str
    worker_pool: str


class _AcceptanceResultSnapshot(_StrictModel):
    application_status: Literal[
        "provisioning",
        "ready",
        "updating",
        "activating",
        "deleting",
        "draining",
        "failed",
        "deleted",
    ]
    candidate_sha: str
    capacity_prepared: bool
    capacity_status: Literal["shadow", "prepared", "waiting", "available"]
    deployment_generation: int = Field(gt=0)
    identity: _AcceptanceResultIdentity
    keep_data: bool
    max_slots: int = Field(gt=0)
    min_slots: int = Field(ge=0)
    name: str
    operation_epoch: int = Field(gt=0)
    owner_team_id: str
    owner_user_id: str
    status: Literal[
        "provisioning",
        "ready",
        "updating",
        "activating",
        "deleting",
        "draining",
        "failed",
        "deleted",
    ]
    subject_id: str
    subject_incarnation: str
    worker_available: Literal[False]

    @field_validator("candidate_sha")
    @classmethod
    def _digest(cls, value: str) -> str:
        return _validated_digest(value)

    @field_validator(
        "owner_team_id",
        "owner_user_id",
        "subject_id",
        "subject_incarnation",
    )
    @classmethod
    def _uuid(cls, value: str) -> str:
        return _validated_uuid(value)

    @model_validator(mode="after")
    def _identity_is_derived(self) -> _AcceptanceResultSnapshot:
        identity = derive_identity(self.name)
        expected = {
            "environment": identity.runtime_environment,
            "namespace": identity.namespace,
            "database": identity.database,
            "task_bucket": identity.task_bucket,
            "trajectories_bucket": identity.trajectories_bucket,
            "artifacts_bucket": identity.artifacts_bucket,
            "route_host": identity.route_host,
            "worker_control_plane_host": identity.worker_control_plane_host,
            "worker_gateway_host": identity.worker_gateway_host,
            "route_path": identity.route_path,
            "worker_pool": identity.worker_pool,
        }
        if self.identity.model_dump() != expected:
            raise ValueError("acceptance result identity is invalid")
        return self


class _AcceptanceOwnerResult(_StrictModel):
    initial: _AcceptanceResultSnapshot
    updated: _AcceptanceResultSnapshot
    destroyed: _AcceptanceResultSnapshot
    redeployed: _AcceptanceResultSnapshot | None
    final_destroyed: _AcceptanceResultSnapshot | None


class _CrossOwnerDenial(_StrictModel):
    actor_team_id: str
    actor_user_id: str
    operation: Literal["read", "update", "destroy"]
    target_environment: str
    target_team_id: str
    target_user_id: str
    exit_code: Literal[1]
    stdout_sha256: str
    stderr_sha256: str
    target_before_sha256: str
    target_after_sha256: str

    @field_validator(
        "stdout_sha256",
        "stderr_sha256",
    )
    @classmethod
    def _digest(cls, value: str) -> str:
        return _validated_digest(value)

    @field_validator("target_before_sha256", "target_after_sha256")
    @classmethod
    def _nonempty_evidence_digest(cls, value: str) -> str:
        return _validated_nonempty_evidence_digest(value)

    @field_validator(
        "actor_team_id",
        "actor_user_id",
        "target_team_id",
        "target_user_id",
    )
    @classmethod
    def _uuid(cls, value: str) -> str:
        return _validated_uuid(value)

    @field_validator("target_environment")
    @classmethod
    def _environment_name(cls, value: str) -> str:
        derive_identity(value)
        return value


class _AcceptanceStatusSha256s(_StrictModel):
    pre_deploy: str
    after_initial: str
    after_updates: str
    after_denials: str
    after_destroy: str
    after_redeploy: str
    pre_rollback: str
    rollback_shadow: str

    @field_validator("*")
    @classmethod
    def _digest(cls, value: str) -> str:
        return _validated_nonempty_evidence_digest(value)


class PersonalDevAcceptanceResultV2(_StrictModel):
    """Canonical concurrent-owner zero-capacity acceptance result."""

    schema_name: Literal["loom-personal-dev-zero-capacity-acceptance-result-v2"] = Field(
        alias="schema"
    )
    acceptance_manifest_sha256: str
    acceptance_plan_sha256: str
    release_sha256: str
    shadow_manifest_sha256: str
    owners: tuple[_AcceptanceOwnerResult, _AcceptanceOwnerResult]
    cross_owner_denials: tuple[
        _CrossOwnerDenial,
        _CrossOwnerDenial,
        _CrossOwnerDenial,
        _CrossOwnerDenial,
        _CrossOwnerDenial,
        _CrossOwnerDenial,
    ]
    status_sha256s: _AcceptanceStatusSha256s

    @field_validator(
        "acceptance_manifest_sha256",
        "acceptance_plan_sha256",
        "release_sha256",
        "shadow_manifest_sha256",
    )
    @classmethod
    def _digest(cls, value: str) -> str:
        return _validated_digest(value)

    @field_validator("owners", "cross_owner_denials", mode="before")
    @classmethod
    def _arrays_are_exact(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, list):
            raise ValueError("acceptance result arrays are invalid")
        return tuple(value)

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.model_dump(mode="json", by_alias=True))


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _duplicate_rejecting_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_owner_only(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        before_path = path.lstat()
        if (
            not stat.S_ISREG(before_path.st_mode)
            or stat.S_ISLNK(before_path.st_mode)
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_path.st_nlink != 1
            or not 0 < before_path.st_size <= _MAX_EVIDENCE_BYTES
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before_path):
            raise ValueError
        payload = bytearray()
        while len(payload) <= _MAX_EVIDENCE_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, _MAX_EVIDENCE_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or _identity(os.fstat(descriptor)) != _identity(opened)
            or _identity(path.lstat()) != _identity(before_path)
        ):
            raise ValueError
        return bytes(payload)
    except (OSError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _load_json(path: Path, expected_sha256: str) -> tuple[bytes, dict[str, Any]]:
    if _DIGEST.fullmatch(expected_sha256) is None or expected_sha256 == "0" * 64:
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    payload = _read_owner_only(path)
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected_sha256):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(value, dict) or _canonical_json(value) != payload:
            raise ValueError
    except (RecursionError, UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    return payload, value


def _validate_ready_snapshot(
    snapshot: _AcceptanceResultSnapshot,
    *,
    max_slots: int,
) -> None:
    if (
        snapshot.status != "ready"
        or snapshot.application_status != "ready"
        or snapshot.capacity_status != "prepared"
        or snapshot.capacity_prepared is not True
        or snapshot.worker_available is not False
        or snapshot.keep_data is not False
        or snapshot.min_slots != 0
        or snapshot.max_slots != max_slots
    ):
        raise ValueError("ready acceptance snapshot is invalid")


def _validate_ready_transition(
    initial: _AcceptanceResultSnapshot,
    updated: _AcceptanceResultSnapshot,
    *,
    updated_max_slots: int,
) -> None:
    _validate_ready_snapshot(initial, max_slots=2)
    _validate_ready_snapshot(updated, max_slots=updated_max_slots)
    if (
        initial.name != updated.name
        or initial.owner_team_id != updated.owner_team_id
        or initial.owner_user_id != updated.owner_user_id
        or initial.identity != updated.identity
        or initial.subject_id != updated.subject_id
        or initial.subject_incarnation != updated.subject_incarnation
        or initial.deployment_generation != 1
        or updated.deployment_generation != initial.deployment_generation + 1
        or initial.operation_epoch != 1
        or updated.operation_epoch != initial.operation_epoch + 1
        or hmac.compare_digest(initial.candidate_sha, updated.candidate_sha)
    ):
        raise ValueError("ready acceptance transition is invalid")


def _validate_destroy(
    updated: _AcceptanceResultSnapshot,
    destroyed: _AcceptanceResultSnapshot,
    *,
    keep_data: bool,
) -> None:
    if (
        destroyed.status != "deleted"
        or destroyed.application_status != "deleted"
        or destroyed.capacity_status != "shadow"
        or destroyed.capacity_prepared is not False
        or destroyed.worker_available is not False
        or destroyed.keep_data is not keep_data
        or destroyed.name != updated.name
        or destroyed.owner_team_id != updated.owner_team_id
        or destroyed.owner_user_id != updated.owner_user_id
        or destroyed.identity != updated.identity
        or destroyed.subject_id != updated.subject_id
        or destroyed.subject_incarnation != updated.subject_incarnation
        or destroyed.min_slots != updated.min_slots
        or destroyed.max_slots != updated.max_slots
        or destroyed.deployment_generation != updated.deployment_generation
        or destroyed.operation_epoch != updated.operation_epoch + 1
        or not hmac.compare_digest(destroyed.candidate_sha, updated.candidate_sha)
    ):
        raise ValueError("destroyed acceptance transition is invalid")


def _validate_retained_redeploy(
    destroyed: _AcceptanceResultSnapshot,
    redeployed: _AcceptanceResultSnapshot,
    final_destroyed: _AcceptanceResultSnapshot,
) -> None:
    _validate_ready_snapshot(redeployed, max_slots=2)
    if (
        destroyed.keep_data is not True
        or redeployed.name != destroyed.name
        or redeployed.owner_team_id != destroyed.owner_team_id
        or redeployed.owner_user_id != destroyed.owner_user_id
        or redeployed.identity != destroyed.identity
        or redeployed.subject_id != destroyed.subject_id
        or redeployed.subject_incarnation == destroyed.subject_incarnation
        or redeployed.deployment_generation != 1
        or redeployed.operation_epoch != destroyed.operation_epoch + 1
    ):
        raise ValueError("retained acceptance redeploy is invalid")
    _validate_destroy(redeployed, final_destroyed, keep_data=False)


def _validate_denial_matrix(
    plan: PersonalDevAcceptancePlan,
    denials: tuple[_CrossOwnerDenial, ...],
) -> None:
    expected = tuple(
        (actor_index, target_index, operation)
        for actor_index, target_index in ((0, 1), (1, 0))
        for operation in ("read", "update", "destroy")
    )
    if len(denials) != len(expected):
        raise ValueError("cross-owner denial matrix is incomplete")
    for denial, (actor_index, target_index, operation) in zip(
        denials,
        expected,
        strict=True,
    ):
        actor = plan.acceptance_owners[actor_index]
        target = plan.acceptance_owners[target_index]
        if (
            denial.actor_team_id != str(actor.team_id)
            or denial.actor_user_id != str(actor.user_id)
            or denial.target_team_id != str(target.team_id)
            or denial.target_user_id != str(target.user_id)
            or denial.operation != operation
            or denial.exit_code != 1
            or not hmac.compare_digest(denial.stdout_sha256, _EMPTY_SHA256)
            or not hmac.compare_digest(
                denial.stderr_sha256,
                expected_hidden_denial_sha256(operation),
            )
            or not hmac.compare_digest(
                denial.target_before_sha256,
                denial.target_after_sha256,
            )
        ):
            raise ValueError("cross-owner denial matrix is invalid")


def load_personal_dev_acceptance_result(
    path: Path,
    expected_sha256: str,
    *,
    plan: PersonalDevAcceptancePlan,
    expected_acceptance_manifest_sha256: str,
) -> PersonalDevAcceptanceResultV2:
    """Load strict canonical read-only evidence for the two-owner acceptance run."""

    payload, value = _load_json(path, expected_sha256)
    try:
        _validated_digest(expected_acceptance_manifest_sha256)
        result = PersonalDevAcceptanceResultV2.model_validate(value)
        if (
            plan.schema_version != 2
            or len(plan.acceptance_owners) != 2
            or not hmac.compare_digest(
                result.acceptance_manifest_sha256,
                expected_acceptance_manifest_sha256,
            )
            or not hmac.compare_digest(result.acceptance_plan_sha256, plan.sha256)
            or not hmac.compare_digest(
                result.release_sha256,
                plan.release.trusted_release_sha256,
            )
            or not hmac.compare_digest(
                result.shadow_manifest_sha256,
                plan.release.shadow_manifest_sha256,
            )
        ):
            raise ValueError("acceptance result binding is invalid")

        for owner_index, (owner_result, plan_owner) in enumerate(
            zip(result.owners, plan.acceptance_owners, strict=True)
        ):
            initial = owner_result.initial
            if initial.owner_team_id != str(plan_owner.team_id) or initial.owner_user_id != str(
                plan_owner.user_id
            ):
                raise ValueError("acceptance result owner order is invalid")
            _validate_ready_transition(
                initial,
                owner_result.updated,
                updated_max_slots=3 if owner_index == 0 else 4,
            )
            _validate_destroy(
                owner_result.updated,
                owner_result.destroyed,
                keep_data=owner_index == 1,
            )
            if owner_index == 0:
                if owner_result.redeployed is not None or owner_result.final_destroyed is not None:
                    raise ValueError("default destroy result is invalid")
            elif owner_result.redeployed is None or owner_result.final_destroyed is None:
                raise ValueError("retained destroy result is incomplete")
            else:
                _validate_retained_redeploy(
                    owner_result.destroyed,
                    owner_result.redeployed,
                    owner_result.final_destroyed,
                )

        owner_0 = result.owners[0].initial
        owner_1 = result.owners[1].initial
        if (
            owner_0.name == owner_1.name
            or owner_0.subject_id == owner_1.subject_id
            or owner_0.subject_incarnation == owner_1.subject_incarnation
            or any(
                getattr(owner_0.identity, field) == getattr(owner_1.identity, field)
                for field in _AcceptanceResultIdentity.model_fields
            )
        ):
            raise ValueError("acceptance result owner identities are not disjoint")
        if hmac.compare_digest(
            result.owners[0].updated.candidate_sha,
            result.owners[1].updated.candidate_sha,
        ):
            raise ValueError("acceptance result owner candidates are not independent")

        _validate_denial_matrix(plan, result.cross_owner_denials)
        expected_targets = (
            result.owners[1].initial.name,
            result.owners[1].initial.name,
            result.owners[1].initial.name,
            result.owners[0].initial.name,
            result.owners[0].initial.name,
            result.owners[0].initial.name,
        )
        if any(
            denial.target_environment != target
            for denial, target in zip(
                result.cross_owner_denials,
                expected_targets,
                strict=True,
            )
        ):
            raise ValueError("cross-owner denial target is invalid")
        if result.canonical_bytes() != payload:
            raise ValueError("acceptance result is not canonical")
    except ValueError:
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    return result


def _parse_json_document(path: Path, *, canonical: bool) -> tuple[bytes, Any]:
    payload = _read_owner_only(path)
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if canonical and _canonical_json(value) != payload:
            raise ValueError
    except (RecursionError, UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    return payload, value


def _sha256_owner_only_file(path: Path) -> str:
    descriptor: int | None = None
    try:
        before_path = path.lstat()
        if (
            not stat.S_ISREG(before_path.st_mode)
            or stat.S_ISLNK(before_path.st_mode)
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_path.st_nlink != 1
            or before_path.st_size <= 0
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before_path):
            raise ValueError
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
        if (
            total != opened.st_size
            or _identity(os.fstat(descriptor)) != _identity(opened)
            or _identity(path.lstat()) != _identity(before_path)
        ):
            raise ValueError
        return digest.hexdigest()
    except (OSError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_source_file(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        before_path = path.lstat()
        if (
            not stat.S_ISREG(before_path.st_mode)
            or stat.S_ISLNK(before_path.st_mode)
            or before_path.st_uid != os.geteuid()
            or before_path.st_nlink != 1
            or not 0 < before_path.st_size <= _MAX_SOURCE_BYTES
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before_path):
            raise ValueError
        payload = bytearray()
        while len(payload) <= _MAX_SOURCE_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, _MAX_SOURCE_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or _identity(os.fstat(descriptor)) != _identity(opened)
            or _identity(path.lstat()) != _identity(before_path)
        ):
            raise ValueError
        return bytes(payload)
    except (OSError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _git_output(
    source_root: Path,
    *arguments: str,
    maximum_bytes: int,
) -> bytes:
    environment = {
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_NO_LAZY_FETCH": "1",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_TERMINAL_PROMPT": "0",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
    }
    try:
        result = subprocess.run(
            [
                "/usr/bin/git",
                "--no-replace-objects",
                "-C",
                str(source_root),
                *arguments,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            env=environment,
        )
        if result.returncode != 0 or len(result.stdout) > maximum_bytes:
            raise ValueError
        return result.stdout
    except (OSError, subprocess.SubprocessError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None


def _validate_source_root(
    source_root: Path,
    release: PersonalDevTrustedRelease,
    relative_files: tuple[str, ...],
) -> Path:
    try:
        if not source_root.is_absolute():
            raise ValueError
        root = source_root.resolve(strict=True)
        if root != source_root or not root.is_dir():
            raise ValueError
        top_level = Path(
            os.fsdecode(
                _git_output(
                    root,
                    "rev-parse",
                    "--show-toplevel",
                    maximum_bytes=4096,
                )
            ).strip()
        ).resolve(strict=True)
        head = (
            _git_output(
                root,
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
                maximum_bytes=128,
            )
            .decode("ascii")
            .strip()
        )
        tree = (
            _git_output(
                root,
                "rev-parse",
                "--verify",
                "HEAD^{tree}",
                maximum_bytes=128,
            )
            .decode("ascii")
            .strip()
        )
        status = _git_output(
            root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            *relative_files,
            maximum_bytes=4096,
        )
        if top_level != root or head != release.source_sha or tree != release.source_tree or status:
            raise ValueError
        return root
    except (OSError, UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None


def _source_file_sha256(source_root: Path, source_sha: str, relative: str) -> str:
    object_spec = f"{source_sha}:{relative}"
    try:
        raw_size = (
            _git_output(
                source_root,
                "cat-file",
                "-s",
                object_spec,
                maximum_bytes=64,
            )
            .decode("ascii")
            .strip()
        )
        if re.fullmatch(r"[0-9]+", raw_size) is None:
            raise ValueError
        size = int(raw_size)
        if not 0 < size <= _MAX_SOURCE_BYTES:
            raise ValueError
        payload = _git_output(
            source_root,
            "cat-file",
            "blob",
            object_spec,
            maximum_bytes=size,
        )
        if len(payload) != size:
            raise ValueError
    except (UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    if not hmac.compare_digest(payload, _read_source_file(source_root / relative)):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    return hashlib.sha256(payload).hexdigest()


def build_personal_dev_trusted_launcher_profile(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    source_root: Path,
) -> dict[str, object]:
    """Derive the exact launcher profile from the checked-out source and profile."""

    source_root = _validate_source_root(source_root, release, _LAUNCHER_SOURCE_FILES)
    value = {
        "contract": {
            "candidate_argv_absolute": True,
            "candidate_executable_identity": True,
            "candidate_image_digest": True,
            "immutable_candidate_snapshot": True,
            "release_digest": True,
            "single_use_bootstrap_handoff": True,
        },
        "files": {
            relative: _source_file_sha256(source_root, release.source_sha, relative)
            for relative in _LAUNCHER_SOURCE_FILES
        },
        "protocol_versions": dict(sorted(profile.protocol_versions.items())),
        "schema": "loom-personal-dev-trusted-launcher-profile-v1",
        "source": {
            "commit": release.source_sha,
            "tree": release.source_tree,
        },
    }
    _validate_source_root(source_root, release, _LAUNCHER_SOURCE_FILES)
    return value


def build_personal_dev_scanner_finding_policy(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    source_root: Path,
) -> dict[str, object]:
    """Derive the exact offline scanner policy from source and trusted release."""

    source_root = _validate_source_root(source_root, release, (_SCANNER_SOURCE_FILE,))
    scanner = release.scanner
    value: dict[str, object] = {
        "argv": list(_SCANNER_ARGV),
        "components": list(PERSONAL_DEV_COMPONENTS),
        "denied_finding_fields": [
            "Licenses",
            "Misconfigurations",
            "Secrets",
            "Vulnerabilities",
        ],
        "limits": {
            "max_report_bytes": 16 * 1024 * 1024,
            "timeout_seconds": 900,
        },
        "platforms": list(PERSONAL_DEV_PLATFORMS),
        "release_scanner": {
            "binary_platform": scanner.binary_platform,
            "binary_sha256": scanner.binary_sha256,
            "cache_identity_sha256": scanner.cache_identity_sha256,
            "database_metadata_sha256": scanner.database_metadata_sha256,
            "database_sha256": scanner.database_sha256,
            "java_database_metadata_sha256": scanner.java_database_metadata_sha256,
            "java_database_sha256": scanner.java_database_sha256,
            "lock_sha256": scanner.lock_sha256,
            "trivy_version": scanner.trivy_version,
        },
        "schema": "loom-personal-dev-scanner-finding-policy-v1",
        "source": {
            "commit": release.source_sha,
            "tree": release.source_tree,
        },
        "source_file_sha256": _source_file_sha256(
            source_root,
            release.source_sha,
            _SCANNER_SOURCE_FILE,
        ),
    }
    _validate_source_root(source_root, release, (_SCANNER_SOURCE_FILE,))
    return value


def validate_personal_dev_policy_evidence(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    plan: PersonalDevAcceptancePlan | PersonalDevOperationalPlan,
    source_root: Path,
    trusted_launcher_profile_path: Path,
    scanner_finding_policy_path: Path,
) -> None:
    """Require exact canonical policy artifacts and plan digest bindings."""

    expected_launcher = _canonical_json(
        build_personal_dev_trusted_launcher_profile(
            profile=profile,
            release=release,
            source_root=source_root,
        )
    )
    launcher_payload, _ = _load_json(
        trusted_launcher_profile_path,
        plan.builder.trusted_launcher_profile_sha256,
    )
    expected_scanner = _canonical_json(
        build_personal_dev_scanner_finding_policy(
            profile=profile,
            release=release,
            source_root=source_root,
        )
    )
    scanner_payload, _ = _load_json(
        scanner_finding_policy_path,
        plan.builder.scanner_finding_policy_sha256,
    )
    if not hmac.compare_digest(launcher_payload, expected_launcher) or not hmac.compare_digest(
        scanner_payload,
        expected_scanner,
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")


def _validate_postgres_state(payload: bytes) -> None:
    try:
        if not payload.endswith(b"\n") or b"\r" in payload:
            raise ValueError
        lines = payload.decode("ascii").splitlines()
        parsed: list[tuple[str, str, int, str]] = []
        for line in lines:
            record_type, name, numeric_value, state_value = line.split("\t")
            if not name or "\x00" in name:
                raise ValueError
            if record_type == "table":
                if (
                    re.fullmatch(r"[0-9]+", numeric_value) is None
                    or _DIGEST.fullmatch(state_value) is None
                    or state_value == "0" * 64
                ):
                    raise ValueError
            elif record_type == "sequence":
                if re.fullmatch(r"-?[0-9]+", numeric_value) is None or state_value not in {
                    "f",
                    "t",
                }:
                    raise ValueError
            else:
                raise ValueError
            parsed.append((record_type, name, int(numeric_value), state_value))
        identities = [(record_type, name) for record_type, name, _, _ in parsed]
        if (
            not parsed
            or identities != sorted(identities)
            or len(set(identities)) != len(identities)
            or not any(record_type == "table" for record_type, _, _, _ in parsed)
        ):
            raise ValueError
    except (UnicodeError, ValueError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None


def _validate_shadow_status(
    value: Any,
    *,
    web_expected: bool | None = None,
) -> None:
    base_components = (
        "cluster-resources",
        "manager",
        "namespaced-resources",
        "namespaces",
        "personal-workers",
        "runtime-class",
    )
    schema_three_components = (*base_components, "web")
    expected_fields = {
        "blockers",
        "components",
        "input_sha256",
        "manager_ceiling",
        "mode",
        "ready",
        "release_sha256",
        "schema",
        "worker_available",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected_fields
        or value.get("schema") != "loom-personal-dev-control-plane-status-v1"
        or value.get("mode") != "shadow"
        or value.get("ready") is not True
        or value.get("blockers") != []
        or not isinstance(value.get("input_sha256"), str)
        or _DIGEST.fullmatch(value["input_sha256"]) is None
        or value["input_sha256"] == "0" * 64
        or not isinstance(value.get("release_sha256"), str)
        or _DIGEST.fullmatch(value["release_sha256"]) is None
        or value["release_sha256"] == "0" * 64
        or type(value.get("manager_ceiling")) is not int
        or value.get("manager_ceiling") != 0
        or value.get("worker_available") is not False
        or not isinstance(value.get("components"), list)
        or not value["components"]
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")

    component_names: list[str] = []
    component_observed: dict[str, int] = {}
    for component in value["components"]:
        if (
            not isinstance(component, dict)
            or set(component) != {"name", "observed", "ready"}
            or not isinstance(component.get("name"), str)
            or not component["name"]
            or type(component.get("observed")) is not int
            or component["observed"] < 0
            or component.get("ready") is not True
        ):
            raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
        component_names.append(component["name"])
        component_observed[component["name"]] = component["observed"]
    observed_components = tuple(component_names)
    if web_expected is None:
        component_shape_valid = observed_components in {
            base_components,
            schema_three_components,
        }
    else:
        expected_components = schema_three_components if web_expected else base_components
        component_shape_valid = observed_components == expected_components
    if (
        not component_shape_valid
        or component_observed["cluster-resources"] <= 0
        or component_observed["manager"] != 1
        or component_observed["namespaced-resources"] <= 0
        or component_observed["namespaces"] != 1
        or component_observed["personal-workers"] != 0
        or component_observed["runtime-class"] != 1
        or ("web" in component_observed and component_observed["web"] != 1)
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")


class _UniqueKeySafeLoader(yaml.SafeLoader):  # type: ignore[misc]
    def construct_mapping(self, node: Any, deep: bool = False) -> dict[Any, Any]:
        self.flatten_mapping(node)
        mapping: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                if key in mapping:
                    raise ValueError("duplicate YAML mapping key")
                mapping[key] = self.construct_object(value_node, deep=deep)
            except TypeError:
                raise ValueError("unhashable YAML mapping key") from None
        return mapping


def validate_personal_dev_rollback_shadow_manifest(
    path: Path,
    expected_sha256: str,
    *,
    expected_input_sha256: str,
    expected_release_sha256: str,
) -> None:
    """Bind one exact shadow manifest to its observed render and release digests."""

    try:
        _validated_digest(expected_sha256)
        _validated_digest(expected_input_sha256)
        _validated_digest(expected_release_sha256)
        payload = _read_owner_only(path)
        if not hmac.compare_digest(
            hashlib.sha256(payload).hexdigest(),
            expected_sha256,
        ):
            raise ValueError
        documents = list(yaml.load_all(payload, Loader=_UniqueKeySafeLoader))
        if not documents:
            raise ValueError
        for document in documents:
            if not isinstance(document, dict):
                raise ValueError
            metadata = document.get("metadata")
            annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
            if (
                not isinstance(annotations, dict)
                or annotations.get("loom.dev/render-input-sha256") != expected_input_sha256
                or annotations.get("loom.dev/trusted-release-sha256") != expected_release_sha256
            ):
                raise ValueError
    except (OSError, RecursionError, TypeError, UnicodeError, ValueError, yaml.YAMLError):
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None


def load_personal_dev_rollback_shadow_status(
    path: Path,
    expected_sha256: str,
) -> dict[str, Any]:
    """Load canonical owner-only evidence for the final inert shadow state."""

    _, value = _load_json(path, expected_sha256)
    _validate_shadow_status(value)
    return value


def _validate_storage_inventory(
    value: Any,
    *,
    release: PersonalDevTrustedRelease,
) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    items = value["items"]
    pvcs = {
        item.get("metadata", {}).get("name"): item
        for item in items
        if isinstance(item, dict) and item.get("kind") == "PersistentVolumeClaim"
    }
    statefulsets = {
        item.get("metadata", {}).get("name"): item
        for item in items
        if isinstance(item, dict) and item.get("kind") == "StatefulSet"
    }
    if set(pvcs) != {"data-loom-dev-postgres-0", "data-loom-dev-minio-0"} or any(
        item.get("spec", {}).get("storageClassName") != "longhorn" for item in pvcs.values()
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")

    def image(name: str, container_name: str) -> str | None:
        item = statefulsets.get(name, {})
        containers = item.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        if not isinstance(containers, list):
            return None
        matches = [
            container.get("image")
            for container in containers
            if isinstance(container, dict) and container.get("name") == container_name
        ]
        return matches[0] if len(matches) == 1 and isinstance(matches[0], str) else None

    if (
        set(statefulsets) != {"loom-dev-postgres", "loom-dev-minio"}
        or image("loom-dev-postgres", "postgres") != release.images.postgres
        or image("loom-dev-minio", "minio") != release.images.minio
        or image("loom-dev-minio", "admin") != release.images.minio_client
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")


def build_personal_dev_backup_restore_evidence(
    *,
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    release_sha256: str,
    started_at: str,
    completed_at: str,
    postgres_dump_path: Path,
    postgres_source_state_path: Path,
    postgres_restored_state_path: Path,
    source_schema_head: str,
    restored_schema_head: str,
    minio_source_manifest_path: Path,
    minio_restored_manifest_path: Path,
    minio_payload_root: Path,
    secret_key_inventory_path: Path,
    pre_shadow_status_path: Path,
    post_shadow_status_path: Path,
    storage_inventory_path: Path,
) -> dict[str, object]:
    """Derive a canonical backup/restore record from exact supporting evidence."""

    if _DIGEST.fullmatch(release_sha256) is None or release_sha256 == "0" * 64:
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    source_state = _read_owner_only(postgres_source_state_path)
    restored_state = _read_owner_only(postgres_restored_state_path)
    _validate_postgres_state(source_state)
    _validate_postgres_state(restored_state)
    if not hmac.compare_digest(source_state, restored_state):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    try:
        source_minio_manifest = load_personal_dev_minio_manifest(minio_source_manifest_path)
        restored_minio_manifest = load_personal_dev_minio_manifest(minio_restored_manifest_path)
        retained_inventory = validate_personal_dev_minio_payload_root(
            source_minio_manifest,
            minio_payload_root,
        )
    except ValueError:
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    source_manifest = source_minio_manifest.canonical_bytes
    restored_manifest = restored_minio_manifest.canonical_bytes
    object_count = source_minio_manifest.object_count
    if (
        restored_minio_manifest.object_count != object_count
        or not hmac.compare_digest(source_manifest, restored_manifest)
        or retained_inventory != source_minio_manifest.payload_inventory_bytes
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")

    retained_payloads = {
        object.payload_sha256: object.size_bytes for object in source_minio_manifest.objects
    }

    secret_payload, secret_value = _parse_json_document(secret_key_inventory_path, canonical=False)
    expected_secret_inventory = {
        "items": [
            {"keys": list(_MANAGEMENT_SECRET_KEYS), "name": profile.identities.management_secret},
            {"keys": ["private-key"], "name": profile.identities.activation_private_secret},
            {"keys": ["public-key"], "name": profile.identities.activation_public_secret},
        ]
    }
    expected_secret_inventory["items"].sort(key=lambda item: str(item["name"]))
    if secret_value != expected_secret_inventory:
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")

    _, pre_status = _parse_json_document(pre_shadow_status_path, canonical=False)
    _, post_status = _parse_json_document(post_shadow_status_path, canonical=False)
    web_expected = release.images.loom_web is not None
    _validate_shadow_status(pre_status, web_expected=web_expected)
    _validate_shadow_status(post_status, web_expected=web_expected)
    _, storage_inventory = _parse_json_document(storage_inventory_path, canonical=False)
    _validate_storage_inventory(storage_inventory, release=release)

    state_sha256 = hashlib.sha256(source_state).hexdigest()
    manifest_sha256 = hashlib.sha256(source_manifest).hexdigest()
    value: dict[str, object] = {
        "cleanup": {
            "isolated_minio_absent": True,
            "isolated_network_absent": True,
            "isolated_postgres_absent": True,
        },
        "completed_at": completed_at,
        "manager": {
            "executable_new_capacity_ceiling": 0,
            "personal_worker_count": 0,
        },
        "minio": {
            "backup_manifest_sha256": manifest_sha256,
            "image": release.images.minio,
            "restored_manifest_sha256": manifest_sha256,
            "restored_object_count": object_count,
            "source_object_count": object_count,
        },
        "namespace": "loom-dev",
        "postgres": {
            "dump_sha256": _sha256_owner_only_file(postgres_dump_path),
            "image": release.images.postgres,
            "restored_schema_head": restored_schema_head,
            "restored_state_sha256": state_sha256,
            "source_schema_head": source_schema_head,
            "source_state_sha256": state_sha256,
        },
        "release_sha256": release_sha256,
        "schema": (
            "loom-personal-dev-backup-restore-evidence-v1"
            if object_count == 0
            else "loom-personal-dev-backup-restore-evidence-v2"
        ),
        "secrets": {
            "key_inventory_sha256": hashlib.sha256(secret_payload).hexdigest(),
            "values_included": False,
        },
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "started_at": started_at,
        "storage": {
            "minio_pvc": "data-loom-dev-minio-0",
            "postgres_pvc": "data-loom-dev-postgres-0",
            "storage_class": "longhorn",
        },
    }
    if object_count != 0:
        value["minio"] = {
            **value["minio"],  # type: ignore[dict-item]
            "retained_payload_inventory_sha256": hashlib.sha256(retained_inventory).hexdigest(),
            "retained_payload_count": len(retained_payloads),
            "retained_payload_bytes": sum(retained_payloads.values()),
        }
    try:
        parsed = PersonalDevBackupRestoreEvidence.model_validate(value)
    except ValueError:
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    return parsed.model_dump(mode="json", by_alias=True)


def load_personal_dev_backup_restore_evidence(
    path: Path,
    *,
    expected_sha256: str,
    release: PersonalDevTrustedRelease,
    release_sha256: str,
    expected_schema_head: str,
) -> PersonalDevBackupRestoreEvidence:
    """Load and semantically validate one canonical backup/restore drill record."""

    payload, value = _load_json(path, expected_sha256)
    try:
        parsed = PersonalDevBackupRestoreEvidence.model_validate(value)
    except ValueError:
        raise PersonalDevAcceptanceEvidenceError(
            "personal-dev acceptance evidence is invalid"
        ) from None
    if (
        parsed.source.commit != release.source_sha
        or parsed.source.tree != release.source_tree
        or parsed.release_sha256 != release_sha256
        or parsed.postgres.image != release.images.postgres
        or parsed.minio.image != release.images.minio
        or parsed.postgres.source_schema_head != expected_schema_head
        or _canonical_json(parsed.model_dump(mode="json", by_alias=True)) != payload
    ):
        raise PersonalDevAcceptanceEvidenceError("personal-dev acceptance evidence is invalid")
    return parsed


__all__ = [
    "PersonalDevAcceptanceEvidenceError",
    "PersonalDevAcceptanceResultV2",
    "PersonalDevBackupRestoreEvidence",
    "build_personal_dev_backup_restore_evidence",
    "build_personal_dev_scanner_finding_policy",
    "build_personal_dev_trusted_launcher_profile",
    "load_personal_dev_acceptance_result",
    "load_personal_dev_backup_restore_evidence",
    "validate_personal_dev_policy_evidence",
    "validate_personal_dev_rollback_shadow_manifest",
]
