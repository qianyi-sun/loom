"""Canonical contracts for task-image build projection and attestation."""

from __future__ import annotations

import hashlib
import re
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from uuid import UUID

import rfc8785
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

MAX_SIGNED_BIGINT = (1 << 63) - 1
MAX_CONTRACT_BYTES = 64 * 1024
MAX_GRANT_LIFETIME = timedelta(hours=4)
MAX_CHALLENGE_LIFETIME = timedelta(seconds=60)
MAX_ATTESTATION_LIFETIME = timedelta(seconds=60)
MAX_SESSION_LIFETIME = timedelta(minutes=15)

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")

CpuArchitecture = Literal["x86_64", "arm64"]
SlurmClusterId = Literal["oldlab", "gb10"]
BuildPurpose = Literal["production", "shadow"]
GuardScope = Literal["task-image:project", "task-image:attest"]
PositiveSignedBigint = Annotated[int, Field(gt=0, le=MAX_SIGNED_BIGINT)]
Identifier = Annotated[
    str, Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
]
NodeName = Annotated[
    str, Field(min_length=1, max_length=253, pattern=r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")
]
SlurmJobId = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,31}$")]


def _nonzero_uuid(value: UUID) -> UUID:
    if value.int == 0:
        raise ValueError("authority UUID must be nonzero")
    return value


def _nonzero_digest(value: str) -> str:
    if _DIGEST_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError("authority digest must be a nonzero lowercase SHA-256")
    return value


def _safe_cgroup_path(value: str) -> str:
    if (
        not value.startswith("/sys/fs/cgroup/")
        or "//" in value
        or "\x00" in value
        or PurePosixPath(value).as_posix() != value
        or any(part in {"", ".", ".."} for part in PurePosixPath(value).parts[1:])
    ):
        raise ValueError("authority cgroup path is unsafe")
    return value


NonzeroUUID = Annotated[UUID, AfterValidator(_nonzero_uuid)]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$"), AfterValidator(_nonzero_digest)]
CgroupPath = Annotated[
    str,
    Field(min_length=16, max_length=4096),
    AfterValidator(_safe_cgroup_path),
]


def _parse_timestamp(value: str) -> datetime | str:
    candidate = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return value


class StrictTaskImageAuthorityModel(BaseModel):
    """Frozen strict base for persisted task-image authority documents."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1

    @model_validator(mode="before")
    @classmethod
    def _restore_json_types(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        for name, item in normalized.items():
            if isinstance(item, list):
                normalized[name] = tuple(item)
            elif isinstance(item, str) and (
                name.endswith("_at") or name.endswith("_expires_at")
            ):
                normalized[name] = _parse_timestamp(item)
        return normalized

    @field_validator("*", mode="after")
    @classmethod
    def _canonicalize_timestamps(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("authority timestamp must include a timezone")
            return value.astimezone(UTC)
        return value


def _validate_native_pair(cluster: SlurmClusterId, architecture: CpuArchitecture) -> None:
    if (cluster, architecture) not in {("oldlab", "x86_64"), ("gb10", "arm64")}:
        raise ValueError("authority Slurm cluster and architecture are not native")


def _validate_interval(
    issued_at: datetime,
    expires_at: datetime,
    *,
    maximum: timedelta,
    label: str,
) -> None:
    lifetime = expires_at - issued_at
    if lifetime <= timedelta(0) or lifetime > maximum:
        raise ValueError(f"authority {label} lifetime is invalid")


class TaskImageBuildGrantAuthorityV1(StrictTaskImageAuthorityModel):
    purpose: BuildPurpose
    shadow_campaign_id: NonzeroUUID | None
    environment: Identifier
    pool_id: Identifier
    slurm_cluster_id: SlurmClusterId
    cpu_arch: CpuArchitecture
    slurm_request_sha256: Digest
    builder_release_sha256: Digest
    build_policy_sha256: Digest
    containment_policy_sha256: Digest
    resource_profile_sha256: Digest
    issued_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def _authority_is_exact(self) -> TaskImageBuildGrantAuthorityV1:
        if (self.purpose == "production") != (self.shadow_campaign_id is None):
            raise ValueError("authority purpose and shadow campaign disagree")
        _validate_native_pair(self.slurm_cluster_id, self.cpu_arch)
        _validate_interval(
            self.issued_at,
            self.expires_at,
            maximum=MAX_GRANT_LIFETIME,
            label="grant",
        )
        return self


class TaskImageGuardPrincipalV1(StrictTaskImageAuthorityModel):
    principal_id: Identifier
    slurm_cluster_id: SlurmClusterId
    node_name: NodeName
    scopes: Annotated[tuple[GuardScope, ...], Field(min_length=1, max_length=2)]

    @field_validator("scopes")
    @classmethod
    def _canonical_scopes(cls, value: tuple[GuardScope, ...]) -> tuple[GuardScope, ...]:
        if len(value) != len(set(value)):
            raise ValueError("authority principal has duplicate scopes")
        return tuple(sorted(value))


class TaskImageProjectionRequestV1(StrictTaskImageAuthorityModel):
    request_id: NonzeroUUID
    grant_id: NonzeroUUID
    observed_at: datetime
    node_name: NodeName
    node_boot_id: NonzeroUUID
    slurm_cluster_id: SlurmClusterId
    slurm_job_id: SlurmJobId
    supervisor_pid: PositiveSignedBigint
    supervisor_uid: PositiveSignedBigint
    supervisor_gid: PositiveSignedBigint
    supervisor_executable_sha256: Digest
    cgroup_path: CgroupPath
    cgroup_inode: PositiveSignedBigint
    submitting_identity: Literal["loom-builder"]
    slurm_account: Literal["loom-task-builder"]
    slurm_partition: Literal["loom-task-builder"]
    slurm_qos: Literal[
        "loom-task-image-builder-rootless-oldlab",
        "loom-task-image-builder-rootless-gb10",
    ]
    cpu_arch: CpuArchitecture
    slurm_request_sha256: Digest

    @model_validator(mode="after")
    def _request_is_native(self) -> TaskImageProjectionRequestV1:
        _validate_native_pair(self.slurm_cluster_id, self.cpu_arch)
        expected_qos = f"loom-task-image-builder-rootless-{self.slurm_cluster_id}"
        if self.slurm_qos != expected_qos:
            raise ValueError("authority Slurm QoS and cluster disagree")
        return self


def _canonical_ids(value: tuple[int, ...]) -> tuple[int, ...]:
    if len(value) != len(set(value)):
        raise ValueError("authority kernel IDs contain duplicates")
    if tuple(sorted(value)) != value:
        raise ValueError("authority kernel IDs are not in canonical order")
    return value


class TaskImageContainmentAttachmentV1(StrictTaskImageAuthorityModel):
    cgroup_inode: PositiveSignedBigint
    containment_root: CgroupPath
    trusted_service_cgroup: CgroupPath
    build_egress_cgroup: CgroupPath
    bpf_program_sha256: Digest
    bpf_map_schema_sha256: Digest
    containment_policy_sha256: Digest
    resource_limits_sha256: Digest
    probe_sha256: Digest
    link_ids: Annotated[tuple[PositiveSignedBigint, ...], Field(min_length=1, max_length=32)]
    program_ids: Annotated[
        tuple[PositiveSignedBigint, ...], Field(min_length=1, max_length=32)
    ]
    map_ids: Annotated[tuple[PositiveSignedBigint, ...], Field(min_length=1, max_length=32)]

    @field_validator("link_ids", "program_ids", "map_ids")
    @classmethod
    def _ids_are_canonical(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        return _canonical_ids(value)

    @model_validator(mode="after")
    def _children_are_exact(self) -> TaskImageContainmentAttachmentV1:
        root = PurePosixPath(self.containment_root)
        if (
            root.name != "loom-builder"
            or PurePosixPath(self.trusted_service_cgroup).parent != root
            or PurePosixPath(self.trusted_service_cgroup).name != "trusted-service"
            or PurePosixPath(self.build_egress_cgroup).parent != root
            or PurePosixPath(self.build_egress_cgroup).name != "build-egress"
        ):
            raise ValueError("authority containment cgroup layout is invalid")
        return self


class TaskImageProjectionChallengeV1(StrictTaskImageAuthorityModel):
    request_id: NonzeroUUID
    grant_id: NonzeroUUID
    request_sha256: Digest
    challenge_nonce: NonzeroUUID
    containment_policy_sha256: Digest
    resource_profile_sha256: Digest
    issued_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def _bounded_challenge(self) -> TaskImageProjectionChallengeV1:
        _validate_interval(
            self.issued_at,
            self.expires_at,
            maximum=MAX_CHALLENGE_LIFETIME,
            label="challenge",
        )
        return self


def _attachment_is_below_request(
    *,
    cgroup_path: str,
    cgroup_inode: int,
    attachment: TaskImageContainmentAttachmentV1,
) -> None:
    if attachment.cgroup_inode != cgroup_inode:
        raise ValueError("authority attachment cgroup inode disagrees with request")
    if PurePosixPath(attachment.containment_root).parent != PurePosixPath(cgroup_path):
        raise ValueError("authority containment root is outside request cgroup")


class TaskImageAttachmentProofV1(StrictTaskImageAuthorityModel):
    proof_id: NonzeroUUID
    grant_id: NonzeroUUID
    request_id: NonzeroUUID
    request_sha256: Digest
    challenge_nonce: NonzeroUUID
    observed_at: datetime
    node_name: NodeName
    node_boot_id: NonzeroUUID
    slurm_cluster_id: SlurmClusterId
    slurm_job_id: SlurmJobId
    cgroup_path: CgroupPath
    cgroup_inode: PositiveSignedBigint
    attachment: TaskImageContainmentAttachmentV1
    attestation_generation: Literal[1]
    attestation_expires_at: datetime

    @model_validator(mode="after")
    def _proof_binds_initial_attestation(self) -> TaskImageAttachmentProofV1:
        _attachment_is_below_request(
            cgroup_path=self.cgroup_path,
            cgroup_inode=self.cgroup_inode,
            attachment=self.attachment,
        )
        _validate_interval(
            self.observed_at,
            self.attestation_expires_at,
            maximum=MAX_ATTESTATION_LIFETIME,
            label="attestation",
        )
        return self


class _SecretBearingAuthorityModel(StrictTaskImageAuthorityModel):
    pass


class TaskImageProjectionReceiptV1(_SecretBearingAuthorityModel):
    grant_id: NonzeroUUID
    proof_id: NonzeroUUID
    proof_sha256: Digest
    bootstrap_token: Annotated[str, Field(pattern=r"^loom_tibp_[A-Za-z0-9_-]{64,128}$")]
    issued_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def _bounded_bootstrap(self) -> TaskImageProjectionReceiptV1:
        _validate_interval(
            self.issued_at,
            self.expires_at,
            maximum=MAX_ATTESTATION_LIFETIME,
            label="bootstrap",
        )
        return self

    def public_binding(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json", exclude={"bootstrap_token"})
        payload["bootstrap_token_sha256"] = hashlib.sha256(
            self.bootstrap_token.encode("utf-8")
        ).hexdigest()
        return payload


class TaskImageBootstrapExchangeV1(_SecretBearingAuthorityModel):
    exchange_id: NonzeroUUID
    grant_id: NonzeroUUID
    proof_sha256: Digest
    bootstrap_token: Annotated[str, Field(pattern=r"^loom_tibp_[A-Za-z0-9_-]{64,128}$")]
    observed_at: datetime

    def public_binding(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json", exclude={"bootstrap_token"})
        payload["bootstrap_token_sha256"] = hashlib.sha256(
            self.bootstrap_token.encode("utf-8")
        ).hexdigest()
        return payload


class TaskImageProjectionRevocationV1(StrictTaskImageAuthorityModel):
    grant_id: NonzeroUUID
    reason: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")]
    observed_at: datetime


class TaskImageBuildSessionV1(_SecretBearingAuthorityModel):
    grant_id: NonzeroUUID
    session_id: NonzeroUUID
    purpose: BuildPurpose
    shadow_campaign_id: NonzeroUUID | None
    pool_id: Identifier
    cpu_arch: CpuArchitecture
    session_token: Annotated[str, Field(pattern=r"^loom_tibs_[A-Za-z0-9_-]{64,128}$")]
    attestation_generation: PositiveSignedBigint
    attestation_sha256: Digest
    issued_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def _session_is_bounded(self) -> TaskImageBuildSessionV1:
        if (self.purpose == "production") != (self.shadow_campaign_id is None):
            raise ValueError("authority session purpose and shadow campaign disagree")
        _validate_interval(
            self.issued_at,
            self.expires_at,
            maximum=MAX_SESSION_LIFETIME,
            label="session",
        )
        return self

    def public_binding(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json", exclude={"session_token"})
        payload["session_token_sha256"] = hashlib.sha256(
            self.session_token.encode("utf-8")
        ).hexdigest()
        return payload


class TaskImageContainmentAttestationV1(StrictTaskImageAuthorityModel):
    attestation_id: NonzeroUUID
    grant_id: NonzeroUUID
    generation: PositiveSignedBigint
    node_name: NodeName
    node_boot_id: NonzeroUUID
    slurm_cluster_id: SlurmClusterId
    slurm_job_id: SlurmJobId
    cgroup_path: CgroupPath
    cgroup_inode: PositiveSignedBigint
    attachment: TaskImageContainmentAttachmentV1
    issued_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def _attestation_is_exact(self) -> TaskImageContainmentAttestationV1:
        _attachment_is_below_request(
            cgroup_path=self.cgroup_path,
            cgroup_inode=self.cgroup_inode,
            attachment=self.attachment,
        )
        _validate_interval(
            self.issued_at,
            self.expires_at,
            maximum=MAX_ATTESTATION_LIFETIME,
            label="attestation",
        )
        return self


def canonical_authority_bytes(model: StrictTaskImageAuthorityModel) -> bytes:
    """Encode one nonsecret authority model as bounded RFC 8785 JSON."""

    if isinstance(model, _SecretBearingAuthorityModel):
        raise TypeError("secret-bearing authority models require public_binding()")
    if not isinstance(model, StrictTaskImageAuthorityModel):
        raise TypeError("authority canonicalization requires a strict authority model")
    payload = rfc8785.dumps(model.model_dump(mode="json", exclude_none=False))
    if len(payload) > MAX_CONTRACT_BYTES:
        raise ValueError("authority contract exceeds maximum byte size")
    return payload


def canonical_authority_sha256(model: StrictTaskImageAuthorityModel) -> str:
    return hashlib.sha256(canonical_authority_bytes(model)).hexdigest()


def canonical_public_binding_sha256(
    model: TaskImageProjectionReceiptV1
    | TaskImageBootstrapExchangeV1
    | TaskImageBuildSessionV1,
) -> str:
    """Hash the canonical nonsecret binding of a secret-bearing contract."""

    payload = rfc8785.dumps(model.public_binding())
    if len(payload) > MAX_CONTRACT_BYTES:
        raise ValueError("authority public binding exceeds maximum byte size")
    return hashlib.sha256(payload).hexdigest()


def new_bootstrap_token() -> str:
    return "loom_tibp_" + secrets.token_urlsafe(48)


def new_session_token() -> str:
    return "loom_tibs_" + secrets.token_urlsafe(48)


__all__ = [
    "MAX_ATTESTATION_LIFETIME",
    "MAX_CHALLENGE_LIFETIME",
    "MAX_CONTRACT_BYTES",
    "MAX_GRANT_LIFETIME",
    "MAX_SESSION_LIFETIME",
    "MAX_SIGNED_BIGINT",
    "BuildPurpose",
    "CgroupPath",
    "CpuArchitecture",
    "Digest",
    "GuardScope",
    "Identifier",
    "NodeName",
    "NonzeroUUID",
    "PositiveSignedBigint",
    "SlurmClusterId",
    "SlurmJobId",
    "StrictTaskImageAuthorityModel",
    "TaskImageAttachmentProofV1",
    "TaskImageBootstrapExchangeV1",
    "TaskImageBuildGrantAuthorityV1",
    "TaskImageBuildSessionV1",
    "TaskImageContainmentAttachmentV1",
    "TaskImageContainmentAttestationV1",
    "TaskImageGuardPrincipalV1",
    "TaskImageProjectionChallengeV1",
    "TaskImageProjectionReceiptV1",
    "TaskImageProjectionRequestV1",
    "TaskImageProjectionRevocationV1",
    "canonical_authority_bytes",
    "canonical_authority_sha256",
    "canonical_public_binding_sha256",
    "new_bootstrap_token",
    "new_session_token",
]
