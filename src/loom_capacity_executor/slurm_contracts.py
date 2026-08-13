"""Strict scheduler-only values for the executable Slurm boundary."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_SLURM_QUANTITY = (1 << 63) - 1
MAX_SLURM_NODES = 128
MAX_SLURM_FEATURES = 64
MAX_GENERIC_TRES = 64
MAX_ACCOUNTING_RECORDS = 10_000
MEBIBYTE = 1024 * 1024

_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
_HOST_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}[A-Za-z0-9]$"
_JOB_ID_PATTERN = r"^[1-9][0-9]{0,19}$"
_DIGEST_PATTERN = r"^[0-9a-f]{64}$"
_JOB_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
_OWNERSHIP_PATTERN = r"^[A-Za-z0-9_-]{43,4096}$"
_IMAGE_DIGEST_PATTERN = (
    r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
)
_NODE_PATTERN = re.compile(_IDENTIFIER_PATTERN)
_FEATURE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")

SlurmIdentifier = Annotated[str, Field(min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)]
SlurmControllerHost = Annotated[str, Field(min_length=3, max_length=254, pattern=_HOST_PATTERN)]
SlurmJobId = Annotated[str, Field(pattern=_JOB_ID_PATTERN)]
Sha256Digest = Annotated[str, Field(pattern=_DIGEST_PATTERN)]
SlurmQuantity = Annotated[int, Field(ge=0, le=MAX_SLURM_QUANTITY)]
PositiveSlurmQuantity = Annotated[int, Field(gt=0, le=MAX_SLURM_QUANTITY)]
OwnershipToken = Annotated[str, Field(pattern=_OWNERSHIP_PATTERN)]

SlurmJobState = Literal[
    "PENDING",
    "CONFIGURING",
    "RUNNING",
    "COMPLETING",
    "SUSPENDED",
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
]
SlurmTerminalState = Literal[
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
]


class StrictSlurmV2Model(BaseModel):
    """Frozen, coercion-free base for scheduler boundary values."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[2] = 2


class SlurmExecutableIdentityV2(StrictSlurmV2Model):
    """Absolute executable path bound to owner and content digest."""

    path: Annotated[str, Field(min_length=1, max_length=4096)]
    sha256: Sha256Digest
    owner_uid: Annotated[int, Field(ge=0, le=(1 << 31) - 1)]

    @field_validator("path")
    @classmethod
    def _absolute_path(cls, value: str) -> str:
        if "\0" in value or not Path(value).is_absolute():
            raise ValueError("Slurm executable path must be absolute")
        return value


class SlurmExecutablesV2(StrictSlurmV2Model):
    scontrol: SlurmExecutableIdentityV2
    sacctmgr: SlurmExecutableIdentityV2
    squeue: SlurmExecutableIdentityV2
    sbatch: SlurmExecutableIdentityV2
    scancel: SlurmExecutableIdentityV2
    sacct: SlurmExecutableIdentityV2


class SlurmTresValueV2(StrictSlurmV2Model):
    name: Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            pattern=r"^[a-z][a-z0-9_.-]*(?:/[a-z0-9_.-]+(?::[a-z0-9_.-]+)?)?$",
        ),
    ]
    value: PositiveSlurmQuantity

    @field_validator("name")
    @classmethod
    def _generic_name(cls, value: str) -> str:
        if value in {"billing", "cpu", "mem", "node", "gres/gpu"}:
            raise ValueError("reserved Slurm TRES name")
        if not value.startswith("gres/"):
            raise ValueError("generic Slurm TRES must name a GRES")
        return value


def _canonical_tres(
    value: tuple[SlurmTresValueV2, ...],
) -> tuple[SlurmTresValueV2, ...]:
    names = [item.name for item in value]
    if len(names) != len(set(names)):
        raise ValueError("duplicate Slurm TRES name")
    return tuple(sorted(value, key=lambda item: item.name))


def _typed_gpu_total(value: tuple[SlurmTresValueV2, ...]) -> int | None:
    typed = tuple(item for item in value if item.name.startswith("gres/gpu:"))
    return sum(item.value for item in typed) if typed else None


class SlurmResourceV2(StrictSlurmV2Model):
    cpus: Annotated[int, Field(ge=0, le=65_536)] = 0
    memory_bytes: SlurmQuantity = 0
    gpus: Annotated[int, Field(ge=0, le=1_024)] = 0
    generic_tres: Annotated[tuple[SlurmTresValueV2, ...], Field(max_length=MAX_GENERIC_TRES)] = ()

    @field_validator("generic_tres")
    @classmethod
    def _unique_tres(cls, value: tuple[SlurmTresValueV2, ...]) -> tuple[SlurmTresValueV2, ...]:
        return _canonical_tres(value)

    @model_validator(mode="after")
    def _typed_gpu_ceiling(self) -> SlurmResourceV2:
        typed_total = _typed_gpu_total(self.generic_tres)
        if typed_total is not None and typed_total > self.gpus:
            raise ValueError("typed GPU TRES exceeds aggregate GPU ceiling")
        return self


class SlurmAuthorityV2(StrictSlurmV2Model):
    """Expected local process, controller, association, and command authority."""

    cluster: SlurmIdentifier
    controller_host: SlurmControllerHost
    partition: SlurmIdentifier
    account: SlurmIdentifier
    submitter: SlurmIdentifier
    qos: SlurmIdentifier
    local_uid: Annotated[int, Field(ge=0, le=(1 << 31) - 1)]
    executables: SlurmExecutablesV2
    resource_ceiling: SlurmResourceV2
    command_timeout_seconds: Annotated[float, Field(ge=0.05, le=60.0)] = 10.0
    max_stdout_bytes: Annotated[int, Field(ge=128, le=4 * 1024 * 1024)] = 1024 * 1024
    max_stderr_bytes: Annotated[int, Field(ge=128, le=1024 * 1024)] = 256 * 1024


def _canonical_nodes(value: tuple[str, ...], *, allow_empty: bool) -> tuple[str, ...]:
    if not allow_empty and not value:
        raise ValueError("Slurm node set must not be empty")
    if len(value) > MAX_SLURM_NODES:
        raise ValueError("Slurm node set exceeds its limit")
    if any(_NODE_PATTERN.fullmatch(node) is None for node in value):
        raise ValueError("Slurm node name is invalid")
    if len(value) != len(set(value)):
        raise ValueError("duplicate Slurm node")
    return tuple(sorted(value))


class SlurmLaunchRequestV2(StrictSlurmV2Model):
    """One exact trusted-wrapper launch; no candidate text or script body exists."""

    cluster: SlurmIdentifier
    controller_host: SlurmControllerHost
    partition: SlurmIdentifier
    account: SlurmIdentifier
    submitter: SlurmIdentifier
    qos: SlurmIdentifier
    job_name: Annotated[str, Field(pattern=_JOB_NAME_PATTERN)]
    operation_id: UUID
    nodes: Annotated[tuple[str, ...], Field(min_length=1, max_length=MAX_SLURM_NODES)]
    features: Annotated[tuple[str, ...], Field(max_length=MAX_SLURM_FEATURES)] = ()
    cpus: Annotated[int, Field(gt=0, le=65_536)]
    memory_bytes: PositiveSlurmQuantity
    gpus: Annotated[int, Field(ge=0, le=1_024)] = 0
    generic_tres: Annotated[tuple[SlurmTresValueV2, ...], Field(max_length=MAX_GENERIC_TRES)] = ()
    time_limit_seconds: Annotated[int, Field(gt=0, le=7 * 24 * 60 * 60)]
    launcher: SlurmExecutableIdentityV2
    launcher_release_sha256: Sha256Digest
    image_digest: Annotated[str, Field(max_length=512, pattern=_IMAGE_DIGEST_PATTERN)]
    ownership_token: OwnershipToken

    @field_validator("nodes")
    @classmethod
    def _nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_nodes(value, allow_empty=False)

    @field_validator("features")
    @classmethod
    def _features(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(_FEATURE_PATTERN.fullmatch(feature) is None for feature in value):
            raise ValueError("Slurm feature is invalid")
        if len(value) != len(set(value)):
            raise ValueError("duplicate Slurm feature")
        return tuple(sorted(value))

    @field_validator("memory_bytes")
    @classmethod
    def _whole_mebibytes(cls, value: int) -> int:
        if value % MEBIBYTE:
            raise ValueError("Slurm memory must be an exact whole MiB")
        return value

    @field_validator("generic_tres")
    @classmethod
    def _unique_tres(cls, value: tuple[SlurmTresValueV2, ...]) -> tuple[SlurmTresValueV2, ...]:
        return _canonical_tres(value)

    @model_validator(mode="after")
    def _typed_gpu_request(self) -> SlurmLaunchRequestV2:
        typed_total = _typed_gpu_total(self.generic_tres)
        if typed_total is not None and typed_total != self.gpus:
            raise ValueError("typed GPU TRES must equal aggregate GPU request")
        return self

    def trusted_launcher_argv(self) -> tuple[str, ...]:
        """Render only typed, digest-bound arguments for the trusted wrapper."""

        return (
            self.launcher.path,
            f"--launcher-sha256={self.launcher.sha256}",
            f"--operation-id={self.operation_id}",
            f"--image-digest={self.image_digest}",
            f"--release-sha256={self.launcher_release_sha256}",
            f"--ownership-token={self.ownership_token}",
        )


class SlurmSubmissionV2(StrictSlurmV2Model):
    cluster: SlurmIdentifier
    job_id: SlurmJobId


class SlurmCancelRequestV2(StrictSlurmV2Model):
    cluster: SlurmIdentifier
    job_id: SlurmJobId
    submitter: SlurmIdentifier
    account: SlurmIdentifier


class SlurmJobObservationV2(StrictSlurmV2Model):
    cluster: SlurmIdentifier
    job_id: SlurmJobId
    state: SlurmJobState
    submitter: SlurmIdentifier
    account: SlurmIdentifier
    partition: SlurmIdentifier
    cpus: Annotated[int, Field(gt=0, le=65_536)]
    memory_bytes: PositiveSlurmQuantity
    gpus: Annotated[int, Field(ge=0, le=1_024)]
    generic_tres: Annotated[tuple[SlurmTresValueV2, ...], Field(max_length=MAX_GENERIC_TRES)] = ()
    nodes: Annotated[tuple[str, ...], Field(max_length=MAX_SLURM_NODES)]
    pending_reason: Annotated[str | None, Field(max_length=256)] = None
    ownership_token: OwnershipToken

    @field_validator("nodes")
    @classmethod
    def _nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_nodes(value, allow_empty=True)

    @field_validator("generic_tres")
    @classmethod
    def _unique_tres(cls, value: tuple[SlurmTresValueV2, ...]) -> tuple[SlurmTresValueV2, ...]:
        return _canonical_tres(value)

    @model_validator(mode="after")
    def _pending_reason_state(self) -> SlurmJobObservationV2:
        if self.pending_reason is not None and self.state != "PENDING":
            raise ValueError("pending reason requires PENDING state")
        typed_total = _typed_gpu_total(self.generic_tres)
        if typed_total is not None and typed_total != self.gpus:
            raise ValueError("typed GPU TRES conflicts with aggregate GPU observation")
        return self


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Slurm timestamp must include an offset")
    return value.astimezone(UTC)


class SlurmTerminalEvidenceV2(StrictSlurmV2Model):
    cluster: SlurmIdentifier
    job_id: SlurmJobId
    state: SlurmTerminalState
    submitter: SlurmIdentifier
    account: SlurmIdentifier
    submitted_at: datetime
    started_at: datetime | None
    ended_at: datetime
    elapsed_seconds: SlurmQuantity
    exit_code: Annotated[str, Field(pattern=r"^[0-9]{1,3}:[0-9]{1,3}$")]
    cpus: Annotated[int, Field(gt=0, le=65_536)]
    memory_bytes: PositiveSlurmQuantity
    gpus: Annotated[int, Field(ge=0, le=1_024)]
    generic_tres: Annotated[tuple[SlurmTresValueV2, ...], Field(max_length=MAX_GENERIC_TRES)] = ()
    nodes: Annotated[tuple[str, ...], Field(max_length=MAX_SLURM_NODES)]
    ownership_token: OwnershipToken

    @field_validator("submitted_at", "started_at", "ended_at")
    @classmethod
    def _timestamps(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc(value)

    @field_validator("nodes")
    @classmethod
    def _nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_nodes(value, allow_empty=True)

    @field_validator("generic_tres")
    @classmethod
    def _unique_tres(cls, value: tuple[SlurmTresValueV2, ...]) -> tuple[SlurmTresValueV2, ...]:
        return _canonical_tres(value)

    @model_validator(mode="after")
    def _ordered_times(self) -> SlurmTerminalEvidenceV2:
        if self.started_at is not None and self.started_at < self.submitted_at:
            raise ValueError("Slurm start precedes submission")
        if self.ended_at < (self.started_at or self.submitted_at):
            raise ValueError("Slurm end precedes start")
        typed_total = _typed_gpu_total(self.generic_tres)
        if typed_total is not None and typed_total != self.gpus:
            raise ValueError("typed GPU TRES conflicts with aggregate terminal GPU evidence")
        return self


class SlurmAccountingHighWaterV2(StrictSlurmV2Model):
    cluster: SlurmIdentifier
    account: SlurmIdentifier
    submitter: SlurmIdentifier
    since: datetime
    observed_through: datetime
    terminal_jobs: Annotated[
        tuple[SlurmTerminalEvidenceV2, ...], Field(max_length=MAX_ACCOUNTING_RECORDS)
    ]

    @field_validator("since", "observed_through")
    @classmethod
    def _timestamps(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("terminal_jobs")
    @classmethod
    def _unique_jobs(
        cls, value: tuple[SlurmTerminalEvidenceV2, ...]
    ) -> tuple[SlurmTerminalEvidenceV2, ...]:
        job_ids = [item.job_id for item in value]
        if len(job_ids) != len(set(job_ids)):
            raise ValueError("duplicate Slurm terminal job")
        return tuple(sorted(value, key=lambda item: int(item.job_id)))

    @model_validator(mode="after")
    def _bounded_window(self) -> SlurmAccountingHighWaterV2:
        if self.observed_through < self.since:
            raise ValueError("Slurm accounting high-water precedes its lower bound")
        return self


def strict_datetime(value: str) -> datetime:
    """Parse one ISO-8601 scheduler timestamp without accepting naive values."""

    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError("Slurm timestamp is invalid")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return _utc(parsed)


__all__ = [
    "MAX_ACCOUNTING_RECORDS",
    "MAX_GENERIC_TRES",
    "MEBIBYTE",
    "SlurmAccountingHighWaterV2",
    "SlurmAuthorityV2",
    "SlurmCancelRequestV2",
    "SlurmExecutableIdentityV2",
    "SlurmExecutablesV2",
    "SlurmJobObservationV2",
    "SlurmLaunchRequestV2",
    "SlurmResourceV2",
    "SlurmSubmissionV2",
    "SlurmTerminalEvidenceV2",
    "SlurmTresValueV2",
    "strict_datetime",
]
