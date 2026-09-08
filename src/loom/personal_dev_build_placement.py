"""Pure placement contracts for Slurm-contained personal-dev native builds.

Runtime profile certificates represented here are upstream-verified evidence. A
digest match is only a placement comparison and does not authenticate a node or
its report.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

_SIGNED_64_MAX = 2**63 - 1
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_OBSERVABLE_NODE_IDS = frozenset(f"trt-gb10-{node}" for node in range(1, 16))
_WORKER_NODE_IDS = frozenset(f"trt-gb10-{node}" for node in range(3, 16))
_ELIGIBLE_SLURM_STATES = frozenset(("IDLE", "MIXED"))


def _validate_uuid(value: UUID, *, label: str) -> None:
    if not isinstance(value, UUID) or value.int == 0:
        raise ValueError(f"native build {label} must be a nonzero UUID")


def _validate_digest(value: str, *, label: str) -> None:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f"native build {label} digest is invalid")
    if value == "0" * 64:
        raise ValueError(f"native build {label} digest is invalid")


def _validate_integer(
    value: int,
    *,
    label: str,
    minimum: int,
) -> None:
    if type(value) is not int or not minimum <= value <= _SIGNED_64_MAX:
        raise ValueError(f"native build {label} must be a signed 64-bit integer")


def _validate_bounded_string(value: str, *, label: str) -> None:
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise ValueError(f"native build {label} must be a nonempty string of at most 64 characters")


def _normalize_datetime(value: datetime, *, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"native build {label} must include a timezone")
    return value.astimezone(UTC)


def _validate_node_id(value: str, *, label: str, allowed: frozenset[str]) -> None:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"native build {label} node id is invalid")


def _is_within_age(
    observed_at: datetime,
    *,
    now: datetime,
    maximum_age_seconds: int,
) -> bool:
    if now < observed_at:
        return False
    age = now - observed_at
    age_seconds = age.days * 86_400 + age.seconds
    return (age_seconds, age.microseconds) <= (maximum_age_seconds, 0)


@dataclass(frozen=True, slots=True)
class NativeBuildPlacementPolicy:
    """Resource, freshness, and release requirements for native build nodes."""

    allowed_node_ids: tuple[str, ...]
    runtime_profile_sha256: str
    cpu_millicores: int
    memory_bytes: int
    minimum_disk_free_bytes: int
    minimum_free_inodes: int
    max_observation_age_seconds: int

    def __post_init__(self) -> None:
        if not isinstance(self.allowed_node_ids, tuple) or not self.allowed_node_ids:
            raise ValueError("native build policy node ids must be a nonempty tuple")
        for node_id in self.allowed_node_ids:
            _validate_node_id(node_id, label="policy", allowed=_WORKER_NODE_IDS)
        if len(set(self.allowed_node_ids)) != len(self.allowed_node_ids):
            raise ValueError("native build policy node ids must be unique")
        object.__setattr__(self, "allowed_node_ids", tuple(sorted(self.allowed_node_ids)))
        _validate_digest(self.runtime_profile_sha256, label="runtime profile")
        _validate_integer(self.cpu_millicores, label="CPU millicores", minimum=1)
        _validate_integer(self.memory_bytes, label="memory bytes", minimum=1)
        _validate_integer(
            self.minimum_disk_free_bytes,
            label="minimum free disk bytes",
            minimum=1,
        )
        _validate_integer(
            self.minimum_free_inodes,
            label="minimum free inodes",
            minimum=1,
        )
        _validate_integer(
            self.max_observation_age_seconds,
            label="maximum observation age seconds",
            minimum=1,
        )


@dataclass(frozen=True, slots=True)
class NativeBuildNodeObservation:
    """Upstream-reported resource and readiness evidence for one inventory node."""

    node_id: str
    boot_id: UUID
    observed_at: datetime
    architecture: str
    slurm_state: str
    reserved: bool
    kvm_available: bool
    available_cpu_millicores: int
    available_memory_bytes: int
    available_disk_bytes: int
    available_inodes: int
    certified_runtime_profile_sha256: str | None

    def __post_init__(self) -> None:
        _validate_node_id(self.node_id, label="observation", allowed=_OBSERVABLE_NODE_IDS)
        _validate_uuid(self.boot_id, label="node boot id")
        object.__setattr__(
            self,
            "observed_at",
            _normalize_datetime(self.observed_at, label="observation timestamp"),
        )
        _validate_bounded_string(self.architecture, label="architecture")
        _validate_bounded_string(self.slurm_state, label="Slurm state")
        if type(self.reserved) is not bool:
            raise ValueError("native build reserved status must be a boolean")
        if type(self.kvm_available) is not bool:
            raise ValueError("native build KVM status must be a boolean")
        _validate_integer(
            self.available_cpu_millicores,
            label="available CPU millicores",
            minimum=0,
        )
        _validate_integer(
            self.available_memory_bytes,
            label="available memory bytes",
            minimum=0,
        )
        _validate_integer(
            self.available_disk_bytes,
            label="available disk bytes",
            minimum=0,
        )
        _validate_integer(
            self.available_inodes,
            label="available inodes",
            minimum=0,
        )
        if self.certified_runtime_profile_sha256 is not None:
            _validate_digest(
                self.certified_runtime_profile_sha256,
                label="certified runtime profile",
            )


def eligible_native_build_nodes(
    policy: NativeBuildPlacementPolicy,
    observations: tuple[NativeBuildNodeObservation, ...],
    *,
    now: datetime,
) -> tuple[NativeBuildNodeObservation, ...]:
    """Return deterministic eligible nodes from upstream-verified observations.

    Runtime profile digest equality compares certification evidence already
    verified by the caller; this pure function does not authenticate reports.
    """

    if not isinstance(policy, NativeBuildPlacementPolicy):
        raise ValueError("native build placement policy is invalid")
    if not isinstance(observations, tuple) or len(observations) > 15:
        raise ValueError("native build observations must be a tuple of at most 15 items")
    if any(not isinstance(item, NativeBuildNodeObservation) for item in observations):
        raise ValueError("native build observations contain an invalid item")
    node_ids = tuple(item.node_id for item in observations)
    if len(set(node_ids)) != len(node_ids):
        raise ValueError("native build observations contain duplicate node identity")
    normalized_now = _normalize_datetime(now, label="now")

    eligible = (
        observation
        for observation in observations
        if observation.node_id in policy.allowed_node_ids
        and not observation.reserved
        and observation.architecture == "aarch64"
        and observation.slurm_state in _ELIGIBLE_SLURM_STATES
        and observation.kvm_available
        and observation.available_cpu_millicores >= policy.cpu_millicores
        and observation.available_memory_bytes >= policy.memory_bytes
        and observation.available_disk_bytes >= policy.minimum_disk_free_bytes
        and observation.available_inodes >= policy.minimum_free_inodes
        and observation.certified_runtime_profile_sha256 == policy.runtime_profile_sha256
        and _is_within_age(
            observation.observed_at,
            now=normalized_now,
            maximum_age_seconds=policy.max_observation_age_seconds,
        )
    )
    return tuple(sorted(eligible, key=lambda observation: observation.node_id))


@dataclass(frozen=True, slots=True)
class NativeBuildAllocationBinding:
    """Immutable manager, build attempt, Slurm job, and node identity binding."""

    manager_reservation_id: UUID
    candidate_id: UUID
    candidate_sha256: str
    attempt_id: UUID
    attempt_lease_epoch: int
    owner_user_id: UUID
    runtime_profile_sha256: str
    slurm_cluster: str
    slurm_job_id: str
    node_id: str
    node_boot_id: UUID

    def __post_init__(self) -> None:
        _validate_uuid(self.manager_reservation_id, label="manager reservation id")
        _validate_uuid(self.candidate_id, label="candidate id")
        _validate_digest(self.candidate_sha256, label="candidate")
        _validate_uuid(self.attempt_id, label="attempt id")
        _validate_integer(self.attempt_lease_epoch, label="attempt lease epoch", minimum=1)
        _validate_uuid(self.owner_user_id, label="owner user id")
        _validate_digest(self.runtime_profile_sha256, label="runtime profile")
        if self.slurm_cluster != "trt-gb10":
            raise ValueError("native build Slurm cluster is invalid")
        if (
            not isinstance(self.slurm_job_id, str)
            or not self.slurm_job_id.isascii()
            or not self.slurm_job_id.isdecimal()
            or self.slurm_job_id.startswith("0")
            or int(self.slurm_job_id) > _SIGNED_64_MAX
        ):
            raise ValueError("native build Slurm job id is invalid")
        _validate_node_id(self.node_id, label="allocation", allowed=_WORKER_NODE_IDS)
        _validate_uuid(self.node_boot_id, label="node boot id")


__all__ = [
    "NativeBuildAllocationBinding",
    "NativeBuildNodeObservation",
    "NativeBuildPlacementPolicy",
    "eligible_native_build_nodes",
]
