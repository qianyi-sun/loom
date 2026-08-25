"""Immutable scheduling policy for Daytona execution and local-first overflow.

The policy is intentionally data, not deployment configuration: every accepted
batch and child trial retains the exact authority, compatibility, resource,
price, runtime, retry, and budget inputs used by the scheduler.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.models.task import TaskConfig

DAYTONA_POLICY_VERSION = "loom.daytona-backend-policy.v1"
LEGACY_POLICY_DIGEST = "sha256:" + "0" * 64


class DaytonaResources(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cpu: int = Field(ge=1)
    memory_gib: int = Field(ge=1)
    disk_gib: int = Field(ge=1)


class DaytonaPriceSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    effective_at: datetime
    currency: Literal["USD"] = "USD"
    cpu_usd_per_hour: Decimal = Field(ge=0)
    memory_gib_usd_per_hour: Decimal = Field(ge=0)
    disk_gib_usd_per_hour: Decimal = Field(ge=0)

    @field_validator("effective_at")
    @classmethod
    def effective_at_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Daytona price effective_at must include a timezone")
        return value

    @model_validator(mode="after")
    def require_a_positive_rate(self) -> DaytonaPriceSnapshot:
        if (
            self.cpu_usd_per_hour
            + self.memory_gib_usd_per_hour
            + self.disk_gib_usd_per_hour
            <= 0
        ):
            raise ValueError("Daytona price snapshot must contain a positive rate")
        return self


class BackendPolicyRequest(BaseModel):
    """Operator-authorized Daytona policy supplied with batch creation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: Literal["explicit", "overflow"]
    allowed_backends: tuple[Literal["docker", "daytona"], ...]
    spillover_after_queue_seconds: int = Field(default=0, ge=0, le=86_400)
    daytona_resources: DaytonaResources
    daytona_price_snapshot: DaytonaPriceSnapshot
    max_cloud_cost_usd: Decimal = Field(gt=0)
    max_runtime_seconds: int = Field(gt=0, le=86_400)

    @model_validator(mode="after")
    def validate_backend_set(self) -> BackendPolicyRequest:
        allowed = tuple(dict.fromkeys(self.allowed_backends))
        if allowed != self.allowed_backends:
            raise ValueError("allowed_backends must not contain duplicates")
        expected = ("daytona",) if self.mode == "explicit" else ("docker", "daytona")
        if allowed != expected:
            raise ValueError(
                f"{self.mode} policy requires allowed_backends={list(expected)!r} in priority order"
            )
        if self.mode == "explicit" and self.spillover_after_queue_seconds != 0:
            raise ValueError("explicit Daytona policy cannot set a spillover delay")
        return self


class BackendPolicySnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["loom.daytona-backend-policy.v1"] = (
        "loom.daytona-backend-policy.v1"
    )
    mode: Literal["local_only", "explicit", "overflow"]
    allowed_backends: tuple[Literal["docker", "daytona"], ...]
    spillover_after_queue_seconds: int = Field(ge=0, le=86_400)
    daytona_resources: DaytonaResources | None = None
    daytona_price_snapshot: DaytonaPriceSnapshot | None = None
    max_cloud_cost_usd: Decimal | None = Field(default=None, gt=0)
    max_runtime_seconds: int | None = Field(default=None, gt=0, le=86_400)
    max_attempts: int = Field(ge=1)
    expected_trial_count: int = Field(ge=1)
    worst_case_cloud_cost_usd: Decimal | None = Field(default=None, ge=0)
    authority: dict[str, Any] = Field(min_length=1)
    accepted_at: datetime

    @field_validator("accepted_at")
    @classmethod
    def accepted_at_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("backend policy accepted_at must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_complete_cloud_snapshot(self) -> BackendPolicySnapshot:
        cloud_values = (
            self.daytona_resources,
            self.daytona_price_snapshot,
            self.max_cloud_cost_usd,
            self.max_runtime_seconds,
            self.worst_case_cloud_cost_usd,
        )
        if self.mode == "local_only":
            if self.allowed_backends != ("docker",) or any(v is not None for v in cloud_values):
                raise ValueError("local-only policy cannot contain Daytona authority")
            if self.spillover_after_queue_seconds != 0:
                raise ValueError("local-only policy cannot contain a spillover delay")
        else:
            expected = ("daytona",) if self.mode == "explicit" else ("docker", "daytona")
            if self.allowed_backends != expected:
                raise ValueError(
                    f"{self.mode} policy snapshot requires allowed_backends={list(expected)!r}"
                )
            if self.mode == "explicit" and self.spillover_after_queue_seconds != 0:
                raise ValueError("explicit Daytona policy snapshot cannot set a spillover delay")
            if any(v is None for v in cloud_values):
                raise ValueError("Daytona policy snapshot is incomplete")
            assert self.worst_case_cloud_cost_usd is not None
            assert self.max_cloud_cost_usd is not None
            if self.worst_case_cloud_cost_usd > self.max_cloud_cost_usd:
                raise ValueError("Daytona worst-case cost exceeds the hard cloud budget")
        return self


def policy_digest(snapshot: BackendPolicySnapshot | dict[str, Any]) -> str:
    payload = (
        snapshot.model_dump(mode="json")
        if isinstance(snapshot, BackendPolicySnapshot)
        else snapshot
    )
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def build_policy_snapshot(
    *,
    request: BackendPolicyRequest | None,
    expected_trial_count: int,
    max_attempts: int,
    authority: dict[str, Any],
    accepted_at: datetime | None = None,
) -> BackendPolicySnapshot:
    accepted = accepted_at or datetime.now(UTC)
    if request is None:
        return BackendPolicySnapshot(
            mode="local_only",
            allowed_backends=("docker",),
            spillover_after_queue_seconds=0,
            max_attempts=max_attempts,
            expected_trial_count=expected_trial_count,
            authority=authority,
            accepted_at=accepted,
        )

    resources = request.daytona_resources
    price = request.daytona_price_snapshot
    hourly = (
        Decimal(resources.cpu) * price.cpu_usd_per_hour
        + Decimal(resources.memory_gib) * price.memory_gib_usd_per_hour
        + Decimal(resources.disk_gib) * price.disk_gib_usd_per_hour
    )
    worst_case = (
        hourly
        * Decimal(request.max_runtime_seconds)
        * Decimal(max_attempts)
        * Decimal(expected_trial_count)
        / Decimal(3600)
    ).quantize(Decimal("0.000001"), rounding=ROUND_CEILING)
    return BackendPolicySnapshot(
        mode=request.mode,
        allowed_backends=request.allowed_backends,
        spillover_after_queue_seconds=request.spillover_after_queue_seconds,
        daytona_resources=resources,
        daytona_price_snapshot=price,
        max_cloud_cost_usd=request.max_cloud_cost_usd,
        max_runtime_seconds=request.max_runtime_seconds,
        max_attempts=max_attempts,
        expected_trial_count=expected_trial_count,
        worst_case_cloud_cost_usd=worst_case,
        authority=authority,
        accepted_at=accepted,
    )


def daytona_incompatibilities(task: TaskConfig) -> list[dict[str, str]]:
    """Return stable, structured reasons that make Daytona execution unsafe."""

    env = task.environment
    reasons: list[dict[str, str]] = []

    def reject(code: str, detail: str) -> None:
        reasons.append({"code": code, "detail": detail})

    if env.cpu_arch not in {"x86_64", "any"}:
        reject("cpu_arch_unsupported", "Daytona service workers execute x86_64 images")
    if env.gpu_vendor != "none" or env.gpus > 0:
        reject("gpu_unsupported", "Daytona overflow is CPU-only")
    if env.sidecars:
        reject("sidecars_unsupported", "Daytona does not implement task sidecars")
    if env.extra_hosts or env.dns or env.tmpfs:
        reject(
            "custom_network_unsupported",
            "Daytona cannot honor extra_hosts, custom DNS, or tmpfs settings",
        )
    if env.skills_dir is not None:
        reject("local_resource_unsupported", "host-local skills_dir cannot cross into cloud")
    image = env.docker_image
    if env.dockerfile is None and (image is None or "@sha256:" not in image):
        reject(
            "mutable_image_unsupported",
            "Daytona requires a build materialization or digest-pinned image",
        )
    if any(step.verifier is not None and step.verifier.env_mode == "separate" for step in task.steps):
        reject(
            "private_verifier_unsupported",
            "Daytona does not support the separate private verifier runtime",
        )
    return reasons
