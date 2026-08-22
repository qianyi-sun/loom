"""Inert allocation-scoped task-image build environment contracts.

This module deliberately has no production composition or subprocess runner.
The checked-in policy remains disabled; callers must durably journal a grant
before injecting a runner and invoking :meth:`submit_once`.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_control_plane.elastic_slurm_worker_controller import SbatchRequest

_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_FEATURE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_ABSOLUTE_PATH_RE = re.compile(r"^/[A-Za-z0-9._/-]+$")
_WALL_TIME_RE = re.compile(r"^[0-9]{2}:[0-5][0-9]:[0-5][0-9]$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_JOB_ID_RE = re.compile(r"^[0-9]+$")
_COMMENT_PREFIX = "loom-task-builder-v1:grant="


class _StrictFrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_by_name=True,
    )


class RootlessBuildResourceRequestV1(_StrictFrozenModel):
    cpus: int = Field(gt=0)
    memory_mib: int = Field(gt=0)
    pids: int = Field(gt=0)
    scratch_bytes: int = Field(gt=0)
    scratch_inodes: int = Field(gt=0)
    wall_time: str
    swap_bytes: Literal[0] = 0

    @field_validator("wall_time")
    @classmethod
    def _wall_time(cls, value: str) -> str:
        if _WALL_TIME_RE.fullmatch(value) is None or value == "00:00:00":
            raise ValueError("rootless builder wall time must be positive HH:MM:SS")
        return value


class SlurmBuildRequestIdentityV1(_StrictFrozenModel):
    slurm_cluster_id: Literal["oldlab", "gb10"]
    cpu_arch: Literal["x86_64", "arm64"]
    submitting_identity: str
    partition: str
    account: str
    qos: str
    feature_constraint: str
    supervisor_path: str
    resources: RootlessBuildResourceRequestV1

    @field_validator("submitting_identity", "partition", "account", "qos")
    @classmethod
    def _safe_name(cls, value: str) -> str:
        if _SAFE_NAME_RE.fullmatch(value) is None:
            raise ValueError("rootless builder Slurm identity field is invalid")
        return value

    @field_validator("feature_constraint")
    @classmethod
    def _feature_constraint(cls, value: str) -> str:
        if _SAFE_FEATURE_RE.fullmatch(value) is None:
            raise ValueError("rootless builder feature constraint is invalid")
        return value

    @field_validator("supervisor_path")
    @classmethod
    def _supervisor_path(cls, value: str) -> str:
        if _SAFE_ABSOLUTE_PATH_RE.fullmatch(value) is None:
            raise ValueError("rootless builder supervisor path is invalid")
        return value

    @model_validator(mode="after")
    def _native_cluster_and_qos(self) -> SlurmBuildRequestIdentityV1:
        expected = {
            "oldlab": ("x86_64", "loom-task-image-builder-rootless-oldlab"),
            "gb10": ("arm64", "loom-task-image-builder-rootless-gb10"),
        }[self.slurm_cluster_id]
        if (self.cpu_arch, self.qos) != expected:
            raise ValueError("rootless builder architecture or QoS differs from its cluster")
        if self.partition != "loom-task-builder" or self.account != "loom-task-builder":
            raise ValueError("rootless builder must use its dedicated partition and account")
        if self.submitting_identity != "loom-builder":
            raise ValueError("rootless builder must use its dedicated submission identity")
        if self.feature_constraint != "loom_rootless_buildkit":
            raise ValueError("rootless builder capability constraint is invalid")
        return self


class SlurmBuildEnvironmentPolicyV1(_StrictFrozenModel):
    schema_version: Literal["loom.task-image-build-environment-policy/v1"] = Field(
        alias="schema"
    )
    enabled: bool
    activation_blockers: tuple[str, ...]
    slurm_cluster_id: Literal["oldlab", "gb10"]
    cpu_arch: Literal["x86_64", "arm64"]
    submitting_identity: str
    partition: str
    account: str
    qos: str
    feature_constraint: str
    supervisor_path: str
    sbatch_path: str
    resources: RootlessBuildResourceRequestV1

    @field_validator("activation_blockers")
    @classmethod
    def _blockers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(_SAFE_NAME_RE.fullmatch(item) is None for item in value):
            raise ValueError("rootless builder activation blocker is invalid")
        if len(value) != len(set(value)):
            raise ValueError("rootless builder activation blockers must be unique")
        return tuple(sorted(value))

    @field_validator("sbatch_path")
    @classmethod
    def _sbatch_path(cls, value: str) -> str:
        if _SAFE_ABSOLUTE_PATH_RE.fullmatch(value) is None:
            raise ValueError("rootless builder sbatch path is invalid")
        return value

    @model_validator(mode="after")
    def _request_and_activation(self) -> SlurmBuildEnvironmentPolicyV1:
        self.request_identity()
        if self.enabled and self.activation_blockers:
            raise ValueError("rootless builder cannot be enabled while activation blockers remain")
        return self

    def request_identity(self) -> SlurmBuildRequestIdentityV1:
        return SlurmBuildRequestIdentityV1(
            slurm_cluster_id=self.slurm_cluster_id,
            cpu_arch=self.cpu_arch,
            submitting_identity=self.submitting_identity,
            partition=self.partition,
            account=self.account,
            qos=self.qos,
            feature_constraint=self.feature_constraint,
            supervisor_path=self.supervisor_path,
            resources=self.resources,
        )


def canonical_request_sha256(request: SlurmBuildRequestIdentityV1) -> str:
    payload = rfc8785.dumps(request.model_dump(mode="json"))
    return hashlib.sha256(payload).hexdigest()


class SlurmBuildGrantV1(_StrictFrozenModel):
    schema_version: Literal["loom.task-image-build-grant/v1"] = Field(alias="schema")
    grant_id: UUID
    request: SlurmBuildRequestIdentityV1
    request_sha256: str
    comment: str

    @field_validator("request_sha256")
    @classmethod
    def _digest_shape(cls, value: str) -> str:
        if _DIGEST_RE.fullmatch(value) is None:
            raise ValueError("rootless builder request digest is invalid")
        return value

    @model_validator(mode="after")
    def _exact_bindings(self) -> SlurmBuildGrantV1:
        if self.request_sha256 != canonical_request_sha256(self.request):
            raise ValueError("rootless builder request digest changed")
        if self.comment != f"{_COMMENT_PREFIX}{self.grant_id}":
            raise ValueError("rootless builder grant comment changed")
        return self


def issue_slurm_build_grant(
    policy: SlurmBuildEnvironmentPolicyV1,
    *,
    grant_id: UUID,
) -> SlurmBuildGrantV1:
    request = policy.request_identity()
    return SlurmBuildGrantV1(
        schema_version="loom.task-image-build-grant/v1",
        grant_id=grant_id,
        request=request,
        request_sha256=canonical_request_sha256(request),
        comment=f"{_COMMENT_PREFIX}{grant_id}",
    )


class SlurmBuildJobObservationV1(_StrictFrozenModel):
    job_id: str
    state: Literal["pending", "running", "terminal", "unknown"]
    held: bool
    comment: str
    submitting_identity: str
    request: SlurmBuildRequestIdentityV1

    @field_validator("job_id")
    @classmethod
    def _job_id(cls, value: str) -> str:
        if _JOB_ID_RE.fullmatch(value) is None:
            raise ValueError("rootless builder Slurm job ID is invalid")
        return value


class SlurmBuildInventoryV1(_StrictFrozenModel):
    controller_authoritative: bool
    accounting_authoritative: bool
    observed_at: datetime
    jobs: tuple[SlurmBuildJobObservationV1, ...]

    @field_validator("jobs")
    @classmethod
    def _unique_jobs(
        cls,
        value: tuple[SlurmBuildJobObservationV1, ...],
    ) -> tuple[SlurmBuildJobObservationV1, ...]:
        job_ids = [item.job_id for item in value]
        if len(job_ids) != len(set(job_ids)):
            raise ValueError("rootless builder inventory contains duplicate job IDs")
        return tuple(sorted(value, key=lambda item: int(item.job_id)))


class SlurmBuildEnvironmentRunner(Protocol):
    async def submit(self, request: SbatchRequest) -> str: ...

    async def inventory(self, grant: SlurmBuildGrantV1) -> SlurmBuildInventoryV1: ...

    async def cancel(self, job_id: str) -> None: ...

    async def release(self, job_id: str) -> None: ...


class BuildEnvironmentProvider(Protocol):
    def render_submission(self, grant: SlurmBuildGrantV1) -> SbatchRequest: ...

    async def submit_once(self, grant: SlurmBuildGrantV1) -> str: ...

    async def inventory(self, grant: SlurmBuildGrantV1) -> SlurmBuildInventoryV1: ...

    async def cancel(self, job_id: str) -> None: ...

    async def release(self, job_id: str) -> None: ...


class BuildEnvironmentDisabledError(RuntimeError):
    """The inert provider lacks activation authority."""


def _validate_grant_policy(
    policy: SlurmBuildEnvironmentPolicyV1,
    grant: SlurmBuildGrantV1,
) -> None:
    if grant.request != policy.request_identity():
        raise ValueError("rootless builder grant differs from provider policy")


def render_rootless_builder_sbatch_request(
    policy: SlurmBuildEnvironmentPolicyV1,
    grant: SlurmBuildGrantV1,
) -> SbatchRequest:
    _validate_grant_policy(policy, grant)
    request = grant.request
    args = (
        policy.sbatch_path,
        "--parsable",
        "--hold",
        "--no-requeue",
        "--nodes=1",
        "--ntasks=1",
        f"--cpus-per-task={request.resources.cpus}",
        f"--mem={request.resources.memory_mib}M",
        f"--time={request.resources.wall_time}",
        f"--partition={request.partition}",
        f"--account={request.account}",
        f"--qos={request.qos}",
        f"--constraint={request.feature_constraint}",
        "--export=NONE",
        f"--comment={grant.comment}",
    )
    stdin = (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"exec {request.supervisor_path} --grant-id {grant.grant_id}\n"
    )
    return SbatchRequest(args=args, stdin=stdin)


class SlurmBuildEnvironmentProvider:
    def __init__(
        self,
        *,
        policy: SlurmBuildEnvironmentPolicyV1,
        runner: SlurmBuildEnvironmentRunner,
    ) -> None:
        self.policy = policy
        self.runner = runner

    def render_submission(self, grant: SlurmBuildGrantV1) -> SbatchRequest:
        return render_rootless_builder_sbatch_request(self.policy, grant)

    async def submit_once(self, grant: SlurmBuildGrantV1) -> str:
        if not self.policy.enabled or self.policy.activation_blockers:
            raise BuildEnvironmentDisabledError("rootless builder provider is disabled")
        return await self.runner.submit(self.render_submission(grant))

    async def inventory(self, grant: SlurmBuildGrantV1) -> SlurmBuildInventoryV1:
        _validate_grant_policy(self.policy, grant)
        return await self.runner.inventory(grant)

    async def cancel(self, job_id: str) -> None:
        await self.runner.cancel(job_id)

    async def release(self, job_id: str) -> None:
        await self.runner.release(job_id)


__all__ = [
    "BuildEnvironmentDisabledError",
    "BuildEnvironmentProvider",
    "RootlessBuildResourceRequestV1",
    "SlurmBuildEnvironmentPolicyV1",
    "SlurmBuildEnvironmentProvider",
    "SlurmBuildEnvironmentRunner",
    "SlurmBuildGrantV1",
    "SlurmBuildInventoryV1",
    "SlurmBuildJobObservationV1",
    "SlurmBuildRequestIdentityV1",
    "canonical_request_sha256",
    "issue_slurm_build_grant",
    "render_rootless_builder_sbatch_request",
]
