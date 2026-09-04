"""Pure derivation of one frozen, session-bound task-image build plan."""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Protocol
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from loom.models.task import TaskConfig, TaskSidecarConfig
from loom.task_image_materialization import required_task_image_architectures

MAX_TASK_IMAGE_BUILD_BUNDLE_FILES = 2_000
MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES = 512 * 1024 * 1024
MAX_TASK_IMAGE_BUILD_PLAN_BYTES = 64 * 1024
MAX_TASK_IMAGE_BUILD_TIMEOUT_SECONDS = 2 * 60 * 60
MAX_TASK_IMAGE_BUILD_COMPONENTS = 128

_BARE_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_METADATA_DIGEST_RE = re.compile(r"sha256:([0-9a-f]{64})")
_BUCKET_RE = re.compile(r"[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])?")
_COMPONENT_RE = re.compile(r"(?:task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})")


class _MaterializationRow(Protocol):
    id: UUID
    task_id: str
    task_checksum: str
    cpu_arch: str
    task_config: dict[str, Any]
    task_source: str | None
    task_source_provenance: dict[str, Any]


class _BuildSessionAuthorization(Protocol):
    @property
    def grant_id(self) -> UUID: ...

    @property
    def session_id(self) -> UUID: ...

    @property
    def session_generation(self) -> int: ...

    @property
    def authority_version(self) -> int: ...

    @property
    def builder_release_sha256(self) -> str | None: ...

    @property
    def cpu_arch(self) -> str: ...

    @property
    def attestation_expires_at(self) -> datetime: ...

    @property
    def session_expires_at(self) -> datetime: ...

    @property
    def grant_expires_at(self) -> datetime: ...


def _nonzero_uuid(value: UUID) -> UUID:
    if value.int == 0:
        raise ValueError("build plan UUIDs must be nonzero")
    return value


def _nonzero_digest(value: str) -> str:
    if _BARE_DIGEST_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError("build plan digest must be a nonzero lowercase SHA-256")
    return value


NonzeroUUID = Annotated[UUID, AfterValidator(_nonzero_uuid)]
Digest = Annotated[
    str,
    Field(pattern=r"^[0-9a-f]{64}$"),
    AfterValidator(_nonzero_digest),
]


def _canonical_relative_path(value: str, *, allow_root: bool, label: str) -> str:
    if value == "." and allow_root:
        return value
    if (
        not value
        or "\x00" in value
        or "\\" in value
        or value.startswith("/")
        or value == "."
        or (value != "." and PurePosixPath(value).as_posix() != value)
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ValueError(f"{label} path is not canonical relative POSIX")
    return value


def _path_is_within_context(dockerfile: str, context: str) -> bool:
    if context == ".":
        return True
    context_parts = PurePosixPath(context).parts
    dockerfile_parts = PurePosixPath(dockerfile).parts
    return dockerfile_parts[: len(context_parts)] == context_parts


def _canonical_bundle_location(source: object) -> tuple[str, str]:
    if not isinstance(source, str) or not source.startswith("s3://"):
        raise ValueError("task-image bundle source must be canonical s3")
    location = source.removeprefix("s3://")
    bucket, separator, prefix = location.partition("/")
    if (
        separator != "/"
        or _BUCKET_RE.fullmatch(bucket) is None
        or ".." in bucket
        or bucket.startswith("xn--")
        or bucket.endswith("-s3alias")
        or not prefix
        or not prefix.endswith("/")
        or "\x00" in prefix
        or "\\" in prefix
        or "?" in prefix
        or "#" in prefix
        or any(part in {"", ".", ".."} for part in prefix[:-1].split("/"))
    ):
        raise ValueError("task-image bundle source is not canonical")
    return bucket, prefix


class TaskImageBuildComponentV1(BaseModel):
    """One Dockerfile-backed component in canonical execution order."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    name: Annotated[str, Field(min_length=1, max_length=136, pattern=_COMPONENT_RE.pattern)]
    dockerfile_path: Annotated[str, Field(min_length=1, max_length=4096)]
    context_path: Annotated[str, Field(min_length=1, max_length=4096)]
    oci_output_path: Annotated[
        str,
        Field(pattern=r"^oci/(?:0|[1-9][0-9]{0,2}){4}\.tar$"),
    ]

    @model_validator(mode="after")
    def _paths_are_safe(self) -> TaskImageBuildComponentV1:
        _canonical_relative_path(
            self.dockerfile_path,
            allow_root=False,
            label="Dockerfile",
        )
        _canonical_relative_path(self.context_path, allow_root=True, label="context")
        _canonical_relative_path(self.oci_output_path, allow_root=False, label="OCI output")
        if not _path_is_within_context(self.dockerfile_path, self.context_path):
            raise ValueError("Dockerfile path is outside its build context")
        return self


class TaskImageBuildPlanV1(BaseModel):
    """Strict, immutable plan containing no transport URL or credential."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["loom.task-image-build-plan.v1"] = "loom.task-image-build-plan.v1"
    grant_id: NonzeroUUID
    session_id: NonzeroUUID
    session_generation: Annotated[int, Field(gt=0)]
    materialization_id: NonzeroUUID
    builder_id: Annotated[
        str,
        Field(pattern=r"^rootless:[0-9a-f]{32}$", min_length=41, max_length=41),
    ]
    task_id: Annotated[str, Field(min_length=1, max_length=512)]
    task_checksum: Digest
    cpu_arch: Literal["x86_64", "arm64"]
    platform: Literal["linux/amd64", "linux/arm64"]
    bundle_bucket: Annotated[str, Field(min_length=3, max_length=63)]
    bundle_prefix: Annotated[str, Field(min_length=2, max_length=4096)]
    bundle_file_metadata_sha256: Digest
    bundle_file_limit: Annotated[
        int,
        Field(gt=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_FILES),
    ]
    bundle_byte_limit: Annotated[
        int,
        Field(gt=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES),
    ]
    build_timeout_seconds: Annotated[
        float,
        Field(gt=0, le=MAX_TASK_IMAGE_BUILD_TIMEOUT_SECONDS, allow_inf_nan=False),
    ]
    authorization_expires_at: datetime
    components: Annotated[
        tuple[TaskImageBuildComponentV1, ...],
        Field(min_length=1, max_length=MAX_TASK_IMAGE_BUILD_COMPONENTS),
    ]

    @field_validator("authorization_expires_at")
    @classmethod
    def _canonical_expiry(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("build plan authorization expiry must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _bindings_are_exact(self) -> TaskImageBuildPlanV1:
        if self.builder_id != f"rootless:{self.session_id.hex}":
            raise ValueError("build plan builder identity differs from its session")
        expected_platform = "linux/amd64" if self.cpu_arch == "x86_64" else "linux/arm64"
        if self.platform != expected_platform:
            raise ValueError("build plan architecture and platform disagree")
        source = f"s3://{self.bundle_bucket}/{self.bundle_prefix}"
        if _canonical_bundle_location(source) != (
            self.bundle_bucket,
            self.bundle_prefix,
        ):
            raise ValueError("build plan bundle location is not canonical")
        component_names = tuple(component.name for component in self.components)
        if len(component_names) != len(set(component_names)):
            raise ValueError("build plan contains duplicate components")
        if component_names != tuple(
            sorted(component_names, key=lambda item: (item != "task", item))
        ):
            raise ValueError("build plan components are not in canonical order")
        for index, component in enumerate(self.components):
            if component.oci_output_path != f"oci/{index:04d}.tar":
                raise ValueError("build plan OCI output names are not canonical")
        return self


def _raw_environment(task_config: object) -> dict[str, Any]:
    if not isinstance(task_config, dict):
        raise ValueError("frozen task config must be an object")
    environment = task_config.get("environment")
    if not isinstance(environment, dict):
        raise ValueError("frozen task environment must be an object")
    return environment


def _raw_optional_path(container: dict[str, Any], field: str, *, label: str) -> str | None:
    value = container.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{label} path must be a string")
    return _canonical_relative_path(
        value, allow_root=(field == "docker_build_context"), label=label
    )


def _component(
    *,
    name: str,
    dockerfile: str,
    context: str | None,
    index: int,
) -> TaskImageBuildComponentV1:
    context_path = context or "."
    return TaskImageBuildComponentV1(
        name=name,
        dockerfile_path=dockerfile,
        context_path=context_path,
        oci_output_path=f"oci/{index:04d}.tar",
    )


def _derived_components(
    task: TaskConfig,
    raw_environment: dict[str, Any],
) -> tuple[TaskImageBuildComponentV1, ...]:
    raw_sidecars = raw_environment.get("sidecars", [])
    if not isinstance(raw_sidecars, list):
        raise ValueError("frozen task sidecars must be an array")
    if len(raw_sidecars) != len(task.environment.sidecars):
        raise ValueError("frozen task sidecars changed during validation")

    names = [sidecar.name for sidecar in task.environment.sidecars]
    if len(names) != len(set(names)):
        raise ValueError("frozen task has a duplicate sidecar name")

    definitions: list[tuple[str, str, str | None]] = []
    raw_primary_dockerfile = _raw_optional_path(
        raw_environment,
        "dockerfile",
        label="primary Dockerfile",
    )
    raw_primary_context = _raw_optional_path(
        raw_environment,
        "docker_build_context",
        label="primary context",
    )
    if raw_primary_dockerfile is not None:
        definitions.append(("task", raw_primary_dockerfile, raw_primary_context))

    dockerfile_sidecars: list[tuple[str, str, str | None]] = []
    paired: list[tuple[TaskSidecarConfig, dict[str, Any]]] = []
    for sidecar, raw_sidecar in zip(task.environment.sidecars, raw_sidecars, strict=True):
        if not isinstance(raw_sidecar, dict):
            raise ValueError("frozen task sidecar must be an object")
        paired.append((sidecar, raw_sidecar))
    for sidecar, raw_sidecar in sorted(paired, key=lambda item: item[0].name):
        dockerfile = _raw_optional_path(
            raw_sidecar,
            "dockerfile",
            label=f"sidecar {sidecar.name} Dockerfile",
        )
        context = _raw_optional_path(
            raw_sidecar,
            "docker_build_context",
            label=f"sidecar {sidecar.name} context",
        )
        if dockerfile is not None:
            dockerfile_sidecars.append((f"sidecar:{sidecar.name}", dockerfile, context))
    definitions.extend(dockerfile_sidecars)

    if not definitions:
        raise ValueError("frozen task has no Dockerfile-backed component")
    if len(definitions) > MAX_TASK_IMAGE_BUILD_COMPONENTS:
        raise ValueError("frozen task has too many Dockerfile-backed components")
    return tuple(
        _component(
            name=name,
            dockerfile=dockerfile,
            context=context,
            index=index,
        )
        for index, (name, dockerfile, context) in enumerate(definitions)
    )


def derive_task_image_build_plan(
    row: _MaterializationRow,
    authorization: _BuildSessionAuthorization,
) -> TaskImageBuildPlanV1:
    """Derive all task-authored build inputs from one immutable database row."""

    if authorization.authority_version != 2 or authorization.builder_release_sha256 is None:
        raise ValueError("task-image build plan requires V2 release authority")
    if row.cpu_arch not in {"x86_64", "arm64"} or row.cpu_arch != authorization.cpu_arch:
        raise ValueError("task-image materialization and session architecture disagree")

    raw_environment = _raw_environment(row.task_config)
    task = TaskConfig.model_validate(row.task_config)
    if task.environment.os != "linux":
        raise ValueError("task-image build plan requires a Linux task environment")
    if task.task.id != row.task_id:
        raise ValueError("frozen task identity differs from its materialization")
    components = _derived_components(task, raw_environment)
    if row.cpu_arch not in required_task_image_architectures(task):
        raise ValueError("materialization architecture is not required by the frozen task")
    if _BARE_DIGEST_RE.fullmatch(row.task_checksum) is None or row.task_checksum == "0" * 64:
        raise ValueError("task-image materialization checksum is invalid")

    metadata = row.task_source_provenance.get("bundle_file_metadata_sha256")
    match = _METADATA_DIGEST_RE.fullmatch(metadata) if isinstance(metadata, str) else None
    if match is None or match.group(1) == "0" * 64:
        raise ValueError("task-image bundle metadata digest is missing or invalid")
    bundle_bucket, bundle_prefix = _canonical_bundle_location(row.task_source)

    timeout = float(task.environment.build_timeout_sec)
    if not math.isfinite(timeout) or timeout <= 0 or timeout > MAX_TASK_IMAGE_BUILD_TIMEOUT_SECONDS:
        raise ValueError("task-image build_timeout_sec is outside the allocation limit")

    plan = TaskImageBuildPlanV1(
        grant_id=authorization.grant_id,
        session_id=authorization.session_id,
        session_generation=authorization.session_generation,
        materialization_id=row.id,
        builder_id=f"rootless:{authorization.session_id.hex}",
        task_id=row.task_id,
        task_checksum=row.task_checksum,
        cpu_arch=row.cpu_arch,
        platform="linux/amd64" if row.cpu_arch == "x86_64" else "linux/arm64",
        bundle_bucket=bundle_bucket,
        bundle_prefix=bundle_prefix,
        bundle_file_metadata_sha256=match.group(1),
        bundle_file_limit=MAX_TASK_IMAGE_BUILD_BUNDLE_FILES,
        bundle_byte_limit=MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
        build_timeout_seconds=timeout,
        authorization_expires_at=min(
            authorization.attestation_expires_at,
            authorization.session_expires_at,
            authorization.grant_expires_at,
        ),
        components=components,
    )
    if len(plan.model_dump_json().encode("utf-8")) > MAX_TASK_IMAGE_BUILD_PLAN_BYTES:
        raise ValueError("task-image build plan exceeds the authority response limit")
    return plan


__all__ = [
    "MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES",
    "MAX_TASK_IMAGE_BUILD_BUNDLE_FILES",
    "MAX_TASK_IMAGE_BUILD_COMPONENTS",
    "MAX_TASK_IMAGE_BUILD_PLAN_BYTES",
    "MAX_TASK_IMAGE_BUILD_TIMEOUT_SECONDS",
    "TaskImageBuildComponentV1",
    "TaskImageBuildPlanV1",
    "derive_task_image_build_plan",
]
