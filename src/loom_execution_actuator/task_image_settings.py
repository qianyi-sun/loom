"""Native builder configuration shared by offline rendering and the actuator.

Keep service/database imports out of this module: deployment preflight only
needs to validate settings, not instantiate a running controller.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:
    from loom_execution_actuator.task_image_renderer import TaskImageJobConfig


class NativeTaskImageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    namespace: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$")
    pool_id: str = "nebius-cpu"
    service_image: str
    source_secret_name: str = "loom-task-build-source"
    cache_secret_name: str | None = None
    registry_secret_name: str = "loom-task-build-registry"
    registry_auth_kind: Literal["docker-config", "nebius"] = "nebius"
    storage_endpoint: str
    storage_region: str
    source_bucket: str
    cache_bucket: str | None = None
    registry_repository: str
    cpu_millis: int = Field(default=1000, ge=100, le=64000)
    memory_mib: int = Field(default=2048, ge=512, le=262144)
    ephemeral_storage_mib: int = Field(default=16384, ge=16384, le=524288)
    max_processes: int = Field(default=512, ge=64, le=4096)
    active_deadline_seconds: int = Field(default=1800, ge=60, le=7200)
    max_concurrent: int = Field(default=1, ge=1, le=16)
    # buildkit (default) or compose (opt-in, one Job per mat — see #2086).
    builder_engine: Literal["buildkit", "compose"] = "buildkit"
    # OverlayFS is the measured Nebius default; set "native" to roll back.
    snapshotter: Literal["overlayfs", "native"] = "overlayfs"
    # When "same_task", prepare may import BuildKit cache from a prior ready
    # revision of the same task_id+cpu_arch. Results still use this mat key.
    compatible_revision_cache: Literal["off", "same_task"] = "off"
    # BuildKit local export mode; keep max until Nebius timing compares say otherwise.
    export_cache_mode: Literal["max", "min"] = "max"
    # Incremental content-addressed blobs (default) or whole-archive tar rollback.
    cache_transfer: Literal["tar", "blobs"] = "blobs"
    # BuildKit OCI export shape; archive is today's path, directory is measure-gated.
    oci_export_format: Literal["archive", "directory"] = "archive"

    @model_validator(mode="after")
    def _compose_rejects_buildkit_only_knobs(self) -> NativeTaskImageSettings:
        if self.builder_engine != "compose":
            return self
        # Compose v1 does not speak BuildKit local cache or S3 task-build-cache.
        if self.cache_bucket is not None or self.cache_secret_name is not None:
            raise ValueError("compose builder cannot use BuildKit S3 cache")
        if self.compatible_revision_cache != "off":
            raise ValueError("compose builder does not support compatible_revision_cache")
        if self.cache_transfer != "blobs":
            raise ValueError("compose builder does not use cache_transfer")
        if self.export_cache_mode != "max":
            raise ValueError("compose builder does not use export_cache_mode")
        if self.snapshotter != "overlayfs":
            raise ValueError("compose builder does not use BuildKit snapshotter")
        if self.oci_export_format != "archive":
            raise ValueError("compose builder v1 only supports oci_export_format=archive")
        return self

    def job_config(self) -> TaskImageJobConfig:
        from loom_execution_actuator.task_image_renderer import TaskImageJobConfig

        return TaskImageJobConfig(**{key: getattr(self, key) for key in (
            "service_image", "source_secret_name", "cache_secret_name", "registry_secret_name", "registry_auth_kind",
            "cpu_millis", "memory_mib", "ephemeral_storage_mib", "max_processes", "active_deadline_seconds",
            "builder_engine", "snapshotter", "export_cache_mode", "oci_export_format",
        )})

    def runtime_configuration(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in (
            "storage_endpoint", "storage_region", "source_bucket", "cache_bucket", "registry_repository",
            "builder_engine", "cache_transfer", "oci_export_format",
        )}
