"""Durable task-image materialization identities and queue helpers."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TrialTaskImageMaterialization,
)
from loom.models.task import TaskConfig

NativeCPUArch = Literal["x86_64", "arm64"]
_CHECKSUM_RE = re.compile(r"[0-9a-f]{64}")
_KEY_DOMAIN = "task-image-materialization-v1"
_BareSHA256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_ImmutableRegistryImage = Annotated[
    str,
    StringConstraints(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$"),
]


class TaskImageExecutionGrantV1(BaseModel):
    """Immutable build evidence carried from scheduling into execution."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["loom.task-image-execution-grant.v1"]
    materialization_id: UUID
    materialization_key: _BareSHA256
    cpu_arch: NativeCPUArch
    task_checksum: _BareSHA256
    task_config: dict[str, Any]
    task_source: str | None
    task_source_provenance: dict[str, Any]
    registry_images: dict[str, _ImmutableRegistryImage]

    @field_validator("registry_images")
    @classmethod
    def registry_images_must_not_be_empty(cls, values: dict[str, str]) -> dict[str, str]:
        if not values:
            raise ValueError("registry_images must not be empty")
        return values

    @model_validator(mode="after")
    def matches_frozen_task_snapshot(self) -> TaskImageExecutionGrantV1:
        task = TaskConfig.model_validate(self.task_config)
        if self.cpu_arch not in required_task_image_architectures(task):
            raise ValueError("cpu_arch is not required by the frozen task snapshot")
        expected = required_task_image_components(task)
        if set(self.registry_images) != expected:
            raise ValueError("registry_images do not match the frozen task snapshot")
        return self


def canonical_task_checksum(task_checksum: str) -> str:
    checksum = task_checksum.removeprefix("sha256:")
    if _CHECKSUM_RE.fullmatch(checksum) is None:
        raise ValueError("task_checksum must be a SHA-256 digest")
    return checksum


def required_task_image_architectures(task: TaskConfig) -> tuple[NativeCPUArch, ...]:
    has_dockerfile = task.environment.dockerfile is not None or any(
        sidecar.dockerfile is not None for sidecar in task.environment.sidecars
    )
    if not has_dockerfile:
        return ()
    if task.environment.cpu_arch == "any":
        return ("x86_64", "arm64")
    return (task.environment.cpu_arch,)


def required_task_image_components(task: TaskConfig) -> set[str]:
    components: set[str] = set()
    if task.environment.dockerfile is not None:
        components.add("task")
    components.update(
        f"sidecar:{sidecar.name}"
        for sidecar in task.environment.sidecars
        if sidecar.dockerfile is not None
    )
    return components


def task_image_materialization_key(
    *,
    task_id: str,
    task_checksum: str,
    cpu_arch: str,
) -> str:
    if cpu_arch not in {"x86_64", "arm64"}:
        raise ValueError("cpu_arch must be x86_64 or arm64")
    checksum = canonical_task_checksum(task_checksum)
    material = "\0".join((_KEY_DOMAIN, task_id, checksum, cpu_arch))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


async def ensure_task_image_materializations(
    session: AsyncSession,
    *,
    task_row: Task,
) -> tuple[TaskImageMaterialization, ...]:
    task = TaskConfig.model_validate(task_row.config)
    architectures = required_task_image_architectures(task)
    if not architectures:
        return ()

    task_checksum = canonical_task_checksum(task_row.checksum)
    keys = {
        cpu_arch: task_image_materialization_key(
            task_id=task_row.id,
            task_checksum=task_checksum,
            cpu_arch=cpu_arch,
        )
        for cpu_arch in architectures
    }
    for cpu_arch in architectures:
        await session.execute(
            pg_insert(TaskImageMaterialization)
            .values(
                id=uuid4(),
                materialization_key=keys[cpu_arch],
                task_id=task_row.id,
                task_checksum=task_checksum,
                cpu_arch=cpu_arch,
                task_config=task_row.config,
                task_source=task_row.source,
                task_source_provenance=task_row.source_provenance,
                state="queued",
            )
            .on_conflict_do_nothing(index_elements=["materialization_key"])
        )

    rows = (
        (
            await session.execute(
                select(TaskImageMaterialization)
                .where(TaskImageMaterialization.materialization_key.in_(keys.values()))
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    by_arch = {row.cpu_arch: row for row in rows}
    if set(by_arch) != set(architectures):
        raise RuntimeError("task image materialization identity conflict")
    now = datetime.now(UTC)
    for row in rows:
        row.last_referenced_at = now
        row.unreferenced_at = None
        row.updated_at = now
        if row.state == "retired":
            row.state = "queued"
            row.attempt_count = 0
            row.next_attempt_at = None
            row.claimed_by = None
            row.lease_expires_at = None
            row.registry_images = {}
            row.failure_reason = None
            row.failure_message = None
            row.claimed_at = None
            row.started_at = None
            row.ready_at = None
            row.finished_at = None
    await session.flush()
    return tuple(by_arch[cpu_arch] for cpu_arch in architectures)


async def get_trial_task_image_execution_grant(
    session: AsyncSession,
    *,
    trial_id: UUID,
    cpu_arches: list[str],
) -> TaskImageExecutionGrantV1 | None:
    """Lock and return the exact ready image snapshot selected for a claim."""
    row = await session.scalar(
        select(TaskImageMaterialization)
        .join(
            TrialTaskImageMaterialization,
            TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
        )
        .where(
            TrialTaskImageMaterialization.trial_id == trial_id,
            TaskImageMaterialization.cpu_arch.in_(cpu_arches),
            TaskImageMaterialization.state == "ready",
        )
        .order_by(TaskImageMaterialization.cpu_arch, TaskImageMaterialization.id)
        .limit(1)
        .with_for_update()
    )
    if row is None:
        has_prerequisite = bool(
            await session.scalar(
                select(exists().where(TrialTaskImageMaterialization.trial_id == trial_id))
            )
        )
        if has_prerequisite:
            raise RuntimeError("claimed trial no longer has a ready task-image materialization")
        return None
    return TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1",
        materialization_id=row.id,
        materialization_key=row.materialization_key,
        cpu_arch=row.cpu_arch,
        task_checksum=row.task_checksum,
        task_config=row.task_config,
        task_source=row.task_source,
        task_source_provenance=row.task_source_provenance,
        registry_images=row.registry_images,
    )
