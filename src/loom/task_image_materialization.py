"""Durable task-image materialization identities and queue helpers."""

from __future__ import annotations

import hashlib
import re
from typing import Literal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Task, TaskImageMaterialization
from loom.models.task import TaskConfig

NativeCPUArch = Literal["x86_64", "arm64"]
_CHECKSUM_RE = re.compile(r"[0-9a-f]{64}")
_KEY_DOMAIN = "task-image-materialization-v1"


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
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.materialization_key.in_(keys.values())
                )
            )
        )
        .scalars()
        .all()
    )
    by_arch = {row.cpu_arch: row for row in rows}
    if set(by_arch) != set(architectures):
        raise RuntimeError("task image materialization identity conflict")
    return tuple(by_arch[cpu_arch] for cpu_arch in architectures)
