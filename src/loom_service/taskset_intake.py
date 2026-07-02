"""TaskSet intake: manifest validation, blob upload, transactional enqueue.

Sub-plan 3 implements the ``task_set_materialization_jobs`` consumer.
Jobs remain ``queued`` after successful POST until that worker ships.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from fastapi import HTTPException, UploadFile
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskSet,
    TaskSetManifest,
    TaskSetMaterializationJob,
)
from loom.db.task_set_visibility import visible_task_sets
from loom.models.taskset import (
    UserTaskSetManifest,
    bundle_object_key,
    task_set_id_for,
)
from loom.taskset.intents import IntentWarning, normalize_intents

_ACTIVE_JOB_STATES = frozenset({"queued", "claimed", "running"})
_PATH_TRAVERSAL = re.compile(r"(^|[/\\])\.\.([/\\]|$)|(^|[/\\])\.\.?$")
_MANIFEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class TaskSetIntakeResult:
    task_set_id: str
    status: str
    intents: list[str]
    manifest_intents: list[str]
    inferred_intents: list[str]
    capabilities: list[str]
    warnings: list[dict[str, str]]
    evaluation_ready: bool
    task_count: int
    job_id: UUID


def _reject_unsafe_filename(filename: str) -> None:
    if not filename or filename.strip() != filename:
        raise HTTPException(status_code=400, detail="invalid upload filename")
    if _PATH_TRAVERSAL.search(filename) or filename.startswith("/"):
        raise HTTPException(status_code=400, detail="path traversal in upload filename")


def _blob_uri(*, bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def _storage_prefix(*, team_id: UUID, slug: str) -> str:
    return f"tasksets/user/{team_id}/{slug}"


async def _read_upload(upload: UploadFile) -> bytes:
    data = await upload.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty upload part")
    return data


async def parse_manifest_upload(manifest: UploadFile) -> tuple[UserTaskSetManifest, dict[str, Any]]:
    _reject_unsafe_filename(manifest.filename or "manifest")
    raw_bytes = await _read_upload(manifest)
    try:
        parsed = yaml.safe_load(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid manifest yaml: {exc}") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="manifest must be a yaml mapping")
    try:
        model = UserTaskSetManifest.model_validate(parsed)
    except ValidationError as exc:
        for err in exc.errors():
            if "verifier_required_for_evaluation" in str(err.get("msg", "")):
                raise HTTPException(
                    status_code=400,
                    detail="verifier_required_for_evaluation",
                ) from exc
        raise HTTPException(status_code=400, detail=exc.errors()) from exc
    return model, parsed


def _warnings_to_dict(warnings: tuple[IntentWarning, ...]) -> list[dict[str, str]]:
    return [{"code": w.code, "message": w.message} for w in warnings]


def _upload_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


async def submit_task_set(
    session: AsyncSession,
    *,
    team_id: UUID,
    minio_client: Any,
    artifacts_bucket: str,
    manifest_upload: UploadFile,
    verifier_upload: UploadFile | None,
    transform_upload: UploadFile | None,
) -> TaskSetIntakeResult:
    manifest_model, raw_manifest = await parse_manifest_upload(manifest_upload)
    slug = manifest_model.slug
    task_set_id = task_set_id_for(team_id=str(team_id), slug=slug)
    prefix = _storage_prefix(team_id=team_id, slug=slug)

    verifier_bytes: bytes | None = None
    verifier_key: str | None = None
    if manifest_model.verifier is not None:
        if verifier_upload is None:
            raise HTTPException(
                status_code=400,
                detail="verifier file required when manifest declares verifier",
            )
        _reject_unsafe_filename(verifier_upload.filename or manifest_model.verifier.file)
        verifier_bytes = await _read_upload(verifier_upload)
        verifier_key = bundle_object_key(
            prefix=prefix, relative_path=manifest_model.verifier.file,
        )

    transform_bytes: bytes | None = None
    transform_key: str | None = None
    if manifest_model.transform is not None:
        if transform_upload is None:
            raise HTTPException(
                status_code=400,
                detail="transform file required when manifest declares transform",
            )
        _reject_unsafe_filename(transform_upload.filename or manifest_model.transform.file)
        transform_bytes = await _read_upload(transform_upload)
        transform_key = bundle_object_key(
            prefix=prefix, relative_path=manifest_model.transform.file,
        )

    manifest_key = f"{prefix}/manifest.yaml"
    manifest_bytes = yaml.safe_dump(
        raw_manifest, sort_keys=False,
    ).encode("utf-8")

    normalized = normalize_intents(
        manifest_model,
        verifier_file_present=verifier_bytes is not None,
    )

    manifest_blob_uri = _blob_uri(bucket=artifacts_bucket, key=manifest_key)
    verifier_blob_uri: str | None = None
    transform_blob_uri: str | None = None
    if verifier_bytes is not None and verifier_key is not None:
        verifier_blob_uri = _blob_uri(bucket=artifacts_bucket, key=verifier_key)
    if transform_bytes is not None and transform_key is not None:
        transform_blob_uri = _blob_uri(bucket=artifacts_bucket, key=transform_key)

    job = TaskSetMaterializationJob(
        task_set_id=task_set_id,
        owning_team_id=team_id,
        state="queued",
    )
    task_set = TaskSet(
        id=task_set_id,
        owning_team_id=team_id,
        slug=slug,
        display_name=manifest_model.metadata.display_name,
        status="materializing",
        intents=normalized.effective_intents,
        evaluation_ready=False,
        manifest_blob_uri=manifest_blob_uri,
        task_count=0,
    )
    manifest_row = TaskSetManifest(
        task_set_id=task_set_id,
        schema_version=_MANIFEST_SCHEMA_VERSION,
        manifest=raw_manifest,
        verifier_blob_uri=verifier_blob_uri,
        transform_blob_uri=transform_blob_uri,
    )
    session.add(task_set)
    session.add(manifest_row)
    session.add(job)
    try:
        await session.flush()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="task_set slug already exists for this team",
        ) from exc

    try:
        _upload_object(
            minio_client,
            bucket=artifacts_bucket,
            key=manifest_key,
            body=manifest_bytes,
            content_type="application/x-yaml",
        )
        if verifier_bytes is not None and verifier_key is not None:
            _upload_object(
                minio_client,
                bucket=artifacts_bucket,
                key=verifier_key,
                body=verifier_bytes,
                content_type="application/octet-stream",
            )
        if transform_bytes is not None and transform_key is not None:
            _upload_object(
                minio_client,
                bucket=artifacts_bucket,
                key=transform_key,
                body=transform_bytes,
                content_type="text/x-python",
            )
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    await session.refresh(job)

    return TaskSetIntakeResult(
        task_set_id=task_set_id,
        status="materializing",
        intents=normalized.effective_intents,
        manifest_intents=normalized.manifest_intents,
        inferred_intents=normalized.inferred_intents,
        capabilities=normalized.capabilities,
        warnings=_warnings_to_dict(normalized.warnings),
        evaluation_ready=False,
        task_count=0,
        job_id=job.id,
    )


async def get_visible_task_set(
    session: AsyncSession,
    *,
    team_id: UUID | None,
    task_set_id: str,
) -> TaskSet:
    row = (await session.execute(
        visible_task_sets(team_id=team_id).where(TaskSet.id == task_set_id),
    )).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="task_set not found")
    return row


async def get_latest_job(
    session: AsyncSession,
    task_set_id: str,
) -> TaskSetMaterializationJob | None:
    return (await session.execute(
        select(TaskSetMaterializationJob)
        .where(TaskSetMaterializationJob.task_set_id == task_set_id)
        .order_by(TaskSetMaterializationJob.enqueued_at.desc())
        .limit(1),
    )).scalar_one_or_none()


async def rebuild_task_set(
    session: AsyncSession,
    *,
    team_id: UUID | None,
    task_set_id: str,
) -> TaskSetIntakeResult:
    task_set = await get_visible_task_set(
        session, team_id=team_id, task_set_id=task_set_id,
    )
    if task_set.status == "deleted":
        raise HTTPException(status_code=404, detail="task_set not found")

    active = (await session.execute(
        select(TaskSetMaterializationJob).where(
            TaskSetMaterializationJob.task_set_id == task_set_id,
            TaskSetMaterializationJob.state.in_(_ACTIVE_JOB_STATES),
        ),
    )).scalar_one_or_none()
    if active is not None:
        raise HTTPException(
            status_code=409,
            detail="materialization_already_active",
        )

    manifest_row = (await session.execute(
        select(TaskSetManifest).where(TaskSetManifest.task_set_id == task_set_id),
    )).scalar_one_or_none()
    if manifest_row is None:
        raise HTTPException(status_code=404, detail="task_set manifest not found")

    try:
        manifest_model = UserTaskSetManifest.model_validate(manifest_row.manifest)
    except ValidationError as exc:
        raise HTTPException(status_code=500, detail="stored manifest invalid") from exc

    normalized = normalize_intents(
        manifest_model,
        verifier_file_present=manifest_row.verifier_blob_uri is not None,
    )

    task_set.status = "materializing"
    task_set.status_reason = None
    task_set.updated_at = datetime.now(UTC)
    job = TaskSetMaterializationJob(
        task_set_id=task_set_id,
        owning_team_id=task_set.owning_team_id,
        state="queued",
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="materialization_already_active",
        ) from exc
    await session.refresh(job)

    return TaskSetIntakeResult(
        task_set_id=task_set_id,
        status="materializing",
        intents=normalized.effective_intents,
        manifest_intents=normalized.manifest_intents,
        inferred_intents=normalized.inferred_intents,
        capabilities=normalized.capabilities,
        warnings=_warnings_to_dict(normalized.warnings),
        evaluation_ready=task_set.evaluation_ready,
        task_count=task_set.task_count,
        job_id=job.id,
    )


async def delete_task_set(
    session: AsyncSession,
    *,
    team_id: UUID | None,
    task_set_id: str,
) -> None:
    task_set = await get_visible_task_set(
        session, team_id=team_id, task_set_id=task_set_id,
    )
    now = datetime.now(UTC)
    task_set.status = "deleted"
    task_set.soft_deleted_at = now
    task_set.updated_at = now

    active_jobs = (await session.execute(
        select(TaskSetMaterializationJob).where(
            TaskSetMaterializationJob.task_set_id == task_set_id,
            TaskSetMaterializationJob.state.in_(_ACTIVE_JOB_STATES),
        ),
    )).scalars().all()
    for job in active_jobs:
        job.state = "cancelled"
        job.finished_at = now
        job.updated_at = now
    await session.commit()


def intake_result_to_response(result: TaskSetIntakeResult) -> dict[str, Any]:
    return {
        "task_set_id": result.task_set_id,
        "status": result.status,
        "intents": result.intents,
        "manifest_intents": result.manifest_intents,
        "inferred_intents": result.inferred_intents,
        "capabilities": result.capabilities,
        "warnings": result.warnings,
        "evaluation_ready": result.evaluation_ready,
        "task_count": result.task_count,
        "materialization_job_id": str(result.job_id),
    }
